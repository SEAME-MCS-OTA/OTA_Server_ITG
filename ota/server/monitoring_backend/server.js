const express = require("express");
const cors = require("cors");
const dotenv = require("dotenv");
const fs = require("fs");
const path = require("path");

dotenv.config();

const { pool } = require("./db");

const app = express();
app.use(cors());
app.use(express.json({ limit: "2mb" }));

const CITY_COORDS = {
  Berlin: { lat: 52.5200, lon: 13.4050 },
  Munich: { lat: 48.1351, lon: 11.5820 },
  Hamburg: { lat: 53.5511, lon: 9.9937 },
  Wolfsburg: { lat: 52.4227, lon: 10.7865 },
  Stuttgart: { lat: 48.7758, lon: 9.1829 },
  Frankfurt: { lat: 50.1109, lon: 8.6821 },
  Cologne: { lat: 50.9375, lon: 6.9603 },
  "Düsseldorf": { lat: 51.2277, lon: 6.7735 },
  Leipzig: { lat: 51.3397, lon: 12.3731 },
  Hannover: { lat: 52.3759, lon: 9.7320 }
};

const SUCCESS_NAMES = new Set(["OK", "SUCCESS", "SUCCEEDED", "PASS", "PASSED"]);
const FAILED_CASE_BASE_DIR = path.resolve(
  process.env.FAILED_CASE_BASE_DIR || path.join(__dirname, "..", "..", "failed case")
);

function normalizeFolderName(name) {
  return String(name || "")
    .trim()
    .toUpperCase()
    .replace(/[^\w.-]+/g, "_") || "UNKNOWN";
}

function resolveCaseFolder(rec) {
  const errorCode = normalizeFolderName(
    pick(rec, "error.code", pick(rec, "error_code", ""))
  );
  const eventCode = normalizeFolderName(
    pick(rec, "ota.event", pick(rec, "ota_event", ""))
  );
  const phaseCode = normalizeFolderName(
    pick(rec, "ota.phase", pick(rec, "ota_phase", ""))
  );

  if (SUCCESS_NAMES.has(errorCode) || SUCCESS_NAMES.has(eventCode)) {
    return "SUCCESS";
  }
  if (!errorCode || errorCode === "NONE") {
    if (phaseCode === "REPORT" || !eventCode || SUCCESS_NAMES.has(eventCode)) {
      return "SUCCESS";
    }
    return "UNKNOWN";
  }
  return errorCode;
}

function buildRefinedResult(rec) {
  return {
    event_id: pick(rec, "ota.ota_id", pick(rec, "ota_id", "")),
    ts: rec.ts || "",
    device_id: pick(rec, "device.device_id", pick(rec, "device_id", "")),
    ota_id: pick(rec, "ota.ota_id", pick(rec, "ota_id", "")),
    current_version: pick(rec, "ota.current_version", pick(rec, "current_version", "")),
    target_version: pick(rec, "ota.target_version", pick(rec, "target_version", "")),
    ota_phase: pick(rec, "ota.phase", pick(rec, "ota_phase", "")),
    error: {
      code: pick(rec, "error.code", pick(rec, "error_code", "UNKNOWN")),
      message: pick(rec, "error.message", ""),
      retryable: Boolean(pick(rec, "error.retryable", false))
    },
    context: {
      region: {
        country: pick(rec, "context.region.country", ""),
        city: pick(rec, "context.region.city", ""),
        timezone: pick(rec, "context.region.timezone", "")
      },
      time: {
        local: pick(rec, "context.time.local", ""),
        day_of_week: pick(rec, "context.time.day_of_week", ""),
        time_bucket: pick(rec, "context.time.time_bucket", "")
      },
      power: {
        source: pick(rec, "context.power.source", ""),
        battery_pct: Number(pick(rec, "context.power.battery.pct", pick(rec, "context.power.battery_pct", 0))) || 0
      },
      network: {
        rssi_dbm: Number(pick(rec, "context.network.rssi_dbm", 0)) || 0,
        latency_ms: Number(pick(rec, "context.network.latency_ms", 0)) || 0
      }
    },
    evidence: {
      ota_log: pick(rec, "evidence.ota_log", []),
      journal_log: pick(rec, "evidence.journal_log", []),
      filesystem: pick(rec, "evidence.filesystem", []),
      screenshot_text: pick(rec, "evidence.screenshot_text", "")
    },
    vlm: {
      root_cause: pick(rec, "vlm.root_cause", pick(rec, "error.code", "UNKNOWN")),
      confidence: Number(pick(rec, "vlm.confidence", 0)) || 0,
      supporting_evidence: pick(rec, "vlm.supporting_evidence", [])
    },
    analysis: {
      tags: pick(rec, "analysis.tags", []),
      cluster_id: pick(rec, "analysis.cluster_id", ""),
      impact: pick(rec, "analysis.impact", { affected_devices: 0 })
    },
    log_vehicle: {
      brand: pick(rec, "log_vehicle.brand", ""),
      series: pick(rec, "log_vehicle.series", ""),
      segment: pick(rec, "log_vehicle.segment", ""),
      fuel: pick(rec, "log_vehicle.fuel", "")
    }
  };
}

function appendJsonl(filePath, record) {
  fs.mkdirSync(path.dirname(filePath), { recursive: true });
  fs.appendFileSync(filePath, `${JSON.stringify(record)}\n`, "utf8");
}

function shouldPersistCaseFiles(rec) {
  const src = String(pick(rec, "meta.source", "")).trim().toLowerCase();
  // Summary-only records from OTA_GH are useful for dashboard timelines,
  // but too sparse for 9-class failed-case datasets.
  if (src === "mqtt_status" || src === "api_report") {
    return false;
  }
  return true;
}

function persistFailedCaseFiles(rec) {
  if (!shouldPersistCaseFiles(rec)) {
    return "SKIPPED_SUMMARY";
  }
  const folder = resolveCaseFolder(rec);
  const dir = path.join(FAILED_CASE_BASE_DIR, folder);
  appendJsonl(path.join(dir, "dummy.jsonl"), rec);
  appendJsonl(path.join(dir, "result.jsonl"), buildRefinedResult(rec));
  return folder;
}

function isFailure(errorCode) {
  if (!errorCode) return false;
  const code = String(errorCode).toUpperCase();
  return code !== "NONE";
}

function pick(obj, path, fallback = "") {
  return path.split(".").reduce((acc, key) => (acc && acc[key] !== undefined ? acc[key] : undefined), obj) ?? fallback;
}

app.get("/health", async (_req, res) => {
  try {
    await pool.query("SELECT 1");
    res.json({ ok: true });
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message });
  }
});

app.post("/ingest", async (req, res) => {
  const rec = req.body;
  if (!rec || typeof rec !== "object") {
    return res.status(400).json({ error: "Invalid JSON body" });
  }

  const errorCode = pick(rec, "error.code", "");
  const isFail = isFailure(errorCode);

  const payload = {
    ts: rec.ts || null,
    device_id: rec.device_id || pick(rec, "device.device_id", ""),
    ota_id: rec.ota_id || pick(rec, "ota.ota_id", ""),
    current_version: rec.current_version || pick(rec, "ota.current_version", ""),
    target_version: rec.target_version || pick(rec, "ota.target_version", ""),
    ota_phase: rec.ota_phase || pick(rec, "ota.phase", ""),
    vehicle_brand: pick(rec, "log_vehicle.brand", ""),
    vehicle_series: pick(rec, "log_vehicle.series", ""),
    vehicle_segment: pick(rec, "log_vehicle.segment", ""),
    vehicle_fuel: pick(rec, "log_vehicle.fuel", ""),
    error_code: errorCode,
    error_message: pick(rec, "error.message", ""),
    retryable: pick(rec, "error.retryable", false) ? 1 : 0,
    country: pick(rec, "context.region.country", ""),
    city: pick(rec, "context.region.city", ""),
    timezone: pick(rec, "context.region.timezone", ""),
    local_time: pick(rec, "context.time.local", ""),
    day_of_week: pick(rec, "context.time.day_of_week", ""),
    time_bucket: pick(rec, "context.time.time_bucket", ""),
    power_source: pick(rec, "context.power.source", ""),
    battery_pct: pick(rec, "context.power.battery_pct", 0),
    rssi_dbm: pick(rec, "context.network.rssi_dbm", 0),
    latency_ms: pick(rec, "context.network.latency_ms", 0),
    vlm_root_cause: pick(rec, "vlm.root_cause", ""),
    vlm_confidence: pick(rec, "vlm.confidence", 0),
    supporting_evidence: JSON.stringify(pick(rec, "vlm.supporting_evidence", [])),
    tags: JSON.stringify(pick(rec, "analysis.tags", [])),
    is_failure: isFail ? 1 : 0,
    raw_json: JSON.stringify(rec)
  };

  try {
    // Keep filesystem artifacts in sync with MySQL ingest.
    // This preserves raw(dummy) and refined(result) JSONL streams by case.
    const caseFolder = persistFailedCaseFiles(rec);
    const sql = `
      INSERT INTO ota_logs (
        ts, device_id, ota_id, current_version, target_version, ota_phase,
        vehicle_brand, vehicle_series, vehicle_segment, vehicle_fuel,
        error_code, error_message, retryable,
        country, city, timezone, local_time, day_of_week, time_bucket,
        power_source, battery_pct, rssi_dbm, latency_ms,
        vlm_root_cause, vlm_confidence, supporting_evidence, tags,
        is_failure, raw_json
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `;
    const values = [
      payload.ts,
      payload.device_id,
      payload.ota_id,
      payload.current_version,
      payload.target_version,
      payload.ota_phase,
      payload.vehicle_brand,
      payload.vehicle_series,
      payload.vehicle_segment,
      payload.vehicle_fuel,
      payload.error_code,
      payload.error_message,
      payload.retryable,
      payload.country,
      payload.city,
      payload.timezone,
      payload.local_time,
      payload.day_of_week,
      payload.time_bucket,
      payload.power_source,
      payload.battery_pct,
      payload.rssi_dbm,
      payload.latency_ms,
      payload.vlm_root_cause,
      payload.vlm_confidence,
      payload.supporting_evidence,
      payload.tags,
      payload.is_failure,
      payload.raw_json
    ];
    await pool.execute(sql, values);
    res.json({ ok: true, case_folder: caseFolder });
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message });
  }
});

app.get("/stats/summary", async (req, res) => {
  const city = req.query.city;
  const where = city ? "WHERE city = ?" : "";
  const sql = `
    SELECT
      COUNT(*) AS total_records,
      SUM(is_failure) AS failure_records,
      CASE WHEN COUNT(*) = 0 THEN 0 ELSE SUM(is_failure) / COUNT(*) END AS failure_rate
    FROM ota_logs
    ${where}
  `;
  const [rows] = await pool.query(sql, city ? [city] : []);
  res.json(rows[0]);
});

app.get("/stats/root-cause", async (req, res) => {
  const city = req.query.city;
  const where = city ? "WHERE city = ?" : "";
  const sql = `
    SELECT
      COALESCE(NULLIF(vlm_root_cause, ''), 'UNKNOWN') AS root_cause,
      COUNT(*) AS count
    FROM ota_logs
    WHERE is_failure = 1
    ${city ? "AND city = ?" : ""}
    GROUP BY root_cause
    ORDER BY count DESC
  `;
  const [rows] = await pool.query(sql, city ? [city] : []);
  res.json(rows);
});

app.get("/stats/cities", async (_req, res) => {
  const sql = `
    SELECT
      COALESCE(NULLIF(city, ''), 'UNKNOWN') AS city,
      SUM(is_failure) AS failures,
      COUNT(*) AS total
    FROM ota_logs
    GROUP BY city
  `;
  const [rows] = await pool.query(sql);
  const data = rows.map((row) => {
    const coords = CITY_COORDS[row.city] || null;
    return {
      city: row.city,
      failures: Number(row.failures || 0),
      total: Number(row.total || 0),
      failure_rate: row.total ? Number(row.failures || 0) / Number(row.total) : 0,
      coords
    };
  });
  res.json(data);
});

app.get("/stats/time-bucket", async (req, res) => {
  const city = req.query.city;
  const where = city ? "WHERE city = ?" : "";
  const sql = `
    SELECT
      COALESCE(NULLIF(time_bucket, ''), 'UNKNOWN') AS time_bucket,
      SUM(is_failure) AS failures
    FROM ota_logs
    ${where}
    GROUP BY time_bucket
    ORDER BY time_bucket
  `;
  const [rows] = await pool.query(sql, city ? [city] : []);
  res.json(rows);
});

app.get("/stats/network-buckets", async (req, res) => {
  const city = req.query.city;
  const where = city ? "AND city = ?" : "";
  const sql = `
    SELECT rssi_dbm, latency_ms, is_failure
    FROM ota_logs
    WHERE is_failure = 1
    ${where}
  `;
  const [rows] = await pool.query(sql, city ? [city] : []);

  const rssiBins = [-100, -85, -75, -65, -55, -45, -35];
  const latencyBins = [0, 50, 100, 200, 300, 500, 1000];

  const rssiCounts = {};
  const latencyCounts = {};

  function bucketize(value, bins) {
    for (let i = 0; i < bins.length - 1; i += 1) {
      if (value >= bins[i] && value < bins[i + 1]) {
        return `${bins[i]}..${bins[i + 1]}`;
      }
    }
    return `${bins[bins.length - 1]}+`;
  }

  for (const row of rows) {
    const rssi = Number(row.rssi_dbm || 0);
    const latency = Number(row.latency_ms || 0);

    const rssiKey = bucketize(rssi, rssiBins);
    const latencyKey = bucketize(latency, latencyBins);

    rssiCounts[rssiKey] = (rssiCounts[rssiKey] || 0) + 1;
    latencyCounts[latencyKey] = (latencyCounts[latencyKey] || 0) + 1;
  }

  res.json({ rssi: rssiCounts, latency: latencyCounts });
});

app.get("/stats/models", async (req, res) => {
  const city = req.query.city;
  const where = city ? "AND city = ?" : "";
  const sql = `
    SELECT
      COALESCE(NULLIF(vehicle_series, ''), 'UNKNOWN') AS series,
      COUNT(*) AS count
    FROM ota_logs
    WHERE vehicle_series IS NOT NULL AND vehicle_series <> ''
    ${where}
    GROUP BY series
    ORDER BY count DESC
  `;
  const [rows] = await pool.query(sql, city ? [city] : []);
  res.json(rows);
});

app.get("/stats/raw", async (_req, res) => {
  const sql = `
    SELECT raw_json
    FROM ota_logs
    ORDER BY id DESC
    LIMIT 200
  `;
  const [rows] = await pool.query(sql);
  const parsedRows = rows.map((row) => {
    const raw = row.raw_json;
    if (raw === null || raw === undefined) {
      return {};
    }
    if (typeof raw === "string") {
      try {
        return JSON.parse(raw);
      } catch (_err) {
        return { raw_json: raw };
      }
    }
    if (typeof raw === "object") {
      return raw;
    }
    return { raw_json: raw };
  });
  res.json(parsedRows);
});

const port = Number(process.env.PORT || 4000);
app.listen(port, () => {
  console.log(`Backend listening on http://localhost:${port}`);
});
