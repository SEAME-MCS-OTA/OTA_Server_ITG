import React, { useEffect, useMemo, useState } from 'react';
import {
  RefreshCw,
  Server,
  Wifi,
  WifiOff,
  Download,
  CheckCircle,
  XCircle,
  Clock,
  AlertCircle,
  BarChart3,
  Activity,
  MapPin,
} from 'lucide-react';
import { MapContainer, TileLayer, CircleMarker, Tooltip as LeafletTooltip } from 'react-leaflet';

const API_BASE_URL =
  import.meta.env.VITE_APP_API_URL ||
  `${window.location.protocol}//${window.location.hostname}:8080`;

const MONITORING_API_BASE_URL =
  import.meta.env.VITE_VLM_API_URL ||
  '';

const REFRESH_INTERVAL = 5000;
const DEFAULT_ONLINE_WINDOW_SEC = 300;

const OTADashboard = () => {
  const [activeTab, setActiveTab] = useState('operations');

  const [serverHealth, setServerHealth] = useState(null);
  const [vehicles, setVehicles] = useState([]);
  const [firmware, setFirmware] = useState([]);
  const [selectedFirmwareId, setSelectedFirmwareId] = useState(null);
  const [uploadFile, setUploadFile] = useState(null);
  const [uploadVersion, setUploadVersion] = useState('');
  const [uploadNotes, setUploadNotes] = useState('');
  const [uploading, setUploading] = useState(false);
  const [loading, setLoading] = useState(true);

  const [monitoringSummary, setMonitoringSummary] = useState(null);
  const [monitoringRootCause, setMonitoringRootCause] = useState([]);
  const [monitoringCities, setMonitoringCities] = useState([]);
  const [monitoringTimeBucket, setMonitoringTimeBucket] = useState([]);
  const [monitoringModels, setMonitoringModels] = useState([]);
  const [monitoringNetwork, setMonitoringNetwork] = useState({ rssi: {}, latency: {} });
  const [monitoringLoading, setMonitoringLoading] = useState(true);
  const [monitoringError, setMonitoringError] = useState('');
  const [monitoringCity, setMonitoringCity] = useState('');

  const [lastUpdate, setLastUpdate] = useState(new Date());

  const fetchJsonOrThrow = async (url) => {
    const res = await fetch(url);
    if (!res.ok) {
      const text = await res.text().catch(() => '');
      throw new Error(text || `HTTP ${res.status}`);
    }
    return res.json();
  };

  const fetchOperationsData = async () => {
    try {
      const [health, vehiclesData, firmwareData] = await Promise.all([
        fetchJsonOrThrow(`${API_BASE_URL}/health`),
        fetchJsonOrThrow(`${API_BASE_URL}/api/v1/vehicles`),
        fetchJsonOrThrow(`${API_BASE_URL}/api/v1/firmware`),
      ]);

      const vehicleList = vehiclesData.vehicles || [];
      const firmwareList = firmwareData.firmware || [];

      setServerHealth(health);
      setVehicles(vehicleList);
      setFirmware(firmwareList);

      if (!selectedFirmwareId && firmwareList.length > 0) {
        const active = firmwareList.find((f) => !!f.is_active) || firmwareList[0];
        setSelectedFirmwareId(active.id);
      }
    } catch (error) {
      console.error('Failed to fetch operations data:', error);
    } finally {
      setLoading(false);
    }
  };

  const fetchMonitoringData = async () => {
    if (!MONITORING_API_BASE_URL) {
      setMonitoringError('');
      setMonitoringLoading(false);
      return;
    }
    try {
      const cityQuery = monitoringCity ? `?city=${encodeURIComponent(monitoringCity)}` : '';
      const labels = ['summary', 'rootCause', 'cities', 'timeBucket', 'models', 'network'];
      const results = await Promise.allSettled([
        fetchJsonOrThrow(`${MONITORING_API_BASE_URL}/stats/summary${cityQuery}`),
        fetchJsonOrThrow(`${MONITORING_API_BASE_URL}/stats/root-cause${cityQuery}`),
        fetchJsonOrThrow(`${MONITORING_API_BASE_URL}/stats/cities`),
        fetchJsonOrThrow(`${MONITORING_API_BASE_URL}/stats/time-bucket${cityQuery}`),
        fetchJsonOrThrow(`${MONITORING_API_BASE_URL}/stats/models${cityQuery}`),
        fetchJsonOrThrow(`${MONITORING_API_BASE_URL}/stats/network-buckets${cityQuery}`),
      ]);

      const failures = [];
      results.forEach((result, idx) => {
        if (result.status === 'rejected') {
          failures.push(`${labels[idx]}: ${result.reason?.message || 'failed'}`);
        }
      });

      const summary = results[0].status === 'fulfilled' ? results[0].value : monitoringSummary;
      const rootCause = results[1].status === 'fulfilled' ? results[1].value : monitoringRootCause;
      const cities = results[2].status === 'fulfilled' ? results[2].value : monitoringCities;
      const timeBucket =
        results[3].status === 'fulfilled' ? results[3].value : monitoringTimeBucket;
      const models = results[4].status === 'fulfilled' ? results[4].value : monitoringModels;
      const network = results[5].status === 'fulfilled' ? results[5].value : monitoringNetwork;

      setMonitoringSummary(summary || null);
      setMonitoringRootCause(Array.isArray(rootCause) ? rootCause : []);
      setMonitoringCities(Array.isArray(cities) ? cities : []);
      setMonitoringTimeBucket(Array.isArray(timeBucket) ? timeBucket : []);
      setMonitoringModels(Array.isArray(models) ? models : []);
      setMonitoringNetwork(network || { rssi: {}, latency: {} });
      setMonitoringError(failures.length ? failures.join(' | ') : '');
    } catch (error) {
      console.error('Failed to fetch monitoring data:', error);
      setMonitoringError(error.message || 'Monitoring backend unreachable');
    } finally {
      setMonitoringLoading(false);
    }
  };

  const fetchAllData = async () => {
    await Promise.allSettled([fetchOperationsData(), fetchMonitoringData()]);
    setLastUpdate(new Date());
  };

  useEffect(() => {
    fetchAllData();
    const interval = setInterval(fetchAllData, REFRESH_INTERVAL);
    return () => clearInterval(interval);
  }, [monitoringCity]);

  const getStatusColor = (status) => {
    const colors = {
      completed: 'bg-green-100 text-green-800 border-green-200',
      downloading: 'bg-blue-100 text-blue-800 border-blue-200',
      verifying: 'bg-yellow-100 text-yellow-800 border-yellow-200',
      installing: 'bg-purple-100 text-purple-800 border-purple-200',
      failed: 'bg-red-100 text-red-800 border-red-200',
      idle: 'bg-gray-100 text-gray-800 border-gray-200',
    };
    return colors[status] || colors.idle;
  };

  const parseServerDate = (dateString) => {
    if (!dateString) return null;
    const raw = String(dateString).trim();
    if (!raw) return null;
    const hasZone = /[zZ]|[+\-]\d{2}:\d{2}$/.test(raw);
    const normalized = hasZone ? raw : `${raw}Z`;
    const parsed = new Date(normalized);
    if (Number.isNaN(parsed.getTime())) return null;
    return parsed;
  };

  const isVehicleOnline = (lastSeen) => {
    if (!lastSeen) return false;
    const serverWindowSec = Number(serverHealth?.vehicle_online_window_sec || 0);
    const onlineWindowSec = serverWindowSec > 0 ? serverWindowSec : DEFAULT_ONLINE_WINDOW_SEC;
    const seenDate = parseServerDate(lastSeen);
    if (!seenDate) return false;
    const ageSec = (Date.now() - seenDate.getTime()) / 1000;
    return ageSec <= onlineWindowSec;
  };

  const getStatusIcon = (status) => {
    const icons = {
      completed: <CheckCircle className="w-4 h-4" />,
      downloading: <Download className="w-4 h-4" />,
      verifying: <AlertCircle className="w-4 h-4" />,
      installing: <RefreshCw className="w-4 h-4 animate-spin" />,
      failed: <XCircle className="w-4 h-4" />,
      idle: <Clock className="w-4 h-4" />,
    };
    return icons[status] || icons.idle;
  };

  const triggerUpdate = async (vehicleId, version = null, force = false) => {
    try {
      const payload = { vehicle_id: vehicleId };
      if (version) payload.version = version;
      if (force) payload.force = true;

      const response = await fetch(`${API_BASE_URL}/api/v1/admin/trigger-update`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
      });

      let body = {};
      try {
        body = await response.json();
      } catch (_) {
        body = {};
      }

      if (response.ok) {
        const topic = body.cmd_topic ? `\nTopic: ${body.cmd_topic}` : '';
        alert(`Update command sent: ${vehicleId}${topic}`);
        fetchOperationsData();
      } else {
        const genericError = String(body.error || '').trim();
        const detailError = String(body.detail || '').trim();
        const detail =
          (genericError === 'Failed to send update command' && detailError) ||
          genericError ||
          detailError ||
          `HTTP ${response.status}`;
        const detailLower = String(detail).toLowerCase();
        if (!force && detailLower.includes('offline or stale')) {
          const ok = window.confirm(
            `Vehicle appears offline or stale.\nRetry with force mode?\n\nvehicle: ${vehicleId}`
          );
          if (ok) {
            await triggerUpdate(vehicleId, version, true);
            return;
          }
        }
        if (!force && detailLower.includes('already up to date')) {
          const current = body.current_version || '-';
          const target = body.target_version || version || '-';
          const ok = window.confirm(
            `Target version is same as or older than current.\n` +
              `current: ${current}\n` +
              `target: ${target}\n\n` +
              'Run forced update (reinstall/rollback)?'
          );
          if (ok) {
            await triggerUpdate(vehicleId, version, true);
            return;
          }
        }
        alert(`Failed to send update command: ${detail}`);
      }
    } catch (error) {
      alert(`Error: ${error.message}`);
    }
  };

  const formatTime = (dateString) => {
    if (!dateString) return '-';
    const date = parseServerDate(dateString);
    if (!date) return '-';
    return date.toLocaleString('en-US');
  };

  const getRelativeTime = (dateString) => {
    if (!dateString) return '-';
    const date = parseServerDate(dateString);
    if (!date) return '-';
    const seconds = Math.floor((new Date() - date) / 1000);

    if (seconds < 60) return `${seconds}s ago`;
    if (seconds < 3600) return `${Math.floor(seconds / 60)}m ago`;
    if (seconds < 86400) return `${Math.floor(seconds / 3600)}h ago`;
    return `${Math.floor(seconds / 86400)}d ago`;
  };

  const selectedFirmware = firmware.find((f) => f.id === selectedFirmwareId) || null;

  const uploadFirmware = async () => {
    if (!uploadFile) {
      alert('Select a firmware file (.raucb) to upload.');
      return;
    }
    if (!uploadVersion.trim()) {
      alert('Enter a version (e.g. 1.2.13).');
      return;
    }

    try {
      setUploading(true);
      const form = new FormData();
      form.append('file', uploadFile);
      form.append('version', uploadVersion.trim());
      form.append('release_notes', uploadNotes || '');
      form.append('overwrite', 'true');

      const res = await fetch(`${API_BASE_URL}/api/v1/admin/firmware`, {
        method: 'POST',
        body: form,
      });
      const body = await res.json().catch(() => ({}));
      if (!res.ok) {
        throw new Error(body.error || `HTTP ${res.status}`);
      }

      alert(`Upload complete: v${body?.firmware?.version || uploadVersion.trim()}`);
      setUploadFile(null);
      setUploadVersion('');
      setUploadNotes('');
      await fetchOperationsData();
      if (body?.firmware?.id) {
        setSelectedFirmwareId(body.firmware.id);
      }
    } catch (err) {
      alert(`Upload failed: ${err.message}`);
    } finally {
      setUploading(false);
    }
  };

  const activateFirmware = async (fw) => {
    try {
      const res = await fetch(`${API_BASE_URL}/api/v1/admin/firmware/activate`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ id: fw.id }),
      });
      const body = await res.json().catch(() => ({}));
      if (!res.ok) {
        throw new Error(body.error || `HTTP ${res.status}`);
      }
      setSelectedFirmwareId(fw.id);
      await fetchOperationsData();
    } catch (err) {
      alert(`Activation failed: ${err.message}`);
    }
  };

  const deleteFirmware = async (fw) => {
    const ok = window.confirm(`Delete firmware v${fw.version}?`);
    if (!ok) return;

    try {
      const res = await fetch(`${API_BASE_URL}/api/v1/admin/firmware/${fw.id}`, {
        method: 'DELETE',
      });
      const body = await res.json().catch(() => ({}));
      if (!res.ok) {
        throw new Error(body.error || `HTTP ${res.status}`);
      }
      await fetchOperationsData();
      if (selectedFirmwareId === fw.id) {
        const next =
          firmware.find((x) => x.id !== fw.id && x.is_active) ||
          firmware.find((x) => x.id !== fw.id) ||
          null;
        setSelectedFirmwareId(next ? next.id : null);
      }
    } catch (err) {
      alert(`Delete failed: ${err.message}`);
    }
  };

  const sortedRootCause = useMemo(
    () => [...monitoringRootCause].sort((a, b) => Number(b.count || 0) - Number(a.count || 0)),
    [monitoringRootCause]
  );

  const sortedCities = useMemo(
    () => [...monitoringCities].sort((a, b) => Number(b.failures || 0) - Number(a.failures || 0)),
    [monitoringCities]
  );

  const sortedTimeBuckets = useMemo(
    () => [...monitoringTimeBucket].sort((a, b) => Number(b.failures || 0) - Number(a.failures || 0)),
    [monitoringTimeBucket]
  );

  const sortedModels = useMemo(
    () => [...monitoringModels].sort((a, b) => Number(b.count || 0) - Number(a.count || 0)),
    [monitoringModels]
  );

  const normalizeBucketData = (obj) => {
    if (!obj || typeof obj !== 'object') return [];
    return Object.entries(obj)
      .map(([bucket, count]) => ({ bucket, count: Number(count || 0) }))
      .sort((a, b) => a.bucket.localeCompare(b.bucket, undefined, { numeric: true }));
  };

  const rssiBuckets = useMemo(() => normalizeBucketData(monitoringNetwork.rssi), [monitoringNetwork]);
  const latencyBuckets = useMemo(() => normalizeBucketData(monitoringNetwork.latency), [monitoringNetwork]);

  const failureRatePct = useMemo(() => {
    const raw = Number(monitoringSummary?.failure_rate || 0);
    if (Number.isNaN(raw)) return '0.0';
    return (raw * 100).toFixed(1);
  }, [monitoringSummary]);

  const mapCities = useMemo(
    () =>
      sortedCities.filter(
        (city) =>
          city?.coords &&
          Number.isFinite(Number(city.coords.lat)) &&
          Number.isFinite(Number(city.coords.lon))
      ),
    [sortedCities]
  );

  const scopeLabel = monitoringCity ? `City: ${monitoringCity}` : 'Germany';

  const renderBarList = (rows, labelKey, valueKey, barClass) => {
    const max = rows.reduce((acc, row) => Math.max(acc, Number(row[valueKey] || 0)), 0) || 1;
    return (
      <div className="space-y-3">
        {rows.map((row) => {
          const value = Number(row[valueKey] || 0);
          const widthPct = Math.max(6, Math.round((value / max) * 100));
          return (
            <div key={`${row[labelKey]}-${value}`}>
              <div className="flex items-center justify-between text-sm text-gray-700 mb-1">
                <span className="truncate pr-2">{row[labelKey]}</span>
                <span className="font-semibold text-gray-900">{value}</span>
              </div>
              <div className="w-full h-2 bg-gray-200 rounded-full overflow-hidden">
                <div className={`h-full ${barClass}`} style={{ width: `${widthPct}%` }} />
              </div>
            </div>
          );
        })}
      </div>
    );
  };

  return (
    <div className="min-h-screen bg-gray-50">
      <div className="bg-white shadow-sm border-b border-gray-200">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-4">
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-3">
              <Server className="w-8 h-8 text-blue-600" />
              <div>
                <h1 className="text-2xl font-bold text-gray-900">OTA Dashboard</h1>
                <p className="text-sm text-gray-500">Unified Operations + Monitoring</p>
              </div>
            </div>

            <div className="flex items-center gap-4">
              <div className="flex items-center gap-2 px-3 py-2 bg-gray-50 rounded-lg">
                {serverHealth?.mqtt_connected ? (
                  <Wifi className="w-4 h-4 text-green-600" />
                ) : (
                  <WifiOff className="w-4 h-4 text-red-600" />
                )}
                <span className="text-sm font-medium">
                  {serverHealth?.mqtt_connected ? 'MQTT Connected' : 'MQTT Disconnected'}
                </span>
              </div>

              <button
                onClick={fetchAllData}
                className="flex items-center gap-2 px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-colors"
              >
                <RefreshCw className="w-4 h-4" />
                Refresh
              </button>
            </div>
          </div>

          <div className="mt-4 flex items-center gap-2">
            <button
              onClick={() => setActiveTab('operations')}
              className={`px-4 py-2 rounded-lg text-sm font-medium border transition-colors ${
                activeTab === 'operations'
                  ? 'bg-blue-600 text-white border-blue-600'
                  : 'bg-white text-gray-700 border-gray-300 hover:bg-gray-100'
              }`}
            >
              <div className="flex items-center gap-2">
                <Activity className="w-4 h-4" />
                Operations
              </div>
            </button>
            <button
              onClick={() => setActiveTab('monitoring')}
              className={`px-4 py-2 rounded-lg text-sm font-medium border transition-colors ${
                activeTab === 'monitoring'
                  ? 'bg-blue-600 text-white border-blue-600'
                  : 'bg-white text-gray-700 border-gray-300 hover:bg-gray-100'
              }`}
            >
              <div className="flex items-center gap-2">
                <BarChart3 className="w-4 h-4" />
                Monitoring
              </div>
            </button>
          </div>

          <div className="mt-2 text-xs text-gray-500">Last update: {lastUpdate.toLocaleTimeString('en-US')}</div>
        </div>
      </div>

      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-6">
        {activeTab === 'operations' &&
          (loading ? (
            <div className="flex items-center justify-center h-64">
              <RefreshCw className="w-8 h-8 text-blue-600 animate-spin" />
            </div>
          ) : (
            <div className="space-y-6">
              <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
                <div className="bg-white rounded-lg shadow p-6 border border-gray-200">
                  <div className="flex items-center justify-between">
                    <div>
                      <p className="text-sm text-gray-600">Total Vehicles</p>
                      <p className="text-3xl font-bold text-gray-900 mt-1">{vehicles.length}</p>
                    </div>
                    <Server className="w-12 h-12 text-blue-600 opacity-20" />
                  </div>
                </div>

                <div className="bg-white rounded-lg shadow p-6 border border-gray-200">
                  <div className="flex items-center justify-between">
                    <div>
                      <p className="text-sm text-gray-600">Firmware</p>
                      <p className="text-3xl font-bold text-gray-900 mt-1">{firmware.length}</p>
                    </div>
                    <Download className="w-12 h-12 text-green-600 opacity-20" />
                  </div>
                </div>

                <div className="bg-white rounded-lg shadow p-6 border border-gray-200">
                  <div className="flex items-center justify-between">
                    <div>
                      <p className="text-sm text-gray-600">Updating</p>
                      <p className="text-3xl font-bold text-gray-900 mt-1">
                        {vehicles.filter((v) => ['downloading', 'verifying', 'installing'].includes(v.status)).length}
                      </p>
                    </div>
                    <RefreshCw className="w-12 h-12 text-purple-600 opacity-20" />
                  </div>
                </div>
              </div>

              <div className="bg-white rounded-lg shadow border border-gray-200">
                <div className="px-6 py-4 border-b border-gray-200">
                  <h2 className="text-lg font-semibold text-gray-900">Firmware Management</h2>
                  <p className="text-xs text-gray-500 mt-1">
                    Select one firmware version and run vehicle updates with it.
                  </p>
                </div>
                <div className="px-6 py-4 border-b border-gray-100 bg-gray-50">
                  <div className="grid grid-cols-1 md:grid-cols-4 gap-2">
                    <input
                      type="file"
                      accept=".raucb,.tar.gz,.zip"
                      onChange={(e) => setUploadFile((e.target.files && e.target.files[0]) || null)}
                      className="text-sm"
                    />
                    <input
                      type="text"
                      value={uploadVersion}
                      onChange={(e) => setUploadVersion(e.target.value)}
                      placeholder="version (e.g. 1.2.13)"
                      className="px-3 py-2 text-sm border border-gray-300 rounded"
                    />
                    <input
                      type="text"
                      value={uploadNotes}
                      onChange={(e) => setUploadNotes(e.target.value)}
                      placeholder="release notes"
                      className="px-3 py-2 text-sm border border-gray-300 rounded"
                    />
                    <button
                      onClick={uploadFirmware}
                      disabled={uploading}
                      className={`px-3 py-2 text-sm font-medium rounded ${
                        uploading ? 'bg-gray-300 text-gray-700' : 'bg-blue-600 text-white hover:bg-blue-700'
                      }`}
                    >
                      {uploading ? 'Uploading...' : 'Upload + Activate'}
                    </button>
                  </div>
                </div>
                <div className="divide-y divide-gray-200">
                  {firmware.length === 0 ? (
                    <div className="px-6 py-8 text-center text-gray-500">No firmware uploaded</div>
                  ) : (
                    firmware.map((fw) => (
                      <div key={fw.id} className="px-6 py-4 hover:bg-gray-50">
                        <div className="flex items-center justify-between">
                          <div className="flex-1">
                            <div className="flex items-center gap-3">
                              <input
                                type="checkbox"
                                checked={selectedFirmwareId === fw.id}
                                onChange={() => setSelectedFirmwareId(fw.id)}
                                title="Select target firmware version"
                              />
                              <span className="text-lg font-semibold text-gray-900">v{fw.version}</span>
                              <span
                                className={`px-2 py-1 text-xs font-medium rounded ${
                                  fw.is_active ? 'bg-green-100 text-green-800' : 'bg-gray-100 text-gray-700'
                                }`}
                              >
                                {fw.is_active ? 'Active' : 'Inactive'}
                              </span>
                            </div>
                            <p className="text-sm text-gray-600 mt-1">{fw.filename}</p>
                            <p className="text-xs text-gray-500 mt-1">
                              Size: {(fw.file_size / 1024).toFixed(1)} KB | SHA256: {fw.sha256.substring(0, 16)}...
                            </p>
                            {fw.release_notes && <p className="text-sm text-gray-700 mt-2">{fw.release_notes}</p>}
                          </div>
                          <div className="ml-4 flex items-center gap-2">
                            {!fw.is_active && (
                              <button
                                onClick={() => activateFirmware(fw)}
                                className="px-3 py-2 text-xs font-medium rounded bg-emerald-600 text-white hover:bg-emerald-700"
                              >
                                Activate
                              </button>
                            )}
                            <button
                              onClick={() => deleteFirmware(fw)}
                              className="px-3 py-2 text-xs font-medium rounded bg-red-600 text-white hover:bg-red-700"
                            >
                              Delete
                            </button>
                            <div className="text-xs text-gray-500">{formatTime(fw.created_at)}</div>
                          </div>
                        </div>
                      </div>
                    ))
                  )}
                </div>
              </div>

              <div className="bg-white rounded-lg shadow border border-gray-200">
                <div className="px-6 py-4 border-b border-gray-200">
                  <h2 className="text-lg font-semibold text-gray-900">Vehicles</h2>
                </div>
                <div className="divide-y divide-gray-200">
                  {vehicles.length === 0 ? (
                    <div className="px-6 py-8 text-center text-gray-500">No vehicles registered</div>
                  ) : (
                    vehicles.map((vehicle) => {
                      const online =
                        typeof vehicle.online === 'boolean'
                          ? vehicle.online
                          : isVehicleOnline(vehicle.last_seen);
                      return (
                        <div key={vehicle.id} className="px-6 py-4 hover:bg-gray-50">
                          <div className="flex items-start justify-between">
                            <div className="flex-1">
                              <div className="flex items-center gap-3">
                                <h3 className="text-lg font-semibold text-gray-900">{vehicle.vehicle_id}</h3>
                                <span
                                  className={`flex items-center gap-1 px-2 py-1 text-xs font-medium rounded border ${getStatusColor(
                                    vehicle.status
                                  )}`}
                                >
                                  {getStatusIcon(vehicle.status)}
                                  {vehicle.status}
                                </span>
                                <span
                                  className={`px-2 py-1 text-xs font-medium rounded border ${
                                    online
                                      ? 'bg-green-50 text-green-700 border-green-200'
                                      : 'bg-gray-100 text-gray-600 border-gray-300'
                                  }`}
                                >
                                  {online ? 'online' : 'offline'}
                                </span>
                              </div>

                              <div className="mt-2 grid grid-cols-2 md:grid-cols-5 gap-4 text-sm">
                                <div>
                                  <p className="text-gray-500">Current Version</p>
                                  <p className="font-medium text-gray-900">{vehicle.current_version || '-'}</p>
                                </div>
                                <div>
                                  <p className="text-gray-500">Last IP</p>
                                  <p className="font-medium text-gray-900">{vehicle.last_ip || '-'}</p>
                                </div>
                                <div>
                                  <p className="text-gray-500">Last Seen</p>
                                  <p className="font-medium text-gray-900">{getRelativeTime(vehicle.last_seen)}</p>
                                </div>
                                <div>
                                  <p className="text-gray-500">Registered</p>
                                  <p className="font-medium text-gray-900">{formatTime(vehicle.created_at).split(' ')[0]}</p>
                                </div>
                                <div>
                                  <p className="text-gray-500">Updated</p>
                                  <p className="font-medium text-gray-900">{formatTime(vehicle.updated_at).split(' ')[1]}</p>
                                </div>
                              </div>

                              {vehicle.recent_updates && vehicle.recent_updates.length > 0 && (
                                <div className="mt-3 p-3 bg-gray-50 rounded border border-gray-200">
                                  <p className="text-xs font-medium text-gray-700 mb-2">Recent Updates</p>
                                  <div className="space-y-1">
                                    {vehicle.recent_updates.slice(0, 3).map((update, idx) => (
                                      <div key={idx} className="text-xs text-gray-600 flex items-center gap-2">
                                        <span
                                          className={`w-2 h-2 rounded-full ${
                                            update.status === 'completed'
                                              ? 'bg-green-500'
                                              : update.status === 'failed'
                                              ? 'bg-red-500'
                                              : 'bg-yellow-500'
                                          }`}
                                        />
                                        <span>
                                          {update.from_version || '?'} {'->'} {update.target_version}
                                        </span>
                                        <span className="text-gray-400">({update.status})</span>
                                        <span className="text-gray-400 ml-auto">{getRelativeTime(update.started_at)}</span>
                                      </div>
                                    ))}
                                  </div>
                                </div>
                              )}
                            </div>

                            <div className="ml-4">
                              <button
                                onClick={() => triggerUpdate(vehicle.vehicle_id, selectedFirmware?.version || null)}
                                className={`px-4 py-2 text-sm font-medium rounded-lg transition-colors ${
                                  online
                                    ? 'bg-blue-600 text-white hover:bg-blue-700'
                                    : 'bg-amber-500 text-white hover:bg-amber-600'
                                }`}
                                title={
                                  selectedFirmware
                                    ? `Update with selected version v${selectedFirmware.version}`
                                    : 'Update with current active version'
                                }
                              >
                                Update {selectedFirmware ? `(v${selectedFirmware.version})` : '(Active)'}
                              </button>
                            </div>
                          </div>
                        </div>
                      );
                    })
                  )}
                </div>
              </div>
            </div>
          ))}

        {activeTab === 'monitoring' &&
          (monitoringLoading ? (
            <div className="flex items-center justify-center h-64">
              <RefreshCw className="w-8 h-8 text-blue-600 animate-spin" />
            </div>
          ) : (
            <div className="space-y-6">
              {monitoringError && (
                <div className="bg-amber-50 border border-amber-200 text-amber-800 rounded-lg p-4 text-sm">
                  Monitoring backend warning: {monitoringError}
                </div>
              )}

              <div className="bg-white rounded-lg shadow border border-gray-200 p-6">
                <div className="flex flex-wrap items-center justify-between gap-3 mb-4">
                  <div>
                    <h2 className="text-lg font-semibold text-gray-900">Germany Failure Map</h2>
                    <p className="text-xs text-gray-500 mt-1">Click a city marker to filter monitoring scope.</p>
                  </div>
                  <div className="flex items-center gap-2">
                    <span className="text-xs text-gray-500">Scope</span>
                    <span className="px-2 py-1 rounded border text-xs font-medium text-gray-700 bg-gray-50 border-gray-200">
                      {scopeLabel}
                    </span>
                    <button
                      onClick={() => setMonitoringCity('')}
                      disabled={!monitoringCity}
                      className={`px-3 py-1.5 rounded text-xs font-medium border ${
                        monitoringCity
                          ? 'bg-blue-600 text-white border-blue-600 hover:bg-blue-700'
                          : 'bg-gray-100 text-gray-400 border-gray-200 cursor-not-allowed'
                      }`}
                    >
                      Reset Filter
                    </button>
                  </div>
                </div>

                {mapCities.length === 0 ? (
                  <p className="text-sm text-gray-500">No city coordinate data available.</p>
                ) : (
                  <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
                    <div className="lg:col-span-2">
                      <div className="h-72 rounded-lg border border-slate-300 overflow-hidden">
                        <MapContainer
                          center={[51.1657, 10.4515]}
                          zoom={6}
                          scrollWheelZoom={true}
                          className="h-full w-full"
                        >
                          <TileLayer
                            attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors'
                            url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
                          />
                          {mapCities.map((city) => {
                            const lat = Number(city.coords?.lat);
                            const lon = Number(city.coords?.lon);
                            if (!Number.isFinite(lat) || !Number.isFinite(lon)) return null;
                            const rate = Number(city.failure_rate || 0);
                            const radius = 7 + Math.round(Math.min(10, rate * 20));
                            const active = monitoringCity === city.city;
                            return (
                              <CircleMarker
                                key={`marker-${city.city}`}
                                center={[lat, lon]}
                                radius={radius}
                                pathOptions={{
                                  color: active ? '#ffffff' : '#fde68a',
                                  weight: active ? 3 : 2,
                                  fillColor: active ? '#ef4444' : '#f59e0b',
                                  fillOpacity: 0.85,
                                }}
                                eventHandlers={{
                                  click: () =>
                                    setMonitoringCity((prev) => (prev === city.city ? '' : city.city)),
                                }}
                              >
                                <LeafletTooltip direction="top" offset={[0, -8]} opacity={0.95}>
                                  <div className="text-xs">
                                    <div className="font-semibold">{city.city}</div>
                                    <div>
                                      {city.failures}/{city.total} ({(rate * 100).toFixed(1)}%)
                                    </div>
                                  </div>
                                </LeafletTooltip>
                              </CircleMarker>
                            );
                          })}
                        </MapContainer>
                      </div>
                      <p className="text-[11px] text-gray-500 mt-2">
                        Map data: OpenStreetMap. Click marker to filter.
                      </p>
                    </div>
                    <div className="bg-gray-50 rounded-lg border border-gray-200 p-3 overflow-auto max-h-72">
                      <div className="text-xs font-semibold text-gray-700 mb-2">Top Cities</div>
                      <div className="space-y-1">
                        {sortedCities.slice(0, 12).map((city) => {
                          const active = monitoringCity === city.city;
                          return (
                            <button
                              type="button"
                              key={`scope-${city.city}`}
                              onClick={() =>
                                setMonitoringCity((prev) => (prev === city.city ? '' : city.city))
                              }
                              className={`w-full text-left px-2 py-1.5 rounded text-xs border ${
                                active
                                  ? 'bg-blue-600 text-white border-blue-600'
                                  : 'bg-white text-gray-700 border-gray-200 hover:bg-blue-50'
                              }`}
                            >
                              <div className="flex items-center justify-between gap-2">
                                <span className="truncate">{city.city}</span>
                                <span className="font-semibold">
                                  {city.failures}/{city.total}
                                </span>
                              </div>
                            </button>
                          );
                        })}
                      </div>
                    </div>
                  </div>
                )}
              </div>

              <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
                <div className="bg-white rounded-lg shadow p-6 border border-gray-200">
                  <p className="text-sm text-gray-600">Total Logs</p>
                  <p className="text-3xl font-bold text-gray-900 mt-1">{monitoringSummary?.total_records ?? 0}</p>
                </div>
                <div className="bg-white rounded-lg shadow p-6 border border-gray-200">
                  <p className="text-sm text-gray-600">Failure Logs</p>
                  <p className="text-3xl font-bold text-red-600 mt-1">{monitoringSummary?.failure_records ?? 0}</p>
                </div>
                <div className="bg-white rounded-lg shadow p-6 border border-gray-200">
                  <p className="text-sm text-gray-600">Failure Rate</p>
                  <p className="text-3xl font-bold text-gray-900 mt-1">{failureRatePct}%</p>
                </div>
                <div className="bg-white rounded-lg shadow p-6 border border-gray-200">
                  <p className="text-sm text-gray-600">Tracked Cities</p>
                  <p className="text-3xl font-bold text-gray-900 mt-1">{monitoringCities.length}</p>
                </div>
              </div>

              <div className="grid grid-cols-1 sm:grid-cols-2 xl:grid-cols-4 gap-4">
                <div className="bg-white rounded-lg shadow border border-gray-200 p-4">
                  <div className="flex items-center gap-2 mb-3">
                    <AlertCircle className="w-4 h-4 text-blue-600" />
                    <h2 className="text-sm font-semibold text-gray-900">Root Cause</h2>
                  </div>
                  {sortedRootCause.length === 0 ? (
                    <p className="text-sm text-gray-500">No data</p>
                  ) : (
                    renderBarList(sortedRootCause.slice(0, 5), 'root_cause', 'count', 'bg-blue-500')
                  )}
                </div>

                <div className="bg-white rounded-lg shadow border border-gray-200 p-4">
                  <div className="flex items-center gap-2 mb-3">
                    <Clock className="w-4 h-4 text-blue-600" />
                    <h2 className="text-sm font-semibold text-gray-900">Time Bucket</h2>
                  </div>
                  {sortedTimeBuckets.length === 0 ? (
                    <p className="text-sm text-gray-500">No data</p>
                  ) : (
                    renderBarList(sortedTimeBuckets.slice(0, 5), 'time_bucket', 'failures', 'bg-indigo-500')
                  )}
                </div>

                <div className="bg-white rounded-lg shadow border border-gray-200 p-4">
                  <div className="flex items-center gap-2 mb-3">
                    <Server className="w-4 h-4 text-blue-600" />
                    <h2 className="text-sm font-semibold text-gray-900">Vehicle Series</h2>
                  </div>
                  {sortedModels.length === 0 ? (
                    <p className="text-sm text-gray-500">No data</p>
                  ) : (
                    renderBarList(sortedModels.slice(0, 5), 'series', 'count', 'bg-cyan-500')
                  )}
                </div>

                <div className="bg-white rounded-lg shadow border border-gray-200 p-4">
                  <div className="flex items-center gap-2 mb-3">
                    <Wifi className="w-4 h-4 text-blue-600" />
                    <h2 className="text-sm font-semibold text-gray-900">Network</h2>
                  </div>
                  <div className="space-y-4">
                    <div>
                      <p className="text-xs font-medium text-gray-600 mb-2">RSSI</p>
                      {rssiBuckets.length === 0 ? (
                        <p className="text-sm text-gray-500">No data</p>
                      ) : (
                        renderBarList(rssiBuckets.slice(0, 4), 'bucket', 'count', 'bg-emerald-500')
                      )}
                    </div>
                    <div>
                      <p className="text-xs font-medium text-gray-600 mb-2">Latency</p>
                      {latencyBuckets.length === 0 ? (
                        <p className="text-sm text-gray-500">No data</p>
                      ) : (
                        renderBarList(latencyBuckets.slice(0, 4), 'bucket', 'count', 'bg-purple-500')
                      )}
                    </div>
                  </div>
                </div>
              </div>
            </div>
          ))}
      </div>
    </div>
  );
};

export default OTADashboard;
