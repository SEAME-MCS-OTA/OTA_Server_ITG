CREATE DATABASE IF NOT EXISTS ota_dashboard;
USE ota_dashboard;

CREATE TABLE IF NOT EXISTS ota_logs (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  ts VARCHAR(40) NULL,
  device_id VARCHAR(64) NULL,
  ota_id VARCHAR(64) NULL,
  current_version VARCHAR(32) NULL,
  target_version VARCHAR(32) NULL,
  ota_phase VARCHAR(32) NULL,
  vehicle_brand VARCHAR(64) NULL,
  vehicle_series VARCHAR(64) NULL,
  vehicle_segment VARCHAR(32) NULL,
  vehicle_fuel VARCHAR(16) NULL,
  error_code VARCHAR(64) NULL,
  error_message VARCHAR(255) NULL,
  retryable TINYINT(1) NOT NULL DEFAULT 0,
  country VARCHAR(8) NULL,
  city VARCHAR(64) NULL,
  timezone VARCHAR(64) NULL,
  local_time VARCHAR(32) NULL,
  day_of_week VARCHAR(8) NULL,
  time_bucket VARCHAR(16) NULL,
  power_source VARCHAR(16) NULL,
  battery_pct INT NULL,
  rssi_dbm INT NULL,
  latency_ms INT NULL,
  vlm_root_cause VARCHAR(64) NULL,
  vlm_confidence FLOAT NULL,
  supporting_evidence JSON NULL,
  tags JSON NULL,
  is_failure TINYINT(1) NOT NULL DEFAULT 0,
  raw_json JSON NOT NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_city ON ota_logs (city);
CREATE INDEX idx_root_cause ON ota_logs (vlm_root_cause);
CREATE INDEX idx_failure ON ota_logs (is_failure);
CREATE INDEX idx_time_bucket ON ota_logs (time_bucket);
CREATE INDEX idx_vehicle_series ON ota_logs (vehicle_series);
