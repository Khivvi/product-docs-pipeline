CREATE TABLE IF NOT EXISTS pipeline_metrics (
  metric_id BIGSERIAL PRIMARY KEY,
  pipeline_name TEXT NOT NULL,
  run_started_at TIMESTAMP WITHOUT TIME ZONE NOT NULL,
  run_finished_at TIMESTAMP WITHOUT TIME ZONE NOT NULL,
  processed_count INTEGER NOT NULL,
  ok200_count INTEGER NOT NULL,
  ok304_count INTEGER NOT NULL,
  error_count INTEGER NOT NULL,
  error_rate NUMERIC(7,4) NOT NULL,
  run_duration_ms BIGINT NOT NULL,
  created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_pipeline_metrics_pipeline_time
  ON pipeline_metrics (pipeline_name, run_finished_at DESC);

CREATE TABLE IF NOT EXISTS alerts (
  alert_id BIGSERIAL PRIMARY KEY,
  metric_id BIGINT REFERENCES pipeline_metrics(metric_id) ON DELETE SET NULL,
  pipeline_name TEXT NOT NULL,
  alert_type TEXT NOT NULL,
  severity TEXT NOT NULL,
  message TEXT NOT NULL,
  details JSONB,
  triggered_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_alerts_pipeline_time
  ON alerts (pipeline_name, triggered_at DESC);
