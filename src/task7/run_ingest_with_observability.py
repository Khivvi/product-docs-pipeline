from datetime import datetime, timedelta
from pathlib import Path
import sys
import time

from dotenv import load_dotenv
from psycopg2.extras import DictCursor, Json

SRC_ROOT = Path(__file__).resolve().parents[1]
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from pipeline.db import connect_db
from pipeline.ingest import run_content_ingest

PIPELINE_NAME = "content_ingest"
BASELINE_RUNS = 10
STALE_HOURS = 24


def _read_local_file(filename: str) -> str:
    return (Path(__file__).resolve().parent / filename).read_text(encoding="utf-8")


def ensure_schema(cur):
    cur.execute(_read_local_file("task7_create_tables.sql"))


def load_baseline(cur, pipeline_name: str) -> dict:
    cur.execute(
        """
        SELECT
          COALESCE(AVG(error_rate), 0) AS avg_error_rate,
          COALESCE(AVG(run_duration_ms), 0) AS avg_duration_ms,
          COUNT(*) AS sample_count
        FROM (
          SELECT error_rate, run_duration_ms
          FROM pipeline_metrics
          WHERE pipeline_name = %s
          ORDER BY run_finished_at DESC
          LIMIT %s
        ) t;
        """,
        (pipeline_name, BASELINE_RUNS),
    )
    row = cur.fetchone()
    return {
        "avg_error_rate": float(row["avg_error_rate"] or 0),
        "avg_duration_ms": float(row["avg_duration_ms"] or 0),
        "sample_count": int(row["sample_count"] or 0),
    }


def insert_metric(cur, payload: dict) -> int:
    cur.execute(
        """
        INSERT INTO pipeline_metrics
          (pipeline_name, run_started_at, run_finished_at, processed_count,
           ok200_count, ok304_count, error_count, error_rate, run_duration_ms)
        VALUES
          (%(pipeline_name)s, %(run_started_at)s, %(run_finished_at)s, %(processed_count)s,
           %(ok200_count)s, %(ok304_count)s, %(error_count)s, %(error_rate)s, %(run_duration_ms)s)
        RETURNING metric_id;
        """,
        payload,
    )
    return cur.fetchone()["metric_id"]


def insert_alert(
    cur,
    *,
    metric_id: int,
    pipeline_name: str,
    alert_type: str,
    severity: str,
    message: str,
    details: dict,
):
    cur.execute(
        """
        INSERT INTO alerts
          (metric_id, pipeline_name, alert_type, severity, message, details)
        VALUES (%s, %s, %s, %s, %s, %s);
        """,
        (metric_id, pipeline_name, alert_type, severity, message, Json(details)),
    )


def latest_pipeline_check(cur):
    cur.execute(
        """
        SELECT MAX(last_checked_at) AS latest_checked_at
        FROM candidate_rk_document_content;
        """
    )
    row = cur.fetchone()
    return row["latest_checked_at"]


def evaluate_and_store_alerts(
    cur,
    *,
    metric_id: int,
    pipeline_name: str,
    run_started_at: datetime,
    run_finished_at: datetime,
    stats: dict,
    run_duration_ms: int,
    baseline: dict,
) -> list[dict]:
    alerts = []
    processed = int(stats.get("processed", 0))
    errors = int(stats.get("err", 0))
    error_rate = (errors / processed) if processed else 0.0

    if processed == 0:
        alerts.append(
            {
                "alert_type": "empty_result_set",
                "severity": "warning",
                "message": "Pipeline run processed 0 URLs.",
                "details": {"processed_count": processed},
            }
        )

    if processed > 0 and error_rate >= 0.30:
        alerts.append(
            {
                "alert_type": "anomalous_failure_rate",
                "severity": "critical",
                "message": f"Failure rate is high ({error_rate:.2%}).",
                "details": {"error_rate": error_rate, "errors": errors, "processed": processed},
            }
        )
    elif processed >= 20 and error_rate >= 0.15:
        alerts.append(
            {
                "alert_type": "anomalous_failure_rate",
                "severity": "warning",
                "message": f"Failure rate increased ({error_rate:.2%}).",
                "details": {"error_rate": error_rate, "errors": errors, "processed": processed},
            }
        )

    avg_error_rate = baseline.get("avg_error_rate", 0.0)
    if baseline.get("sample_count", 0) >= 3 and avg_error_rate > 0:
        if error_rate > avg_error_rate * 2 and error_rate >= 0.05:
            alerts.append(
                {
                    "alert_type": "anomalous_failure_rate",
                    "severity": "warning",
                    "message": "Failure rate is more than 2x recent baseline.",
                    "details": {
                        "current_error_rate": error_rate,
                        "baseline_error_rate": avg_error_rate,
                    },
                }
            )

    avg_duration_ms = baseline.get("avg_duration_ms", 0.0)
    if baseline.get("sample_count", 0) >= 3 and avg_duration_ms > 0:
        if run_duration_ms > avg_duration_ms * 2 and (run_duration_ms - avg_duration_ms) > 5_000:
            alerts.append(
                {
                    "alert_type": "performance_degradation",
                    "severity": "warning",
                    "message": "Run duration is more than 2x recent baseline.",
                    "details": {
                        "current_duration_ms": run_duration_ms,
                        "baseline_duration_ms": avg_duration_ms,
                    },
                }
            )

    latest_checked_at = latest_pipeline_check(cur)
    stale_cutoff = run_finished_at - timedelta(hours=STALE_HOURS)
    if latest_checked_at is None or latest_checked_at < stale_cutoff:
        alerts.append(
            {
                "alert_type": "pipeline_staleness",
                "severity": "critical",
                "message": "No recent content checks in document_content table.",
                "details": {
                    "latest_checked_at": str(latest_checked_at) if latest_checked_at else None,
                    "stale_cutoff": str(stale_cutoff),
                },
            }
        )

    for a in alerts:
        insert_alert(
            cur,
            metric_id=metric_id,
            pipeline_name=pipeline_name,
            alert_type=a["alert_type"],
            severity=a["severity"],
            message=a["message"],
            details=a["details"],
        )

    return alerts


def main():
    load_dotenv()

    run_started_at = datetime.utcnow()
    t0 = time.perf_counter()
    stats = run_content_ingest()
    run_finished_at = datetime.utcnow()
    run_duration_ms = int((time.perf_counter() - t0) * 1000)

    processed = int(stats.get("processed", 0))
    errors = int(stats.get("err", 0))
    error_rate = round((errors / processed), 4) if processed else 0.0

    with connect_db() as conn, conn.cursor(cursor_factory=DictCursor) as cur:
        ensure_schema(cur)
        baseline = load_baseline(cur, PIPELINE_NAME)

        metric_id = insert_metric(
            cur,
            {
                "pipeline_name": PIPELINE_NAME,
                "run_started_at": run_started_at,
                "run_finished_at": run_finished_at,
                "processed_count": processed,
                "ok200_count": int(stats.get("ok200", 0)),
                "ok304_count": int(stats.get("ok304", 0)),
                "error_count": errors,
                "error_rate": error_rate,
                "run_duration_ms": run_duration_ms,
            },
        )

        alerts = evaluate_and_store_alerts(
            cur,
            metric_id=metric_id,
            pipeline_name=PIPELINE_NAME,
            run_started_at=run_started_at,
            run_finished_at=run_finished_at,
            stats=stats,
            run_duration_ms=run_duration_ms,
            baseline=baseline,
        )
        conn.commit()

    print(f"Run stats: {stats}")
    print(f"Run duration (ms): {run_duration_ms}")
    print(f"Metric row saved with metric_id={metric_id}")
    print(f"Alerts created: {len(alerts)}")
    for a in alerts:
        print(f"- [{a['severity']}] {a['alert_type']}: {a['message']}")


if __name__ == "__main__":
    main()
