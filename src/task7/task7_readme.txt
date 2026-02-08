Task 7: Observability and Alerting

For this task, I tried to think like an operator and ask simple but useful questions: Did this run actually do work? Are failures suddenly rising? Is the pipeline getting slower than usual? Has the pipeline stopped being fresh? Based on those questions, I added two tables: `pipeline_metrics` for one row per run, and `alerts` for one row per triggered issue.

I implemented Task 7 as a wrapper around the existing content ingestion flow (Task 3), because that is where most runtime risk exists: network calls, retries, timeouts, and variable run duration. The script creates tables if needed, runs the ingest pipeline, stores run metrics, then evaluates alert rules and stores any alerts.

The alert rules are intentionally simple and practical. I used failure-rate thresholds (15% warning, 30% critical), a baseline comparison rule (more than 2x recent average), a staleness rule (no recent checks in 24 hours), an empty-result rule (processed = 0), and a performance rule (duration > 2x baseline and at least 5 seconds slower). These are not perfect, but they are easy to explain and good enough to catch real operational issues early.

How to run:
`python src\task7\run_ingest_with_observability.py`
