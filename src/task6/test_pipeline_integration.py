from unittest.mock import MagicMock, patch
from pipeline.ingest import run_content_ingest


@patch("pipeline.ingest.requests.Session")
@patch("pipeline.ingest.connect_db")
def test_pipeline_runs_and_upserts(connect_db_mock, session_mock):
    conn = MagicMock()
    cur = MagicMock()
    conn.__enter__.return_value = conn
    conn.cursor.return_value.__enter__.return_value = cur
    connect_db_mock.return_value = conn

    cur.fetchall.side_effect = [
        [{"url": "https://example.com/a", "etag": None, "last_modified": None}],
        [],
    ]

    s = MagicMock()
    session_mock.return_value.__enter__.return_value = s

    r = MagicMock()
    r.status_code = 200
    r.headers = {"Content-Type": "text/html"}
    r.iter_content.return_value = [b"hello"]
    r.encoding = "utf-8"
    s.get.return_value = r

    stats = run_content_ingest(batch_size=1, host_delay=0.0)

    assert stats["processed"] == 1
    assert stats["ok200"] == 1
    assert stats["err"] == 0
    assert cur.execute.call_count >= 1


@patch("pipeline.ingest.requests.Session")
@patch("pipeline.ingest.connect_db")
def test_idempotency_row_counts_stable(connect_db_mock, session_mock):
    conn = MagicMock()
    cur = MagicMock()
    conn.__enter__.return_value = conn
    conn.cursor.return_value.__enter__.return_value = cur
    connect_db_mock.return_value = conn

    cur.fetchall.return_value = []

    s = MagicMock()
    session_mock.return_value.__enter__.return_value = s

    stats1 = run_content_ingest()
    stats2 = run_content_ingest()

    assert stats1["processed"] == 0
    assert stats2["processed"] == 0
