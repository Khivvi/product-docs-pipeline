from unittest.mock import MagicMock

from pipeline.sitemap import consolidate_docs_master


def test_task2_consolidation_executes_sql():
    cur = MagicMock()
    consolidate_docs_master(cur)
    assert cur.execute.call_count == 1

    sql = cur.execute.call_args[0][0]
    assert "INSERT INTO candidate_rk_docs_master" in sql
    assert "ON CONFLICT (url)" in sql
    assert "UNNEST(candidate_rk_docs_master.sources || EXCLUDED.sources)" in sql
