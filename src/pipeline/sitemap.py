from pathlib import Path


def _task2_sql() -> str:
    sql_path = (
        Path(__file__).resolve().parents[1] / "task2" / "task2_data_consolidation.sql"
    )
    return sql_path.read_text(encoding="utf-8")


def consolidate_docs_master(cur):
    cur.execute(_task2_sql())
