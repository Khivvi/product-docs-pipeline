from pathlib import Path
import sys

from dotenv import load_dotenv

SRC_ROOT = Path(__file__).resolve().parents[1]
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from pipeline.db import connect_db

load_dotenv()


def run_task2_consolidation():
    sql_path = Path(__file__).with_name("task2_data_consolidation.sql")
    sql = sql_path.read_text(encoding="utf-8")

    with connect_db() as conn, conn.cursor() as cur:
        cur.execute(sql)
        conn.commit()


if __name__ == "__main__":
    run_task2_consolidation()
    print("Task 2 consolidation completed.")
