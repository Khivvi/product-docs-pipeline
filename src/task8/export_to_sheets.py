import os
import datetime as dt
from pathlib import Path
import pandas as pd
import psycopg2
import gspread
from gspread.exceptions import WorksheetNotFound
from google.oauth2.service_account import Credentials
from dotenv import load_dotenv

load_dotenv()

spreadsheet_id = "1QOptHKFCY0WIp1JJ4FAXMBXr30MxqOjcs8G6jvzA2Cg"
TASK4_SQL_PATH = Path(__file__).resolve().parents[1] / "task4" / "task4_analytics_queries.sql"

def open_database_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
    )

def load_sql_queries(sql_file: Path) -> dict[str, str]:
    queries = {}
    current_name = None
    current_lines = []

    for raw_line in sql_file.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if line.startswith("-- name:"):
            if current_name and current_lines:
                queries[current_name] = "\n".join(current_lines).strip()
            current_name = line.split(":", 1)[1].strip()
            current_lines = []
            continue
        if current_name:
            current_lines.append(raw_line)

    if current_name and current_lines:
        queries[current_name] = "\n".join(current_lines).strip()

    if not queries:
        raise ValueError(f"No SQL queries found in {sql_file}")

    return queries

def run_query(sql_query: str) -> pd.DataFrame:
    with open_database_connection() as connection:
        return pd.read_sql(sql_query, connection)

def open_google_sheet():
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]

    credentials = Credentials.from_service_account_file(
        "service_account.json",
        scopes=scopes
    )

    client = gspread.authorize(credentials)
    return client.open_by_key(spreadsheet_id)

def make_cell_value(value):
    if value is None:
        return ""
    if isinstance(value, float) and pd.isna(value):
        return ""
    if pd.isna(value):
        return ""
    if isinstance(value, (pd.Timestamp, dt.datetime, dt.date)):
        return value.isoformat()
    return value

def to_sheet_values(dataframe: pd.DataFrame):
    cleaned = dataframe.where(pd.notnull(dataframe), None)
    header = [str(col) for col in cleaned.columns.tolist()]
    rows = [[make_cell_value(v) for v in row] for row in cleaned.to_numpy().tolist()]
    return [header] + rows

def write_dataframe(sheet, tab_name: str, dataframe: pd.DataFrame):
    try:
        worksheet = sheet.worksheet(tab_name)
        worksheet.clear()
    except WorksheetNotFound:
        worksheet = sheet.add_worksheet(title=tab_name, rows="100", cols="20")

    values = to_sheet_values(dataframe)

    row_count = max(len(values), 2)
    col_count = max(len(values[0]) if values else 1, 1)

    worksheet.resize(rows=row_count, cols=col_count)
    worksheet.update(values)

def main():
    sql_queries = load_sql_queries(TASK4_SQL_PATH)
    sheet = open_google_sheet()

    for tab_name, sql_query in sql_queries.items():
        dataframe = run_query(sql_query)
        write_dataframe(sheet, tab_name, dataframe)

if __name__ == "__main__":
    main()
