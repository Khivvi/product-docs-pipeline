import os
import datetime as dt
import pandas as pd
import psycopg2
import gspread
from gspread.exceptions import WorksheetNotFound
from google.oauth2.service_account import Credentials
from dotenv import load_dotenv

load_dotenv()

spreadsheet_id = "1QOptHKFCY0WIp1JJ4FAXMBXr30MxqOjcs8G6jvzA2Cg"

def open_database_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
    )

sql_queries = {
    "source_counts": """SELECT
        unnest(sources) AS source,
        COUNT(*) AS doc_count
    FROM candidate_rk_docs_master
    GROUP BY source
    ORDER BY doc_count DESC, source;
    """,

    "monthly_distribution": """SELECT
        DATE_TRUNC('month', first_seen_at) AS month,
        COUNT(*) AS doc_count
    FROM candidate_rk_docs_master
    WHERE first_seen_at >= CURRENT_DATE - INTERVAL '12 months'
    GROUP BY month
    ORDER BY month;
    """,

    "success_rate": """SELECT
        src.source,
        COUNT(*) FILTER (WHERE dc.status_code = 200) * 1.0 / NULLIF(COUNT(*), 0) AS success_rate
    FROM candidate_rk_docs_master dm
    CROSS JOIN LATERAL unnest(dm.sources) AS src(source)
    LEFT JOIN candidate_rk_document_content dc
        ON dm.url = dc.url
    GROUP BY src.source
    ORDER BY success_rate DESC, src.source;
    """,

    "top_paths": """SELECT
        split_part(
            regexp_replace(url, '^https?://[^/]+/(en|de|fr|ja|ko|pt)/', ''),
            '/',
            1
        ) AS path_segment,
        COUNT(*) AS freq
    FROM candidate_rk_docs_master
    GROUP BY path_segment
    ORDER BY freq DESC, path_segment
    LIMIT 10;
    """,

    "stale_docs": """SELECT
        stale.stale_count,
        CASE
            WHEN total.total_count = 0 THEN 0
            ELSE stale.stale_count * 100.0 / total.total_count
        END AS stale_percentage
    FROM
        (SELECT COUNT(*) AS stale_count
         FROM candidate_rk_docs_master
         WHERE first_seen_at < CURRENT_DATE - INTERVAL '180 days') stale
    CROSS JOIN
        (SELECT COUNT(*) AS total_count
         FROM candidate_rk_docs_master) total;
    """
}

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
    sheet = open_google_sheet()

    for tab_name, sql_query in sql_queries.items():
        dataframe = run_query(sql_query)
        write_dataframe(sheet, tab_name, dataframe)

if __name__ == "__main__":
    main()
