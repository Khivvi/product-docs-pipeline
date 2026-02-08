import os

import psycopg2


def connect_db():
    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=int(os.getenv("DB_PORT", "5432")),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
    )


def pick_batch(cur, batch_size: int):
    cur.execute(
        """
      SELECT dm.url, dc.etag, dc.last_modified
      FROM candidate_rk_docs_master dm
      LEFT JOIN candidate_rk_document_content dc ON dc.url = dm.url
      WHERE dc.url IS NULL
         OR dc.last_checked_at IS NULL
         OR dc.status_code IS DISTINCT FROM 200
         OR dc.last_checked_at < (NOW() - INTERVAL '1 day')
      ORDER BY COALESCE(dc.last_checked_at, TIMESTAMP '1970-01-01') ASC
      LIMIT %s;
    """,
        (batch_size,),
    )
    return cur.fetchall()


def upsert_document_content(cur, payload: dict):
    cur.execute(
        """
      INSERT INTO candidate_rk_document_content
        (url, etag, last_modified, content_hash, content, content_bytes, content_type,
         status_code, fetched_at, last_checked_at, error_message, was_truncated, is_too_large)
      VALUES
        (%(url)s,%(etag)s,%(lm)s,%(ch)s,%(c)s,%(cb)s,%(ct)s,
         %(sc)s,%(fa)s,%(lca)s,%(err)s,%(wt)s,%(tl)s)
      ON CONFLICT (url) DO UPDATE SET
        etag = COALESCE(EXCLUDED.etag, candidate_rk_document_content.etag),
        last_modified = COALESCE(EXCLUDED.last_modified, candidate_rk_document_content.last_modified),
        content_hash = COALESCE(EXCLUDED.content_hash, candidate_rk_document_content.content_hash),
        content = COALESCE(EXCLUDED.content, candidate_rk_document_content.content),
        content_bytes = COALESCE(EXCLUDED.content_bytes, candidate_rk_document_content.content_bytes),
        content_type = COALESCE(EXCLUDED.content_type, candidate_rk_document_content.content_type),
        status_code = EXCLUDED.status_code,
        fetched_at = COALESCE(EXCLUDED.fetched_at, candidate_rk_document_content.fetched_at),
        last_checked_at = EXCLUDED.last_checked_at,
        error_message = EXCLUDED.error_message,
        was_truncated = EXCLUDED.was_truncated,
        is_too_large = EXCLUDED.is_too_large;
    """,
        payload,
    )
