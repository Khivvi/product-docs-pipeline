import os, time, hashlib
from datetime import datetime
from urllib.parse import urlparse

import requests, psycopg2
from psycopg2.extras import DictCursor
from dotenv import load_dotenv

load_dotenv()

MAX_BYTES = 1_000_000
BATCH_SIZE = 200
HOST_DELAY = 0.30
TIMEOUT = (5, 20)          # connect, read
RETRIES = 3
BACKOFF = 0.8
UA = "rk-doc-ingestor/1.0"

PRINT_EVERY = 25           # live progress in console
DB_PROGRESS_EVERY = 200    # optional DB count refresh

def db():
    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=int(os.getenv("DB_PORT", "5432")),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
    )

def sha256(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()

def pick_batch(cur):
    # Optional: if a doc is too large, don't recheck too frequently
    cur.execute("""
      SELECT dm.url, dc.etag, dc.last_modified, dc.is_too_large
      FROM candidate_rk_docs_master dm
      LEFT JOIN candidate_rk_document_content dc ON dc.url = dm.url
      WHERE dc.url IS NULL
         OR dc.last_checked_at IS NULL
         OR dc.status_code IS DISTINCT FROM 200
         OR (
              dc.is_too_large = FALSE
              AND dc.last_checked_at < (NOW() - INTERVAL '1 day')
            )
         OR (
              dc.is_too_large = TRUE
              AND dc.last_checked_at < (NOW() - INTERVAL '7 days')
            )
      ORDER BY COALESCE(dc.last_checked_at, TIMESTAMP '1970-01-01') ASC
      LIMIT %s;
    """, (BATCH_SIZE,))
    return cur.fetchall()

def upsert(cur, url, status_code, checked_at, fetched_at=None,
           etag=None, last_modified=None, content=None, content_hash=None,
           content_type=None, content_bytes=None, err=None,
           was_trunc=False, too_large=False):
    cur.execute("""
      INSERT INTO candidate_rk_document_content
        (url, etag, last_modified, content_hash, content, content_bytes, content_type,
         status_code, fetched_at, last_checked_at, error_message, was_truncated, is_too_large)
      VALUES
        (%(url)s,%(etag)s,%(lm)s,%(ch)s,%(c)s,%(cb)s,%(ct)s,
         %(sc)s,%(fa)s,%(lca)s,%(err)s,%(wt)s,%(tl)s)
      ON CONFLICT (url) DO UPDATE SET
        etag = COALESCE(EXCLUDED.etag, candidate_rk_document_content.etag),
        last_modified = COALESCE(EXCLUDED.last_modified, candidate_rk_document_content.last_modified),

        -- Only replace content fields when we actually have a new 200 payload
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
    """, dict(url=url, etag=etag, lm=last_modified, ch=content_hash, c=content,
              cb=content_bytes, ct=content_type, sc=status_code, fa=fetched_at,
              lca=checked_at, err=err, wt=was_trunc, tl=too_large))

def fetch(session, url, etag=None, last_modified=None):
    headers = {"User-Agent": UA}
    if etag:
        headers["If-None-Match"] = etag
    if last_modified:
        headers["If-Modified-Since"] = last_modified

    for a in range(RETRIES + 1):
        try:
            r = session.get(url, headers=headers, stream=True, timeout=TIMEOUT, allow_redirects=True)

            if r.status_code == 304:
                return {"status": 304, "ctype": r.headers.get("Content-Type")}

            if r.status_code != 200:
                # retry transient 5xx
                if 500 <= r.status_code <= 599 and a < RETRIES:
                    time.sleep(BACKOFF * (2 ** a))
                    continue
                return {"status": r.status_code, "err": f"HTTP {r.status_code}",
                        "ctype": r.headers.get("Content-Type")}

            buf = bytearray()
            exceeded = False

            for chunk in r.iter_content(8192):
                if not chunk:
                    continue
                if len(buf) + len(chunk) > MAX_BYTES:
                    rem = MAX_BYTES - len(buf)
                    if rem > 0:
                        buf.extend(chunk[:rem])
                    exceeded = True
                    break
                buf.extend(chunk)

            raw = bytes(buf)
            return {
                "status": 200,
                "etag": r.headers.get("ETag"),
                "lm": r.headers.get("Last-Modified"),
                "ctype": r.headers.get("Content-Type"),
                "bytes": len(raw),
                "hash": sha256(raw),
                "text": raw.decode(r.encoding or "utf-8", errors="replace"),
                "was_truncated": exceeded,   # we stored only first 1MB
                "is_too_large": exceeded,    # treat as too large
            }

        except (requests.Timeout, requests.ConnectionError) as e:
            if a < RETRIES:
                time.sleep(BACKOFF * (2 ** a))
                continue
            return {"status": None, "err": f"{type(e).__name__}: {e}"}
        except Exception as e:
            return {"status": None, "err": f"Unhandled: {type(e).__name__}: {e}"}

def count_done(cur):
    # "done" definition: checked at least once
    cur.execute("""
      SELECT COUNT(*) AS done
      FROM candidate_rk_document_content
      WHERE last_checked_at IS NOT NULL;
    """)
    return cur.fetchone()["done"]

def main():
    host_next = {}
    processed = ok200 = notmod304 = errors = 0

    with db() as conn, conn.cursor(cursor_factory=DictCursor) as cur, requests.Session() as s:
        total_master = None
        cur.execute("SELECT COUNT(*) AS n FROM candidate_rk_docs_master;")
        total_master = cur.fetchone()["n"]

        print(f"Master URLs: {total_master}")

        while True:
            batch = pick_batch(cur)
            if not batch:
                break

            for row in batch:
                url, etag, lm = row["url"], row["etag"], row["last_modified"]
                host = urlparse(url).netloc or "unknown"

                now = time.time()
                if now < host_next.get(host, 0):
                    time.sleep(host_next[host] - now)
                host_next[host] = time.time() + HOST_DELAY

                checked_at = datetime.utcnow()
                res = fetch(s, url, etag, lm)

                st = res.get("status")
                if st == 304:
                    notmod304 += 1
                    upsert(cur, url, 304, checked_at, err=None, content_type=res.get("ctype"))
                elif st == 200:
                    ok200 += 1
                    upsert(cur, url, 200, checked_at, fetched_at=checked_at,
                           etag=res.get("etag"), last_modified=res.get("lm"),
                           content=res.get("text"), content_hash=res.get("hash"),
                           content_type=res.get("ctype"), content_bytes=res.get("bytes"),
                           err=None,
                           was_trunc=res.get("was_truncated", False),
                           too_large=res.get("is_too_large", False))
                else:
                    errors += 1
                    upsert(cur, url, st, checked_at,
                           err=res.get("err"), content_type=res.get("ctype"))

                processed += 1

                if processed % PRINT_EVERY == 0:
                    print(f"[run] processed={processed} 200={ok200} 304={notmod304} err={errors}")

                if processed % DB_PROGRESS_EVERY == 0:
                    conn.commit()
                    done = count_done(cur)
                    print(f"[db] done={done}/{total_master} ({done*100/total_master:.1f}%)")

            conn.commit()

        # final progress
        done = count_done(cur)
        print(f"✅ Done. run_processed={processed} 200={ok200} 304={notmod304} err={errors}")
        print(f"✅ DB progress: {done}/{total_master} ({done*100/total_master:.1f}%)")

if __name__ == "__main__":
    main()
