from datetime import datetime
from urllib.parse import urlparse

import requests
from psycopg2.extras import DictCursor

from .db import connect_db, pick_batch, upsert_document_content
from .http import fetch_url, host_throttle_sleep


def run_content_ingest(
    *,
    batch_size: int = 200,
    host_delay: float = 0.30,
    max_bytes: int = 1_000_000,
):
    host_next = {}
    stats = {"processed": 0, "ok200": 0, "ok304": 0, "err": 0}

    with connect_db() as conn, conn.cursor(cursor_factory=DictCursor) as cur, requests.Session() as s:
        while True:
            batch = pick_batch(cur, batch_size)
            if not batch:
                break

            for row in batch:
                url, etag, lm = row["url"], row["etag"], row["last_modified"]

                host = urlparse(url).netloc or "unknown"
                host_throttle_sleep(host_next, host, host_delay)

                checked_at = datetime.utcnow()
                res = fetch_url(s, url, etag=etag, last_modified=lm, max_bytes=max_bytes)

                stats["processed"] += 1

                if res.get("status") == 304:
                    stats["ok304"] += 1
                    upsert_document_content(
                        cur,
                        {
                            "url": url,
                            "etag": None,
                            "lm": None,
                            "ch": None,
                            "c": None,
                            "cb": None,
                            "ct": None,
                            "sc": 304,
                            "fa": None,
                            "lca": checked_at,
                            "err": None,
                            "wt": False,
                            "tl": False,
                        },
                    )

                elif res.get("status") == 200:
                    stats["ok200"] += 1
                    upsert_document_content(
                        cur,
                        {
                            "url": url,
                            "etag": res.get("etag"),
                            "lm": res.get("lm"),
                            "ch": res.get("hash"),
                            "c": res.get("text"),
                            "cb": res.get("bytes"),
                            "ct": res.get("ctype"),
                            "sc": 200,
                            "fa": checked_at,
                            "lca": checked_at,
                            "err": None,
                            "wt": bool(res.get("trunc", False)),
                            "tl": bool(res.get("too_large", False)),
                        },
                    )
                else:
                    stats["err"] += 1
                    upsert_document_content(
                        cur,
                        {
                            "url": url,
                            "etag": None,
                            "lm": None,
                            "ch": None,
                            "c": None,
                            "cb": None,
                            "ct": res.get("ctype"),
                            "sc": res.get("status"),
                            "fa": None,
                            "lca": checked_at,
                            "err": res.get("err"),
                            "wt": False,
                            "tl": False,
                        },
                    )

            conn.commit()

    return stats
