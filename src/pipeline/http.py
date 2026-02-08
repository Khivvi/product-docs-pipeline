import hashlib
import time

import requests

MAX_BYTES_DEFAULT = 1_000_000


def sha256(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def host_throttle_sleep(
    host_next: dict,
    host: str,
    host_delay: float,
    now_fn=time.time,
    sleep_fn=time.sleep,
):
    now = now_fn()
    if now < host_next.get(host, 0):
        sleep_fn(host_next[host] - now)
    host_next[host] = now_fn() + host_delay


def fetch_url(
    session: requests.Session,
    url: str,
    *,
    etag: str | None = None,
    last_modified: str | None = None,
    max_bytes: int = MAX_BYTES_DEFAULT,
    timeout=(5, 20),
    retries: int = 3,
    backoff: float = 0.8,
    user_agent: str = "rk-doc-ingestor/1.0",
):
    headers = {"User-Agent": user_agent}
    if etag:
        headers["If-None-Match"] = etag
    if last_modified:
        headers["If-Modified-Since"] = last_modified

    for a in range(retries + 1):
        try:
            r = session.get(
                url,
                headers=headers,
                stream=True,
                timeout=timeout,
                allow_redirects=True,
            )

            if r.status_code == 304:
                return {"status": 304}

            if r.status_code != 200:
                if 500 <= r.status_code <= 599 and a < retries:
                    time.sleep(backoff * (2**a))
                    continue
                return {
                    "status": r.status_code,
                    "err": f"HTTP {r.status_code}",
                    "ctype": r.headers.get("Content-Type"),
                }

            buf = bytearray()
            too_large = False
            for chunk in r.iter_content(8192):
                if not chunk:
                    continue
                if len(buf) + len(chunk) > max_bytes:
                    rem = max_bytes - len(buf)
                    if rem > 0:
                        buf.extend(chunk[:rem])
                    too_large = True
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
                "trunc": too_large,
                "too_large": too_large,
            }

        except (requests.Timeout, requests.ConnectionError) as e:
            if a < retries:
                time.sleep(backoff * (2**a))
                continue
            return {"status": None, "err": f"{type(e).__name__}: {e}"}
        except Exception as e:
            return {"status": None, "err": f"Unhandled: {type(e).__name__}: {e}"}
