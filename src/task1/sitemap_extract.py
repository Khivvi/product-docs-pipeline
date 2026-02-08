import os
from datetime import datetime
import requests
from lxml import etree
import psycopg2
from dotenv import load_dotenv

load_dotenv()

SITEMAP_URLS = [
    "https://docs.snowflake.com/en/sitemap.xml",
    "https://other-docs.snowflake.com/en/sitemap.xml",
]

def db_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=int(os.getenv("DB_PORT", "5432")),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
    )

def clean_text(s: str):
    if s is None:
        return None
    return " ".join(s.split())

def parse_lastmod(text: str):
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except Exception:
        return None

def fetch_xml(url: str) -> bytes:
    r = requests.get(url, timeout=30, headers={"User-Agent": "sitemap-bot/1.0"})
    r.raise_for_status()
    return r.content

def local_name(tag: str) -> str:
    return tag.split("}")[-1]

def is_sitemap_index(root) -> bool:
    return local_name(root.tag).lower() == "sitemapindex"

def upsert_row(cur, url: str, source: str, lastmod):
    url = clean_text(url)
    source = clean_text(source)
    if not url or not url.startswith("http"):
        return

    cur.execute(
        """
        INSERT INTO candidate_rk_sitemap_staging (url, source, lastmod)
        VALUES (%s, %s, %s)
        ON CONFLICT (url)
        DO UPDATE SET
          source = EXCLUDED.source,
          lastmod = COALESCE(EXCLUDED.lastmod, candidate_rk_sitemap_staging.lastmod);
        """,
        (url, source, lastmod),
    )

def process_sitemap(sitemap_url: str, visited: set, cur):
    sitemap_url = clean_text(sitemap_url)
    if sitemap_url in visited:
        return
    visited.add(sitemap_url)
    print("Processing:", sitemap_url)

    root = etree.fromstring(fetch_xml(sitemap_url))

    if is_sitemap_index(root):
        child_sitemaps = root.xpath(
            "//*[local-name()='sitemap']/*[local-name()='loc']/text()"
        )
        for child in child_sitemaps:
            process_sitemap(child, visited, cur)
    else:
        src = sitemap_url  # source = which sitemap file it came from
        urls = root.xpath("//*[local-name()='url']/*[local-name()='loc']/text()")
        lastmods = root.xpath("//*[local-name()='url']/*[local-name()='lastmod']/text()")

        for i, doc_url in enumerate(urls):
            lastmod_text = lastmods[i] if i < len(lastmods) else None
            upsert_row(cur, doc_url, src, parse_lastmod(clean_text(lastmod_text)))

def main():
    visited = set()
    with db_connection() as conn:
        with conn.cursor() as cur:
            for s in SITEMAP_URLS:
                process_sitemap(s, visited, cur)
        conn.commit()
    print(f"Done. Visited {len(visited)} sitemap files.")

if __name__ == "__main__":
    main()
