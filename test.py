import requests
u = "https://docs.snowflake.com/en/migrations/snowconvert-docs/translation-references/transact/transact-general-statements"
for ua in ["rk-doc-ingestor/1.0", "Mozilla/5.0"]:
    r = requests.get(u, headers={"User-Agent": ua}, timeout=30, allow_redirects=True)
    print(ua, r.status_code, r.url)
