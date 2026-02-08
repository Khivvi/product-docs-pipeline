-- ============================================================
-- TASK 5: Query Optimization (PostgreSQL)
-- For each scenario, provide two implementations + rationale.
-- ============================================================

-- ----------------------------------------------------------------
-- Scenario 1: Documents modified within a 7-day rolling window
-- Optimization axis: Cost ↔ Latency
--
-- Assumptions:
-- - Prefer sitemap-derived timestamp when available: candidate_rk_sitemap_staging.lastmod
-- - If lastmod is missing, fall back to docs_master.last_seen_at (ingestion signal)
--   so the query still returns something deterministic.
-- ----------------------------------------------------------------

-- Scenario 1 — COST-EFFICIENT APPROACH
-- Rationale: Avoid scanning the entire staging table by reducing it to per-URL max(lastmod)
--           first, then joining to master. This typically lowers IO and CPU because
--           it aggregates early and only touches each URL once in the join.

DROP MATERIALIZED VIEW IF EXISTS mv_lastmod_per_url;

CREATE MATERIALIZED VIEW mv_lastmod_per_url AS
SELECT
  dm.url,
  COALESCE(
    MAX(ss.lastmod),
    MAX(dc.last_modified::timestamptz),
    MAX(dc.fetched_at),
    MAX(dc.last_checked_at)
  ) AS lastmod_ts
FROM candidate_rk_docs_master dm
LEFT JOIN candidate_rk_sitemap_staging ss ON ss.url = dm.url
LEFT JOIN candidate_rk_document_content dc ON dc.url = dm.url
GROUP BY dm.url;

CREATE INDEX IF NOT EXISTS idx_mv_lastmod_per_url_lastmod
  ON mv_lastmod_per_url (lastmod_ts);

CREATE INDEX IF NOT EXISTS idx_mv_lastmod_per_url_url
  ON mv_lastmod_per_url (url);


WITH lastmod_per_url AS (
  SELECT
    url,
    MAX(lastmod) AS lastmod
  FROM candidate_rk_sitemap_staging
  GROUP BY url
),
effective_ts AS (
  SELECT
    dm.url,
    COALESCE(lpu.lastmod, dm.last_seen_at) AS effective_modified_ts
  FROM candidate_rk_docs_master dm
  LEFT JOIN lastmod_per_url lpu ON lpu.url = dm.url
)
SELECT
  COUNT(*) AS docs_modified_last_7d
FROM effective_ts
WHERE effective_modified_ts >= (NOW() - INTERVAL '7 days');


-- Scenario 1 — TIME-EFFICIENT APPROACH
-- Rationale: Turn the “max(lastmod) per url” into a reusable pre-aggregated object.
--           This trades storage/maintenance for faster queries (lower latency) by
--           avoiding repeated GROUP BY over a potentially large staging table.
--
-- NOTE: If your submission environment allows DDL, keep these. Otherwise, comment them.
-- Create an index that helps the refresh / join:
-- CREATE INDEX IF NOT EXISTS idx_sitemap_staging_url_lastmod
--   ON candidate_rk_sitemap_staging (url, lastmod DESC);

-- Optional materialized view for very fast reads:
-- CREATE MATERIALIZED VIEW IF NOT EXISTS mv_lastmod_per_url AS
-- SELECT url, MAX(lastmod) AS lastmod
-- FROM candidate_rk_sitemap_staging
-- GROUP BY url;
-- CREATE UNIQUE INDEX IF NOT EXISTS mv_lastmod_per_url_pk ON mv_lastmod_per_url(url);

-- Refresh strategy example (out of band / scheduled):
-- REFRESH MATERIALIZED VIEW CONCURRENTLY mv_lastmod_per_url;

-- Fast query using MV (falls back to last_seen_at if lastmod is null):
SELECT
  COUNT(*) AS docs_modified_last_7d
FROM candidate_rk_docs_master dm
LEFT JOIN mv_lastmod_per_url mv ON mv.url = dm.url
WHERE COALESCE(mv.lastmod_ts, dm.last_seen_at) >= (NOW() - INTERVAL '7 days');



-- ----------------------------------------------------------------
-- Scenario 2: Unique URL count per source with mean content length
-- Optimization axis: Compute ↔ Parallelism
--
-- Sources are stored in candidate_rk_docs_master.sources (text[]).
-- Content length is taken from candidate_rk_document_content.content_bytes (integer).
-- ----------------------------------------------------------------

-- Scenario 2 — COST-EFFICIENT (COMPUTE-LEAN) APPROACH
-- Rationale: Aggregate content bytes per URL first (1 row per URL), then unnest sources.
--           This minimizes join amplification and repeated aggregation work.

DROP TABLE IF EXISTS tmp_source_url_facts;

CREATE TEMP TABLE tmp_source_url_facts AS
SELECT
  s.source AS source_identifier,
  s.url,
  COALESCE(dc.content_bytes, 0) AS content_bytes
FROM candidate_rk_sitemap_staging s
LEFT JOIN candidate_rk_document_content dc ON dc.url = s.url;

CREATE INDEX IF NOT EXISTS idx_tmp_source_url_facts_source
  ON tmp_source_url_facts (source_identifier);

CREATE INDEX IF NOT EXISTS idx_tmp_source_url_facts_url
  ON tmp_source_url_facts (url);



WITH content_per_url AS (
  SELECT
    url,
    MAX(content_bytes) FILTER (WHERE status_code = 200) AS content_bytes_ok
  FROM candidate_rk_document_content
  GROUP BY url
),
url_facts AS (
  SELECT
    dm.url,
    dm.sources,
    cpu.content_bytes_ok
  FROM candidate_rk_docs_master dm
  LEFT JOIN content_per_url cpu ON cpu.url = dm.url
)
SELECT
  s.source_identifier,
  COUNT(DISTINCT uf.url) AS unique_url_count,
  AVG(uf.content_bytes_ok)::numeric(18,2) AS mean_content_bytes
FROM url_facts uf
CROSS JOIN LATERAL UNNEST(uf.sources) AS s(source_identifier)
GROUP BY s.source_identifier
ORDER BY unique_url_count DESC, s.source_identifier;


-- Scenario 2 — TIME-EFFICIENT (PARALLELISM-FRIENDLY) APPROACH
-- Rationale: Create a “flat” table (or materialized view) that already has one row per
--           (source_identifier, url) with content bytes attached. This allows the final
--           aggregation to be a simple GROUP BY over a narrow, parallelizable dataset.
--
-- This is a classic warehouse pattern: denormalize to compute faster.
--
-- NOTE: If DDL is not allowed, you can implement this as a TEMP TABLE for the run.
-- CREATE INDEX recommendations are included to highlight the tradeoff.

-- Recommended indexes:
-- CREATE INDEX IF NOT EXISTS idx_docs_master_url ON candidate_rk_docs_master(url);
-- CREATE INDEX IF NOT EXISTS idx_doc_content_url_status ON candidate_rk_document_content(url, status_code);

-- Option A: TEMP TABLE (fast for a single run)
-- DROP TABLE IF EXISTS tmp_source_url_facts;
-- CREATE TEMP TABLE tmp_source_url_facts AS
-- SELECT
--   s.source_identifier,
--   dm.url,
--   dc.content_bytes
-- FROM candidate_rk_docs_master dm
-- CROSS JOIN LATERAL UNNEST(dm.sources) AS s(source_identifier)
-- LEFT JOIN candidate_rk_document_content dc
--   ON dc.url = dm.url AND dc.status_code = 200;

-- Final aggregation (works with TEMP TABLE or a persistent table/view):
SELECT
  t.source_identifier,
  COUNT(DISTINCT t.url) AS unique_url_count,
  AVG(t.content_bytes)::numeric(18,2) AS mean_content_bytes
FROM tmp_source_url_facts t
GROUP BY t.source_identifier
ORDER BY unique_url_count DESC, t.source_identifier;



-- ----------------------------------------------------------------
-- Scenario 3: Content deduplication detection (identical hashes, distinct URLs)
-- Optimization axis: Join complexity ↔ Speed
--
-- We want: content_hash groups where multiple URLs share the same hash.
-- ----------------------------------------------------------------

-- Scenario 3 — COST-EFFICIENT (LOW COMPLEXITY) APPROACH
-- Rationale: Single-table aggregation (no joins). Cheapest and simplest.
--           Uses WHERE filters to ignore null hashes and non-200 fetches.
SELECT
  content_hash,
  COUNT(*) AS url_rows,
  COUNT(DISTINCT url) AS distinct_urls
FROM candidate_rk_document_content
WHERE status_code = 200
  AND content_hash IS NOT NULL
GROUP BY content_hash
HAVING COUNT(DISTINCT url) > 1
ORDER BY distinct_urls DESC, content_hash
LIMIT 50;


-- Scenario 3 — TIME-EFFICIENT (SPEED) APPROACH
-- Rationale: Add an index on (content_hash) and prefilter to only 200 rows.
--           The index enables faster grouping / hashing and dramatically reduces
--           runtime when content_hash cardinality is high.
--
-- If allowed:
-- CREATE INDEX IF NOT EXISTS idx_doc_content_hash_200
--   ON candidate_rk_document_content(content_hash)
--   WHERE status_code = 200 AND content_hash IS NOT NULL;

-- Also, for faster “show me one canonical + duplicates” retrieval, use DISTINCT ON:
WITH hash_groups AS (
  SELECT
    content_hash,
    COUNT(DISTINCT url) AS distinct_urls
  FROM candidate_rk_document_content
  WHERE status_code = 200
    AND content_hash IS NOT NULL
  GROUP BY content_hash
  HAVING COUNT(DISTINCT url) > 1
),
top_hashes AS (
  SELECT content_hash, distinct_urls
  FROM hash_groups
  ORDER BY distinct_urls DESC, content_hash
  LIMIT 20
),
dupe_rows AS (
  SELECT
    dc.content_hash,
    dc.url,
    dc.last_checked_at,
    dc.content_bytes
  FROM candidate_rk_document_content dc
  JOIN top_hashes th ON th.content_hash = dc.content_hash
  WHERE dc.status_code = 200
)
SELECT
  content_hash,
  url,
  content_bytes,
  last_checked_at
FROM dupe_rows
ORDER BY content_hash, url;  -- deterministic ordering
