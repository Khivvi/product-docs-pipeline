INSERT INTO candidate_rk_docs_master (url, sources, first_seen_at, last_seen_at)
SELECT
  s.url,
  ARRAY_AGG(DISTINCT s.source ORDER BY s.source) FILTER (WHERE s.source IS NOT NULL) AS sources,
  MIN(s.discovered_at) AS first_seen_at,
  MAX(s.discovered_at) AS last_seen_at
FROM candidate_rk_sitemap_staging s
GROUP BY s.url
ON CONFLICT (url)
DO UPDATE SET
  sources = (
    SELECT ARRAY(
      SELECT DISTINCT x
      FROM UNNEST(candidate_rk_docs_master.sources || EXCLUDED.sources) AS t(x)
      WHERE x IS NOT NULL
      ORDER BY x
    )
  ),
  first_seen_at = LEAST(candidate_rk_docs_master.first_seen_at, EXCLUDED.first_seen_at),
  last_seen_at  = GREATEST(candidate_rk_docs_master.last_seen_at, EXCLUDED.last_seen_at);
