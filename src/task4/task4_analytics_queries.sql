-- name: source_counts
SELECT
    unnest(sources) AS source,
    COUNT(*) AS doc_count
FROM candidate_rk_docs_master
GROUP BY source
ORDER BY doc_count DESC, source;

-- name: monthly_distribution
SELECT
    DATE_TRUNC('month', first_seen_at) AS month,
    COUNT(*) AS doc_count
FROM candidate_rk_docs_master
WHERE first_seen_at >= CURRENT_DATE - INTERVAL '12 months'
GROUP BY month
ORDER BY month;

-- name: success_rate
SELECT
    src.source,
    COUNT(*) FILTER (WHERE dc.status_code = 200) * 1.0 / NULLIF(COUNT(*), 0) AS success_rate
FROM candidate_rk_docs_master dm
CROSS JOIN LATERAL unnest(dm.sources) AS src(source)
LEFT JOIN candidate_rk_document_content dc
    ON dm.url = dc.url
GROUP BY src.source
ORDER BY success_rate DESC, src.source;

-- name: top_paths
SELECT
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

-- name: stale_docs
SELECT
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
