{{ config(
    materialized='table',
    schema='monitoring'
) }}

WITH row_count_metrics AS (
    {{ metrics.calculate(
        metric('table_row_count'),
        grain='week',
        dimensions=['table_name', 'schema_name', 'database_name'],
        secondary_calculations=[
            metrics.period_over_period(comparison_strategy="ratio", interval=1, alias="wow_change"),
            metrics.period_over_period(comparison_strategy="ratio", interval=4, alias="mom_change")
        ]
    )
    }}
),

size_metrics AS (
    {{ metrics.calculate(
        metric('table_size_bytes'),
        grain='week',
        dimensions=['table_name', 'schema_name', 'database_name'],
        secondary_calculations=[
            metrics.period_over_period(comparison_strategy="ratio", interval=1, alias="wow_change"),
            metrics.period_over_period(comparison_strategy="ratio", interval=4, alias="mom_change")
        ]
    )
    }}
)

SELECT
    r.table_name,
    r.schema_name,
    r.database_name,
    r.metric_date,
    r.table_row_count AS avg_row_count,
    r.table_row_count_wow_change AS row_count_week_over_week_ratio,
    r.table_row_count_mom_change AS row_count_month_over_month_ratio,
    s.table_size_bytes AS avg_size_bytes,
    s.table_size_bytes_wow_change AS size_week_over_week_ratio,
    s.table_size_bytes_mom_change AS size_month_over_month_ratio,
    -- Flag stale tables
    CASE
        WHEN r.table_row_count_wow_change BETWEEN 0.99 AND 1.01
             AND r.table_row_count_mom_change BETWEEN 0.99 AND 1.01
        THEN TRUE
        ELSE FALSE
    END AS is_stale,
    -- Flag rapid growth
    CASE
        WHEN r.table_row_count_wow_change > 1.5 -- 50% growth in a week
        THEN TRUE
        ELSE FALSE
    END AS has_rapid_growth
FROM
    row_count_metrics r
JOIN
    size_metrics s
ON
    r.table_name = s.table_name
    AND r.schema_name = s.schema_name
    AND r.database_name = s.database_name
    AND r.metric_date = s.metric_date
ORDER BY
    r.metric_date DESC,
    r.schema_name,
    r.table_name
