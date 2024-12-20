WITH current_and_previous AS (
    SELECT
        actorid,
        is_active,
        quality_class,
        current_year,
        lag(current_year, 1)
            OVER (
                PARTITION BY actorid
                ORDER BY current_year ASC
            )
        AS year_previous,
        lag(is_active, 1)
            OVER (
                PARTITION BY actorid
                ORDER BY current_year ASC
            )
        AS is_active_previous,
        lag(quality_class, 1)
            OVER (
                PARTITION BY actorid
                ORDER BY current_year ASC
            )
        AS quality_class_previous
    FROM actors
),

changing_indicators AS (

    SELECT
        *,
        CASE WHEN is_active <> is_active_previous THEN 1 ELSE 0
        END AS changing_indicator_is_active,
        CASE WHEN quality_class <> quality_class_previous THEN 1 ELSE 0
        END AS changing_indicator_quality_class
    FROM current_and_previous
),

changing_indicator_combined AS (
    SELECT
        *,
        CASE
            WHEN
                changing_indicator_is_active = 0
                AND changing_indicator_quality_class = 0
                THEN 0
            ELSE 1
        END
        AS changing_indicator_combined
    FROM changing_indicators
),

changes_combined_cnt AS (

    SELECT
        *,
        changing_indicator_is_active,
        changing_indicator_quality_class,
        changing_indicator_combined,
        sum(changing_indicator_is_active)
            OVER (
                PARTITION BY actorid
                ORDER BY current_year ASC
            )
        AS changes_is_active,
        sum(changing_indicator_quality_class)
            OVER (
                PARTITION BY actorid
                ORDER BY current_year ASC
            )
        AS changes_quality_class,
        sum(changing_indicator_combined)
            OVER (
                PARTITION BY actorid
                ORDER BY current_year ASC
            )
        AS changes_combined
    FROM changing_indicator_combined
    ORDER BY actorid ASC, current_year ASC
)


SELECT
    actorid,
    is_active,
    quality_class,
    min(current_year) AS year_start,
    max(current_year) AS year_end
FROM changes_combined_cnt
GROUP BY actorid, changes_combined, is_active, quality_class
ORDER BY actorid
