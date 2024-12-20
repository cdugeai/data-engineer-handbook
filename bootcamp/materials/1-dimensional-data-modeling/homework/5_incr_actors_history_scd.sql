WITH last_year_scd AS (
    SELECT * FROM actors_history_scd
    WHERE
        end_date = 1979
        AND current_year = 1979
),

historic_scd AS (
    SELECT
        actorid,
        quality_class,
        is_active,
        start_date,
        end_date
    FROM actors_history_scd
    WHERE
        current_year = 1979
        OR end_date < 1979
),


this_year_actors AS (
    SELECT * FROM actors
    WHERE current_year = 1980
),

unchanged_records AS (

    SELECT
        a.actorid,
        a.quality_class,
        a.is_active,
        b.start_date,
        -- we change end_date of the record to cureent year
        a.current_year AS end_date
    FROM this_year_actors AS a
    LEFT JOIN last_year_scd AS b
        ON a.actorid = b.actorid
    WHERE a.quality_class = b.quality_class AND a.is_active = b.is_active
),

changed_records AS (

    SELECT
        a.actorid,
        a.quality_class,
        a.is_active,
        a.current_year AS start_date,
        a.current_year AS end_date
    FROM this_year_actors AS a
    LEFT JOIN last_year_scd AS b
        ON a.actorid = b.actorid
    WHERE a.quality_class <> b.quality_class OR a.is_active <> b.is_active
),


new_records AS (

    SELECT
        a.actorid,
        a.quality_class,
        a.is_active,
        a.current_year AS start_date,
        a.current_year AS end_date
    FROM this_year_actors AS a
    LEFT JOIN last_year_scd AS b
        ON a.actorid = b.actorid
    WHERE b.actorid IS null
)

SELECT
    *,
    1980 AS current_season
FROM (
    SELECT * FROM historic_scd
    UNION ALL
    SELECT * FROM unchanged_records
    UNION ALL
    SELECT * FROM changed_records
    UNION ALL
    SELECT * FROM new_records
) AS a
