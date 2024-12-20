WITH yesterday AS (
    SELECT * FROM actors
    WHERE current_year = 1969
),

today AS (
    SELECT
        actorid,
        array_agg(row(film, votes, rating, filmid)::film) AS films,
        CASE
            WHEN avg(rating) > 8 THEN 'star'::quality_class
            WHEN avg(rating) > 7 THEN 'good'::quality_class
            WHEN avg(rating) > 6 THEN 'average'::quality_class
            ELSE 'bad'::quality_class
        END AS quality_class,
        true AS is_active,
        max(year) AS current_year
    --, count(*) AS n_films
    FROM actor_films
    WHERE year = 1970
    GROUP BY actorid

)

INSERT INTO actors
SELECT
    coalesce(y.actorid, t.actorid) AS actorid,
    y.films || t.films AS films,
    coalesce(t.quality_class, y.quality_class) AS quality_class,
    coalesce(t.is_active, false) AS is_active,
    coalesce(t.current_year, y.current_year + 1) AS current_year
FROM today AS t
FULL OUTER JOIN
    yesterday AS y ON t.actorid = y.actorid
