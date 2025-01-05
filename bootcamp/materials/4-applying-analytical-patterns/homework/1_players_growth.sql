--delete from players_growth;

WITH yesterday AS (
    SELECT * FROM players_growth
    WHERE current_season = 1999 - 1
),

today AS (
    SELECT
        player_name,
        season AS current_season
    FROM player_seasons
    WHERE season = 1999

)

INSERT INTO players_growth
SELECT
    COALESCE(t.player_name, y.player_name) AS player_name,
    COALESCE(y.first_active_season, t.current_season) AS first_active_season,
    COALESCE(t.current_season, y.last_active_season) AS last_active_season,
    CASE
        WHEN y.current_season IS null THEN 'New'
        WHEN
            t.current_season IS null AND y.last_active_season = y.current_season
            THEN 'Retired'
        WHEN
            y.last_active_season = t.current_season - 1
            THEN 'Continued Playing'
        WHEN
            y.last_active_season < t.current_season - 1
            THEN 'Returned from Retirement'
        WHEN
            y.last_active_season < y.current_season
            AND t.current_season IS null
            THEN 'Stayed Retired'
    END AS season_active_state,
    CASE
        WHEN y.seasons_active IS null THEN ARRAY[t.current_season]
        WHEN
            y.seasons_active IS NOT null AND t.current_season IS null
            THEN y.seasons_active
        WHEN
            y.seasons_active IS NOT null AND t.current_season IS NOT null
            THEN t.current_season || y.seasons_active
    END AS seasons_active,
    COALESCE(t.current_season, y.current_season + 1) AS current_season
FROM today AS t FULL OUTER JOIN yesterday AS y
    ON t.player_name = y.player_name
