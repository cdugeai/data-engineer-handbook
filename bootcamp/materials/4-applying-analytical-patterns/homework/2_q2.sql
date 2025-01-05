-- Q2 - Who scored the most points in one season?
-- List of most scoring player for every season


WITH rank_players_in_season AS (
    SELECT
        player_name,
        season,
        total_player_points,
        row_number()
            OVER (
                PARTITION BY season
                ORDER BY total_player_points DESC
            )
        AS player_rank_in_season
    FROM game_details_agg
    WHERE aggregation_level = 'player__season'
)

SELECT
    player_name,
    season,
    total_player_points
FROM rank_players_in_season
WHERE player_rank_in_season = 1
ORDER BY season DESC
