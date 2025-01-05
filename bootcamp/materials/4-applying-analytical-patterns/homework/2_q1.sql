-- Q1 - Who scored the most points playing for one team ?
-- List of most scoring player for each team

WITH rank_players_in_teams AS (
    SELECT
        team_abbreviation,
        player_name,
        total_player_points,
        row_number()
            OVER (
                PARTITION BY team_abbreviation
                ORDER BY total_player_points DESC
            )
        AS player_rank_in_team
    FROM game_details_agg
    WHERE aggregation_level = 'player__team'
)

SELECT
    team_abbreviation,
    player_name,
    total_player_points
FROM rank_players_in_teams
WHERE player_rank_in_team = 1
ORDER BY total_player_points DESC
