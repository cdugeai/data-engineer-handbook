-- GSW has 66 wins at most in the last 90 days

WITH
teams_dedupe AS (
    SELECT
        team_id,
        abbreviation
    FROM teams
    GROUP BY team_id, abbreviation
),

game_team_winner AS (
    SELECT
        game_id,
        g.game_date_est AS game_date,
        t_home.abbreviation AS team_home,
        t_visitor.abbreviation AS team_visitor,
        1 AS win,
        CASE
            WHEN home_team_wins = 1 THEN t_home.abbreviation
            ELSE t_visitor.abbreviation
        END AS winning_team

    FROM games AS g
    LEFT JOIN teams_dedupe AS t_home ON g.home_team_id = t_home.team_id
    LEFT JOIN teams_dedupe AS t_visitor ON g.visitor_team_id = t_visitor.team_id
),

sum_home_vs_visitor AS (
    SELECT
        team_home,
        team_visitor,
        sum(CASE WHEN winning_team = team_home THEN 1 ELSE 0 END)
            OVER (
                ORDER BY game_date ROWS BETWEEN 90 PRECEDING AND CURRENT ROW
            )
        AS victories_home,
        sum(CASE WHEN winning_team = team_visitor THEN 1 ELSE 0 END)
            OVER (
                ORDER BY game_date ROWS BETWEEN 90 PRECEDING AND CURRENT ROW
            )
        AS victories_visitor

    FROM game_team_winner
)

SELECT
    CASE
        WHEN victories_home > victories_visitor THEN team_home
        ELSE team_visitor
    END AS team,
    greatest(victories_home, victories_visitor) AS victories
FROM sum_home_vs_visitor
ORDER BY victories DESC
LIMIT 1
