DROP TABLE IF EXISTS game_details_agg;

CREATE TABLE game_details_agg (
    player_name text,
    team_abbreviation text,
    season integer,

    total_player_points integer,
    --total_home_team_wins integer,
    winning_team text,
    wins integer,
    aggregation_level text


    --PRIMARY KEY (player_name, team_abbreviation, season)

);



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
        CASE
            WHEN home_team_wins = 1 THEN t_home.abbreviation
            ELSE t_visitor.abbreviation
        END AS winning_team

    FROM games AS g
    LEFT JOIN teams_dedupe AS t_home ON g.home_team_id = t_home.team_id
    LEFT JOIN teams_dedupe AS t_visitor ON g.visitor_team_id = t_visitor.team_id
)




INSERT INTO game_details_agg
SELECT
    coalesce(d.player_name, '_overall') AS player_name,
    coalesce(d.team_abbreviation, '_overall') AS team_abbreviation,
    coalesce(g.season, -1) AS season,
    sum(coalesce(d.pts, 0)) AS total_player_points,
    gtw.winning_team,
    count(*) AS wins,
    CASE
        WHEN
            grouping(d.player_name) = 0
            AND grouping(d.team_abbreviation) = 0
            AND grouping(g.season) = 0
            THEN 'player__team__season'
        WHEN
            grouping(d.player_name) = 0
            AND grouping(d.team_abbreviation) = 0
            THEN 'player__team'
        WHEN
            grouping(d.player_name) = 0
            AND grouping(g.season) = 0
            THEN 'player__season'
        WHEN grouping(gtw.winning_team) = 0
            THEN 'winning_team'
    END AS aggregation_level
FROM game_details AS d
LEFT JOIN games AS g ON d.game_id = g.game_id
LEFT JOIN game_team_winner AS gtw ON g.game_id = gtw.game_id
GROUP BY GROUPING SETS (
    (d.player_name, d.team_abbreviation, g.season, gtw.winning_team),
    -- who scored the most points playing for one team
    (d.player_name, d.team_abbreviation),
    -- who scored the most points in one season?
    (d.player_name, g.season),
    -- which team has won the most games?
    (gtw.winning_team)

)
