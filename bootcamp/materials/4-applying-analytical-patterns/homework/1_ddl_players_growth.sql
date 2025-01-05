DROP TABLE IF EXISTS players_growth;

CREATE TABLE players_growth (
    player_name TEXT,
    first_active_season INTEGER,
    last_active_season INTEGER,
    season_active_state TEXT,
    seasons_active INTEGER [],
    current_season INTEGER,
    PRIMARY KEY (player_name, current_season)
);
