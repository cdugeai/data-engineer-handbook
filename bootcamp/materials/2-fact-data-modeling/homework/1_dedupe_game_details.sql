WITH deduped AS (
    SELECT
        *,
        row_number() OVER (PARTITION BY game_id, team_id, player_id) AS rownum
    FROM game_details
)

SELECT *
FROM deduped
WHERE rownum = 1
