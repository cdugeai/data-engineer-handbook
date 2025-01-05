-- Q3 - Which team has won the most games?
-- -> team is GSW with 11583 wins
WITH winnning AS (
    SELECT
        winning_team,
        wins
    FROM game_details_agg
    WHERE aggregation_level = 'winning_team'
    ORDER BY wins DESC

)

SELECT *
FROM winnning
LIMIT 1

