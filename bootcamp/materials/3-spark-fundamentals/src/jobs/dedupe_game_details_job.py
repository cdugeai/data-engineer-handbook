from pyspark.sql import SparkSession

query = """
WITH deduped AS (
    SELECT
        *,
        row_number() OVER (PARTITION BY game_id, team_id, player_id ORDER BY game_id, team_id, player_id) AS rownum
    FROM game_details
)

SELECT *
FROM deduped
WHERE rownum = 1
"""


def do_dedupe_game_details_transformation(spark, dataframe):
    dataframe.createOrReplaceTempView("game_details")
    return spark.sql(query)


def main():
    spark = SparkSession.builder.master("local").appName("players_scd").getOrCreate()
    output_df = do_dedupe_game_details_transformation(spark, spark.table("players"))
    output_df.write.mode("overwrite").insertInto("players_scd")
