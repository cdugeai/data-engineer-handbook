from chispa.dataframe_comparer import assert_df_equality
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import col

from ..jobs.dedupe_game_details_job import do_dedupe_game_details_transformation
from collections import namedtuple


GameDetails = namedtuple(
    "GameDetails",
    "player_id game_id start_position pts team_id team_abbreviation",
)
GameDetailsDeduped = namedtuple(
    "GameDetailsDeduped",
    "player_id game_id start_position pts team_id team_abbreviation rownum",
)


def test_vertex_generation_dedupe(spark):
    input_data = [
        GameDetails(1, 2, "F", 99, "San Francisco", "GSW A"),
        GameDetails(1, 2, "F", 99, "San Francisco", "GSW B"),
    ]

    input_dataframe = spark.createDataFrame(input_data)
    actual_df = do_dedupe_game_details_transformation(spark, input_dataframe)
    expected_output = [
        GameDetailsDeduped(1, 2, "F", 99, "San Francisco", "GSW A", 1),
    ]
    expected_df = spark.createDataFrame(expected_output).withColumn(
        "rownum", col("rownum").cast(IntegerType()).alias("rownum")
    )
    assert_df_equality(actual_df, expected_df, ignore_nullable=True)
