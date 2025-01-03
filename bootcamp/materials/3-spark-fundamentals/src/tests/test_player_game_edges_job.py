from chispa.dataframe_comparer import assert_df_equality

from ..jobs.player_game_edges_job import do_player_game_edges_transformation
from collections import namedtuple


GameDetails = namedtuple(
    "GameDetails",
    "player_id game_id start_position pts team_id team_abbreviation",
)
PlayerGameEdge = namedtuple(
    "PlayerGameEdge",
    "subject_identifier subject_type object_identifier object_type edge_type properties",
)


def test_vertex_generation_dedupe(spark):
    input_data = [
        GameDetails(1, 2, "F", 99, "San Francisco", "GSW"),
        GameDetails(1, 2, "F", 99, "San Francisco (WRONG)", "GSW"),
    ]

    input_dataframe = spark.createDataFrame(input_data)
    actual_df = do_player_game_edges_transformation(spark, input_dataframe)
    expected_output = [
        PlayerGameEdge(
            subject_identifier=1,
            subject_type="player",
            object_identifier=2,
            object_type="game",
            edge_type="plays_in",
            properties={
                "start_position": "F",
                "pts": 99,
                "team_id": "San Francisco",
                "team_abbreviation": "GSW",
            },
        )
    ]
    expected_df = spark.createDataFrame(expected_output)
    assert_df_equality(actual_df, expected_df, ignore_nullable=True)
