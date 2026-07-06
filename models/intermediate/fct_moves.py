import re

from snowflake.snowpark.functions import col
from snowflake.snowpark.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    FloatType,
)


RESULTS = {"1-0", "0-1", "1/2-1/2"}


def clock_to_seconds(clock: str):
    if clock is None:
        return None

    parts = clock.split(":")

    if len(parts) == 3:
        hours = int(parts[0])
        minutes = int(parts[1])
        seconds = float(parts[2])
        return hours * 3600 + minutes * 60 + seconds

    if len(parts) == 2:
        minutes = int(parts[0])
        seconds = float(parts[1])
        return minutes * 60 + seconds

    return float(clock)


def parse_CHESS_MATCH_by_turn(game_id: str, CHESS_MATCH: str):
    if CHESS_MATCH is None:
        return []

    pattern = re.compile(
        r"""
        \b(?P<turn>\d+)\.\s+
        (?P<white_move>[^\s{}]+)
        \s+\{\[%clk\s+(?P<white_clock>[0-9:.]+)\]\}
        (?:
            \s+
            (?P=turn)\.\.\.\s+
            (?P<black_move>[^\s{}]+)
            \s+\{\[%clk\s+(?P<black_clock>[0-9:.]+)\]\}
        )?
        """,
        re.VERBOSE,
    )

    rows = []

    for match in pattern.finditer(CHESS_MATCH):
        turn = int(match.group("turn"))

        white_move = match.group("white_move")
        black_move = match.group("black_move")

        white_clock = match.group("white_clock")
        black_clock = match.group("black_clock")

        if white_move in RESULTS:
            continue

        rows.append(
            (
                game_id,
                turn,
                white_move,
                black_move,
                clock_to_seconds(white_clock),
                clock_to_seconds(black_clock),
            )
        )

    return rows


def model(dbt, session):
    dbt.config(
        materialized="incremental", incremental_strategy="merge", unique_key=["GAME_ID", "TURN"]
    )
    
    session.sql(f"USE DATABASE {dbt.this.database}").collect()
    session.sql(f"USE SCHEMA {dbt.this.database}.{dbt.this.schema}").collect()

    games = dbt.ref("stg_chess_matches")

    games_pdf = games.select(
        col("GAME_ID"),
        col("CHESS_MATCH"),
    ).to_pandas()

    output_rows = []

    for _, row in games_pdf.iterrows():
        output_rows.extend(
            parse_CHESS_MATCH_by_turn(
                game_id=row["GAME_ID"],
                CHESS_MATCH=row["CHESS_MATCH"],
            )
        )

    schema = StructType(
        [
            StructField("GAME_ID", StringType()),
            StructField("TURN", IntegerType()),
            StructField("WHITE_MOVE", StringType()),
            StructField("BLACK_MOVE", StringType()),
            StructField("WHITE_SECONDS_LEFT", FloatType()),
            StructField("BLACK_SECONDS_LEFT", FloatType()),
        ]
    )

    return session.create_dataframe(output_rows, schema=schema)