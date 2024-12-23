import logging

import numpy as np


def get_team_not_started(df):
    """
    Get the teams that have not started yet.
    """
    return df["Team"].unique()


def get_percentiles(start=0.01, end=0.99, step=0.01):
    """
    Get the percentiles to save.
    """
    return np.linspace(start, end, int((end - start) / step) + 1)


def round_numeric_columns(df):
    """
    Round all numeric columns to 2 decimal places.

    Args:
        df: pandas DataFrame with numeric columns to round
    """
    logging.info(f"Rounding numeric columns")
    numeric_cols = df.select_dtypes(
        include=["float64", "float32", "int64", "int32"]
    ).columns
    df[numeric_cols] = df[numeric_cols].round(2)
    return df


def sort_percentiles(df):
    """
    Sort the percentiles DataFrame by Team, Position, and percentile.

    Args:
        df: pandas DataFrame to sort
    """
    logging.info("Sorting percentiles by Team, Position, percentile")
    return df.sort_values(["Team", "Position", "percentile"])


def filter_unique_players(percentiles_df):
    """
    Filter players based on specific criteria:
    - At the 20th percentile
    - Has positive yards in either passing, rushing, or receiving
    - Is a QB, RB, WR, or TE

    Args:
        percentiles_df: DataFrame containing percentile calculations

    Returns:
        Series of unique PlayerIDs meeting the criteria
    """
    logging.info("Filtering unique players")

    mask = (
        (percentiles_df["percentile"] == 0.20)
        & (
            (percentiles_df["pass_yards"] > 0)
            | (percentiles_df["rush_yards"] > 0)
            | (percentiles_df["rec_yards"] > 0)
        )
        & (percentiles_df["Position"].isin(["QB", "RB", "WR", "TE"]))
    )

    unique_players = percentiles_df[mask]["PlayerID"].unique()
    logging.info(f"Found {len(unique_players)} unique players")

    return unique_players
