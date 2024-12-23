import logging
import time

import pandas as pd
import numpy as np

from aggregate import aggregate_percentiles
from aggregate_polars import aggregate_percentiles_polars
from utils import (
    filter_unique_players,
    get_team_not_started,
    get_percentiles,
    round_numeric_columns,
    sort_percentiles,
)
from melt import melt_position_data
from columns import qb_measure_cols, pc_measure_cols, rb_measure_cols

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(filename)s - %(levelname)s - %(message)s",
)

DATA_DIRECTORY = "data"


def load_data():
    """
    Load the data from the parquet file.
    """
    df = pd.read_parquet(f"{DATA_DIRECTORY}/ETR_SimOutput_2024_15.parquet")
    logging.info(f"Loaded {df.shape[0]} rows and {df.shape[1]} columns")
    logging.info(f"Size of df: {df.memory_usage().sum() / 1024 ** 3} GB")
    return df


def main():
    logging.info("Starting pipeline")
    start_time = time.time()
    df = load_data()

    team_not_started = get_team_not_started(df)
    logging.info(f"Teams not started: {team_not_started}")

    percentiles = get_percentiles()

    percentiles_df = aggregate_percentiles(df, team_not_started, percentiles)

    percentiles_df = round_numeric_columns(percentiles_df)
    percentiles_df = sort_percentiles(percentiles_df)
    unique_players = filter_unique_players(percentiles_df)

    alt_calc_data_qb = melt_position_data(
        df=percentiles_df,
        unique_players=unique_players,
        positions=["QB"],
        measure_vars=qb_measure_cols,
    )

    alt_calc_data_pc = melt_position_data(
        df=percentiles_df,
        unique_players=unique_players,
        positions=["WR", "TE"],
        measure_vars=pc_measure_cols,
    )

    alt_calc_data_rb = melt_position_data(
        df=percentiles_df,
        unique_players=unique_players,
        positions=["RB"],
        measure_vars=rb_measure_cols,
    )

    alt_calc_data = pd.concat(
        [alt_calc_data_qb, alt_calc_data_pc, alt_calc_data_rb], ignore_index=True
    )

    alt_calc_data.sort_values(by=["Player", "stat", "percentile"], inplace=True)
    alt_calc_data.reset_index(drop=True, inplace=True)

    # Save the result to a CSV file
    alt_calc_data.to_parquet("data/percentiles_df.parquet", index=False)

    total_time = time.time() - start_time
    logging.info(f"Total time taken: {total_time} seconds")


if __name__ == "__main__":
    main()
