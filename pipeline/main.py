import logging
import time

import pandas as pd
import numpy as np

from aggregate import aggregate_percentiles, aggregate_percentiles_parallel
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
S3_BUCKET = "etr-data-lhlithmw"
ENVIRONMENT = "local"


def load_data():
    """
    Load the data from the parquet file.
    """
    load_start_time = time.time()

    if ENVIRONMENT == "local":
        df = pd.read_parquet(f"{DATA_DIRECTORY}/ETR_SimOutput_2024_15.parquet")
    else:
        df = pd.read_parquet(f"s3://{S3_BUCKET}/input/ETR_SimOutput_2024_15.parquet")

    logging.info(f"Loaded {df.shape[0]} rows and {df.shape[1]} columns")
    logging.info(f"Size of df: {df.memory_usage().sum() / 1024 ** 3} GB")
    logging.info(f"Load took: {time.time() - load_start_time} seconds")
    return df


def main():
    logging.info(f"Starting pipeline in {ENVIRONMENT} environment")
    start_time = time.time()
    df = load_data()

    teams_not_started = get_team_not_started(df)
    df = df[df["Team"].isin(teams_not_started)]

    percentiles = get_percentiles()
    aggregate_start_time = time.time()
    percentiles_df = aggregate_percentiles_parallel(df, percentiles)
    logging.info(f"Aggregate took: {time.time() - aggregate_start_time} seconds")
    percentiles_df.fillna(0, inplace=True)
    percentiles_df = round_numeric_columns(percentiles_df)
    percentiles_df = sort_percentiles(percentiles_df)
    unique_players = filter_unique_players(percentiles_df)

    melt_start_time = time.time()
    qb_data = melt_position_data(
        df=percentiles_df,
        unique_players=unique_players,
        positions=["QB"],
        measure_vars=qb_measure_cols,
    )

    pc_data = melt_position_data(
        df=percentiles_df,
        unique_players=unique_players,
        positions=["WR", "TE"],
        measure_vars=pc_measure_cols,
    )

    rb_data = melt_position_data(
        df=percentiles_df,
        unique_players=unique_players,
        positions=["RB"],
        measure_vars=rb_measure_cols,
    )
    logging.info(f"Melt took: {time.time() - melt_start_time} seconds")

    postprocess_start_time = time.time()
    final_data = pd.concat([qb_data, pc_data, rb_data], ignore_index=True)
    final_data.sort_values(by=["Player", "stat", "percentile"], inplace=True)
    final_data.reset_index(drop=True, inplace=True)
    logging.info(f"Postprocess took: {time.time() - postprocess_start_time} seconds")

    save_start_time = time.time()

    if ENVIRONMENT == "local":
        final_data.to_parquet("data/percentiles_df.parquet", index=False)
    else:
        final_data.to_parquet(
            f"s3://{S3_BUCKET}/output/percentiles_df.parquet", index=False
        )
    logging.info(f"Save took: {time.time() - save_start_time} seconds")
    logging.info(f"Total time taken: {time.time() - start_time} seconds")


if __name__ == "__main__":
    main()
