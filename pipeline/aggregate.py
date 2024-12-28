import logging
import os
from multiprocessing import Pool
import time


import numpy as np
import pandas as pd


def aggregate_percentiles_parallel(df, percentiles):
    """
    Aggregates simulation data at multiple percentiles for each player/team.
    """
    n_cpus = os.cpu_count()
    logging.info(f"Aggregating percentiles in parallel with {n_cpus} processes")
    teams = df["Team"].unique()
    team_chunks = np.array_split(teams, n_cpus)
    for i, chunk in enumerate(team_chunks):
        logging.info(f"Chunk {i} size: {len(chunk)} teams: {chunk.tolist()}")

    with Pool(processes=n_cpus) as pool:
        results = pool.starmap(
            aggregate_percentiles_worker,
            [
                (df[df["Team"].isin(chunk)], percentiles, i)
                for i, chunk in enumerate(team_chunks)
            ],
        )

    logging.info(f"Combining results from {n_cpus} processes")
    combined_df = pd.concat(results, ignore_index=True)
    return combined_df


def aggregate_percentiles_worker(df, percentiles, worker_id):
    """
    Wrapper function for aggregate_percentiles that adds logging.
    """
    logging.info(f"Worker {worker_id} starting with {len(df)} rows")
    start_time = time.time()
    result = aggregate_percentiles(df, percentiles)
    logging.info(
        f"Worker {worker_id} finished in {time.time() - start_time:.2f} seconds"
    )
    return result


def aggregate_percentiles(df, percentiles):
    """
    Aggregates simulation data at multiple percentiles for each player/team.
    """
    logging.info(f"Processing chunk with shape: {df.shape}")
    logging.info(f"Aggregating percentiles...")
    cols_to_quantile = [
        "sim_pass_attempts",
        "sim_comp",
        "sim_tds_pass",
        "sim_ints",
        "sim_pass_yards",
        "sim_longest_pass",
        "sim_rush_attempts",
        "sim_rush_yards",
        "sim_longest_rush",
        "sim_targets",
        "sim_receptions",
        "sim_rec_yards",
        "sim_longest_reception",
        "sim_tds_rush_pass",
        "sim_tds_rush_rec",
        "sim_yards_rush_pass",
        "sim_yards_rush_rec",
    ]

    group_cols = [
        "Season",
        "Week",
        "GameSimID",
        "Team",
        "Opponent",
        "Player",
        "Position",
        "GSISID",
        "PlayerID",
    ]

    df_percentiles = (
        df.groupby(group_cols)[cols_to_quantile]
        .quantile(percentiles, interpolation="linear")
        .reset_index()
    )
    rename_dict = {
        col: "percentile" for col in df_percentiles.columns if col.startswith("level_")
    }
    df_percentiles.rename(columns=rename_dict, inplace=True)
    return rename_columns(df_percentiles)


def rename_columns(df):
    """
    Renames columns from their original 'sim_*' names to the
    final names used in the R script, e.g., pass_att, rush_att, etc.
    """

    col_mapping = {
        "sim_pass_attempts": "pass_att",
        "sim_comp": "pass_comp",
        "sim_tds_pass": "pass_td",
        "sim_ints": "pass_int",
        "sim_pass_yards": "pass_yards",
        "sim_longest_pass": "pass_longest",
        "sim_rush_attempts": "rush_att",
        "sim_rush_yards": "rush_yards",
        "sim_longest_rush": "rush_longest",
        "sim_targets": "targets",
        "sim_receptions": "catches",
        "sim_rec_yards": "rec_yards",
        "sim_longest_reception": "rec_longest",
        "sim_tds_rush_pass": "pass_rush_td",
        "sim_tds_rush_rec": "rec_rush_td",
        "sim_yards_rush_pass": "pass_rush_yards",
        "sim_yards_rush_rec": "rec_rush_yards",
    }

    for old_name, new_name in col_mapping.items():
        if old_name in df.columns:
            df.rename(columns={old_name: new_name}, inplace=True)

    return df
