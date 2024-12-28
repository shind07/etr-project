import logging
import time

import numpy as np


def aggregate_percentiles(df, percentiles):
    """
    Aggregates simulation data at multiple percentiles for each player/team.
    """
    start_time = time.time()
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

    df_percentiles = df.groupby(group_cols)[cols_to_quantile].quantile(
        percentiles, interpolation="linear"
    ).reset_index()
    rename_dict = {
        col: "percentile" for col in df_percentiles.columns if col.startswith("level_")
    }
    df_percentiles.rename(columns=rename_dict, inplace=True)
    logging.info(f"Aggregate took: {time.time() - start_time} seconds")
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
