import logging
import time

import polars as pl
import pandas as pd


def aggregate_percentiles_polars(df, teams_not_started, percentile_seq):
    if not isinstance(df, pl.DataFrame):
        df = pl.DataFrame(df)

    if hasattr(teams_not_started, "tolist"):
        teams_not_started = teams_not_started.tolist()
    start_time = time.time()

    df_filtered = df.filter(pl.col("Team").is_in(teams_not_started))
    logging.info(f"Type of df_filtered: {type(df_filtered)}")
    lazy_df = df_filtered.lazy()
    logging.info(f"Type of lazy_df: {type(lazy_df)}")

    percentiles_list = percentile_seq.tolist()

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

    start_time = time.time()
    aggs = []
    for c in cols_to_quantile:
        for q in percentiles_list:
            aggs.append(pl.col(c).quantile(q).alias(f"{c}_q{q:.2f}"))

    result = lazy_df.group_by(group_cols).agg(aggs).collect()
    end_time = time.time()
    logging.info(f"Aggregation took: {end_time - start_time} seconds")

    result = result.to_pandas()
    df_long = polars_to_long_format(df, group_cols)

    logging.info(f"Type of result: {type(result)}")
    logging.info(result.head())

    return df_long


def polars_to_long_format(df, group_cols):
    all_cols = df.columns
    stat_cols = [c for c in all_cols if "_q" in c]

    melted = df.melt(
        id_vars=group_cols,
        value_vars=stat_cols,
        variable_name="stat_raw",
        value_name="value",
    )

    def parse_col(col_name):
        base_stat, q_str = col_name.split("_q")
        return base_stat, float(q_str)

    melted["base_stat"] = melted["stat_raw"].apply(lambda x: parse_col(x)[0])
    melted["percentile"] = melted["stat_raw"].apply(lambda x: parse_col(x)[1])

    melted.drop(columns=["stat_raw"], inplace=True)

    out = melted.pivot(
        index=group_cols + ["percentile"], columns="base_stat", values="value"
    ).reset_index()

    return out
