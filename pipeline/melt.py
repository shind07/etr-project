import logging

import pandas as pd


def melt_position_data(df, unique_players, positions, measure_vars):
    logging.info(f"Melt position data for {positions} with {measure_vars}")
    # 1) Filter for the specified positions + unique players
    subset = df.loc[
        (df["PlayerID"].isin(unique_players)) & (df["Position"].isin(positions))
    ].copy()

    # 2) Melt (pivot longer) so that measure_vars become 'stat' and 'value'
    melted_df = pd.melt(
        subset,
        id_vars=["Player", "percentile"],  # same as in R: c("Player", "percentile")
        value_vars=measure_vars,
        var_name="stat",
        value_name="value",
    )

    return melted_df
