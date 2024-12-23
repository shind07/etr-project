import numpy as np
import time
import logging


def aggregate_percentiles(df, teams_not_started, percentile_seq):
    """
    Aggregates simulation data at multiple percentiles for each player/team.

    Parameters
    ----------
    df : pd.DataFrame
        The full dataset containing simulation columns (e.g. 'sim_pass_attempts', 'sim_rush_yards', etc.)
    teams_not_started : list or array-like
        List of Teams for which we want to filter (e.g., the R script uses unique(player_sims$Team)).
    percentile_seq : list or array-like
        Sequence of percentiles at which to aggregate (e.g., np.arange(0.01, 1.00, 0.01)).

    Returns
    -------
    pd.DataFrame
        A DataFrame containing aggregated percentiles for each combination of
        (Season, Week, GameSimID, Team, Opponent, Player, Position, GSISID, PlayerID).
        The result includes a 'percentile' column plus columns for each stat's quantiles.
    """
    # 1) Filter down to the teams we care about
    df_filtered = df[df["Team"].isin(teams_not_started)].copy()

    # 2) Define which columns to compute quantiles for
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

    # 3) Define the columns to group by
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

    # 4) Group and calculate multiple quantiles in one pass
    #    This is much faster than manual loops or multiple calls.
    start_time = time.time()
    result = df_filtered.groupby(group_cols)[cols_to_quantile].quantile(
        percentile_seq, interpolation="linear"
    )
    end_time = time.time()
    logging.info(f"Aggregation took: {end_time - start_time} seconds")

    # 5) Reset the index so that 'percentile' becomes a column instead of part of a MultiIndex
    result = result.reset_index()

    # By default, Pandas will name the new index level something like 'level_9'
    # (depending on how many group columns you have).
    # Let's automatically rename any 'level_*' column to 'percentile'.
    rename_dict = {
        col: "percentile" for col in result.columns if col.startswith("level_")
    }
    result.rename(columns=rename_dict, inplace=True)

    # 6) (Optional) Fill any missing values with 0, like in R
    result.fillna(0, inplace=True)

    # 7) (Optional) Round numeric columns to 2 decimals
    numeric_cols = result.select_dtypes(include=[np.number]).columns
    result[numeric_cols] = result[numeric_cols].round(2)

    # 8) Sort if desired (mimicking the R script order)
    result.sort_values(by=["Team", "Position", "percentile"], inplace=True)
    result.reset_index(drop=True, inplace=True)
    result = rename_columns(result)

    return result


def rename_columns(df):
    """
    Renames columns from their original 'sim_*' names to the
    final names used in the R script, e.g., pass_att, rush_att, etc.

    Parameters
    ----------
    df : pd.DataFrame
        The DataFrame whose columns should be renamed in-place.

    Returns
    -------
    pd.DataFrame
        The same DataFrame (for chaining) with renamed columns.
    """
    # Mapping from original column name -> final column name
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

    # Perform the renaming in-place, only on columns that exist in df
    for old_name, new_name in col_mapping.items():
        if old_name in df.columns:
            df.rename(columns={old_name: new_name}, inplace=True)

    return df
