import logging
import time

import pandas as pd
import numpy as np


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def load_data():
    """
    Load the data from the parquet file.
    """
    df = pd.read_parquet("data/ETR_SimOutput_2024_15.parquet")
    logger.info(f"Loaded {df.shape[0]} rows and {df.shape[1]} columns")
    logger.info(f"Size of df: {df.memory_usage().sum() / 1024 ** 3} GB")
    return df


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
        "sim_pass_attempts", "sim_comp", "sim_tds_pass", "sim_ints",
        "sim_pass_yards", "sim_longest_pass",
        "sim_rush_attempts", "sim_rush_yards", "sim_longest_rush",
        "sim_targets", "sim_receptions", "sim_rec_yards", "sim_longest_reception",
        "sim_tds_rush_pass", "sim_tds_rush_rec", "sim_yards_rush_pass", "sim_yards_rush_rec"
    ]

    # 3) Define the columns to group by
    group_cols = [
        "Season", "Week", "GameSimID", "Team", "Opponent",
        "Player", "Position", "GSISID", "PlayerID"
    ]

    # 4) Group and calculate multiple quantiles in one pass
    #    This is much faster than manual loops or multiple calls.
    result = (
        df_filtered.groupby(group_cols)[cols_to_quantile]
                  .quantile(percentile_seq, interpolation="linear")
    )

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
        "sim_pass_attempts":    "pass_att",
        "sim_comp":             "pass_comp",
        "sim_tds_pass":         "pass_td",
        "sim_ints":             "pass_int",
        "sim_pass_yards":       "pass_yards",
        "sim_longest_pass":     "pass_longest",
        "sim_rush_attempts":    "rush_att",
        "sim_rush_yards":       "rush_yards",
        "sim_longest_rush":     "rush_longest",
        "sim_targets":          "targets",
        "sim_receptions":       "catches",
        "sim_rec_yards":        "rec_yards",
        "sim_longest_reception":"rec_longest",
        "sim_tds_rush_pass":    "pass_rush_td",
        "sim_tds_rush_rec":     "rec_rush_td",
        "sim_yards_rush_pass":  "pass_rush_yards",
        "sim_yards_rush_rec":   "rec_rush_yards",
    }

    # Perform the renaming in-place, only on columns that exist in df
    for old_name, new_name in col_mapping.items():
        if old_name in df.columns:
            df.rename(columns={old_name: new_name}, inplace=True)

    return df


def round_numeric_columns(df):
    """
    Round all numeric columns to 2 decimal places.

    Args:
        df: pandas DataFrame with numeric columns to round
    """
    logger.info(f"Rounding numeric columns")
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
    logger.info("Sorting percentiles by Team, Position, percentile")
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
    logger.info("Filtering unique players")
    
    mask = (
        (percentiles_df['percentile'] == 0.20) & 
        ((percentiles_df['pass_yards'] > 0) | 
         (percentiles_df['rush_yards'] > 0) | 
         (percentiles_df['rec_yards'] > 0)) &
        (percentiles_df['Position'].isin(['QB', 'RB', 'WR', 'TE']))
    )
    
    unique_players = percentiles_df[mask]['PlayerID'].unique()
    logger.info(f"Found {len(unique_players)} unique players")
    
    return unique_players


import pandas as pd

def melt_position_data(df, unique_players, positions, measure_vars):
    """
    Filters by unique_players & given positions, then melts the DataFrame.
    
    Parameters
    ----------
    df : pd.DataFrame
        The full 'percentiles' dataset containing columns like 'rush_yards', 'rush_att', etc.
    unique_players : array-like
        The list/array of PlayerIDs to include.
    positions : list
        The positions to include (e.g. ['QB'], or ['WR','TE'], or ['RB']).
    measure_vars : list
        Columns to melt (i.e., columns containing stats).
        
    Returns
    -------
    pd.DataFrame
        A long-form DataFrame with columns: Player, percentile, stat, value.
    """
    logger.info(f"Melt position data for {positions} with {measure_vars}")
    # 1) Filter for the specified positions + unique players
    subset = df.loc[
        (df["PlayerID"].isin(unique_players)) & 
        (df["Position"].isin(positions))
    ].copy()

    # 2) Melt (pivot longer) so that measure_vars become 'stat' and 'value'
    melted_df = pd.melt(
        subset,
        id_vars=["Player", "percentile"],  # same as in R: c("Player", "percentile")
        value_vars=measure_vars,
        var_name="stat",
        value_name="value"
    )

    return melted_df


def main():
    """
    Main function to run the script.
    """
    start_time = time.time()
    df = load_data()
    logger.info(f"Data loaded in {time.time() - start_time} seconds")

    start_time = time.time()
    team_not_started = get_team_not_started(df)
    logger.info(f"Teams not started: {team_not_started}")

    percentiles = get_percentiles()
    logger.info(f"Percentiles: {percentiles}")

    percentiles_df = aggregate_percentiles(df, team_not_started, percentiles)

    end_time = time.time()
    logger.info(f"Aggregation taken: {end_time - start_time} seconds")

    percentiles_df = round_numeric_columns(percentiles_df)
    percentiles_df = sort_percentiles(percentiles_df)

    unique_players = filter_unique_players(percentiles_df)
    # Example measure columns from your R script:

    # For QBs
    qb_measure_cols = [
        "rush_yards", "rush_att", "rush_longest",
        "pass_att", "pass_comp", "pass_yards", "pass_int",
        "pass_longest", "pass_td", "pass_rush_td", "pass_rush_yards"
    ]

    # For pass catchers (WR, TE)
    pc_measure_cols = [
        "rec_yards", "catches", "targets",
        "rec_longest", "rec_rush_td"
    ]

    # For RBs
    rb_measure_cols = [
        "rush_yards", "rush_att", "rush_longest",
        "rec_yards", "catches", "targets", "rec_longest",
        "rec_rush_td", "rec_rush_yards"
    ]

    # Now call melt_position_data
    alt_calc_data_qb = melt_position_data(
        df=percentiles_df, 
        unique_players=unique_players, 
        positions=["QB"], 
        measure_vars=qb_measure_cols
    )

    alt_calc_data_pc = melt_position_data(
        df=percentiles_df, 
        unique_players=unique_players, 
        positions=["WR","TE"], 
        measure_vars=pc_measure_cols
    )

    alt_calc_data_rb = melt_position_data(
        df=percentiles_df, 
        unique_players=unique_players, 
        positions=["RB"], 
        measure_vars=rb_measure_cols
    )

    # Finally, combine them all (similar to rbindlist in R):
    alt_calc_data = pd.concat([alt_calc_data_qb, alt_calc_data_pc, alt_calc_data_rb], ignore_index=True)

    # Sort like in R:
    alt_calc_data.sort_values(by=["Player", "stat", "percentile"], inplace=True)
    alt_calc_data.reset_index(drop=True, inplace=True)

    # Save the result to a CSV file
    alt_calc_data.to_parquet("data/percentiles_df.parquet", index=False)

    total_time = time.time() - start_time
    logger.info(f"Total time taken: {total_time} seconds")

if __name__ == "__main__":
    main()