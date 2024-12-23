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
    Aggregate data to the nearest percentile for each statistic.
    
    Args:
        df: pandas DataFrame with simulation data
        teams_not_started: list of teams that haven't started
        percentile_seq: sequence of percentiles to calculate
    """
    logger.info(f"Aggregating data to percentiles: {percentile_seq}")
    # Filter for teams that haven't started
    mask = df['Team'].isin(teams_not_started)
    filtered_df = df[mask]
    
    # Define stats to calculate percentiles for
    stats_mapping = {
        'pass_att': 'sim_pass_attempts',
        'pass_comp': 'sim_comp',
        'pass_td': 'sim_tds_pass',
        'pass_int': 'sim_ints',
        'pass_yards': 'sim_pass_yards',
        'pass_longest': 'sim_longest_pass',
        'rush_att': 'sim_rush_attempts',
        'rush_yards': 'sim_rush_yards',
        'rush_longest': 'sim_longest_rush',
        'targets': 'sim_targets',
        'catches': 'sim_receptions',
        'rec_yards': 'sim_rec_yards',
        'rec_longest': 'sim_longest_reception',
        'pass_rush_td': 'sim_tds_rush_pass',
        'rec_rush_td': 'sim_tds_rush_rec',
        'pass_rush_yards': 'sim_yards_rush_pass',
        'rec_rush_yards': 'sim_yards_rush_rec'
    }
    
    # Group by relevant columns
    group_cols = ['Season', 'Week', 'GameSimID', 'Team', 'Opponent', 
                 'Player', 'Position', 'GSISID', 'PlayerID']
    
    # Calculate percentiles for each stat
    result = []
    for group_key, group_df in filtered_df.groupby(group_cols):
        row = dict(zip(group_cols, group_key))
        row['percentile'] = percentile_seq
        
        # Calculate percentiles for each stat
        for new_name, orig_name in stats_mapping.items():
            row[new_name] = np.quantile(group_df[orig_name], percentile_seq)
            
        result.append(row)
    
    logger.info(f"Result shape: {len(result)}")
    
    # Convert to DataFrame and replace NAs with 0
    percentiles = pd.DataFrame(result)
    percentiles = percentiles.fillna(0)
    
    return percentiles

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
    logger.info(f"Time taken: {end_time - start_time} seconds")

    # Save the result to a CSV file
    percentiles_df.to_parquet("data/percentiles_df.parquet", index=False)


if __name__ == "__main__":
    main()