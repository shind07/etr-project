import pandas as pd


def main():
    df = pd.read_parquet("data/ETR_SimOutput_2024_15.parquet")
    print(df.head())

if __name__ == "__main__":
    main()