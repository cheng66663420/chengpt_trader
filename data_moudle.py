import os
import pandas as pd
import dask.dataframe as dd
from dask import delayed


def get_quote_data(
    ticker_symbol: str,
    start_time: str,
    end_time: str,
    period: str,
    data_dir: str = "D:/qmt_datadir",
) -> pd.DataFrame:
    file_path = os.path.join(data_dir, period, ticker_symbol)
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")

    start_time_year_month = pd.to_datetime(start_time).strftime("%Y%m")
    end_time_year_month = pd.to_datetime(end_time).strftime("%Y%m")
    file_list = [
        file
        for file in os.listdir(file_path)
        if start_time_year_month <= file[:6] <= end_time_year_month
    ]

    @delayed
    def read_parquet(file):
        return pd.read_parquet(os.path.join(file_path, file))

    delayed_dfs = [read_parquet(file) for file in file_list]
    dask_df = dd.from_delayed(delayed_dfs)

    result_df = dask_df[
        (dask_df["trade_time"] >= start_time) & (dask_df["trade_time"] <= end_time)
    ].compute()

    return result_df.sort_values(by="trade_time")


if __name__ == "__main__":
    # Example usage
    ticker_symbol = "159941.SZ"
    start_time = "20100101"
    end_time = "20240815"
    period = "1d"
    data_dir = "D:/qmt_datadir"
    quote_data = get_quote_data(ticker_symbol, start_time, end_time, period, data_dir)
    print(quote_data)
