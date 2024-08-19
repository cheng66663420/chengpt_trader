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
    """
    获取指定时间区间的股票数据。

    Parameters
    ----------
    ticker_symbol : str
        股票代码
    start_time : str
        开始时间
    end_time : str
        结束时间
    period : str
        周期
    data_dir : _type_, optional
        数据地址, by default "D:/qmt_datadir"

    Returns
    -------
    pd.DataFrame
        包含指定时间区间的股票数据的DataFrame

    Raises
    ------
    FileNotFoundError
        如果指定的文件不存在，则抛出此异常
    """
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


def resample_stock_data(df: pd.DataFrame, resample_period: str = "30T") -> pd.DataFrame:
    """
    对股票数据进行重采样。

    Parameters
    ----------
    df : pd.DataFrame
        包含股票数据的DataFrame
    resample_period : str, optional
        重采样周期, by default "30T"

    Returns
    -------
    pd.DataFrame
        重采样后的股票数据的DataFrame
    """
    df = df.copy()
    df["trade_time"] = pd.to_datetime(df["trade_time"])
    df.set_index("trade_time", inplace=True)
    result_list = []
    for ticker_symbol, df_grouped in df.groupby("ticker_symbol"):
        # 重采样并聚合数据
        resampled_df = (
            df_grouped.resample(resample_period)
            .agg(
                {
                    "open": "first",
                    "high": "max",
                    "low": "min",
                    "close": "last",
                    "volume": "sum",
                    "amount": "sum",
                    "settelement_price": "last",
                    "open_interest": "last",
                    "preclose": "last",
                    "suspend_flag": "last",
                }
            )
            .reset_index()
        )
        # 将 ticker_symbol 列插入到第二列
        resampled_df.insert(1, "ticker_symbol", ticker_symbol)
        result_list.append(resampled_df)
    return pd.concat(result_list)


if __name__ == "__main__":
    # Example usage
    ticker_symbol = "159941.SZ"
    start_time = "20100101"
    end_time = "20240815"
    period = "1d"
    data_dir = "D:/qmt_datadir"
    quote_data = get_quote_data(ticker_symbol, start_time, end_time, period, data_dir)
    print(quote_data)
