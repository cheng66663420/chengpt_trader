import os
import pandas as pd
import dask.dataframe as dd
from dask import delayed
import dask
from numba import njit


def get_quote_data(
    ticker_symbol: str,
    start_time: str,
    end_time: str,
    period: str,
    adjust: str = None,
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
    adjust : str
        复权方式, by default None
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
    result_df = result_df.sort_values(by="trade_time")
    result_df = process_adjust(
        df=result_df, ticker_symbol=ticker_symbol, adjust=adjust, data_dir=data_dir
    )
    return result_df


def process_adjust(
    df: pd.DataFrame, ticker_symbol: str, adjust: str = None, data_dir="D:/qmt_datadir"
) -> pd.DataFrame:
    """
    处理复权数据。


    Parameters
    ----------
    df : pd.DataFrame
        原始数据
    ticker_symbol : str
        代码名称
    adjust : str
        复权方式, by default None， hfq后复权，qfq前复权， None不复权

    data_dir : str, optional
        _description_, by default "D:/qmt_datadir"

    Returns
    -------
    pd.DataFrame
        复权后的数据

    Raises
    ------
    ValueError
        如果复权方式不支持，则抛出此异常
    """
    df = df.copy()
    if df.empty:
        return None
    if adjust not in [None, "hfq", "qfq"]:
        raise ValueError(f"复权方式不支持: {adjust}")

    if adjust:
        adjust_factor = get_divid_factors(ticker_symbol, data_dir=data_dir)
        if adjust_factor is None:
            return df
        adjust_factor.index = adjust_factor.trade_time.dt.date
        df.index = df.trade_time.dt.date

        @delayed
        def adjust_column(col):
            if adjust == "hfq":
                return process_backward(df[[col]], adjust_factor)
            elif adjust == "qfq":
                return process_forward(df[[col]], adjust_factor)
            return df[[col]]

        delayed_columns = [
            adjust_column(col) for col in ["open", "high", "low", "close"]
        ]
        adjusted_columns = dask.compute(*delayed_columns)

        for col, adjusted_col in zip(
            ["open", "high", "low", "close"], adjusted_columns
        ):
            df[col] = adjusted_col

        df.reset_index(drop=True, inplace=True)
    return df


def get_divid_factors(
    ticker_symbol: str,
    data_dir: str = "D:/qmt_datadir",
) -> pd.DataFrame:
    adjust_factor_path = os.path.join(
        data_dir, "adjust_factor", f"{ticker_symbol}.parquet"
    )
    if not os.path.exists(adjust_factor_path):
        return None
    return pd.read_parquet(adjust_factor_path)


@njit
def calc_front_numba(v, interest, allotPrice, allotNum, stockBonus, stockGift):
    return (v - interest + allotPrice * allotNum) / (
        1 + allotNum + stockBonus + stockGift
    )


@njit
def calc_back_numba(v, interest, allotPrice, allotNum, stockBonus, stockGift):
    return (
        v * (1 + stockGift + stockBonus + allotNum) + interest - allotNum * allotPrice
    )


def process_forward(quote_datas, divid_datas):
    quote_datas = quote_datas.copy()
    divid_datas = divid_datas.sort_index()

    for i in range(len(quote_datas)):
        date = quote_datas.index[i]
        mask = divid_datas.index <= date
        if mask.any():
            latest_divid = divid_datas[mask].iloc[-1]
            quote_datas.iloc[i] = calc_front_numba(
                quote_datas.iloc[i].values[0],
                latest_divid["interest"],
                latest_divid["allotPrice"],
                latest_divid["allotNum"],
                latest_divid["stockBonus"],
                latest_divid["stockGift"],
            )

    return quote_datas


def process_backward(quote_datas, divid_datas):
    quote_datas = quote_datas.copy()
    divid_datas = divid_datas.sort_index()

    for i in range(len(quote_datas)):
        date = quote_datas.index[i]
        mask = divid_datas.index <= date
        if mask.any():
            latest_divid = divid_datas[mask].iloc[-1]
            quote_datas.iloc[i] = calc_back_numba(
                quote_datas.iloc[i].values[0],
                latest_divid["interest"],
                latest_divid["allotPrice"],
                latest_divid["allotNum"],
                latest_divid["stockBonus"],
                latest_divid["stockGift"],
            )

    return quote_datas


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
    from xtquant import xtdata

    # # Example usage
    s = "159941.SZ"

    # dd = xtdata.get_divid_factors(s)

    # # 复权计算用于处理价格字段
    # field_list = ["open", "high", "low", "close"]
    # datas_ori = xtdata.get_market_data(field_list, [s], "1d", dividend_type="none")[
    #     "close"
    # ].T
    # print(datas_ori)

    # # 等比前复权
    # datas_forward_ratio = process_forward_ratio(datas_ori, dd)
    # print("datas_forward_ratio", datas_forward_ratio)

    # # 等比后复权
    # datas_backward_ratio = process_backward_ratio(datas_ori, dd)
    # print("datas_backward_ratio", datas_backward_ratio)

    # # 前复权
    # datas_forward = process_forward(datas_ori, dd)
    # print("datas_forward", datas_forward)

    # 后复权
    # datas_backward = process_backward(datas_ori, dd)
    # print("datas_backward", datas_backward)
    df = get_quote_data(
        ticker_symbol="159941.SZ",
        start_time="20240202",
        end_time="20240601",
        period="1d",
        adjust=None,
        data_dir="D:/qmt_datadir",
    )
    print(df)
