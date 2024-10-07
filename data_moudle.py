import os
import pandas as pd
import dask.dataframe as dd
from dask import delayed
import dask
from numba import njit
import duckdb


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

    # 连接到 DuckDB
    start_time = pd.to_datetime(start_time)
    end_time = pd.to_datetime(end_time)

    con = duckdb.connect()
    duck_query = f"""
    SELECT
        trade_time,
        ticker_symbol,
        open,
        high,
        low,
        close,
        volume,
        amount,
        settelement_price,
        open_interest,
        preclose,
        suspend_flag
    FROM
        read_parquet('{file_path}/*.parquet', union_by_name=True)

    WHERE
        1=1
        and trade_time between '{start_time}' and '{end_time}'
        order by trade_time
    """
    result_df = con.execute(duck_query).df()
    result_df = process_adjust(
        df=result_df,
        ticker_symbol=ticker_symbol,
        start_time=start_time,
        end_time=end_time,
        adjust=adjust,
        data_dir=data_dir,
    )
    return result_df


def process_adjust(
    df: pd.DataFrame,
    ticker_symbol: str,
    start_time: str,
    end_time: str,
    adjust: str = None,
    data_dir="D:/qmt_datadir",
) -> pd.DataFrame:
    """
    处理复权数据。


    Parameters
    ----------
    df : pd.DataFrame
        原始数据
    ticker_symbol : str
        代码名称
    start_time : str
        开始时间
    end_time : str
        结束时间
    adjust : str
        复权方式, by default None， hfq后复权，qfq前复权， None不复权

    data_dir : str, optional
        数据存储位置, by default "D:/qmt_datadir"

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
        adjust_factor = get_divid_factors(
            ticker_symbol=ticker_symbol,
            start_time=start_time,
            end_time=end_time,
            data_dir=data_dir,
        )
        if adjust_factor is None:
            return df
        if adjust == "hfq":
            return process_backward(df, adjust_factor)
        elif adjust == "qfq":
            return process_forward(df, adjust_factor)
        df.reset_index(drop=True, inplace=True)
    return df


def get_divid_factors(
    ticker_symbol: str,
    start_time: str = "20050101",
    end_time: str = None,
    data_dir: str = "D:/qmt_datadir",
) -> pd.DataFrame:
    if end_time is None:
        end_time = pd.to_datetime("today")
    start_time = pd.to_datetime(start_time)
    file_path = os.path.join(data_dir, "adjust_factor")

    con = duckdb.connect()
    duck_query = f"""
    SELECT 
        *
    FROM 
        read_parquet('{file_path}/*.parquet')

    WHERE 
        1=1
        AND time between '{start_time}' and '{end_time}'
        AND ticker_symbol = '{ticker_symbol}'
    order by 
        time
    """
    result_df = con.execute(duck_query).df()
    return None if result_df.empty else result_df


def calc_front_numba(v, interest, allotPrice, allotNum, stockBonus, stockGift):
    return (v - interest + allotPrice * allotNum) / (
        1 + allotNum + stockBonus + stockGift
    )


def calc_back_numba(v, interest, allotPrice, allotNum, stockBonus, stockGift):
    return (
        v * (1 + stockGift + stockBonus + allotNum) + interest - allotNum * allotPrice
    )


def process_forward(quote_datas: pd.DataFrame, divid_datas: pd.DataFrame):
    quote_datas = quote_datas.copy()
    quote_datas.index = quote_datas.trade_time.dt.date
    divid_datas = divid_datas.copy()
    divid_datas.index = divid_datas.time.dt.date
    divid_datas.drop(columns=["time", "ticker_symbol"], inplace=True)
    cols = quote_datas.columns
    df = quote_datas.merge(divid_datas, how="left", left_index=True, right_index=True)
    df.fillna(0, inplace=True)
    for col in ["open", "high", "low", "close"]:
        df[col] = calc_front_numba(
            df[col],
            df["interest"],
            df["allotPrice"],
            df["allotNum"],
            df["stockBonus"],
            df["stockGift"],
        )
    return df[cols].reset_index(drop=True)


def process_backward(quote_datas, divid_datas):
    quote_datas = quote_datas.copy()
    quote_datas.index = quote_datas.trade_time.dt.date
    divid_datas = divid_datas.copy()
    divid_datas.index = divid_datas.time.dt.date
    divid_datas.drop(columns=["time", "ticker_symbol"], inplace=True)
    cols = quote_datas.columns
    df = quote_datas.merge(divid_datas, how="left", left_index=True, right_index=True)
    df.fillna(0, inplace=True)

    for col in ["open", "high", "low", "close"]:
        df[col] = calc_back_numba(
            df[col],
            df["interest"],
            df["allotPrice"],
            df["allotNum"],
            df["stockBonus"],
            df["stockGift"],
        )
    return df[cols].reset_index(drop=True)


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
    df = get_quote_data(
        ticker_symbol="159941.SZ",
        start_time="20000101",
        end_time="20240924",
        period="1m",
        adjust="hfq",
        data_dir="D:/qmt_datadir",
    )
    df.to_excel("d:/159941.xlsx", index=False)
