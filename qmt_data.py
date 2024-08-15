from xtquant import xtdata
import pandas as pd
from tqdm import tqdm
import os
import akshare as ak
from dask import delayed, compute

tool_trade_date_hist_sina_df = ak.tool_trade_date_hist_sina()
TRADE_DATE_LIST = tool_trade_date_hist_sina_df["trade_date"].tolist()


def check_missing_mintues_decorator(func):
    def wrapper(*args, **kwargs):
        df = func(*args, **kwargs)
        temp_ = df.groupby("date")["date"].count()
        check_ = temp_[temp_ < 240]
        if check_.shape[0] > 0:
            raise ValueError(f"分钟线不满足240或241: {check_.index}")
        return df

    return wrapper


def check_missing_dates_decorator(func):
    def wrapper(*args, **kwargs):
        df = func(*args, **kwargs)
        if df.empty:
            raise ValueError(f"{df.ticker_symbol.tolist()[0]}数据为空")
        trade_date_list = df.date.unique().tolist()
        trade_date_list.sort()
        temp_check_list = TRADE_DATE_LIST[
            TRADE_DATE_LIST.index(trade_date_list[0]) : TRADE_DATE_LIST.index(
                trade_date_list[-1]
            )
            + 1
        ]
        minutes_list = list(set(temp_check_list) - set(trade_date_list))
        if minutes_list:
            raise ValueError(f"缺失数据日期: {minutes_list}")
        return df

    return wrapper


class QmtData:
    """
    qmt自动登录
    """

    def __init__(self):
        xtdata.enable_hello = False
        xtdata.connect()

    def get_tickers(self, needed_sector_list: list = None):
        if needed_sector_list is None:
            needed_sector_list = [
                "沪深A股",
                "沪深B股",
                "沪深基金",
                "沪深指数",
                "沪深转债",
            ]
        ticker_list = [
            [temp, sector]
            for sector in needed_sector_list
            for temp in xtdata.get_stock_list_in_sector(sector)
        ]
        return pd.DataFrame(ticker_list, columns=["ticker", "sector"])

    def download_data(
        self,
        start_time: str,
        end_time: str,
        stock_list: list = None,
        period: str = "1m",
    ):
        if stock_list is None:
            stock_list = self.get_tickers()["ticker"].tolist()

        for stock_code in tqdm(stock_list, leave=True):
            xtdata.download_history_data(
                stock_code=stock_code,
                start_time=start_time,
                end_time=end_time,
                period=period,
            )
            tqdm.write(f"Downloaded data for {stock_code}")

    @check_missing_dates_decorator
    @check_missing_mintues_decorator
    def get_local_data(
        self,
        start_time: str,
        end_time: str,
        stock_code: str,
        period: str = "1m",
    ):
        temp_dict = xtdata.get_local_data(
            stock_list=[stock_code],
            start_time=start_time,
            end_time=end_time,
            period=period,
            field_list=[
                "open",
                "high",
                "low",
                "close",
                "volume",
                "amount",
                "settelementPrice",
                "openInterest",
                "preClose",
                "suspendFlag",
            ],
        )
        result = []
        for key, df in temp_dict.items():
            df_temp = df.copy().reset_index()
            df_temp.rename(columns={"index": "trade_time"}, inplace=True)
            df_temp["trade_time"] = pd.to_datetime(df_temp["trade_time"])
            df_temp["ticker_symbol"] = key
            df_temp["date"] = df_temp["trade_time"].dt.date
            df_temp["year_month"] = df_temp["trade_time"].dt.strftime("%Y%m")
            result.append(df_temp)
        result = pd.concat(result)

        if result.empty:
            raise ValueError(f"{stock_code}数据为空")
        rename_col = {
            "settelementPrice": "settelement_price",
            "openInterest": "open_interest",
            "preClose": "preclose",
            "suspendFlag": "suspend_flag",
        }
        result.rename(columns=rename_col, inplace=True)
        return result

    def write_to_ftr(
        self,
        start_time: str,
        end_time: str,
        stock_code: str,
        period: str = "1m",
        root_path: str = "D:/qmt_datadir/",
    ):
        col_need = [
            "trade_time",
            "ticker_symbol",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "amount",
            "settelement_price",
            "open_interest",
            "preclose",
            "suspend_flag",
        ]

        try:
            df = self.get_local_data(
                start_time=start_time,
                end_time=end_time,
                stock_code=stock_code,
                period=period,
            )
            maked_path = os.path.join(root_path, period, stock_code)
            os.makedirs(maked_path, exist_ok=True)
            for year_month, df_grouped in df.groupby("year_month"):
                file_path = os.path.join(maked_path, f"{year_month}.parquet")
                check_conditin = os.path.exists(file_path)
                df_result = df_grouped[col_need]
                if check_conditin:
                    df_old = pd.read_parquet(file_path)
                    df_result = pd.concat([df_old, df_result])
                    df_result.sort_values(by=["trade_time"], inplace=True)
                    df_result.drop_duplicates(
                        subset=["trade_time", "ticker_symbol"], inplace=True
                    )
                df_result.to_parquet(file_path, compression="snappy")
            start_date_rocorded = df.date.min().strftime("%Y%m%d")
            end_date_rocorded = df.date.max().strftime("%Y%m%d")
            record = {
                "ticker": stock_code,
                "start_date": start_date_rocorded,
                "end_date": end_date_rocorded,
                "if_complete": True,
            }
        except Exception as e:
            print(f"Error occurred while processing {stock_code}: {e}")
            record = {"ticker": stock_code, "if_complete": False, "error": str(e)}
        return record

    def write_to_ftr_parallel(
        self,
        start_time: str,
        end_time: str,
        stock_list: list = None,
        period: str = "1m",
    ):
        if stock_list is None:
            stock_list = self.get_tickers()["ticker"].tolist()

        # Wrap the function calls with dask.delayed
        delayed_tasks = [
            delayed(self.write_to_ftr)(
                stock_code=ticker,
                start_time=start_time,
                end_time=end_time,
                period=period,
            )
            for ticker in stock_list
        ]

        # Execute the tasks in parallel with tqdm for progress tracking

        return list(
            tqdm(
                compute(*delayed_tasks, scheduler="processes"),
                total=len(stock_list),
            )
        )


if __name__ == "__main__":
    qmt_data = QmtData()
    ticker_df = qmt_data.get_tickers()
    stock_list = ticker_df["ticker"].tolist()
    start_time = "20240810"
    end_time = "20240815"
    period = "1m"
    # qmt_data.download_data(
    #     start_time=start_time,
    #     end_time=end_time,
    #     period=period,
    # )
    record = qmt_data.write_to_ftr_parallel(
        start_time=start_time, end_time=end_time, period=period
    )
    record_df = pd.DataFrame(record)
    record_df.to_excel(
        f"D:/qmt_datadir/logging/{end_time}_record__{period}.xlsx", index=False
    )
