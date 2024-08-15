from xtquant import xtdata
import pandas as pd
from tqdm import tqdm
from datetime import datetime
import dask.dataframe as dd


class QmtData:
    """
    qmt自动登录
    """

    def __init__(self):
        xtdata.connect()
        xtdata.enable_hello = False

    def get_tickers(self, needed_sector_list: list = None):
        if needed_sector_list is None:
            needed_sector_list = [
                "沪深A股",
                "沪深B股",
                "沪深基金",
                "沪深指数",
                "沪深转债",
            ]
        ticker_list = []
        for sector in needed_sector_list:
            temp_ticker = xtdata.get_stock_list_in_sector(sector)
            for temp in temp_ticker:
                ticker_list.append([temp, sector])

        ticker_df = pd.DataFrame(ticker_list, columns=["ticker", "sector"])
        return ticker_df

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

    def get_local_data(
        self,
        start_time: str,
        end_time: str,
        stock_code: str,
        period: str = "1m",
    ):

        stock_data_dict = xtdata.get_local_data(
            stock_list=[stock_code],
            start_time=start_time,
            end_time=end_time,
            period=period,
        )
        dask_dataframes = []
        for code, stock_data in stock_data_dict.items():
            temp_data = stock_data.copy()
            temp_data["ticker_symbol"] = code
            temp_data["time"] = temp_data["time"].apply(
                lambda x: datetime.fromtimestamp(x / 1000)
            )
            dask_dataframes.append(dd.from_pandas(temp_data, npartitions=1))

        # 合并所有 Dask DataFrame
        combined_data = dd.concat(dask_dataframes)

        # 计算结果并转换为 Pandas DataFrame
        result = combined_data.compute()

        return result


if __name__ == "__main__":
    qmt_date = QmtData()
    ticker_df = qmt_date.get_tickers()
    stock_list = ticker_df["ticker"].tolist()

    df = qmt_date.get_local_data("20240731", "20240731", stock_code="000001.SZ")
    print(df)
