import datetime
from qmt_data import QmtData
import os
import pandas as pd

if __name__ == "__main__":
    today = datetime.datetime.now().strftime("%Y%m%d")
    start_time = "20241101"
    end_time = today
    qmt_data = QmtData()
    ticker_df = qmt_data.get_tickers()
    stock_list = ticker_df["ticker"].tolist()
    stock_list.sort()
    today = datetime.datetime.now().strftime("%Y%m%d")
    start_time = "20241101"
    qmt_data = QmtData()
    ticker_df = qmt_data.get_tickers()
    stock_list = ticker_df["ticker"].tolist()
    stock_list.sort()
    today = datetime.datetime.now().strftime("%Y%m%d")
    start_time = "20241101"
    end_time = today
    qmt_data.remove_qmt_datadir()

    # qmt_data.download_adjust_factor(
    #     stock_list=stock_list, start_time=start_time, end_time=end_time
    # )
    # for period in ["1m", "5m", "1d"]:
    #     qmt_data.download_data(
    #         start_time=start_time,
    #         end_time=end_time,
    #         period=period,
    #         stock_list=stock_list,
    #     )
    #     record = qmt_data.write_to_ftr_parallel(
    #         stock_list=stock_list,
    #         start_time=start_time,
    #         end_time=end_time,
    #         period=period,
    #     )
    #     record_df = pd.DataFrame(record)
    #     os.makedirs(
    #         "D:/qmt_datadir/logging/",
    #         exist_ok=True,
    #     )
    #     record_df.to_excel(
    #         f"D:/qmt_datadir/logging/{end_time}_record_{period}.xlsx",
    #         index=False,
    #         engine="openpyxl",
    #     )
