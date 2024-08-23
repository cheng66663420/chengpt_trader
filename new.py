import os
import pandas as pd
from concurrent.futures import ProcessPoolExecutor, as_completed
import tqdm
from tqdm import tqdm


def helper_function(ticker, df_group, output_path, cols):
    df_group["year_month"] = df_group["trade_time"].dt.strftime("%Y%m")
    ticker_path = os.path.join(output_path, ticker)
    os.makedirs(ticker_path, exist_ok=True)
    for year_month, df_month in df_group.groupby("year_month"):
        df_to_save = df_month[cols]
        output_file_path = os.path.join(ticker_path, f"{year_month}.parquet")
        df_to_save.to_parquet(
            output_file_path,
            engine="pyarrow",
            compression="snappy",
            index=False,
        )
        tqdm.write(f"拆分{ticker} {year_month} 完成！")
    tqdm.write(f"拆分{ticker} 完成！")


def split_sigle_parquet_file(input_path, output_path):
    cols = None

    tqdm.write(f"读取文件: {input_path}")
    df = pd.read_parquet(input_path)
    if cols is None:
        cols = df.columns.tolist()
    tqdm.write(f"拆分{input_path} 开始！")
    with ProcessPoolExecutor(max_workers=10) as executor:
        futures = [
            executor.submit(
                helper_function,
                ticker,
                df_group,
                output_path,
                cols,
            )
            for ticker, df_group in df.groupby("ticker_symbol")
        ]
        for future in tqdm(as_completed(futures), leave=True):
            try:
                future.result()
            except Exception as e:
                tqdm.write(f"处理周期时发生错误: {e}")
    tqdm.write(f"拆分{input_path} 完成！")


def split_parquet_multi_files():
    input_root_path = "g:\\qmt_datadir"
    output_root_path = "d:\\qmt_datadir"
    periods = ["1m"]

    for period in periods:
        os.makedirs(os.path.join(output_root_path, period), exist_ok=True)
        tqdm.write(f"开始处理周期: {period}")
        tqdm.write(f"读取目录: {os.path.join(input_root_path, period)}")
        tqdm.write(f"输出目录: {os.path.join(output_root_path, period)}")
        tqdm.write("开始拆分文件！")
        for i in os.listdir(os.path.join(input_root_path, period)):
            input_path = os.path.join(input_root_path, period, i)
            output_path = os.path.join(output_root_path, period)

            split_sigle_parquet_file(input_path, output_path)


if __name__ == "__main__":
    split_parquet_multi_files()
