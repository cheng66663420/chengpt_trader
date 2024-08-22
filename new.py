import pandas as pd
import dask
from dask.distributed import Client
from dask.diagnostics import ProgressBar
import os
from tqdm import tqdm
import dask.dataframe as dd


@dask.delayed
def read_file(filename):
    return pd.read_parquet(filename)


@dask.delayed
def write_file(file_path, period, file):
    temp_path = os.path.join(file_path, period, file)
    print(temp_path)
    parquet_list = [os.path.join(temp_path, file) for file in os.listdir(temp_path)]

    df_list = [read_file(file) for file in parquet_list]

    # 计算延迟对象
    df_list = dask.compute(*df_list)
    df = pd.concat(df_list)
    df.sort_values(by="trade_time", inplace=True)
    save_path = f"G:/qmt_datadir/{period}"
    os.makedirs(save_path, exist_ok=True)
    df.to_parquet(f"{save_path}/{file}.parquet", compression="snappy")
    return None


if __name__ == "__main__":
    client = Client()
    root_path = "D:/qmt_datadir/"
    period_list = ["1m", "5m", "1d"]
    result = []
    for period in tqdm(period_list):
        file_path = os.path.join(root_path, period)
        file_list = list(os.listdir(file_path))
        for file in tqdm(file_list, leave=True):
            result.append(write_file(root_path, period, file))
    dask.compute(*result)
