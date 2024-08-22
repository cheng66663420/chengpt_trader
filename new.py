import os
import pandas as pd


def main():
    # 设置输入和输出路径
    input_root_path = "d:/qmt_datadir/"
    output_root_path = "G:/qmt_datadir/"
    periods = ["1m", "5m", "1d"]
    # 创建Dask客户端
    # 这将使用所有可用的本地核心
    for period in periods:
        input_path = os.path.join(input_root_path, period)
        output_path = os.path.join(output_root_path, period)
        os.makedirs(output_path, exist_ok=True)
        subdirs = [
            d
            for d in os.listdir(input_path)
            if os.path.isdir(os.path.join(input_path, d))
        ]
        groups = [subdirs[i : i + 50] for i in range(0, len(subdirs), 50)]
        for num, group in enumerate(groups):
            dfs = []
            print(f"合并第{num}组开始！")
            for ticker in group:
                path = os.path.join(input_path, ticker)
                files = os.listdir(path)
                for file in files:
                    dfs.append(pd.read_parquet(os.path.join(path, file)))

            merged_df = pd.concat(dfs)
            merged_df.to_parquet(
                os.path.join(output_path, f"merged_{num}.parquet"), engine="pyarrow"
            )
            print(f"合并第{num}组完成！")

        print("合并完成！")


if __name__ == "__main__":
    main()
