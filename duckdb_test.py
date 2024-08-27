import duckdb
import os
from tqdm import tqdm


con = duckdb.connect(database="f:\\qmt_datadir\\quotes.db", read_only=False)

file_list = os.listdir("f:\\qmt_datadir\\1m")

for i, file in enumerate(tqdm(file_list, leave=True)):
    tqdm.write(f"处理文件: {file}")
    if i == 0:
        con.execute(
            f"CREATE TABLE quotes_1m AS SELECT * FROM read_parquet('f:\\qmt_datadir\\1m\\{file}');"
        )
    con.execute(f"COPY quotes_1m from 'f:\\qmt_datadir\\1m\\{file}' (FORMAT PARQUET);")
    tqdm.write(f"处理文件: {file}完成")
