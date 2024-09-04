import numpy as np

from numpy.lib.stride_tricks import as_strided as strided
import pandas as pd
import backtrader as bt


def rolling_window(a: np.array, window: int):
    "生成滚动窗口，以三维数组的形式展示"
    shape = a.shape[:-1] + (a.shape[-1] - window + 1, window)
    strides = a.strides + (a.strides[-1],)
    return strided(a, shape=shape, strides=strides)


def numpy_rolling_regress(x1, y1, window: int = 18, array: bool = False):
    "在滚动窗口内进行，每个矩阵对应进行回归"
    x_series = np.array(x1)
    y_series = np.array(y1)
    # 创建一个一维数组
    dd = x_series
    x = rolling_window(dd, window)
    yT = rolling_window(y_series, window)
    y = np.array([i.reshape(window, 1) for i in yT])
    ones_vector = np.ones((1, x.shape[1]))
    XT = np.stack([np.vstack([ones_vector, row]) for row in x])  # 加入常数项
    X = np.array([matrix.T for matrix in XT])  # 以行数组表示
    reg_result = np.linalg.pinv(XT @ X) @ XT @ y  # 线性回归公示

    if array:
        return reg_result
    else:
        return _numpy_rolling_regress_helper(x_series, reg_result, x1)


# TODO Rename this here and in `numpy_rolling_regress`
def _numpy_rolling_regress_helper(x_series, reg_result, x1):
    frame = pd.DataFrame()
    result_const = np.zeros(x_series.shape[0])
    const = reg_result.reshape(-1, 2)[:, 0]
    result_const[-const.shape[0] :] = const
    frame["const"] = result_const
    frame.index = x1.index
    for i in range(1, reg_result.shape[1]):
        result = np.zeros(x_series.shape[0])
        beta = reg_result.reshape(-1, 2)[:, i]
        result[-beta.shape[0] :] = beta
        frame[f"factor{i}"] = result
    return frame


def cal_rsrs(low, high, N: int = 18, M: int = 600):
    beta_series = numpy_rolling_regress(low, high, window=N, array=True)
    beta = beta_series.reshape(-1, 2)[:, 1]

    beta_rollwindow = rolling_window(beta, M)
    beta_mean = np.mean(beta_rollwindow, axis=1)
    beta_std = np.std(beta_rollwindow, axis=1)
    zscore = (beta[M - 1 :] - beta_mean) / beta_std
    len_to_pad = len(low) - len(zscore)
    pad = [np.nan for _ in range(len_to_pad)]
    pad.extend(zscore)
    zscore = np.array(pad)

    len_to_pad = len(low) - len(beta)
    pad = [np.nan for _ in range(len_to_pad)]
    pad.extend(beta)
    beta = np.array(pad)
    return zscore, beta


class RSRS(bt.Indicator):
    lines = ("rsrs",)
    params = (
        ("N", 18),
        ("M", 600),
    )

    def __init__(self):
        self.addminperiod(self.params.M)

    def next(self):
        low = np.array([self.data.low[-i] for i in range(self.params.M)])
        high = np.array([self.data.high[-i] for i in range(self.params.M)])
        self.lines.rsrs[0] = cal_rsrs(low, high, self.params.N, self.params.M)[-1]


class RSRSStrategy(bt.Strategy):
    params = (
        ("N", 18),
        ("M", 600),
    )

    def __init__(self):
        self.rsrs = RSRS(self.data)

    def next(self):
        print(self.data.close)
        print(self.rsrs[0])


if __name__ == "__main__":
    import akshare as ak
    from trader import run_strategy

    df = ak.stock_us_hist(
        symbol="105.TQQQ", start_date="20100101", end_date="20240903", adjust="hfq"
    )
    # df = ak.fund_etf_hist_em(
    #     symbol="159941", start_date="20100101", end_date="20240902", adjust="hfq"
    # )
    df.rename(
        columns={
            "日期": "trade_time",
            "开盘": "open",
            "收盘": "close",
            "最高": "high",
            "最低": "low",
            "成交量": "volume",
            "成交额": "amount",
        },
        inplace=True,
    )
    df["trade_time"] = pd.to_datetime(df["trade_time"])
    # df = df.query("trade_time >= '20170101'")
    # # 添加数据
    # df = get_quote_data(
    #     ticker_symbol="159941.SZ",
    #     start_time="20140101",
    #     end_time="20240819",
    #     period="1d",
    # )
    cols = ["open", "high", "low", "close", "volume", "amount"]
    df.set_index("trade_time", inplace=True)
    zsocre, rsrs = cal_rsrs(df.low, df.high, 18, 600)
    df["zscore"] = zsocre
    df["rsrs"] = rsrs
    # run_strategy(df, [RSRSStrategy], if_plot=False)
