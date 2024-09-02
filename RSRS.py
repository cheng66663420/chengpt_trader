import numpy as np

from numpy.lib.stride_tricks import as_strided as strided
import pandas as pd


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


def RSRS(low: pd.Series, high: pd.Series, N: int = 18, M: int = 600):
    beta_series = numpy_rolling_regress(low, high, window=N, array=True)
    beta = beta_series.reshape(-1, 2)[:, 1]

    beta_rollwindow = rolling_window(beta, M)
    beta_mean = np.mean(beta_rollwindow, axis=1)
    beta_std = np.std(beta_rollwindow, axis=1)
    zscore = (beta[M - 1 :] - beta_mean) / beta_std
    len_to_pad = len(low.index) - len(zscore)
    # print(len_to_pad)
    pad = [np.nan for i in range(len_to_pad)]
    pad.extend(zscore)
    zscore = pd.Series(pad, index=low.index)

    len_to_pad = len(low.index) - len(beta)
    pad = [np.nan for i in range(len_to_pad)]
    pad.extend(beta)
    beta = pd.Series(pad, index=low.index)

    return beta
