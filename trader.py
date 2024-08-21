import backtrader as bt

from data_moudle import get_quote_data
import pandas as pd
import akshare as ak
import numpy as np


class DrawdownStrategy(bt.Strategy):
    params = (
        ("lookback_period", 888),  # 滚动回撤计算周期
        ("drawdown_threshold_1", -10.0),  # 回撤阈值1
        ("drawdown_threshold_2", -15.0),  # 回撤阈值2
        ("drawdown_threshold_3", -20.0),  # 回撤阈值3
        ("position_size_1", 0.7),  # 仓位比例1
        ("position_size_2", 0.8),  # 仓位比例2
        ("position_size_3", 0.9),  # 仓位比例3
        ("position_size_high", 0.6),  # 新高仓位比例
    )

    def __init__(self):
        self.dataclose = self.datas[0].close
        self.order = None
        self.buy_signal = False

    def next(self):
        if self.order:
            return

        if len(self.datas[0]) < self.params.lookback_period:
            self.log("Not enough data to calculate drawdown")
            return

        # 计算滚动高点和当前回撤
        lookback_high = max(self.datas[0].close.get(size=len(self)))

        current_close = self.datas[0].close[0]
        drawdown = (current_close - lookback_high) / lookback_high * 100

        # 获取当前持仓
        position = self.broker.getposition(self.datas[0])

        # 当前持仓的价值
        current_value = position.size * self.datas[0].close[0]

        # 账户总价值
        total_value = self.broker.getvalue()
        # 当前百分比仓位
        if total_value > 0:
            current_percentage = current_value / total_value
        else:
            current_percentage = 0
        # self.log(f"Current Position Size: {current_percentage*100:.2f}%")

        if self.buy_signal is False:
            self.buy_signal = self.params.position_size_high

        # 根据回撤决定仓位
        if drawdown <= self.params.drawdown_threshold_3:
            self.buy_signal = self.params.position_size_3
        elif drawdown <= self.params.drawdown_threshold_2:
            self.buy_signal = self.params.position_size_2
        elif drawdown <= self.params.drawdown_threshold_1:
            self.buy_signal = self.params.position_size_1
        elif drawdown == 0:
            self.buy_signal = self.params.position_size_high

        # 计算目标仓位与当前仓位的差异
        target_position_size = self.buy_signal
        position_change = abs(target_position_size - current_percentage)
        self.log(
            f"收盘价{current_close};最高价{lookback_high};Current Drawdown: {drawdown:.2f}%"
        )
        # 如果仓位变动小于5%，则不调整仓位
        if position_change < 0.1:
            return  # 不调整仓位

        # 如果有买入信号，进行下单
        if self.buy_signal:
            self.order_target_percent(target=self.buy_signal)

    def notify_order(self, order):
        if order.status in [order.Submitted, order.Accepted]:
            return

        if order.status in [order.Completed]:
            if order.isbuy():
                self.log(
                    f"BUY EXECUTED, Size: {order.size}, Price: {order.executed.price:.2f}"
                )
            elif order.issell():
                self.log(
                    f"SELL EXECUTED, Size: {order.size}, Price: {order.executed.price:.2f}"
                )

        self.order = None

    def log(self, txt, dt=None):
        dt = dt or self.datas[0].datetime.date(0)
        print(f"{dt.isoformat()}, {txt}")


class DynamicGridStrategy(bt.Strategy):
    params = (
        ("initial_position", 0.7),  # 初始仓位60%
        ("up_threshold", 0.1),  # 上涨10%
        ("down_threshold", -0.05),  # 下跌5%
        ("up_adjustment", 0.1),  # 上涨时调整10%
        ("down_adjustment", 0.05),  # 下跌时调整5%
        ("min_position", 0.7),  # 最低仓位50%
        ("max_position", 1),  # 最高仓位100%
        ("drawdown_threshold", -0.11),  # 回撤阈值10%
    )

    def __init__(self):
        self.dataclose = self.datas[0].close
        self.order = None
        self.position_size = self.params.initial_position
        self.last_price = self.dataclose[0]
        self.pct_return = []
        self.first_drawndown_break_flag = True
        self.init = True

    def next(self):
        if self.order:
            return

        if self.init:
            self.order_target_percent(target=self.params.initial_position)
            self.init = False

        pct_return = np.log(self.dataclose[0] / self.dataclose[-1])
        self.pct_return.append(pct_return)
        # 计算滚动高点和当前回撤
        lookback_high = max(self.datas[0].close.get(size=len(self)))
        current_close = self.datas[0].close[0]
        drawdown = (current_close - lookback_high) / lookback_high

        change_percent_position = 0
        current_price = self.dataclose[0]
        price_change = (current_price - self.last_price) / self.last_price
        self.position_size = self.get_current_percentage_position()
        # if self.position_size == 0:
        #     self.order_target_percent(target=self.position_size)

        # 价格变动涨幅大于上涨阈值,减仓
        if price_change >= self.params.up_threshold:
            if drawdown <= -0.2:
                change_percent_position = 0
                # self.last_price = current_price
            else:
                change_percent_position = -self.params.up_adjustment
                self.last_price = current_price
            self.first_drawndown_break_flag = drawdown == 0
        elif (
            price_change <= self.params.down_threshold
            and drawdown <= self.params.drawdown_threshold
        ):
            if self.first_drawndown_break_flag:
                self.first_drawndown_break_flag = False
                change_percent_position = (
                    np.floor(abs(drawdown / self.params.down_threshold))
                    * self.params.down_adjustment
                )
            else:
                change_percent_position = self.params.down_adjustment
            self.last_price = current_price
        # 仓位变动大于0.02%，则调整仓位
        if abs(change_percent_position) > 0.02:
            self.position_size += change_percent_position
            self.position_size = max(
                self.params.min_position,
                min(self.params.max_position, self.position_size),
            )
            self.order_target_percent(target=self.position_size)

            self.log(
                f"""回撤率{drawdown*100:.2f}%;仓位变动值{change_percent_position*100:.2f}%;结束仓位{self.position_size*100:.2f}%
                """
            )

    def get_current_percentage_position(self):
        # 获取当前持仓
        position = self.broker.getposition(self.datas[0])
        # 当前持仓的价值
        current_value = position.size * self.dataclose[0]
        # 账户总价值
        total_value = self.broker.getvalue()
        # 当前百分比仓位
        return current_value / total_value if total_value > 0 else 0

    def notify_order(self, order):
        if order.status in [order.Submitted, order.Accepted]:
            return

        if order.status in [order.Completed]:
            if order.isbuy():
                self.log(
                    f"BUY EXECUTED, Size: {order.executed.size}, Price: {order.executed.price:.2f}"
                )
            elif order.issell():
                self.log(
                    f"SELL EXECUTED, Size: {order.executed.size}, Price: {order.executed.price:.2f}"
                )

        self.order = None

    def log(self, txt, dt=None):
        dt = dt or self.datas[0].datetime.date(0)
        print(f"{dt.isoformat()}, {txt}")


def run_strategy(df: pd.DataFrame):
    cerebro = bt.Cerebro()
    # df = pd.read_excel("f:/H20269.CSI.xlsx")

    data = bt.feeds.PandasData(dataname=df)
    cerebro.adddata(data)

    # 添加策略
    cerebro.addstrategy(DynamicGridStrategy)

    # 设置初始资金
    cerebro.broker.setcash(100000.0)
    # 设置交易手续费
    cerebro.broker.setcommission(commission=1 / 10000)
    cerebro.broker.set_slippage_perc(0.001)
    # 添加分析器
    # cerebro.addanalyzer(bt.analyzers.SharpeRatio, _name="sharpe")
    # cerebro.addanalyzer(bt.analyzers.DrawDown, _name="drawdown")
    # cerebro.addanalyzer(bt.analyzers.AnnualReturn, _name="annual_ret")
    # cerebro.addanalyzer(bt.analyzers.TimeDrawDown, _name="Calmar")

    # 运行回测
    results = cerebro.run()

    for analyzer in results[0].analyzers:
        analyzer.print()
    cerebro.plot()
    # 打印最终资金
    print(f"Final Portfolio Value: {cerebro.broker.getvalue():.2f}")


if __name__ == "__main__":
    df = ak.fund_etf_hist_em(
        symbol="159941", start_date="20100101", end_date="20240821", adjust="hfq"
    )
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
    df = df[cols]
    run_strategy(df=df)
