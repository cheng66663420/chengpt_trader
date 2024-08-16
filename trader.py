import backtrader as bt
from data_moudle import get_quote_data


class DynamicGridStrategy(bt.Strategy):
    params = (
        ("grid_spacing", 0.1),  # 网格间距，例如 5%
        ("grid_size", 10),  # 每个网格的订单数量
        ("order_size", 1),  # 每个订单的数量
    )

    def __init__(self):
        self.order = None
        self.buy_price = None
        self.sell_price = None
        self.grid_spacing = self.params.grid_spacing
        self.grid_size = self.params.grid_size
        self.order_size = self.params.order_size

    def next(self):
        if self.order:
            return  # 如果有未完成的订单，则不执行新的交易

        current_price = self.data.close[0]

        if self.position:
            # 如果有持仓，则设置卖出网格
            if self.sell_price is None or current_price >= self.sell_price:
                self.sell_price = current_price * (1 + self.grid_spacing)
                self.order = self.sell(size=self.order_size, price=self.sell_price)
        else:
            # 如果没有持仓，则设置买入网格
            if self.buy_price is None or current_price <= self.buy_price:
                self.buy_price = current_price * (1 - self.grid_spacing)
                self.order = self.buy(size=self.order_size, price=self.buy_price)

    def notify_order(self, order):
        if order.status in [order.Completed]:
            self.order = None
            self.buy_price = None
            self.sell_price = None


class PandasData(bt.feeds.DataBase):
    lines = ("datetime", "open", "high", "low", "close", "volume")
    params = (
        ("datetime", None),
        ("open", "open"),
        ("high", "high"),
        ("low", "low"),
        ("close", "close"),
        ("volume", "volume"),
        ("openinterest", "openi_nterest"),
    )

    def __init__(self, data):
        self.data = data
        super(PandasData, self).__init__()

    def start(self):
        super(PandasData, self).start()
        self.iter = self.data.itertuples()

    def _load(self):
        try:
            row = next(self.iter)
        except StopIteration:
            return False

        self.lines.datetime[0] = bt.date2num(row.Index)
        self.lines.open[0] = row.open
        self.lines.high[0] = row.high
        self.lines.low[0] = row.low
        self.lines.close[0] = row.close
        self.lines.volume[0] = row.volume
        self.lines.openinterest[0] = 0  # 如果没有 openinterest 数据，可以设置为 0

        return True


if __name__ == "__main__":
    ticker_symbol = "159941.SZ"
    start_time = "20100101"
    end_time = "20240815"
    period = "1d"
    data_dir = "D:/qmt_datadir"
    quote_data = get_quote_data(ticker_symbol, start_time, end_time, period, data_dir)
    quote_data.set_index("trade_time", inplace=True)
    cerebro = bt.Cerebro()

    # 添加数据源
    data = PandasData(data=quote_data)
    cerebro.adddata(data)

    # 添加策略
    cerebro.addstrategy(DynamicGridStrategy)

    # 设置初始资金
    cerebro.broker.setcash(100000.0)

    # 运行回测
    print("Starting Portfolio Value: %.2f" % cerebro.broker.getvalue())
    cerebro.run()
    print("Final Portfolio Value: %.2f" % cerebro.broker.getvalue())

    # 绘制结果
    cerebro.plot()
