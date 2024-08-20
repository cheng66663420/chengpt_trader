import backtrader as bt
from data_moudle import get_quote_data


class CustomPandasData(bt.feeds.PandasData):
    params = (
        ("datetime", None),
        ("open", "open"),
        ("high", "high"),
        ("low", "low"),
        ("close", "close"),
        ("volume", "volume"),
        ("openinterest", "open_interest"),
    )


class QmtDataSource(bt.feeds.DataBase):
    def __init__(self, start_time, end_time, ticker_symbol, period):
        self.start_time = start_time
        self.end_time = end_time
        self.ticker_symbol = ticker_symbol
        self.period = period
        self.data = None

    def start(self):
        df = get_quote_data(
            ticker_symbol=self.ticker_symbol,
            start_time=self.start_time,
            end_time=self.end_time,
            period=self.period,
        )
        df = df.set_index("trade_time")
        self.data = CustomPandasData(dataname=df)

    def getdata(self):
        return self.data


if __name__ == "__main__":
    df = get_quote_data(
        ticker_symbol="159941.SZ",
        start_time="20140101",
        end_time="20240819",
        period="1d",
    )
    df = df.set_index("trade_time")
    data = CustomPandasData(dataname=df)
