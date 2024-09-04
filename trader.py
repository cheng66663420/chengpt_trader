import backtrader as bt

import pandas as pd
import akshare as ak


def run_strategy(
    df: pd.DataFrame, strategy_list: list[bt.Strategy], if_plot: bool = True
) -> None:
    cerebro = bt.Cerebro()
    data = bt.feeds.PandasData(dataname=df)
    cerebro.adddata(data)
    for stategy in strategy_list:
        # 添加策略
        cerebro.addstrategy(stategy)

    # 设置初始资金
    cerebro.broker.setcash(100000.0)
    # 设置交易手续费
    cerebro.broker.setcommission(commission=1 / 10000)
    # cerebro.broker.set_slippage_perc(0.0)
    # 添加分析器
    cerebro.addanalyzer(bt.analyzers.SharpeRatio, _name="sharpe")
    cerebro.addanalyzer(bt.analyzers.DrawDown, _name="drawdown")
    cerebro.addanalyzer(bt.analyzers.AnnualReturn, _name="annual_ret")
    cerebro.addanalyzer(bt.analyzers.TimeDrawDown, _name="Calmar")

    # 运行回测
    results = cerebro.run()
    for result in results:
        for analyzer in result.analyzers:
            analyzer.print()
    if if_plot:
        # 绘制回测结果
        cerebro.plot()
    # 打印最终资金
    print(f"Final Portfolio Value: {cerebro.broker.getvalue():.2f}")
    # 打印回测结果


if __name__ == "__main__":
    df = ak.fund_etf_hist_em(
        symbol="159941", start_date="20100101", end_date="20240902", adjust="hfq"
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
    run_strategy(df=df, strategy_list=[DrawdownStrategy, DynamicGridStrategy])
