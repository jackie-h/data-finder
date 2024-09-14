import datetime


def find_trades(trade_finder):
    print(f'Finding trades')

    trades = trade_finder.find_all(datetime.date.today(), datetime.date.today(), "LATEST",
                                   trade_finder.symbol().eq("AAPL"),
                                   [trade_finder.symbol(), trade_finder.price()])
    np_trades = trades.to_numpy()
    print(np_trades)
    df = trades.to_pandas()
    print(df)

    trades = trade_finder.find_all(datetime.date.today(), datetime.date.today(), "LATEST",
                                   trade_finder.price() > 200.0,
                                   [trade_finder.symbol(), trade_finder.price()])
    np_trades = trades.to_numpy()
    print(np_trades)
    df = trades.to_pandas()
    print(df)

    trades = trade_finder.find_all(datetime.date.today(), datetime.date.today(), "LATEST",
                                   (trade_finder.symbol() == "AAPL").and_op(trade_finder.price() == 84.11),
                                   [trade_finder.symbol(), trade_finder.price()])
    np_trades = trades.to_numpy()
    print(np_trades)
    df = trades.to_pandas()
    print(df)
