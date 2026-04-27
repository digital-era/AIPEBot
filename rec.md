2026-04-27 09:45:51,718 - INFO - 盘中静态信号: 买入 4 条, 卖出 0 条
Traceback (most recent call last):
  File "D:\AIPEQModelSIRIUS\static\SIRIUST1BotStatic.py", line 1078, in <module>
    bot.run_full_day_once_static()
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~^^
  File "D:\AIPEQModelSIRIUS\static\SIRIUST1BotStatic.py", line 1031, in run_full_day_once_static
    self.intraday_trade_once_static()
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~^^
  File "D:\AIPEQModelSIRIUS\static\SIRIUST1BotStatic.py", line 871, in intraday_trade_once_static
    self.executor.execute_orders(buy_orders, sell_orders, self.qmt)
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "D:\AIPEQModelSIRIUS\static\SIRIUST1BotStatic.py", line 738, in execute_orders
    success = qmt_client.place_order(order['code'], 'buy', order['volume'], order['price'])
  File "D:\AIPEQModelSIRIUS\static\SIRIUST1BotStatic.py", line 528, in place_order
    order_id = self.xt_trader.order_stock_async(
        self.account, code, xtconstant.STOCK_BUY, volume, price, 'limit'
    )
  File "C:\Users\DELL\AppData\Local\Packages\PythonSoftwareFoundation.Python.3.13_qbz5n2kfra8p0\LocalCache\local-packages\Python313\site-packages\xtquant\xttrader.py", line 469, in order_stock_async
    req.m_nPriceType = price_type
    ^^^^^^^^^^^^^^^^
TypeError: (): incompatible function arguments. The following argument types are supported:
    1. (self: xtquant.xtpythonclient.OrderStockReq, arg0: int) -> None

Invoked with: <xtquant.xtpythonclient.OrderStockReq object at 0x00000213B09644B0>, 100.24
