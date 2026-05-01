# @title SIRIUS T1 Trade Trigger
"""
miniQMT 多标的监控交易系统
功能：
- 多标的实时监控
- 跌幅触发买入
- 涨幅触发卖出
"""

import time
import logging
from datetime import datetime

from xtquant import xtdata
from xtquant import xtconstant
from xtquant.xttrader import XtQuantTrader, XtQuantTraderCallback
from xtquant.xttype import StockAccount


# ================== 基础配置 ==================
ACCOUNT_ID = "8886036261"
QMT_PATH = r"D:\国金证券QMT交易端\userdata_mini"

CHECK_INTERVAL = 3   # 秒

# ================== 标的配置 ==================
WATCH_LIST = {
    "601857.SH": {"buy_drop": -0.02, "sell_rise": 0.02, "volume": 100}, #8000
   #"601333.SH": {"buy_drop": -0.02, "sell_rise": 0.02, "volume": 100}, #3200
    "601088.SH": {"buy_drop": -0.02, "sell_rise": 0.02, "volume": 100}, #2000
   #"002008.SZ": {"buy_drop": -0.03, "sell_rise": 0.025, "volume": 100}, #100
}

REF_PRICE_TYPE = "pre_close"

# ================== 日志 ==================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


# ================== 回调类 ==================
class MyCallback(XtQuantTraderCallback):
    def on_order_stock_async_response(self, response):
        logging.info(f"异步下单响应: seq={response.seq}, order_id={response.order_id}")
        
    def on_order_event(self, order, cancel_info=None):
        logging.info(f"委托状态: {order.stock_code}, status={order.order_status}, vol={order.order_volume}")
        
    def on_trade_event(self, trade):
        logging.info(f"成交: {trade.stock_code}, 成交{trade.traded_volume}股 @ {trade.traded_price}")


# ================== 标的状态 ==================
class SymbolState:
    def __init__(self, config):
        self.config = config
        self.ref_price = None
        self.has_bought = False
        self.has_sold = False

    def reset_daily(self):
        self.has_bought = False
        self.has_sold = False


# ================== 主交易系统 ==================
class MultiSymbolTrader:

    def __init__(self):
        self.trader = None
        self.account = None
        self.callback = None
        self.symbols = {
            code: SymbolState(cfg) for code, cfg in WATCH_LIST.items()
        }

    # ===== 连接 =====
    def connect(self):
        self.trader = XtQuantTrader(QMT_PATH, 1)
        self.account = StockAccount(ACCOUNT_ID)
        
        # 关键修复：注册回调对象
        self.callback = MyCallback()
        self.trader.register_callback(self.callback)
        
        self.trader.start()
        ret = self.trader.connect()

        if ret != 0:
            raise Exception(f"连接失败: {ret}")

        logging.info("交易连接成功")

    # ===== 初始化参考价 =====
    def init_ref_prices(self):
        codes = list(self.symbols.keys())
        for code in codes:
            tick = xtdata.get_full_tick([code]).get(code)
            if tick:
                if REF_PRICE_TYPE == "open":
                    price = tick.get("open")
                else:
                    price = tick.get("lastClose")
                if price:
                    self.symbols[code].ref_price = float(price)
                    logging.info(f"{code} 参考价: {price}")
                    continue
            logging.error(f"{code} 无法获取参考价")

    # ===== 获取tick =====
    def get_ticks(self):
        codes = list(self.symbols.keys())
        return xtdata.get_full_tick(codes)

    # ===== 下单 =====
    def order(self, code, price, volume, side):
        if side == "buy":
            order_type = xtconstant.STOCK_BUY
        else:
            order_type = xtconstant.STOCK_SELL

        logging.info(f"{code} 下单 {side} price={price} vol={volume}")

        seq = self.trader.order_stock_async(
            self.account,
            code,
            order_type,
            volume,
            xtconstant.FIX_PRICE,
            price=price,
            strategy_name="SIRIUS_T1",
            order_remark=f"{side}_{code}"
        )
        logging.info(f"下单请求已发送, seq={seq}")

    # ===== 主逻辑 =====
    def run(self):
        self.connect()
        self.init_ref_prices()

        while True:
            try:
                ticks = self.get_ticks()
                for code, state in self.symbols.items():
                    tick = ticks.get(code)
                    if not tick:
                        continue

                    price = tick.get("lastPrice")
                    if not price or state.ref_price is None:
                        continue

                    pct = (price / state.ref_price) - 1
                    cfg = state.config

                    logging.info(f"{code} price={price:.2f} pct={pct:.2%}")

                    # 买入
                    if pct <= cfg["buy_drop"] and not state.has_bought:
                        self.order(code, price, cfg["volume"], "buy")
                        state.has_bought = True

                    # 卖出
                    if pct >= cfg["sell_rise"] and not state.has_sold:
                        self.order(code, price, cfg["volume"], "sell")
                        state.has_sold = True

                time.sleep(CHECK_INTERVAL)

            except Exception as e:
                logging.error(f"异常: {e}")
                time.sleep(2)


# ================== 启动 ==================
if __name__ == "__main__":
    trader = MultiSymbolTrader()
    trader.run()
