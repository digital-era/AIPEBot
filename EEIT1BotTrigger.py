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
from xtquant import xttrader
from xtquant.xttype import StockAccount

# ================== 基础配置 ==================
ACCOUNT_ID = "你的资金账号"
QMT_PATH = r"D:\国金证券QMT交易端\userdata_mini"

CHECK_INTERVAL = 3   # 秒

# ================== 标的配置 ==================
WATCH_LIST = {
    "601857.SH": {
        "buy_drop": -0.02,
        "sell_rise": 0.02,
        "volume": 100 #8000
    },
    "601333.SH": {
        "buy_drop": -0.02,
        "sell_rise": 0.02,
        "volume": 100 #3200
    },
    "601088.SH": {
        "buy_drop": -0.02,
        "sell_rise": 0.02,
        "volume": 100 #2000
    }
}

REF_PRICE_TYPE = "pre_close"  # or open

# ================== 日志 ==================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

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

        self.symbols = {
            code: SymbolState(cfg) for code, cfg in WATCH_LIST.items()
        }

    # ===== 连接 =====
    def connect(self):
        self.trader = xttrader.XtQuantTrader(path=QMT_PATH)
        self.account = StockAccount(ACCOUNT_ID)

        self.trader.start()
        ret = self.trader.connect()

        if ret != 0:
            raise Exception(f"连接失败: {ret}")

        logging.info("✅ 交易连接成功")

    # ===== 初始化参考价 =====
    def init_ref_prices(self):
        codes = list(self.symbols.keys())

        data = xtdata.get_market_data(
            field_list=["open", "pre_close"],
            stock_list=codes,
            period="1d",
            count=1
        )

        for code in codes:
            df = data.get(code)
            if df is None or df.empty:
                logging.warning(f"{code} 无日线数据")
                continue

            if REF_PRICE_TYPE == "open":
                price = float(df["open"].iloc[-1])
            else:
                price = float(df["pre_close"].iloc[-1])

            self.symbols[code].ref_price = price
            logging.info(f"{code} 参考价: {price}")

    # ===== 获取tick =====
    def get_ticks(self):
        codes = list(self.symbols.keys())
        return xtdata.get_full_tick(codes)

    # ===== 下单 =====
    def order(self, code, price, volume, side):
        if side == "buy":
            order_type = 23
        else:
            order_type = 24

        logging.info(f"{code} 下单 {side} price={price} vol={volume}")

        self.trader.order_stock(
            account=self.account,
            stock_code=code,
            order_type=order_type,
            price=price,
            volume=volume
        )

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

                    # ===== 买入 =====
                    if (pct <= cfg["buy_drop"]) and (not state.has_bought):
                        self.order(code, price, cfg["volume"], "buy")
                        state.has_bought = True

                    # ===== 卖出 =====
                    if (
                        pct >= cfg["sell_rise"]
                        and state.has_bought
                        and not state.has_sold
                    ):
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
