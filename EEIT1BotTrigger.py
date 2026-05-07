# @title SIRIUS T1 Trade Trigger 
"""
miniQMT 多标的监控交易系统 - 优化版
功能：
- 多标的实时监控
- 跌幅触发买入 / 涨幅触发卖出
- 完整交易日志记录（CSV + JSON 双格式持久化）
- 每日交易统计与盈亏分析
- 策略运行状态持久化
"""

import os
import sys
import csv
import json
import time
import logging
import traceback
from datetime import datetime, date
from pathlib import Path
from dataclasses import dataclass, asdict
from typing import Dict, List, Optional

from xtquant import xtdata
from xtquant import xtconstant
from xtquant.xttrader import XtQuantTrader, XtQuantTraderCallback
from xtquant.xttype import StockAccount


# ================== 基础配置 ==================
ACCOUNT_ID = ""
QMT_PATH = r"D:\国金证券QMT交易端\userdata_mini"
SESSION_ID = int(str(time.time_ns())[-8:]) + os.getpid() % 1000        # 会话ID（不同策略使用不同ID）

CHECK_INTERVAL = 3   # 秒

# ================== 标的配置 ==================
WATCH_LIST = {
    # "601857.SH": {"buy_drop": -0.02, "sell_rise": 0.02, "volume": 100},
    # "601333.SH": {"buy_drop": -0.02, "sell_rise": 0.02, "volume": 100},
    # "601088.SH": {"buy_drop": -0.02, "sell_rise": 0.02, "volume": 100},
    # "002008.SZ": {"buy_drop": -0.03, "sell_rise": 0.025, "volume": 100},
    "002838.SZ": {"buy_drop": -0.015, "sell_rise": 0.025, "volume": 100},
}

REF_PRICE_TYPE = "pre_close"  # "open" 或 "pre_close"

# ================== 日志配置 ==================
LOG_DIR = "./trade_logs"
Path(LOG_DIR).mkdir(exist_ok=True)

# 控制台 + 文件双重日志
today_str = date.today().strftime("%Y%m%d")
log_file = os.path.join(LOG_DIR, f"sirius_t1_{today_str}.log")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(log_file, encoding="utf-8"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("SIRIUS_T1")


# ================== 交易记录数据结构 ==================
@dataclass
class TradeRecord:
    """单笔交易记录"""
    trade_id: str           # 交易唯一ID
    timestamp: str          # 成交时间
    date: str               # 交易日期
    code: str               # 标的代码
    side: str               # BUY / SELL
    price: float            # 成交价格
    volume: int             # 成交数量
    amount: float           # 成交金额
    ref_price: float        # 参考价
    pct_change: float       # 触发时涨跌幅
    trigger_reason: str     # 触发原因
    order_id: int = 0       # 委托ID
    status: str = "PENDING" # PENDING / FILLED / CANCELLED / FAILED


@dataclass
class DailySummary:
    """每日交易汇总"""
    date: str
    total_trades: int = 0
    buy_count: int = 0
    sell_count: int = 0
    total_buy_amount: float = 0.0
    total_sell_amount: float = 0.0
    symbols_traded: List[str] = None

    def __post_init__(self):
        if self.symbols_traded is None:
            self.symbols_traded = []


# ================== 交易日志管理器 ==================
class TradeLogger:
    """
    交易日志管理器 - 负责所有交易记录的持久化
    支持 CSV（便于Excel分析）和 JSON（便于程序读取）双格式
    """

    def __init__(self, log_dir: str = "./trade_logs"):
        self.log_dir = Path(log_dir)
        self.log_dir.mkdir(exist_ok=True)
        self.today = date.today().strftime("%Y%m%d")

        # 文件路径
        self.csv_file = self.log_dir / f"trades_{self.today}.csv"
        self.json_file = self.log_dir / f"trades_{self.today}.json"
        self.summary_file = self.log_dir / f"summary_{self.today}.json"

        # 内存中的记录
        self.records: List[TradeRecord] = []
        self.summary = DailySummary(date=self.today)

        # 初始化CSV（写入表头）
        self._init_csv()

        # 加载历史记录（如果当天已有）
        self._load_history()

    def _init_csv(self):
        """初始化CSV文件"""
        if not self.csv_file.exists():
            with open(self.csv_file, 'w', newline='', encoding='utf-8-sig') as f:
                writer = csv.writer(f)
                writer.writerow([
                    'trade_id', 'timestamp', 'date', 'code', 'side', 
                    'price', 'volume', 'amount', 'ref_price', 'pct_change',
                    'trigger_reason', 'order_id', 'status'
                ])

    def _load_history(self):
        """加载当天历史记录"""
        if self.json_file.exists():
            try:
                with open(self.json_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    for item in data:
                        self.records.append(TradeRecord(**item))
                logger.info(f"已加载 {len(self.records)} 条历史交易记录")
            except Exception as e:
                logger.warning(f"加载历史记录失败: {e}")

    def add_record(self, record: TradeRecord):
        """添加新交易记录"""
        self.records.append(record)

        # 更新汇总
        self.summary.total_trades += 1
        if record.side == "BUY":
            self.summary.buy_count += 1
            self.summary.total_buy_amount += record.amount
        else:
            self.summary.sell_count += 1
            self.summary.total_sell_amount += record.amount

        if record.code not in self.summary.symbols_traded:
            self.summary.symbols_traded.append(record.code)

        # 写入CSV（追加模式）
        self._append_to_csv(record)

        # 保存JSON
        self._save_json()

        # 保存汇总
        self._save_summary()

        logger.info(f"交易记录已保存: {record.trade_id} [{record.side}] {record.code}")

    def _append_to_csv(self, record: TradeRecord):
        """追加到CSV"""
        with open(self.csv_file, 'a', newline='', encoding='utf-8-sig') as f:
            writer = csv.writer(f)
            writer.writerow([
                record.trade_id, record.timestamp, record.date, record.code,
                record.side, record.price, record.volume, record.amount,
                record.ref_price, record.pct_change, record.trigger_reason,
                record.order_id, record.status
            ])

    def _save_json(self):
        """保存JSON格式"""
        with open(self.json_file, 'w', encoding='utf-8') as f:
            json.dump([asdict(r) for r in self.records], f, ensure_ascii=False, indent=2)

    def _save_summary(self):
        """保存每日汇总"""
        summary_dict = asdict(self.summary)
        # 计算净盈亏（简化版，实际需匹配买卖对）
        summary_dict['net_pnl'] = self.summary.total_sell_amount - self.summary.total_buy_amount
        summary_dict['avg_buy_price'] = (
            self.summary.total_buy_amount / self.summary.buy_count if self.summary.buy_count > 0 else 0
        )
        summary_dict['avg_sell_price'] = (
            self.summary.total_sell_amount / self.summary.sell_count if self.summary.sell_count > 0 else 0
        )

        with open(self.summary_file, 'w', encoding='utf-8') as f:
            json.dump(summary_dict, f, ensure_ascii=False, indent=2)

    def update_order_status(self, trade_id: str, order_id: int, status: str, 
                           fill_price: float = 0, fill_volume: int = 0):
        """更新订单状态（成交后回调）"""
        for record in self.records:
            if record.trade_id == trade_id:
                record.order_id = order_id
                record.status = status
                if fill_price > 0:
                    record.price = fill_price
                    record.amount = fill_price * fill_volume
                self._save_json()
                logger.info(f"订单状态更新: {trade_id} -> {status}")
                break

    def get_daily_report(self) -> str:
        """生成每日交易报告"""
        s = self.summary
        net_pnl = s.total_sell_amount - s.total_buy_amount
        report = f"""
{'='*50}
📊 SIRIUS T1 每日交易报告 [{s.date}]
{'='*50}
总交易次数: {s.total_trades}
买入次数: {s.buy_count} | 卖出次数: {s.sell_count}
买入金额: ¥{s.total_buy_amount:,.2f}
卖出金额: ¥{s.total_sell_amount:,.2f}
净盈亏: ¥{net_pnl:,.2f}
交易标的: {', '.join(s.symbols_traded)}
{'='*50}
"""
        return report


# ================== 标的状态 ==================
class SymbolState:
    """单个标的的跟踪状态"""

    def __init__(self, code: str, config: dict):
        self.code = code
        self.config = config
        self.ref_price: Optional[float] = None
        self.has_bought = False
        self.has_sold = False
        self.last_price: Optional[float] = None
        self.last_pct: Optional[float] = None
        self.buy_trade_id: Optional[str] = None  # 记录买入ID，用于配对

    def reset_daily(self):
        """每日重置"""
        self.has_bought = False
        self.has_sold = False
        self.buy_trade_id = None

    def update_price(self, price: float) -> float:
        """更新价格并返回涨跌幅"""
        self.last_price = price
        if self.ref_price and self.ref_price > 0:
            self.last_pct = (price / self.ref_price) - 1
        return self.last_pct or 0


# ================== 回调类 ==================
class MyCallback(XtQuantTraderCallback):
    """miniQMT 交易回调"""

    def __init__(self, trade_logger: TradeLogger):
        super().__init__()
        self.trade_logger = trade_logger
        self.pending_orders: Dict[int, str] = {}  # order_id -> trade_id

    def on_order_stock_async_response(self, response):
        """异步下单响应"""
        logger.info(f"异步下单响应: seq={response.seq}, order_id={response.order_id}")
        if response.order_id and response.order_id > 0:
            # 关联订单ID与交易记录
            if hasattr(response, 'seq') and response.seq in self.pending_orders:
                trade_id = self.pending_orders.pop(response.seq, None)
                if trade_id:
                    self.trade_logger.update_order_status(
                        trade_id, response.order_id, "SUBMITTED"
                    )

    def on_order_event(self, order, cancel_info=None):
        """委托状态变化"""
        status_map = {
            48: "未报", 49: "待报", 50: "已报", 51: "已撤",
            52: "部撤", 53: "已成交", 54: "部成", 55: "废单"
        }
        status = status_map.get(order.order_status, f"未知({order.order_status})")
        logger.info(
            f"委托状态: {order.stock_code} | "
            f"订单ID={order.order_id} | 状态={status} | "
            f"委托量={order.order_volume} | 价格={order.price}"
        )

    def on_trade_event(self, trade):
        """成交回报"""
        side_str = "买入" if trade.order_type == xtconstant.STOCK_BUY else "卖出"
        logger.info(
            f"✅ 成交确认: {trade.stock_code} | {side_str} | "
            f"成交价={trade.traded_price} | 成交量={trade.traded_volume} | "
            f"订单ID={trade.order_id}"
        )
        # 更新交易记录状态
        # 注意：这里简化处理，实际应通过order_id关联
        for trade_id, record in self.trade_logger.records.items():
            if record.order_id == trade.order_id:
                self.trade_logger.update_order_status(
                    trade_id, trade.order_id, "FILLED",
                    trade.traded_price, trade.traded_volume
                )
                break


# ================== 主交易系统 ==================
class MultiSymbolTrader:
    """多标的交易系统主类"""

    def __init__(self):
        self.trader: Optional[XtQuantTrader] = None
        self.account: Optional[StockAccount] = None
        self.callback: Optional[MyCallback] = None
        self.trade_logger = TradeLogger()

        # 标的状态管理
        self.symbols: Dict[str, SymbolState] = {
            code: SymbolState(code, cfg) for code, cfg in WATCH_LIST.items()
        }

        # 运行统计
        self.start_time = datetime.now()
        self.loop_count = 0
        self.error_count = 0

    # ===== 连接 =====
    def connect(self):
        """连接miniQMT"""
        logger.info("正在连接 miniQMT...")
        self.trader = XtQuantTrader(QMT_PATH, SESSION_ID )
        self.account = StockAccount(ACCOUNT_ID)

        # 注册回调对象（关键修复）
        self.callback = MyCallback(self.trade_logger)
        self.trader.register_callback(self.callback)

        self.trader.start()
        ret = self.trader.connect()

        if ret != 0:
            raise Exception(f"连接失败，错误码: {ret}")

        logger.info("✅ 交易连接成功")

        # 记录启动信息
        logger.info(f"监控标的: {list(WATCH_LIST.keys())}")
        logger.info(f"参考价类型: {REF_PRICE_TYPE}")
        logger.info(f"检查间隔: {CHECK_INTERVAL}秒")

    # ===== 初始化参考价 =====
    def init_ref_prices(self):
        """初始化各标的参考价"""
        logger.info("正在初始化参考价...")
        codes = list(self.symbols.keys())

        for code in codes:
            try:
                tick = xtdata.get_full_tick([code]).get(code)
                if tick:
                    if REF_PRICE_TYPE == "open":
                        price = tick.get("open")
                    else:
                        price = tick.get("lastClose")  # 昨收

                    if price and price > 0:
                        self.symbols[code].ref_price = float(price)
                        logger.info(f"📊 {code} 参考价({REF_PRICE_TYPE}): {price:.3f}")
                    else:
                        logger.error(f"❌ {code} 参考价无效: {price}")
                else:
                    logger.error(f"❌ {code} 无法获取行情")
            except Exception as e:
                logger.error(f"❌ {code} 初始化异常: {e}")

    # ===== 获取tick =====
    def get_ticks(self) -> Dict:
        """获取所有监控标的的最新tick"""
        codes = list(self.symbols.keys())
        return xtdata.get_full_tick(codes)

    # ===== 下单 =====
    def order(self, code: str, price: float, volume: int, side: str, 
              pct: float, ref_price: float) -> Optional[str]:
        """
        下单并记录交易日志
        :return: trade_id 或 None
        """
        if side == "buy":
            order_type = xtconstant.STOCK_BUY
        else:
            order_type = xtconstant.STOCK_SELL

        # 生成唯一交易ID
        trade_id = f"{side}_{code}_{datetime.now().strftime('%H%M%S_%f')[:-3]}"

        # 计算金额
        amount = price * volume

        # 创建交易记录
        record = TradeRecord(
            trade_id=trade_id,
            timestamp=datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
            date=today_str,
            code=code,
            side=side.upper(),
            price=price,
            volume=volume,
            amount=amount,
            ref_price=ref_price,
            pct_change=pct,
            trigger_reason=f"价格触发_{side}",
            status="PENDING"
        )

        logger.info(
            f"🚀 发起下单 [{trade_id}] {code} | {side} | "
            f"价格={price:.3f} | 数量={volume} | 金额={amount:,.2f}"
        )

        try:
            seq = self.trader.order_stock_async(
                self.account,
                code,
                order_type,
                volume,
                xtconstant.FIX_PRICE,
                price=price,
                strategy_name="SIRIUS_T1",
                order_remark=f"{side}_{code}_{pct:.2%}"
            )

            # 记录seq与trade_id的关联
            if seq:
                self.callback.pending_orders[seq] = trade_id

            # 添加到日志
            self.trade_logger.add_record(record)

            logger.info(f"✅ 下单请求已发送, seq={seq}, trade_id={trade_id}")
            return trade_id

        except Exception as e:
            logger.error(f"❌ 下单异常: {e}")
            record.status = "FAILED"
            self.trade_logger.add_record(record)
            return None

    # ===== 检查是否交易时间 =====
    def is_trading_time(self) -> bool:
        """判断当前是否在交易时间"""
        now = datetime.now()
        if now.weekday() >= 5:  # 周末
            return False
        t = now.time()
        morning = (9, 30) <= (t.hour, t.minute) <= (11, 30)
        afternoon = (13, 0) <= (t.hour, t.minute) <= (15, 0)
        return morning or afternoon

    # ===== 打印状态 =====
    def print_status(self):
        """打印当前状态"""
        logger.info("-" * 60)
        logger.info(f"运行时间: {datetime.now() - self.start_time}")
        logger.info(f"循环次数: {self.loop_count}")
        logger.info(f"错误次数: {self.error_count}")
        for code, state in self.symbols.items():
            pct_str = f"{state.last_pct:.2%}" if state.last_pct else "N/A"
            logger.info(
                f"{code}: 最新价={state.last_price or 'N/A'} | "
                f"涨跌幅={pct_str} | 已买={state.has_bought} | 已卖={state.has_sold}"
            )
        logger.info("-" * 60)

    # ===== 主逻辑 =====
    def run(self):
        """主运行循环"""
        self.connect()
        self.init_ref_prices()

        logger.info("=" * 60)
        logger.info("🎯 SIRIUS T1 交易系统启动")
        logger.info("=" * 60)

        while True:
            try:
                self.loop_count += 1

                # 检查交易时间
                if not self.is_trading_time():
                    if self.loop_count % 20 == 0:  # 每20次循环提示一次
                        logger.info("当前非交易时间，等待中...")
                    time.sleep(60)
                    continue

                # 获取行情
                ticks = self.get_ticks()

                for code, state in self.symbols.items():
                    tick = ticks.get(code)
                    if not tick:
                        continue

                    price = tick.get("lastPrice")
                    if not price or state.ref_price is None:
                        continue

                    # 更新价格并计算涨跌幅
                    pct = state.update_price(price)
                    cfg = state.config

                    # 每10次循环打印一次状态
                    if self.loop_count % 10 == 0:
                        logger.info(
                            f"{code} price={price:.2f} ref={state.ref_price:.2f} "
                            f"pct={pct:.2%}"
                        )

                    # ===== 买入触发 =====
                    if pct <= cfg["buy_drop"] and not state.has_bought:
                        logger.info(
                            f"🔵 买入触发: {code} | 当前{price:.3f} | "
                            f"跌幅{pct:.2%} <= 阈值{cfg['buy_drop']:.2%}"
                        )
                        trade_id = self.order(
                            code, price, cfg["volume"], "buy", pct, state.ref_price
                        )
                        if trade_id:
                            state.has_bought = True
                            state.buy_trade_id = trade_id

                    # ===== 卖出触发 =====
                    if pct >= cfg["sell_rise"] and not state.has_sold:
                        # 检查是否已买入（避免无持仓卖出）
                        if state.has_bought:
                            logger.info(
                                f"🔴 卖出触发: {code} | 当前{price:.3f} | "
                                f"涨幅{pct:.2%} >= 阈值{cfg['sell_rise']:.2%}"
                            )
                            trade_id = self.order(
                                code, price, cfg["volume"], "sell", pct, state.ref_price
                            )
                            if trade_id:
                                state.has_sold = True
                        else:
                            logger.warning(
                                f"⚠️ {code} 卖出条件满足但未买入，跳过"
                            )

                # 定期打印状态报告
                if self.loop_count % 100 == 0:
                    self.print_status()
                    logger.info(self.trade_logger.get_daily_report())

                time.sleep(CHECK_INTERVAL)

            except KeyboardInterrupt:
                logger.info("收到停止信号，正在关闭...")
                break
            except Exception as e:
                self.error_count += 1
                logger.error(f"主循环异常: {e}")
                logger.error(traceback.format_exc())
                time.sleep(2)

        # 退出处理
        self.shutdown()

    def shutdown(self):
        """优雅关闭"""
        logger.info("=" * 60)
        logger.info("🛑 系统关闭中...")
        logger.info("=" * 60)

        # 打印最终报告
        logger.info(self.trade_logger.get_daily_report())

        # 保存最终状态
        self.trade_logger._save_summary()

        if self.trader:
            self.trader.stop()

        logger.info("✅ 系统已安全退出")


# ================== 启动 ==================
if __name__ == "__main__":
    trader = MultiSymbolTrader()
    try:
        trader.run()
    except Exception as e:
        logger.critical(f"致命错误: {e}")
        logger.critical(traceback.format_exc())
        sys.exit(1)
