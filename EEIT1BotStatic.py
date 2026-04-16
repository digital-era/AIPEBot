# @title SIRIUS T1 Real Static
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SIRIUS T+1 自动交易机器人 - 真实交易版（纯盘中静态 + 尾盘强制卖出）
功能：
1. 从 GitHub 拉取最新模型 JSON（支持本地缓存）
2. 连接 MiniQMT，获取真实账户持仓、资金、行情
3. 每天仅 9:45 调仓一次：买入价格 ≤ 模型基准价，卖出正常时段 ≥ 昨日收盘价 
4. 尾盘（14:50）强制卖出所有需要卖出的股票（无价格下限，确保资金释放）
5. 完整日志、交易记录 Excel、持仓快照
6. 支持单次运行和守护模式
"""

import os
import sys
import json
import time
import logging
import argparse
import threading
import requests
from datetime import datetime, time as dt_time
from typing import Dict, List, Optional, Tuple, Any
import pandas as pd

try:
    from xtquant.xttrader import XtQuantTrader
    from xtquant.xttype import StockAccount
    from xtquant import xtdata, xtconstant
    XT_AVAILABLE = True
except ImportError:
    XT_AVAILABLE = False
    print("错误: 未安装 xtquant，请确保 MiniQMT 客户端已安装并配置好 Python 环境")
    sys.exit(1)

# ========================= 配置部分 =========================
class Config:
    """真实交易配置（请根据实际情况修改）"""
    QMT_PATH = r"E:\国金证券QMT交易端\userdata_mini"
    ACCOUNT_ID = "8886036261"

    MODEL_URL = "https://raw.githubusercontent.com/digital-era/AIPEQModel/main/流入模型_New.json"
    LOCAL_MODEL_CACHE = "流入模型_New.json.cache"
    REQUEST_TIMEOUT = 30
    REQUEST_RETRIES = 3

    LOG_DIR = r"D:\AIPEQModelSIRIUS\Real\SIRIUS_Bot_Logs"
    TRADE_RECORD_PATH = os.path.join(LOG_DIR, "trade_records.xlsx")
    POSITION_SNAPSHOT_PATH = os.path.join(LOG_DIR, "position_snapshots.xlsx")

    MORNING_TRADE_TIME = dt_time(9, 45)   # 上午调仓时间点

    MARKET_OPEN = datetime.strptime("09:25", "%H:%M").time()
    MARKET_CLOSE = datetime.strptime("15:05", "%H:%M").time()  # 可调整为 15:05 更安全
    FORCE_SELL_HOUR = 14
    FORCE_SELL_MINUTE = 50
    

    ORDER_TIMEOUT = 10
    MAX_ORDER_VOLUME = 1000000
    TRADE_RATIO = 0.5               # 资金使用比例（0.5 表示只用一半资金）
    SLIPPAGE = 0.002                # 滑点容忍度（0.2%）
    PRICE_TOLERANCE = 0.005
    ORDER_INTERVAL = 1.0
    REAL_TRADE = False
    DEBUG = True

    INTRADAY_TRADING = True
    LOOKBACK_MINUTES = 30
    BUY_THRESHOLD_PCT = -0.5
    SELL_THRESHOLD_PCT = 0.5
    INTRADAY_SCAN_INTERVAL = 60
    INTRADAY_COOLDOWN_SEC = 300     # 同一股票动态交易冷却时间（秒）

    #HTTP_PROXY = os.environ.get('HTTP_PROXY', '')
    #HTTPS_PROXY = os.environ.get('HTTPS_PROXY', '')
    HTTP_PROXY = 'http://127.0.0.1:7890'
    HTTPS_PROXY = 'http://127.0.0.1:7890'

PROXIES = {}
if Config.HTTP_PROXY:
    PROXIES['http'] = Config.HTTP_PROXY
if Config.HTTPS_PROXY:
    PROXIES['https'] = Config.HTTPS_PROXY

# ========================= 日志模块 =========================
def setup_logger() -> logging.Logger:
    logger = logging.getLogger("SIRIUS_Bot")
    if logger.handlers:
        return logger
    if not os.path.exists(Config.LOG_DIR):
        os.makedirs(Config.LOG_DIR)
    log_filename = datetime.now().strftime("SIRIUS_Bot_%Y%m%d.log")
    log_path = os.path.join(Config.LOG_DIR, log_filename)
    logger.setLevel(logging.DEBUG if Config.DEBUG else logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    fh = logging.FileHandler(log_path, encoding='utf-8')
    fh.setFormatter(formatter)
    logger.addHandler(fh)
    ch = logging.StreamHandler()
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    return logger

logger = setup_logger()

# ========================= 模型加载模块 =========================
class ModelLoader:

    @staticmethod
    def _convert_code(code: str) -> str:
        c = str(code).split('.')[0].zfill(6)
        if len(c) > 6 and (c.endswith('.SH') or c.endswith('.SZ')):
            return c
        sh_prefixes = ('60', '68', '51', '56', '58', '55', '900')
        return f"{c}.SH" if any(c.startswith(p) for p in sh_prefixes) else f"{c}.SZ"


    @staticmethod
    def _fetch_with_retry(url: str, retries: int = 3, timeout: int = 30) -> Optional[Dict]:
        for attempt in range(retries):
            try:
                resp = requests.get(url, timeout=timeout, headers={'User-Agent': 'SIRIUS-Bot/1.0'}, proxies=PROXIES)
                if resp.status_code == 200:
                    return resp.json()
                else:
                    logger.warning(f"HTTP {resp.status_code}, attempt {attempt+1}/{retries}")
            except Exception as e:
                logger.warning(f"Request failed: {e}, attempt {attempt+1}/{retries}")
            time.sleep(2 ** attempt)
        return None

    @staticmethod
    def load_latest_model() -> Optional[Dict]:
        logger.info(f"从 GitHub 获取模型: {Config.MODEL_URL}")
        data = ModelLoader._fetch_with_retry(Config.MODEL_URL, Config.REQUEST_RETRIES, Config.REQUEST_TIMEOUT)
        if data:
            try:
                with open(Config.LOCAL_MODEL_CACHE, 'w', encoding='utf-8') as f:
                    json.dump(data, f, ensure_ascii=False, indent=2)
                logger.info("模型已缓存到本地")
            except Exception as e:
                logger.warning(f"缓存写入失败: {e}")
            return data
        else:
            logger.warning("GitHub 获取失败，尝试本地缓存")
            if os.path.exists(Config.LOCAL_MODEL_CACHE):
                try:
                    with open(Config.LOCAL_MODEL_CACHE, 'r', encoding='utf-8') as f:
                        cached = json.load(f)
                    logger.info("使用本地缓存模型（可能不是最新）")
                    return cached
                except Exception as e:
                    logger.error(f"读取缓存失败: {e}")
            else:
                logger.error("无可用模型数据")
            return None

    @staticmethod
    def parse_model(data: Dict) -> Tuple[List[Dict], float]:
        if data.get('运行状态') != '成功':
            logger.error("模型运行状态非成功")
            return [], 0.0
        result = data.get('结果', {})
        config_list = result.get('最优投资组合配置', {}).get('配置详情', [])
        if not config_list:
            logger.error("模型配置详情为空")
            return [], 0.0
        target = []
        for item in config_list:
            raw_code = item.get('代码', '')
            code = ModelLoader._convert_code(raw_code)
            name = item.get('名称', '')
            weight_str = item.get('最优权重(%)', '0')
            weight = float(str(weight_str).replace('%', '')) / 100.0
            ref_price = item.get('最近一日价格')
            if ref_price is None:
                logger.warning(f"股票 {code} 缺少参考价格，跳过")
                continue
            if weight <= 0:
                continue
            target.append({
                'code': code,
                'name': name,
                'weight': weight,
                'ref_price': float(ref_price)
            })
        risk_info = result.get('风控因子信息', {})
        position_factor = float(risk_info.get('综合建议仓位因子', 1.0))
        position_factor = max(0.0, min(1.0, position_factor))
        logger.info(f"解析到 {len(target)} 个目标持仓，仓位因子: {position_factor:.2f}")
        return target, position_factor

# ========================= 真实 QMT 客户端（增强版） =========================
class QMTClient:
    def __init__(self):
        self.xt_trader = None
        self.account = None
        self.connected = False
        self.lock = threading.RLock()
        self.code_to_name = {} # 新增名称映射缓存
        self.last_dynamic_trade_time = {}

        # 新增：记录已订阅的股票和周期
        self.subscribed_stocks = set()          # 已订阅 tick 的股票
        self.subscribed_minute_stocks = set()   # 已订阅 1m 的股票

    def subscribe_stocks(self, stock_codes: List[str], period: str = 'tick'):
        """
        统一订阅指定股票的行情数据（支持 tick 和 1m 周期）
        可重复调用，内部自动去重
        """
        if not stock_codes:
            return
        # 去重
        if period == 'tick':
            new_codes = [c for c in stock_codes if c not in self.subscribed_stocks]
            if new_codes:
                xtdata.subscribe_quote(new_codes, period='tick')
                self.subscribed_stocks.update(new_codes)
                logger.info(f"订阅 {len(new_codes)} 只股票的 tick 行情")
        elif period == '1m':
            new_codes = [c for c in stock_codes if c not in self.subscribed_minute_stocks]
            if new_codes:
                xtdata.subscribe_quote(new_codes, period='1m')
                self.subscribed_minute_stocks.update(new_codes)
                logger.info(f"订阅 {len(new_codes)} 只股票的 1分钟线行情")
        else:
            logger.warning(f"不支持的订阅周期: {period}")

    def subscribe_all_periods(self, stock_codes: List[str]):
        """同时订阅 tick 和 1m 行情"""
        self.subscribe_stocks(stock_codes, period='tick')
        self.subscribe_stocks(stock_codes, period='1m')
        # 可选：等待数据同步（首次订阅后稍作延迟）
        time.sleep(1)

    def connect(self) -> bool:
        if not XT_AVAILABLE:
            logger.error("xtquant 库未安装，无法连接 QMT")
            return False
        try:
            if self.xt_trader is None:
                self.xt_trader = XtQuantTrader(Config.QMT_PATH, 1)
                self.xt_trader.start()
                self.account = StockAccount(Config.ACCOUNT_ID)
            connect_result = self.xt_trader.connect()
            if connect_result != 0:
                logger.error(f"QMT 连接失败，错误码: {connect_result}")
                return False
            subscribe_result = self.xt_trader.subscribe(self.account)
            if subscribe_result != 0:
                logger.error(f"账户订阅失败，错误码: {subscribe_result}")
                return False
            self.connected = True
            logger.info("QMT 连接成功")
            return True
        except Exception as e:
            logger.error(f"连接 QMT 异常: {e}")
            return False

    def reconnect(self) -> bool:
        logger.info("尝试重连 QMT...")
        self.connected = False
        return self.connect()

    def get_positions(self) -> Dict[str, Dict]:
        with self.lock:
            if not self.connected:
                logger.error("未连接到 QMT")
                return {}
            positions = {}
            try:
                position_list = self.xt_trader.query_stock_positions(self.account)
                for pos in position_list:
                    if pos.m_nVolume <= 0:
                        continue
                    code = pos.m_strStockCode
                    positions[code] = {
                        'volume': pos.m_nVolume,
                        'can_sell': pos.m_nCanUseVolume,
                        'avg_price': pos.m_dAvgPrice,
                    }
                logger.info(f"获取到 {len(positions)} 个持仓")
            except Exception as e:
                logger.error(f"获取持仓失败: {e}")
            return positions

    def get_account_info(self) -> Dict:
        if not self.connected:
            return {}
        try:
            asset = self.xt_trader.query_stock_asset(self.account)
            return {
                "total_asset": asset.m_dTotalAsset,
                "available_cash": asset.m_dCash
            }
        except Exception as e:
            logger.error(f"获取账户信息失败: {e}")
            return {}

    def get_realtime_price(self, code: str) -> Optional[float]:
        with self.lock:
            try:
                tick = xtdata.get_full_tick([code])
                if code in tick and "lastPrice" in tick[code]:
                    return tick[code]['lastPrice']
                # 备用：获取日线最近收盘价
                data = xtdata.get_market_data([code], period='1d', count=1)
                if data is not None and code in data:
                    df = data[code]
                    if not df.empty and 'close' in df.columns:
                        return df['close'].iloc[-1]
            except Exception as e:
                logger.error(f"获取 {code} 行情失败: {e}")
            return None

    def get_pre_close(self, code: str) -> Optional[float]:
        try:
            tick = xtdata.get_full_tick([code])
            if code in tick and 'lastClose' in tick[code]:
                return tick[code]['lastClose']
            data = xtdata.get_market_data([code], period='1d', count=2)
            if data is not None and code in data and len(data[code]) >= 2:
                return data[code]['close'].iloc[-2]
        except Exception as e:
            logger.error(f"获取 {code} 前收价失败: {e}")
        return None

    def get_buy_price_constrained(self, code: str, ref_price: float) -> Optional[float]:
        if ref_price <= 0:
            return None
        real = self.get_realtime_price(code)
        if real is None:
            return ref_price
        return min(real, ref_price)

    def get_sell_price_constrained(self, code: str, pre_close: float) -> Optional[float]:
        if pre_close <= 0:
            return None
        real = self.get_realtime_price(code)
        if real is None:
            return pre_close
        return max(real, pre_close)

    def get_sell_price_unconstrained(self, code: str) -> Optional[float]:
        try:
            tick = xtdata.get_full_tick([code])
            if code not in tick:
                return None
            bid1 = tick[code].get('bidPrice', [0])[0] if 'bidPrice' in tick[code] else 0
            last = tick[code].get('lastPrice', 0)
            if bid1 > 0:
                return bid1
            if last > 0:
                return last
            return None
        except Exception as e:
            logger.error(f"获取 {code} 强制卖出价失败: {e}")
            return None

    def is_limit_up_down(self, code: str, price: float, direction: str) -> bool:
        try:
            tick = xtdata.get_full_tick([code])
            if code not in tick:
                return False
            if direction == 'buy':
                return price >= tick[code]['upStopPrice']
            else:
                return price <= tick[code]['downStopPrice']
        except:
            return False

    def place_order(self, code: str, order_type: str, volume: int, price: float) -> bool:
        if not Config.REAL_TRADE:
            logger.info(f"[模拟模式] 跳过真实下单: {order_type} {code} {volume}股 @ {price:.2f}")
            return True
        with self.lock:
            if not self.connected:
                if not self.reconnect():
                    logger.error("未连接到 QMT 且重连失败，无法下单")
                    return False
            if volume <= 0:
                return False
            if volume % 100 != 0:
                logger.warning(f"委托股数 {volume} 不是100的倍数，自动调整为 {volume // 100 * 100}")
                volume = volume // 100 * 100
                if volume == 0:
                    return False
            if self.is_limit_up_down(code, price, order_type):
                logger.warning(f"{code} 已{'涨停' if order_type=='buy' else '跌停'}，放弃下单")
                return False
            if order_type == 'buy':
                order_id = self.xt_trader.order_stock_async(
                    self.account, code, xtconstant.STOCK_BUY, volume, price, 'limit'
                )
            else:
                order_id = self.xt_trader.order_stock_async(
                    self.account, code, xtconstant.STOCK_SELL, volume, price, 'limit'
                )
            if order_id > 0:
                logger.info(f"委托成功: {order_type} {code} {volume}股 @ {price:.2f}，订单号 {order_id}")
                return True
            else:
                logger.error(f"委托失败: {order_type} {code} {volume}股 @ {price:.2f}，错误码 {order_id}")
                return False

    def cancel_order(self, order_id: int) -> bool:
        with self.lock:
            if not self.connected:
                return False
            try:
                result = self.xt_trader.cancel_order(self.account, order_id)
                if result == 0:
                    logger.info(f"撤单成功: 订单号 {order_id}")
                    return True
                else:
                    logger.warning(f"撤单失败: 订单号 {order_id}, 错误码 {result}")
                    return False
            except Exception as e:
                logger.error(f"撤单异常: {e}")
                return False

    def get_pending_sell_orders(self, code: str = None) -> List:
        """获取所有未成交（未报、已报、部分成交）的卖出委托"""
        with self.lock:
            if not self.connected:
                return []
            try:
                orders = self.xt_trader.query_stock_orders(self.account)
                pending = []
                # QMT订单状态说明（根据迅投文档）：
                # 0=未报, 1=已报, 2=部成, 3=已成, 4=已撤, 5=废单, 6=部撤
                # 卖出委托的未完成状态包括：未报(0)、已报(1)、部成(2)
                valid_statuses = {0, 1, 2}
                for order in orders:
                    # 订单类型：1 表示卖出（STOCK_SELL）
                    if order.m_nOrderType != 1:   # 1 = xtconstant.STOCK_SELL
                        continue
                    if order.m_nOrderStatus in valid_statuses:
                        if code is None or order.m_strStockCode == code:
                            pending.append(order)
                return pending
            except Exception as e:
                logger.error(f"查询未成交卖出委托失败: {e}")
                return []


# ========================= 交易信号生成器 =========================
class TradeSignalGenerator:
    @staticmethod
    def calculate_target_volume(total_asset: float, target_weight: float, price: float) -> int:
        target_value = total_asset * target_weight
        target_volume = int(target_value / price / 100) * 100
        return max(0, target_volume)


    @staticmethod
    def generate_orders(current_positions: Dict, target_holdings: List[Dict],
                    total_asset: float, position_factor: float,
                    available_cash: float, qmt_client) -> Tuple[List[Dict], List[Dict]]:
        """
        返回 (buy_orders, sell_orders)
        buy_orders: [{"code", "volume", "price", "name"}]
        sell_orders: [{"code", "volume", "price", "name", "pre_close"}]
        """
        # 引入资金使用比例，例如 0.5 表示只用一半资金
        trade_ratio = getattr(Config, 'TRADE_RATIO', 1.0)
        effective_total_asset = total_asset * trade_ratio
        risk_adjusted_asset = effective_total_asset * position_factor   # 风控后资产
        effective_available_cash = available_cash * trade_ratio

        # 构建目标字典（买入价受约束）
        target_dict = {}
        all_names = {h['code']: h['name'] for h in target_holdings}
        for h in target_holdings:
            code = h['code']
            effective_weight = h['weight'] * position_factor
            buy_price = qmt_client.get_buy_price_constrained(code, h['ref_price'])
            if buy_price is None:
                logger.warning(f"{code} 无法获取有效买入价格，跳过")
                continue
            # 使用 effective_total_asset 计算目标股数
            target_vol = TradeSignalGenerator.calculate_target_volume(risk_adjusted_asset, effective_weight, buy_price)
            if target_vol > 0:
                target_dict[code] = {
                    'volume': target_vol,
                    'price': buy_price,
                    'name': h['name'],
                    'ref_price': h['ref_price']
                }

        # 当前持仓字典
        current_dict = {}
        for code, pos in current_positions.items():
            current_dict[code] = {
                'volume': pos['volume'],
                'can_sell': pos['can_sell'],
                'avg_price': pos['avg_price']
            }

        # 卖出指令（正常时段，价格 ≥ 昨收）
        sell_orders = []
        for code, cur in current_dict.items():
            target_vol = target_dict.get(code, {}).get('volume', 0)
            if cur['volume'] > target_vol:
                sell_vol = min(cur['can_sell'], cur['volume'] - target_vol)
                if sell_vol > 0:
                    pre_close = qmt_client.get_pre_close(code)
                    if pre_close is None:
                        logger.warning(f"{code} 无法获取昨日收盘价，跳过卖出")
                        continue
                    sell_price = qmt_client.get_sell_price_constrained(code, pre_close)
                    if sell_price is None:
                        continue
                    sell_orders.append({
                        'code': code,
                        'volume': sell_vol,
                        'price': sell_price,
                        'name': all_names.get(code, code), # 【修复】优先取中文名
                        'pre_close': pre_close
                    })

        # 买入指令
        buy_orders = []
        estimated_cost = 0.0
        for code, target in target_dict.items():
            cur_vol = current_dict.get(code, {}).get('volume', 0)
            if target['volume'] > cur_vol:
                buy_vol = target['volume'] - cur_vol
                buy_vol = (buy_vol // 100) * 100
                if buy_vol > 0:
                    buy_orders.append({
                        'code': code,
                        'volume': buy_vol,
                        'price': target['price'],
                        'name': target['name']
                    })
                    estimated_cost += buy_vol * target['price']

        # 资金不足时缩减买入量，使用 effective_available_cash
        if estimated_cost > effective_available_cash + 1e-6:
            ratio = effective_available_cash / estimated_cost
            logger.warning(f"资金不足，缩减买入量，比例 {ratio:.2f}")
            for order in buy_orders:
                order['volume'] = int(order['volume'] * ratio / 100) * 100
            buy_orders = [o for o in buy_orders if o['volume'] > 0]

        return buy_orders, sell_orders
   

# ========================= 订单执行器 =========================
class OrderExecutor:
    def __init__(self):
        self.today_trades = []
        # 若要启用 T+1 限制，取消下面注释并修改 execute_orders
        # self.today_buy_volumes = {}

    def execute_orders(self, buy_orders: List[Dict], sell_orders: List[Dict], qmt_client) -> Tuple[List[Dict], List[Dict]]:
        """执行买卖指令，先卖后买。注意：本版本未做 T+1 限制！"""
        executed_sells = []
        for order in sell_orders:
            code = order['code']
            sell_vol = order['volume']
            if sell_vol <= 0:
                continue
            # T+1 检查（如果需要，取消注释）
            # today_buy = self.today_buy_volumes.get(code, 0)
            # if sell_vol > today_buy:
            #     sell_vol = sell_vol - today_buy
            # else:
            #     continue
            success = qmt_client.place_order(code, 'sell', sell_vol, order['price'])
            if success:
                trade_record = {
                    '时间': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    '股票代码': code,
                    '股票名称': order.get('name', code),
                    '方向': '卖出',
                    '委托数量': sell_vol,
                    '成交价': order['price'],
                    '成交金额': sell_vol * order['price'],
                }
                executed_sells.append(trade_record)
                self.today_trades.append(trade_record)
            time.sleep(Config.ORDER_INTERVAL)

        executed_buys = []
        for order in buy_orders:
            success = qmt_client.place_order(order['code'], 'buy', order['volume'], order['price'])
            if success:
                trade_record = {
                    '时间': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    '股票代码': order['code'],
                    '股票名称': order.get('name', order['code']),
                    '方向': '买入',
                    '委托数量': order['volume'],
                    '成交价': order['price'],
                    '成交金额': order['volume'] * order['price'],
                }
                executed_buys.append(trade_record)
                self.today_trades.append(trade_record)
                # T+1 记录
                # self.today_buy_volumes[order['code']] = self.today_buy_volumes.get(order['code'], 0) + order['volume']
            time.sleep(Config.ORDER_INTERVAL)

        return executed_buys, executed_sells

    def reset_daily(self):
        self.today_trades.clear()
        # self.today_buy_volumes.clear()

# ========================= 业绩记录模块 =========================
class PerformanceEvaluator:
    @staticmethod
    def save_trades(trades: List[Dict]):
        if not trades:
            return
        df_new = pd.DataFrame(trades)
        if os.path.exists(Config.TRADE_RECORD_PATH):
            df_old = pd.read_excel(Config.TRADE_RECORD_PATH)
            df_combined = pd.concat([df_old, df_new], ignore_index=True)
        else:
            df_combined = df_new
        os.makedirs(os.path.dirname(Config.TRADE_RECORD_PATH), exist_ok=True)
        df_combined.to_excel(Config.TRADE_RECORD_PATH, index=False)
        logger.info(f"保存 {len(trades)} 条交易记录至 {Config.TRADE_RECORD_PATH}")

    @staticmethod
    def save_position_snapshot(positions: Dict, total_asset: float, name_mapping: Dict = None):
        if name_mapping is None:
            name_mapping = {}
        records = []
        for code, pos in positions.items():
            records.append({
                '日期': datetime.now().strftime('%Y-%m-%d'),
                '股票代码': code,
                '股票名称': name_mapping.get(code, code),
                '持股数量': pos['volume'],
                '可卖数量': pos.get('can_sell', pos['volume']),
                '成本价': pos['avg_price'],
            })
        df_new = pd.DataFrame(records)
        if os.path.exists(Config.POSITION_SNAPSHOT_PATH):
            df_old = pd.read_excel(Config.POSITION_SNAPSHOT_PATH)
            df_combined = pd.concat([df_old, df_new], ignore_index=True)
        else:
            df_combined = df_new
        os.makedirs(os.path.dirname(Config.POSITION_SNAPSHOT_PATH), exist_ok=True)
        df_combined.to_excel(Config.POSITION_SNAPSHOT_PATH, index=False)
        logger.info("保存持仓快照")

# ========================= 主机器人 =========================
class SIRIUSBot:
    def __init__(self):
        self.model_loader = ModelLoader()
        self.qmt = QMTClient()
        self.signal_gen = TradeSignalGenerator()
        self.executor = OrderExecutor()
        self.evaluator = PerformanceEvaluator()

        # 缓存模型数据（启动时立即加载）
        self.cached_target_holdings = None
        self.cached_position_factor = None
        self._load_model_cache()

        self.last_force_sell_date = None
        self.last_intraday_ts = 0
        self.last_dynamic_trade_time = {}   # 冷却字典

    def _load_model_cache(self):
        """加载并缓存模型数据（启动时或强制刷新时调用）"""
        logger.info("加载模型数据...")
        model_data = self.model_loader.load_latest_model()
        if not model_data:
            logger.error("模型加载失败，无法继续")
            return
        target_holdings, position_factor = self.model_loader.parse_model(model_data)
        if target_holdings:
            self.code_to_name = {h['code']: h['name'] for h in target_holdings}
            self.cached_target_holdings = target_holdings
            # 统一订阅所有目标股票的行情
            codes = [h['code'] for h in target_holdings]
            self.qmt.subscribe_all_periods(codes)
        if not target_holdings:
            logger.error("无有效目标持仓，无法继续")
            return
        self.cached_target_holdings = target_holdings
        self.cached_position_factor = position_factor
        logger.info("模型数据已缓存")

    def intraday_trade_once_static(self):
        """执行一次盘中静态交易"""
        if not Config.INTRADAY_TRADING:
            return

        # 全局扫描间隔
        now_ts = time.time()
        if now_ts - self.last_intraday_ts < Config.INTRADAY_SCAN_INTERVAL:
            return
        self.last_intraday_ts = now_ts

        if not self.cached_target_holdings:
            logger.warning("无缓存模型数据，跳过盘中交易")
            return

        account_info = self.qmt.get_account_info()
        if not account_info:
            return
        current_positions = self.qmt.get_positions()
        
        buy_orders, sell_orders = self.signal_gen.generate_orders(
            current_positions,
            self.cached_target_holdings,
            account_info['total_asset'],
            self.cached_position_factor,
            account_info['available_cash'],
            self.qmt
        )

        if buy_orders or sell_orders:
            logger.info(f"盘中静态信号: 买入 {len(buy_orders)} 条, 卖出 {len(sell_orders)} 条")
            self.executor.execute_orders(buy_orders, sell_orders, self.qmt)
            if self.executor.today_trades:
                self.evaluator.save_trades(self.executor.today_trades)
                self.executor.today_trades.clear()   # 添加这一行


    def force_sell_at_close(self):
        """尾盘强制卖出"""
        today = datetime.now().strftime("%Y-%m-%d")
        if self.last_force_sell_date == today:
            logger.info("今日已完成尾盘强制卖出，跳过")
            return

        logger.info("========== 尾盘强制卖出开始 ==========")

        # 撤销未成交卖出委托
        pending_orders = self.qmt.get_pending_sell_orders()
        if pending_orders:
            logger.info(f"发现 {len(pending_orders)} 笔未成交卖出委托，开始撤销...")
            for order in pending_orders:
                self.qmt.cancel_order(order.m_nOrderID)
                time.sleep(0.5)
            time.sleep(2)

        # 确保模型缓存存在
        if self.cached_target_holdings is None:
            self._load_model_cache()
        if not self.cached_target_holdings:
            logger.error("强制卖出：无有效目标持仓，无法计算")
            return

        target_holdings = self.cached_target_holdings
        position_factor = self.cached_position_factor

        account_info = self.qmt.get_account_info()
        if not account_info:
            logger.error("强制卖出：获取账户信息失败")
            return
        total_asset = account_info['total_asset']
        current_positions = self.qmt.get_positions()

        # 计算目标股数
        trade_ratio = Config.TRADE_RATIO
        effective_total_asset = total_asset * trade_ratio
        risk_adjusted_asset = effective_total_asset * position_factor

        target_vol_dict = {}
        for h in target_holdings:
            code = h['code']
            effective_weight = h['weight'] * position_factor
            price = self.qmt.get_realtime_price(code)
            if price is None or price <= 0:
                price = h['ref_price']
            target_vol = TradeSignalGenerator.calculate_target_volume(risk_adjusted_asset, effective_weight, price)
            if target_vol > 0:
                target_vol_dict[code] = target_vol

        force_sell_list = []
        for code, pos in current_positions.items():
            target_vol = target_vol_dict.get(code, 0)
            if pos['volume'] > target_vol:
                sell_vol = min(pos['can_sell'], pos['volume'] - target_vol)
                if sell_vol > 0:
                    force_sell_list.append({
                        'code': code,
                        'volume': sell_vol,
                        'name': self.code_to_name.get(code, code), # 修复：从映射表取名
                        'pre_close': self.qmt.get_pre_close(code)
                    })

        if not force_sell_list:
            logger.info("尾盘强制卖出：无需要卖出的股票")
            return

        logger.info(f"尾盘需要强制卖出 {len(force_sell_list)} 只股票")
        force_trades = []

        for item in force_sell_list:
            code = item['code']
            sell_vol = item['volume']
            price = self.qmt.get_sell_price_unconstrained(code)
            if price is None or price <= 0:
                logger.warning(f"{code} 无法获取有效市价，放弃强制卖出")
                continue
            if self.qmt.is_limit_up_down(code, price, 'sell'):
                logger.warning(f"{code} 已跌停，无法卖出")
                continue

            logger.info(f"尾盘强制卖出 {code} {sell_vol}股 @ {price:.2f} (无价格下限)")
            success = self.qmt.place_order(code, 'sell', sell_vol, price)
            if success:
                force_trades.append({
                    '时间': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    '股票代码': code,
                    '股票名称': item.get('name', code),
                    '方向': '强制卖出',
                    '委托数量': sell_vol,
                    '成交价': price,
                    '成交金额': sell_vol * price,
                })
            time.sleep(Config.ORDER_INTERVAL)

        if force_trades:
            self.evaluator.save_trades(force_trades)
            self.executor.today_trades.extend(force_trades)

        # 再次撤销残留委托
        final_pending = self.qmt.get_pending_sell_orders()
        if final_pending:
            logger.warning(f"仍有 {len(final_pending)} 笔未成交卖出委托，再次撤销")
            for order in final_pending:
                self.qmt.cancel_order(order.m_nOrderID)
                time.sleep(0.5)

        self.last_force_sell_date = today
        logger.info("========== 尾盘强制卖出结束 ==========")

    def after_close(self):
        now = datetime.now()
        close_time = datetime(now.year, now.month, now.day, 15, 0, 0)
        if now < close_time:
            logger.debug(f"当前 {now.strftime('%H:%M')} 未收盘，跳过持仓快照")
            return
        account_info = self.qmt.get_account_info()
        total_asset = account_info.get('total_asset', 0) if account_info else 0
        positions = self.qmt.get_positions()
        self.evaluator.save_position_snapshot(positions, total_asset, self.code_to_name)

    def run_full_day_once_static(self):
        logger.info("========== SIRIUS 完整交易日开始 ==========")

        # 1. 确保模型已加载（内部会订阅目标股票）
        if self.cached_target_holdings is None:
            self._load_model_cache()

        # 2. 获取当前持仓，订阅所有持仓股票（补订阅）
        current_positions = self.qmt.get_positions()
        if current_positions:
            hold_codes = list(current_positions.keys())
            # 去重：只订阅那些尚未订阅的（目标股票已订阅，这里自动跳过）
            self.qmt.subscribe_all_periods(hold_codes)
        
        isoncepass = False

        while True:
            now = datetime.now()
            current_time = now.time()

            # 如果当前时间不在 09:00–16:00 区间，退出整个流程
            if not (Config.MARKET_OPEN <= current_time <= Config.MARKET_CLOSE):
                logger.info(f"当前时间 {current_time} 超出监控区间，程序结束")
                break

            # 到达尾盘强制卖出时间，退出循环
            if now.hour > Config.FORCE_SELL_HOUR or (now.hour == Config.FORCE_SELL_HOUR and now.minute >= Config.FORCE_SELL_MINUTE):
                logger.info("到达尾盘强制卖出时间，退出盘中循环")
                break

            # 正常扫描
            if not isoncepass and current_time >= Config.MORNING_TRADE_TIME:
              self.intraday_trade_once_static()
              isoncepass = True

            time.sleep(Config.INTRADAY_SCAN_INTERVAL)

        # 执行尾盘卖出（仅14:50后有效）
        self.force_sell_at_close()
        # 执行收盘快照（仅15:00后有效）
        self.after_close()
        logger.info("========== SIRIUS 完整交易日结束 ==========")

# ========================= 辅助函数 =========================
def is_trading_time() -> bool:
    now = datetime.now()
    if now.weekday() >= 5:
        return False
    current_time = now.time()
    morning_start = datetime.strptime("09:30", "%H:%M").time()
    morning_end = datetime.strptime("11:30", "%H:%M").time()
    afternoon_start = datetime.strptime("13:00", "%H:%M").time()
    afternoon_end = datetime.strptime("15:00", "%H:%M").time()
    return (morning_start <= current_time <= morning_end) or (afternoon_start <= current_time <= afternoon_end)

def is_opening_period() -> bool:
    now = datetime.now().time()
    start = datetime.strptime("09:30", "%H:%M").time()
    end = datetime.strptime("10:00", "%H:%M").time()
    return start <= now <= end

# ========================= 主入口 =========================
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='SIRIUS T+1 自动交易机器人（盘中静态+尾盘强制卖出）')
    parser.add_argument('--mode', choices=['once', 'daemon'], default='once',
                        help='运行模式: once-执行一次完整交易日流程后退出; daemon-守护模式')
    parser.add_argument('--snapshot-only', action='store_true',
                        help='仅执行收盘快照，不交易（用于收盘后调用）')
    args = parser.parse_args()

    bot = SIRIUSBot()
    if not bot.qmt.connect():
        logger.error("QMT 连接失败，程序退出")
        sys.exit(1)

    if args.mode == 'once':
        if args.snapshot_only:
            bot.after_close()
        else:
            bot.run_full_day_once_static()
    else:  # daemon 模式
        logger.info("启动守护模式（单线程调度器）")
        last_trade_date = ""
        last_force_sell_date = ""   # 已存在        
        while True:
            now = datetime.now()
            today_str = now.strftime("%Y-%m-%d")   # 定义 today_str
            current_time = now.time()
            if now.weekday() >= 5:
                time.sleep(60)
                continue

            try:

                # 盘中交易时段（避开尾盘强制卖出时段）
                if (Config.MARKET_OPEN <= current_time <= Config.MARKET_CLOSE) and not (now.hour > Config.FORCE_SELL_HOUR or
                                              (now.hour == Config.FORCE_SELL_HOUR and now.minute >= Config.FORCE_SELL_MINUTE)):
                    
                    if (last_trade_date != today_str) and (current_time >= Config.MORNING_TRADE_TIME:
                        logger.info(f"进入交易时间，开始今日调仓: {today_str}")
                        bot.intraday_trade_once_static()
                        last_trade_date = today_str

                # 尾盘强制卖出（14:50后）
                if (now.hour > Config.FORCE_SELL_HOUR or
                    (now.hour == Config.FORCE_SELL_HOUR and now.minute >= Config.FORCE_SELL_MINUTE)) and bot.last_force_sell_date != today:
                    bot.force_sell_at_close()

                # 收盘后快照（15:00后）
                if now.hour >= 15 and not hasattr(bot, '_snapshot_done'):
                    bot.after_close()
                    bot._snapshot_done = True

                if bot.last_force_sell_date != today:
                    bot._snapshot_done = False
            except Exception as e:
                logger.error(f"守护模式异常: {e}", exc_info=True)
            time.sleep(5)
