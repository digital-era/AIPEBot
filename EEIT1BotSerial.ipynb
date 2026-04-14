# @title SIRIUS T1 Realpro Serially

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SIRIUS T+1 自动交易机器人 - 动态阈值择时版（尾盘强制卖出 + 部分成交处理）
功能：
1. 买入：监控盘中最低点，从最低点反弹一定比例时买入（价格 ≤ 模型基准价）
2. 卖出：监控盘中最高点，从最高点回落一定比例时卖出（正常时段价格 ≥ 昨收）
3. 支持部分成交：卖出过程中持续监控剩余股数，直到全部卖出或尾盘
4. 尾盘（14:50）强制卖出所有仍未卖出的股票（忽略价格下限，确保资金释放）
5. 代理配置、断线重连、委托间隔可调
"""

import os
import sys
import json
import time
import logging
import argparse
import requests
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple
import pandas as pd
import random

try:
    from xtquant.xttrader import XtQuantTrader
    from xtquant.xttype import StockAccount
    from xtquant import xtconstant
    from xtquant.xtdata import get_full_tick, get_market_data
    XT_AVAILABLE = True
except ImportError:
    XT_AVAILABLE = False
    print("错误: 未安装 xtquant，请确保 MiniQMT 客户端已安装并配置好 Python 环境")
    sys.exit(1)

# ========================= 配置 =========================
class Config:
    QMT_PATH = r"D:\国金证券QMT\userdata_mini"
    ACCOUNT_ID = "8888888888"
    MODEL_URL = "https://raw.githubusercontent.com/digital-era/AIPEQModel/main/流入模型_New.json"
    LOCAL_MODEL_CACHE = "流入模型_New.json.cache"
    LOG_DIR = r"D:\AIPEQModelSIRIUS\Realpro\SIRIUS_Bot_Logs"
    TRADE_RECORD_PATH = os.path.join(LOG_DIR, "trade_records.xlsx")
    POSITION_SNAPSHOT_PATH = os.path.join(LOG_DIR, "position_snapshots.xlsx")
    # 动态择时参数
    BUY_REBOUND_RATIO = 0.0062      # 买入：从最低点反弹 0.2% 触发
    SELL_DROP_RATIO = 0.0038        # 卖出：从最高点回落 0.2% 触发
    MONITOR_START_HOUR = 10
    MONITOR_START_MINUTE = 0
    FORCE_DEADLINE_HOUR = 14
    FORCE_DEADLINE_MINUTE = 50     # 尾盘强制卖出时间
    # 风控与交易参数
    MAX_ORDER_VOLUME = 1000000
    TRADE_RATIO = 0.5
    FORCE_SELL_PRICE_RATIO  =  0.995
    REQUEST_TIMEOUT = 30
    REQUEST_RETRIES = 3
    ORDER_INTERVAL = 1.0
    REAL_TRADE = True
    DEBUG = True
    # 部分成交轮询间隔（秒）
    PARTIAL_FILL_CHECK_INTERVAL = 10

# 代理配置（从环境变量读取）
#HTTP_PROXY = os.environ.get('HTTP_PROXY', '')
#HTTPS_PROXY = os.environ.get('HTTPS_PROXY', '')
HTTP_PROXY = 'http://127.0.0.1:7890'
HTTPS_PROXY = 'http://127.0.0.1:7890'

PROXIES = {}
if HTTP_PROXY:
    PROXIES['http'] = HTTP_PROXY
if HTTPS_PROXY:
    PROXIES['https'] = HTTPS_PROXY

# ========================= 日志 =========================
def setup_logger():
    logger = logging.getLogger("SIRIUS_Bot")

    if logger.handlers:  # ✅ 防止重复添加
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

# ========================= 时间工具 =========================
def now_cn() -> datetime:
    return datetime.now(timezone(timedelta(hours=8))).replace(tzinfo=None)

# ========================= 模型加载 =========================
class ModelLoader:
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
                logger.info("模型已缓存")
            except Exception as e:
                logger.warning(f"缓存写入失败: {e}")
            return data
        logger.warning("GitHub 获取失败，尝试本地缓存")
        if os.path.exists(Config.LOCAL_MODEL_CACHE):
            try:
                with open(Config.LOCAL_MODEL_CACHE, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception as e:
                logger.error(f"读取缓存失败: {e}")
        return None

    @staticmethod
    def parse_model(data: Dict) -> Tuple[List[Dict], float]:
        if data.get('运行状态') != '成功':
            logger.error("模型运行状态非成功")
            return [], 0.0
        config_list = data.get('结果', {}).get('最优投资组合配置', {}).get('配置详情', [])
        if not config_list:
            logger.error("模型配置详情为空")
            return [], 0.0
        target = []
        for item in config_list:
            code = item.get('代码', '')
            name = item.get('名称', '')
            weight_str = item.get('最优权重(%)', '0')
            weight = float(str(weight_str).replace('%', '')) / 100.0
            ref_price = item.get('最近一日价格')
            if not ref_price or weight <= 0:
                continue
            target.append({'code': code, 'name': name, 'weight': weight, 'ref_price': float(ref_price)})
        risk_info = data.get('结果', {}).get('风控因子信息', {})
        position_factor = float(risk_info.get('综合建议仓位因子', 1.0))
        position_factor = max(0.0, min(1.0, position_factor))
        logger.info(f"解析到 {len(target)} 个目标持仓，仓位因子: {position_factor:.2f}")
        return target, position_factor

# ========================= QMT 客户端 =========================
class QMTClient:
    def __init__(self):
        self.xt_trader = None
        self.account = None
        self.connected = False

    def connect(self) -> bool:
        if not XT_AVAILABLE:
            return False
        try:
            self.xt_trader = XtQuantTrader(Config.QMT_PATH, 1)
            self.xt_trader.start()
            self.account = StockAccount(Config.ACCOUNT_ID)
            if self.xt_trader.connect() != 0:
                return False
            if self.xt_trader.subscribe(self.account) != 0:
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
        if not self.connected:
            return {}
        positions = {}
        try:
            for pos in self.xt_trader.query_stock_positions(self.account):
                if pos.m_nVolume > 0:
                    positions[pos.m_strStockCode] = {
                        'volume': pos.m_nVolume,
                        'can_sell': pos.m_nCanUseVolume,
                        'avg_price': pos.m_dAvgPrice,
                    }
            logger.debug(f"获取到 {len(positions)} 个持仓")
        except Exception as e:
            logger.error(f"获取持仓失败: {e}")
        return positions

    def get_account_info(self) -> Dict:
        if not self.connected:
            return {}
        try:
            asset = self.xt_trader.query_stock_asset(self.account)
            return {'total_asset': asset.m_dTotalAsset, 'available_cash': asset.m_dAvailable}
        except Exception as e:
            logger.error(f"获取账户信息失败: {e}")
            return {}

    def get_realtime_price(self, code: str) -> Optional[float]:
        try:
            tick = get_full_tick([code])
            if code in tick:
                return tick[code]['lastPrice']
            data = get_market_data([code], period='1d', count=1)
            if data is not None and not data.empty:
                return data['close'].iloc[-1]
        except Exception as e:
            logger.error(f"获取 {code} 行情失败: {e}")
        return None

    def get_pre_close(self, code: str) -> Optional[float]:
        try:
            tick = get_full_tick([code])
            if code in tick and 'lastClose' in tick[code]:
                return tick[code]['lastClose']
            data = get_market_data([code], period='1d', count=2)
            if data is not None and not data.empty and len(data) >= 2:
                return data['close'].iloc[-2]
        except Exception as e:
            logger.error(f"获取 {code} 前收价失败: {e}")
        return None

    def get_optimal_buy_price(self, code: str, ref_price: float) -> Optional[float]:
        if ref_price <= 0:
            return None
        try:
            tick = get_full_tick([code])
            if not tick or code not in tick:
                return ref_price

            tick_data = tick.get(code, {})
            ask1 = tick_data.get('askPrice', [0])[0]

            if ask1 and ask1 > 0:
                return min(ref_price, ask1)

            last = tick_data.get('lastPrice', 0)
            if last > 0:
                return min(ref_price, last)

            return ref_price
        except Exception as e:
            logger.error(f"获取买入价失败 {code}: {e}")
            return None

    def get_optimal_sell_price(self, code: str, pre_close: float) -> Optional[float]:
        if pre_close <= 0:
            return None
        try:
            tick = get_full_tick([code])
            if not tick or code not in tick:
                return pre_close

            tick_data = tick.get(code, {})
            bid1 = tick_data.get('bidPrice', [0])[0]

            if bid1 and bid1 > 0:
                return max(pre_close, bid1)

            last = tick_data.get('lastPrice', 0)
            if last > 0:
                return max(pre_close, last)

            return pre_close
        except Exception as e:
            logger.error(f"获取卖出价失败 {code}: {e}")
            return None


    def get_sell_price_unconstrained(self, code: str, pre_close: float = None) -> Optional[float]:
        """
        尾盘强制卖出：获取一个既能成交又不至于过低的价格。
        优先使用买一价（bid1），但不得低于保护价（pre_close * FORCE_SELL_PRICE_RATIO）。
        若无买一价，则使用最新价，同样不低于保护价。
        """
        try:
            # 获取保护价
            if pre_close is None:
                pre_close = self.get_pre_close(code)
            if pre_close is None:
                logger.warning(f"{code} 无法获取昨收，无法设置价格下限")
                return None
            protect_price = pre_close * getattr(Config, 'FORCE_SELL_PRICE_RATIO', 0.98)

            bid1 = None
            last = None

            tick = get_full_tick([code])
            if code in tick:
                bid1 = tick[code].get('bidPrice', [0])[0] if 'bidPrice' in tick[code] else 0
                last = tick[code].get('lastPrice', 0)

                if bid1 > 0:
                    best_price = bid1
                elif last > 0:
                    best_price = last
                else:
                    best_price = protect_price
            else:
                price = self.get_realtime_price(code)
                if price is not None:
                    best_price = price
                    last = price
                else:
                    best_price = protect_price

            final_price = max(best_price, protect_price)

            logger.debug(
                f"{code} 强制卖出: 买一{bid1}, 最新{last}, 保护价{protect_price:.2f}, 最终{final_price:.2f}"
            )
            return final_price
        except Exception as e:
            logger.error(f"获取 {code} 强制卖出价失败: {e}")
            return None

    def is_limit_up_down(self, code: str, price: float, direction: str) -> bool:
        try:
            tick = get_full_tick([code])
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
            logger.info(f"[模拟模式] {order_type} {code} {volume}股 @ {price:.2f}")
            return True
        if not self.connected:
            if not self.reconnect():
                logger.error("未连接到 QMT 且重连失败，无法下单")
                return False
        if volume <= 0 or volume % 100 != 0:
            volume = (volume // 100) * 100
            if volume <= 0:
                return False
        if self.is_limit_up_down(code, price, order_type):
            logger.warning(f"{code} 已{'涨停' if order_type=='buy' else '跌停'}，放弃")
            return False
        order_id = self.xt_trader.order_stock_async(
            self.account, code,
            xtconstant.STOCK_BUY if order_type == 'buy' else xtconstant.STOCK_SELL,
            volume, price, 'limit'
        )
        if order_id > 0:
            logger.info(f"委托成功: {order_type} {code} {volume}股 @ {price:.2f} 订单号{order_id}")
            return True
        else:
            logger.error(f"委托失败: {order_type} {code} {volume}股 @ {price:.2f} 错误码{order_id}")
            return False

# ========================= 动态择时执行器（支持部分成交） =========================
class DynamicOrderExecutor:
    def __init__(self, qmt_client):
        self.qmt = qmt_client
        self.today_trades = []

    def _wait_until_start_time(self):
        start = now_cn().replace(hour=Config.MONITOR_START_HOUR, minute=Config.MONITOR_START_MINUTE, second=0)
        if now_cn() < start:
            wait_seconds = (start - now_cn()).total_seconds()
            logger.info(f"等待至 {Config.MONITOR_START_HOUR}:{Config.MONITOR_START_MINUTE:02d} 开始监控")
            time.sleep(wait_seconds)
            

    # 买入（不强求全成交，部分成交可接受，逻辑保持简单）
    def execute_buy(self, code: str, name: str, ref_price: float, target_volume: int) -> bool:
      if target_volume <= 0:
          return False

      logger.info(f"监控买入 {code} {name} 基准价{ref_price:.2f} 目标股数{target_volume}")
      self._wait_until_start_time()

      def get_bought_volume():
          try:
              orders = self.qmt.xt_trader.query_stock_orders(self.qmt.account) or []
              bought = 0
              for o in orders:
                  if (o.m_strStockCode == code and
                      o.m_nOrderType == xtconstant.STOCK_BUY and
                      o.m_nOrderStatus in (xtconstant.ORDER_FILLED, xtconstant.ORDER_PARTIAL)):
                      bought += o.m_nFilledVolume
              return bought
          except Exception as e:
              logger.error(f"查询买入成交失败: {e}")
              return 0

      low_price = float('inf')
      deadline = now_cn().replace(hour=Config.FORCE_DEADLINE_HOUR, minute=Config.FORCE_DEADLINE_MINUTE, second=0)

      bought = get_bought_volume()
      remaining = target_volume - bought
      total_entrusted = 0

      loop_count = 0
      while now_cn() < deadline and remaining > 0:
          loop_count += 1
          if loop_count > 1000:
              logger.warning(f"{code} 循环超限，强制退出")
              break
          cur_price = self.qmt.get_optimal_buy_price(code, ref_price)

          if cur_price is None or cur_price <= 0:
              time.sleep(5)
              continue

          if cur_price < low_price:
              low_price = cur_price

          if cur_price >= low_price * (1 + Config.BUY_REBOUND_RATIO) and cur_price <= ref_price:
              #防止无限挂单 
              if (total_entrusted - bought) > target_volume * 1.2:
                  logger.warning(f"{code} 委托超限，停止继续买入")
                  break

              if total_entrusted - bought >= remaining:
                  time.sleep(5)
                  continue

              to_buy = remaining - (total_entrusted - bought)
              if to_buy <= 0:
                  time.sleep(5)
                  continue

              logger.info(f"{code} 触发买入: 当前{cur_price:.2f} 最低{low_price:.2f}")

              if self.qmt.place_order(code, 'buy', to_buy, cur_price):
                  total_entrusted += to_buy
                  time.sleep(Config.PARTIAL_FILL_CHECK_INTERVAL)

                  bought = get_bought_volume()
                  remaining = target_volume - bought

              else:
                  time.sleep(5)
          else:
              time.sleep(5)

      # 尾盘处理
      if remaining > 0:
          current_price = self.qmt.get_optimal_buy_price(code, ref_price)
          if current_price and current_price <= ref_price:
              logger.info(f"尾盘补买 {code} {remaining}")
              self.qmt.place_order(code, 'buy', remaining, current_price)

      return remaining <= 0

    # 卖出
    def execute_sell(self, code: str, name: str, pre_close: float, target_volume: int) -> bool:
        if target_volume <= 0:
            return True
        logger.info(f"监控卖出 {code} {name} 昨收{pre_close:.2f} 目标股数{target_volume}")
        self._wait_until_start_time()

        # 辅助函数：查询当日该股票已成交的卖出股数
        def get_sold_volume() -> int:
            try:
                orders = self.qmt.xt_trader.query_stock_orders(self.qmt.account) or []
                sold = 0
                for order in orders:
                    if (order.m_strStockCode == code and
                        order.m_nOrderType == xtconstant.STOCK_SELL and
                        order.m_nOrderStatus in (xtconstant.ORDER_FILLED, xtconstant.ORDER_PARTIAL)):
                        sold += order.m_nFilledVolume
                return sold
            except Exception as e:
                logger.error(f"查询 {code} 卖出成交失败: {e}")
                return 0

        # 初始已成交数量
        sold = get_sold_volume()
        remaining = target_volume - sold
        if remaining <= 0:
            logger.info(f"{code} 已全部卖出，无需再操作")
            return True

        high_price = -float('inf')
        deadline = now_cn().replace(hour=Config.FORCE_DEADLINE_HOUR, minute=Config.FORCE_DEADLINE_MINUTE, second=0)
        total_entrusted = 0   # 累计已委托数量（包含已成交和未成交）

        loop_count = 0
        while now_cn() < deadline and remaining > 0:
            loop_count += 1
            if loop_count > 1000:
                logger.warning(f"{code} 循环超限，强制退出")
                break
            cur_price = self.qmt.get_optimal_sell_price(code, pre_close)
            if cur_price is None or cur_price <= 0:
                time.sleep(30)
                continue

            if cur_price > high_price:
                high_price = cur_price
                logger.debug(f"{code} 新高: {high_price:.2f}")

            if cur_price <= high_price * (1 - Config.SELL_DROP_RATIO):
                # 卖出防重复委托
                # 查询未成交委托
                def get_pending_sell_volume():
                    try:
                        orders = self.qmt.xt_trader.query_stock_orders(self.qmt.account) or []
                        pending = 0
                        for o in orders:
                            if (o.m_strStockCode == code and
                                o.m_nOrderType == xtconstant.STOCK_SELL and
                                o.m_nOrderStatus == xtconstant.ORDER_NOT_FILLED):
                                pending += o.m_nVolume - o.m_nFilledVolume
                        return pending
                    except:
                        return 0

                pending = get_pending_sell_volume()

                if pending >= remaining:
                    time.sleep(5)
                    continue


                #防止重复挂单放大
                if (total_entrusted - sold) > target_volume * 1.2:
                  logger.warning(f"{code} 卖出委托超限，停止")
                  break

                to_sell = remaining - (total_entrusted - sold)
                if to_sell <= 0:
                    time.sleep(30)
                    continue

                logger.info(f"{code} 触发卖出: 当前{cur_price:.2f} 最高{high_price:.2f} 回落{((high_price-cur_price)/high_price)*100:.2f}%")
                if self.qmt.place_order(code, 'sell', to_sell, cur_price):
                    total_entrusted += to_sell
                    # 下单后等待成交，然后查询最新成交数量
                    time.sleep(Config.PARTIAL_FILL_CHECK_INTERVAL)
                    sold = get_sold_volume()
                    remaining = target_volume - sold
                    if remaining <= 0:
                        break
                else:
                    logger.warning(f"{code} 卖出委托失败，稍后重试")
                    time.sleep(30)
            else:
                time.sleep(30)

        # 尾盘强制卖出（有保护价下限）
        if remaining > 0:
            logger.info(f"{code} 进入尾盘强制卖出，剩余 {remaining} 股")
            force_price = self.qmt.get_sell_price_unconstrained(code, pre_close)
            if force_price is None or force_price <= 0:
                logger.warning(f"{code} 无法获取市价，放弃强制卖出")
                return False
            if self.qmt.is_limit_up_down(code, force_price, 'sell'):
                logger.warning(f"{code} 已跌停，无法卖出")
                return False
            logger.info(f"尾盘强制卖出 {code} {remaining}股 @ {force_price:.2f} (保护价约束)")
            if self.qmt.place_order(code, 'sell', remaining, force_price):
                time.sleep(Config.PARTIAL_FILL_CHECK_INTERVAL)
                final_sold = get_sold_volume()
                if target_volume - final_sold > 0:
                    logger.warning(f"{code} 尾盘强制卖出后仍有 {target_volume - final_sold} 股未成交")
                    return False
                self._record_trade(code, name, '强制卖出', remaining, force_price)
                return True
            else:
                logger.error(f"{code} 尾盘强制卖出失败")
                return False

        logger.info(f"{code} 卖出完成")
        return True

    def _record_trade(self, code, name, direction, volume, price):
        self.today_trades.append({
            '时间': now_cn().strftime('%Y-%m-%d %H:%M:%S'),
            '股票代码': code,
            '股票名称': name,
            '方向': direction,
            '委托数量': volume,
            '成交价': price,
            '成交金额': volume * price,
        })

# ========================= 信号生成器 =========================
class TradeSignalGenerator:
    @staticmethod
    def calculate_target_volume(total_asset: float, target_weight: float, price: float) -> int:
        target_value = total_asset * target_weight
        return max(0, int(target_value / price / 100) * 100)

    @staticmethod
    def generate_orders(current_positions: Dict, target_holdings: List[Dict],
                    total_asset: float, position_factor: float,
                    available_cash: float, qmt_client) -> Tuple[List[Dict], List[Dict]]:
        # 引入资金使用比例（例如 0.5 表示只用一半资金）
        trade_ratio = getattr(Config, 'TRADE_RATIO', 1.0)
        effective_total_asset = total_asset * trade_ratio
        risk_adjusted_asset = effective_total_asset * position_factor   # 风控后资产
        effective_available_cash = available_cash * trade_ratio

        target_dict = {}
        for h in target_holdings:
            code = h['code']
            effective_weight = h['weight'] * position_factor
            price = qmt_client.get_optimal_buy_price(code, h['ref_price'])
            if price is None or price <= 0:
                price = h['ref_price']
            # 使用缩放后的总资产计算目标股数
            target_vol = TradeSignalGenerator.calculate_target_volume(risk_adjusted_asset, effective_weight, price)
            if target_vol > 0:
                target_dict[code] = {
                    'volume': target_vol,
                    'price': price,
                    'name': h['name'],
                    'ref_price': h['ref_price']
                }

        current_dict = {code: {'volume': pos['volume'], 'can_sell': pos['can_sell']}
                        for code, pos in current_positions.items()}

        sell_orders = []
        for code, cur in current_dict.items():
            target_vol = target_dict.get(code, {}).get('volume', 0)
            if cur['volume'] > target_vol:
                sell_vol = min(cur['can_sell'], cur['volume'] - target_vol)
                if sell_vol > 0:
                    pre_close = qmt_client.get_pre_close(code)
                    if pre_close is None:
                        logger.warning(f"{code} 无法获取昨收，跳过卖出")
                        continue
                    sell_orders.append({'code': code, 'volume': sell_vol, 'name': code, 'pre_close': pre_close})

        buy_orders = []
        estimated_cost = 0.0
        for code, target in target_dict.items():
            cur_vol = current_dict.get(code, {}).get('volume', 0)
            if target['volume'] > cur_vol:
                buy_vol = target['volume'] - cur_vol
                buy_vol = (buy_vol // 100) * 100
                if buy_vol > 0:
                    buy_orders.append({'code': code, 'volume': buy_vol, 'name': target['name'], 'ref_price': target['ref_price']})
                    estimated_cost += buy_vol * target['price']

        # 资金不足时缩减，使用缩放后的可用现金
        if estimated_cost > effective_available_cash + 1e-6:
            ratio = effective_available_cash / estimated_cost
            logger.warning(f"资金不足，缩减买入量，比例 {ratio:.2f}")
            for o in buy_orders:
                o['volume'] = int(o['volume'] * ratio / 100) * 100
            buy_orders = [o for o in buy_orders if o['volume'] > 0]

        return buy_orders, sell_orders

# ========================= 业绩记录 =========================
class PerformanceEvaluator:
    @staticmethod
    def save_trades(trades: List[Dict]):
        if not trades:
            return
        df = pd.DataFrame(trades)
        if os.path.exists(Config.TRADE_RECORD_PATH):
            old = pd.read_excel(Config.TRADE_RECORD_PATH)
            df = pd.concat([old, df], ignore_index=True)
        os.makedirs(os.path.dirname(Config.TRADE_RECORD_PATH), exist_ok=True)
        df.to_excel(Config.TRADE_RECORD_PATH, index=False)
        logger.info(f"保存 {len(trades)} 条交易记录")

    @staticmethod
    def save_position_snapshot(positions: Dict, total_asset: float):
        records = [{'日期': now_cn().strftime('%Y-%m-%d'), '股票代码': code,
                    '持股数量': pos['volume'], '可卖数量': pos.get('can_sell', pos['volume']), '成本价': pos['avg_price']}
                   for code, pos in positions.items()]
        records.append({'日期': now_cn().strftime('%Y-%m-%d'), '股票代码': 'TOTAL', '总资产': total_asset})
        df = pd.DataFrame(records)
        if os.path.exists(Config.POSITION_SNAPSHOT_PATH):
            old = pd.read_excel(Config.POSITION_SNAPSHOT_PATH)
            df = pd.concat([old, df], ignore_index=True)
        os.makedirs(os.path.dirname(Config.POSITION_SNAPSHOT_PATH), exist_ok=True)
        df.to_excel(Config.POSITION_SNAPSHOT_PATH, index=False)
        logger.info("保存持仓快照")

# ========================= 主机器人 =========================
class SIRIUSBot:
    def __init__(self):
        self.qmt = QMTClient()
        self.model_loader = ModelLoader()
        self.signal_gen = TradeSignalGenerator()
        self.evaluator = PerformanceEvaluator()

    def run_once(self):
        logger.info("========== SIRIUS Bot 开始运行 ==========")
        model_data = self.model_loader.load_latest_model()
        if not model_data:
            logger.error("模型加载失败")
            return
        target_holdings, position_factor = self.model_loader.parse_model(model_data)
        if not target_holdings:
            logger.error("无有效目标持仓")
            return

        account = self.qmt.get_account_info()
        if not account:
            logger.error("获取账户信息失败")
            return
        total_asset = account['total_asset']
        available_cash = account['available_cash']
        logger.info(f"总资产: {total_asset:.2f}, 可用资金: {available_cash:.2f}")

        current_positions = self.qmt.get_positions()
        buy_orders, sell_orders = self.signal_gen.generate_orders(
            current_positions, target_holdings, total_asset, position_factor, available_cash, self.qmt
        )
        logger.info(f"生成买入 {len(buy_orders)} 条，卖出 {len(sell_orders)} 条")
        for o in buy_orders:
            logger.info(f"  买入 {o['code']} {o['volume']}股 基准价{o['ref_price']:.2f}")
        for o in sell_orders:
            logger.info(f"  卖出 {o['code']} {o['volume']}股 昨收{o['pre_close']:.2f}")

        executor = DynamicOrderExecutor(self.qmt)
        # 先卖后买
        for sell in sell_orders:
            executor.execute_sell(sell['code'], sell['name'], sell['pre_close'], sell['volume'])
            time.sleep(Config.ORDER_INTERVAL)
        for buy in buy_orders:
            executor.execute_buy(buy['code'], buy['name'], buy['ref_price'], buy['volume'])
            time.sleep(Config.ORDER_INTERVAL)

        if executor.today_trades:
            self.evaluator.save_trades(executor.today_trades)
        self.after_close()
        logger.info("========== 本次运行结束 ==========")

    def after_close(self):
        positions = self.qmt.get_positions()
        account = self.qmt.get_account_info()
        total = account.get('total_asset', 0) if account else 0
        self.evaluator.save_position_snapshot(positions, total)

# ========================= 入口 =========================
def is_trading_time() -> bool:
    now = now_cn()
    if now.weekday() >= 5:
        return False
    t = now.time()
    return (datetime.strptime("09:30", "%H:%M").time() <= t <= datetime.strptime("11:30", "%H:%M").time()) or \
           (datetime.strptime("13:00", "%H:%M").time() <= t <= datetime.strptime("15:00", "%H:%M").time())

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--mode', choices=['once', 'daemon'], default='once')
    args = parser.parse_args()

    bot = SIRIUSBot()
    if not bot.qmt.connect():
        logger.error("QMT 连接失败")
        sys.exit(1)

    if args.mode == 'once':
        bot.run_once()
    else:
        last_date = None
        while True:
            try:
                now = now_cn()
                today = now.strftime("%Y-%m-%d")

                # ✅ 仅在交易时间触发
                if is_trading_time():

                    # ✅ 防止重复执行
                    if last_date != today:
                        logger.info(f"进入交易时间，开始调仓 {today}")

                        try:
                            bot.run_once()
                            last_date = today
                        except Exception as e:
                            logger.error(f"run_once 执行异常: {e}", exc_info=True)

                else:
                    # ✅ 非交易时间重置（关键！防止跨天问题）
                    if last_date != today:
                        logger.info(f"非交易时间，等待开盘... 当前日期: {today}")

                # ✅ 更灵敏轮询（如需修改为 → 30秒）
                #time.sleep(30)
                time.sleep(5 + random.uniform(0, 2))

            except Exception as e:
                logger.error(f"主循环异常: {e}", exc_info=True)
                time.sleep(10)  # 防止异常风暴
