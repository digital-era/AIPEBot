#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
miniQMT 实盘网格交易策略 - 
策略参数：
  - 网格下沿, 网格上沿
  - 网格数量, 间距
  - 胜率, 最大回撤
  
运行前准备：
  1. 安装 xtquant: pip install xtquant
  2. 启动 miniQMT 客户端并登录
  3. 修改配置区的路径和账号信息
"""

import os
import sys
import time
import json
import logging
import datetime
from dataclasses import dataclass, asdict
from typing import Dict, List, Optional, Tuple
from pathlib import Path

# miniQMT 核心库
from xtquant.xttrader import XtQuantTrader, XtQuantTraderCallback
from xtquant.xttype import StockAccount
from xtquant import xtconstant
from xtquant import xtdata

# ==================== 配置区 ====================

class Config:
    """策略配置"""
    # miniQMT 客户端路径（根据实际安装路径修改）
    MINI_QMT_PATH = r"D:\国金证券QMT交易端\userdata_mini"
    
    # 交易账号（替换为您的资金账号）
    ACCOUNT_ID = ""  # 请修改为您的实际资金账号
    
    # 标的配置
    STOCK_CODE = "603379.SH"   # 三美股份
    STOCK_NAME = "三美股份"
    
    # 网格参数
    GRID_LOWER = 60.82          # 网格下沿
    GRID_UPPER = 66.35          # 网格上沿
    GRID_COUNT = 6             # 网格层数
    GRID_STEP = 0.9193         # 网格间距（程序会自动计算，此项为参考）
    
    # 交易参数
    SHARES_PER_GRID = 100     # 每格交易股数（100的整数倍）
    MAX_POSITION = 600        # 最大持仓股数（GRID_COUNT * SHARES_PER_GRID）
    MIN_POSITION = 0           # 最小持仓股数
    
    # 风控参数
    STOP_LOSS_PCT = 0.05       # 止损比例（跌破下沿5%止损）
    MAX_DAILY_TRADE = 3       # 单日最大交易次数
    COOLDOWN_SECONDS = 30      # 同方向交易冷却时间（秒）
    
    # 运行参数
    CHECK_INTERVAL = 3         # 行情检查间隔（秒）
    SESSION_ID = 123456        # 会话ID（不同策略使用不同ID）
    
    # 日志路径
    LOG_DIR = "./grid_logs"

# ==================== 数据结构 ====================

@dataclass
class GridLevel:
    """网格层级"""
    level: int          # 层级编号（0为最低）
    price: float        # 网格价格
    shares: int         # 该层目标持仓
    is_base: bool = False  # 是否基准层

@dataclass
class TradeRecord:
    """交易记录"""
    timestamp: str
    direction: str      # BUY/SELL
    price: float
    volume: int
    grid_level: int
    reason: str

# ==================== 日志配置 ====================

def setup_logging():
    """配置日志"""
    log_dir = Path(Config.LOG_DIR)
    log_dir.mkdir(exist_ok=True)
    
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = log_dir / f"grid_{Config.STOCK_CODE.replace('.', '_')}_{timestamp}.log"
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(message)s',
        handlers=[
            logging.FileHandler(log_file, encoding='utf-8'),
            logging.StreamHandler(sys.stdout)
        ]
    )
    return logging.getLogger("GridStrategy")

# ==================== 网格计算 ====================

class GridCalculator:
    """网格计算器"""
    
    def __init__(self, lower: float, upper: float, count: int, shares: int):
        self.lower = lower
        self.upper = upper
        self.count = count
        self.shares = shares
        self.grids: List[GridLevel] = []
        self._build_grids()
        self.available_shares = 0   # 当前可卖数量
    
    def _build_grids(self):
        """构建等差网格"""
        step = (self.upper - self.lower) / self.count
        
        for i in range(self.count + 1):
            price = round(self.lower + step * i, 3)
            # 持仓分配：越低价持仓越多
            target_shares = self.shares * (self.count - i)
            
            self.grids.append(GridLevel(
                level=i,
                price=price,
                shares=target_shares,
                is_base=(i == self.count // 2)  # 中间层为基准
            ))
        
        logger.info(f"网格构建完成，共{len(self.grids)}层:")
        for g in self.grids:
            logger.info(f"  Level {g.level}: 价格={g.price}, 目标持仓={g.shares}")
    
    def get_grid_level(self, price: float) -> int:
        """根据价格确定当前网格层级"""
        if price <= self.grids[0].price:
            return 0
        if price >= self.grids[-1].price:
            return self.count
        
        for i in range(len(self.grids) - 1):
            if self.grids[i].price <= price < self.grids[i+1].price:
                return i
        return self.count
    
    def get_target_shares(self, price: float) -> int:
        """获取目标持仓"""
        level = self.get_grid_level(price)
        return self.grids[level].shares
    
    def get_trade_signal(self, price: float, current_shares: int) -> Tuple[str, int, int]:
        """
        生成交易信号
        :return: (方向, 数量, 目标层级)
        """
        target = self.get_target_shares(price)
        diff = target - current_shares
        
        if diff > 0:
            return "BUY", diff, self.get_grid_level(price)
        elif diff < 0:
            return "SELL", abs(diff), self.get_grid_level(price)
        else:
            return "HOLD", 0, self.get_grid_level(price)

# ==================== 交易回调 ====================

class GridTradeCallback(XtQuantTraderCallback):
    """交易回调处理"""
    
    def __init__(self, strategy):
        self.strategy = strategy
        self.orders: Dict[str, dict] = {}  # 订单跟踪
    
    def on_disconnected(self):
        """连接断开"""
        logger.error("⚠️ miniQMT 连接断开！")
    
    def on_stock_order(self, order):
        """委托回报"""
        status_map = {
            48: "未报", 49: "待报", 50: "已报", 51: "已撤", 
            52: "部撤", 53: "已成交", 54: "部成"
        }
        status = status_map.get(order.order_status, f"未知({order.order_status})")
        logger.info(f"📋 委托回报: {order.stock_code} | 状态={status} | "
                   f"委托量={order.order_volume} | 价格={order.price}")
    
    def on_stock_trade(self, trade):
        """成交回报"""
        logger.info(f"✅ 成交确认: {trade.stock_code} | 方向={'买' if trade.order_type==xtconstant.STOCK_BUY else '卖'} | "
                   f"成交价={trade.traded_price} | 成交量={trade.traded_volume}")
        # 更新持仓
        self.strategy.update_position_after_trade(trade)
    
    def on_stock_asset(self, asset):
        """资金变动"""
        logger.info(f"💰 资金变动: 可用={asset.cash:.2f} | 总资产={asset.total_asset:.2f}")
    
    def on_stock_position(self, position):
        """持仓变动"""
        logger.info(f"📦 持仓变动: {position.stock_code} | 数量={position.volume} | "
                   f"可卖={position.can_use_volume}")
    
    def on_order_error(self, order_error):
        """委托失败"""
        logger.error(f"❌ 委托失败: 订单ID={order_error.order_id} | "
                    f"错误码={order_error.error_id} | {order_error.error_msg}")
    
    def on_cancel_error(self, cancel_error):
        """撤单失败"""
        logger.error(f"❌ 撤单失败: 订单ID={cancel_error.order_id} | "
                    f"错误码={cancel_error.error_id} | {cancel_error.error_msg}")

# ==================== 主策略类 ====================

class GridStrategy:
    """网格交易策略主类"""
    
    def __init__(self):
        self.logger = logger
        self.config = Config()
        
        # 初始化网格
        self.grid_calc = GridCalculator(
            self.config.GRID_LOWER,
            self.config.GRID_UPPER,
            self.config.GRID_COUNT,
            self.config.SHARES_PER_GRID
        )
        
        # 交易状态
        self.current_shares = 0          # 当前持仓
        self.current_level = -1          # 当前网格层级
        self.daily_trade_count = 0       # 当日交易次数
        self.last_trade_time = 0         # 上次交易时间
        self.last_trade_direction = None # 上次交易方向
        self.is_running = False
        
        # 数据存储
        self.trade_history: List[TradeRecord] = []
        self.state_file = Path("grid_state.json")
        
        # 加载状态
        self._load_state()

        self.pending_orders: Dict[int, int] = {}  # 订单ID -> 委托数量
        
        # 初始化 miniQMT
        self._init_trader()

        
    
    def _init_trader(self):
        """初始化交易接口"""
        self.xt_trader = XtQuantTrader(
            self.config.MINI_QMT_PATH,
            self.config.SESSION_ID
        )
        self.account = StockAccount(self.config.ACCOUNT_ID)
        self.callback = GridTradeCallback(self)
        self.xt_trader.register_callback(self.callback)
    
    def _load_state(self):
        """加载持久化状态"""
        if self.state_file.exists():
            try:
                with open(self.state_file, 'r') as f:
                    state = json.load(f)
                self.current_shares = state.get('shares', 0)
                self.daily_trade_count = state.get('daily_trades', 0)
                self.logger.info(f"📂 加载历史状态: 持仓={self.current_shares}, 日交易={self.daily_trade_count}")
            except Exception as e:
                self.logger.warning(f"状态加载失败: {e}")
    
    def _save_state(self):
        """保存状态"""
        state = {
            'shares': self.current_shares,
            'daily_trades': self.daily_trade_count,
            'last_update': datetime.datetime.now().isoformat()
        }
        with open(self.state_file, 'w') as f:
            json.dump(state, f)
    
    def update_position_after_trade(self, trade):
        if trade.stock_code != self.config.STOCK_CODE:
            return
        if trade.order_type == xtconstant.STOCK_BUY:
            self.current_shares += trade.traded_volume
            # 买入当天不可卖，available_shares 不变
        else:
            self.current_shares -= trade.traded_volume
            self.available_shares -= trade.traded_volume  # 卖出会消耗可卖量
        self._save_state()
        self.logger.info(f"📊 持仓更新: 总={self.current_shares}, 可卖={self.available_shares}")
    
    def get_market_price(self) -> Optional[float]:
        """获取最新市场价格"""
        try:
            # 订阅实时行情
            xtdata.subscribe_quote(self.config.STOCK_CODE, period='tick')
            time.sleep(0.5)  # 等待数据推送
            
            # 获取最新分笔数据
            tick = xtdata.get_full_tick([self.config.STOCK_CODE])
            if tick and self.config.STOCK_CODE in tick:
                price = tick[self.config.STOCK_CODE]['lastPrice']
                return float(price)
            
            # 备用：获取K线最新价
            data = xtdata.get_market_data(
                ['close'], 
                [self.config.STOCK_CODE], 
                period='1m', 
                count=1
            )
            if data and self.config.STOCK_CODE in data:
                return float(data[self.config.STOCK_CODE]['close'][0])
                
        except Exception as e:
            self.logger.error(f"行情获取失败: {e}")
        return None
    
    def check_risk(self, price: float, direction: str) -> Tuple[bool, str]:
        """风险检查"""
        stop_loss_price = self.config.GRID_LOWER * (1 - self.config.STOP_LOSS_PCT)
        if price < stop_loss_price:
            self.logger.warning("止损触发，执行紧急平仓！")
            self.emergency_close()
            return False, "止损已执行"
    
        if self.daily_trade_count >= self.config.MAX_DAILY_TRADE:
            return False, f"日交易次数超限: {self.daily_trade_count}"
    
        if (direction == self.last_trade_direction and 
                time.time() - self.last_trade_time < self.config.COOLDOWN_SECONDS):
            return False, "同方向冷却中"
    
        if direction == "BUY" and self.current_shares >= self.config.MAX_POSITION:
            return False, f"持仓已达上限: {self.current_shares}"
    
        return True, "通过"
    
    def execute_trade(self, direction: str, volume: int, price: float, level: int):
        """执行交易"""
        if direction == "HOLD" or volume <= 0:
            return

        if direction == "BUY":
            volume = (volume // 100) * 100
            if volume <= 0:
                return
        
        # 确定委托类型
        if direction == "BUY":
            order_type = xtconstant.STOCK_BUY
            price_type = xtconstant.FIX_PRICE  # 限价单
            # 买入挂低一档（确保成交）
            order_price = round(price * 0.998, 2)
        else:
            order_type = xtconstant.STOCK_SELL
            price_type = xtconstant.FIX_PRICE
            # 卖出挂高一档
            order_price = round(price * 1.002, 2)
        
        self.logger.info(f"🚀 发起{direction}: {self.config.STOCK_CODE} | "
                        f"价格={order_price} | 数量={volume} | 网格层={level}")
        
        try:
            order_id = self.xt_trader.order_stock(
                self.account,
                self.config.STOCK_CODE,
                order_type,
                volume,
                price_type,
                order_price,
                strategy_name="GridStrategy",
                order_remark=f"Grid_L{level}"
            )
            
            if order_id and order_id > 0:
                self.logger.info(f"✅ 委托成功: 订单ID={order_id}")
                self.daily_trade_count += 1
                self.last_trade_time = time.time()
                self.last_trade_direction = direction
                
                # 记录交易
                record = TradeRecord(
                    timestamp=datetime.datetime.now().isoformat(),
                    direction=direction,
                    price=order_price,
                    volume=volume,
                    grid_level=level,
                    reason=f"网格触发_L{level}"
                )
                self.trade_history.append(record)
                self._save_state()
            else:
                self.logger.error(f"❌ 委托失败: 返回订单ID异常={order_id}")
                
        except Exception as e:
            self.logger.error(f"❌ 交易异常: {e}")
    
    def run_once(self):
        #单次策略循环
        # 1. 先同步持仓
        self._sync_position()
      
        # 获取行情
        price = self.get_market_price()
        if price is None:
            self.logger.warning("未能获取行情，跳过本次检查")
            return
        
        # 计算网格信号
        direction, volume, level = self.grid_calc.get_trade_signal(price, self.current_shares)

        # T+1 限制：卖出量不能超过可卖数量
        if direction == "SELL":
            if volume > self.available_shares:
                self.logger.info(f"⚠️ 卖出量 {volume} 超过可卖 {self.available_shares}，截断为可卖量")
                volume = self.available_shares
            if volume <= 0:
                # 无可卖，暂不操作，但更新层级避免重复日志
                self.current_level = level
                return
        
        # 2. 如果层级未变且方向为HOLD，不操作
        if level == self.current_level and direction == "HOLD":
            self.current_level = level
            return

        # 3. 同方向且同层级，检查是否有未完成的委托
        if (direction == self.last_trade_direction and 
            level == self.current_level and 
            self.has_pending_order(direction)):
            self.logger.info(f"已有同方向委托未完成，跳过")
            return
        
        self.logger.info(f"📈 价格={price:.3f} | 当前层={self.current_level} | "
                        f"目标层={level} | 方向={direction} | 数量={volume}")
        
        # 风控检查
        if direction != "HOLD":
            passed, reason = self.check_risk(price, direction)
            if not passed:
                self.logger.warning(f"⛔ 风控拦截: {reason}")
                return
            
            # 执行交易
            self.execute_trade(direction, volume, price, level)
            self.current_level = level

    def has_pending_order(self, direction: str) -> bool:
        """检查是否有同方向未完成委托"""
        try:
            # 查询当日该股票的所有订单
            orders = self.xt_trader.query_stock_orders(
                self.account, self.config.STOCK_CODE
            )
            # 未完成状态: 未报(48)、待报(49)、已报(50)、部成(54)
            unfinished = {48, 49, 50, 54}
            for order in orders:
                if order.order_status in unfinished:
                    if direction == "BUY" and order.order_type == xtconstant.STOCK_BUY:
                        return True
                    if direction == "SELL" and order.order_type == xtconstant.STOCK_SELL:
                        return True
        except Exception as e:
            self.logger.warning(f"查询未完成委托失败: {e}")
        return False

    def _sync_position(self):
        try:
            positions = self.xt_trader.query_stock_positions(self.account)
            for pos in positions:
                if pos.stock_code == self.config.STOCK_CODE:
                    if pos.volume != self.current_shares or pos.can_use_volume != self.available_shares:
                        self.logger.info(f"持仓同步: 总持仓 {self.current_shares}->{pos.volume}, "
                                         f"可卖 {self.available_shares}->{pos.can_use_volume}")
                        self.current_shares = pos.volume
                        self.available_shares = pos.can_use_volume
                        self._save_state()
                    return
            # 无持仓
            if self.current_shares != 0:
                self.logger.info(f"持仓同步: 总持仓 {self.current_shares}->0, 可卖 {self.available_shares}->0")
                self.current_shares = 0
                self.available_shares = 0
                self._save_state()
        except Exception as e:
            self.logger.error(f"同步持仓失败: {e}")
    
    def emergency_close(self):
        """紧急平仓（止损或收盘）"""
        self.logger.warning("🚨 执行紧急平仓！")
        if self.current_shares > 0:
            self.execute_trade("SELL", self.current_shares, 
                             self.get_market_price() or self.config.GRID_LOWER, -1)
    
    def run(self):
        """主运行循环"""
        self.logger.info("=" * 60)
        self.logger.info("🎯 网格策略启动")
        self.logger.info(f"标的: {self.config.STOCK_NAME}({self.config.STOCK_CODE})")
        self.logger.info(f"网格范围: {self.config.GRID_LOWER} - {self.config.GRID_UPPER}")
        self.logger.info(f"初始持仓: {self.current_shares}")
        self.logger.info("=" * 60)
        
        # 启动交易线程
        self.xt_trader.start()
        connect_result = self.xt_trader.connect()
        if connect_result != 0:
            self.logger.error(f"miniQMT 连接失败: {connect_result}")
            return
        
        # 订阅交易主推
        subscribe_result = self.xt_trader.subscribe(self.account)
        self.logger.info(f"交易订阅结果: {subscribe_result}")
        
        self.is_running = True
        # 启动时同步一次实际持仓，避免本地状态偏差
        self._sync_position()
        
        try:
            while self.is_running:
                # 检查交易时间（9:30-11:30, 13:00-15:00）
                now = datetime.datetime.now()
                if not self._is_trading_time(now):
                    if now.hour == 15 and now.minute == 0:
                        self.logger.info("收盘，保存状态...")
                        self._save_state()
                    time.sleep(60)
                    continue
                
                # 重置日计数（开盘时）
                today = now.date()
                if not hasattr(self, '_last_trade_date') or self._last_trade_date != today:
                    self.daily_trade_count = 0
                    self._last_trade_date = today
                
                # 执行策略
                self.run_once()
                time.sleep(self.config.CHECK_INTERVAL)
                
        except KeyboardInterrupt:
            self.logger.info("收到停止信号...")
        except Exception as e:
            self.logger.error(f"策略异常: {e}")
        finally:
            self._shutdown()
    
    def _is_trading_time(self, dt: datetime.datetime) -> bool:
        """判断是否在交易时间"""
        if dt.weekday() >= 5:  # 周末
            return False
        t = dt.time()
        morning = datetime.time(9, 30) <= t <= datetime.time(11, 30)
        afternoon = datetime.time(13, 0) <= t <= datetime.time(15, 0)
        return morning or afternoon
    
    def _shutdown(self):
        """优雅关闭"""
        self.is_running = False
        self._save_state()
        
        # 保存交易记录
        if self.trade_history:
            record_file = Path(f"trades_{datetime.date.today()}.json")
            with open(record_file, 'w') as f:
                json.dump([asdict(r) for r in self.trade_history], f, indent=2)
            self.logger.info(f"交易记录已保存: {record_file}")
        
        self.xt_trader.stop()
        self.logger.info("策略已停止")

# ==================== 入口 ====================

if __name__ == "__main__":
    # 初始化日志
    logger = setup_logging()
    
    # 创建并运行策略
    strategy = GridStrategy()
    
    try:
        strategy.run()
    except Exception as e:
        logger.critical(f"致命错误: {e}")
        sys.exit(1)
