# -*- coding: gbk -*-
# 国金QMT 内置Python 策略模型（标准QMT客户端框架，不依赖miniQMT）
# 注意：此策略需在QMT中选择"1分钟线"周期运行！
# T1开盘(09:31)对手价买入，T5收盘前(14:50)对手价清仓，尾盘MA20风控
import os
import re
import json
import time
from datetime import datetime, timedelta
import pandas as pd

# ========== 配置 ==========
JSON_DIR = r"E:/AIPEBot/backtest"
TRADE_AMOUNT = 20000
ACC_ID = ''          # 必须与策略绑定的资金账号一致
BUY_HMS = (13, 40)              # T1 开盘买入时点 (小时, 分钟)
SELL_HMS = (14, 50)            # T5 收盘卖出 / 每日风控检查时点

# ========== 模块级全局变量（替代ContextInfo属性）==========
G_INIT_DONE = False            # init是否执行过
G_IS_BOUGHT = False            # T1是否已买入
G_IS_SOLD = False             # T5是否已卖出
G_BUY_PLAN = {}               # 本地买入记录 {code: vol}
G_PLAN_STOCKS = []             # 交易计划股票列表
G_T0 = ''                     # T0日期
G_FINISHED = False            # 策略是否结束


# ---------- 工具函数 ----------
def _norm_date(x):
    s = str(x)[:10].replace('-', '').replace('/', '')
    if len(s) == 8 and s.isdigit():
        return '%s-%s-%s' % (s[:4], s[4:6], s[6:8])
    try:
        return pd.Timestamp(x).strftime('%Y-%m-%d')
    except Exception:
        return str(x)[:10]


def _norm_code(code):
    """补全交易所后缀；已带后缀原样返回"""
    code = str(code).strip().upper()
    if '.' in code:
        return code
    if code.startswith(('6', '5', '9')):
        return code + '.SH'
    if code.startswith(('0', '1', '2', '3')):
        return code + '.SZ'
    if code.startswith(('4', '8')):
        return code + '.BJ'
    return code


def load_last_plan():
    pattern = re.compile(r'^5日周期交易计划_(\d{4})\.json$')
    max_year, max_file = -1, None
    for fname in os.listdir(JSON_DIR):
        m = pattern.match(fname)
        if m:
            y = int(m.group(1))
            if y > max_year:
                max_year, max_file = y, fname
    if max_file is None:
        print('[DEBUG load_last_plan] 未找到交易计划文件')
        return None
    print('[DEBUG load_last_plan] 找到文件: %s' % max_file)
    with open(os.path.join(JSON_DIR, max_file), 'r', encoding='utf-8') as f:
        plan = json.load(f)
    plan_list = plan.get('交易计划', [])
    if plan_list:
        print('[DEBUG load_last_plan] 交易计划条数: %d, 取最后一条' % len(plan_list))
    return plan_list[-1] if plan_list else None


def parse_stocks(last_plan):
    stocks = []
    for s in last_plan['股票明细']:
        stocks.append({
            'code': _norm_code(s['代码']),
            'name': s.get('名称', ''),
            'weight': float(str(s.get('权重(%)', '0')).replace('%', '')) / 100.0,
        })
    print('[DEBUG parse_stocks] 解析到 %d 只股票:' % len(stocks))
    for st in stocks:
        print('  %s %s 权重:%.1f%%' % (st['code'], st['name'], st['weight']*100))
    return stocks


def get_closes(ContextInfo, code, count):
    try:
        data = ContextInfo.get_market_data_ex(['close'], [code], period='1d',
                                              count=count, dividend_type='none')
        if data and code in data:
            df = data[code].dropna()
            return [float(x) for x in df['close'].tolist()]
    except Exception as e:
        print('[DEBUG get_closes] 获取行情失败 %s: %s' % (code, e))
    return []


def get_current_price(ContextInfo, code):
    """实时最新价，get_full_tick不可用时回退到最新收盘价"""
    try:
        tick = ContextInfo.get_full_tick([code])
        if tick and code in tick:
            d = tick[code]
            p = d.get('lastPrice', 0)
            if not p:
                ask = d.get('askPrice') or [0]
                bid = d.get('bidPrice') or [0]
                p = ask[0] or bid[0] or 0
            if p:
                print('[DEBUG get_current_price] %s 实时价: %.3f' % (code, p))
                return float(p)
    except Exception as e:
        print('[DEBUG get_current_price] 获取实时价失败 %s: %s' % (code, e))
    closes = get_closes(ContextInfo, code, 5)
    p = closes[-1] if closes else 0.0
    print('[DEBUG get_current_price] %s 回退到收盘价: %.3f' % (code, p))
    return p


def get_trading_days(ContextInfo, start, end):
    print('[DEBUG get_trading_days] 查询范围: %s ~ %s' % (start, end))
    try:
        data = ContextInfo.get_market_data_ex(['close'], ['000001.SH'], period='1d',
                                              start_time=start, end_time=end,
                                              dividend_type='none')
        if data and '000001.SH' in data:
            df = data['000001.SH'].dropna()
            days = [_norm_date(x) for x in df.index.tolist()]
            print('[DEBUG get_trading_days] 获取到 %d 个交易日: %s' % (len(days), days))
            return days
    except Exception as e:
        print('[DEBUG get_trading_days] 获取交易日历失败: %s' % e)
    return []


def in_trading_window(now):
    """判断是否在交易时段内"""
    hm = now.hour * 60 + now.minute
    result = (571 <= hm <= 690) or (781 <= hm <= 899)
    print('[DEBUG in_trading_window] hm=%d, 交易时段=%s' % (hm, result))
    return result


# ---------- 交易函数：对手价下单 ----------
def order_stock(ContextInfo, code, volume, side):
    """
    正确的passorder调用
    参数顺序: opType, orderType, accountID, orderCode, prType, price, volume, strategyName, quickTrade, ContextInfo
    """
    op = 23 if side == 'buy' else 24
    orderType = 1101
    accountID = ACC_ID
    orderCode = code
    prType = 4 if side == 'buy' else 6
    price = -1.0
    strategyName = 'strategy_' + side
    quickTrade = 2
    
    print('[ORDER] 调用passorder: op=%d, orderType=%d, account=%s, code=%s, prType=%d, price=%.2f, vol=%d, strategy=%s, quick=%d' % (
        op, orderType, accountID, orderCode, prType, price, volume, strategyName, quickTrade))
    
    try:
        passorder(op, orderType, accountID, orderCode, prType, price, volume, strategyName, quickTrade, ContextInfo)
        print('[ORDER] %s %s %d股 [对手价]' % (side, code, volume))
        return True
    except Exception as e:
        print('[ORDER] 下单失败 %s: %s' % (code, str(e)))
        return False


def get_positions(ContextInfo):
    """返回 {code: 可用数量}，T+1用m_nCanUseVolume"""
    print('[DEBUG get_positions] 查询持仓...')
    pos = {}
    try:
        positions = get_trade_detail_data(ACC_ID, 'STOCK', 'POSITION')
        if positions:
            print('[DEBUG get_positions] 获取到 %d 个持仓记录' % len(positions))
            for p in positions:
                vol = getattr(p, 'm_nCanUseVolume', 0)
                if vol and vol > 0:
                    code = '%s.%s' % (p.m_strInstrumentID, p.m_strExchangeID)
                    pos[code] = int(vol)
                    print('[DEBUG get_positions] 持仓: %s %d股' % (code, vol))
        else:
            print('[DEBUG get_positions] 无持仓记录')
    except Exception as e:
        print('[DEBUG get_positions] 查询持仓异常: %s' % e)
    return pos


def get_available_cash(ContextInfo):
    """查询可用资金"""
    print('[DEBUG get_available_cash] 查询资金...')
    try:
        account = get_trade_detail_data(ACC_ID, 'STOCK', 'ACCOUNT')
        if account:
            cash = getattr(account[0], 'm_dAvailable', None)
            if cash is None:
                cash = getattr(account[0], 'm_dCash', 0)
            print('[DEBUG get_available_cash] 可用资金: %.2f' % cash)
            return float(cash)
        else:
            print('[DEBUG get_available_cash] 无账号信息')
    except Exception as e:
        print('[DEBUG get_available_cash] 查询资金异常: %s' % e)
    return 0.0


def get_day_index(ContextInfo, today):
    """动态计算今天是T0之后的第几个交易日（0=T1, 4=T5），-1=不在窗口内"""
    print('[DEBUG get_day_index] G_T0=%s, today=%s' % (G_T0, today))
    
    start = G_T0.replace('-', '')
    today_str = today.replace('-', '')
    days_up_to_today = get_trading_days(ContextInfo, start, today_str)

    t0_index = -1
    for i, d in enumerate(days_up_to_today):
        if d == G_T0:
            t0_index = i
            break

    print('[DEBUG get_day_index] T0在交易日历中的位置: %d' % t0_index)
    
    if t0_index == -1:
        print('[DEBUG get_day_index] T0不在交易日历中，返回-1')
        return -1

    days_after_t0 = days_up_to_today[t0_index + 1:]
    print('[DEBUG get_day_index] T0之后的交易日: %s' % days_after_t0)
    
    if not days_after_t0:
        print('[DEBUG get_day_index] T0之后无交易日，返回-1')
        return -1

    day_index = len(days_after_t0) - 1
    print('[DEBUG get_day_index] 今天是T0之后第%d个交易日，day_index=%d' % (day_index + 1, day_index))
    return day_index


# ---------- 框架入口 ----------
def init(ContextInfo):
    global G_INIT_DONE, G_IS_BOUGHT, G_IS_SOLD, G_BUY_PLAN, G_PLAN_STOCKS, G_T0, G_FINISHED
    
    print('=' * 60)
    print('[INIT] 策略启动')
    print('[INIT] 配置参数: BUY_HMS=%s, SELL_HMS=%s, TRADE_AMOUNT=%d, ACC_ID=%s' % (
        BUY_HMS, SELL_HMS, TRADE_AMOUNT, ACC_ID))
    print('=' * 60)
    
    ContextInfo.accID = ACC_ID

    last_plan = load_last_plan()
    if last_plan is None:
        print('[INIT] 无交易计划，策略空转')
        G_FINISHED = True
        return
    
    G_T0 = last_plan['调仓日期']
    G_PLAN_STOCKS = parse_stocks(last_plan)
    print('[INIT] 最后调仓日期T0: %s, 股票数: %d' % (G_T0, len(G_PLAN_STOCKS)))

    today = datetime.now().strftime('%Y-%m-%d')
    print('[INIT] 今天: %s' % today)
    
    day_index = get_day_index(ContextInfo, today)

    if day_index < 0:
        if today == G_T0:
            print('[INIT] 今天就是T0（调仓日），无需执行买入，策略退出')
        else:
            print('[INIT] T0之后暂无交易日数据，可能是非交易日或数据未就绪，策略退出')
        G_FINISHED = True
        return

    if day_index > 4:
        print('[INIT] 已超过T5（当前为第%d个交易日），策略退出' % (day_index + 1))
        G_FINISHED = True
        return

    day_names = ['T1', 'T2', 'T3', 'T4', 'T5']
    print('[INIT] T0=%s, 今天=%s (第%d个交易日 %s)' % (
        G_T0, today, day_index + 1, day_names[day_index]))

    G_IS_BOUGHT = False
    G_IS_SOLD = False
    G_FINISHED = False
    G_BUY_PLAN = {}
    for st in G_PLAN_STOCKS:
        G_BUY_PLAN[st['code']] = 0
    G_INIT_DONE = True
    print('[INIT] 初始化完成, 等待handlebar驱动...')


def handlebar(ContextInfo):
    """每分钟K线驱动"""
    global G_IS_BOUGHT, G_IS_SOLD, G_BUY_PLAN, G_FINISHED, G_INIT_DONE

    # ===== 关键修复1：跳过历史K线回放，只处理最新K线 =====
    if not ContextInfo.is_last_bar():
        return

    # ===== 关键修复2：init未完成时跳过 =====
    if not G_INIT_DONE:
        print('[HANDLEBAR] init未完成，跳过')
        return

    now = datetime.now()
    print('-' * 40)
    print('[HANDLEBAR] 触发时间: %s' % now.strftime('%Y-%m-%d %H:%M:%S'))
    
    if G_FINISHED:
        print('[HANDLEBAR] G_FINISHED=True, 跳过')
        return

    today = now.strftime('%Y-%m-%d')
    now_hm = (now.hour, now.minute)
    print('[HANDLEBAR] today=%s, now_hm=%s' % (today, now_hm))

    if not in_trading_window(now):
        print('[HANDLEBAR] 非交易时段，跳过')
        return

    day_index = get_day_index(ContextInfo, today)
    print('[HANDLEBAR] day_index=%d' % day_index)
    
    if day_index < 0 or day_index > 4:
        print('[HANDLEBAR] day_index不在0~4范围内，跳过')
        return

    # ===== T1 开盘买入 =====
    if day_index == 0 and not G_IS_BOUGHT:
        print('[HANDLEBAR] T1买入条件: day_index==0 and not G_IS_BOUGHT')
        if now_hm < BUY_HMS:
            print('[HANDLEBAR] 未到买入时间 %s (当前%s)，跳过' % (BUY_HMS, now_hm))
            return
            
        print('[HANDLEBAR] >>> 开始执行T1买入 <<<')
        # ★★★ 关键修复3：立即设置标志位，防止重复下单 ★★★
        G_IS_BOUGHT = True
        
        # 清仓旧持仓
        positions = get_positions(ContextInfo)
        if positions:
            print('[HANDLEBAR] 清仓旧持仓...')
            for code, vol in positions.items():
                order_stock(ContextInfo, code, vol, 'sell')
            print('[HANDLEBAR] 等待卖单回报3秒...')
            time.sleep(3)
        else:
            print('[HANDLEBAR] 无旧持仓需要清仓')

        cash = get_available_cash(ContextInfo)
        total = min(TRADE_AMOUNT, cash)
        per = total / max(len(G_PLAN_STOCKS), 1)
        print('[HANDLEBAR] 总买入金额=%.2f, 每股分配=%.2f' % (total, per))
        
        for st in G_PLAN_STOCKS:
            # 跳过已经下单的股票
            if G_BUY_PLAN.get(st['code'], 0) > 0:
                print('[HANDLEBAR] %s 已下单，跳过' % st['code'])
                continue
            price = get_current_price(ContextInfo, st['code'])
            if price > 0:
                vol = int(per / price / 100) * 100
                print('[HANDLEBAR] %s: 价格=%.3f, 计划买入=%d股' % (st['code'], price, vol))
                if vol > 0:
                    if order_stock(ContextInfo, st['code'], vol, 'buy'):
                        G_BUY_PLAN[st['code']] = vol
                        print('[HANDLEBAR] %s 买入下单成功，记录数量=%d' % (st['code'], vol))
                    else:
                        G_BUY_PLAN[st['code']] = 0
                        print('[HANDLEBAR] %s 买入下单失败，将重试' % st['code'])
                else:
                    print('[HANDLEBAR] %s: 计算买入股数为0，跳过' % st['code'])
            else:
                print('[HANDLEBAR] %s: 无法获取价格，跳过' % st['code'])
        
        print('[HANDLEBAR] T1买入流程结束')
        print('[HANDLEBAR] 买入记录: %s' % str(G_BUY_PLAN))
        return

    # ===== T5 收盘清仓 =====
    if day_index == 4 and now_hm >= SELL_HMS:
        print('[HANDLEBAR] T5卖出条件: day_index==4 and now_hm>=%s' % str(SELL_HMS))
        if not G_IS_SOLD:
            print('[HANDLEBAR] >>> 开始执行T5清仓 <<<')
            G_IS_SOLD = True
            positions = get_positions(ContextInfo)
            for code, vol in positions.items():
                order_stock(ContextInfo, code, vol, 'sell')
            G_FINISHED = True
            print('[HANDLEBAR] T5清仓完成, 策略结束')
        return

    # ===== MA20 风控：尾盘检查 =====
    if day_index != 0 and day_index != 4 and now_hm >= SELL_HMS:
        print('[HANDLEBAR] MA20风控检查: day_index=%d, now_hm>=%s' % (day_index, str(SELL_HMS)))
        if G_IS_BOUGHT:
            positions = get_positions(ContextInfo)
            for code, vol in positions.items():
                closes = get_closes(ContextInfo, code, 21)
                print('[HANDLEBAR] %s: 获取到%d根收盘价' % (code, len(closes)))
                if len(closes) >= 21:
                    ma20 = sum(closes[-21:-1]) / 20.0
                    cur = closes[-1]
                    print('[HANDLEBAR] %s: 现价=%.3f, MA20=%.3f' % (code, cur, ma20))
                    if cur < ma20:
                        order_stock(ContextInfo, code, vol, 'sell')
                        print('[HANDLEBAR] 风控卖出 %s %d股' % (code, vol))
                    else:
                        print('[HANDLEBAR] %s 未触发风控卖出' % code)
        else:
            print('[HANDLEBAR] G_IS_BOUGHT=False, 跳过风控检查')
    else:
        if day_index != 0 and day_index != 4:
            print('[HANDLEBAR] 未到风控检查时间，跳过')
