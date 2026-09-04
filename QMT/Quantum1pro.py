# -*- coding: gbk -*-
# 国金QMT 内置Python 策略模型（标准QMT客户端框架，不依赖miniQMT）
# ====== 增强版：VWAP拉回分批买入 / 冲高分批卖出 / 尾盘强制兜底 ======
# 注意：此策略需在QMT中选择"1分钟线"周期运行！
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

DIP_PCT = 0.004                  # 买入触发：低于VWAP 0.4%
DEEP_DIP_PCT = 0.008             # 深跌触发：低于VWAP 0.8%，一次买满
SPIKE_PCT = 0.004                # 卖出触发：高于VWAP 0.4%
DEEP_SPIKE_PCT = 0.008           # 急拉触发：高于VWAP 0.8%，一次清仓
LOW_TOUCH_PCT = 0.002            # 接近当日低点阈值（买入）
HIGH_TOUCH_PCT = 0.002           # 接近当日高点阈值（卖出）
ORDER_COOLDOWN = 90              # 同一股票两次下单最小间隔（秒）
FORCE_BUY_HMS = (14, 40)         # 强制买入兜底时点
FORCE_SELL_HMS = (14, 50)        # 强制卖出/风控时点

# ========== 模块级全局变量 ==========
G_INIT_DONE = False
G_FINISHED = False
G_CLEARED = False
G_CLEAR_TIME = 0
G_BUY_ORDERED = {}              # {code: 已下单股数}
G_LAST_ORDER_TIME = {}           # {code: 上次下单时间戳}
G_T1_FINALIZED = False
G_BUY_PLAN = {}                  # {code: 计划买入股数}
G_PLAN_STOCKS = []               # 交易计划股票列表
G_T0 = ''
G_T5_FINALIZED = False


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
        print('[DEBUG get_closes] 获取日线失败 %s: %s' % (code, e))
    return []


def get_tick(ContextInfo, code):
    """返回 (last, vwap, day_high, day_low, is_limit_up)；失败返回 None"""
    try:
        tick = ContextInfo.get_full_tick([code])
        if not tick or code not in tick:
            return None
        d = tick[code]
        last = float(d.get('lastPrice') or 0)
        if last <= 0:
            ask = d.get('askPrice') or [0]
            bid = d.get('bidPrice') or [0]
            last = float(ask[0] or bid[0] or 0)
        if last <= 0:
            return None
        day_high = float(d.get('high') or 0)
        day_low = float(d.get('low') or 0)
        asks = d.get('askPrice') or []
        is_limit_up = (not asks or float(asks[0] or 0) <= 0)

        vwap = 0.0
        try:
            today = datetime.now().strftime('%Y%m%d')
            m = ContextInfo.get_market_data_ex(['close', 'volume'], [code],
                                               period='1m', start_time=today,
                                               end_time=today, dividend_type='none')
            if m and code in m:
                df = m[code].dropna()
                if len(df) > 0:
                    pv = (df['close'] * df['volume']).sum()
                    vv = df['volume'].sum()
                    if vv > 0:
                        vwap = float(pv / vv)
                        if day_high <= 0:
                            day_high = float(df['close'].max())
                        if day_low <= 0:
                            day_low = float(df['close'].min())
        except Exception as e:
            print('[DEBUG get_tick] 计算VWAP失败 %s: %s' % (code, e))
        return last, vwap, day_high, day_low, is_limit_up
    except Exception as e:
        print('[DEBUG get_tick] 获取tick失败 %s: %s' % (code, e))
        return None


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
        G_LAST_ORDER_TIME[code] = time.time()
        print('[ORDER] %s %s %d股 [对手价]' % (side, code, volume))
        return True
    except Exception as e:
        print('[ORDER] 下单失败 %s: %s' % (code, str(e)))
        return False


def get_positions(ContextInfo):
    """返回 {code: {'volume':总持仓, 'can_use':可用数量}}"""
    print('[DEBUG get_positions] 查询持仓...')
    pos = {}
    try:
        positions = get_trade_detail_data(ACC_ID, 'STOCK', 'POSITION')
        if positions:
            print('[DEBUG get_positions] 获取到 %d 个持仓记录' % len(positions))
            for p in positions:
                code = '%s.%s' % (p.m_strInstrumentID, p.m_strExchangeID)
                vol = int(getattr(p, 'm_nVolume', 0) or 0)
                can_use = int(getattr(p, 'm_nCanUseVolume', 0) or 0)
                pos[code] = {'volume': vol, 'can_use': can_use}
                print('[DEBUG get_positions] 持仓: %s 总量=%d 可用=%d' % (code, vol, can_use))
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


def in_trading_window(now):
    """判断是否在交易时段内"""
    hm = now.hour * 60 + now.minute
    result = (571 <= hm <= 690) or (781 <= hm <= 899)
    print('[DEBUG in_trading_window] hm=%d, 交易时段=%s' % (hm, result))
    return result


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


def _in_cooldown(code):
    last_t = G_LAST_ORDER_TIME.get(code, 0)
    in_cd = (time.time() - last_t) < ORDER_COOLDOWN
    if in_cd:
        print('[DEBUG _in_cooldown] %s 在冷却中, 剩余%ds' % (code, int(ORDER_COOLDOWN - (time.time() - last_t))))
    return in_cd


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


# ---------- 辅助 ----------
def _log_t1_done():
    print('===== T1 买入汇总 =====')
    for code, planned in G_BUY_PLAN.items():
        ordered = G_BUY_ORDERED.get(code, 0)
        print('  %s 计划%d股 已下买单%d股' % (code, planned, ordered))


# ---------- 框架入口 ----------
def init(ContextInfo):
    global G_INIT_DONE, G_FINISHED, G_CLEARED, G_CLEAR_TIME
    global G_BUY_ORDERED, G_LAST_ORDER_TIME, G_T1_FINALIZED
    global G_BUY_PLAN, G_PLAN_STOCKS, G_T0, G_T5_FINALIZED

    print('=' * 60)
    print('[INIT] 增强版策略启动')
    print('[INIT] 配置: TRADE_AMOUNT=%d, ACC_ID=%s' % (TRADE_AMOUNT, ACC_ID))
    print('[INIT] 买入参数: DIP=%.1f%%, DEEP_DIP=%.1f%%, COOLDOWN=%ds' % (
        DIP_PCT*100, DEEP_DIP_PCT*100, ORDER_COOLDOWN))
    print('[INIT] 卖出参数: SPIKE=%.1f%%, DEEP_SPIKE=%.1f%%' % (
        SPIKE_PCT*100, DEEP_SPIKE_PCT*100))
    print('[INIT] 兜底: FORCE_BUY=%s, FORCE_SELL=%s' % (FORCE_BUY_HMS, FORCE_SELL_HMS))
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

    # 初始化状态
    G_FINISHED = False
    G_CLEARED = False
    G_CLEAR_TIME = 0
    G_BUY_ORDERED = {}
    G_LAST_ORDER_TIME = {}
    G_T1_FINALIZED = False
    G_BUY_PLAN = {}
    G_T5_FINALIZED = False
    G_INIT_DONE = True
    print('[INIT] 初始化完成, 等待handlebar驱动...')


def handlebar(ContextInfo):
    """每分钟K线驱动"""
    global G_FINISHED, G_CLEARED, G_CLEAR_TIME, G_BUY_ORDERED
    global G_T1_FINALIZED, G_BUY_PLAN, G_T5_FINALIZED

    # ===== 跳过历史K线回放，只处理最新K线 =====
    if not ContextInfo.is_last_bar():
        return

    # init未完成时跳过
    if not G_INIT_DONE:
        return

    if G_FINISHED:
        return

    now = datetime.now()
    today = now.strftime('%Y-%m-%d')
    print('-' * 40)
    print('[HANDLEBAR] 触发时间: %s' % now.strftime('%Y-%m-%d %H:%M:%S'))

    if not in_trading_window(now):
        print('[HANDLEBAR] 非交易时段，跳过')
        return

    # ===== 动态计算今天在第几个交易日 =====
    day_index = get_day_index(ContextInfo, today)
    print('[HANDLEBAR] day_index=%d' % day_index)
    if day_index < 0 or day_index > 4:
        print('[HANDLEBAR] day_index不在0~4范围内，跳过')
        return

    # ================= T1 买入日 =================
    if day_index == 0:
        if G_T1_FINALIZED:
            print('[HANDLEBAR] T1已完成，跳过')
            return

        # ---- 09:31 清掉旧持仓 ----
        if not G_CLEARED:
            print('[HANDLEBAR] 买入日 %s 开盘清旧仓' % today)
            for code, p in get_positions(ContextInfo).items():
                if p['can_use'] > 0:
                    order_stock(ContextInfo, code, p['can_use'], 'sell')
            G_CLEARED = True
            G_CLEAR_TIME = time.time()
            return

        # 等待清仓回报（120秒）
        if time.time() - G_CLEAR_TIME < 120:
            print('[HANDLEBAR] 等待清仓回报, 已过%ds' % int(time.time() - G_CLEAR_TIME))
            return

        # ---- 核定买入计划（只核定一次）----
        if not G_BUY_PLAN:
            cash = get_available_cash(ContextInfo)
            total = min(TRADE_AMOUNT, cash)
            per = total / max(len(G_PLAN_STOCKS), 1)
            print('[HANDLEBAR] 总买入金额=%.2f, 每股分配=%.2f' % (total, per))
            for st in G_PLAN_STOCKS:
                t = get_tick(ContextInfo, st['code'])
                ref = t[0] if t else 0.0
                print('[HANDLEBAR] %s 参考价=%.3f' % (st['code'], ref))
                if ref > 0:
                    qty = int(per / ref / 100) * 100
                    if qty >= 100:
                        G_BUY_PLAN[st['code']] = qty
            print('[HANDLEBAR] 买入计划: %s' % str(G_BUY_PLAN))

            # 盘中重启保护：当前持仓视为已下单
            if G_BUY_PLAN:
                for code, p in get_positions(ContextInfo).items():
                    if code in G_BUY_PLAN and p['volume'] > 0:
                        G_BUY_ORDERED[code] = max(
                            G_BUY_ORDERED.get(code, 0), p['volume'])
                        print('[HANDLEBAR] 重启保护: %s 已有持仓%d股' % (code, p['volume']))

        now_hm = (now.hour, now.minute)

        # ---- 14:40 强制兜底，买满剩余额度 ----
        if now_hm >= FORCE_BUY_HMS:
            print('[HANDLEBAR] 进入强制买入兜底时段')
            for code, planned in G_BUY_PLAN.items():
                ordered = G_BUY_ORDERED.get(code, 0)
                remain = planned - ordered
                if remain >= 100 and not _in_cooldown(code):
                    if order_stock(ContextInfo, code, remain, 'buy'):
                        G_BUY_ORDERED[code] = ordered + remain
                        print('[HANDLEBAR] 强制补买 %s %d股' % (code, remain))
            _log_t1_done()
            G_T1_FINALIZED = True
            print('[HANDLEBAR] T1买入流程结束 (兜底完成)')
            return

        # ---- 盘中：拉回低位分批买入 ----
        for code, planned in G_BUY_PLAN.items():
            ordered = G_BUY_ORDERED.get(code, 0)
            remain = planned - ordered
            if remain < 100:
                continue
            if _in_cooldown(code):
                continue
            t = get_tick(ContextInfo, code)
            if t is None:
                continue
            last, vwap, day_high, day_low, is_limit_up = t
            if is_limit_up:
                print('[HANDLEBAR] %s 涨停，跳过' % code)
                continue

            deep_dip = (vwap > 0 and last <= vwap * (1 - DEEP_DIP_PCT))
            dip = (vwap > 0 and last <= vwap * (1 - DIP_PCT)) or \
                  (day_low > 0 and last <= day_low * (1 + LOW_TOUCH_PCT))

            if deep_dip:
                print('[HANDLEBAR] %s 深跌触发: last=%.3f, vwap=%.3f' % (code, last, vwap))
                if order_stock(ContextInfo, code, remain, 'buy'):
                    G_BUY_ORDERED[code] = ordered + remain
                    print('[HANDLEBAR] 深跌买入 %s %d股 @%.3f (VWAP %.3f)' % (code, remain, last, vwap))
            elif dip:
                batch = planned // 3 // 100 * 100
                if batch < 100:
                    batch = 100
                if batch > remain:
                    batch = remain
                print('[HANDLEBAR] %s 拉回触发: last=%.3f, vwap=%.3f' % (code, last, vwap))
                if order_stock(ContextInfo, code, batch, 'buy'):
                    G_BUY_ORDERED[code] = ordered + batch
                    print('[HANDLEBAR] 拉回买入 %s %d股 @%.3f (VWAP %.3f)' % (code, batch, last, vwap))
        return

    # ================= T5 卖出日 =================
    if day_index == 4:
        if G_T5_FINALIZED:
            print('[HANDLEBAR] T5已完成，跳过')
            return

        positions = get_positions(ContextInfo)
        now_hm = (now.hour, now.minute)

        # ---- 14:50 强制清仓兜底 ----
        if now_hm >= FORCE_SELL_HMS:
            print('[HANDLEBAR] 进入强制清仓兜底时段')
            for code, p in positions.items():
                if p['can_use'] > 0:
                    order_stock(ContextInfo, code, p['can_use'], 'sell')
            G_T5_FINALIZED = True
            G_FINISHED = True
            print('[HANDLEBAR] 卖出日 %s 尾盘强制清仓完成，策略结束' % today)
            return

        # ---- 盘中：冲高分批卖出 ----
        for code, p in positions.items():
            vol = p['can_use']
            if vol <= 0 or _in_cooldown(code):
                continue
            t = get_tick(ContextInfo, code)
            if t is None:
                continue
            last, vwap, day_high, day_low, is_limit_up = t

            deep_spike = (vwap > 0 and last >= vwap * (1 + DEEP_SPIKE_PCT))
            spike = (vwap > 0 and last >= vwap * (1 + SPIKE_PCT)) or \
                    (day_high > 0 and last >= day_high * (1 - HIGH_TOUCH_PCT))

            if deep_spike:
                print('[HANDLEBAR] %s 急拉触发: last=%.3f, vwap=%.3f' % (code, last, vwap))
                if order_stock(ContextInfo, code, vol, 'sell'):
                    print('[HANDLEBAR] 急拉清仓 %s %d股 @%.3f (VWAP %.3f)' % (code, vol, last, vwap))
            elif spike:
                batch = vol // 3 // 100 * 100
                if batch < 100:
                    batch = vol if vol < 200 else 100
                if batch > vol:
                    batch = vol
                print('[HANDLEBAR] %s 冲高触发: last=%.3f, vwap=%.3f' % (code, last, vwap))
                if order_stock(ContextInfo, code, batch, 'sell'):
                    print('[HANDLEBAR] 冲高卖出 %s %d股 @%.3f (VWAP %.3f)' % (code, batch, last, vwap))
        return

    # ================= T2~T4：MA20风控（尾盘）=================
    if (now.hour, now.minute) >= FORCE_SELL_HMS:
        print('[HANDLEBAR] MA20风控检查')
        for code, p in get_positions(ContextInfo).items():
            vol = p['can_use']
            if vol <= 0 or _in_cooldown(code):
                continue
            closes = get_closes(ContextInfo, code, 21)
            print('[HANDLEBAR] %s: 获取到%d根收盘价' % (code, len(closes)))
            if len(closes) >= 21:
                ma20 = sum(closes[-21:-1]) / 20.0
                cur = closes[-1]
                print('[HANDLEBAR] %s: 现价=%.3f, MA20=%.3f' % (code, cur, ma20))
                if cur < ma20:
                    if order_stock(ContextInfo, code, vol, 'sell'):
                        print('[HANDLEBAR] 风控卖出 %s %d股 (现价%.3f < MA20 %.3f)' % (code, vol, cur, ma20))
                else:
                    print('[HANDLEBAR] %s 未触发风控卖出' % code)
    else:
        print('[HANDLEBAR] 未到风控检查时间，跳过')
