# -*- coding: utf-8 -*-
# 国金QMT 内置Python 策略模型（标准QMT客户端框架，不依赖miniQMT）
# ====== 增强版：VWAP拉回分批买入 / 冲高分批卖出 / 尾盘强制兜底 ======
import os
import re
import json
import time
from datetime import datetime, timedelta
import pandas as pd

# ========== 配置 ==========
JSON_DIR = r"D:/AIPEBot/backtest"
TRADE_AMOUNT = 100000
ACC_ID = '你的资金账号'          # 必须与策略绑定的资金账号一致

DIP_PCT = 0.004                  # 买入触发：低于VWAP 0.4%
DEEP_DIP_PCT = 0.008             # 深跌触发：低于VWAP 0.8%，一次买满
SPIKE_PCT = 0.004                # 卖出触发：高于VWAP 0.4%
DEEP_SPIKE_PCT = 0.008           # 急拉触发：高于VWAP 0.8%，一次清仓
LOW_TOUCH_PCT = 0.002            # 接近当日低点阈值（买入）
HIGH_TOUCH_PCT = 0.002           # 接近当日高点阈值（卖出）
ORDER_COOLDOWN = 90              # 同一股票两次下单最小间隔（秒）
FORCE_BUY_HMS = (14, 40)         # 强制买入兜底时点
FORCE_SELL_HMS = (14, 50)        # 强制卖出/风控时点


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
        print('未找到交易计划文件')
        return None
    with open(os.path.join(JSON_DIR, max_file), 'r', encoding='utf-8') as f:
        plan = json.load(f)
    plan_list = plan.get('交易计划', [])
    return plan_list[-1] if plan_list else None


def parse_stocks(last_plan):
    stocks = []
    for s in last_plan['股票明细']:
        stocks.append({
            'code': _norm_code(s['代码']),
            'name': s.get('名称', ''),
            'weight': float(str(s.get('权重(%)', '0')).replace('%', '')) / 100.0,
        })
    return stocks


def get_closes(ContextInfo, code, count):
    try:
        data = ContextInfo.get_market_data_ex(['close'], [code], period='1d',
                                              count=count, dividend_type='none')
        if data and code in data:
            df = data[code].dropna()
            return [float(x) for x in df['close'].tolist()]
    except Exception as e:
        print('获取日线失败 %s: %s' % (code, e))
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
            print('计算VWAP失败 %s: %s' % (code, e))
        return last, vwap, day_high, day_low, is_limit_up
    except Exception as e:
        print('获取tick失败 %s: %s' % (code, e))
        return None


# ---------- 交易函数：对手价下单（统一记录冷却时间） ----------
def order_stock(ContextInfo, code, volume, side):
    op = 23 if side == 'buy' else 24
    prType = 4 if side == 'buy' else 6
    passorder(op, 1101, ACC_ID, code, prType, 0, int(volume), 2,
              'strategy_' + side, ContextInfo)
    ContextInfo.last_order_time[code] = time.time()      # 修复：记录冷却时间
    print('%s %s %d股 [对手价]' % (side, code, volume))


def get_positions(ContextInfo):
    pos = {}
    positions = ContextInfo.get_trade_detail_data(ACC_ID, 'stock', 'position')
    if positions:
        for p in positions:
            code = '%s.%s' % (p.m_strInstrumentID, p.m_strExchangeID)
            pos[code] = {
                'volume': int(getattr(p, 'm_nVolume', 0) or 0),
                'can_use': int(getattr(p, 'm_nCanUseVolume', 0) or 0),
            }
    return pos


def get_available_cash(ContextInfo):
    accs = ContextInfo.get_trade_detail_data(ACC_ID, 'stock', 'account')
    return accs[0].m_dCash if accs else 0.0


def in_trading_window(now):
    hm = now.hour * 60 + now.minute
    return (571 <= hm <= 690) or (780 <= hm <= 899)


def get_trading_days(ContextInfo, start, end):
    try:
        data = ContextInfo.get_market_data_ex(['close'], ['000001.SH'], period='1d',
                                              start_time=start, end_time=end,
                                              dividend_type='none')
        if data and '000001.SH' in data:
            df = data['000001.SH'].dropna()
            return [_norm_date(x) for x in df.index.tolist()]
    except Exception as e:
        print('获取交易日历失败: %s' % e)
    return []


def _in_cooldown(ContextInfo, code):
    last_t = ContextInfo.last_order_time.get(code, 0)
    return (time.time() - last_t) < ORDER_COOLDOWN


# ---------- 框架入口 ----------
def init(ContextInfo):
    ContextInfo.accID = ACC_ID

    last_plan = load_last_plan()
    if last_plan is None:
        print('无交易计划，策略空转')
        ContextInfo.finished = True
        return
    T0 = last_plan['调仓日期']
    ContextInfo.plan_stocks = parse_stocks(last_plan)
    print('最后调仓日期T0: %s, 股票数: %d' % (T0, len(ContextInfo.plan_stocks)))

    start = T0.replace('-', '')
    end = (datetime.strptime(T0, '%Y-%m-%d') + timedelta(days=30)).strftime('%Y%m%d')
    days = get_trading_days(ContextInfo, start, end)
    if days and days[0] == T0:
        days = days[1:]
    if len(days) < 5:
        print('交易日不足，策略退出')
        ContextInfo.finished = True
        return
    ContextInfo.T1 = days[0]
    ContextInfo.T5 = days[4]
    ContextInfo.plan_days = set(days[:5])
    ContextInfo.finished = False
    ContextInfo.cleared = False
    ContextInfo.buy_ordered = {}
    ContextInfo.last_order_time = {}
    ContextInfo.t1_finalized = False                 # 修复：T1兜底只执行一次
    print('T1=%s(拉回分批买入)  T5=%s(冲高分批卖出+尾盘兜底)' % (ContextInfo.T1, ContextInfo.T5))

    today = datetime.now().strftime('%Y-%m-%d')
    if today > ContextInfo.T5:
        print('已超过T5，无需执行，策略退出')
        ContextInfo.finished = True
        return

    # 修复：'60' = 每60秒触发（'1n'不是合法参数）
    ContextInfo.run_time('intraday_job', '60', '09:31:00', 'SH')


def handlebar(ContextInfo):
    pass


# ---------- 主调度：每60秒触发 ----------
def intraday_job(ContextInfo):
    if getattr(ContextInfo, 'finished', True):
        return
    now = datetime.now()
    today = now.strftime('%Y-%m-%d')
    if today not in getattr(ContextInfo, 'plan_days', set()):
        return
    if not in_trading_window(now):
        return

    # ================= T1 买入日 =================
    if today == ContextInfo.T1:
        if ContextInfo.t1_finalized:                # 修复：兜底完成后不再处理
            return

        # ---- 09:31 清掉旧持仓 ----
        if not ContextInfo.cleared:
            print('买入日 %s 开盘清旧仓' % ContextInfo.T1)
            for code, p in get_positions(ContextInfo).items():
                if p['can_use'] > 0:
                    order_stock(ContextInfo, code, p['can_use'], 'sell')
            ContextInfo.cleared = True
            ContextInfo.clear_time = time.time()
            return

        if time.time() - getattr(ContextInfo, 'clear_time', 0) < 120:
            return

        # ---- 核定买入计划（只核定一次）----
        if not hasattr(ContextInfo, 'buy_plan') or not ContextInfo.buy_plan:
            total = min(TRADE_AMOUNT, get_available_cash(ContextInfo))
            per = total / max(len(ContextInfo.plan_stocks), 1)
            plan = {}
            for st in ContextInfo.plan_stocks:
                t = get_tick(ContextInfo, st['code'])
                ref = t[0] if t else 0.0
                if ref > 0:
                    qty = int(per / ref / 100) * 100
                    if qty >= 100:
                        plan[st['code']] = qty
            ContextInfo.buy_plan = plan
            # 修复：盘中重启保护——当前持仓视为已下单（开盘已清旧仓，现持仓必为当日新买）
            if plan:
                for code, p in get_positions(ContextInfo).items():
                    if code in plan and p['volume'] > 0:
                        ContextInfo.buy_ordered[code] = max(
                            ContextInfo.buy_ordered.get(code, 0), p['volume'])
            print('买入计划: %s' % str(plan))

        now_hm = (now.hour, now.minute)

        # ---- 14:40 强制兜底，买满剩余额度 ----
        if now_hm >= FORCE_BUY_HMS:
            for code, planned in ContextInfo.buy_plan.items():
                ordered = ContextInfo.buy_ordered.get(code, 0)
                remain = planned - ordered
                if remain >= 100 and not _in_cooldown(ContextInfo, code):
                    order_stock(ContextInfo, code, remain, 'buy')
                    ContextInfo.buy_ordered[code] = ordered + remain
                    print('强制补买 %s %d股' % (code, remain))
            _log_t1_done(ContextInfo)
            ContextInfo.t1_finalized = True
            return

        # ---- 盘中：拉回低位分批买入 ----
        for code, planned in ContextInfo.buy_plan.items():
            ordered = ContextInfo.buy_ordered.get(code, 0)
            remain = planned - ordered
            if remain < 100:
                continue
            if _in_cooldown(ContextInfo, code):
                continue
            t = get_tick(ContextInfo, code)
            if t is None:
                continue
            last, vwap, day_high, day_low, is_limit_up = t
            if is_limit_up:
                continue
            deep_dip = (vwap > 0 and last <= vwap * (1 - DEEP_DIP_PCT))
            dip = (vwap > 0 and last <= vwap * (1 - DIP_PCT)) or \
                  (day_low > 0 and last <= day_low * (1 + LOW_TOUCH_PCT))
            if deep_dip:
                order_stock(ContextInfo, code, remain, 'buy')
                ContextInfo.buy_ordered[code] = ordered + remain
                print('深跌买入 %s %d股 @%.3f (VWAP %.3f)' % (code, remain, last, vwap))
            elif dip:
                batch = planned // 3 // 100 * 100
                if batch < 100:
                    batch = 100
                if batch > remain:
                    batch = remain
                order_stock(ContextInfo, code, batch, 'buy')
                ContextInfo.buy_ordered[code] = ordered + batch
                print('拉回买入 %s %d股 @%.3f (VWAP %.3f)' % (code, batch, last, vwap))
        return

    # ================= T5 卖出日 =================
    if today == ContextInfo.T5:
        positions = get_positions(ContextInfo)
        now_hm = (now.hour, now.minute)

        # ---- 14:50 强制清仓兜底 ----
        if now_hm >= FORCE_SELL_HMS:
            for code, p in positions.items():
                if p['can_use'] > 0:
                    order_stock(ContextInfo, code, p['can_use'], 'sell')
            print('卖出日 %s 尾盘强制清仓完成，策略结束' % ContextInfo.T5)
            ContextInfo.finished = True
            return

        # ---- 盘中：冲高分批卖出 ----
        for code, p in positions.items():
            vol = p['can_use']
            if vol <= 0 or _in_cooldown(ContextInfo, code):
                continue
            t = get_tick(ContextInfo, code)
            if t is None:
                continue
            last, vwap, day_high, day_low, is_limit_up = t
            deep_spike = (vwap > 0 and last >= vwap * (1 + DEEP_SPIKE_PCT))
            spike = (vwap > 0 and last >= vwap * (1 + SPIKE_PCT)) or \
                    (day_high > 0 and last >= day_high * (1 - HIGH_TOUCH_PCT))
            if deep_spike:
                order_stock(ContextInfo, code, vol, 'sell')
                print('急拉清仓 %s %d股 @%.3f (VWAP %.3f)' % (code, vol, last, vwap))
            elif spike:
                batch = vol // 3 // 100 * 100
                if batch < 100:
                    batch = vol if vol < 200 else 100
                if batch > vol:
                    batch = vol
                order_stock(ContextInfo, code, batch, 'sell')
                print('冲高卖出 %s %d股 @%.3f (VWAP %.3f)' % (code, batch, last, vwap))
        return

    # ================= T2~T4：MA20风控（尾盘）=================
    if (now.hour, now.minute) >= FORCE_SELL_HMS:
        for code, p in get_positions(ContextInfo).items():
            vol = p['can_use']
            if vol <= 0 or _in_cooldown(ContextInfo, code):   # 修复：加冷却防重复发单
                continue
            closes = get_closes(ContextInfo, code, 21)
            if len(closes) >= 21:
                ma20 = sum(closes[-21:-1]) / 20.0
                cur = closes[-1]
                if cur < ma20:
                    order_stock(ContextInfo, code, vol, 'sell')
                    print('风控卖出 %s %d股 (现价%.3f < MA20 %.3f)' % (code, vol, cur, ma20))


# ---------- 辅助 ----------
def _log_t1_done(ContextInfo):
    print('===== T1 买入汇总 =====')
    for code, planned in getattr(ContextInfo, 'buy_plan', {}).items():
        ordered = ContextInfo.buy_ordered.get(code, 0)
        print('%s 计划%d股 已下买单%d股' % (code, planned, ordered))
    positions = get_positions(ContextInfo)
    print('当前实际持仓: %s' % str(
        {k: v['volume'] for k, v in positions.items() if v['volume'] > 0}))
