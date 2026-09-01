# -*- coding: gbk -*-
# 国金QMT 内置Python 策略模型（标准QMT客户端框架，不依赖miniQMT）
# T1开盘(09:31)对手价买入，T5收盘前(14:50)对手价清仓，尾盘MA20风控
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
BUY_TIME_HMS = '09:31:00'        # T1 开盘买入时点
SELL_TIME_HMS = '14:50:00'       # T5 收盘卖出 / 每日风控检查时点


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
        print('获取行情失败 %s: %s' % (code, e))
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
                return float(p)
    except Exception as e:
        print('获取实时价失败 %s: %s' % (code, e))
    closes = get_closes(ContextInfo, code, 5)
    return closes[-1] if closes else 0.0


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


# ---------- 交易函数：对手价下单，不挂限价 ----------
def order_stock(ContextInfo, code, volume, side):
    # passorder(方向, 单股单债1101, 账号, 代码, 价格类型, 价格, 数量, 快速下单, 备注, ContextInfo)
    # 价格类型：4=卖1价(买入吃卖一)  6=买1价(卖出打买一)，非限价时价格参数必须填0
    op = 23 if side == 'buy' else 24
    prType = 4 if side == 'buy' else 6
    passorder(op, 1101, ACC_ID, code, prType, 0, int(volume), 2,
              'strategy_' + side, ContextInfo)
    print('%s %s %d股 [对手价]' % (side, code, volume))


def get_positions(ContextInfo):
    """返回 {code: 可用数量}，T+1用m_nCanUseVolume"""
    pos = {}
    positions = ContextInfo.get_trade_detail_data(ACC_ID, 'stock', 'position')
    if positions:
        for p in positions:
            vol = p.m_nCanUseVolume
            if vol and vol > 0:
                code = '%s.%s' % (p.m_strInstrumentID, p.m_strExchangeID)
                pos[code] = int(vol)
    return pos


def get_available_cash(ContextInfo):
    accs = ContextInfo.get_trade_detail_data(ACC_ID, 'stock', 'account')
    return accs[0].m_dCash if accs else 0.0


# ---------- 框架入口 ----------
def init(ContextInfo):
    ContextInfo.accID = ACC_ID

    last_plan = load_last_plan()
    if last_plan is None:
        print('无交易计划，策略空转')
        ContextInfo.finished = True
        return
    T0 = last_plan['调仓日期']
    ContextInfo.plan_stocks = parse_stocks(last_plan)      # 注意：不能用stocks（内置属性）
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
    ContextInfo.plan_days = set(days[:5])                  # T1~T5交易日白名单
    ContextInfo.is_bought = False
    ContextInfo.finished = False
    print('T1=%s(开盘买入)  T5=%s(收盘卖出)' % (ContextInfo.T1, ContextInfo.T5))

    today = datetime.now().strftime('%Y-%m-%d')
    if today > ContextInfo.T5:
        print('已超过T5，无需执行，策略退出')
        ContextInfo.finished = True
        return

    ContextInfo.run_time('daily_job', '1d', BUY_TIME_HMS, 'SH')
    ContextInfo.run_time('daily_job', '1d', SELL_TIME_HMS, 'SH')


def handlebar(ContextInfo):
    pass  # 本策略不依赖K线驱动


# ---------- 每日执行 ----------
def daily_job(ContextInfo):
    if getattr(ContextInfo, 'finished', True):
        return
    now = datetime.now()
    today = now.strftime('%Y-%m-%d')
    is_morning = now.hour < 13

    # 窗口外/非交易日直接返回
    if today not in getattr(ContextInfo, 'plan_days', set()):
        return

    # ===== T1 开盘买入 =====
    if today == ContextInfo.T1 and not ContextInfo.is_bought:
        print('买入日 %s 开盘执行' % ContextInfo.T1)
        for code, vol in get_positions(ContextInfo).items():
            order_stock(ContextInfo, code, vol, 'sell')    # 清仓旧持仓
        time.sleep(3)   # 等待卖单回报，卖出资金可用于当日买入

        total = min(TRADE_AMOUNT, get_available_cash(ContextInfo))
        per = total / max(len(ContextInfo.plan_stocks), 1)
        for st in ContextInfo.plan_stocks:
            price = get_current_price(ContextInfo, st['code'])
            if price > 0:
                vol = int(per / price / 100) * 100
                if vol > 0:
                    order_stock(ContextInfo, st['code'], vol, 'buy')
        ContextInfo.is_bought = True
        return

    # ===== T5 收盘清仓（仅尾盘时点）=====
    if ContextInfo.is_bought and today == ContextInfo.T5 and not is_morning:
        for code, vol in get_positions(ContextInfo).items():
            order_stock(ContextInfo, code, vol, 'sell')
        print('卖出日 %s 收盘清仓，策略结束' % ContextInfo.T5)
        ContextInfo.finished = True
        return

    # ===== MA20 风控：尾盘检查（当日买入因T+1自动不会被卖出）=====
    if ContextInfo.is_bought and today != ContextInfo.T5 and not is_morning:
        for code, vol in get_positions(ContextInfo).items():
            closes = get_closes(ContextInfo, code, 21)
            if len(closes) >= 21:
                ma20 = sum(closes[-21:-1]) / 20.0
                cur = closes[-1]
                if cur < ma20:
                    order_stock(ContextInfo, code, vol, 'sell')
                    print('风控卖出 %s (现价%.3f < MA20 %.3f)' % (code, cur, ma20))
