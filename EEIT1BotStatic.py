# @title SIRIUS T1 BackTest Simulation (Static Edition)
import os
import json
import logging
import pandas as pd
from datetime import datetime, time, timedelta, timezone
import time as time_module
import glob
import requests
import random

# ========================= 1. 配置 (对齐 SIRIUS & 基础设施) =========================
class SimConfig:
    START_DATE = "2026-04-07"
    END_DATE = "2026-04-14"
    INITIAL_CASH = 100000.0

    
    DEBUG = False

    # SIRIUS 策略逻辑
    TRADE_RATIO = 0.5
    BUY_REBOUND_RATIO = 0.0062
    SELL_DROP_RATIO = 0.0038
    FORCE_DEADLINE_TIME = time(14, 50)
    FORCE_SELL_PRICE_RATIO = 0.995

    # 路径配置 (严格遵循参考样例)
    MODEL_HISTORY_DIR = "./historical_models"
    DATA_CACHE_DIR = "./min_data_cache"
    MONTHLY_DIR = "./monthly_data"
    MODEL_NAME_PREFIX = "流入模型"

    API_BASE_URL = "https://query.aivibeinvestment.com/api/query"
    API_REQUEST_INTERVAL = 0.3
    MAX_RETRIES = 5
    EXPONENTIAL_BACKOFF_BASE = 2
    FILL_OHLC_WITH_PRICE = True
    ENABLE_PRELOAD = True
    # 新增涨跌停限制
    LIMIT_UP_RATIO = 0.1    # 涨停 10%
    LIMIT_DOWN_RATIO = -0.1 # 跌停 10%
    # ST股票为 5%，如需支持可扩展为配置项

    # 新增输出配置
    OUTPUT_DIR = "./backtest_results"
    TRADE_RECORD_FILE = os.path.join(OUTPUT_DIR, "trade_records.xlsx")
    DAILY_SNAPSHOT_FILE = os.path.join(OUTPUT_DIR, "daily_snapshots.xlsx")


# 创建目录
for d in [SimConfig.MODEL_HISTORY_DIR, SimConfig.DATA_CACHE_DIR, SimConfig.MONTHLY_DIR, SimConfig.OUTPUT_DIR]:
    os.makedirs(d, exist_ok=True)

# ========================= 2. 日志 (强力清理，杜绝重复打印) =========================
logger = logging.getLogger("SIRIUS_Simulator")
if logger.handlers:
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)

logger.setLevel(logging.DEBUG if SimConfig.DEBUG else logging.INFO)
logger.propagate = False

formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

# 修复 1：先定义 ch，再设置级别
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG if SimConfig.DEBUG else logging.INFO) # ← 放在定义之后
ch.setFormatter(formatter)
logger.addHandler(ch)

# ========================= 3. 数据模块 (严格复刻 Parquet Master 逻辑) =========================
class MarketData:
    @staticmethod
    def _get_current_cn_date() -> str:
        tz_cn = timezone(timedelta(hours=8))
        return datetime.now(tz_cn).strftime('%Y-%m-%d')

    @staticmethod
    def get_monthly_file_path(year_month: str) -> str:
        """【修复点】返回月度 Parquet 文件的标准路径"""
        return os.path.join(SimConfig.MONTHLY_DIR, f"minute_data_{year_month}.parquet")


    @staticmethod
    def get_limit_prices(pre_close: float) -> tuple:
        """计算涨跌停价格"""
        if pre_close <= 0:
            return None, None
        limit_up = round(pre_close * (1 + SimConfig.LIMIT_UP_RATIO), 2)
        limit_down = round(pre_close * (1 + SimConfig.LIMIT_DOWN_RATIO), 2)
        return limit_up, limit_down

    @staticmethod
    def build_date_map(all_model_dates: list) -> dict:
        current_date = MarketData._get_current_cn_date()
        date_map = {}
        for idx, m_date in enumerate(all_model_dates):
            t1 = all_model_dates[idx + 1] if idx + 1 < len(all_model_dates) else current_date
            t2 = all_model_dates[idx + 2] if idx + 2 < len(all_model_dates) else current_date
            date_map[m_date] = (t1, t2)
        return date_map

    @staticmethod
    def _convert_code(code: str) -> str:
        """统一代码格式：002655 -> 002655.SZ"""
        c = str(code).split('.')[0].zfill(6)
        if len(c) > 6 and (c.endswith('.SH') or c.endswith('.SZ')):
            return c
        sh_prefixes = ('60', '68', '51', '56', '58', '55', '900')
        return f"{c}.SH" if any(c.startswith(p) for p in sh_prefixes) else f"{c}.SZ"

    @staticmethod
    def get_model_dates(start_date: str, end_date: str) -> list:
        pattern = os.path.join(SimConfig.MODEL_HISTORY_DIR, f"{SimConfig.MODEL_NAME_PREFIX}_*.json")
        dates = []
        for f in glob.glob(pattern):
            basename = os.path.basename(f)
            date_str = basename.replace(f"{SimConfig.MODEL_NAME_PREFIX}_", "").replace(".json", "")
            if start_date <= date_str <= end_date:
                dates.append(date_str)
        dates.sort()
        return dates

    @staticmethod
    def parse_sirius_model(model_data: dict) -> tuple:
        try:
            res = model_data.get('结果', {})
            details = res.get('最优投资组合配置', {}).get('配置详情', [])
            risk_info = res.get('风控因子信息', {})
            pos_factor = float(risk_info.get('综合建议仓位因子', 1.0))
            
            targets = []
            for item in details:
                weight = float(item.get('最优权重(%)', '0').replace('%', '')) / 100
                if weight <= 0:
                    continue
                
                targets.append({
                    'code': item.get('代码', ''),
                    'name': item.get('名称', ''),           # ← 新增 name 字段
                    'weight': weight,
                    'ref_price': float(item.get('最近一日价格', 0))
                })
            
            return targets, pos_factor
            
        except Exception as e:
            logger.error(f"解析模型失败: {e}")
            return [], 1.0

    @staticmethod
    def merge_monthly_data(year_month: str):
        """将缓存的 CSV 合并到月度 Parquet"""
        pattern = os.path.join(SimConfig.DATA_CACHE_DIR, f"*_{year_month}-*.csv")
        files = glob.glob(pattern)
        if not files: return
        all_data = []
        for f in files:
            try:
                df = pd.read_csv(f)
                parts = os.path.basename(f).replace('.csv', '').split('_')
                if len(parts) >= 2:
                    df['ts_code'], df['trade_date'] = parts[0], parts[1]
                    all_data.append(df)
            except: pass
        if all_data:
            merged = pd.concat(all_data, ignore_index=True)
            monthly_file = MarketData.get_monthly_file_path(year_month)
            if os.path.exists(monthly_file):
                try:
                    existing_df = pd.read_parquet(monthly_file)
                    merged = pd.concat([existing_df, merged], ignore_index=True).drop_duplicates(subset=['ts_code', 'trade_date', 'time'], keep='last')
                except: pass
            merged.to_parquet(monthly_file, index=False, compression='zstd')
            logger.info(f"✨ 已成功归档至 {year_month}.parquet")

    @staticmethod
    def get_minute_data(code: str, date_str: str) -> pd.DataFrame:
        """【最终兼容版】智能识别 Parquet/CSV 中的列名"""
        # 1. 先定义并转换 ts_code
        ts_code = MarketData._convert_code(code)
        monthly_file = MarketData.get_monthly_file_path(date_str[:7])
        df = pd.DataFrame()
        
        # 标准化日期格式（去掉时间部分）
        date_clean = str(date_str).split()[0]

        # 1. 优先从 Parquet 读取
        if os.path.exists(monthly_file):
            try:
                # 2. 先读取 Parquet 到 df 变量中
                df = pd.read_parquet(monthly_file)
                
                # ==========================================
                # ✅ 调试代码放在这里才是安全的，因为此时 ts_code 和 df 都已存在
                logger.debug(f"👉 调试查询: ts_code={ts_code}, trade_date={date_clean}")
                if 'trade_date' in df.columns:
                    logger.debug(f"👉 Parquet中的trade_date样本: {df['trade_date'].unique()[:5]}")
                else:
                    logger.debug("👉 Parquet中没有 'trade_date' 列！当前列名:", df.columns.tolist())
                # ==========================================

                # 标准化日期列并过滤
                if 'trade_date' in df.columns:
                    df['trade_date'] = df['trade_date'].astype(str).str.split().str[0]
                    df = df[df['trade_date'] == date_clean]
                
                if 'ts_code' in df.columns:
                    df = df[df['ts_code'] == ts_code]
                    
            except Exception as e:
                logger.debug(f"Parquet读取失败 {ts_code}@{date_str}: {e}")
                df = pd.DataFrame()

        # 2. 备选从 CSV 读取
        if df.empty:
            cache_file = os.path.join(SimConfig.DATA_CACHE_DIR, f"{ts_code}_{date_clean}.csv")
            if os.path.exists(cache_file):
                try: 
                    df = pd.read_csv(cache_file)
                except Exception as e:
                    logger.debug(f"CSV读取失败 {ts_code}@{date_clean}: {e}")

        # 3. 智能清洗列名
        if not df.empty:
            try:
                # 时间列识别
                if 'trade_time' in df.columns: 
                    df['时间'] = pd.to_datetime(df['trade_time'])
                elif 'date' in df.columns and 'time' in df.columns:
                    df['时间'] = pd.to_datetime(df['date'].astype(str) + " " + df['time'].astype(str))
                elif 'time' in df.columns:
                    df['时间'] = pd.to_datetime(date_clean + " " + df['time'].astype(str))
                elif '时间' in df.columns: 
                    df['时间'] = pd.to_datetime(df['时间'])

                # 价格列识别
                if 'close' in df.columns: 
                    df = df.rename(columns={'close': '收盘'})
                elif 'price' in df.columns: 
                    df = df.rename(columns={'price': '收盘'})

                if '时间' in df.columns and '收盘' in df.columns:
                    return df[['时间', '收盘']].sort_values("时间").drop_duplicates('时间')
                    
            except Exception as e:
                logger.debug(f"数据清洗失败 {ts_code}@{date_str}: {e}")
        
        return pd.DataFrame()

    @staticmethod
    def _fetch_intraday_from_api(code: str, date_str: str) -> pd.DataFrame:
        api_url = f"{SimConfig.API_BASE_URL.rstrip('/')}?type=specifiedIntraday&code={code}&date={date_str}"
        logger.debug(f"🔍 [请求地址] {api_url}")
        for attempt in range(1, SimConfig.MAX_RETRIES + 1):
            try:
                resp = requests.get(api_url, timeout=30)
                if resp.status_code == 404: return pd.DataFrame()
                resp.raise_for_status()
                data = resp.json()
                if not data: continue
                if isinstance(data, dict): data = data.get("data") or data.get("trends")
                df = pd.DataFrame(data)
                df["时间"] = pd.to_datetime(df["date"] + " " + df["time"])
                df["收盘"] = df["price"]
                return df[["时间", "收盘"]].sort_values("时间")
            except:
                time_module.sleep(SimConfig.EXPONENTIAL_BACKOFF_BASE ** attempt)
        return pd.DataFrame()

    @staticmethod
    def preload_from_models(start_date: str, end_date: str):
        logger.info("🚀 启动智能预下载器 (SIRIUS 适配版)")
        dates = MarketData.get_model_dates(start_date, end_date)
        if not dates: return
        date_map = MarketData.build_date_map(dates)
        today_str = MarketData._get_current_cn_date()

        # 1. 收集目标
        raw_pairs = set()
        for m_date in dates:
            t1, t2 = date_map[m_date]
            model_file = os.path.join(SimConfig.MODEL_HISTORY_DIR, f"{SimConfig.MODEL_NAME_PREFIX}_{m_date}.json")
            with open(model_file, 'r', encoding='utf-8') as f:
                targets, _ = MarketData.parse_sirius_model(json.load(f))
                for t in targets:
                    if t1 < today_str: raw_pairs.add((MarketData._convert_code(t['code']), t1))
                    if t2 < today_str: raw_pairs.add((MarketData._convert_code(t['code']), t2))

        # 2. 扫描索引
        parquet_keys_set = set()
        for ym in set(d[:7] for _, d in raw_pairs):
            p_path = MarketData.get_monthly_file_path(ym)
            if os.path.exists(p_path):
                try:
                    df_p = pd.read_parquet(p_path, columns=['ts_code', 'trade_date'])
                    parquet_keys_set.update(set(zip(df_p['ts_code'].astype(str), df_p['trade_date'].astype(str))))
                except: pass

        # 3. 下载缺失
        last_month = None
        for ts_code, t_date in raw_pairs:
            if (ts_code, t_date) in parquet_keys_set: continue
            if os.path.exists(os.path.join(SimConfig.DATA_CACHE_DIR, f"{ts_code}_{t_date}.csv")): continue

            if last_month and t_date[:7] != last_month: MarketData.merge_monthly_data(last_month)
            last_month = t_date[:7]

            df = MarketData._fetch_intraday_from_api(ts_code.split('.')[0], t_date)
            if not df.empty:
                df.to_csv(os.path.join(SimConfig.DATA_CACHE_DIR, f"{ts_code}_{t_date}.csv"), index=False)
                logger.info(f"✓ 下载成功: {ts_code} ({t_date})")
            time_module.sleep(SimConfig.API_REQUEST_INTERVAL)

        if last_month: MarketData.merge_monthly_data(last_month)

# ========================= 4. 账户 & 执行器 (SIRIUS 核心) =========================
class MockAccount:
    def __init__(self, initial_cash):
        self.cash = initial_cash
        self.positions = {}
        self.today_buys = set()  # ← 新增：记录当日买入的股票代码

    def start_day(self):
        """
        每日开盘：清空当日买入标记
        前一日的持仓全部变为可卖
        """
        self.today_buys.clear()  # ← 清空标记，昨日买入今已可卖
        
        # 确保所有持仓都有 can_sell 字段
        for code in self.positions:
            if 'can_sell' not in self.positions[code]:
                self.positions[code]['can_sell'] = 0

    def order(self, date, time_v, code, side, vol, price, reason, name=""):
        vol = (vol // 100) * 100
        if vol <= 0: 
            return False
        
        cost = vol * price
        display_name = f"{code}({name})" if name else code  # ← 新增显示名称

        # ===== 买入逻辑 =====
        if side == 'buy' and self.cash >= cost:
            self.cash -= cost
            
            # 获取或初始化持仓
            if code in self.positions:
                p = self.positions[code]
            else:
                p = {'volume': 0, 'avg_price': 0.0, 'can_sell': 0, 'name': name}  # ← 新增 name 字段
            
            # 更新均价和持仓量
            total_cost = p['volume'] * p['avg_price'] + cost
            p['volume'] += vol
            p['avg_price'] = total_cost / p['volume']
            
            # 保存名称（如果之前没有）
            if 'name' not in p or not p['name']:
                p['name'] = name
            
            self.positions[code] = p
            self.today_buys.add(code)
            
            # ← 使用 display_name
            logger.info(f"💰 {date} {time_v} | 买入 {display_name} {vol}股 @{price:.2f} ({reason}) [T+1锁定]")
            return True

        # ===== 卖出逻辑 =====
        elif side == 'sell':
            if code not in self.positions:
                logger.warning(f"⚠️ 卖出失败：未持有 {display_name}")
                return False
            
            # ← 关键检查：当日买入的不能卖
            if code in self.today_buys:
                logger.warning(f"⚠️ 卖出失败：{display_name} 当日买入，T+1 不可卖")
                return False
            
            p = self.positions[code]
            available = p.get('can_sell', 0)
            
            if available < vol:
                logger.warning(f"⚠️ 卖出失败：{display_name} 可卖 {available} < 需卖 {vol}")
                return False
            
            # 执行卖出
            self.cash += vol * price
            p['volume'] -= vol
            p['can_sell'] -= vol
            
            if p['volume'] <= 0:
                del self.positions[code]
            else:
                self.positions[code] = p
            
            # ← 使用 display_name
            logger.info(f"💰 {date} {time_v} | 卖出 {display_name} {vol}股 @{price:.2f} ({reason})")
            return True
        
        return False

# ========================= 严格对照执行器 =========================
class SiriusStrictExecutor:
    def __init__(self, account):
        self.account = account
        self.today_trades = []  # 用于Excel导出
        self.all_trades = []             # 累计所有交易
        self.daily_snapshots = []        # 每日持仓快照

    def _check_limit_up_down(self, code: str, price: float, side: str, pre_close: float, name: str = "") -> bool:
        """检查是否触及涨跌停"""
        display_name = f"{code}({name})" if name else code
        
        # 防御性检查
        if price is None:
            logger.warning(f"⚠️ {display_name} 价格为空，跳过{side}检测")
            return False
        
        if pre_close is None or pre_close <= 0:
            logger.warning(f"⚠️ {display_name} 昨收价异常({pre_close})，跳过{side}检测")
            return False
        
        limit_up, limit_down = MarketData.get_limit_prices(pre_close)
        if limit_up is None:
            logger.warning(f"⚠️ {display_name} 无法计算涨跌停价")
            return False

        if side == 'buy' and price >= limit_up:
            logger.warning(f"⚠️ {display_name} 触及涨停 {limit_up:.2f}，无法买入")
            return True
        
        if side == 'sell' and price <= limit_down:
            logger.warning(f"⚠️ {display_name} 触及跌停 {limit_down:.2f}，无法卖出")
            return True
        
        return False


    def _get_name(self, code, targets):
        """获取股票名称"""
        for t in targets:
            if t['code'] == code:
                return t.get('name', code)
        return code


    def _get_last_known_price(self, code: str, default_price: float) -> float:
        """从历史持仓快照中寻找该股票上一个交易日的收盘价"""
        for snap in reversed(self.daily_snapshots):
            if snap['code'] == code and snap['last_price'] > 0:
                return snap['last_price']
        return default_price

    def save_daily_snapshot(self, date_str):
        """保存每日持仓快照"""
        # 计算当日净值
        total_value = self.account.cash
        for code, pos in self.account.positions.items():
            df = MarketData.get_minute_data(code, date_str)
            # 修改点：找不到数据时，去历史快照找前一天的价格，而不是用持仓成本价
            if not df.empty:
                last_price = df.iloc[-1]['收盘']
            else:
                last_price = self._get_last_known_price(code, pos['avg_price'])

            market_value = pos['volume'] * last_price
            total_value += market_value
            
            self.daily_snapshots.append({
                'date': date_str,
                'code': code,
                'name': pos.get('name', code),
                'volume': pos['volume'],
                'can_sell': pos.get('can_sell', 0),
                'avg_price': pos['avg_price'],
                'last_price': last_price,
                'market_value': market_value,
                'weight': 0  # 稍后计算
            })
        
        # 计算权重
        for snap in self.daily_snapshots:
            if snap['date'] == date_str:
                snap['weight'] = snap['market_value'] / total_value if total_value > 0 else 0
        
        # 添加现金记录
        self.daily_snapshots.append({
            'date': date_str,
            'code': 'CASH',
            'name': '现金',
            'volume': 0,
            'can_sell': 0,
            'avg_price': 0,
            'last_price': 1,
            'market_value': self.account.cash,
            'weight': self.account.cash / total_value if total_value > 0 else 0
        })
        
        # 添加汇总记录
        self.daily_snapshots.append({
            'date': date_str,
            'code': 'TOTAL',
            'name': '总资产',
            'volume': 0,
            'market_value': total_value,
            'weight': 1.0
        })
        
        return total_value

    def export_to_excel(self):
        """导出所有交易记录和持仓快照到Excel"""
        if not self.all_trades and not self.daily_snapshots:
            logger.warning("无数据可导出")
            return
        
        # 使用 ExcelWriter 创建多sheet文件
        with pd.ExcelWriter(SimConfig.TRADE_RECORD_FILE, engine='openpyxl') as writer:
            # Sheet 1: 交易记录
            if self.all_trades:
                df_trades = pd.DataFrame(self.all_trades)
                # 调整列顺序
                cols = ['date', 'time', 'code', 'name', 'side', 'volume', 'price', 
                        'amount', 'reason', 'day']
                # 计算成交金额
                df_trades['amount'] = df_trades['volume'] * df_trades['price']
                # 添加交易日序号
                df_trades['day'] = pd.Categorical(df_trades['date']).codes + 1
                df_trades = df_trades[[c for c in cols if c in df_trades.columns]]
                
                df_trades.to_excel(writer, sheet_name='交易记录', index=False)
                logger.info(f"✅ 导出交易记录: {len(df_trades)} 笔")
            
            # Sheet 2: 持仓快照
            if self.daily_snapshots:
                df_snapshots = pd.DataFrame(self.daily_snapshots)
                df_snapshots.to_excel(writer, sheet_name='持仓快照', index=False)
                logger.info(f"✅ 导出持仓快照: {len(df_snapshots)} 条")
            
            # Sheet 3: 每日汇总
            if self.daily_snapshots:
                df_summary = pd.DataFrame([
                    s for s in self.daily_snapshots 
                    if s['code'] == 'TOTAL'
                ])[['date', 'market_value']]
                df_summary.columns = ['日期', '总资产']
                df_summary['收益率'] = df_summary['总资产'].pct_change()
                df_summary['累计收益'] = (df_summary['总资产'] / SimConfig.INITIAL_CASH - 1)
                df_summary.to_excel(writer, sheet_name='每日汇总', index=False)


    def simulate_day(self, date_str, targets, pos_factor, pre_closes):
        """
        严格模拟真实代码的两个阶段：
        1. 10:00 调仓阶段 (run_once)
        2. 14:50 强制卖出阶段 (force_sell_at_close)
        """
        # --- 0. 开盘准备 ---
        self.account.start_day()

        # --- 1. 早盘调仓阶段 (模拟 10:00) ---
        trade_time_morning = time(10, 0)

        # 获取 10:00 的快照价格作为“当前价”
        prices_1000 = {}
        all_codes = list(set([t['code'] for t in targets] + list(self.account.positions.keys())))
        for code in all_codes:
            df = MarketData.get_minute_data(code, date_str)
            if not df.empty:
                # 找到最接近 10:00 的一行记录
                target_dt = datetime.combine(datetime.strptime(date_str, "%Y-%m-%d").date(), trade_time_morning)
                mask = df['时间'] >= target_dt
                prices_1000[code] = df.loc[mask, '收盘'].iloc[0] if mask.any() else df.iloc[-1]['收盘']

        # 计算资产和目标 (完全对照 TradeSignalGenerator 逻辑)
        #total_asset = self.account.cash + sum(p['volume'] * prices_1000.get(c, p['avg_price']) for c, p in self.account.positions.items())
        # === 修改后 ===
        total_pos_value = 0
        for c, p in self.account.positions.items():
            if c in prices_1000:
                price = prices_1000[c]
            else:
                # 获取不到10:00实时价时，也用上一交易日收盘价估算
                price = self._get_last_known_price(c, p['avg_price'])
            total_pos_value += p['volume'] * price
            
        total_asset = self.account.cash + total_pos_value
        # =================
        effective_asset = total_asset * SimConfig.TRADE_RATIO * pos_factor

        target_vols = {t['code']: int(effective_asset * t['weight'] / t['ref_price'] / 100) * 100 for t in targets}

        # --- 生成调仓指令 ---
        for code, pos in list(self.account.positions.items()):
            t_vol = target_vols.get(code, 0)
            if pos['volume'] > t_vol:
                sell_vol = pos.get('can_sell', 0)
                if sell_vol > 0:
                    real_p = prices_1000.get(code)
                    pre_close = pre_closes.get(code, pos['avg_price'])
                    
                    # 从持仓获取名称
                    name = pos.get('name', '')

                    # 修复：检查价格是否存在
                    if real_p is None:
                        logger.warning(f"⚠️ {code}({name}) 无实时价格，跳过卖出")
                        continue

                    # 跌停检测
                    if self._check_limit_up_down(code, real_p, 'sell', pre_close, name):
                        continue  # 跌停，跳过卖出
                    
                    if real_p >= pre_close:  # 严格对照：正常时段受昨收约束
                        # 传递 name 参数
                        self.account.order(date_str, trade_time_morning, code, 'sell', 
                                        sell_vol, real_p, "早盘止盈", name)
                        
        # B. 买入指令 (约束：价格 <= 基准价)
        available_cash = self.account.cash * SimConfig.TRADE_RATIO
        for code, t_vol in target_vols.items():
            cur_vol = self.account.positions.get(code, {}).get('volume', 0)
            if t_vol > cur_vol:
                buy_vol = t_vol - cur_vol
                real_p = prices_1000.get(code)
                
                # 获取目标信息
                target_info = next((t for t in targets if t['code'] == code), None)
                if target_info is None:
                    continue
                    
                ref_p = target_info['ref_price']
                name = target_info.get('name', '')  # ← 获取名称

                if real_p is None:
                    logger.warning(f"⚠️ {code}({name}) 无实时价格，跳过买入")
                    continue
                                  
                pre_close = pre_closes.get(code, ref_p)
                if self._check_limit_up_down(code, real_p, 'buy', pre_close, name):
                    continue

                if real_p <= ref_p:
                    exec_p = min(real_p, ref_p)
                    if self.account.cash >= buy_vol * exec_p:
                        # ← 传递 name 参数
                        self.account.order(date_str, trade_time_morning, code, 'buy', 
                                        buy_vol, exec_p, "早盘调仓", name)
                        
        # # --- 2. 尾盘强制卖出阶段 (模拟 14:50) ---
        # trade_time_close = time(14, 50)

        # # 获取 14:50 的快照价格
        # prices_1450 = {}
        # for code in self.account.positions.keys():
        #     df = MarketData.get_minute_data(code, date_str)
        #     if not df.empty:
        #         target_dt = datetime.combine(datetime.strptime(date_str, "%Y-%m-%d").date(), trade_time_close)
        #         mask = df['时间'] >= target_dt
        #         prices_1450[code] = df.loc[mask, '收盘'].iloc[0] if mask.any() else df.iloc[-1]['收盘']

        # # 严格执行 force_sell_at_close 逻辑
        # for code, pos in list(self.account.positions.items()):
        #     t_vol = target_vols.get(code, 0)
        #     if pos['volume'] > t_vol:
        #         # 重新计算需要卖出的数量
        #         sell_needed = pos['volume'] - t_vol
        #         can_sell = pos.get('can_sell', 0)
        #         actual_sell = min(can_sell, sell_needed)

        #         if actual_sell > 0:
        #             real_p = prices_1450.get(code)
        #             if real_p:
        #                 # 强制卖出逻辑：撤销昨收价约束
        #                 # 这里模拟您代码中的 get_sell_price_unconstrained
        #                 pre_close = pre_closes.get(code)
        #                 # 保护价逻辑：不低于昨收的 99.5%
        #                 exec_p = max(real_p, pre_close * 0.995) if pre_close else real_p
        #                 self.account.order(date_str, trade_time_close, code, 'sell', actual_sell, exec_p, "尾盘强制")
        

# ========================= 回测主函数 =========================
def run_strict_backtest():
    if SimConfig.ENABLE_PRELOAD:
        MarketData.preload_from_models(SimConfig.START_DATE, SimConfig.END_DATE)

    account = MockAccount(SimConfig.INITIAL_CASH)
    executor = SiriusStrictExecutor(account)
    
    # 获取模型日期列表
    model_dates = MarketData.get_model_dates(SimConfig.START_DATE, SimConfig.END_DATE)
    
    if not model_dates:
        logger.error("未找到模型文件")
        return
    
    logger.info(f"找到模型日期: {model_dates}")

    # 获取中国时间今天日期
    tz_cn = timezone(timedelta(hours=8))
    today_cn = datetime.now(tz_cn).strftime('%Y-%m-%d')
    logger.info(f"中国时间今天: {today_cn}")

    # =========================================================
    # ✨ 核心修复：基于实际交易日历构建 T+1 映射
    # =========================================================
    trade_map = {}  # {交易日期: 模型日期}
    for i in range(len(model_dates)):
        model_date = model_dates[i]
        
        # 1. 如果不是最后一个模型日期，T+1 交易日就是列表中的【下一个模型日期】
        if i < len(model_dates) - 1:
            trade_date = model_dates[i + 1]
            
        # 2. 如果是最后一个模型日期
        else:
            # 如果今天(today_cn)不在模型列表中，且今天日期大于最后模型日，则将今天作为 T+1
            if today_cn not in model_dates and today_cn > model_date:
                trade_date = today_cn
            else:
                # 兜底情况（例如今天已包含在模型中，说明最新模型的 T+1 在未来）
                # 采用自然日+1，后续循环会被「日期过滤逻辑」安全跳过或作为未发生交易处理
                model_dt = datetime.strptime(model_date, "%Y-%m-%d")
                trade_date = (model_dt + timedelta(days=1)).strftime("%Y-%m-%d")
                
        trade_map[trade_date] = model_date
    # =========================================================
    
    logger.info(f"交易日映射(交易日期->模型日期): {trade_map}")

    # 按交易日期排序执行
    for trade_date in sorted(trade_map.keys()):
        model_date = trade_map[trade_date]
        
        # 检查是否在回测区间内，或等于今天（中国时间）
        if not (SimConfig.START_DATE <= trade_date <= SimConfig.END_DATE) and trade_date != today_cn:
            logger.debug(f"交易日期 {trade_date} 超出回测区间且不是今天，跳过")
            continue
        
        logger.info(f"========== 模型[{model_date}] -> 交易[{trade_date}] ==========")
        
        # 1. 加载模型（用前一日模型）
        model_file = os.path.join(SimConfig.MODEL_HISTORY_DIR, 
                                  f"{SimConfig.MODEL_NAME_PREFIX}_{model_date}.json")
        if not os.path.exists(model_file):
            logger.error(f"模型文件不存在: {model_file}")
            continue
            
        with open(model_file, 'r', encoding='utf-8') as f:
            targets, pf = MarketData.parse_sirius_model(json.load(f))

        # 2. 准备昨收价（模型中的 ref_price 是 model_date 的收盘价）
        # 对于 trade_date 来说，这就是昨收价
        pre_closes = {t['code']: t['ref_price'] for t in targets}

        # 3. 运行仿真（在 trade_date 这一天交易）
        executor.simulate_day(trade_date, targets, pf, pre_closes)
        
        # 4. 累计交易记录
        executor.all_trades.extend(executor.today_trades)
        executor.today_trades = []
        
        # 5. 保存每日快照
        total_asset = executor.save_daily_snapshot(trade_date)
        logger.info(f"Day End Asset: {total_asset:.2f}")

    # 6. 导出结果
    executor.export_to_excel()
    logger.info(f"🎉 回测完成！结果保存至: {SimConfig.TRADE_RECORD_FILE}")

    # 7. 打印统计
    _print_statistics(executor, account)


def _print_statistics(executor, account):
    """打印回测统计"""
    if not executor.all_trades:
        return
    
    df = pd.DataFrame(executor.all_trades)
    buy_count = len(df[df['side'] == 'buy'])
    sell_count = len(df[df['side'] == 'sell'])
    
    # 计算最终收益
    final_snapshots = [s for s in executor.daily_snapshots if s['code'] == 'TOTAL']
    if final_snapshots:
        final_asset = final_snapshots[-1]['market_value']
        total_return = (final_asset / SimConfig.INITIAL_CASH - 1) * 100
        logger.info(f"\n{'='*50}")
        logger.info(f"回测统计:")
        logger.info(f"  初始资金: {SimConfig.INITIAL_CASH:.2f}")
        logger.info(f"  最终资产: {final_asset:.2f}")
        logger.info(f"  总收益率: {total_return:.2f}%")
        logger.info(f"  买入次数: {buy_count}")
        logger.info(f"  卖出次数: {sell_count}")
        logger.info(f"{'='*50}")

if __name__ == "__main__":
    run_strict_backtest()
