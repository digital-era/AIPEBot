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
    START_DATE = "2025-09-12"
    END_DATE = "2026-04-20"
    INITIAL_CASH = 100000.0

    # SIRIUS 策略逻辑
    TRADE_RATIO = 1
    BUY_REBOUND_RATIO = 0.0062
    SELL_DROP_RATIO = 0.0038
    FORCE_DEADLINE_TIME = time(14, 45)
    FORCE_SELL_PRICE_RATIO = 0.995

    # 路径配置 (严格遵循参考样例)
    MODEL_HISTORY_DIR = "./historical_models"
    DATA_CACHE_DIR = "./min_data_cache"
    MONTHLY_DIR = "./monthly_data"
    MODEL_NAME_PREFIX = f"{TARGET_MODE_NAME}"

    API_BASE_URL = "https://query.aivibeinvestment.com/api/query"
    API_REQUEST_INTERVAL = 0.3
    MAX_RETRIES = 5
    EXPONENTIAL_BACKOFF_BASE = 2
    FILL_OHLC_WITH_PRICE = True
    ENABLE_PRELOAD = True

    # 【新增】输出路径配置
    OUTPUT_DIR = "./backtest_results"
    TRADE_RECORD_FILE = os.path.join(OUTPUT_DIR, f"trade_records_static_{MODEL_NAME_PREFIX}.xlsx")
    DAILY_SNAPSHOT_FILE = os.path.join(OUTPUT_DIR, "daily_snapshots.xlsx")

# 创建目录
for d in [SimConfig.MODEL_HISTORY_DIR, SimConfig.DATA_CACHE_DIR, SimConfig.MONTHLY_DIR, SimConfig.OUTPUT_DIR]:
    os.makedirs(d, exist_ok=True)

# ========================= 2. 日志 (强力清理，杜绝重复打印) =========================
logger = logging.getLogger("SIRIUS_Simulator")
if logger.handlers:
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)

logger.setLevel(logging.DEBUG)
logger.propagate = False # 防止传递给 root logger 导致双重打印

formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
ch = logging.StreamHandler()
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
        return os.path.join(SimConfig.MONTHLY_DIR, f"minute_data_{year_month}_qmt.parquet")

    @staticmethod
    def get_limit_prices(pre_close: float) -> tuple:
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

    # def _convert_code(code: str) -> str:
    #     c = str(code).split('.')[0].zfill(6)
    #     if len(c) > 6 and (c.endswith('.SH') or c.endswith('.SZ')):
    #         return c
    #     sh_prefixes = ('60', '68', '51', '56', '58', '55', '900')
    #     return f"{c}.SH" if any(c.startswith(p) for p in sh_prefixes) else f"{c}.SZ"

    @staticmethod
    def _convert_code(code: str) -> str:
      return code

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
                    'code': MarketData._convert_code(item.get('代码', '')),
                    'name': item.get('名称', ''),
                    'weight': weight,
                    'ref_price': float(item.get('最近一日价格', 0))
                })
            return targets, pos_factor
        except Exception as e:
            logger.error(f"解析模型失败: {e}")
            return [], 1.0


    @staticmethod
    def merge_monthly_data(year_month: str):
        """
        将 Cache 中的 CSV 数据合并到月度 Parquet 文件中
        year_month 格式: "2026-04"
        """
        p_path = MarketData.get_monthly_file_path(year_month)

        # 1. 获取 Cache 中属于该月份的所有 CSV 文件
        cache_files = [
            f for f in os.listdir(SimConfig.DATA_CACHE_DIR)
            if f.endswith(".csv") and year_month in f
        ]

        if not cache_files:
            return

        # 2. 读取所有新下载的 CSV 数据
        new_dfs = []
        for f in cache_files:
            try:
                # 【修复核心 1】：强制指定 ts_code 和 trade_date 为字符串格式
                temp_df = pd.read_csv(
                    os.path.join(SimConfig.DATA_CACHE_DIR, f),
                    dtype={'ts_code': str, 'trade_date': str}
                )
                new_dfs.append(temp_df)
            except Exception as e:
                logger.error(f"读取缓存CSV失败 {f}: {e}")

        if not new_dfs:
            return

        combined_df = pd.concat(new_dfs, ignore_index=True)

        # 3. 如果原有 Parquet 存在，先读取它
        if os.path.exists(p_path):
            try:
                old_df = pd.read_parquet(p_path)
                # 【修复核心 2】：旧数据读出来后，强制转字符串并补齐 6 位，防止原本存的已经是 int
                if 'ts_code' in old_df.columns:
                    old_df['ts_code'] = old_df['ts_code'].astype(str).str.zfill(6)

                # 将旧数据和新数据合并
                combined_df = pd.concat([old_df, combined_df], ignore_index=True)
                logger.info(f"正在合并旧数据 ({len(old_df)}条) 与新数据...")
            except Exception as e:
                logger.error(f"读取旧 Parquet 失败，可能会导致覆盖: {e}")

        # 4. 去重并保存
        if not combined_df.empty:
            # ========================================================
            # 4. 【核心修复区】：在去重和保存之前，暴力清洗所有的关键列类型
            # ========================================================
            if not combined_df.empty:
                # 修复 ArrowTypeError: 强制将 "时间" 列转为标准的 datetime 对象
                if '时间' in combined_df.columns:
                    combined_df['时间'] = pd.to_datetime(combined_df['时间'])

                # 强制统一 ts_code 和 trade_date 为纯正的字符串，杜绝混合类型
                if 'ts_code' in combined_df.columns:
                    combined_df['ts_code'] = combined_df['ts_code'].astype(str).str.strip().str.zfill(6)
                if 'trade_date' in combined_df.columns:
                    combined_df['trade_date'] = pd.to_datetime(combined_df['trade_date'].astype(str)).dt.strftime('%Y-%m-%d')

                # ========================================================

            # 以时间、代码、日期作为唯一键去重
            combined_df.drop_duplicates(subset=['时间', 'ts_code', 'trade_date'], inplace=True)
            # 排序，保证 Parquet 文件内部有序
            combined_df.sort_values(['ts_code', '时间'], inplace=True)

            # 5. 写入 Parquet (此时类型绝对统一，不会再崩溃)
            combined_df.to_parquet(p_path, index=False, engine='pyarrow')
            logger.info(f"✅ 月度数据已更新: {p_path} (新增后总计: {len(combined_df)} 条)")

            # 6. 合并成功后删除对应的 CSV 缓存
            for f in cache_files:
                try:
                    os.remove(os.path.join(SimConfig.DATA_CACHE_DIR, f))
                except:
                    pass

    @staticmethod
    def get_minute_data(code: str, date_str: str) -> pd.DataFrame:
        ts_code = MarketData._convert_code(code)
        monthly_file = MarketData.get_monthly_file_path(date_str[:7])
        date_clean = str(date_str).split()[0]
        df = pd.DataFrame()

        if os.path.exists(monthly_file):
            try:
                df = pd.read_parquet(monthly_file)
                if 'trade_date' in df.columns:
                    df['trade_date'] = df['trade_date'].astype(str).str.split().str[0]
                    df = df[df['trade_date'] == date_clean]
                if 'ts_code' in df.columns:
                    df = df[df['ts_code'] == ts_code]
            except Exception as e:
                logger.debug(f"Parquet读取失败 {ts_code}@{date_str}: {e}")
                df = pd.DataFrame()

        if df.empty:
            cache_file = os.path.join(SimConfig.DATA_CACHE_DIR, f"{ts_code}_{date_clean}.csv")
            if os.path.exists(cache_file):
                try:
                    df = pd.read_csv(cache_file)
                except Exception as e:
                    logger.debug(f"CSV读取失败 {ts_code}_{date_clean}: {e}")

        if not df.empty:
            try:
                if 'trade_time' in df.columns:
                    df['时间'] = pd.to_datetime(df['trade_time'])
                elif 'date' in df.columns and 'time' in df.columns:
                    df['时间'] = pd.to_datetime(df['date'].astype(str) + " " + df['time'].astype(str))
                elif 'time' in df.columns:
                    df['时间'] = pd.to_datetime(date_clean + " " + df['time'].astype(str))
                elif '时间' in df.columns:
                    df['时间'] = pd.to_datetime(df['时间'])

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
                if df.empty: return pd.DataFrame()

                df["时间"] = pd.to_datetime(df["date"] + " " + df["time"])

                # 兼容 API 可能返回的字段名 (price 或 close)
                df["收盘"] = df.get("price", df.get("close", 0.0))

                # 获取其他字段，如果 API 没有提供，则使用收盘价/0 兜底，保证 Schema 完整
                df["开盘"] = df.get("open", df["收盘"])
                df["最高"] = df.get("high", df["收盘"])
                df["最低"] = df.get("low", df["收盘"])
                # 成交量可能是 volume 或 vol
                df["成交量"] = df.get("volume", df.get("vol", 0.0))

                # 返回完整的6个基础字段
                return df[["时间", "开盘", "收盘", "最高", "最低", "成交量"]].sort_values("时间")
            except Exception as e:
                logger.error(f"请求异常: {e}")
                time_module.sleep(SimConfig.EXPONENTIAL_BACKOFF_BASE ** attempt)
        return pd.DataFrame()


    import shutil # 需要导入 shutil
    @staticmethod
    def preload_from_models(start_date: str, end_date: str):
        logger.info("预下载器启动")

        # 【新增】：每次运行前清空 Cache 目录，确保不读取旧的、残缺的中间文件
        if os.path.exists(SimConfig.DATA_CACHE_DIR):
            logger.info(f"正在清理缓存目录: {SimConfig.DATA_CACHE_DIR}")
            shutil.rmtree(SimConfig.DATA_CACHE_DIR)
        os.makedirs(SimConfig.DATA_CACHE_DIR, exist_ok=True)

        dates = MarketData.get_model_dates(start_date, end_date)
        if not dates: return
        date_map = MarketData.build_date_map(dates)
        today_str = MarketData._get_current_cn_date()

        raw_pairs = set()
        for m_date in dates:
            t1, t2 = date_map[m_date]
            model_file = os.path.join(SimConfig.MODEL_HISTORY_DIR, f"{SimConfig.MODEL_NAME_PREFIX}_{m_date}.json")
            with open(model_file, 'r', encoding='utf-8') as f:
                targets, _ = MarketData.parse_sirius_model(json.load(f))
                for t in targets:
                    # 统一格式化 code，确保匹配时字符串完全一致
                    code = MarketData._convert_code(t['code'])
                    if t1 <= today_str: raw_pairs.add((code, t1))
                    if t2 <= today_str: raw_pairs.add((code, t2))

        # 构建已存在数据的集合
        parquet_keys_set = set()
        # 修正：更鲁棒的月份提取方式 (处理 2023-10-27 或 20231027)
        unique_months = set()
        for _, d in raw_pairs:
            clean_d = d.replace('-', '') # 转为 20231027
            unique_months.add(f"{clean_d[:4]}-{clean_d[4:6]}") # 统一转为 2023-10

        for ym in unique_months:
            p_path = MarketData.get_monthly_file_path(ym)
            if os.path.exists(p_path):
                try:
                    df_p = pd.read_parquet(p_path, columns=['ts_code', 'trade_date'])
                    # 【核心修复】：统一格式化
                    df_p['trade_date'] = pd.to_datetime(df_p['trade_date'].astype(str)).dt.strftime('%Y-%m-%d')
                    df_p['ts_code'] = df_p['ts_code'].astype(str).str.strip().str.zfill(6)

                    parquet_keys_set.update( (str(c).strip().zfill(6),pd.to_datetime(d).strftime('%Y-%m-%d'))for c, d in zip(df_p['ts_code'], df_p['trade_date']))
                    logger.info(f"已加载 {ym} 历史数据，共 {len(df_p)} 条记录")

                except Exception as e:
                    logger.error(f"读取 Parquet 异常 {p_path}: {e}")

        logger.info("raw_pairs")
        logger.info(raw_pairs)

        last_month = None
        for ts_code, t_date in raw_pairs:
            # 如果 Parquet 里已经有了，就不再下载
            # ⭐ 关键：统一格式（就在这里加）
            ts_code = str(ts_code).strip().zfill(6)
            t_date  = pd.to_datetime(t_date).strftime('%Y-%m-%d')
            if (ts_code, t_date) in parquet_keys_set:
                continue

            # 这里的 os.path.exists(csv) 在清空 Cache 后必然为 False，起到二次保险作用
            csv_path = os.path.join(SimConfig.DATA_CACHE_DIR, f"{ts_code}_{t_date}.csv")
            if os.path.exists(csv_path): continue

            if last_month and t_date[:7] != last_month:
                MarketData.merge_monthly_data(last_month)
            last_month = t_date[:7]

            # 执行下载
            df = MarketData._fetch_intraday_from_api(ts_code.split('.')[0], t_date)
            if not df.empty:
                df["ts_code"] = ts_code
                df["trade_date"] = t_date
                standard_columns = ['时间', '开盘', '收盘', '最高', '最低', '成交量', 'ts_code', 'trade_date']
                df = df[standard_columns]
                df.to_csv(csv_path, index=False)
                logger.info(f"下载成功: {ts_code} ({t_date})")

            time_module.sleep(SimConfig.API_REQUEST_INTERVAL)

        if last_month:
            MarketData.merge_monthly_data(last_month)

        missing = []
        for ts_code, t_date in raw_pairs:
           if t_date not in dates:
              continue
           df = MarketData.get_minute_data(ts_code, t_date)
           if df.empty:
              missing.append((ts_code, t_date))

        if missing:
           logger.error(f"❌ 缺失数据: {len(missing)} 条")
           logger.error(missing[:20])



    _daily_df = None
    _trading_dates_cache = {}

    @classmethod
    def _load_daily_data(cls):
        """加载 daily_data.parquet 并缓存"""
        if cls._daily_df is not None:
            return cls._daily_df

        daily_path = os.path.join(SimConfig.MONTHLY_DIR, "daily_data_qmt.parquet")
        if not os.path.exists(daily_path):
            logger.warning(f"日线数据文件不存在: {daily_path}")
            cls._daily_df = pd.DataFrame()
        else:
            df = pd.read_parquet(daily_path)
            # 统一格式化代码和日期
            if 'ts_code' in df.columns:
                df['ts_code'] = df['ts_code'].astype(str).str.strip().str.zfill(6)
            if 'trade_date' in df.columns:
                df['trade_date'] = pd.to_datetime(df['trade_date']).dt.strftime('%Y-%m-%d')
            cls._daily_df = df
        return cls._daily_df

    @classmethod
    def _get_trading_dates(cls, code: str) -> list:
        """基于日线数据获取该股票的所有交易日（升序）"""
        if code in cls._trading_dates_cache:
            return cls._trading_dates_cache[code]

        df = cls._load_daily_data()
        if df.empty:
            return []

        dates = df[df['ts_code'] == code]['trade_date'].unique().tolist()
        dates.sort()
        cls._trading_dates_cache[code] = dates
        return dates

    @classmethod
    def get_prev_trading_date(cls, code: str, current_date: str) -> str | None:
        """返回 current_date 之前最近的一个交易日"""
        dates = cls._get_trading_dates(code)
        if not dates:
            return None
        try:
            idx = dates.index(current_date)
            return dates[idx - 1] if idx > 0 else None
        except ValueError:
            # current_date 不在列表中（如停牌日），取第一个小于 current_date 的日期
            prev = [d for d in dates if d < current_date]
            return prev[-1] if prev else None

    @classmethod
    def get_prev_close(cls, code: str, trade_date: str) -> float | None:
        """
        从日线数据获取前一交易日的官方收盘价。
        若无法获取，返回 None。
        """
        prev_date = cls.get_prev_trading_date(code, trade_date)
        if prev_date is None:
            return None

        df = cls._load_daily_data()
        if df.empty:
            return None

        mask = (df['ts_code'] == code) & (df['trade_date'] == prev_date)
        row = df.loc[mask, '收盘']
        if not row.empty:
            return float(row.iloc[0])
        return None

# ========================= 4. 账户 & 执行器 (SIRIUS 核心) =========================
class MockAccount:
    def __init__(self, initial_cash):
        self.cash, self.positions = initial_cash, {}

    def start_day(self):
        for c in self.positions:
            self.positions[c]['can_sell'] = self.positions[c]['volume']

    # 【修改】增加 name 参数记录股票名称
    def order(self, date, time_v, code, side, vol, price, reason, name=""):
        vol = (vol // 100) * 100
        if vol <= 0: return False
        cost = vol * price

        actual_name = name if name else code
        display_name = f"{code}({actual_name})"

        if side == 'buy' and self.cash >= cost:
            self.cash -= cost
            p = self.positions.get(code, {'volume': 0, 'avg_price': 0.0, 'can_sell': 0, 'name': actual_name})

            p['avg_price'] = (p['volume'] * p['avg_price'] + cost) / (p['volume'] + vol)
            p['volume'] += vol
            p['name'] = actual_name
            if 'can_sell' not in p: p['can_sell'] = 0

            self.positions[code] = p
            logger.info(f"💰 {date} {time_v} | 买入 {display_name} {vol}股 @{price:.2f} ({reason})")
            return True

        elif side == 'sell':
            p = self.positions.get(code)
            if p and p.get('can_sell', 0) >= vol:
                self.cash += cost
                p['volume'] -= vol
                p['can_sell'] -= vol
                if p['volume'] <= 0: del self.positions[code]
                else: self.positions[code] = p
                logger.info(f"💰 {date} {time_v} | 卖出 {display_name} {vol}股 @{price:.2f} ({reason})")
                return True
        return False

# ========================= 严格对照执行器 =========================
class SiriusStrictExecutor:
    def __init__(self, account):
        self.account = account
        # 【新增】列表以追踪交易和快照
        self.all_trades = []
        self.daily_snapshots = []

    # 【新增】每日持仓快照功能
    def save_daily_snapshot(self, date_str):
        total_value = self.account.cash
        day_snaps = []

        for code, pos in self.account.positions.items():
            df = MarketData.get_minute_data(code, date_str)
            #last_price = df.iloc[-1]['收盘'] if not df.empty else pos['avg_price']
            if not df.empty:
                last_price = df.iloc[-1]['收盘']
            else:
                last_price = pos.get('last_price', pos['avg_price'])

            pos['last_price'] = last_price

            market_value = pos['volume'] * last_price
            total_value += market_value

            day_snaps.append({
                'date': date_str, 'code': code, 'name': pos.get('name', code),
                'volume': pos['volume'], 'can_sell': pos.get('can_sell', 0),
                'avg_price': pos['avg_price'], 'last_price': last_price,
                'market_value': market_value, 'weight': 0.0
            })

        for snap in day_snaps:
            snap['weight'] = snap['market_value'] / total_value if total_value > 0 else 0
            self.daily_snapshots.append(snap)

        self.daily_snapshots.append({
            'date': date_str, 'code': 'CASH', 'name': '现金',
            'volume': 0, 'can_sell': 0, 'avg_price': 0, 'last_price': 1,
            'market_value': self.account.cash, 'weight': self.account.cash / total_value if total_value > 0 else 0
        })
        self.daily_snapshots.append({
            'date': date_str, 'code': 'TOTAL', 'name': '总资产',
            'volume': 0, 'market_value': total_value, 'weight': 1.0
        })
        return total_value

    # 【新增】导出 Excel 功能
    def export_to_excel(self):
        with pd.ExcelWriter(SimConfig.TRADE_RECORD_FILE, engine='openpyxl') as writer:
            if self.all_trades:
                df_trades = pd.DataFrame(self.all_trades)
                df_trades['amount'] = df_trades['volume'] * df_trades['price']
                df_trades['day'] = pd.Categorical(df_trades['date']).codes + 1
                df_trades.to_excel(writer, sheet_name='交易记录', index=False)
                logger.info(f"导出交易记录: {len(df_trades)} 笔")
            if self.daily_snapshots:
                df_snap = pd.DataFrame(self.daily_snapshots)
                df_snap.to_excel(writer, sheet_name='持仓快照', index=False)
                logger.info(f"导出持仓快照: {len(df_snap)} 条")

    def simulate_day(self, date_str, targets, pos_factor, pre_closes_ignored=None):
        self.account.start_day()
        trade_time_morning = time(9, 30)

        # 构建名称映射字典用于交易记录
        name_map = {t['code']: t['name'] for t in targets}
        for code, pos in self.account.positions.items():
            if code not in name_map: name_map[code] = pos.get('name', code)

        prices_1000 = {}
        all_codes = list(set([t['code'] for t in targets] + list(self.account.positions.keys())))
        for code in all_codes:
            df = MarketData.get_minute_data(code, date_str)
            if not df.empty:
                target_dt = datetime.combine(datetime.strptime(date_str, "%Y-%m-%d").date(), trade_time_morning)
                mask = df['时间'] >= target_dt
                prices_1000[code] = df.loc[mask, '收盘'].iloc[0] if mask.any() else df.iloc[-1]['收盘']

        total_asset = self.account.cash + sum(p['volume'] * prices_1000.get(c, p['avg_price']) for c, p in self.account.positions.items())
        effective_asset = total_asset * SimConfig.TRADE_RATIO * pos_factor
        target_vols = {t['code']: int(effective_asset * t['weight'] / t['ref_price'] / 100) * 100 for t in targets}

        # ========== 修正：为所有持仓股票获取真实昨日收盘价 ==========
        real_prev_closes = {}
        for code in self.account.positions.keys():
            prev_close = MarketData.get_prev_close(code, date_str)
            if prev_close is not None:
                real_prev_closes[code] = prev_close
            else:
                # 兜底：无前收盘数据，使用成本价（并给出警告）
                real_prev_closes[code] = self.account.positions[code]['avg_price']
                logger.warning(f"{date_str} {code} 无前收盘价，使用成本价作为卖出参考")

        # A. 卖出指令
        for code, pos in list(self.account.positions.items()):
            t_vol = target_vols.get(code, 0)
            if pos['volume'] > t_vol:
                sell_vol = min(pos['can_sell'], pos['volume'] - t_vol)
                if sell_vol > 0:
                    real_p = prices_1000.get(code)
                    pre_close = real_prev_closes.get(code)
                    if real_p and real_p >= pre_close:
                        stk_name = name_map.get(code, code)
                        if self.account.order(date_str, trade_time_morning, code, 'sell', sell_vol, real_p, "早盘止盈", stk_name):
                            # 【新增】记录交易
                            self.all_trades.append({
                                'date': date_str, 'time': trade_time_morning.strftime('%H:%M'),
                                'code': code, 'name': stk_name, 'side': 'sell',
                                'volume': sell_vol, 'price': real_p, 'reason': "早盘止盈"
                            })

        # B. 买入指令
        available_cash = self.account.cash * SimConfig.TRADE_RATIO
        for code, t_vol in target_vols.items():
            cur_vol = self.account.positions.get(code, {}).get('volume', 0)
            if t_vol > cur_vol:
                buy_vol = t_vol - cur_vol
                real_p = prices_1000.get(code)
                ref_p = next(t['ref_price'] for t in targets if t['code'] == code)
                if real_p and real_p <= ref_p:
                    exec_p = min(real_p, ref_p)
                    if self.account.cash >= buy_vol * exec_p:
                        stk_name = name_map.get(code, code)
                        if self.account.order(date_str, trade_time_morning, code, 'buy', buy_vol, exec_p, "早盘调仓", stk_name):
                             # 【新增】记录交易
                             self.all_trades.append({
                                'date': date_str, 'time': trade_time_morning.strftime('%H:%M'),
                                'code': code, 'name': stk_name, 'side': 'buy',
                                'volume': buy_vol, 'price': exec_p, 'reason': "早盘调仓"
                            })

        # --- 2. 尾盘强制卖出阶段 ---
        trade_time_close = time(14, 50)
        prices_1450 = {}
        for code in self.account.positions.keys():
            df = MarketData.get_minute_data(code, date_str)
            if not df.empty:
                target_dt = datetime.combine(datetime.strptime(date_str, "%Y-%m-%d").date(), trade_time_close)
                mask = df['时间'] >= target_dt
                prices_1450[code] = df.loc[mask, '收盘'].iloc[0] if mask.any() else df.iloc[-1]['收盘']

        for code, pos in list(self.account.positions.items()):
            can_sell = pos.get('can_sell', 0)
            if can_sell <= 0:
                continue

            t_vol = target_vols.get(code, 0)

            # === 情况1：不在目标池 → 全清 ===
            if code not in target_vols:
                actual_sell = can_sell

            # === 情况2：在目标池但超仓 → 卖差额 ===
            elif pos['volume'] > t_vol:
                sell_needed = pos['volume'] - t_vol
                actual_sell = min(can_sell, sell_needed)

            # === 情况3：不用卖 ===
            else:
                actual_sell = 0

            if actual_sell <= 0:
                continue

            real_p = prices_1450.get(code)
            if not real_p:
                continue

            exec_p = real_p
            stk_name = name_map.get(code, code)

            if self.account.order(date_str, trade_time_close, code, 'sell',
                                  actual_sell, exec_p, "尾盘强制", stk_name):
                self.all_trades.append({
                    'date': date_str,
                    'time': trade_time_close.strftime('%H:%M'),
                    'code': code,
                    'name': stk_name,
                    'side': 'sell',
                    'volume': actual_sell,
                    'price': exec_p,
                    'reason': "尾盘强制"
                })

# ========================= 回测主函数 =========================
def run_strict_backtest():
    if SimConfig.ENABLE_PRELOAD:
        MarketData.preload_from_models(SimConfig.START_DATE, SimConfig.END_DATE)
        if getattr(SimConfig, 'ONLY_PRELOAD', False):
            logger.info("预加载完成，退出")
            return

    account = MockAccount(SimConfig.INITIAL_CASH)
    executor = SiriusStrictExecutor(account)
    model_dates = MarketData.get_model_dates(SimConfig.START_DATE, SimConfig.END_DATE)
    if not model_dates:
        logger.error("未找到模型文件")
        return
    logger.info(f"模型日期: {model_dates}")

    tz_cn = timezone(timedelta(hours=8))
    today_cn = datetime.now(tz_cn).strftime('%Y-%m-%d')

    trade_map = {}
    for i, m_date in enumerate(model_dates):
        if i < len(model_dates) - 1:
            trade_date = model_dates[i + 1]
        else:
            if today_cn not in model_dates and today_cn > m_date:
                trade_date = today_cn
            else:
                model_dt = datetime.strptime(m_date, "%Y-%m-%d")
                trade_date = (model_dt + timedelta(days=1)).strftime("%Y-%m-%d")

        # 👇【修复核心：这里少了一行赋值代码！】
        trade_map[trade_date] = m_date

    logger.info(f"交易日映射: {trade_map}")

    total_asset = SimConfig.INITIAL_CASH

    for trade_date in sorted(trade_map.keys()):
        if not (SimConfig.START_DATE <= trade_date <= SimConfig.END_DATE) and trade_date != today_cn:
          continue
        model_date = trade_map[trade_date]
        logger.info(f"========== 模型[{model_date}] -> 交易[{trade_date}] ==========")

        model_file = os.path.join(SimConfig.MODEL_HISTORY_DIR, f"{SimConfig.MODEL_NAME_PREFIX}_{model_date}.json")

        if not os.path.exists(model_file):
            logger.error(f"模型文件不存在: {model_file}")
            continue

        with open(model_file, 'r', encoding='utf-8') as f:
            targets, pf = MarketData.parse_sirius_model(json.load(f))

        pre_closes = {t['code']: t['ref_price'] for t in targets}

        # === 修复1：过滤无行情数据交易日 ===
        has_data = False
        check_codes = set([t['code'] for t in targets]) | set(account.positions.keys())

        for code in check_codes:
            df = MarketData.get_minute_data(code, trade_date)
            if not df.empty:
                has_data = True
                break

        if not has_data:
            logger.warning(f"{trade_date} 无行情数据，跳过交易，仅记录资产")
            total_asset = executor.save_daily_snapshot(trade_date)
            logger.info(f"交易日结束资产: {total_asset:.2f}")
            continue

        executor.simulate_day(trade_date, targets, pf, pre_closes)

        # 【修改】使用统一的快照方法计算并记录当日资产情况
        total_asset = executor.save_daily_snapshot(trade_date)
        logger.info(f"交易日结束资产: {total_asset:.2f}")

    # 【新增】导出所有交易和快照到 Excel
    executor.export_to_excel()
    logger.info(f"回测完成，结果保存至: {SimConfig.TRADE_RECORD_FILE}")

    # ==========================================
    # 【修改】参照代码2的最终统计样式
    # ==========================================
    if executor.all_trades:
        df = pd.DataFrame(executor.all_trades)
        buy_cnt = len(df[df['side'] == 'buy'])
        sell_cnt = len(df[df['side'] == 'sell'])

        final_snap = [s for s in executor.daily_snapshots if s.get('code') == 'TOTAL']
        if final_snap:
            final_asset = final_snap[-1]['market_value']
            ret = (final_asset / SimConfig.INITIAL_CASH - 1) * 100
            logger.info(f"\n{'='*50}\n回测统计:\n  初始资金: {SimConfig.INITIAL_CASH:.2f}\n"
                        f"  最终资产: {final_asset:.2f}\n  收益率: {ret:.2f}%\n"
                        f"  买入次数: {buy_cnt}\n  卖出次数: {sell_cnt}\n{'='*50}")

if __name__ == "__main__":
    run_strict_backtest()


# @title SIRIUS T1 BackTest Simulation (Dynamic Edition)

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SIRIUS T+1 自动交易策略 - 回测模拟器（纯盘中动态 + 尾盘强制卖出）
功能：
1. 从历史模型 JSON 加载目标权重
2. 使用分钟级 K 线数据模拟盘中动态交易：
   - 基于实时价格与 N 分钟均线的偏差，低买高卖
   - 冷却时间、滑点控制
3. 尾盘（14:50）强制卖出超出目标权重的股票（无价格下限）
4. 完整输出交易记录 Excel、每日持仓快照、收益曲线
"""

import os
import json
import logging
import pandas as pd
import numpy as np
from datetime import datetime, time, timedelta, timezone
import time as time_module
import glob
import requests
from typing import Dict, List, Optional, Tuple

# ========================= 1. 配置 (SIRIUS 回测专用) =========================
class SimConfig:
    # 回测时间范围
    START_DATE = "2025-09-12"
    END_DATE = "2026-04-20"
    INITIAL_CASH = 100000.0
    DEBUG = True

    # SIRIUS 策略参数
    TRADE_RATIO = 1               # 资金使用比例
    SLIPPAGE = 0.002                 # 滑点容忍度 (0.2%)
    PRICE_TOLERANCE = 0.005          # 价格容忍度 (0.5%)
    LOOKBACK_MINUTES = 30            # 计算均线的回溯分钟数
    BUY_THRESHOLD_PCT = -0.5         # 低于均线 X% 触发买入（负值）
    SELL_THRESHOLD_PCT = 0.5         # 高于均线 X% 触发卖出（正值）
    INTRADAY_SCAN_INTERVAL = 60      # 盘中扫描间隔（秒），在回测中对应分钟线频率
    INTRADAY_COOLDOWN_SEC = 120         # 同一股票动态交易冷却时间（秒）

    # 尾盘强制卖出时间
    FORCE_SELL_HOUR = 14
    FORCE_SELL_MINUTE = 50

    # 数据与模型路径
    MODEL_HISTORY_DIR = "./historical_models"
    DATA_CACHE_DIR = "./min_data_cache"
    MONTHLY_DIR = "./monthly_data"
    MODEL_NAME_PREFIX = f"{TARGET_MODE_NAME}" 

    # 数据获取 API（用于预下载）
    API_BASE_URL = "https://query.aivibeinvestment.com/api/query"
    API_REQUEST_INTERVAL = 0.3
    MAX_RETRIES = 5
    EXPONENTIAL_BACKOFF_BASE = 2

    # 预加载开关
    ENABLE_PRELOAD = True
    ONLY_PRELOAD = False

    # 涨跌停限制
    LIMIT_UP_RATIO = 0.1
    LIMIT_DOWN_RATIO = -0.1

    # 输出路径
    OUTPUT_DIR = "./backtest_results"
    TRADE_RECORD_FILE = os.path.join(OUTPUT_DIR, f"trade_records_dynamic_{MODEL_NAME_PREFIX}.xlsx")
    DAILY_SNAPSHOT_FILE = os.path.join(OUTPUT_DIR, "daily_snapshots.xlsx")

# 创建必要目录
for d in [SimConfig.MODEL_HISTORY_DIR, SimConfig.DATA_CACHE_DIR, SimConfig.MONTHLY_DIR, SimConfig.OUTPUT_DIR]:
    os.makedirs(d, exist_ok=True)

# ========================= 2. 日志 =========================
logger = logging.getLogger("SIRIUS_Simulator")
if logger.handlers:
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)

logger.setLevel(logging.DEBUG)
logger.propagate = False # 防止传递给 root logger 导致双重打印

formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
ch = logging.StreamHandler()
ch.setFormatter(formatter)
logger.addHandler(ch)

# ========================= 3. 数据模块 (完全复用原框架) =========================
class MarketData:
    @staticmethod
    def _get_current_cn_date() -> str:
        tz_cn = timezone(timedelta(hours=8))
        return datetime.now(tz_cn).strftime('%Y-%m-%d')

    @staticmethod
    def get_monthly_file_path(year_month: str) -> str:
        return os.path.join(SimConfig.MONTHLY_DIR, f"minute_data_{year_month}_qmt.parquet")

    @staticmethod
    def get_limit_prices(pre_close: float) -> tuple:
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

    # def _convert_code(code: str) -> str:
    #     c = str(code).split('.')[0].zfill(6)
    #     if len(c) > 6 and (c.endswith('.SH') or c.endswith('.SZ')):
    #         return c
    #     sh_prefixes = ('60', '68', '51', '56', '58', '55', '900')
    #     return f"{c}.SH" if any(c.startswith(p) for p in sh_prefixes) else f"{c}.SZ"

    @staticmethod
    def _convert_code(code: str) -> str:
      return code

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
                    'code': MarketData._convert_code(item.get('代码', '')),
                    'name': item.get('名称', ''),
                    'weight': weight,
                    'ref_price': float(item.get('最近一日价格', 0))
                })
            return targets, pos_factor
        except Exception as e:
            logger.error(f"解析模型失败: {e}")
            return [], 1.0


    @staticmethod
    def merge_monthly_data(year_month: str):
        """
        将 Cache 中的 CSV 数据合并到月度 Parquet 文件中
        year_month 格式: "2026-04"
        """
        p_path = MarketData.get_monthly_file_path(year_month)

        # 1. 获取 Cache 中属于该月份的所有 CSV 文件
        cache_files = [
            f for f in os.listdir(SimConfig.DATA_CACHE_DIR)
            if f.endswith(".csv") and year_month in f
        ]

        if not cache_files:
            return

        # 2. 读取所有新下载的 CSV 数据
        new_dfs = []
        for f in cache_files:
            try:
                # 【修复核心 1】：强制指定 ts_code 和 trade_date 为字符串格式
                temp_df = pd.read_csv(
                    os.path.join(SimConfig.DATA_CACHE_DIR, f),
                    dtype={'ts_code': str, 'trade_date': str}
                )
                new_dfs.append(temp_df)
            except Exception as e:
                logger.error(f"读取缓存CSV失败 {f}: {e}")

        if not new_dfs:
            return

        combined_df = pd.concat(new_dfs, ignore_index=True)

        # 3. 如果原有 Parquet 存在，先读取它
        if os.path.exists(p_path):
            try:
                old_df = pd.read_parquet(p_path)
                # 【修复核心 2】：旧数据读出来后，强制转字符串并补齐 6 位，防止原本存的已经是 int
                if 'ts_code' in old_df.columns:
                    old_df['ts_code'] = old_df['ts_code'].astype(str).str.zfill(6)

                # 将旧数据和新数据合并
                combined_df = pd.concat([old_df, combined_df], ignore_index=True)
                logger.info(f"正在合并旧数据 ({len(old_df)}条) 与新数据...")
            except Exception as e:
                logger.error(f"读取旧 Parquet 失败，可能会导致覆盖: {e}")

        # 4. 去重并保存
        if not combined_df.empty:
            # ========================================================
            # 4. 【核心修复区】：在去重和保存之前，暴力清洗所有的关键列类型
            # ========================================================
            if not combined_df.empty:
                # 修复 ArrowTypeError: 强制将 "时间" 列转为标准的 datetime 对象
                if '时间' in combined_df.columns:
                    combined_df['时间'] = pd.to_datetime(combined_df['时间'])

                # 强制统一 ts_code 和 trade_date 为纯正的字符串，杜绝混合类型
                if 'ts_code' in combined_df.columns:
                    combined_df['ts_code'] = combined_df['ts_code'].astype(str).str.strip().str.zfill(6)
                if 'trade_date' in combined_df.columns:
                    combined_df['trade_date'] = pd.to_datetime(combined_df['trade_date'].astype(str)).dt.strftime('%Y-%m-%d')

                # ========================================================

            # 以时间、代码、日期作为唯一键去重
            combined_df.drop_duplicates(subset=['时间', 'ts_code', 'trade_date'], inplace=True)
            # 排序，保证 Parquet 文件内部有序
            combined_df.sort_values(['ts_code', '时间'], inplace=True)

            # 5. 写入 Parquet (此时类型绝对统一，不会再崩溃)
            combined_df.to_parquet(p_path, index=False, engine='pyarrow')
            logger.info(f"✅ 月度数据已更新: {p_path} (新增后总计: {len(combined_df)} 条)")

            # 6. 合并成功后删除对应的 CSV 缓存
            for f in cache_files:
                try:
                    os.remove(os.path.join(SimConfig.DATA_CACHE_DIR, f))
                except:
                    pass

    @staticmethod
    def get_minute_data(code: str, date_str: str) -> pd.DataFrame:
        ts_code = MarketData._convert_code(code)
        monthly_file = MarketData.get_monthly_file_path(date_str[:7])
        date_clean = str(date_str).split()[0]
        df = pd.DataFrame()

        if os.path.exists(monthly_file):
            try:
                df = pd.read_parquet(monthly_file)
                if 'trade_date' in df.columns:
                    df['trade_date'] = df['trade_date'].astype(str).str.split().str[0]
                    df = df[df['trade_date'] == date_clean]
                if 'ts_code' in df.columns:
                    df = df[df['ts_code'] == ts_code]
            except Exception as e:
                logger.debug(f"Parquet读取失败 {ts_code}@{date_str}: {e}")
                df = pd.DataFrame()

        if df.empty:
            cache_file = os.path.join(SimConfig.DATA_CACHE_DIR, f"{ts_code}_{date_clean}.csv")
            if os.path.exists(cache_file):
                try:
                    df = pd.read_csv(cache_file)
                except Exception as e:
                    logger.debug(f"CSV读取失败 {ts_code}_{date_clean}: {e}")

        if not df.empty:
            try:
                if 'trade_time' in df.columns:
                    df['时间'] = pd.to_datetime(df['trade_time'])
                elif 'date' in df.columns and 'time' in df.columns:
                    df['时间'] = pd.to_datetime(df['date'].astype(str) + " " + df['time'].astype(str))
                elif 'time' in df.columns:
                    df['时间'] = pd.to_datetime(date_clean + " " + df['time'].astype(str))
                elif '时间' in df.columns:
                    df['时间'] = pd.to_datetime(df['时间'])

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
                if df.empty: return pd.DataFrame()

                df["时间"] = pd.to_datetime(df["date"] + " " + df["time"])

                # 兼容 API 可能返回的字段名 (price 或 close)
                df["收盘"] = df.get("price", df.get("close", 0.0))

                # 获取其他字段，如果 API 没有提供，则使用收盘价/0 兜底，保证 Schema 完整
                df["开盘"] = df.get("open", df["收盘"])
                df["最高"] = df.get("high", df["收盘"])
                df["最低"] = df.get("low", df["收盘"])
                # 成交量可能是 volume 或 vol
                df["成交量"] = df.get("volume", df.get("vol", 0.0))

                # 返回完整的6个基础字段
                return df[["时间", "开盘", "收盘", "最高", "最低", "成交量"]].sort_values("时间")
            except Exception as e:
                logger.error(f"请求异常: {e}")
                time_module.sleep(SimConfig.EXPONENTIAL_BACKOFF_BASE ** attempt)
        return pd.DataFrame()


    import shutil # 需要导入 shutil
    @staticmethod
    def preload_from_models(start_date: str, end_date: str):
        logger.info("预下载器启动")

        # 【新增】：每次运行前清空 Cache 目录，确保不读取旧的、残缺的中间文件
        if os.path.exists(SimConfig.DATA_CACHE_DIR):
            logger.info(f"正在清理缓存目录: {SimConfig.DATA_CACHE_DIR}")
            shutil.rmtree(SimConfig.DATA_CACHE_DIR)
        os.makedirs(SimConfig.DATA_CACHE_DIR, exist_ok=True)

        dates = MarketData.get_model_dates(start_date, end_date)
        if not dates: return
        date_map = MarketData.build_date_map(dates)
        today_str = MarketData._get_current_cn_date()

        raw_pairs = set()
        for m_date in dates:
            t1, t2 = date_map[m_date]
            model_file = os.path.join(SimConfig.MODEL_HISTORY_DIR, f"{SimConfig.MODEL_NAME_PREFIX}_{m_date}.json")
            with open(model_file, 'r', encoding='utf-8') as f:
                targets, _ = MarketData.parse_sirius_model(json.load(f))
                for t in targets:
                    # 统一格式化 code，确保匹配时字符串完全一致
                    code = MarketData._convert_code(t['code'])
                    if t1 <= today_str: raw_pairs.add((code, t1))
                    if t2 <= today_str: raw_pairs.add((code, t2))

        # 构建已存在数据的集合
        parquet_keys_set = set()
        # 修正：更鲁棒的月份提取方式 (处理 2023-10-27 或 20231027)
        unique_months = set()
        for _, d in raw_pairs:
            clean_d = d.replace('-', '') # 转为 20231027
            unique_months.add(f"{clean_d[:4]}-{clean_d[4:6]}") # 统一转为 2023-10

        for ym in unique_months:
            p_path = MarketData.get_monthly_file_path(ym)
            if os.path.exists(p_path):
                try:
                    df_p = pd.read_parquet(p_path, columns=['ts_code', 'trade_date'])
                    # 【核心修复】：统一格式化
                    df_p['trade_date'] = pd.to_datetime(df_p['trade_date'].astype(str)).dt.strftime('%Y-%m-%d')
                    df_p['ts_code'] = df_p['ts_code'].astype(str).str.strip().str.zfill(6)

                    parquet_keys_set.update( (str(c).strip().zfill(6),pd.to_datetime(d).strftime('%Y-%m-%d'))for c, d in zip(df_p['ts_code'], df_p['trade_date']))

                    logger.info(f"已加载 {ym} 历史数据，共 {len(df_p)} 条记录")

                except Exception as e:
                    logger.error(f"读取 Parquet 异常 {p_path}: {e}")

        logger.info("raw_pairs")
        logger.info(raw_pairs)


        last_month = None
        for ts_code, t_date in raw_pairs:
            # 如果 Parquet 里已经有了，就不再下载
            # ⭐ 关键：统一格式（就在这里加）
            ts_code = str(ts_code).strip().zfill(6)
            t_date  = pd.to_datetime(t_date).strftime('%Y-%m-%d')
            if (ts_code, t_date) in parquet_keys_set:
                continue

            # 这里的 os.path.exists(csv) 在清空 Cache 后必然为 False，起到二次保险作用
            csv_path = os.path.join(SimConfig.DATA_CACHE_DIR, f"{ts_code}_{t_date}.csv")
            if os.path.exists(csv_path): continue

            if last_month and t_date[:7] != last_month:
                MarketData.merge_monthly_data(last_month)
            last_month = t_date[:7]

            # 执行下载
            df = MarketData._fetch_intraday_from_api(ts_code.split('.')[0], t_date)
            if not df.empty:
                df["ts_code"] = ts_code
                df["trade_date"] = t_date
                standard_columns = ['时间', '开盘', '收盘', '最高', '最低', '成交量', 'ts_code', 'trade_date']
                df = df[standard_columns]
                df.to_csv(csv_path, index=False)
                logger.info(f"下载成功: {ts_code} ({t_date})")

            time_module.sleep(SimConfig.API_REQUEST_INTERVAL)

        if last_month:
            MarketData.merge_monthly_data(last_month)

        missing = []
        for ts_code, t_date in raw_pairs:
           if t_date not in dates:
              continue
           df = MarketData.get_minute_data(ts_code, t_date)
           if df.empty:
              missing.append((ts_code, t_date))

        if missing:
           logger.error(f"❌ 缺失数据: {len(missing)} 条")
           logger.error(missing[:20])

    _daily_df = None
    _trading_dates_cache = {}

    @classmethod
    def _load_daily_data(cls):
        """加载 daily_data.parquet 并缓存"""
        if cls._daily_df is not None:
            return cls._daily_df

        daily_path = os.path.join(SimConfig.MONTHLY_DIR, "daily_data_qmt.parquet")
        if not os.path.exists(daily_path):
            logger.warning(f"日线数据文件不存在: {daily_path}")
            cls._daily_df = pd.DataFrame()
        else:
            df = pd.read_parquet(daily_path)
            # 统一格式化代码和日期
            if 'ts_code' in df.columns:
                df['ts_code'] = df['ts_code'].astype(str).str.strip().str.zfill(6)
            if 'trade_date' in df.columns:
                df['trade_date'] = pd.to_datetime(df['trade_date']).dt.strftime('%Y-%m-%d')
            cls._daily_df = df
        return cls._daily_df

    @classmethod
    def _get_trading_dates(cls, code: str) -> list:
        """基于日线数据获取该股票的所有交易日（升序）"""
        if code in cls._trading_dates_cache:
            return cls._trading_dates_cache[code]

        df = cls._load_daily_data()
        if df.empty:
            return []

        dates = df[df['ts_code'] == code]['trade_date'].unique().tolist()
        dates.sort()
        cls._trading_dates_cache[code] = dates
        return dates

    @classmethod
    def get_prev_trading_date(cls, code: str, current_date: str) -> str | None:
        """返回 current_date 之前最近的一个交易日"""
        dates = cls._get_trading_dates(code)
        if not dates:
            return None
        try:
            idx = dates.index(current_date)
            return dates[idx - 1] if idx > 0 else None
        except ValueError:
            # current_date 不在列表中（如停牌日），取第一个小于 current_date 的日期
            prev = [d for d in dates if d < current_date]
            return prev[-1] if prev else None

    @classmethod
    def get_prev_close(cls, code: str, trade_date: str) -> float | None:
        """
        从日线数据获取前一交易日的官方收盘价。
        若无法获取，返回 None。
        """
        prev_date = cls.get_prev_trading_date(code, trade_date)
        if prev_date is None:
            return None

        df = cls._load_daily_data()
        if df.empty:
            return None

        mask = (df['ts_code'] == code) & (df['trade_date'] == prev_date)
        row = df.loc[mask, '收盘']
        if not row.empty:
            return float(row.iloc[0])
        return None

# ========================= 4. 模拟账户 (修正名称存储) =========================
class MockAccount:
    def __init__(self, initial_cash):
        self.cash = initial_cash
        self.positions = {}
        self.today_buys = set()

    def start_day(self):
        self.today_buys.clear()
        for code in self.positions:
            self.positions[code]['can_sell'] = self.positions[code].get('volume', 0)

    def order(self, date, time_v, code, side, vol, price, reason, name=""):
        vol = (vol // 100) * 100
        if vol <= 0: return False
        cost = vol * price

        # 确保名称始终有效
        actual_name = name if name else code
        display_name = f"{code}({actual_name})"

        if side == 'buy' and self.cash >= cost:
            self.cash -= cost
            if code in self.positions:
                p = self.positions[code]
            else:
                p = {'volume': 0, 'avg_price': 0.0, 'can_sell': 0, 'name': actual_name}

            total_cost = p['volume'] * p['avg_price'] + cost
            p['volume'] += vol
            p['avg_price'] = total_cost / p['volume']
            p['name'] = actual_name # 强制更新/保持名称
            self.positions[code] = p
            self.today_buys.add(code)
            logger.info(f"💰 {date} {time_v} | 买入 {display_name} {vol}股 @{price:.2f} ({reason}) [T+1锁定]")
            return True

        elif side == 'sell':
            if code not in self.positions: return False
            if code in self.today_buys: return False
            p = self.positions[code]
            available = p.get('can_sell', 0)
            if available < vol: return False

            self.cash += vol * price
            p['volume'] -= vol
            p['can_sell'] -= vol
            if p['volume'] <= 0:
                del self.positions[code]
            else:
                self.positions[code] = p
            logger.info(f"💰 {date} {time_v} | 卖出 {display_name} {vol}股 @{price:.2f} ({reason})")
            return True
        return False

# ========================= 5. SIRIUS 模拟执行器 (修正名称查找) =========================
class SiriusSimulator:
    def __init__(self, account):
        self.account = account
        self.today_trades = []
        self.all_trades = []
        self.daily_snapshots = []
        self.last_dynamic_trade_time = {}

    # 修正：增加一个全局代码-名称映射表，防止名称丢失
    def _get_names_map(self, targets):
        n_map = {t['code']: t['name'] for t in targets}
        # 补全当前持仓里的名称
        for code, pos in self.account.positions.items():
            if code not in n_map:
                n_map[code] = pos.get('name', code)
        return n_map

    def save_daily_snapshot(self, date_str):
        total_value = self.account.cash
        day_snaps = [] # 临时存放当日记录

        for code, pos in self.account.positions.items():
            df = MarketData.get_minute_data(code, date_str)
            #last_price = df.iloc[-1]['收盘'] if not df.empty else pos['avg_price']
            if not df.empty:
                last_price = df.iloc[-1]['收盘']
            else:
                last_price = pos.get('last_price', pos['avg_price'])
            pos['last_price'] = last_price

            market_value = pos['volume'] * last_price
            total_value += market_value

            day_snaps.append({
                'date': date_str, 'code': code, 'name': pos.get('name', code),
                'volume': pos['volume'], 'can_sell': pos.get('can_sell', 0),
                'avg_price': pos['avg_price'], 'last_price': last_price,
                'market_value': market_value, 'weight': 0.0
            })

        # 计算权重
        for snap in day_snaps:
            snap['weight'] = snap['market_value'] / total_value if total_value > 0 else 0
            self.daily_snapshots.append(snap)

        self.daily_snapshots.append({
            'date': date_str, 'code': 'CASH', 'name': '现金',
            'volume': 0, 'can_sell': 0, 'avg_price': 0, 'last_price': 1,
            'market_value': self.account.cash, 'weight': self.account.cash / total_value if total_value > 0 else 0
        })
        self.daily_snapshots.append({
            'date': date_str, 'code': 'TOTAL', 'name': '总资产',
            'volume': 0, 'market_value': total_value, 'weight': 1.0
        })
        return total_value

    def simulate_day(self, date_str: str, targets: List[Dict], position_factor: float, pre_closes_ignored: Dict):
        """盘中动态交易 + 限制条件"""
        self.account.start_day()
        self.today_trades.clear()

        # 建立名称和参考价映射表，方便快速查找
        target_info = {t['code']: {'name': t['name'], 'ref_price': t['ref_price']} for t in targets}

        # 1. 加载数据
        all_codes = set([t['code'] for t in targets] + list(self.account.positions.keys()))
        daily_data = {}
        for code in all_codes:
            df = MarketData.get_minute_data(code, date_str)
            if not df.empty:
                df = df.set_index('时间')
                daily_data[code] = df


        # ========== 修正：为所有持仓股票获取真实昨日收盘价 ==========
        real_prev_closes = {}
        for code in self.account.positions.keys():
            prev_close = MarketData.get_prev_close(code, date_str)
            if prev_close is not None:
                real_prev_closes[code] = prev_close
            else:
                # 兜底：无前收盘数据，使用成本价（并给出警告）
                real_prev_closes[code] = self.account.positions[code]['avg_price']
                logger.warning(f"{date_str} {code} 无前收盘价，使用成本价作为卖出参考")

        # 2. 计算基准资产和目标股数
        dt_start = datetime.combine(datetime.strptime(date_str, "%Y-%m-%d").date(), time(9, 31))
        
        # 初始定价逻辑
        initial_prices = {}
        for code, df in daily_data.items():
            mask = df.index >= dt_start
            initial_prices[code] = df[mask]['收盘'].iloc[0] if mask.any() else real_prev_closes.get(code, 0)

        def compute_target_volumes(current_asset):
            risk_asset = current_asset * SimConfig.TRADE_RATIO * position_factor
            target = {}
            for t in targets:
                price = initial_prices.get(t['code'], t['ref_price'])
                if price <= 0: continue
                vol = int(risk_asset * t['weight'] / price / 100) * 100
                if vol > 0: target[t['code']] = vol
            return target

        # ==================== 核心修复点开始 ====================
        # 在开盘前一次性计算出当天的初始总资产，并锁定今天的目标买入股数
        daily_start_asset = self.account.cash
        for code, pos in self.account.positions.items():
            price = initial_prices.get(code, real_prev_closes.get(code, pos['avg_price']))
            daily_start_asset += pos['volume'] * price

        target_vols = compute_target_volumes(daily_start_asset)
        # ==================== 核心修复点结束 ====================

        # 3. 分钟循环
        all_timestamps = set()
        for df in daily_data.values():
            all_timestamps.update(df[df.index >= dt_start].index)
        sorted_times = sorted(list(all_timestamps))

        last_scan_ts = 0.0

        for current_dt in sorted_times:
            current_time = current_dt.time()
            now_ts = current_dt.timestamp()

            # （此处的 target_vols 已固定，不再随盘中价格实时变动）

            # ---- 尾盘强制卖出 ----
            if current_time >= time(SimConfig.FORCE_SELL_HOUR, SimConfig.FORCE_SELL_MINUTE):
                for code, pos in list(self.account.positions.items()):
                    target_vol = target_vols.get(code, 0)
                    if pos['volume'] > target_vol:
                        sell_vol = min(pos.get('can_sell', 0), pos['volume'] - target_vol)
                        price = daily_data[code].loc[current_dt, '收盘']
                        stk_name = target_info.get(code, {}).get('name', code)
                        if self.account.order(date_str, current_time, code, 'sell', sell_vol, price, "强制卖出", stk_name):
                            self.today_trades.append({
                                'date': date_str, 'time': current_time.strftime('%H:%M'),
                                'code': code, 'name': stk_name, 'side': 'sell',
                                'volume': sell_vol, 'price': price, 'reason': "尾盘强制卖出"
                            })
                continue

            # ---- 盘中动态交易扫描 ----
            if (now_ts - last_scan_ts) >= SimConfig.INTRADAY_SCAN_INTERVAL:
                last_scan_ts = now_ts

                for code, target_vol in target_vols.items():
                    df = daily_data.get(code)
                    if df is None or current_dt not in df.index: continue

                    real_price = df.loc[current_dt, '收盘']
                    # 获取模型参考价（昨收）
                    ref_price = target_info[code]['ref_price']
                    stk_name = target_info[code]['name']

                    # 计算均线偏离度
                    recent_df = df.loc[:current_dt].tail(SimConfig.LOOKBACK_MINUTES)
                    if len(recent_df) < 5: continue
                    dyn_price = recent_df['收盘'].mean()
                    deviation = (real_price - dyn_price) / dyn_price * 100

                    # 冷却检查
                    if now_ts - self.last_dynamic_trade_time.get(code, 0) < SimConfig.INTRADAY_COOLDOWN_SEC:
                        continue

                    pos = self.account.positions.get(code, {'volume': 0, 'can_sell': 0, 'avg_price': 0.0})

                    # 【买入逻辑】
                    if deviation <= SimConfig.BUY_THRESHOLD_PCT:
                        if real_price <= ref_price * (1 + SimConfig.PRICE_TOLERANCE):
                            buy_vol = (target_vol - pos['volume']) // 100 * 100
                            if buy_vol >= 100:
                                if self.account.order(date_str, current_time, code, 'buy', buy_vol, real_price,
                                                      f"动态低吸(偏离{deviation:.1f}%)", stk_name):
                                    self.today_trades.append({
                                        'date': date_str, 'time': current_time.strftime('%H:%M'),
                                        'code': code, 'name': stk_name, 'side': 'buy',
                                        'volume': buy_vol, 'price': real_price, 'reason': f"动态买入(偏离{deviation:.1f}%)"
                                    })
                                    self.last_dynamic_trade_time[code] = now_ts

                    # 【卖出逻辑】
                    elif deviation >= SimConfig.SELL_THRESHOLD_PCT:
                        if real_price >= real_prev_closes.get(code, pos['avg_price']) * (1 - SimConfig.PRICE_TOLERANCE):
                            excess = pos['volume'] - target_vol
                            sell_vol = min(pos['can_sell'], excess) // 100 * 100
                            if sell_vol >= 100:
                                if self.account.order(date_str, current_time, code, 'sell', sell_vol, real_price,
                                                      f"动态高抛(偏离{deviation:.1f}%)", stk_name):
                                    self.today_trades.append({
                                        'date': date_str, 'time': current_time.strftime('%H:%M'),
                                        'code': code, 'name': stk_name, 'side': 'sell',
                                        'volume': sell_vol, 'price': real_price, 'reason': f"动态卖出(偏离{deviation:.1f}%)"
                                    })
                                    self.last_dynamic_trade_time[code] = now_ts

        self.all_trades.extend(self.today_trades)

    def export_to_excel(self):
        """导出交易记录和快照"""
        with pd.ExcelWriter(SimConfig.TRADE_RECORD_FILE, engine='openpyxl') as writer:
            if self.all_trades:
                df_trades = pd.DataFrame(self.all_trades)
                df_trades['amount'] = df_trades['volume'] * df_trades['price']
                df_trades['day'] = pd.Categorical(df_trades['date']).codes + 1
                df_trades.to_excel(writer, sheet_name='交易记录', index=False)
                logger.info(f"导出交易记录: {len(df_trades)} 笔")
            if self.daily_snapshots:
                df_snap = pd.DataFrame(self.daily_snapshots)
                df_snap.to_excel(writer, sheet_name='持仓快照', index=False)
                logger.info(f"导出持仓快照: {len(df_snap)} 条")

# ========================= 6. 回测主流程 =========================
def run_backtest():
    if SimConfig.ENABLE_PRELOAD:
        MarketData.preload_from_models(SimConfig.START_DATE, SimConfig.END_DATE)
        if SimConfig.ONLY_PRELOAD:
            logger.info("预加载完成，退出")
            return

    account = MockAccount(SimConfig.INITIAL_CASH)
    sim = SiriusSimulator(account)

    model_dates = MarketData.get_model_dates(SimConfig.START_DATE, SimConfig.END_DATE)
    if not model_dates:
        logger.error("未找到模型文件")
        return
    logger.info(f"模型日期: {model_dates}")

    tz_cn = timezone(timedelta(hours=8))
    today_cn = datetime.now(tz_cn).strftime('%Y-%m-%d')

    # 构建交易日映射 (交易日期 -> 模型日期)
    trade_map = {}
    for i, m_date in enumerate(model_dates):
        if i < len(model_dates) - 1:
            trade_date = model_dates[i + 1]
        else:
            if today_cn not in model_dates and today_cn > m_date:
                trade_date = today_cn
            else:
                model_dt = datetime.strptime(m_date, "%Y-%m-%d")
                trade_date = (model_dt + timedelta(days=1)).strftime("%Y-%m-%d")
        trade_map[trade_date] = m_date

    logger.info(f"交易日映射: {trade_map}")

    for trade_date in sorted(trade_map.keys()):
        if not (SimConfig.START_DATE <= trade_date <= SimConfig.END_DATE) and trade_date != today_cn:
            continue
        model_date = trade_map[trade_date]
        logger.info(f"========== 模型[{model_date}] -> 交易[{trade_date}] ==========")

        model_file = os.path.join(SimConfig.MODEL_HISTORY_DIR, f"{SimConfig.MODEL_NAME_PREFIX}_{model_date}.json")
        if not os.path.exists(model_file):
            logger.error(f"模型文件不存在: {model_file}")
            continue
        with open(model_file, 'r', encoding='utf-8') as f:
            targets, pos_factor = MarketData.parse_sirius_model(json.load(f))

        pre_closes = {t['code']: t['ref_price'] for t in targets}
        sim.simulate_day(trade_date, targets, pos_factor, pre_closes)

        total_asset = sim.save_daily_snapshot(trade_date)
        logger.info(f"交易日结束资产: {total_asset:.2f}")

    sim.export_to_excel()
    logger.info(f"回测完成，结果保存至: {SimConfig.TRADE_RECORD_FILE}")

    # 统计输出
    if sim.all_trades:
        df = pd.DataFrame(sim.all_trades)
        buy_cnt = len(df[df['side'] == 'buy'])
        sell_cnt = len(df[df['side'] == 'sell'])
        final_snap = [s for s in sim.daily_snapshots if s.get('code') == 'TOTAL']
        if final_snap:
            final_asset = final_snap[-1]['market_value']
            ret = (final_asset / SimConfig.INITIAL_CASH - 1) * 100
            logger.info(f"\n{'='*50}\n回测统计:\n  初始资金: {SimConfig.INITIAL_CASH:.2f}\n"
                        f"  最终资产: {final_asset:.2f}\n  收益率: {ret:.2f}%\n"
                        f"  买入次数: {buy_cnt}\n  卖出次数: {sell_cnt}\n{'='*50}")

if __name__ == "__main__":
    run_backtest()
