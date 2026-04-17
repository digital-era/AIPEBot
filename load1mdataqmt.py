# @title Preload Minute Data from QMT
import os
import json
import logging
import pandas as pd
from datetime import datetime, time, timedelta, timezone
import time as time_module
import glob
import requests
import random
import shutil
from typing import List, Tuple, Set

# 尝试导入 miniQMT 接口
try:
    from xtquant import xtdata
    MINIQMT_AVAILABLE = True
except ImportError:
    MINIQMT_AVAILABLE = False
    logger.warning("miniQMT 未安装或无法导入，preload_from_miniqmt 将不可用")

# ========================= 1. 配置 ) =========================

START_DATE = "2026-01-05"
END_DATE = "2026-04-17"

ODEL_HISTORY_DIR = "./historical_models"
MONTHLY_DIR = "./monthly_data"
MODEL_NAME_PREFIX = "流入模型"
DATA_CACHE_DIR = "./min_data_cache"
API_BASE_URL = "https://query.aivibeinvestment.com/api/query"
API_REQUEST_INTERVAL = 0.3
MAX_RETRIES = 5
EXPONENTIAL_BACKOFF_BASE = 2
FILL_OHLC_WITH_PRICE = True

# 创建目录
for d in [MODEL_HISTORY_DIR, MONTHLY_DIR]:
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
        return os.path.join(MONTHLY_DIR, f"minute_data_{year_month}.parquet")

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
      return code

    @staticmethod
    def get_model_dates(start_date: str, end_date: str) -> list:
        pattern = os.path.join(MODEL_HISTORY_DIR, f"{MODEL_NAME_PREFIX}_*.json")
        dates = []
        for f in glob.glob(pattern):
            basename = os.path.basename(f)
            date_str = basename.replace(f"{MODEL_NAME_PREFIX}_", "").replace(".json", "")
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
        p_path = MarketData.get_monthly_file_path(year_month)
        cache_files = [
            f for f in os.listdir(DATA_CACHE_DIR)
            if f.endswith(".csv") and year_month in f
        ]

        if not cache_files:
            return

        new_dfs = []
        for f in cache_files:
            try:
                temp_df = pd.read_csv(
                    os.path.join(DATA_CACHE_DIR, f),
                    dtype={'ts_code': str, 'trade_date': str}
                )
                new_dfs.append(temp_df)
            except Exception as e:
                logger.error(f"读取缓存CSV失败 {f}: {e}")

        if not new_dfs:
            return

        combined_df = pd.concat(new_dfs, ignore_index=True)

        if os.path.exists(p_path):
            try:
                old_df = pd.read_parquet(p_path)
                if 'ts_code' in old_df.columns:
                    old_df['ts_code'] = old_df['ts_code'].astype(str).str.zfill(6)
                combined_df = pd.concat([old_df, combined_df], ignore_index=True)
            except Exception as e:
                logger.error(f"读取旧 Parquet 失败: {e}")

        if not combined_df.empty:
            combined_df['ts_code'] = combined_df['ts_code'].astype(str).str.zfill(6)
            combined_df['trade_date'] = combined_df['trade_date'].astype(str)
            combined_df.drop_duplicates(subset=['时间', 'ts_code', 'trade_date'], inplace=True)
            combined_df.sort_values(['ts_code', '时间'], inplace=True)
            combined_df.to_parquet(p_path, index=False, engine='pyarrow')
            
            for f in cache_files:
                try:
                    os.remove(os.path.join(DATA_CACHE_DIR, f))
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
                df = pd.DataFrame()

        if df.empty:
            cache_file = os.path.join(ATA_CACHE_DIR, f"{ts_code}_{date_clean}.csv")
            if os.path.exists(cache_file):
                try:
                    df = pd.read_csv(cache_file)
                except Exception as e:
                    pass

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
                pass
        return pd.DataFrame()

    @staticmethod
    def _fetch_intraday_from_api(code: str, date_str: str) -> pd.DataFrame:
        api_url = f"{API_BASE_URL.rstrip('/')}?type=specifiedIntraday&code={code}&date={date_str}"
        for attempt in range(1, MAX_RETRIES + 1):
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
                df["收盘"] = df.get("price", df.get("close", 0.0))
                df["开盘"] = df.get("open", df["收盘"])
                df["最高"] = df.get("high", df["收盘"])
                df["最低"] = df.get("low", df["收盘"])
                df["成交量"] = df.get("volume", df.get("vol", 0.0))
                return df[["时间", "开盘", "收盘", "最高", "最低", "成交量"]].sort_values("时间")
            except Exception as e:
                time_module.sleep(EXPONENTIAL_BACKOFF_BASE ** attempt)
        return pd.DataFrame()

    import shutil
    @staticmethod
    def preload_from_models(start_date: str, end_date: str):
        if os.path.exists(DATA_CACHE_DIR):
            shutil.rmtree(DATA_CACHE_DIR)
        os.makedirs(DATA_CACHE_DIR, exist_ok=True)

        dates = MarketData.get_model_dates(start_date, end_date)
        if not dates: return
        date_map = MarketData.build_date_map(dates)
        today_str = MarketData._get_current_cn_date()

        raw_pairs = set()
        for m_date in dates:
            t1, t2 = date_map[m_date]
            model_file = os.path.join(MODEL_HISTORY_DIR, f"{MODEL_NAME_PREFIX}_{m_date}.json")
            with open(model_file, 'r', encoding='utf-8') as f:
                targets, _ = MarketData.parse_sirius_model(json.load(f))
                for t in targets:
                    code = MarketData._convert_code(t['code'])
                    if t1 <= today_str: raw_pairs.add((code, t1))
                    if t2 <= today_str: raw_pairs.add((code, t2))

        parquet_keys_set = set()
        unique_months = set()
        for _, d in raw_pairs:
            clean_d = d.replace('-', '')
            unique_months.add(f"{clean_d[:4]}-{clean_d[4:6]}")

        for ym in unique_months:
            p_path = MarketData.get_monthly_file_path(ym)
            if os.path.exists(p_path):
                try:
                    df_p = pd.read_parquet(p_path, columns=['ts_code', 'trade_date'])
                    df_p['trade_date'] = pd.to_datetime(df_p['trade_date']).dt.strftime('%Y-%m-%d')
                    df_p['ts_code'] = df_p['ts_code'].astype(str)
                    parquet_keys_set.update(set(zip(df_p['ts_code'], df_p['trade_date'])))
                except Exception as e:
                    pass

        last_month = None
        for ts_code, t_date in raw_pairs:
            if (ts_code, t_date) in parquet_keys_set:
                continue
            csv_path = os.path.join(DATA_CACHE_DIR, f"{ts_code}_{t_date}.csv")
            if os.path.exists(csv_path): continue

            if last_month and t_date[:7] != last_month:
                MarketData.merge_monthly_data(last_month)
            last_month = t_date[:7]

            df = MarketData._fetch_intraday_from_api(ts_code.split('.')[0], t_date)
            if not df.empty:
                df["ts_code"] = ts_code
                df["trade_date"] = t_date
                standard_columns = ['时间', '开盘', '收盘', '最高', '最低', '成交量', 'ts_code', 'trade_date']
                df = df[standard_columns]
                df.to_csv(csv_path, index=False)

            time_module.sleep(API_REQUEST_INTERVAL)

        if last_month:
            MarketData.merge_monthly_data(last_month)


        @staticmethod
    def _to_miniqmt_code(code: str) -> str:
        """将6位数字代码转换为 miniQMT 格式（带市场后缀）"""
        code = str(code).zfill(6)
        if code.startswith('6'):
            return f"{code}.SH"
        elif code.startswith('0') or code.startswith('3'):
            return f"{code}.SZ"
        else:
            # 默认当作深市
            return f"{code}.SZ"

    @staticmethod
    def _fetch_miniqmt_intraday(code: str, date_str: str) -> pd.DataFrame:
        """
        通过 miniQMT 获取某只股票某一天的1分钟K线数据
        返回 DataFrame 包含列：时间, 开盘, 收盘, 最高, 最低, 成交量
        """
        if not MINIQMT_AVAILABLE:
            raise RuntimeError("miniQMT 不可用，请检查 xtquant 导入")

        miniqmt_code = MarketData._to_miniqmt_code(code)
        # download_history_data 参数：股票代码，周期，开始时间，结束时间
        # 周期 '1m' 表示1分钟线，时间格式 'YYYYMMDD'
        start_time = date_str.replace('-', '')
        end_time = start_time
        try:
            # 下载数据（阻塞）
            xtdata.download_history_data(miniqmt_code, '1m', start_time, end_time)
            # 获取数据
            data = xtdata.get_market_data(
                stock_list=[miniqmt_code],
                period='1m',
                start_time=start_time,
                end_time=end_time,
                fields=['open', 'close', 'high', 'low', 'volume']
            )
            if data is None or len(data) == 0:
                return pd.DataFrame()
            # data 通常是一个 dict，键为股票代码，值为 numpy 数组或 DataFrame
            # 根据实际 xtdata 版本处理，这里按常见结构解析
            if isinstance(data, dict) and miniqmt_code in data:
                df = data[miniqmt_code]
                if isinstance(df, pd.DataFrame):
                    # 确保索引为时间
                    df = df.reset_index().rename(columns={'index': '时间'})
                else:
                    # 可能是 numpy structured array，转换为 DataFrame
                    df = pd.DataFrame(df)
                    if 'time' in df.columns:
                        df = df.rename(columns={'time': '时间'})
            elif isinstance(data, pd.DataFrame):
                df = data.reset_index().rename(columns={'index': '时间'})
            else:
                return pd.DataFrame()

            # 标准化列名
            rename_map = {
                'open': '开盘', 'close': '收盘', 'high': '最高',
                'low': '最低', 'volume': '成交量', 'vol': '成交量'
            }
            df = df.rename(columns=rename_map)
            required = ['时间', '开盘', '收盘', '最高', '最低', '成交量']
            for col in required:
                if col not in df.columns:
                    if col == '成交量' and 'volume' in df.columns:
                        continue
                    return pd.DataFrame()
            # 转换时间列
            df['时间'] = pd.to_datetime(df['时间'])
            # 过滤掉非交易时段（可选）
            df = df.sort_values('时间')
            return df[required]
        except Exception as e:
            logger.error(f"miniQMT 获取 {code} {date_str} 失败: {e}")
            return pd.DataFrame()

    @staticmethod
    def preload_from_miniqmt(start_date: str, end_date: str):
        """
        通过 miniQMT 下载分钟数据，生成带 _qmt 后缀的月度 parquet 文件。
        数据格式与 preload_from_models 完全一致。
        """
        if not MINIQMT_AVAILABLE:
            logger.error("miniQMT 未就绪，无法执行 preload_from_miniqmt")
            return

        # 1. 获取模型日期及映射
        dates = MarketData.get_model_dates(start_date, end_date)
        if not dates:
            logger.warning("未找到任何模型文件")
            return
        date_map = MarketData.build_date_map(dates)
        today_str = MarketData._get_current_cn_date()

        # 2. 收集所有需要下载的 (股票代码, 日期)
        raw_pairs: Set[Tuple[str, str]] = set()
        for m_date in dates:
            t1, t2 = date_map[m_date]
            model_file = os.path.join(MODEL_HISTORY_DIR, f"{MODEL_NAME_PREFIX}_{m_date}.json")
            if not os.path.exists(model_file):
                continue
            with open(model_file, 'r', encoding='utf-8') as f:
                targets, _ = MarketData.parse_sirius_model(json.load(f))
                for t in targets:
                    code = MarketData._convert_code(t['code'])
                    if t1 <= today_str:
                        raw_pairs.add((code, t1))
                    if t2 <= today_str:
                        raw_pairs.add((code, t2))

        # 3. 按月份分组，并检查已存在的 _qmt parquet 中已包含的数据
        monthly_pairs: dict[str, List[Tuple[str, str]]] = {}
        for code, date in raw_pairs:
            ym = date[:7]  # YYYY-MM
            monthly_pairs.setdefault(ym, []).append((code, date))

        # 对于每个月份，读取已有 _qmt 文件的 (ts_code, trade_date) 集合，避免重复下载
        for ym in list(monthly_pairs.keys()):
            qmt_parquet_path = os.path.join(MONTHLY_DIR, f"minute_data_{ym}_qmt.parquet")
            existing_keys = set()
            if os.path.exists(qmt_parquet_path):
                try:
                    existing_df = pd.read_parquet(qmt_parquet_path, columns=['ts_code', 'trade_date'])
                    existing_df['ts_code'] = existing_df['ts_code'].astype(str)
                    existing_df['trade_date'] = existing_df['trade_date'].astype(str)
                    existing_keys = set(zip(existing_df['ts_code'], existing_df['trade_date']))
                except Exception as e:
                    logger.warning(f"读取已有 _qmt 文件失败 {qmt_parquet_path}: {e}")
            # 过滤掉已存在的
            remaining = [(c, d) for (c, d) in monthly_pairs[ym] if (c, d) not in existing_keys]
            if remaining:
                monthly_pairs[ym] = remaining
            else:
                del monthly_pairs[ym]

        if not monthly_pairs:
            logger.info("所有数据在 _qmt 文件中均已存在，无需下载")
            return

        # 4. 逐月下载并合并
        for ym, pairs in monthly_pairs.items():
            logger.info(f"开始处理月份 {ym}，共 {len(pairs)} 条待下载记录")
            new_dfs = []
            for idx, (code, date) in enumerate(pairs):
                logger.debug(f"下载 {code} {date}")
                df_min = MarketData._fetch_miniqmt_intraday(code, date)
                if not df_min.empty:
                    df_min['ts_code'] = code
                    df_min['trade_date'] = date
                    # 确保列顺序与原有 parquet 一致
                    df_min = df_min[['时间', '开盘', '收盘', '最高', '最低', '成交量', 'ts_code', 'trade_date']]
                    new_dfs.append(df_min)
                # miniQMT 本地调用无需 sleep，但为避免过于频繁可加微小延时
                # time_module.sleep(0.05)
            if not new_dfs:
                logger.warning(f"月份 {ym} 无任何有效数据")
                continue

            combined = pd.concat(new_dfs, ignore_index=True)
            combined['ts_code'] = combined['ts_code'].astype(str).str.zfill(6)
            combined['trade_date'] = combined['trade_date'].astype(str)

            # 合并到已有的 _qmt 文件
            qmt_parquet_path = os.path.join(MONTHLY_DIR, f"minute_data_{ym}_qmt.parquet")
            if os.path.exists(qmt_parquet_path):
                try:
                    old_df = pd.read_parquet(qmt_parquet_path)
                    # 统一数据类型
                    old_df['ts_code'] = old_df['ts_code'].astype(str).str.zfill(6)
                    old_df['trade_date'] = old_df['trade_date'].astype(str)
                    combined = pd.concat([old_df, combined], ignore_index=True)
                except Exception as e:
                    logger.error(f"读取旧 _qmt 文件失败: {e}")

            combined.drop_duplicates(subset=['时间', 'ts_code', 'trade_date'], inplace=True)
            combined.sort_values(['ts_code', '时间'], inplace=True)
            combined.to_parquet(qmt_parquet_path, index=False, engine='pyarrow')
            logger.info(f"已生成 {qmt_parquet_path}，共 {len(combined)} 行")

        logger.info("miniQMT 数据预加载完成")

# ========================= 回测主函数 =========================
def run_download():
    #MarketData.preload_from_models(START_DATE, END_DATE)
    MarketData.preload_from_miniqmt(START_DATE, END_DATE)
    logger.info("Market数据下载完成，退出")
    return

