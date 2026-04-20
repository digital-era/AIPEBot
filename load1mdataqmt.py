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
from typing import Dict, List, Tuple, Set, Optional, Tuple, Any

# ========================= 日志初始化 (提前，避免未定义错误) =========================
logger = logging.getLogger("SIRIUS_Simulator")
if logger.handlers:
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)

logger.setLevel(logging.DEBUG)
logger.propagate = False

formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
ch = logging.StreamHandler()
ch.setFormatter(formatter)
logger.addHandler(ch)

# 尝试导入 miniQMT 接口
try:
    from xtquant import xtdata
    MINIQMT_AVAILABLE = True
except ImportError:
    MINIQMT_AVAILABLE = False
    logger.warning("miniQMT 未安装或无法导入，preload_from_miniqmt 将不可用")

# ========================= 1. 配置 =========================
START_DATE = "2026-01-29"
END_DATE = "2026-02-28"

MODEL_HISTORY_DIR = "./historical_models"      # 统一变量名
MONTHLY_DIR = "./monthly_data"
MODEL_NAME_PREFIX = "流入模型"
DATA_CACHE_DIR = "./min_data_cache"

# 代理配置
HTTP_PROXY = 'http://127.0.0.1:7890'
HTTPS_PROXY = 'http://127.0.0.1:7890'
PROXIES = {}
if HTTP_PROXY:
    PROXIES['http'] = HTTP_PROXY
if HTTPS_PROXY:
    PROXIES['https'] = HTTPS_PROXY

# 创建目录
for d in [MODEL_HISTORY_DIR, MONTHLY_DIR, DATA_CACHE_DIR]:
    os.makedirs(d, exist_ok=True)

# ========================= 2. 模型下载模块 =========================
MODEL_API_BASE_URL = "https://raw.githubusercontent.com/digital-era/AIPEQModel/main/流入模型_"
MODEL_REQUEST_RETRIES = 3
MODEL_REQUEST_TIMEOUT = 30
MODEL_REQUEST_INTERVAL = 0.5

class ModelDownloader:
    @staticmethod
    def _build_model_url(date_str: str) -> str:
        return f"{MODEL_API_BASE_URL}{date_str}.json"

    @staticmethod
    def _fetch_with_retry(url: str, retries: int = MODEL_REQUEST_RETRIES, timeout: int = MODEL_REQUEST_TIMEOUT) -> Optional[Dict]:
            for attempt in range(1, retries + 1):
                try:
                    resp = requests.get(url, timeout=timeout, headers={'User-Agent': 'SIRIUS-Bot/1.0'}, proxies=PROXIES)
                    if resp.status_code == 200:
                        return resp.json()
                    else:
                        logger.warning(f"HTTP {resp.status_code} from {url}, attempt {attempt}/{retries}")
                except Exception as e:
                    logger.warning(f"Request failed: {e}, attempt {attempt}/{retries}")
                if attempt < retries:
                    time_module.sleep(2 ** attempt)
            return None

    @staticmethod
    def download_model_for_date(date_str: str, force: bool = False) -> bool:
        filename = f"{MODEL_NAME_PREFIX}_{date_str}.json"
        filepath = os.path.join(MODEL_HISTORY_DIR, filename)
        if not force and os.path.exists(filepath):
            logger.debug(f"模型文件已存在，跳过: {filepath}")
            return True

        url = ModelDownloader._build_model_url(date_str)
        logger.info(f"下载模型: {date_str} -> {url}")
        data = ModelDownloader._fetch_with_retry(url)
        if data is None:
            logger.error(f"下载失败: {date_str}")
            return False

        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            logger.info(f"模型已保存: {filepath}")
            return True
        except Exception as e:
            logger.error(f"保存文件失败 {filepath}: {e}")
            return False

    @staticmethod
    def download_models_for_date_range(start_date: str, end_date: str, force: bool = False) -> List[str]:
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")
        current_dt = start_dt
        success_dates = []

        logger.info(f"开始批量下载模型，范围 {start_date} 至 {end_date}")
        while current_dt <= end_dt:
            date_str = current_dt.strftime("%Y-%m-%d")
            if ModelDownloader.download_model_for_date(date_str, force=force):
                success_dates.append(date_str)
            time_module.sleep(MODEL_REQUEST_INTERVAL)
            current_dt += timedelta(days=1)

        logger.info(f"批量下载完成，成功 {len(success_dates)} 天")
        return success_dates

# ========================= 3. 数据模块 =========================
class MarketData:
    @staticmethod
    def _get_current_cn_date() -> str:
        tz_cn = timezone(timedelta(hours=8))
        return datetime.now(tz_cn).strftime('%Y-%m-%d')

    @staticmethod
    def get_monthly_file_path(year_month: str, qmt_suffix: bool = False) -> str:
        if qmt_suffix:
            return os.path.join(MONTHLY_DIR, f"minute_data_{year_month}_qmt.parquet")
        else:
            return os.path.join(MONTHLY_DIR, f"minute_data_{year_month}.parquet")
    
    @staticmethod
    def get_daily_file_path(qmt_suffix: bool = False) -> str:
        if qmt_suffix:
            return os.path.join(MONTHLY_DIR, "daily_data_qmt.parquet")
        else:
            return os.path.join(MONTHLY_DIR, "daily_data.parquet")

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
        # 保持原样，不做转换（如需市场前缀可在此扩展）
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
                weight_str = item.get('最优权重(%)', '0')
                weight = float(weight_str.replace('%', '')) / 100
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
    def merge_monthly_data(year_month: str, qmt_suffix: bool = False):
        """
        将缓存目录中该月份的 CSV 文件合并到对应的 Parquet 文件中
        """
        p_path = MarketData.get_monthly_file_path(year_month, qmt_suffix)
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

            # 删除已合并的 CSV 文件
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
            cache_file = os.path.join(DATA_CACHE_DIR, f"{ts_code}_{date_clean}.csv")
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

    # ==================== miniQMT 相关方法（修复缩进后移至类内平级） ====================
    @staticmethod
    def _to_miniqmt_code(code: str) -> str:
        """将6位数字代码转换为 miniQMT 格式（带市场后缀）"""
        code = str(code).zfill(6)
        if code.startswith('6'):
            return f"{code}.SH"
        elif code.startswith('0') or code.startswith('3'):
            return f"{code}.SZ"
        else:
            return f"{code}.SZ"

    @staticmethod
    def _fetch_miniqmt_intraday(code: str, date_str: str) -> pd.DataFrame:
        """最终稳定版：miniQMT 获取1分钟数据"""

        if not MINIQMT_AVAILABLE:
            raise RuntimeError("miniQMT 不可用")

        miniqmt_code = MarketData._to_miniqmt_code(code)

        # ✅ 使用完整时间范围（关键）
        start_time = date_str.replace('-', '') + "000000"
        end_time = date_str.replace('-', '') + "235959"

        try:
            # =========================
            # Step1: 下载数据到本地
            # =========================
            xtdata.download_history_data(
                miniqmt_code,
                period='1m',
                start_time=start_time,
                end_time=end_time
            )

            # =========================
            # Step2: 获取数据（兼容新旧接口）
            # =========================
            try:
                data = xtdata.get_market_data_ex(
                    field_list=['time', 'open', 'close', 'high', 'low', 'volume'],
                    stock_list=[miniqmt_code],
                    period='1m',
                    start_time=start_time,
                    end_time=end_time
                )
            except Exception:
                data = xtdata.get_market_data(
                    field_list=['time', 'open', 'close', 'high', 'low', 'volume'],
                    stock_list=[miniqmt_code],
                    period='1m',
                    start_time=start_time,
                    end_time=end_time
                )

            # =========================
            # Step3: 统一结构
            # =========================
            df = None

            if isinstance(data, dict):
                df = data.get(miniqmt_code)
            elif isinstance(data, pd.DataFrame):
                df = data

            if df is None or len(df) == 0:
                logger.warning(f"miniQMT 空数据: {code} {date_str}")
                return pd.DataFrame()

            df = pd.DataFrame(df)

            # =========================
            # Step4: 字段标准化
            # =========================
            df = df.rename(columns={
                'time': '时间',
                'open': '开盘',
                'close': '收盘',
                'high': '最高',
                'low': '最低',
                'volume': '成交量',
                'vol': '成交量'
            })

            required_cols = ['时间', '开盘', '收盘', '最高', '最低', '成交量']

            for col in required_cols:
                if col not in df.columns:
                    logger.warning(f"字段缺失 {col}: {code} {date_str}")
                    return pd.DataFrame()

            # =========================
            # Step5: 时间解析（核心修复）
            # =========================
            raw_time = df['时间']

            try:
                if raw_time.dtype in ['int64', 'float64']:
                    length = raw_time.astype(str).str.len().iloc[0]

                    if length >= 13:
                        df['时间'] = pd.to_datetime(raw_time, unit='ms', errors='coerce')
                    else:
                        df['时间'] = pd.to_datetime(raw_time, unit='s', errors='coerce')
                else:
                    df['时间'] = pd.to_datetime(raw_time, errors='coerce')
            except Exception as e:
                logger.warning(f"时间解析异常 {code} {date_str}: {e}")
                return pd.DataFrame()
            
            # ⭐ 核心修复（加这一行）
            df['时间'] = df['时间'] + pd.Timedelta(hours=8)
            df = df.dropna(subset=['时间'])

            if df.empty:
                logger.warning(f"时间解析后为空: {code} {date_str}")
                return pd.DataFrame()

            # =========================
            # Step6: 调试（关键定位）
            # =========================
            logger.debug(
                f"{code} 时间范围: {df['时间'].min()} ~ {df['时间'].max()} 行数:{len(df)}"
            )

            # =========================
            # Step7: 交易时间过滤（宽松版，避免误杀）
            # =========================
            df = df[
                (df['时间'].dt.hour >= 9) &
                (df['时间'].dt.hour <= 15)
            ]

            if df.empty:
                logger.warning(f"过滤后为空: {code} {date_str}")
                return pd.DataFrame()

            # =========================
            # Step8: 排序输出
            # =========================
            df = df.sort_values('时间').reset_index(drop=True)

            return df[required_cols]

        except Exception as e:
            logger.error(f"miniQMT 获取失败 {code} {date_str}: {e}")
            return pd.DataFrame()

    @staticmethod
    def _fetch_miniqmt_daily(code: str, date_str: str) -> pd.DataFrame:
        """miniQMT 获取日线数据（完全对齐分钟结构）"""
    
        if not MINIQMT_AVAILABLE:
            raise RuntimeError("miniQMT 不可用")
    
        miniqmt_code = MarketData._to_miniqmt_code(code)
    
        start_time = date_str.replace('-', '') + "000000"
        end_time = date_str.replace('-', '') + "235959"
    
        try:
            # 下载
            xtdata.download_history_data(
                miniqmt_code,
                period='1d',
                start_time=start_time,
                end_time=end_time
            )
    
            # 获取
            try:
                data = xtdata.get_market_data_ex(
                    field_list=['time', 'open', 'close', 'high', 'low', 'volume'],
                    stock_list=[miniqmt_code],
                    period='1d',
                    start_time=start_time,
                    end_time=end_time
                )
            except Exception:
                data = xtdata.get_market_data(
                    field_list=['time', 'open', 'close', 'high', 'low', 'volume'],
                    stock_list=[miniqmt_code],
                    period='1d',
                    start_time=start_time,
                    end_time=end_time
                )
    
            df = None
            if isinstance(data, dict):
                df = data.get(miniqmt_code)
            elif isinstance(data, pd.DataFrame):
                df = data
    
            if df is None or len(df) == 0:
                logger.warning(f"日线空数据: {code} {date_str}")
                return pd.DataFrame()
    
            df = pd.DataFrame(df)
    
            # 字段统一
            df = df.rename(columns={
                'time': '时间',
                'open': '开盘',
                'close': '收盘',
                'high': '最高',
                'low': '最低',
                'volume': '成交量'
            })
    
            # 时间处理（关键：转中国时间 + 归一到日期）
            raw_time = df['时间']
            try:
                if raw_time.dtype in ['int64', 'float64']:
                    length = raw_time.astype(str).str.len().iloc[0]
            
                    if length >= 13:
                        df['时间'] = pd.to_datetime(raw_time, unit='ms', errors='coerce')
                    else:
                        df['时间'] = pd.to_datetime(raw_time, unit='s', errors='coerce')
                else:
                    df['时间'] = pd.to_datetime(raw_time, errors='coerce')
            except Exception as e:
                logger.warning(f"日线时间解析异常 {code} {date_str}: {e}")
                return pd.DataFrame()
    
            # ⭐ 同样加8小时（与你分钟数据保持一致）
            df['时间'] = df['时间'] + pd.Timedelta(hours=8)
    
            # ⭐ 归一为日期（核心区别）
            df['时间'] = df['时间'].dt.normalize()
    
            df = df.dropna(subset=['时间'])
    
            required_cols = ['时间', '开盘', '收盘', '最高', '最低', '成交量']
            for col in required_cols:
                if col not in df.columns:
                    return pd.DataFrame()
    
            return df[required_cols].sort_values('时间')
    
        except Exception as e:
            logger.error(f"miniQMT 日线失败 {code} {date_str}: {e}")
            return pd.DataFrame()

    @staticmethod
    def preload_from_miniqmt(start_date: str, end_date: str):
        """通过 miniQMT 下载分钟数据，生成带 _qmt 后缀的月度 parquet 文件"""
        if not MINIQMT_AVAILABLE:
            logger.error("miniQMT 未就绪，无法执行 preload_from_miniqmt")
            return

        dates = MarketData.get_model_dates(start_date, end_date)
        if not dates:
            logger.warning("未找到任何模型文件")
            return
        date_map = MarketData.build_date_map(dates)
        today_str = MarketData._get_current_cn_date()

        # 收集所有需要下载的 (股票代码, 日期)
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

        # 按月份分组，并过滤已存在的数据
        monthly_pairs: dict[str, List[Tuple[str, str]]] = {}
        for code, date in raw_pairs:
            ym = date[:7]
            monthly_pairs.setdefault(ym, []).append((code, date))

        for ym in list(monthly_pairs.keys()):
            qmt_parquet_path = MarketData.get_monthly_file_path(ym, qmt_suffix=True)
            existing_keys = set()
            if os.path.exists(qmt_parquet_path):
                try:
                    existing_df = pd.read_parquet(qmt_parquet_path, columns=['ts_code', 'trade_date'])
                    existing_df['ts_code'] = existing_df['ts_code'].astype(str)
                    existing_df['trade_date'] = existing_df['trade_date'].astype(str)
                    existing_keys = set(zip(existing_df['ts_code'], existing_df['trade_date']))
                except Exception as e:
                    logger.warning(f"读取已有 _qmt 文件失败 {qmt_parquet_path}: {e}")
            remaining = [(c, d) for (c, d) in monthly_pairs[ym] if (c, d) not in existing_keys]
            if remaining:
                monthly_pairs[ym] = remaining
            else:
                del monthly_pairs[ym]

        if not monthly_pairs:
            logger.info("所有数据在 _qmt 文件中均已存在，无需下载")
            return

        # 逐月下载并合并
        for ym, pairs in monthly_pairs.items():
            logger.info(f"开始处理月份 {ym}，共 {len(pairs)} 条待下载记录")
            new_dfs = []
            for idx, (code, date) in enumerate(pairs):
                logger.debug(f"下载 {code} {date}")
                df_min = MarketData._fetch_miniqmt_intraday(code, date)
                if not df_min.empty:
                    df_min['ts_code'] = code
                    df_min['trade_date'] = date
                    df_min = df_min[['时间', '开盘', '收盘', '最高', '最低', '成交量', 'ts_code', 'trade_date']]
                    new_dfs.append(df_min)
            if not new_dfs:
                logger.warning(f"月份 {ym} 无任何有效数据")
                continue

            combined = pd.concat(new_dfs, ignore_index=True)
            combined['ts_code'] = combined['ts_code'].astype(str).str.zfill(6)
            combined['trade_date'] = combined['trade_date'].astype(str)

            qmt_parquet_path = MarketData.get_monthly_file_path(ym, qmt_suffix=True)
            if os.path.exists(qmt_parquet_path):
                try:
                    old_df = pd.read_parquet(qmt_parquet_path)
                    old_df['ts_code'] = old_df['ts_code'].astype(str).str.zfill(6)
                    old_df['trade_date'] = old_df['trade_date'].astype(str)
                    combined = pd.concat([old_df, combined], ignore_index=True)
                except Exception as e:
                    logger.error(f"读取旧 _qmt 文件失败: {e}")

            combined.drop_duplicates(subset=['时间', 'ts_code', 'trade_date'], inplace=True)
            combined.sort_values(['ts_code', '时间'], inplace=True)
            combined.to_parquet(qmt_parquet_path, index=False, engine='pyarrow')
            logger.info(f"已生成 {qmt_parquet_path}，共 {len(combined)} 行")
        
        if not df_min.empty:
            logger.debug(f"成功获取 {code} {date} 行数: {len(df_min)}")
        else:
            logger.debug(f"失败/空数据 {code} {date}")
            
        logger.info("miniQMT 数据预加载完成")


    @staticmethod
    def preload_daily_from_miniqmt(start_date: str, end_date: str):
        """预加载日线数据（单文件模式 + 增量更新）"""
    
        if not MINIQMT_AVAILABLE:
            logger.error("miniQMT 未就绪")
            return
    
        dates = MarketData.get_model_dates(start_date, end_date)
        if not dates:
            logger.warning("未找到模型日期")
            return
    
        date_map = MarketData.build_date_map(dates)
        today_str = MarketData._get_current_cn_date()
    
        # =========================
        # Step1: 收集下载任务
        # =========================
        raw_pairs = set()
    
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
    
        if not raw_pairs:
            logger.warning("无日线任务")
            return
    
        logger.info(f"[日线] 原始任务数: {len(raw_pairs)}")
    
        # =========================
        # Step2: 过滤已有数据（核心优化）
        # =========================
        p_path = MarketData.get_daily_file_path(qmt_suffix=True)
    
        existing_keys = set()
    
        if os.path.exists(p_path):
            try:
                old_df = pd.read_parquet(p_path, columns=['ts_code', 'trade_date'])
                old_df['ts_code'] = old_df['ts_code'].astype(str)
                old_df['trade_date'] = old_df['trade_date'].astype(str)
                existing_keys = set(zip(old_df['ts_code'], old_df['trade_date']))
            except Exception as e:
                logger.warning(f"读取已有日线失败: {e}")
    
        # 过滤掉已存在的
        raw_pairs = [p for p in raw_pairs if p not in existing_keys]
    
        if not raw_pairs:
            logger.info("[日线] 数据已全部存在，无需下载")
            return
    
        logger.info(f"[日线] 实际下载任务数: {len(raw_pairs)}")
    
        # =========================
        # Step3: 下载数据
        # =========================
        new_dfs = []
    
        for idx, (code, date) in enumerate(raw_pairs):
            logger.debug(f"[日线] 下载 {idx+1}/{len(raw_pairs)}: {code} {date}")
    
            df = MarketData._fetch_miniqmt_daily(code, date)
    
            if not df.empty:
                df['ts_code'] = str(code).zfill(6)
                df['trade_date'] = str(date)
                df = df[['时间', '开盘', '收盘', '最高', '最低', '成交量', 'ts_code', 'trade_date']]
                new_dfs.append(df)
    
        if not new_dfs:
            logger.warning("[日线] 下载结果为空")
            return
    
        combined = pd.concat(new_dfs, ignore_index=True)
    
        # =========================
        # Step4: 合并旧数据
        # =========================
        if os.path.exists(p_path):
            try:
                old_df = pd.read_parquet(p_path)
                old_df['ts_code'] = old_df['ts_code'].astype(str).str.zfill(6)
                old_df['trade_date'] = old_df['trade_date'].astype(str)
    
                combined = pd.concat([old_df, combined], ignore_index=True)
            except Exception as e:
                logger.warning(f"合并旧数据失败: {e}")
    
        # =========================
        # Step5: 去重 + 排序
        # =========================
        combined.drop_duplicates(subset=['时间', 'ts_code', 'trade_date'], inplace=True)
        combined.sort_values(['ts_code', '时间'], inplace=True)
    
        # =========================
        # Step6: 保存
        # =========================
        combined.to_parquet(p_path, index=False, engine='pyarrow')
    
        logger.info(f"[日线] 更新完成: {p_path} 总行数:{len(combined)}")

# ========================= 回测主函数 =========================
def run_download():
    # 先下载模型文件
    #ModelDownloader.download_models_for_date_range(START_DATE, END_DATE, force=False)
    #logger.info("模型数据下载完成，退出")
    
    # 如果 miniQMT 可用，通过它预加载1分钟数据
    if MINIQMT_AVAILABLE:
        MarketData.preload_from_miniqmt(START_DATE, END_DATE)
        MarketData.preload_daily_from_miniqmt(START_DATE, END_DATE)   # ✅新增
    logger.info("Market数据下载完成，退出")

if __name__ == "__main__":
    run_download()
