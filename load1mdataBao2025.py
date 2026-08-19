# @title Preload Minute Data from Baostock (5min) + GitHub HTTP
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
import subprocess
from typing import Dict, List, Tuple, Set, Optional, Any

# ========================= 安装与导入 Baostock =========================
try:
    import baostock as bs
    BAOSTOCK_AVAILABLE = True
except ImportError:
    BAOSTOCK_AVAILABLE = False
    print("baostock 未安装，正在自动安装...")
    subprocess.check_call(['pip', 'install', 'baostock', '-q'])
    import baostock as bs
    BAOSTOCK_AVAILABLE = True

# ========================= 日志初始化 =========================
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

# ========================= 1. 配置 =========================
START_DATE = "2025-01-07"
END_DATE = "2025-09-15"

MODEL_HISTORY_DIR = "/content/AIPEQModel2025"
MONTHLY_DIR = "./monthly_data"
DATA_CACHE_DIR = "./min_data_cache"

MODEL_NAME_PREFIX = "流入模型"

# ========================= GitHub 配置（HTTP + PAT） =========================
GIT_USERNAME = "digital-era"
GIT_EMAIL = "digital_era@sina.com"
GIT_REPO_NAME = "AIPEQModel2025"
GIT_TARGET_BRANCH = "main"
GIT_REPO_URL = f"https://github.com/{GIT_USERNAME}/{GIT_REPO_NAME}.git"
LOCAL_GIT_WORKSPACE = "/content/AIPEQModel2025"

# 从环境变量读取 Personal Access Token（必须在运行前设置）
GIT_TOKEN = ""

if not GIT_TOKEN:
    logger.warning("⚠️ 环境变量 GIT_TOKEN 未设置，GitHub 推送可能失败！")
    logger.warning("请在 Colab 中运行: os.environ['GIT_TOKEN'] = '你的token'")

# 创建目录
for d in [MODEL_HISTORY_DIR, MONTHLY_DIR, DATA_CACHE_DIR]:
    os.makedirs(d, exist_ok=True)

# ========================= 2. 模型下载模块（不变） =========================
MODEL_API_BASE_URL = f"https://raw.githubusercontent.com/digital-era/AIPEQModel2025/main/{MODEL_NAME_PREFIX}_"
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
                resp = requests.get(url, timeout=timeout, headers={'User-Agent': 'SIRIUS-Bot/1.0'})
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

# ========================= 3. 数据模块（Baostock 实现） =========================
class MarketData:
    @staticmethod
    def _get_current_cn_date() -> str:
        tz_cn = timezone(timedelta(hours=8))
        return datetime.now(tz_cn).strftime('%Y-%m-%d')

    @staticmethod
    def get_monthly_file_path(year_month: str, suffix: bool = False) -> str:
        if suffix:
            return os.path.join(MONTHLY_DIR, f"minute_data_{year_month}_baostock.parquet")
        else:
            return os.path.join(MONTHLY_DIR, f"minute_data_{year_month}.parquet")
    
    @staticmethod
    def get_daily_file_path(suffix: bool = False) -> str:
        if suffix:
            return os.path.join(MONTHLY_DIR, "daily_data_baostock.parquet")
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
        logger.info(f"[模型日期] 找到 {len(dates)} 个匹配日期 (范围 {start_date}~{end_date})")
        if dates:
            logger.debug(f"[模型日期] 前5个: {dates[:5]}")
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
    def _to_baostock_code(code: str) -> str:
        code = str(code).zfill(6)
        if code.startswith('6'):
            return f"sh.{code}"
        else:
            return f"sz.{code}"

    @staticmethod
    def _fetch_baostock_intraday(code: str, date_str: str, retry: bool = False) -> pd.DataFrame:
        """使用 Baostock 获取 5 分钟 K 线，不包含登录/登出，支持重试"""
        if not BAOSTOCK_AVAILABLE:
            return pd.DataFrame()
        bs_code = MarketData._to_baostock_code(code)
        try:
            rs = bs.query_history_k_data_plus(
                bs_code,
                "date,time,open,high,low,close,volume",
                start_date=date_str,
                end_date=date_str,
                frequency="5",
                adjustflag="3"
            )
            if rs.error_code != '0':
                logger.warning(f"Baostock 查询失败 {bs_code} {date_str}: {rs.error_msg}")
                return pd.DataFrame()
            data_list = rs.get_data()
            if len(data_list) == 0:
                if not retry:
                    logger.warning(f"Baostock 返回空数据 {bs_code} {date_str}，1秒后重试...")
                    time_module.sleep(1)
                    return MarketData._fetch_baostock_intraday(code, date_str, retry=True)
                else:
                    logger.warning(f"Baostock 重试后仍返回空数据 {bs_code} {date_str}")
                    return pd.DataFrame()
            df = pd.DataFrame(data_list, columns=rs.fields)
            df = df[df['date'].notna() & df['time'].notna()]
            
            # 正确解析时间：time 字段为 YYYYMMDDHHMMSSsss（长度≥14）
            def build_datetime(row):
                try:
                    time_str = str(row['time']).strip()
                    if len(time_str) >= 14:
                        dt_str = time_str[:14]  # 取前14位：YYYYMMDDHHMMSS
                        return datetime.strptime(dt_str, "%Y%m%d%H%M%S")
                    else:
                        # 兼容其他格式（如 HHMMSS）
                        return datetime.strptime(f"{row['date']} {time_str.zfill(6)}", "%Y-%m-%d %H%M%S")
                except Exception as e:
                    logger.debug(f"时间解析失败: {row['date']} {row['time']} - {e}")
                    return pd.NaT
            df['时间'] = df.apply(build_datetime, axis=1)
            df = df.dropna(subset=['时间'])
            df = df[(df['时间'].dt.hour >= 9) & (df['时间'].dt.hour <= 15)]
            df.rename(columns={
                'open': '开盘',
                'high': '最高',
                'low': '最低',
                'close': '收盘',
                'volume': '成交量'
            }, inplace=True)
            df = df[['时间', '开盘', '收盘', '最高', '最低', '成交量']]
            df['开盘'] = df['开盘'].astype(float)
            df['收盘'] = df['收盘'].astype(float)
            df['最高'] = df['最高'].astype(float)
            df['最低'] = df['最低'].astype(float)
            df['成交量'] = df['成交量'].astype(float)
            return df.sort_values('时间').reset_index(drop=True)
        except Exception as e:
            logger.error(f"Baostock 分钟数据失败 {code} {date_str}: {e}")
            return pd.DataFrame()

    @staticmethod
    def _fetch_baostock_daily(code: str, date_str: str, retry: bool = False) -> pd.DataFrame:
        """使用 Baostock 获取日线数据，不包含登录/登出，支持重试"""
        if not BAOSTOCK_AVAILABLE:
            return pd.DataFrame()
        bs_code = MarketData._to_baostock_code(code)
        try:
            rs = bs.query_history_k_data_plus(
                bs_code,
                "date,open,high,low,close,volume",
                start_date=date_str,
                end_date=date_str,
                frequency="d",
                adjustflag="3"
            )
            if rs.error_code != '0':
                logger.warning(f"Baostock 日线查询失败 {bs_code} {date_str}: {rs.error_msg}")
                return pd.DataFrame()
            data_list = rs.get_data()
            if len(data_list) == 0:
                if not retry:
                    logger.warning(f"Baostock 返回空日线数据 {bs_code} {date_str}，1秒后重试...")
                    time_module.sleep(1)
                    return MarketData._fetch_baostock_daily(code, date_str, retry=True)
                else:
                    logger.warning(f"Baostock 重试后仍返回空日线数据 {bs_code} {date_str}")
                    return pd.DataFrame()
            df = pd.DataFrame(data_list, columns=rs.fields)
            df = df[df['date'].notna()]
            df['时间'] = pd.to_datetime(df['date'], errors='coerce')
            df = df.dropna(subset=['时间'])
            df.rename(columns={
                'open': '开盘',
                'high': '最高',
                'low': '最低',
                'close': '收盘',
                'volume': '成交量'
            }, inplace=True)
            df = df[['时间', '开盘', '收盘', '最高', '最低', '成交量']]
            df['开盘'] = df['开盘'].astype(float)
            df['收盘'] = df['收盘'].astype(float)
            df['最高'] = df['最高'].astype(float)
            df['最低'] = df['最低'].astype(float)
            df['成交量'] = df['成交量'].astype(float)
            return df.sort_values('时间').reset_index(drop=True)
        except Exception as e:
            logger.error(f"Baostock 日线失败 {code} {date_str}: {e}")
            return pd.DataFrame()

    # ===== 预加载分钟数据 =====
    @staticmethod
    def preload_from_baostock(start_date: str, end_date: str):
        if not BAOSTOCK_AVAILABLE:
            return

        # 登录一次
        lg = bs.login()
        if lg.error_code != '0':
            logger.error(f"Baostock 登录失败: {lg.error_msg}")
            return
        logger.info("Baostock 登录成功")

        try:
            # 测试查询：确认环境正常
            test_df = MarketData._fetch_baostock_intraday("600000", "2025-01-07")
            if test_df.empty:
                logger.error("测试查询失败，Baostock 可能无法正常返回数据，终止预加载")
                return
            else:
                logger.info(f"测试查询成功，获取 {len(test_df)} 条记录")

            dates = MarketData.get_model_dates(start_date, end_date)
            if not dates:
                logger.warning("[分钟] 没有找到任何模型日期，跳过数据获取")
                return

            date_map = MarketData.build_date_map(dates)
            today_str = MarketData._get_current_cn_date()
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

            logger.info(f"[分钟] 共收集到 {len(raw_pairs)} 个 (股票, 日期) 对")

            monthly_pairs = {}
            for code, date in raw_pairs:
                ym = date[:7]
                monthly_pairs.setdefault(ym, []).append((code, date))

            # 去重（已缓存）
            for ym in list(monthly_pairs.keys()):
                baostock_parquet_path = MarketData.get_monthly_file_path(ym, suffix=True)
                existing_keys = set()
                if os.path.exists(baostock_parquet_path):
                    try:
                        existing_df = pd.read_parquet(baostock_parquet_path, columns=['ts_code', 'trade_date'])
                        existing_keys = set(zip(existing_df['ts_code'].astype(str), existing_df['trade_date'].astype(str)))
                    except:
                        pass
                remaining = [(c, d) for (c, d) in monthly_pairs[ym] if (c, d) not in existing_keys]
                if remaining:
                    monthly_pairs[ym] = remaining
                else:
                    del monthly_pairs[ym]

            if not monthly_pairs:
                logger.info("[分钟] 所有数据已在缓存中，无需下载")
                return

            for ym, pairs in monthly_pairs.items():
                logger.info(f"[分钟] 开始处理月份 {ym}，共 {len(pairs)} 个 (股票,日期)")
                new_dfs = []
                total = len(pairs)
                for idx, (code, date) in enumerate(pairs, 1):
                    logger.info(f"[分钟] 拉取 {code} {date} ({idx}/{total})")
                    df_min = MarketData._fetch_baostock_intraday(code, date)
                    if not df_min.empty:
                        logger.info(f"[分钟] 成功获取 {len(df_min)} 条记录")
                        df_min['ts_code'] = code
                        df_min['trade_date'] = date
                        new_dfs.append(df_min)
                    else:
                        logger.warning(f"[分钟] 未获取到数据 {code} {date}")
                    time_module.sleep(1.0)   # 增加延迟至1秒

                if not new_dfs:
                    continue

                combined = pd.concat(new_dfs, ignore_index=True)
                combined['ts_code'] = combined['ts_code'].astype(str).str.zfill(6)
                combined['trade_date'] = combined['trade_date'].astype(str)

                baostock_parquet_path = MarketData.get_monthly_file_path(ym, suffix=True)
                if os.path.exists(baostock_parquet_path):
                    try:
                        old_df = pd.read_parquet(baostock_parquet_path)
                        combined = pd.concat([old_df, combined], ignore_index=True)
                    except:
                        pass
                combined.drop_duplicates(subset=['时间', 'ts_code', 'trade_date'], inplace=True)
                combined.sort_values(['ts_code', '时间'], inplace=True)
                combined.to_parquet(baostock_parquet_path, index=False, engine='pyarrow')
                logger.info(f"[分钟] 已保存 {ym} 数据到 {baostock_parquet_path}，共 {len(combined)} 条")

        finally:
            bs.logout()
            logger.info("[分钟] Baostock 登出")

    # ===== 预加载日线数据 =====
    @staticmethod
    def preload_daily_from_baostock(start_date: str, end_date: str):
        if not BAOSTOCK_AVAILABLE:
            return

        lg = bs.login()
        if lg.error_code != '0':
            logger.error(f"Baostock 登录失败: {lg.error_msg}")
            return
        logger.info("Baostock 登录成功")

        try:
            # 测试查询
            test_df = MarketData._fetch_baostock_daily("600000", "2025-01-07")
            if test_df.empty:
                logger.error("测试日线查询失败，终止预加载")
                return
            else:
                logger.info(f"测试日线查询成功，获取 {len(test_df)} 条记录")

            dates = MarketData.get_model_dates(start_date, end_date)
            if not dates:
                logger.warning("[日线] 没有找到任何模型日期，跳过数据获取")
                return

            date_map = MarketData.build_date_map(dates)
            today_str = MarketData._get_current_cn_date()
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

            logger.info(f"[日线] 共收集到 {len(raw_pairs)} 个 (股票, 日期) 对")

            p_path = MarketData.get_daily_file_path(suffix=True)
            existing_keys = set()
            if os.path.exists(p_path):
                try:
                    old_df = pd.read_parquet(p_path, columns=['ts_code', 'trade_date'])
                    existing_keys = set(zip(old_df['ts_code'].astype(str), old_df['trade_date'].astype(str)))
                    logger.info(f"[日线] 已有缓存 {len(existing_keys)} 条记录")
                except:
                    pass

            raw_pairs = [p for p in raw_pairs if p not in existing_keys]
            if not raw_pairs:
                logger.info("[日线] 所有数据已在缓存中")
                return

            logger.info(f"[日线] 需要下载 {len(raw_pairs)} 个 (股票,日期)")
            new_dfs = []
            total = len(raw_pairs)
            for idx, (code, date) in enumerate(raw_pairs, 1):
                logger.info(f"[日线] 拉取 {code} {date} ({idx}/{total})")
                df = MarketData._fetch_baostock_daily(code, date)
                if not df.empty:
                    logger.info(f"[日线] 成功获取 {len(df)} 条记录")
                    df['ts_code'] = str(code).zfill(6)
                    df['trade_date'] = str(date)
                    new_dfs.append(df)
                else:
                    logger.warning(f"[日线] 未获取到数据 {code} {date}")
                time_module.sleep(1.0)

            if not new_dfs:
                logger.warning("[日线] 未获取到任何有效数据")
                return

            combined = pd.concat(new_dfs, ignore_index=True)
            if os.path.exists(p_path):
                try:
                    old_df = pd.read_parquet(p_path)
                    combined = pd.concat([old_df, combined], ignore_index=True)
                    logger.info(f"[日线] 合并旧数据，旧数据行数 {len(old_df)}")
                except:
                    pass
            combined.drop_duplicates(subset=['时间', 'ts_code', 'trade_date'], inplace=True)
            combined.sort_values(['ts_code', '时间'], inplace=True)
            combined.to_parquet(p_path, index=False, engine='pyarrow')
            logger.info(f"[日线] 已保存到 {p_path}，共 {len(combined)} 条记录")

        finally:
            bs.logout()
            logger.info("[日线] Baostock 登出")

# ========================= 4. GitHub 上传模块（HTTP + PAT） =========================
class GitHubUploader:
    def __init__(self):
        if GIT_TOKEN:
            self.auth_repo_url = f"https://{GIT_USERNAME}:{GIT_TOKEN}@github.com/{GIT_USERNAME}/{GIT_REPO_NAME}.git"
            logger.info("GitHub 认证: 使用 Personal Access Token")
        else:
            self.auth_repo_url = GIT_REPO_URL
            logger.warning("GitHub 认证: 无 Token，推送可能失败")
        self.env = os.environ.copy()

    def run_cmd(self, cmd: str, cwd: str = None, timeout: int = 60) -> Tuple[bool, str]:
        try:
            result = subprocess.run(
                cmd, cwd=cwd, shell=True, env=self.env,
                check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
                timeout=timeout
            )
            return True, result.stdout
        except subprocess.TimeoutExpired:
            logger.error(f"Git 命令超时: {cmd} (>{timeout}秒)")
            return False, "Timeout"
        except subprocess.CalledProcessError as e:
            logger.error(f"Git 命令失败: {cmd}\n{e.stderr}")
            return False, e.stderr

    def _is_git_repo(self, path: str) -> bool:
        return os.path.exists(os.path.join(path, '.git'))

    def test_auth(self) -> bool:
        if not GIT_TOKEN:
            logger.warning("未设置 GIT_TOKEN，跳过认证测试")
            return False
        test_url = f"https://{GIT_USERNAME}:{GIT_TOKEN}@github.com/{GIT_USERNAME}/{GIT_REPO_NAME}.git"
        success, output = self.run_cmd(f"git ls-remote {test_url} HEAD", timeout=30)
        if success:
            logger.info("[Git] Token 认证成功")
            return True
        else:
            logger.error(f"[Git] Token 认证失败，请检查 Token 权限（需 repo 范围）: {output}")
            return False

    def init_workspace(self) -> bool:
        if os.path.exists(LOCAL_GIT_WORKSPACE) and not self._is_git_repo(LOCAL_GIT_WORKSPACE):
            logger.warning(f"[Git] 目录 {LOCAL_GIT_WORKSPACE} 存在但不是 Git 仓库，将删除并重新克隆")
            shutil.rmtree(LOCAL_GIT_WORKSPACE)

        if not os.path.exists(LOCAL_GIT_WORKSPACE):
            logger.info(f"[Git] 克隆仓库 (HTTP) ...")
            if GIT_TOKEN:
                if not self.test_auth():
                    logger.error("[Git] Token 认证失败，请检查 Token 是否正确且具有 repo 权限")
                    return False
            success, output = self.run_cmd(f"git clone --depth 1 {self.auth_repo_url} {LOCAL_GIT_WORKSPACE}")
            if not success:
                logger.error(f"[Git] 克隆失败: {output}")
                return False
            logger.info("[Git] 克隆成功")
        else:
            logger.info("[Git] 工作区已存在，更新远程地址并拉取最新代码...")
            self.run_cmd(f"git remote set-url origin {self.auth_repo_url}", cwd=LOCAL_GIT_WORKSPACE)
            self.run_cmd("git fetch origin", cwd=LOCAL_GIT_WORKSPACE)
            self.run_cmd(f"git reset --hard origin/{GIT_TARGET_BRANCH}", cwd=LOCAL_GIT_WORKSPACE)
            self.run_cmd("git clean -fd", cwd=LOCAL_GIT_WORKSPACE)
            self.run_cmd(f"git pull origin {GIT_TARGET_BRANCH}", cwd=LOCAL_GIT_WORKSPACE)

        self.run_cmd(f'git config user.name "{GIT_USERNAME}"', cwd=LOCAL_GIT_WORKSPACE)
        self.run_cmd(f'git config user.email "{GIT_EMAIL}"', cwd=LOCAL_GIT_WORKSPACE)
        return True

    def sync_files_to_workspace(self, src_backup_dir: str, target_repo_path: str):
        full_target_path = os.path.join(LOCAL_GIT_WORKSPACE, target_repo_path)
        os.makedirs(full_target_path, exist_ok=True)
        for file_name in os.listdir(src_backup_dir):
            src_file = os.path.join(src_backup_dir, file_name)
            if os.path.isfile(src_file):
                dst_file = os.path.join(full_target_path, file_name)
                try:
                    shutil.copy2(src_file, dst_file)
                    logger.debug(f"[Git] 同步: {dst_file}")
                except Exception as e:
                    logger.error(f"[Git] 拷贝失败 {src_file}: {e}")

    def commit_and_push(self) -> bool:
        self.run_cmd(f"git remote set-url origin {self.auth_repo_url}", cwd=LOCAL_GIT_WORKSPACE)
        self.run_cmd("git add .", cwd=LOCAL_GIT_WORKSPACE)
        success, stdout = self.run_cmd("git status --porcelain", cwd=LOCAL_GIT_WORKSPACE)
        if not stdout.strip():
            logger.info("[Git] 无变化，跳过提交")
            return True

        commit_msg = f"Auto-update model market data at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        logger.info("[Git] 提交更改...")
        success, _ = self.run_cmd(f'git commit -m "{commit_msg}"', cwd=LOCAL_GIT_WORKSPACE)
        if not success:
            logger.error("[Git] Commit 失败")
            return False

        logger.info(f"[Git] 推送到 {GIT_TARGET_BRANCH} ...")
        success, stderr = self.run_cmd(f"git push origin {GIT_TARGET_BRANCH}", cwd=LOCAL_GIT_WORKSPACE)
        if success:
            logger.info("[Git] 推送成功！")
        else:
            logger.error(f"[Git] Push 失败: {stderr}")
        return success

# ========================= 5. 主函数 =========================
def run_download():
    global MODEL_NAME_PREFIX, MODEL_API_BASE_URL
    
    models = [
        "流入模型",
        "大成模型",
        #"大智模型",
        #"低波稳健模型",
        #"高潜模型"
    ]
    
    backup_base_dir = r"/content/minute_backup_2025"
    
    for model_name in models:
        MODEL_NAME_PREFIX = model_name
        MODEL_API_BASE_URL = f"https://raw.githubusercontent.com/digital-era/AIPEQModel2025/main/{MODEL_NAME_PREFIX}_"
        
        logger.info(f"\n===================== 处理模型: {MODEL_NAME_PREFIX} =====================")
        
        # ✅ 启用模型下载（确保 JSON 文件存在）
        #ModelDownloader.download_models_for_date_range(START_DATE, END_DATE, force=False)
        
        if BAOSTOCK_AVAILABLE:
            MarketData.preload_from_baostock(START_DATE, END_DATE)
            ##先下载下面Daily
            ##MarketData.preload_daily_from_baostock(START_DATE, END_DATE)
            logger.info(f"[{MODEL_NAME_PREFIX}] 数据下载完成")
            
            target_dir = os.path.join(backup_base_dir, MODEL_NAME_PREFIX)
            os.makedirs(target_dir, exist_ok=True)
            
            if os.path.exists(MONTHLY_DIR):
                for file_name in os.listdir(MONTHLY_DIR):
                    src_file = os.path.join(MONTHLY_DIR, file_name)
                    if os.path.isfile(src_file):
                        dst_file = os.path.join(target_dir, file_name)
                        try:
                            shutil.copy2(src_file, dst_file)
                        except Exception as e:
                            logger.error(f"拷贝失败 {src_file}: {e}")
                logger.info(f"[{MODEL_NAME_PREFIX}] 数据已备份至: {target_dir}")
            else:
                logger.warning(f"目录 {MONTHLY_DIR} 不存在")
            
            # 清空临时目录
            if os.path.exists(MONTHLY_DIR):
                shutil.rmtree(MONTHLY_DIR)
                os.makedirs(MONTHLY_DIR, exist_ok=True)
                logger.info(f"[{MODEL_NAME_PREFIX}] 临时目录已清空")
        else:
            logger.warning("Baostock 不可用，跳过数据获取")

    logger.info("\n========== 所有模型数据处理完成 ==========")
    logger.info("\n========== 开始统一提交到 GitHub ==========")

    github_manager = GitHubUploader()
    if github_manager.init_workspace():
        for model_name in models:
            src_backup_dir = os.path.join(backup_base_dir, model_name)
            if os.path.exists(src_backup_dir):
                target_repo_path = f"minute/{model_name}"
                github_manager.sync_files_to_workspace(src_backup_dir, target_repo_path)
        github_manager.commit_and_push()
    else:
        logger.error("[Git] 工作区初始化失败，取消提交")

    logger.info("\n========== 任务结束 ==========")

if __name__ == "__main__":
    run_download()
