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
import subprocess  # ✅ 新增：用于执行系统git命令
from typing import Dict, List, Tuple, Set, Optional, Any

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

# 尝试导入 miniQMT 接口
try:
    from xtquant import xtdata
    MINIQMT_AVAILABLE = True
except ImportError:
    MINIQMT_AVAILABLE = False
    logger.warning("miniQMT 未安装或无法导入，preload_from_miniqmt 将不可用")

# ========================= 1. 配置 =========================
START_DATE = "2026-04-30"
END_DATE = "2026-05-12"

MODEL_HISTORY_DIR = "./Github_AIPEQModel_Workspace"
MONTHLY_DIR = "./monthly_data"
DATA_CACHE_DIR = "./min_data_cache"

MODEL_NAME_PREFIX = "流入模型"

# 代理配置
HTTP_PROXY = 'http://127.0.0.1:7897'
HTTPS_PROXY = 'http://127.0.0.1:7897'
PROXIES = {}
if HTTP_PROXY:
    PROXIES['http'] = HTTP_PROXY
if HTTPS_PROXY:
    PROXIES['https'] = HTTPS_PROXY

# ========================= 新增：GitHub 配置 =========================
GIT_USERNAME = "digital-era"
GIT_EMAIL = "digital_era@sina.com"
GIT_REPO_NAME = "AIPEQModel"
GIT_TARGET_BRANCH = "main"
# ✅ 修改为 SSH 地址，实现免密提交
GIT_REPO_URL = f"git@github.com:{GIT_USERNAME}/{GIT_REPO_NAME}.git"
LOCAL_GIT_WORKSPACE = "./Github_AIPEQModel_Workspace" # 临时克隆目录

# 创建目录
for d in [MODEL_HISTORY_DIR, MONTHLY_DIR, DATA_CACHE_DIR]:
    os.makedirs(d, exist_ok=True)

# ========================= 2. 模型下载模块 =========================
MODEL_API_BASE_URL = f"https://raw.githubusercontent.com/digital-era/AIPEQModel/main/{MODEL_NAME_PREFIX}_"
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
        success_dates =[]

        logger.info(f"开始批量下载模型，范围 {start_date} 至 {end_date}")
        while current_dt <= end_dt:
            date_str = current_dt.strftime("%Y-%m-%d")
            if ModelDownloader.download_model_for_date(date_str, force=force):
                success_dates.append(date_str)
            time_module.sleep(MODEL_REQUEST_INTERVAL)
            current_dt += timedelta(days=1)

        logger.info(f"批量下载完成，成功 {len(success_dates)} 天")
        return success_dates

# ========================= 3. 数据模块 (省略了中间内部实现以保持清晰，代码无更改) =========================
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
        return code

    @staticmethod
    def get_model_dates(start_date: str, end_date: str) -> list:
        pattern = os.path.join(MODEL_HISTORY_DIR, f"{MODEL_NAME_PREFIX}_*.json")
        dates =[]
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
            details = res.get('最优投资组合配置', {}).get('配置详情',[])
            risk_info = res.get('风控因子信息', {})
            pos_factor = float(risk_info.get('综合建议仓位因子', 1.0))
            targets =[]
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
            return[], 1.0

    @staticmethod
    def _to_miniqmt_code(code: str) -> str:
        code = str(code).zfill(6)
        if code.startswith('6'): return f"{code}.SH"
        else: return f"{code}.SZ"

    @staticmethod
    def _fetch_miniqmt_intraday(code: str, date_str: str) -> pd.DataFrame:
        if not MINIQMT_AVAILABLE: return pd.DataFrame()
        miniqmt_code = MarketData._to_miniqmt_code(code)
        start_time = date_str.replace('-', '') + "000000"
        end_time = date_str.replace('-', '') + "235959"
        try:
            xtdata.download_history_data(miniqmt_code, period='1m', start_time=start_time, end_time=end_time)
            try: data = xtdata.get_market_data_ex(field_list=['time', 'open', 'close', 'high', 'low', 'volume'], stock_list=[miniqmt_code], period='1m', start_time=start_time, end_time=end_time)
            except: data = xtdata.get_market_data(field_list=['time', 'open', 'close', 'high', 'low', 'volume'], stock_list=[miniqmt_code], period='1m', start_time=start_time, end_time=end_time)
            df = data.get(miniqmt_code) if isinstance(data, dict) else data
            if df is None or len(df) == 0: return pd.DataFrame()
            df = pd.DataFrame(df).rename(columns={'time': '时间', 'open': '开盘', 'close': '收盘', 'high': '最高', 'low': '最低', 'volume': '成交量', 'vol':'成交量'})
            raw_time = df['时间']
            if raw_time.dtype in ['int64', 'float64']:
                length = raw_time.astype(str).str.len().iloc[0]
                df['时间'] = pd.to_datetime(raw_time, unit='ms' if length >= 13 else 's', errors='coerce')
            else:
                df['时间'] = pd.to_datetime(raw_time, errors='coerce')
            df['时间'] = df['时间'] + pd.Timedelta(hours=8)
            df = df.dropna(subset=['时间'])
            df = df[(df['时间'].dt.hour >= 9) & (df['时间'].dt.hour <= 15)].sort_values('时间').reset_index(drop=True)
            return df[['时间', '开盘', '收盘', '最高', '最低', '成交量']]
        except Exception as e: return pd.DataFrame()

    @staticmethod
    def _fetch_miniqmt_daily(code: str, date_str: str) -> pd.DataFrame:
        if not MINIQMT_AVAILABLE: return pd.DataFrame()
        miniqmt_code = MarketData._to_miniqmt_code(code)
        start_time = date_str.replace('-', '') + "000000"
        end_time = date_str.replace('-', '') + "235959"
        try:
            xtdata.download_history_data(miniqmt_code, period='1d', start_time=start_time, end_time=end_time)
            try: data = xtdata.get_market_data_ex(field_list=['time', 'open', 'close', 'high', 'low', 'volume'], stock_list=[miniqmt_code], period='1d', start_time=start_time, end_time=end_time)
            except: data = xtdata.get_market_data(field_list=['time', 'open', 'close', 'high', 'low', 'volume'], stock_list=[miniqmt_code], period='1d', start_time=start_time, end_time=end_time)
            df = data.get(miniqmt_code) if isinstance(data, dict) else data
            if df is None or len(df) == 0: return pd.DataFrame()
            df = pd.DataFrame(df).rename(columns={'time': '时间', 'open': '开盘', 'close': '收盘', 'high': '最高', 'low': '最低', 'volume': '成交量'})
            raw_time = df['时间']
            if raw_time.dtype in ['int64', 'float64']:
                length = raw_time.astype(str).str.len().iloc[0]
                df['时间'] = pd.to_datetime(raw_time, unit='ms' if length >= 13 else 's', errors='coerce')
            else:
                df['时间'] = pd.to_datetime(raw_time, errors='coerce')
            df['时间'] = df['时间'] + pd.Timedelta(hours=8)
            df['时间'] = df['时间'].dt.normalize()
            df = df.dropna(subset=['时间'])
            return df[['时间', '开盘', '收盘', '最高', '最低', '成交量']].sort_values('时间')
        except Exception as e: return pd.DataFrame()

    @staticmethod
    def preload_from_miniqmt(start_date: str, end_date: str):
        if not MINIQMT_AVAILABLE: return
        dates = MarketData.get_model_dates(start_date, end_date)
        if not dates: return
        date_map = MarketData.build_date_map(dates)
        today_str = MarketData._get_current_cn_date()
        raw_pairs = set()
        for m_date in dates:
            t1, t2 = date_map[m_date]
            model_file = os.path.join(MODEL_HISTORY_DIR, f"{MODEL_NAME_PREFIX}_{m_date}.json")
            if not os.path.exists(model_file): continue
            with open(model_file, 'r', encoding='utf-8') as f:
                targets, _ = MarketData.parse_sirius_model(json.load(f))
                for t in targets:
                    code = MarketData._convert_code(t['code'])
                    if t1 <= today_str: raw_pairs.add((code, t1))
                    if t2 <= today_str: raw_pairs.add((code, t2))
        monthly_pairs = {}
        for code, date in raw_pairs:
            ym = date[:7]
            monthly_pairs.setdefault(ym,[]).append((code, date))
        for ym in list(monthly_pairs.keys()):
            qmt_parquet_path = MarketData.get_monthly_file_path(ym, qmt_suffix=True)
            existing_keys = set()
            if os.path.exists(qmt_parquet_path):
                try:
                    existing_df = pd.read_parquet(qmt_parquet_path, columns=['ts_code', 'trade_date'])
                    existing_keys = set(zip(existing_df['ts_code'].astype(str), existing_df['trade_date'].astype(str)))
                except: pass
            remaining = [(c, d) for (c, d) in monthly_pairs[ym] if (c, d) not in existing_keys]
            if remaining: monthly_pairs[ym] = remaining
            else: del monthly_pairs[ym]
        for ym, pairs in monthly_pairs.items():
            new_dfs = []
            for code, date in pairs:
                df_min = MarketData._fetch_miniqmt_intraday(code, date)
                if not df_min.empty:
                    df_min['ts_code'] = code
                    df_min['trade_date'] = date
                    new_dfs.append(df_min)
            if not new_dfs: continue
            combined = pd.concat(new_dfs, ignore_index=True)
            combined['ts_code'] = combined['ts_code'].astype(str).str.zfill(6)
            combined['trade_date'] = combined['trade_date'].astype(str)
            qmt_parquet_path = MarketData.get_monthly_file_path(ym, qmt_suffix=True)
            if os.path.exists(qmt_parquet_path):
                try:
                    old_df = pd.read_parquet(qmt_parquet_path)
                    combined = pd.concat([old_df, combined], ignore_index=True)
                except: pass
            combined.drop_duplicates(subset=['时间', 'ts_code', 'trade_date'], inplace=True)
            combined.sort_values(['ts_code', '时间'], inplace=True)
            combined.to_parquet(qmt_parquet_path, index=False, engine='pyarrow')

    @staticmethod
    def preload_daily_from_miniqmt(start_date: str, end_date: str):
        if not MINIQMT_AVAILABLE: return
        dates = MarketData.get_model_dates(start_date, end_date)
        if not dates: return
        date_map = MarketData.build_date_map(dates)
        today_str = MarketData._get_current_cn_date()
        raw_pairs = set()
        for m_date in dates:
            t1, t2 = date_map[m_date]
            model_file = os.path.join(MODEL_HISTORY_DIR, f"{MODEL_NAME_PREFIX}_{m_date}.json")
            if not os.path.exists(model_file): continue
            with open(model_file, 'r', encoding='utf-8') as f:
                targets, _ = MarketData.parse_sirius_model(json.load(f))
                for t in targets:
                    code = MarketData._convert_code(t['code'])
                    if t1 <= today_str: raw_pairs.add((code, t1))
                    if t2 <= today_str: raw_pairs.add((code, t2))
        p_path = MarketData.get_daily_file_path(qmt_suffix=True)
        existing_keys = set()
        if os.path.exists(p_path):
            try:
                old_df = pd.read_parquet(p_path, columns=['ts_code', 'trade_date'])
                existing_keys = set(zip(old_df['ts_code'].astype(str), old_df['trade_date'].astype(str)))
            except: pass
        raw_pairs = [p for p in raw_pairs if p not in existing_keys]
        if not raw_pairs: return
        new_dfs = []
        for code, date in raw_pairs:
            df = MarketData._fetch_miniqmt_daily(code, date)
            if not df.empty:
                df['ts_code'] = str(code).zfill(6)
                df['trade_date'] = str(date)
                new_dfs.append(df)
        if not new_dfs: return
        combined = pd.concat(new_dfs, ignore_index=True)
        if os.path.exists(p_path):
            try:
                old_df = pd.read_parquet(p_path)
                combined = pd.concat([old_df, combined], ignore_index=True)
            except: pass
        combined.drop_duplicates(subset=['时间', 'ts_code', 'trade_date'], inplace=True)
        combined.sort_values(['ts_code', '时间'], inplace=True)
        combined.to_parquet(p_path, index=False, engine='pyarrow')

# ========================= 4. 新增：GitHub 自动提交模块 =========================
class GitHubUploader:
    def __init__(self):
        # 确保使用系统代理访问 GitHub
        self.env = os.environ.copy()
        if HTTP_PROXY: self.env['HTTP_PROXY'] = HTTP_PROXY
        if HTTPS_PROXY: self.env['HTTPS_PROXY'] = HTTPS_PROXY
        # ✅ 新增：自动接受 GitHub 的 SSH host key，避免首次连接时交互式确认卡住脚本
        self.env['GIT_SSH_COMMAND'] = 'ssh -o StrictHostKeyChecking=accept-new'

    def run_cmd(self, cmd: str, cwd: str = None, timeout: int = 60) -> Tuple[bool, str]:
        """执行 Git 命令行操作，增加超时防止密码提示导致无限卡住"""
        try:
            result = subprocess.run(
                cmd, cwd=cwd, shell=True, env=self.env,
                check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
                timeout=timeout
            )
            return True, result.stdout
        except subprocess.TimeoutExpired:
            logger.error(f"执行 Git 命令超时: {cmd} (>{timeout}秒)，可能被密码提示卡住")
            return False, "Timeout"
        except subprocess.CalledProcessError as e:
            logger.error(f"执行 Git 命令失败: {cmd}\n错误信息: {e.stderr}")
            return False, e.stderr

    def init_workspace(self) -> bool:
        """初始化或更新本地的临时 Git 仓库"""
        if not os.path.exists(LOCAL_GIT_WORKSPACE):
            logger.info(f"[Git] 首次克隆仓库 {GIT_REPO_URL} ... (可能需要数秒)")
            success, _ = self.run_cmd(f"git clone {GIT_REPO_URL} {LOCAL_GIT_WORKSPACE}")
            if not success: return False
        else:
            logger.info("[Git] 工作区已存在，拉取最新代码...")
            self.run_cmd("git fetch origin", cwd=LOCAL_GIT_WORKSPACE)
            self.run_cmd(f"git reset --hard origin/{GIT_TARGET_BRANCH}", cwd=LOCAL_GIT_WORKSPACE)
            self.run_cmd("git clean -fd", cwd=LOCAL_GIT_WORKSPACE)
            self.run_cmd(f"git pull origin {GIT_TARGET_BRANCH}", cwd=LOCAL_GIT_WORKSPACE)

        # 写入配置
        self.run_cmd(f'git config user.name "{GIT_USERNAME}"', cwd=LOCAL_GIT_WORKSPACE)
        self.run_cmd(f'git config user.email "{GIT_EMAIL}"', cwd=LOCAL_GIT_WORKSPACE)
        return True

    def sync_files_to_workspace(self, src_backup_dir: str, target_repo_path: str):
        """将备份目录的文件覆盖写入到 Git 工作区对应的目录中"""
        full_target_path = os.path.join(LOCAL_GIT_WORKSPACE, target_repo_path)
        os.makedirs(full_target_path, exist_ok=True)
        
        for file_name in os.listdir(src_backup_dir):
            src_file = os.path.join(src_backup_dir, file_name)
            if os.path.isfile(src_file):
                dst_file = os.path.join(full_target_path, file_name)
                try:
                    shutil.copy2(src_file, dst_file)
                    logger.debug(f"[Git] 同步文件至工作区: {dst_file}")
                except Exception as e:
                    logger.error(f"[Git] 拷贝到工作区失败 {src_file}: {e}")

    def commit_and_push(self) -> bool:
        """提交所有变动并 Push 到 GitHub"""
        self.run_cmd("git add .", cwd=LOCAL_GIT_WORKSPACE)
        
        # 检查是否有文件改动
        success, stdout = self.run_cmd("git status --porcelain", cwd=LOCAL_GIT_WORKSPACE)
        if not stdout.strip():
            logger.info("[Git] 仓库内的数据没有变动，无需提交。")
            return True

        commit_msg = f"Auto-update model market data at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        logger.info("[Git] 正在执行 Commit...")
        success, _ = self.run_cmd(f'git commit -m "{commit_msg}"', cwd=LOCAL_GIT_WORKSPACE)
        if not success:
            logger.error("[Git] Commit 失败，取消 Push")
            return False
        
        logger.info(f"[Git] 正在推送到远程分支 {GIT_TARGET_BRANCH}...")
        success, stderr = self.run_cmd(f"git push origin {GIT_TARGET_BRANCH}", cwd=LOCAL_GIT_WORKSPACE)
        if success:
            logger.info("[Git] 成功推送至 GitHub！")
        else:
            logger.error(f"[Git] Push 失败: {stderr}")
        return success

# ========================= 回测主函数 (增强版) =========================
def run_download():
    global MODEL_NAME_PREFIX, MODEL_API_BASE_URL
    
    models = [
        "流入模型",
        "大成模型",
        "大智模型",
        "低波稳健模型",
        "高潜模型"
    ]
    
    backup_base_dir = r"D:\AIPEQModelSIRIUS\static\minute_backup"
    
    # 【一、 遍历各个模型下载、预加载和备份】
    for model_name in models:
        MODEL_NAME_PREFIX = model_name
        MODEL_API_BASE_URL = f"https://raw.githubusercontent.com/digital-era/AIPEQModel/main/{MODEL_NAME_PREFIX}_"
        
        logger.info(f"\n===================== 开始处理模型: {MODEL_NAME_PREFIX} =====================")
        
        # ModelDownloader.download_models_for_date_range(START_DATE, END_DATE, force=False)
        
        if MINIQMT_AVAILABLE:
            MarketData.preload_from_miniqmt(START_DATE, END_DATE)
            MarketData.preload_daily_from_miniqmt(START_DATE, END_DATE)
            logger.info(f"[{MODEL_NAME_PREFIX}] Market数据下载完成")
            
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
                            logger.error(f"拷贝文件失败 {src_file}: {e}")
                logger.info(f"[{MODEL_NAME_PREFIX}] 数据已备份并覆盖至: {target_dir}")
            else:
                logger.warning(f"数据源目录 {MONTHLY_DIR} 不存在，无数据可备份。")
        else:
            logger.warning("miniQMT未就绪，跳过数据获取阶段")

    logger.info("\n========== 所有模型数据获取及备份处理完成 ==========")
    logger.info("\n========== 开始统一提交数据到 GitHub ==========")

    # 【二、 统一提交到 GitHub】
    github_manager = GitHubUploader()
    if github_manager.init_workspace():
        # 遍历并将所有模型数据同步到Git克隆工作区
        for model_name in models:
            src_backup_dir = os.path.join(backup_base_dir, model_name)
            if os.path.exists(src_backup_dir):
                target_repo_path = f"minute/{model_name}"
                github_manager.sync_files_to_workspace(src_backup_dir, target_repo_path)
        
        # 统一 Commit 和 Push
        github_manager.commit_and_push()
    else:
        logger.error("[Git] GitHub 工作区初始化失败，取消自动提交。请检查代理或网络连接。")

    logger.info("\n========== 任务结束 ==========")

if __name__ == "__main__":
    run_download()
