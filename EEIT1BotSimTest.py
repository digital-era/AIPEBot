#@title Clone AIPEQModel

!pip install json5

#@title Commit EEIFlow*.xlsx to Github AIPEQModel
import pandas as pd
import numpy as np
from openpyxl import load_workbook
from pandas import ExcelWriter
from google.colab import userdata # 用于获取 secrets
import datetime
import os # 导入 os 模块用于路径操作和文件列表
import shutil # 导入 shutil 模块用于文件复制

#%cd /content
os.chdir('/content')

# --- 1. 配置 GitHub 信息 ---
GIT_USERNAME = "digital-era"  # 替换为你的 GitHub 用户名
GIT_EMAIL = "digital_era@sina.com" # 替换为你的 GitHub 邮箱
GIT_REPO_NAME = "AIPEQModel"      # 替换为你的仓库名称
GIT_TARGET_BRANCH = "main"            # 或者 "master"，你的主要分支
# 文件在仓库中的目标路径，例如 "data/"。如果希望直接放在仓库根目录，则留空 ""
TARGET_REPO_PATH = "data/"

GIT_PAT = None
try:
    GIT_PAT = userdata.get('GITHUB_PAT')
except Exception as e:
    print("终止操作, 因为没有 PAT 无法进行 GitHub 操作")
    exit() # 终止执行，因为没有 PAT 无法进行 GitHub 操作

# 构建克隆 URL，使用 PAT 进行认证
# 注意：{GIT_PAT} 是从 Colab Secrets 获取的。
GIT_URL = f"https://{GIT_USERNAME}:{GIT_PAT}@github.com/{GIT_USERNAME}/{GIT_REPO_NAME}.git"

# 定义仓库将要克隆到的本地路径（在 Colab 文件系统中）
# 通常克隆到 /content/ 目录下
REPO_CLONE_PATH = f"/content/{GIT_REPO_NAME}"

# 为了确保每次运行都从一个干净的状态开始，可以先删除旧的仓库目录（可选但推荐）
import os
import shutil
if os.path.exists(REPO_CLONE_PATH):
    shutil.rmtree(REPO_CLONE_PATH)
    print(f"已移除旧的仓库目录: {REPO_CLONE_PATH}")

print(f"正在克隆仓库 {GIT_REPO_NAME} 到 {REPO_CLONE_PATH}...")
# 执行 git clone 命令。在 Colab 中，使用 ! 前缀来执行 shell 命令。
!git clone $GIT_URL $REPO_CLONE_PATH

# 检查克隆是否成功
if os.path.exists(REPO_CLONE_PATH):
    print("仓库克隆成功。")
    # 克隆成功后，通常会切换到克隆的目录进行后续操作
    #%cd $REPO_CLONE_PATH
    os.chdir(REPO_CLONE_PATH)

    print(f"已切换到仓库目录: {os.getcwd()}")
else:
    print("仓库克隆失败。请检查你的 GitHub 用户名、仓库名称或 PAT 是否正确。")


#@title ZIP Model Files
import os
import zipfile
from google.colab import files

# 指定目录路径
directory = '/content/AIPEQModel'

# 压缩包文件名（可根据需要修改）
zip_filename = '流入模型_json_files.zip'

# 收集符合条件的文件
files_to_zip = []
for filename in os.listdir(directory):
    if filename.startswith('流入模型_') and filename.endswith('.json'):
        full_path = os.path.join(directory, filename)
        files_to_zip.append(full_path)

# 执行打包
if files_to_zip:
    with zipfile.ZipFile(zip_filename, 'w', zipfile.ZIP_DEFLATED) as zipf:
        for file_path in files_to_zip:
            # 只保留文件名，不保留目录结构
            arcname = os.path.basename(file_path)
            zipf.write(file_path, arcname)

    print(f"✅ 打包完成！共包含 {len(files_to_zip)} 个文件。")
    print(f"压缩包名称：{zip_filename}")

    # 自动下载到本地
    files.download(zip_filename)
else:
    print("⚠️ 未在目录中找到任何以 “流入模型_” 开头的 .json 文件。")


#@title Unzip Model Files to historical_models Directory
import os
import zipfile
import shutil

# 定义相关路径
historical_dir = '/content/historical_models'
zip_file_path = '/content/AIPEQModel/流入模型_json_files.zip'

# 步骤 1：确保目标目录存在（若不存在则创建）
os.makedirs(historical_dir, exist_ok=True)

# 步骤 2：清除 /content/historical_models 目录下所有 .json 文件
deleted_count = 0
for filename in os.listdir(historical_dir):
    if filename.endswith('.json'):
        file_path = os.path.join(historical_dir, filename)
        os.remove(file_path)
        deleted_count += 1

print(f"已清除 {deleted_count} 个 .json 文件（目录：{historical_dir}）。")

# 步骤 3：从压缩包中解压所有 .json 文件到目标目录
if os.path.exists(zip_file_path):
    extracted_count = 0
    with zipfile.ZipFile(zip_file_path, 'r') as zipf:
        for member in zipf.namelist():
            if member.endswith('.json'):
                # 仅提取 .json 文件，并保持文件名不变
                zipf.extract(member, historical_dir)
                extracted_count += 1

    print(f"已成功解压 {extracted_count} 个 .json 文件至 {historical_dir} 目录。")
    print("操作完成。")
else:
    print(f"错误：压缩包文件 {zip_file_path} 不存在，请确认已生成该压缩包。")


!mkdir -p /content/monthly_data && cp /content/AIPEQModel/minute/*.parquet /content/monthly_data

!pip install akshare
!pip install tushare


%cd /content


#@title EEIT1Bot BackTest Simulation (Parquet Master Edition)
import os
import json
import logging
import pandas as pd
from datetime import datetime, time, timedelta, timezone
import time as time_module
import glob
import requests
import random

# ========================= 回测配置 =========================
class SimConfig:
    # ⚠️ 注意：这里必须填写过去真实的日期！不能是未来的日期！
    START_DATE = "2026-04-07"
    END_DATE = "2026-04-13"
    # 🌟 新增：回测引擎真实截止日期(避免未收盘数据污染回测)
    TRADE_END_DATE = "2026-04-14"  
    INITIAL_CASH = 1000000.0

    BUY_REBOUND_RATIO = 0.0062
    SELL_DROP_RATIO = 0.0038
    FORCE_DEADLINE_TIME = time(14, 50)
    TRADE_RATIO = 0.5

    MODEL_HISTORY_DIR = "./historical_models"
    DATA_CACHE_DIR = "./min_data_cache"
    MONTHLY_DIR = "./monthly_data"
    MODEL_NAME_PREFIX = "流入模型"

    ENABLE_PRELOAD = True
    PRELOAD_ONLY = False

    MIN_WEIGHT_THRESHOLD = 0.0

    # ---------- API 配置（必填） ----------
    API_BASE_URL = "https://query.aivibeinvestment.com/api/query"  # TODO: 替换为你自己的 Cloudflare Workers 地址
    API_REQUEST_INTERVAL = 0.3  # 正常请求间隔（秒）
    
    # ---------- 容错与退避配置 ----------
    MAX_RETRIES = 5                
    EXPONENTIAL_BACKOFF_BASE = 2   
    FILL_OHLC_WITH_PRICE = True    

# 创建目录
os.makedirs(SimConfig.MODEL_HISTORY_DIR, exist_ok=True)
os.makedirs(SimConfig.DATA_CACHE_DIR, exist_ok=True)
os.makedirs(SimConfig.MONTHLY_DIR, exist_ok=True)

# ========================= 日志配置 =========================
root_logger = logging.getLogger()
root_logger.handlers.clear()
logger = logging.getLogger("Simulator")
logger.handlers.clear()
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)
file_handler = logging.FileHandler('backtest.log', encoding='utf-8')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

# ========================= 数据模块 =========================
class MarketData:

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
    def parse_targets(model_data: dict) -> list:
        try:
            details = model_data['结果']['最优投资组合配置']['配置详情']
        except (KeyError, TypeError):
            return []
        targets = []
        for item in details:
            weight_str = item.get('最优权重(%)', '0%')
            weight = float(weight_str.replace('%', '')) / 100.0
            ref_price = float(item.get('最近一日价格', 0))
            targets.append({'code': item.get('代码', ''), 'weight': weight, 'ref_price': ref_price})
        return targets

    @staticmethod
    def _get_current_cn_date() -> str:
        tz_cn = timezone(timedelta(hours=8))
        return datetime.now(tz_cn).strftime('%Y-%m-%d')

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
        sh_prefixes = ('60', '68', '51', '56', '58', '55', '900')
        if any(code.startswith(p) for p in sh_prefixes):
            return f"{code}.SH"
        return f"{code}.SZ"

    @staticmethod
    def get_monthly_file_path(year_month: str) -> str:
        return os.path.join(SimConfig.MONTHLY_DIR, f"minute_data_{year_month}.parquet")

    @staticmethod
    def get_minute_data(code: str, date_str: str, use_cache_only: bool = True) -> pd.DataFrame:
        ts_code = MarketData._convert_code(code)
        year_month = date_str[:7]
        monthly_file = MarketData.get_monthly_file_path(year_month)

        if os.path.exists(monthly_file):
            try:
                df_month = pd.read_parquet(monthly_file)
                df = df_month[(df_month['ts_code'] == ts_code) & (df_month['trade_date'] == date_str)].copy()
                if not df.empty:
                    df = df.rename(columns={"trade_time": "时间", "close": "收盘", "open": "开盘", "high": "最高", "low": "最低", "vol": "成交量"})
                    if '时间' in df.columns:
                        df['时间'] = pd.to_datetime(df['时间'])
                    return df
            except Exception as e:
                logger.error(f"读取 Parquet 失败: {monthly_file}, 错误: {e}")

        cache_file = os.path.join(SimConfig.DATA_CACHE_DIR, f"{ts_code}_{date_str}.csv")
        if os.path.exists(cache_file):
            df = pd.read_csv(cache_file)
            df = df.rename(columns={"trade_time": "时间", "close": "收盘", "open": "开盘", "high": "最高", "low": "最低", "vol": "成交量"})
            if '时间' in df.columns:
                df['时间'] = pd.to_datetime(df['时间'])
            return df

        return pd.DataFrame()

    @staticmethod
    def merge_monthly_data(year_month: str):
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
            except Exception: pass
        if all_data:
            merged = pd.concat(all_data, ignore_index=True)
            monthly_file = MarketData.get_monthly_file_path(year_month)
            if os.path.exists(monthly_file):
                try:
                    existing_df = pd.read_parquet(monthly_file)
                    merged = pd.concat([existing_df, merged], ignore_index=True).drop_duplicates(subset=['ts_code', 'trade_date', 'time'], keep='last')
                except Exception:
                    pass
            merged.to_parquet(monthly_file, index=False, compression='zstd')
            logger.info(f"✅ 已合并至 {year_month}: {len(merged)}条 -> {monthly_file}")

    @staticmethod
    def _fetch_intraday_from_api(code: str, date_str: str) -> pd.DataFrame:
        api_url = f"{SimConfig.API_BASE_URL.rstrip('/')}?type=specifiedIntraday&code={code}&date={date_str}"
        logger.debug(f"🔍 [请求地址] {api_url}")

        for attempt in range(1, SimConfig.MAX_RETRIES + 1):
            try:
                resp = requests.get(api_url, timeout=30)
                
                if resp.status_code == 404:
                    return pd.DataFrame()

                resp.raise_for_status()

                if not resp.text or not resp.text.strip():
                    raise ValueError("EmptyBodyError")

                result = resp.json()
                data = result if isinstance(result, list) else None
                if not data and isinstance(result, dict):
                    data = result.get("data") or result.get("trends") or result.get("list")
                    if not data:
                        for v in result.values():
                            if isinstance(v, list): data = v; break

                if not data:
                    raise ValueError("EmptyDataError")

                df = pd.DataFrame(data)
                if "date" not in df.columns or "price" not in df.columns:
                    raise ValueError("MissingColumnsError")

                df["时间"] = pd.to_datetime(df["date"] + " " + df["time"])
                df["收盘"] = df["price"]
                df["成交量"] = df.get("volume", 0)

                if SimConfig.FILL_OHLC_WITH_PRICE:
                    df["开盘"] = df["最高"] = df["最低"] = df["price"]
                else:
                    df["开盘"] = df["最高"] = df["最低"] = float('nan')

                return df[["时间", "开盘", "收盘", "最高", "最低", "成交量"]].sort_values("时间")

            except requests.exceptions.HTTPError as e:
                status = e.response.status_code if e.response is not None else 0
                if 400 <= status < 500: return pd.DataFrame()
                wait_time = SimConfig.EXPONENTIAL_BACKOFF_BASE * (2 ** (attempt - 1)) + random.uniform(0, 1)
                time_module.sleep(wait_time)

            except (requests.exceptions.ConnectionError, requests.exceptions.Timeout):
                wait_time = SimConfig.EXPONENTIAL_BACKOFF_BASE * (2 ** (attempt - 1)) + random.uniform(0, 1)
                time_module.sleep(wait_time)

            except ValueError as e:
                if "EmptyBodyError" in str(e) or "EmptyDataError" in str(e) or "MissingColumnsError" in str(e):
                    wait_time = SimConfig.EXPONENTIAL_BACKOFF_BASE * (2 ** (attempt - 1)) + random.uniform(0, 1)
                    time_module.sleep(wait_time)
                else: raise

            except (json.JSONDecodeError, requests.exceptions.JSONDecodeError):
                wait_time = SimConfig.EXPONENTIAL_BACKOFF_BASE * (2 ** (attempt - 1)) + random.uniform(0, 1)
                time_module.sleep(wait_time)

        return pd.DataFrame()

    @staticmethod
    def preload_from_models(start_date: str, end_date: str):
        logger.info("🚀 启动智能预下载器")
        
        FAILED_RECORD_FILE = "./preload_failed_tasks.json"
        failed_tasks = set()
        if os.path.exists(FAILED_RECORD_FILE):
            with open(FAILED_RECORD_FILE, 'r', encoding='utf-8') as f:
                failed_tasks = set(json.load(f))
            logger.info(f"📂 发现历史失败记录，共 {len(failed_tasks)} 条")

        dates = MarketData.get_model_dates(start_date, end_date)
        if not dates:
            logger.error("❌ 没有找到模型文件！")
            return

        date_map = MarketData.build_date_map(dates)

        raw_pairs = set()
        for model_date_str in dates:
            t1_date, t2_date = date_map[model_date_str]
            model_file = os.path.join(SimConfig.MODEL_HISTORY_DIR, f"{SimConfig.MODEL_NAME_PREFIX}_{model_date_str}.json")
            if not os.path.exists(model_file): continue
            with open(model_file, 'r', encoding='utf-8') as f:
                for t in MarketData.parse_targets(json.load(f)):
                    if t.get('weight', 0) > SimConfig.MIN_WEIGHT_THRESHOLD:
                        raw_pairs.add((t['code'], t1_date))
                        raw_pairs.add((t['code'], t2_date))

        logger.info("🔍 正在扫描已有 Parquet 文件索引...")
        months_needed = set(d[:7] for _, d in raw_pairs)
        parquet_keys_set = set()
        
        for ym in months_needed:
            p_path = MarketData.get_monthly_file_path(ym)
            if os.path.exists(p_path):
                try:
                    df_p = pd.read_parquet(p_path, columns=['ts_code', 'trade_date'])
                    keys = set(zip(df_p['ts_code'], df_p['trade_date']))
                    parquet_keys_set.update(keys)
                    logger.info(f"  📦 {ym}.parquet 含 {len(keys)} 条记录索引")
                except Exception as e:
                    logger.error(f"  ❌ 读取 {p_path} 索引失败: {e}")

        pending_tasks = []
        cached_count = 0
        known_failed_count = 0

        for code, trade_date_str in raw_pairs:
            ts_code = MarketData._convert_code(code)
            task_key = f"{ts_code}_{trade_date_str}"

            if (ts_code, trade_date_str) in parquet_keys_set:
                cached_count += 1
                continue
                
            cache_file = os.path.join(SimConfig.DATA_CACHE_DIR, f"{ts_code}_{trade_date_str}.csv")
            if os.path.exists(cache_file):
                cached_count += 1
                continue
                
            if task_key in failed_tasks:
                known_failed_count += 1
                continue
                
            pending_tasks.append((code, trade_date_str))

        logger.info(f"🟢 已缓存跳过(主要来自Parquet): {cached_count} | 🔴 已知失败跳过: {known_failed_count} | 🟡 待下载: {len(pending_tasks)}")
        
        if not pending_tasks:
            logger.info("✅ 数据已全量就绪，无需下载，退出预下载器。")
            return

        success_count = 0
        new_failed_count = 0
        last_month = None

        for idx, (code, trade_date_str) in enumerate(pending_tasks, 1):
            current_month = trade_date_str[:7]
            task_key = f"{MarketData._convert_code(code)}_{trade_date_str}"
            ts_code = MarketData._convert_code(code)
            cache_file = os.path.join(SimConfig.DATA_CACHE_DIR, f"{ts_code}_{trade_date_str}.csv")

            if last_month and current_month != last_month:
                MarketData.merge_monthly_data(last_month)
            last_month = current_month

            logger.info(f"[{idx}/{len(pending_tasks)}] 下载: {code} 交易日({trade_date_str})")
            df = MarketData._fetch_intraday_from_api(code, trade_date_str)

            if not df.empty:
                df.to_csv(cache_file, index=False)
                success_count += 1
                if task_key in failed_tasks: failed_tasks.remove(task_key)
                logger.info(f"  ✓ 成功并落盘 ({len(df)} 条)")
            else:
                new_failed_count += 1
                failed_tasks.add(task_key)
                with open(FAILED_RECORD_FILE, 'w', encoding='utf-8') as f:
                    json.dump(list(failed_tasks), f, ensure_ascii=False, indent=2)
                logger.error(f"  ✗ 彻底失败，已记录至 {FAILED_RECORD_FILE}")

            time_module.sleep(SimConfig.API_REQUEST_INTERVAL)

        if last_month: MarketData.merge_monthly_data(last_month)

        if not failed_tasks and os.path.exists(FAILED_RECORD_FILE):
            os.remove(FAILED_RECORD_FILE)
            logger.info("🎉 所有历史遗留失败项均已补全，清理失败记录文件！")

        logger.info("=" * 60)
        logger.info(f"📊 本轮执行汇总: 成功 {success_count} | 失败 {new_failed_count}")
        logger.info("=" * 60)


# ========================= 虚拟账户模块 =========================
class MockAccount:
    def __init__(self, initial_cash):
        self.cash = initial_cash
        self.positions = {}
        self.trade_history = []

    def start_new_day(self):
        for code in self.positions:
            self.positions[code]['can_sell'] = self.positions[code]['volume']

    def place_order(self, date_str, exec_time, code, order_type, volume, price, reason="正常择时"):
        cost = volume * price
        if order_type == 'buy':
            if self.cash >= cost:
                self.cash -= cost
                pos = self.positions.get(code, {'volume': 0, 'can_sell': 0, 'avg_price': 0.0})
                total_cost = pos['volume'] * pos['avg_price'] + cost
                pos['volume'] += volume
                pos['avg_price'] = total_cost / pos['volume']
                self.positions[code] = pos
                self._record(date_str, exec_time, code, "买入", volume, price, reason)
                return True
        elif order_type == 'sell':
            pos = self.positions.get(code)
            if pos and pos['can_sell'] >= volume:
                self.cash += cost
                pos['volume'] -= volume
                pos['can_sell'] -= volume
                if pos['volume'] == 0: del self.positions[code]
                self._record(date_str, exec_time, code, "卖出", volume, price, reason)
                return True
        return False

    def _record(self, date, time_val, code, direction, vol, price, reason):
        logger.info(f"{date} {time_val} | {direction} {code} {vol} @ {price:.2f} ({reason})")
        self.trade_history.append({'日期': date, '时间': time_val, '代码': code, '方向': direction, '数量': vol, '价格': price, '类型': reason})

    def get_total_asset(self, current_prices: dict):
        stock_value = sum([pos['volume'] * current_prices.get(code, pos['avg_price']) for code, pos in self.positions.items()])
        return self.cash + stock_value

# ========================= 执行器 =========================
class SimExecutor:
    def __init__(self, account): self.account = account

    def simulate_day(self, date_str, buy_targets, sell_targets):
        self.account.start_new_day()
        all_codes = set([t['code'] for t in buy_targets] + [t['code'] for t in sell_targets])
        min_data_dict = {code: MarketData.get_minute_data(code, date_str) for code in all_codes}

        buy_orders = {t['code']: {'vol': t['volume'], 'ref_price': t['ref_price'], 'low': float('inf')} for t in buy_targets}
        sell_orders = {t['code']: {'vol': t['volume'], 'pre_close': t['pre_close'], 'high': -float('inf')} for t in sell_targets}

        valid_df = next((df for df in min_data_dict.values() if not df.empty), None)
        if valid_df is None: return  # ✅ 修复点：严格判断是否为 None

        for _, row in valid_df.iterrows():
            current_time, curr_t = row['时间'], row['时间'].time()

            for code, order in list(sell_orders.items()):
                df = min_data_dict.get(code)
                if df is None or df.empty: continue
                minute_row = df[df['时间'] == current_time]
                if minute_row.empty: continue
                curr_price = minute_row.iloc[0]['收盘']
                if curr_price > order['high']: order['high'] = curr_price
                if curr_t >= SimConfig.FORCE_DEADLINE_TIME:
                    self.account.place_order(date_str, curr_t, code, 'sell', order['vol'], max(curr_price, order['pre_close'] * 0.995), "尾盘强制卖出")
                    del sell_orders[code]
                elif curr_price <= order['high'] * (1 - SimConfig.SELL_DROP_RATIO):
                    self.account.place_order(date_str, curr_t, code, 'sell', order['vol'], curr_price, "高位回落卖出")
                    del sell_orders[code]

            for code, order in list(buy_orders.items()):
                df = min_data_dict.get(code)
                if df is None or df.empty: continue
                minute_row = df[df['时间'] == current_time]
                if minute_row.empty: continue
                curr_price = minute_row.iloc[0]['收盘']
                if curr_price < order['low']: order['low'] = curr_price
                if curr_t >= SimConfig.FORCE_DEADLINE_TIME:
                    if curr_price <= order['ref_price']: self.account.place_order(date_str, curr_t, code, 'buy', order['vol'], curr_price, "尾盘强制买入")
                    del buy_orders[code]
                elif curr_price >= order['low'] * (1 + SimConfig.BUY_REBOUND_RATIO) and curr_price <= order['ref_price']:
                    self.account.place_order(date_str, curr_t, code, 'buy', order['vol'], curr_price, "低位反弹买入")
                    del buy_orders[code]

# ========================= 回测主引擎 =========================
def run_backtest():
    if SimConfig.ENABLE_PRELOAD:
        MarketData.preload_from_models(SimConfig.START_DATE, SimConfig.END_DATE)
        if SimConfig.PRELOAD_ONLY:
            logger.info("⏹️ 仅预下载模式，程序退出")
            return

    account = MockAccount(SimConfig.INITIAL_CASH)
    executor = SimExecutor(account)
    # 改为（获取全量模型，建立映射关系）：
    dates = MarketData.get_model_dates(SimConfig.START_DATE, SimConfig.END_DATE)
    date_map = MarketData.build_date_map(dates)
    daily_stats = []

    # 原来：
    # for date_str in dates:
    #     model_file = ...
    #     if not os.path.exists(model_file): continue
    #     logger.info(f"========== {date_str} ==========")
    #     ...
    #     executor.simulate_day(date_str, buy_targets, sell_targets)
    #     ...
    #     df = MarketData.get_minute_data(code, date_str)
    #     daily_stats.append({'日期': date_str, ...})

    # 改为：
    for model_date_str in dates:
        model_file = os.path.join(SimConfig.MODEL_HISTORY_DIR, f"{SimConfig.MODEL_NAME_PREFIX}_{model_date_str}.json")
        if not os.path.exists(model_file): continue
        
        t1_date = date_map[model_date_str][0] # 🌟 提取真实的交易日期(T+1)
        
        if t1_date > SimConfig.TRADE_END_DATE: # 🌟 超过回测截止日则跳过
            continue
            
        logger.info(f"========== 模型:{model_date_str} -> 交易日:{t1_date} ==========")

        with open(model_file, 'r', encoding='utf-8') as f: model_data = json.load(f)
        target_holdings = MarketData.parse_targets(model_data)
        target_codes = [t['code'] for t in target_holdings if t.get('weight', 0) > SimConfig.MIN_WEIGHT_THRESHOLD]

        buy_targets, sell_targets = [], []
        for code, pos in account.positions.items():
            if code not in target_codes: sell_targets.append({'code': code, 'volume': pos['can_sell'], 'pre_close': pos['avg_price'] * 1.01})

        available_cash = account.cash * SimConfig.TRADE_RATIO
        for t in target_holdings:
            if t['code'] not in account.positions:
                vol = int((available_cash * t['weight'] / t['ref_price']) // 100) * 100
                if vol > 0: buy_targets.append({'code': t['code'], 'volume': vol, 'ref_price': t['ref_price']})

        executor.simulate_day(t1_date, buy_targets, sell_targets) # 🌟 传入真实交易日期

        closing_prices = {}
        for code in account.positions:
            df = MarketData.get_minute_data(code, t1_date) # 🌟 拉取真实交易日的数据结算
            if not df.empty:
                closing_prices[code] = df.iloc[-1]['收盘']
                
        nav = account.get_total_asset(closing_prices)
        daily_stats.append({'日期': t1_date, '总资产': nav, '现金': account.cash}) # 🌟 记录真实交易日
        logger.info(f"--- 当日总资产: {nav:,.2f} ---")


    if account.trade_history: pd.DataFrame(account.trade_history).to_excel("sim_trade_records.xlsx", index=False)
    if daily_stats:
        pd.DataFrame(daily_stats).to_excel("sim_daily_nav.xlsx", index=False)
        final_nav = daily_stats[-1]['总资产']
        logger.info(f"\n📊 回测完成: 初始{SimConfig.INITIAL_CASH:,.0f} → 最终{final_nav:,.2f} ({(final_nav - SimConfig.INITIAL_CASH) / SimConfig.INITIAL_CASH * 100:+.2f}%)")

if __name__ == "__main__":
    run_backtest()


# @title Commit Parquet File to Github AIPEQModel
import os
import datetime
import shutil # 引入文件操作库
import re

%cd /content

# --- 1. 配置 GitHub 信息 ---
GIT_USERNAME = "digital-era"
GIT_EMAIL = "digital_era@sina.com"
GIT_REPO_NAME = "AIPEQModel"
GIT_TARGET_BRANCH = "main"
# 文件在仓库中的目标路径。根目录留空 ""
TARGET_REPO_PATH = "minute"

# !!! 安全警告 !!!
# 直接将 PAT 写入代码非常不安全。强烈建议使用 Colab Secrets 来存储它。
# 1. 点击左侧边栏的 🔑 图标。
# 2. 添加一个新的 Secret，名称为 `GITHUB_PAT`。
# 3. 将您的 PAT 粘贴到值中。
# 4. 勾选 "Notebook access"。
# 5. 然后使用下一行代码来安全地获取它。
# from google.colab import userdata
# GIT_PAT = userdata.get('GITHUB_PAT')
GIT_PAT = "" # 暂时使用硬编码，但请尽快切换到 Secrets

# --- 2. 动态查找源parquet文件 ---
search_directory = '/content/monthly_data'
repo_directory = f'/content/{GIT_REPO_NAME}' # 定义仓库路径
file_prefixes = ['minute_data_2026-04.parquet']
source_files_to_commit = []

print(f"--- 步骤1: 在 '{search_directory}' 查找模型parquet文件 ---")
all_files_in_content = os.listdir(search_directory)
for filename in all_files_in_content:
    if filename.endswith('.parquet'):
        for prefix in file_prefixes:
            if filename.startswith(prefix):
                source_files_to_commit.append(os.path.join(search_directory, filename))
                break

# --- 3. 移动文件并执行 Git 操作 ---
if not source_files_to_commit:
    print("\n⚠️ 未找到任何需要提交的parquet文件。脚本执行结束。")
elif not os.path.isdir(repo_directory):
    print(f"\n❌ 错误：Git仓库目录 '{repo_directory}' 不存在。请确保您已成功克隆了仓库。")
else:
    print("\n✅ 找到以下源文件:")
    for file_path in source_files_to_commit:
        print(f"- {os.path.basename(file_path)}")

    # --- 关键修复 1: 将文件拷贝到仓库目录内 ---
    print(f"\n--- 步骤2: 将文件拷贝到仓库 '{repo_directory}' ---")
    files_to_add_in_repo = []
    for src_path in source_files_to_commit:
        file_name = os.path.basename(src_path)
        # 目标路径可以是仓库的根目录，或指定的子目录
        dest_path = os.path.join(repo_directory, TARGET_REPO_PATH, file_name)

        # 确保目标子目录存在 (如果TARGET_REPO_PATH不为空)
        os.makedirs(os.path.dirname(dest_path), exist_ok=True)

        shutil.copy(src_path, dest_path)
        print(f"已拷贝 '{file_name}' -> '{dest_path}'")
        # 记录在仓库中的相对路径，用于 git add
        files_to_add_in_repo.append(os.path.join(TARGET_REPO_PATH, file_name))

    # --- 关键修复 2: 进入仓库目录执行Git命令，并使用PAT ---
    print("\n--- 步骤3: 执行Git操作 ---")

    # 保存当前工作目录
    original_directory = os.getcwd()
    try:
        # 进入 Git 仓库目录，这是执行git命令的最佳实践
        os.chdir(repo_directory)

        !git config user.name "$GIT_USERNAME"
        !git config user.email "$GIT_EMAIL"
        print("Git 用户名和邮箱已配置。")

        print(f"正在切换到分支: {GIT_TARGET_BRANCH}...")
        !git checkout $GIT_TARGET_BRANCH

        # 为文件名加上引号，以处理可能存在的空格等特殊字符
        quoted_file_paths = [f'"{path}"' for path in files_to_add_in_repo]
        files_to_add_str = " ".join(quoted_file_paths)

        print("正在添加指定文件到 Git 暂存区...")
        !git add {files_to_add_str}

        commit_message = f"Update parquet from Colab: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        print(f"正在提交更改: '{commit_message}'...")
        # 使用 git status 检查暂存区状态，如果无更改则不提交
        # `git diff --staged --quiet` 如果有暂存更改，则退出码为1，否则为0
        if (os.system('git diff --staged --quiet') != 0) or True:
            !git commit -m "{commit_message}"

            # 构建带PAT的远程URL
            remote_url = f"https://{GIT_PAT}@github.com/{GIT_USERNAME}/{GIT_REPO_NAME}.git"

            print(f"正在推送到远程仓库的 '{GIT_TARGET_BRANCH}' 分支...")
            # 使用构建好的URL进行推送
            !git push "{remote_url}" {GIT_TARGET_BRANCH}

            print("\n🎉 所有指定的parquet文件已成功推送到 GitHub。")
        else:
            print("没有检测到文件更改，无需提交。")

    finally:
        # 无论成功与否，都切换回原始目录，避免影响后续的cell
        os.chdir(original_directory)
        print(f"\n已返回原始工作目录: {original_directory}")


# @title Convert Parquet File to Excel
import pandas as pd
import glob
import os

%cd /content

PARQUET_DIR = "./monthly_data"
EXCEL_DIR = "./excel_output"

os.makedirs(EXCEL_DIR, exist_ok=True)

files = glob.glob(os.path.join(PARQUET_DIR, "*.parquet"))
if not files:
    print("❌ 没有找到 parquet 文件")
else:
    for f in files:
        name = os.path.basename(f).replace(".parquet", ".xlsx")
        out_path = os.path.join(EXCEL_DIR, name)
        df = pd.read_parquet(f)
        df.to_excel(out_path, index=False)
        print(f"✅ {os.path.basename(f)} → {name} ({len(df)} 行)")
    print(f"\n共转换 {len(files)} 个文件，输出目录: {EXCEL_DIR}/")

