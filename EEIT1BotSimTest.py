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
    MONITOR_START_HOUR = 9
    MONITOR_START_MINUTE = 45
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
    ONLY_PRELOAD = False
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
            # ✅ 核心修复：前一日持仓全部可卖
            self.positions[code]['can_sell'] = self.positions[code].get('volume', 0)
            #if 'can_sell' not in self.positions[code]:
                #self.positions[code]['can_sell'] = 0

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
            logger.debug(f"⚠️ {display_name} 价格为空，跳过{side}检测")
            return False
        
        if pre_close is None or pre_close <= 0:
            logger.debug(f"⚠️ {display_name} 昨收价异常({pre_close})，跳过{side}检测")
            return False
        
        limit_up, limit_down = MarketData.get_limit_prices(pre_close)
        if limit_up is None:
            logger.debug(f"⚠️ {display_name} 无法计算涨跌停价")
            return False

        if side == 'buy' and price >= limit_up:
            logger.debug(f"⚠️ {display_name} 触及涨停 {limit_up:.2f}，无法买入")
            return True
        
        if side == 'sell' and price <= limit_down:
            logger.debug(f"⚠️ {display_name} 触及跌停 {limit_down:.2f}，无法卖出")
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
        🚀 升级版：分钟级时间轴动态撮合引擎 (Time-Driven Matching Engine)
        完美模拟真实交易所的挂单等待与价格触达成交逻辑。
        """
        # --- 0. 开盘准备 ---
        self.account.start_day()

        # 1. 预加载当日所有相关股票的分钟数据 (提速并为时间轴做准备)
        all_codes = set([t['code'] for t in targets] + list(self.account.positions.keys()))
        daily_data = {}
        for code in all_codes:
            df = MarketData.get_minute_data(code, date_str)
            if not df.empty:
                # 将时间设为索引，方便后续按时间戳切片查询
                df = df.set_index('时间')
                daily_data[code] = df

        # 获取 10:00 (或开盘后最近时间) 的价格，用于计算初始资产和目标仓位
        dt_1000 = datetime.combine(datetime.strptime(date_str, "%Y-%m-%d").date(), time(SimConfig.MONITOR_START_HOUR, SimConfig.MONITOR_START_MINUTE))
        prices_1000 = {}
        for code, df in daily_data.items():
            mask = df.index >= dt_1000
            if mask.any():
                prices_1000[code] = df[mask]['收盘'].iloc[0]

        # 2. 计算目标仓位 (10:00 截面)
        total_pos_value = 0
        for c, p in self.account.positions.items():
            price = prices_1000.get(c, self._get_last_known_price(c, p['avg_price']))
            total_pos_value += p['volume'] * price
            
        total_asset = self.account.cash + total_pos_value
        effective_asset = total_asset * SimConfig.TRADE_RATIO * pos_factor
        target_vols = {t['code']: int(effective_asset * t['weight'] / t['ref_price'] / 100) * 100 for t in targets}

        # ==========================================
        # 3. 生成 10:00 挂单意图 (Pending Orders Book)
        # ==========================================
        pending_sells = {}
        pending_buys = {}

        # 卖出意图池
        for code, pos in list(self.account.positions.items()):
            t_vol = target_vols.get(code, 0)
            if pos['volume'] > t_vol:
                #sell_vol = pos.get('can_sell', 0)
                sell_needed = pos['volume'] - t_vol
                sell_vol = min(pos['can_sell'], sell_needed)
                if sell_vol > 0:
                    pre_close = pre_closes.get(code, pos['avg_price'])
                    pending_sells[code] = {
                        'vol': sell_vol,
                        'limit_price': pre_close, # 限价条件：市价 >= 昨收
                        'name': pos.get('name', ''),
                        'pre_close': pre_close
                    }

        # 买入意图池
        for code, t_vol in target_vols.items():
            cur_vol = self.account.positions.get(code, {}).get('volume', 0)
            if t_vol > cur_vol:
                target_info = next((t for t in targets if t['code'] == code), None)
                if target_info:
                    ref_p = target_info['ref_price']
                    pending_buys[code] = {
                        'vol': t_vol - cur_vol,
                        'limit_price': ref_p, # 限价条件：市价 <= 基准价
                        'name': target_info.get('name', ''),
                        'pre_close': pre_closes.get(code, ref_p)
                    }

        # ==========================================
        # 4. 按分钟推进，模拟交易所真实撮合过程
        # ==========================================
        # 提取当天所有大于 10:00 的有效分钟时间戳并排序
        all_timestamps = set()
        for df in daily_data.values():
            all_timestamps.update(df[df.index >= dt_1000].index)
        sorted_times = sorted(list(all_timestamps))

        forced_sell_time = time(14, 50)
        forced_sell_triggered = False

        for current_dt in sorted_times:
            current_time = current_dt.time()

            # --- 14:50 强制卖出逻辑更新挂单条件 ---
            # if current_time >= forced_sell_time and not forced_sell_triggered:
            #     forced_sell_triggered = True
            #     for code, order in pending_sells.items():
            #         df = daily_data.get(code)
            #         if df is not None and current_dt in df.index:
            #             p_1450 = df.loc[current_dt, '收盘']
            #             # 取消昨收约束，更新卖出底线为保护价：(当前价 与 昨收99.5% 的较大值)
            #             order['limit_price'] = max(p_1450, order['pre_close'] * FORCE_SELL_PRICE_RATIO)

            # --- A. 撮合卖出 (优先卖出，释放现金给买单用) ---
            completed_sells = []
            for code, order in pending_sells.items():
                df = daily_data.get(code)
                if df is None or current_dt not in df.index: continue
                price = df.loc[current_dt, '收盘']

                if self._check_limit_up_down(code, price, 'sell', order['pre_close'], order['name']):
                    continue

                # 触价成交核心逻辑
                if price >= order['limit_price']:
                    reason = "尾盘强制" if forced_sell_triggered else "盘中达成止盈"
                    if self.account.order(date_str, current_time, code, 'sell', order['vol'], price, reason, order['name']):
                        completed_sells.append(code)
                        # 记录交易明细 (原代码漏掉了这一步，导致 Excel 没数据)
                        self.today_trades.append({
                            'date': date_str, 'time': current_time.strftime('%H:%M'), 
                            'code': code, 'name': order['name'], 'side': 'sell', 
                            'volume': order['vol'], 'price': price, 'reason': reason
                        })
            
            # 清理已成交的卖单
            for c in completed_sells: 
                del pending_sells[c]

            # --- B. 撮合买入 ---
            completed_buys = []
            for code, order in pending_buys.items():
                df = daily_data.get(code)
                if df is None or current_dt not in df.index: continue
                price = df.loc[current_dt, '收盘']

                if self._check_limit_up_down(code, price, 'buy', order['pre_close'], order['name']):
                    continue

                # 触价成交核心逻辑
                if price <= order['limit_price']:
                    if self.account.order(date_str, current_time, code, 'buy', order['vol'], price, "盘中达成抄底", order['name']):
                        completed_buys.append(code)
                        self.today_trades.append({
                            'date': date_str, 'time': current_time.strftime('%H:%M'), 
                            'code': code, 'name': order['name'], 'side': 'buy', 
                            'volume': order['vol'], 'price': price, 'reason': "盘中达成抄底"
                        })
                    else:
                        # 如果因当前没钱 order 返回 False，不移除挂单。
                        # 等后续某分钟有其他股票卖出了有了钱，这里依然可以自动买入！
                        pass
            
            # 清理已成交的买单
            for c in completed_buys: 
                del pending_buys[c]

            # --- 性能优化：如果没有排队挂单了，提前结束今天的时间轴循环 ---
            if not pending_sells and not pending_buys:
                break
                        

# ========================= 回测主函数 =========================
def run_strict_backtest():
    if SimConfig.ENABLE_PRELOAD:
        MarketData.preload_from_models(SimConfig.START_DATE, SimConfig.END_DATE)
        if SimConfig.ONLY_PRELOAD:
          logger.info("✅ 预加载完成")
          return

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


# @title SIRIUS T1 BackTest Simulation (Pro Dynamic Edition)

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
    START_DATE = "2026-04-07"
    END_DATE = "2026-04-14"
    INITIAL_CASH = 100000.0
    DEBUG = False

    # SIRIUS 策略参数
    TRADE_RATIO = 0.5                # 资金使用比例
    SLIPPAGE = 0.002                 # 滑点容忍度 (0.2%)
    LOOKBACK_MINUTES = 30            # 计算均线的回溯分钟数
    BUY_THRESHOLD_PCT = -0.5         # 低于均线 X% 触发买入（负值）
    SELL_THRESHOLD_PCT = 0.5         # 高于均线 X% 触发卖出（正值）
    INTRADAY_SCAN_INTERVAL = 60      # 盘中扫描间隔（秒），在回测中对应分钟线频率
    INTRADAY_COOLDOWN_SEC = 300      # 同一股票动态交易冷却时间（秒）

    # 尾盘强制卖出时间
    FORCE_SELL_HOUR = 14
    FORCE_SELL_MINUTE = 50

    # 数据与模型路径
    MODEL_HISTORY_DIR = "./historical_models"
    DATA_CACHE_DIR = "./min_data_cache"
    MONTHLY_DIR = "./monthly_data"
    MODEL_NAME_PREFIX = "流入模型"

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
    TRADE_RECORD_FILE = os.path.join(OUTPUT_DIR, "trade_records.xlsx")
    DAILY_SNAPSHOT_FILE = os.path.join(OUTPUT_DIR, "daily_snapshots.xlsx")

# 创建必要目录
for d in [SimConfig.MODEL_HISTORY_DIR, SimConfig.DATA_CACHE_DIR, SimConfig.MONTHLY_DIR, SimConfig.OUTPUT_DIR]:
    os.makedirs(d, exist_ok=True)

# ========================= 2. 日志 =========================
logger = logging.getLogger("SIRIUS_Simulator")
if logger.handlers:
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)

logger.setLevel(logging.DEBUG if SimConfig.DEBUG else logging.INFO)
logger.propagate = False
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG if SimConfig.DEBUG else logging.INFO)
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
        return os.path.join(SimConfig.MONTHLY_DIR, f"minute_data_{year_month}.parquet")

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

    @staticmethod
    def _convert_code(code: str) -> str:
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
            logger.info(f"归档至 {year_month}.parquet")

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
        logger.info("预下载器启动")
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
                    if t1 < today_str: raw_pairs.add((MarketData._convert_code(t['code']), t1))
                    if t2 < today_str: raw_pairs.add((MarketData._convert_code(t['code']), t2))

        parquet_keys_set = set()
        for ym in set(d[:7] for _, d in raw_pairs):
            p_path = MarketData.get_monthly_file_path(ym)
            if os.path.exists(p_path):
                try:
                    df_p = pd.read_parquet(p_path, columns=['ts_code', 'trade_date'])
                    parquet_keys_set.update(set(zip(df_p['ts_code'].astype(str), df_p['trade_date'].astype(str))))
                except: pass

        last_month = None
        for ts_code, t_date in raw_pairs:
            if (ts_code, t_date) in parquet_keys_set: continue
            if os.path.exists(os.path.join(SimConfig.DATA_CACHE_DIR, f"{ts_code}_{t_date}.csv")): continue
            if last_month and t_date[:7] != last_month: MarketData.merge_monthly_data(last_month)
            last_month = t_date[:7]
            df = MarketData._fetch_intraday_from_api(ts_code.split('.')[0], t_date)
            if not df.empty:
                df.to_csv(os.path.join(SimConfig.DATA_CACHE_DIR, f"{ts_code}_{t_date}.csv"), index=False)
                logger.info(f"下载成功: {ts_code} ({t_date})")
            time_module.sleep(SimConfig.API_REQUEST_INTERVAL)
        if last_month: MarketData.merge_monthly_data(last_month)

# ========================= 4. 模拟账户 (含 T+1 限制) =========================
class MockAccount:
    def __init__(self, initial_cash):
        self.cash = initial_cash
        self.positions = {}        # code -> {volume, avg_price, can_sell, name}
        self.today_buys = set()    # 当日买入的股票代码

    def start_day(self):
        """每日开盘重置 T+1 标记，并将所有持仓设为可卖"""
        self.today_buys.clear()
        for code in self.positions:
            self.positions[code]['can_sell'] = self.positions[code].get('volume', 0)

    def order(self, date, time_v, code, side, vol, price, reason, name=""):
        vol = (vol // 100) * 100
        if vol <= 0:
            return False
        cost = vol * price
        display_name = f"{code}({name})" if name else code

        if side == 'buy' and self.cash >= cost:
            self.cash -= cost
            if code in self.positions:
                p = self.positions[code]
            else:
                p = {'volume': 0, 'avg_price': 0.0, 'can_sell': 0, 'name': name}
            total_cost = p['volume'] * p['avg_price'] + cost
            p['volume'] += vol
            p['avg_price'] = total_cost / p['volume']
            if 'name' not in p or not p['name']:
                p['name'] = name
            self.positions[code] = p
            self.today_buys.add(code)
            logger.info(f"💰 {date} {time_v} | 买入 {display_name} {vol}股 @{price:.2f} ({reason}) [T+1锁定]")
            return True

        elif side == 'sell':
            if code not in self.positions:
                logger.warning(f"⚠️ 卖出失败：未持有 {display_name}")
                return False
            if code in self.today_buys:
                logger.warning(f"⚠️ 卖出失败：{display_name} 当日买入，T+1不可卖")
                return False
            p = self.positions[code]
            available = p.get('can_sell', 0)
            if available < vol:
                logger.warning(f"⚠️ 卖出失败：{display_name} 可卖 {available} < {vol}")
                return False
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

# ========================= 5. SIRIUS 模拟执行器 =========================
class SiriusSimulator:
    def __init__(self, account):
        self.account = account
        self.today_trades = []      # 当日交易记录
        self.all_trades = []        # 所有交易记录
        self.daily_snapshots = []   # 每日快照
        self.last_dynamic_trade_time = {}   # 冷却字典

    def _get_name(self, code, targets):
        for t in targets:
            if t['code'] == code:
                return t.get('name', code)
        return code

    def _get_last_known_price(self, code: str, default_price: float) -> float:
        for snap in reversed(self.daily_snapshots):
            if snap.get('code') == code and snap.get('last_price', 0) > 0:
                return snap['last_price']
        return default_price

    def _check_limit(self, code: str, price: float, side: str, pre_close: float, name: str = "") -> bool:
        if pre_close is None or pre_close <= 0:
            return False
        limit_up, limit_down = MarketData.get_limit_prices(pre_close)
        if limit_up is None:
            return False
        display = f"{code}({name})" if name else code
        if side == 'buy' and price >= limit_up:
            logger.debug(f"{display} 涨停 {limit_up:.2f}，拒绝买入")
            return True
        if side == 'sell' and price <= limit_down:
            logger.debug(f"{display} 跌停 {limit_down:.2f}，拒绝卖出")
            return True
        return False

    def save_daily_snapshot(self, date_str):
        total_value = self.account.cash
        for code, pos in self.account.positions.items():
            df = MarketData.get_minute_data(code, date_str)
            if not df.empty:
                last_price = df.iloc[-1]['收盘']
            else:
                last_price = self._get_last_known_price(code, pos['avg_price'])
            market_value = pos['volume'] * last_price
            total_value += market_value
            self.daily_snapshots.append({
                'date': date_str, 'code': code, 'name': pos.get('name', code),
                'volume': pos['volume'], 'can_sell': pos.get('can_sell', 0),
                'avg_price': pos['avg_price'], 'last_price': last_price,
                'market_value': market_value, 'weight': 0.0
            })
        # 计算权重并添加现金/总计
        for snap in self.daily_snapshots:
            if snap['date'] == date_str:
                snap['weight'] = snap['market_value'] / total_value if total_value > 0 else 0
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

    def _calculate_dynamic_reference_price(self, df: pd.DataFrame, minutes: int) -> Optional[float]:
        """基于分钟线 DataFrame 计算最近 N 分钟均价"""
        if df is None or df.empty:
            return None
        # 取最近 minutes 条数据（分钟线）
        recent = df.tail(minutes)
        if len(recent) < 5:   # 数据不足时返回 None
            return None
        return float(recent['收盘'].mean())

    def simulate_day(self, date_str: str, targets: List[Dict], position_factor: float, pre_closes: Dict):
        """盘中动态交易 + 尾盘强制卖出（分钟级循环）"""
        self.account.start_day()
        self.today_trades.clear()

        # 1. 加载当日所有相关股票的分钟数据
        all_codes = set([t['code'] for t in targets] + list(self.account.positions.keys()))
        daily_data = {}
        for code in all_codes:
            df = MarketData.get_minute_data(code, date_str)
            if not df.empty:
                df = df.set_index('时间')
                daily_data[code] = df

        # 2. 计算当日目标股数（基于开盘后第一根有效K线，这里使用 09:31 价格）
        #    实际策略应基于实时资产动态调整，为简化，使用开盘后不久的价格计算一次目标仓位作为基准
        #    但盘中交易会基于最新价格实时调整目标股数，所以这里只用于强制卖出的基准计算
        #    我们沿用真实逻辑：目标股数 = effective_asset * weight / price
        #    使用 10:00 附近的价格计算初始基准资产
        dt_start = datetime.combine(datetime.strptime(date_str, "%Y-%m-%d").date(), time(9, 31))
        initial_prices = {}
        for code, df in daily_data.items():
            mask = df.index >= dt_start
            if mask.any():
                initial_prices[code] = df[mask]['收盘'].iloc[0]
            else:
                initial_prices[code] = pre_closes.get(code, 0)

        total_pos_value = 0.0
        for code, pos in self.account.positions.items():
            price = initial_prices.get(code, self._get_last_known_price(code, pos['avg_price']))
            total_pos_value += pos['volume'] * price
        total_asset = self.account.cash + total_pos_value
        effective_asset = total_asset * SimConfig.TRADE_RATIO * position_factor

        # 目标股数字典（基于当前资产和模型权重，盘中会动态刷新）
        def compute_target_volumes(current_asset):
            risk_asset = current_asset * SimConfig.TRADE_RATIO * position_factor
            target = {}
            for t in targets:
                price = initial_prices.get(t['code'], t['ref_price'])
                if price <= 0:
                    continue
                vol = int(risk_asset * t['weight'] / price / 100) * 100
                if vol > 0:
                    target[t['code']] = vol
            return target

        target_vols = compute_target_volumes(total_asset)

        # 3. 准备分钟时间轴（从 09:31 到 14:59）
        all_timestamps = set()
        for df in daily_data.values():
            all_timestamps.update(df[df.index >= dt_start].index)
        sorted_times = sorted(list(all_timestamps))

        forced_sell_triggered = False
        last_scan_ts = 0.0

        # 4. 逐分钟循环
        for current_dt in sorted_times:
            current_time = current_dt.time()
            now_ts = current_dt.timestamp()

            # ---- 尾盘强制卖出触发 ----
            if not forced_sell_triggered and current_time >= time(SimConfig.FORCE_SELL_HOUR, SimConfig.FORCE_SELL_MINUTE):
                forced_sell_triggered = True
                logger.info(f"{date_str} {current_time} 触发尾盘强制卖出")
                # 撤销未成交委托（模拟中无委托簿，直接执行强制卖出逻辑）
                # 重新计算目标股数（基于最新资产）
                # 获取最新资产（当前持仓市值 + 现金）
                current_total_asset = self.account.cash
                for code, pos in self.account.positions.items():
                    df = daily_data.get(code)
                    if df is not None and current_dt in df.index:
                        price = df.loc[current_dt, '收盘']
                        current_total_asset += pos['volume'] * price
                    else:
                        price = initial_prices.get(code, 0)
                        current_total_asset += pos['volume'] * price
                target_vols = compute_target_volumes(current_total_asset)

                # 执行强制卖出（超出目标部分，无价格下限）
                for code, pos in list(self.account.positions.items()):
                    target_vol = target_vols.get(code, 0)
                    if pos['volume'] > target_vol:
                        sell_vol = min(pos.get('can_sell', 0), pos['volume'] - target_vol)
                        if sell_vol <= 0:
                            continue
                        df = daily_data.get(code)
                        if df is None or current_dt not in df.index:
                            continue
                        price = df.loc[current_dt, '收盘']
                        if self._check_limit(code, price, 'sell', pre_closes.get(code, 0), pos.get('name', '')):
                            continue
                        reason = "尾盘强制卖出"
                        if self.account.order(date_str, current_time, code, 'sell', sell_vol, price, reason, pos.get('name', '')):
                            self.today_trades.append({
                                'date': date_str, 'time': current_time.strftime('%H:%M'),
                                'code': code, 'name': pos.get('name', code), 'side': 'sell',
                                'volume': sell_vol, 'price': price, 'reason': reason
                            })
                # 强制卖出后不再进行盘中动态交易，直接跳出循环
                break

            # ---- 盘中动态交易（每分钟扫描） ----
            if (now_ts - last_scan_ts) >= SimConfig.INTRADAY_SCAN_INTERVAL:
                last_scan_ts = now_ts

                # 实时更新目标股数（基于当前总资产）
                current_total_asset = self.account.cash
                for code, pos in self.account.positions.items():
                    df = daily_data.get(code)
                    if df is not None and current_dt in df.index:
                        price = df.loc[current_dt, '收盘']
                        current_total_asset += pos['volume'] * price
                    else:
                        price = initial_prices.get(code, 0)
                        current_total_asset += pos['volume'] * price
                target_vols = compute_target_volumes(current_total_asset)

                # 遍历每个标的，生成买卖信号
                for code in target_vols.keys():
                    # 获取实时价格
                    df = daily_data.get(code)
                    if df is None or current_dt not in df.index:
                        continue
                    real_price = df.loc[current_dt, '收盘']
                    # 计算动态参考价（N分钟均价）
                    dyn_price = self._calculate_dynamic_reference_price(df.loc[:current_dt], SimConfig.LOOKBACK_MINUTES)
                    if dyn_price is None or dyn_price <= 0:
                        continue

                    deviation = (real_price - dyn_price) / dyn_price * 100

                    # 冷却检查
                    last_trade = self.last_dynamic_trade_time.get(code, 0)
                    if now_ts - last_trade < SimConfig.INTRADAY_COOLDOWN_SEC:
                        continue

                    pos = self.account.positions.get(code, {})
                    current_vol = pos.get('volume', 0)
                    can_sell = pos.get('can_sell', 0)
                    avg_price = pos.get('avg_price', 0)

                    # 买入信号
                    if deviation <= SimConfig.BUY_THRESHOLD_PCT:
                        if real_price > dyn_price * (1 - SimConfig.SLIPPAGE):
                            continue
                        target_vol = target_vols.get(code, 0)
                        if target_vol > current_vol:
                            buy_vol = target_vol - current_vol
                            buy_vol = (buy_vol // 100) * 100
                            if buy_vol >= 100:
                                if self._check_limit(code, real_price, 'buy', pre_closes.get(code, 0), pos.get('name', '')):
                                    continue
                                if self.account.order(date_str, current_time, code, 'buy', buy_vol, real_price,
                                                      f"动态买入(偏离{deviation:.1f}%)", pos.get('name', '')):
                                    self.today_trades.append({
                                        'date': date_str, 'time': current_time.strftime('%H:%M'),
                                        'code': code, 'name': pos.get('name', code), 'side': 'buy',
                                        'volume': buy_vol, 'price': real_price,
                                        'reason': f"动态买入(偏离{deviation:.1f}%)"
                                    })
                                    self.last_dynamic_trade_time[code] = now_ts

                    # 卖出信号
                    elif deviation >= SimConfig.SELL_THRESHOLD_PCT:
                        if real_price < dyn_price * (1 + SimConfig.SLIPPAGE):
                            continue
                        if avg_price > 0 and real_price < avg_price * (1 + SimConfig.SLIPPAGE):
                            continue
                        target_vol = target_vols.get(code, 0)
                        excess = current_vol - target_vol
                        if excess > 0:
                            sell_vol = min(can_sell, excess)
                            sell_vol = (sell_vol // 100) * 100
                            if sell_vol >= 100:
                                if self._check_limit(code, real_price, 'sell', pre_closes.get(code, 0), pos.get('name', '')):
                                    continue
                                if self.account.order(date_str, current_time, code, 'sell', sell_vol, real_price,
                                                      f"动态卖出(偏离{deviation:.1f}%)", pos.get('name', '')):
                                    self.today_trades.append({
                                        'date': date_str, 'time': current_time.strftime('%H:%M'),
                                        'code': code, 'name': pos.get('name', code), 'side': 'sell',
                                        'volume': sell_vol, 'price': real_price,
                                        'reason': f"动态卖出(偏离{deviation:.1f}%)"
                                    })
                                    self.last_dynamic_trade_time[code] = now_ts

        # 保存当日交易记录
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

