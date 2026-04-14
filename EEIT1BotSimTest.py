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
    END_DATE = "2026-04-13"
    INITIAL_CASH = 100000.0

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

# 创建目录
for d in [SimConfig.MODEL_HISTORY_DIR, SimConfig.DATA_CACHE_DIR, SimConfig.MONTHLY_DIR]:
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
        """【修复点】返回月度 Parquet 文件的标准路径"""
        return os.path.join(SimConfig.MONTHLY_DIR, f"minute_data_{year_month}.parquet")

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
            targets = [{'code': item.get('代码', ''), 'weight': float(item.get('最优权重(%)', '0').replace('%',''))/100,
                        'ref_price': float(item.get('最近一日价格', 0))} for item in details if float(item.get('最优权重(%)', '0').replace('%','')) > 0]
            return targets, pos_factor
        except: return [], 1.0

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
        ts_code = MarketData._convert_code(code)
        monthly_file = MarketData.get_monthly_file_path(date_str[:7])
        df = pd.DataFrame()

        # 1. 优先从 Parquet 读取
        if os.path.exists(monthly_file):
            try:
                df = pd.read_parquet(monthly_file, filters=[('ts_code', '==', ts_code), ('trade_date', '==', date_str)])
            except: pass

        # 2. 备选从 CSV 读取
        if df.empty:
            cache_file = os.path.join(SimConfig.DATA_CACHE_DIR, f"{ts_code}_{date_str}.csv")
            if os.path.exists(cache_file):
                try: df = pd.read_csv(cache_file)
                except: pass

        # 3. 智能清洗列名
        if not df.empty:
            try:
                # 时间列识别
                if 'trade_time' in df.columns: df['时间'] = pd.to_datetime(df['trade_time'])
                elif 'date' in df.columns and 'time' in df.columns:
                    df['时间'] = pd.to_datetime(df['date'].astype(str) + " " + df['time'].astype(str))
                elif 'time' in df.columns:
                    df['时间'] = pd.to_datetime(date_str + " " + df['time'].astype(str))
                elif '时间' in df.columns: df['时间'] = pd.to_datetime(df['时间'])

                # 价格列识别
                if 'close' in df.columns: df = df.rename(columns={'close': '收盘'})
                elif 'price' in df.columns: df = df.rename(columns={'price': '收盘'})

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
        self.cash, self.positions = initial_cash, {}

    def start_day(self):
        # 每天开盘，将持仓量同步到可卖量
        for c in self.positions:
            self.positions[c]['can_sell'] = self.positions[c]['volume']

    def order(self, date, time_v, code, side, vol, price, reason):
        vol = (vol // 100) * 100
        if vol <= 0: return False
        cost = vol * price

        if side == 'buy' and self.cash >= cost:
            self.cash -= cost
            # --- 修正点 1：初始化时增加 can_sell: 0 ---
            p = self.positions.get(code, {'volume': 0, 'avg_price': 0.0, 'can_sell': 0})

            p['avg_price'] = (p['volume'] * p['avg_price'] + cost) / (p['volume'] + vol)
            p['volume'] += vol

            # 确保即使是已存在的持仓，如果没这个键也补上（防错）
            if 'can_sell' not in p: p['can_sell'] = 0

            self.positions[code] = p
            logger.info(f"💰 {date} {time_v} | 买入 {code} {vol}股 @{price:.2f} ({reason})")
            return True

        elif side == 'sell':
            p = self.positions.get(code)
            # --- 修正点 2：增加安全获取，防止 KeyError ---
            if p and p.get('can_sell', 0) >= vol:
                self.cash += cost
                p['volume'] -= vol
                p['can_sell'] -= vol
                if p['volume'] <= 0: del self.positions[code]
                logger.info(f"💰 {date} {time_v} | 卖出 {code} {vol}股 @{price:.2f} ({reason})")
                return True
        return False

# ========================= 严格对照执行器 =========================
class SiriusStrictExecutor:
    def __init__(self, account):
        self.account = account

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
        total_asset = self.account.cash + sum(p['volume'] * prices_1000.get(c, p['avg_price']) for c, p in self.account.positions.items())
        effective_asset = total_asset * SimConfig.TRADE_RATIO * pos_factor

        target_vols = {t['code']: int(effective_asset * t['weight'] / t['ref_price'] / 100) * 100 for t in targets}

        # --- 生成调仓指令 ---
        # A. 卖出指令 (约束：价格 >= 昨收)
        for code, pos in list(self.account.positions.items()):
            t_vol = target_vols.get(code, 0)
            if pos['volume'] > t_vol:
                sell_vol = pos['can_sell'] # 早盘只卖 T+1 的部分
                if sell_vol > 0:
                    real_p = prices_1000.get(code)
                    pre_close = pre_closes.get(code, pos['avg_price'])
                    if real_p and real_p >= pre_close: # 严格对照：正常时段受昨收约束
                        self.account.order(date_str, trade_time_morning, code, 'sell', sell_vol, real_p, "早盘止盈")

        # B. 买入指令 (约束：价格 <= 基准价)
        available_cash = self.account.cash * SimConfig.TRADE_RATIO
        for code, t_vol in target_vols.items():
            cur_vol = self.account.positions.get(code, {}).get('volume', 0)
            if t_vol > cur_vol:
                buy_vol = t_vol - cur_vol
                real_p = prices_1000.get(code)
                ref_p = next(t['ref_price'] for t in targets if t['code'] == code)
                if real_p and real_p <= ref_p: # 严格对照：买入受基准价约束
                    exec_p = min(real_p, ref_p)
                    if self.account.cash >= buy_vol * exec_p:
                        self.account.order(date_str, trade_time_morning, code, 'buy', buy_vol, exec_p, "早盘调仓")

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
    # 数据预载 (保持不变)
    if SimConfig.ENABLE_PRELOAD:
        MarketData.preload_from_models(SimConfig.START_DATE, SimConfig.END_DATE)

    account = MockAccount(SimConfig.INITIAL_CASH)
    executor = SiriusStrictExecutor(account)
    dates = MarketData.get_model_dates(SimConfig.START_DATE, SimConfig.END_DATE)

    for d_str in dates:
        logger.info(f"========== {d_str} ==========")
        # 1. 加载模型
        model_file = os.path.join(SimConfig.MODEL_HISTORY_DIR, f"{SimConfig.MODEL_NAME_PREFIX}_{d_str}.json")
        with open(model_file, 'r', encoding='utf-8') as f:
            targets, pf = MarketData.parse_sirius_model(json.load(f))

        # 2. 准备昨收价
        pre_closes = {t['code']: t['ref_price'] for t in targets}

        # 3. 运行严格仿真
        executor.simulate_day(d_str, targets, pf, pre_closes)

        # 4. 结算 (取当日收盘价计算净值)
        v = 0
        for c, p in account.positions.items():
            df = MarketData.get_minute_data(c, d_str)
            last_p = df.iloc[-1]['收盘'] if not df.empty else p['avg_price']
            v += p['volume'] * last_p
        logger.info(f"Day End Asset: {account.cash + v:.2f}")

    logger.info("回测完成！")

if __name__ == "__main__":
    run_strict_backtest()


# @title SIRIUS T1 BackTest Simulation (Pro State-Machine Edition)
import os
import json
import logging
import pandas as pd
from datetime import datetime, time, timedelta, timezone
import time as time_module
import glob
import requests
import random

# ========================= 1. 配置 (对齐 Real Pro 参数) =========================
class SimConfig:
    START_DATE = "2026-04-07"
    END_DATE = "2026-04-13"
    INITIAL_CASH = 100000.0

    # SIRIUS Pro 核心择时参数
    BUY_REBOUND_RATIO = 0.0062      # 买入反弹阈值
    SELL_DROP_RATIO = 0.0038        # 卖出回落阈值
    TRADE_RATIO = 0.5               # 资金使用比例
    FORCE_DEADLINE_TIME = time(14, 50) # 14:50 强制收网
    FORCE_SELL_PRICE_RATIO = 0.995  # 强平保护价
    
    # 基础设施
    MODEL_HISTORY_DIR = "./historical_models"
    DATA_CACHE_DIR = "./min_data_cache"
    MONTHLY_DIR = "./monthly_data"
    MODEL_NAME_PREFIX = "流入模型"

    API_BASE_URL = "https://query.aivibeinvestment.com/api/query"
    API_REQUEST_INTERVAL = 0.3
    ENABLE_PRELOAD = True

# ========================= 2. 日志 (防重复打印) =========================
logger = logging.getLogger("SIRIUS_Pro_Sim")
if logger.handlers:
    for handler in logger.handlers[:]: logger.removeHandler(handler)
logger.setLevel(logging.DEBUG)
logger.propagate = False
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
ch = logging.StreamHandler()
ch.setFormatter(formatter)
logger.addHandler(ch)

# ========================= 3. 数据模块 (完全复刻 Parquet Master) =========================
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
            targets = [{'code': item.get('代码', ''), 'weight': float(item.get('最优权重(%)', '0').replace('%',''))/100, 
                        'ref_price': float(item.get('最近一日价格', 0))} for item in details if float(item.get('最优权重(%)', '0').replace('%','')) > 0]
            return targets, pos_factor
        except: return [], 1.0

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
        ts_code = MarketData._convert_code(code)
        monthly_file = MarketData.get_monthly_file_path(date_str[:7])
        df = pd.DataFrame()

        # 1. 优先从 Parquet 读取
        if os.path.exists(monthly_file):
            try:
                df = pd.read_parquet(monthly_file, filters=[('ts_code', '==', ts_code), ('trade_date', '==', date_str)])
            except: pass

        # 2. 备选从 CSV 读取
        if df.empty:
            cache_file = os.path.join(SimConfig.DATA_CACHE_DIR, f"{ts_code}_{date_str}.csv")
            if os.path.exists(cache_file):
                try: df = pd.read_csv(cache_file)
                except: pass

        # 3. 智能清洗列名
        if not df.empty:
            try:
                # 时间列识别
                if 'trade_time' in df.columns: df['时间'] = pd.to_datetime(df['trade_time'])
                elif 'date' in df.columns and 'time' in df.columns:
                    df['时间'] = pd.to_datetime(df['date'].astype(str) + " " + df['time'].astype(str))
                elif 'time' in df.columns:
                    df['时间'] = pd.to_datetime(date_str + " " + df['time'].astype(str))
                elif '时间' in df.columns: df['时间'] = pd.to_datetime(df['时间'])
                
                # 价格列识别
                if 'close' in df.columns: df = df.rename(columns={'close': '收盘'})
                elif 'price' in df.columns: df = df.rename(columns={'price': '收盘'})
                
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

# ========================= 4. 虚拟账户 (T+1 逻辑) =========================
class MockAccount:
    def __init__(self, initial_cash):
        self.cash, self.positions = initial_cash, {}

    def start_day(self):
        for c in self.positions: self.positions[c]['can_sell'] = self.positions[c]['volume']

    def order(self, date, time_v, code, side, vol, price, reason):
        vol = (vol // 100) * 100
        if vol <= 0: return False
        if side == 'buy':
            if self.cash >= vol * price:
                self.cash -= vol * price
                p = self.positions.get(code, {'volume': 0, 'avg_price': 0.0, 'can_sell': 0})
                p['avg_price'] = (p['volume'] * p['avg_price'] + vol * price) / (p['volume'] + vol)
                p['volume'] += vol
                self.positions[code] = p
                logger.info(f"💰 {date} {time_v} | 买入 {code} {vol}股 @{price:.2f} ({reason})")
                return True
        elif side == 'sell':
            p = self.positions.get(code)
            if p and p.get('can_sell', 0) >= vol:
                self.cash += vol * price
                p['volume'] -= vol
                p['can_sell'] -= vol
                if p['volume'] <= 0: del self.positions[code]
                logger.info(f"💰 {date} {time_v} | 卖出 {code} {vol}股 @{price:.2f} ({reason})")
                return True
        return False

# ========================= 5. Pro 版仿真执行器 (状态机轮询版) =========================
class SiriusProExecutor:
    def __init__(self, account):
        self.account = account

    def simulate_day(self, date_str, targets, pos_factor, pre_closes):
        """模拟 Pro 版的非阻塞全局扫描大循环"""
        self.account.start_day()
        
        # 1. 初始化调仓目标 (对应 Pro 版信号生成)
        stock_val = sum(p['volume'] * pre_closes.get(c, p['avg_price']) for c,p in self.account.positions.items())
        total_asset = self.account.cash + stock_val
        effective_asset = total_asset * SimConfig.TRADE_RATIO * pos_factor
        
        # 建立任务字典 (状态机)
        sell_tasks = {}
        target_vols = {}
        for t in targets:
            target_vols[t['code']] = int(effective_asset * t['weight'] / t['ref_price'] / 100) * 100
            
        # 初始化卖出任务 (对比当前持仓 vs 目标)
        for code, pos in self.account.positions.items():
            t_vol = target_vols.get(code, 0)
            if pos['volume'] > t_vol:
                sell_tasks[code] = {
                    'target': pos['volume'] - t_vol,
                    'sold': 0,
                    'high_price': -float('inf'),
                    'pre_close': pre_closes.get(code, pos['avg_price']),
                    'done': False
                }
        
        # 初始化买入任务
        buy_tasks = {}
        for code, t_vol in target_vols.items():
            cur_v = self.account.positions.get(code, {}).get('volume', 0)
            if t_vol > cur_v:
                buy_tasks[code] = {
                    'target': t_vol - cur_v,
                    'bought': 0,
                    'low_price': float('inf'),
                    'ref_price': next(t['ref_price'] for t in targets if t['code']==code),
                    'done': False
                }

        # 2. 模拟盘中扫描 (10:00 - 15:00)
        all_codes = list(set(list(sell_tasks.keys()) + list(buy_tasks.keys())))
        min_dict = {c: MarketData.get_minute_data(c, date_str) for c in all_codes}
        
        # 获取时间轴
        time_axis = []
        for c in min_dict:
            if not min_dict[c].empty: 
                time_axis = min_dict[c]['时间'].tolist()
                break
        
        logger.info(f"--- 盘中择时启动：监控 {len(buy_tasks)} 买 / {len(sell_tasks)} 卖 ---")

        for curr_dt in time_axis:
            curr_t = curr_dt.time()
            if curr_t < time(10, 0): continue
            
            # 14:50 触发收网，跳出动态择时循环
            if curr_t >= SimConfig.FORCE_DEADLINE_TIME:
                break

            # --- 全局状态机轮询模拟 ---
            # 检查卖出任务
            for code, task in sell_tasks.items():
                if task['done']: continue
                df = min_dict[code]
                row = df[df['时间'] == curr_dt]
                if row.empty: continue
                price = row.iloc[0]['收盘']
                
                if price > task['high_price']: task['high_price'] = price
                
                # 触发条件：价格 >= 昨收 且 回落触发
                if price >= task['pre_close'] and price <= task['high_price'] * (1 - SimConfig.SELL_DROP_RATIO):
                    if self.account.order(date_str, curr_t, code, 'sell', task['target'], price, "Pro择时卖出"):
                        task['done'] = True
            
            # 检查买入任务
            for code, task in buy_tasks.items():
                if task['done']: continue
                df = min_dict[code]
                row = df[df['时间'] == curr_dt]
                if row.empty: continue
                price = row.iloc[0]['收盘']
                
                if price < task['low_price']: task['low_price'] = price
                
                # 触发条件：反弹触发 且 价格 <= 基准价
                if price >= task['low_price'] * (1 + SimConfig.BUY_REBOUND_RATIO) and price <= task['ref_price']:
                    if self.account.order(date_str, curr_t, code, 'buy', task['target'], price, "Pro择时买入"):
                        task['done'] = True

        # --- 3. 尾盘强制兜底 (14:50) ---
        logger.info(f"--- 14:50 尾盘兜底时刻 ---")
        for code, task in sell_tasks.items():
            if not task['done']:
                df = min_dict[code]
                price = df.iloc[-1]['收盘'] if not df.empty else task['pre_close']
                exec_p = max(price, task['pre_close'] * SimConfig.FORCE_SELL_PRICE_RATIO)
                self.account.order(date_str, "14:50", code, 'sell', task['target'], exec_p, "Pro尾盘强清")
                
        for code, task in buy_tasks.items():
            if not task['done']:
                df = min_dict[code]
                price = df.iloc[-1]['收盘'] if not df.empty else task['ref_price']
                if price <= task['ref_price']:
                    self.account.order(date_str, "14:50", code, 'buy', task['target'], price, "Pro尾盘补仓")

# ========================= 6. 主引擎 =========================
def run_backtest():
    # 数据预下载 (略过具体代码，假设MarketData已处理)
    account = MockAccount(SimConfig.INITIAL_CASH)
    executor = SiriusProExecutor(account)
    dates = MarketData.get_model_dates(SimConfig.START_DATE, SimConfig.END_DATE)
    
    for d_str in dates:
        logger.info(f"========== {d_str} ==========")
        model_file = os.path.join(SimConfig.MODEL_HISTORY_DIR, f"{SimConfig.MODEL_NAME_PREFIX}_{d_str}.json")
        with open(model_file, 'r', encoding='utf-8') as f:
            targets, pf = MarketData.parse_sirius_model(json.load(f))
        
        # 运行仿真
        executor.simulate_day(d_str, targets, pf, {t['code']: t['ref_price'] for t in targets})
        
        # 日结统计
        v = sum(p['volume'] * (MarketData.get_minute_data(c, d_str).iloc[-1]['收盘'] if not MarketData.get_minute_data(c, d_str).empty else p['avg_price']) for c,p in account.positions.items())
        logger.info(f"Day End Asset: {account.cash + v:.2f}")

    logger.info("回测完成！")

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

