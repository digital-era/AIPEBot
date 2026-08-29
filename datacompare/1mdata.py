# ============================================================
# 在 Google Colab 中运行以下代码
# ============================================================

# 1. 安装依赖（若在 Colab 中运行，取消下面一行的注释）
# !pip install pandas openpyxl requests

import requests
import pandas as pd
from datetime import datetime

# ============================================================
# 2. 调用东方财富 HTTP API 获取 1 分钟 K 线数据
# ============================================================

def get_minute_kline(code, date, period=1, limit=750):
    """
    通过东方财富 API 获取指定日期的分钟 K 线数据
    
    参数:
        code: 股票代码，如 '002415'
        date: 日期，如 '20260828'
        period: K线周期，1=1分钟，5=5分钟，15=15分钟，30=30分钟，60=60分钟
        limit: 获取条数，最多约750条
    返回:
        DataFrame: 包含分钟K线数据
    """
    # 构建 secid：深市以 0. 开头，沪市以 1. 开头
    # 002415 为深市股票，所以 secid = '0.002415'
    if code.startswith('6'):
        secid = f'1.{code}'
    else:
        secid = f'0.{code}'
    
    # API 请求地址与参数
    url = 'https://push2his.eastmoney.com/api/qt/stock/kline/get'
    
    # fields1: 元数据字段
    # fields2: K线数据字段
    #   f51 = 时间, f52 = 开盘, f53 = 收盘, f54 = 最高, f55 = 最低,
    #   f56 = 成交量, f57 = 成交额, f58 = 振幅, f59 = 涨跌幅,
    #   f60 = 涨跌额, f61 = 换手率
    params = {
        'secid': secid,
        'fields1': 'f1,f2,f3,f4,f5,f6',
        'fields2': 'f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61',
        'klt': period,          # K线类型：1=1分钟
        'fqt': 1,               # 复权类型：1=前复权
        'end': date,            # 截止日期：20260828
        'lmt': limit,           # 获取条数
    }
    
    # 发送请求
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Referer': 'https://quote.eastmoney.com/'
    }
    
    response = requests.get(url, params=params, headers=headers)
    data = response.json()
    
    # 检查返回数据
    if data.get('data') is None:
        print(f'请求失败: {data}')
        return None
    
    klines = data['data'].get('klines', [])
    if not klines:
        print('未获取到数据，请检查日期是否正确（仅支持近5个交易日）')
        return None
    
    # 解析 K 线数据
    rows = []
    for kline in klines:
        parts = kline.split(',')
        # parts 顺序对应 fields2: f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61
        rows.append({
            'time': parts[0],           # f51: 日期时间
            'open': float(parts[1]),    # f52: 开盘价
            'close': float(parts[2]),   # f53: 收盘价
            'high': float(parts[3]),    # f54: 最高价
            'low': float(parts[4]),     # f55: 最低价
            'volume': float(parts[5]),  # f56: 成交量
            'amount': float(parts[6]),  # f57: 成交额
            'amplitude': float(parts[7]),   # f58: 振幅
            'change_pct': float(parts[8]),  # f59: 涨跌幅
            'change': float(parts[9]),      # f60: 涨跌额
            'turnover': float(parts[10]),   # f61: 换手率
        })
    
    df = pd.DataFrame(rows)
    
    # 分割日期和时间
    df['date'] = df['time'].str.split(' ').str[0]
    df['time_only'] = df['time'].str.split(' ').str[1]
    
    # 重新排列列顺序，匹配用户要求的输出格式
    df = df[[
        'date', 'time_only', 'open', 'close', 'high', 'low', 
        'volume', 'amount', 'amplitude', 'change_pct', 'change', 'turnover'
    ]]
    df.columns = ['date', 'time', 'open', 'close', 'high', 'low', 
                  'volume', 'amount', 'amplitude', 'change_pct', 'change', 'turnover']
    
    return df

# ============================================================
# 3. 执行获取
# ============================================================

# 获取 002415 在 2026-08-28 的 1 分钟 K 线数据
df = get_minute_kline(
    code='002415',
    date='20260828',
    period=1,
    limit=750
)

# 查看数据概览
print(f'共获取 {len(df)} 条数据')
print('\n前5条数据:')
print(df.head())

print('\n数据列信息:')
print(df.info())

# ============================================================
# 4. 导出为 Excel 文件
# ============================================================

# 保存为 Excel 文件（.xlsx）
excel_filename = '002415_20260828_1min_kline.xlsx'
df.to_excel(excel_filename, index=False, sheet_name='1分钟K线')

print(f'\n✅ 数据已保存至: {excel_filename}')

# 若在 Colab 中运行，可下载文件到本地
from google.colab import files
files.download(excel_filename)
