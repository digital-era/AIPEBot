# @title Get and Commit FlowInfoBase.json, HKFlowInfoBase.json and EEIFlow30Days.xlsx to OSS
# @title Get and Commit FlowInfoBase.json, HKFlowInfoBase.json and EEIFlow30Days.xlsx to OSS

import requests
from pathlib import Path

# ==================== 配置 ====================
files_to_download = [
    {
        "url": "https://raw.githubusercontent.com/digital-era/AIPEQModel/main/month/EEIFlow30Days.xlsx",
        "save_as": "/content/EEIFlow30Days.xlsx"
    },
    {
        "url": "https://raw.githubusercontent.com/digital-era/AIPEHotTracker/main/data/FlowInfoBase.json",
        "save_as": "/content/FlowInfoBase.json"
    },
    {
        "url": "https://raw.githubusercontent.com/digital-era/AIPEHotTracker/main/data/HKFlowInfoBase.json",
        "save_as": "/content/HKFlowInfoBase.json"
    }
]

# ==================== 下载函数 ====================
def download_file(url, save_path):
    try:
        print(f"正在下载: {save_path}")
        response = requests.get(url, timeout=30)
        response.raise_for_status()  # 检查请求是否成功
        
        # 核心修复：以二进制模式(wb)写入，使用 response.content
        with open(save_path, 'wb') as f:
            f.write(response.content)
        
        print(f"✅ 下载完成: {save_path}  ({len(response.content)/1024:.1f} KB)")
        return True
        
    except Exception as e:
        print(f"❌ 下载失败 {save_path}: {e}")
        return False

# ==================== 执行下载 ====================
print("开始下载 AIPEHotTracker 数据文件...\n")

success_count = 0
for file in files_to_download:
    if download_file(file["url"], file["save_as"]):
        success_count += 1

print(f"\n下载完成！成功下载 {success_count}/{len(files_to_download)} 个文件。")

# 显示当前目录下的文件（包括 json 和 xlsx）
print("\n当前目录文件列表：")
for p in Path("/content").glob("*.*"):
    if p.suffix in ['.json', '.xlsx']:
        size = p.stat().st_size / 1024
        print(f"   • {p.name}  ({size:.1f} KB)")



# 1. 安装阿里云 OSS SDK (Colab 默认不包含)
!pip install oss2

import os
import datetime
import shutil
import subprocess
import pandas as pd
import json
import oss2

# ================= 配置信息 =================
ACCESS_KEY_ID = ''
ACCESS_KEY_SECRET = ''
ENDPOINT = 'http://oss-cn-hangzhou.aliyuncs.com'  # 例如：oss-cn-shanghai.aliyuncs.com
BUCKET_NAME = 'aiep-users'

auth = oss2.Auth(ACCESS_KEY_ID, ACCESS_KEY_SECRET)
bucket = oss2.Bucket(auth, ENDPOINT, BUCKET_NAME)

print("正在上传FlowInfoBase.json到 OSS...")
try:
  # ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
  # 优雅新增：检查并上传 MarketDate.json
  if os.path.exists('/content/FlowInfoBase.json'):
      res_data = bucket.put_object_from_file('FlowInfoBase.json', '/content/FlowInfoBase.json')
      if res_data.status == 200:
          print(f"上传FlowInfoBase成功! 文件位置: oss://{BUCKET_NAME}/FlowInfoBase.json")
  # ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

except oss2.exceptions.OssError as e:
  print(f"OSS 上传FlowInfoBase发生错误: {e}")


print("正在上传HKFlowInfoBase.json到 OSS...")
try:
  # ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
  # 优雅新增：检查并上传 MarketDate.json
  if os.path.exists('/content/HKFlowInfoBase.json'):
      res_data = bucket.put_object_from_file('HKFlowInfoBase.json', '/content/HKFlowInfoBase.json')
      if res_data.status == 200:
          print(f"上传HKFlowInfoBase成功! 文件位置: oss://{BUCKET_NAME}/HKFlowInfoBase.json")
  # ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

except oss2.exceptions.OssError as e:
  print(f"OSS 上传HKFlowInfoBase.json发生错误: {e}")


print("正在上传EEIFlow30Days.xlsx到 OSS...")
try:
  # ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
  # 优雅新增：检查并上传 MarketDate.json
  if os.path.exists('/content/EEIFlow30Days.xlsx'):
      res_data = bucket.put_object_from_file('EEIFlow30Days.xlsx', '/content/EEIFlow30Days.xlsx')
      if res_data.status == 200:
          print(f"上传EEIFlow30Days.xlsx成功! 文件位置: oss://{BUCKET_NAME}/EEIFlow30Days.xlsx")
  # ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

except oss2.exceptions.OssError as e:
  print(f"OSS 上传EEIFlow30Days.xlsx发生错误: {e}")
