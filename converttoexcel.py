# @title Convert Parquet File to Excel
import pandas as pd
import glob
import os


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
