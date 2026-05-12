import requests
import os

# ========================= 配置 =========================
# GitHub raw 文件地址（不是 blob 页面）
GITHUB_RAW_URL = "https://raw.githubusercontent.com/digital-era/AIPEBot/main/load1mdataqmt.py"
OUTPUT_FILE = "load1mdataqmt.py"

# 代理配置（与之前代码保持一致）
HTTP_PROXY = 'http://127.0.0.1:7897'
HTTPS_PROXY = 'http://127.0.0.1:7897'
PROXIES = {}
if HTTP_PROXY:
    PROXIES['http'] = HTTP_PROXY
if HTTPS_PROXY:
    PROXIES['https'] = HTTPS_PROXY

# ========================= 下载逻辑 =========================
def download_and_overwrite():
    try:
        print(f"正在下载: {GITHUB_RAW_URL}")
        resp = requests.get(
            GITHUB_RAW_URL, 
            proxies=PROXIES, 
            timeout=30,
            headers={'User-Agent': 'Mozilla/5.0'}
        )
        resp.raise_for_status()
        
        # 直接覆盖写入当前目录
        with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
            f.write(resp.text)
        
        abs_path = os.path.abspath(OUTPUT_FILE)
        print(f"✅ 下载成功，已覆盖: {abs_path}")
        print(f"   文件大小: {len(resp.text)} 字符")
        return True
        
    except requests.exceptions.ProxyError as e:
        print(f"❌ 代理错误: {e}，请检查代理是否可用")
    except requests.exceptions.Timeout:
        print(f"❌ 请求超时，请检查网络连接")
    except requests.exceptions.HTTPError as e:
        print(f"❌ HTTP 错误: {e}")
    except Exception as e:
        print(f"❌ 下载失败: {e}")
    
    return False

if __name__ == "__main__":
    download_and_overwrite()
