# QMTAPISERVER.py
# Windows 本地 Flask 服务 - 大QMT数据服务桥接版（最终稳定版）
from flask import Flask, request, jsonify
from flask_cors import CORS
import datetime
import time
import re
import threading
import json
import os
from typing import Dict, List, Tuple, Any

app = Flask(__name__)
CORS(app, origins=[
    "https://aivibeinvestment.com",
    "http://localhost:3000",
    "http://127.0.0.1:3000",
    "http://localhost:8080",
    "http://127.0.0.1:8080"
])

# ==============================
# 配置 (路径必须与QMT策略完全一致)
# ==============================
QMT_COMMAND_FILE = r"E:\AIPEQModelSIRIUS\QMTAPI\qmt_command.txt"
QMT_RESPONSE_FILE = r"E:\AIPEQModelSIRIUS\QMTAPI\qmt_response.txt"
REQUEST_TIMEOUT = 25          # 建议 20~30 秒
MAX_RETRIES = 2
RETRY_DELAY = 1.5

# 全局变量 + 锁
response_event = threading.Event()
response_data = None
last_response_time = 0.0
response_lock = threading.Lock()
current_request_id = None

# ==============================
# 代码转换
# ==============================
def convert_code(code: str) -> Tuple[str, str]:
    code_upper = code.upper().strip()
    if code_upper.startswith("HK"):
        pure = code_upper.replace("HK", "")
        return f"{pure}.HK", "HKD"
    elif re.match(r"^(60|68|51|56|58|55|900)", code):
        return f"{code}.SH", "CNY"
    elif re.match(r"^(00|30|15|200)", code):
        return f"{code}.SZ", "CNY"
    elif re.match(r"^(4|8)", code):
        return f"{code}.BJ", "CNY"
    else:
        return None, None

def json_response(data: Dict, status: int = 200):
    response = jsonify(data)
    response.status_code = status
    response.headers["Content-Type"] = "application/json"
    response.headers["Cache-Control"] = "max-age=5, stale-while-revalidate=10"
    return response

# ==============================
# QMT 通信（核心修复）
# ==============================
def send_qmt_request(request_data: Dict) -> Any:
    global response_data, response_event, last_response_time, current_request_id

    request_id = f"req_{int(time.time() * 1000)}"
    request_data["request_id"] = request_id

    for attempt in range(MAX_RETRIES):
        try:
            # 清理旧响应（防止读到脏数据）
            for f in [QMT_RESPONSE_FILE, QMT_RESPONSE_FILE + '.notify']:
                if os.path.exists(f):
                    try:
                        os.remove(f)
                    except:
                        pass

            with response_lock:
                response_event.clear()
                response_data = None
                current_request_id = request_id

            # 写入命令文件
            with open(QMT_COMMAND_FILE, 'w', encoding='utf-8') as f:
                json.dump(request_data, f, ensure_ascii=False, indent=2)
                f.flush()
                os.fsync(f.fileno())

            # 写入通知文件
            with open(QMT_COMMAND_FILE + '.notify', 'w', encoding='utf-8') as f:
                f.write(str(time.time()))
                f.flush()
                os.fsync(f.fileno())

            print(f"[DEBUG] 已发送命令: {request_data.get('function')} | id={request_id}")

            # 等待响应
            start_time = time.time()
            while time.time() - start_time < REQUEST_TIMEOUT:
                if response_event.wait(timeout=0.15):
                    with response_lock:
                        data = response_data
                        response_event.clear()

                    if data is None:
                        continue

                    # 校验 request_id
                    if isinstance(data, dict) and data.get('request_id') == request_id:
                        last_response_time = time.time()
                        # 自动解包 data 字段
                        if 'data' in data:
                            return data['data']
                        return data
                    else:
                        print(f"[WARN] 收到不匹配的响应 id，期望 {request_id}")
                        continue

            if attempt < MAX_RETRIES - 1:
                print(f"[WARN] QMT request timeout, retrying... (attempt {attempt + 1}/{MAX_RETRIES})")
                time.sleep(RETRY_DELAY)
            else:
                raise TimeoutError(f"QMT request timeout: {request_data.get('function')}")
        except Exception as e:
            if attempt < MAX_RETRIES - 1:
                print(f"[ERROR] send_qmt_request failed (attempt {attempt + 1}): {e}")
                time.sleep(RETRY_DELAY)
            else:
                raise

    raise TimeoutError(f"QMT request failed: {request_data.get('function')}")

def response_monitor():
    global response_data, response_event, last_response_time
    print("[MONITOR] 响应监控线程已启动")
    while True:
        try:
            notify_file = QMT_RESPONSE_FILE + '.notify'
            if os.path.exists(notify_file) and os.path.exists(QMT_RESPONSE_FILE):
                try:
                    with open(QMT_RESPONSE_FILE, 'r', encoding='utf-8') as f:
                        data = json.load(f)

                    with response_lock:
                        response_data = data
                        response_event.set()
                        last_response_time = time.time()

                    # 清理文件
                    try:
                        os.remove(QMT_RESPONSE_FILE)
                        os.remove(notify_file)
                    except Exception as e:
                        print(f"[WARN] 清理响应文件失败: {e}")

                    print(f"[MONITOR] 收到响应: {data.get('request_id', 'unknown')} | status={data.get('status')}")
                except Exception as e:
                    print(f"[WARN] 读取响应失败: {e}")
                    # 失败也尝试清理，防止死循环
                    try:
                        if os.path.exists(QMT_RESPONSE_FILE):
                            os.remove(QMT_RESPONSE_FILE)
                        if os.path.exists(notify_file):
                            os.remove(notify_file)
                    except:
                        pass
        except Exception as e:
            print(f"[WARN] response_monitor error: {e}")
        time.sleep(0.08)

# ==============================
# 数据获取逻辑
# ==============================
def get_last_trade_date() -> str:
    now = datetime.datetime.now()
    today_str = now.strftime("%Y%m%d")
    try:
        trading_dates = send_qmt_request({
            "function": "get_trading_dates",
            "params": {"market": "SH", "start_time": "20260101", "end_time": today_str, "count": -1}
        })
        if trading_dates and len(trading_dates) > 0:
            # QMT 返回的是时间戳（毫秒）或日期字符串，做兼容
            last = trading_dates[-1]
            if isinstance(last, (int, float)):
                return time.strftime('%Y%m%d', time.localtime(last / 1000 if last > 1e12 else last))
            return str(last).replace('-', '')[:8]
    except Exception as e:
        print(f"[WARN] get_trading_dates failed: {e}")

    weekday = now.weekday()
    days_back = 3 if weekday == 0 else (2 if weekday == 6 else 1)
    return (now - datetime.timedelta(days=days_back)).strftime("%Y%m%d")

def fetch_price_single(qmt_code: str, orig_code: str, currency: str) -> Dict:
    try:
        result = send_qmt_request({"function": "get_full_tick", "params": {"stock_codes": [qmt_code]}})
        if not result or qmt_code not in result:
            return None

        tick_data = result[qmt_code]
        latest_price = tick_data.get("lastPrice")
        prev_close = tick_data.get("lastClose")
        if latest_price is None or prev_close is None:
            return None

        name = orig_code
        try:
            detail = send_qmt_request({
                "function": "get_instrument_detail",
                "params": {"stock_code": qmt_code, "iscomplete": False}
            })
            if detail and detail.get("InstrumentName"):
                name = detail["InstrumentName"]
        except:
            pass

        change_amount = latest_price - prev_close
        change_percent = round((change_amount / prev_close) * 100, 6) if prev_close else 0.0
        return {
            "name": name,
            "latestPrice": latest_price,
            "changePercent": change_percent,
            "changeAmount": change_amount,
            "source": "qmt",
            "currency": currency,
            "dailydata": None
        }
    except Exception as e:
        print(f"[ERROR] fetch_price_single failed: {e}")
        return None

def fetch_intraday_single(qmt_code: str, orig_code: str, trade_date: str, prev_close: float) -> List[Dict]:
    try:
        # 先下载
        send_qmt_request({
            "function": "download_history_data",
            "params": {"stock_code": qmt_code, "period": "1m", "start_time": trade_date, "end_time": trade_date}
        })
        time.sleep(0.3)  # 给一点下载时间

        result = send_qmt_request({
            "function": "get_market_data_ex",
            "params": {
                "fields": ['time', 'open', 'high', 'low', 'close', 'volume', 'amount'],
                "stock_codes": [qmt_code],
                "period": "1m",
                "start_time": trade_date,
                "end_time": trade_date
            }
        })

        if not result or qmt_code not in result:
            return None
        df_data = result[qmt_code]
        if not df_data or len(df_data) == 0:
            return None

        import pandas as pd
        result_list = []
        cumulative_amount, cumulative_volume, is_first = 0.0, 0.0, True

        for row in df_data:
            time_val = row.get('time')
            if isinstance(time_val, str):
                dt = datetime.datetime.strptime(time_val[:19], "%Y-%m-%dT%H:%M:%S" if 'T' in time_val else "%Y-%m-%d %H:%M:%S")
            else:
                dt = pd.to_datetime(time_val).to_pydatetime()

            date_str, time_str = dt.strftime("%Y-%m-%d"), dt.strftime("%H:%M:%S")
            price = float(prev_close) if is_first and prev_close is not None else float(row['close'])
            is_first = False

            volume = float(row.get('volume', 0) or 0)
            amount = float(row.get('amount', 0) or 0)
            cumulative_amount += amount
            cumulative_volume += volume if volume > 0 else 0
            avg_price = round(cumulative_amount / cumulative_volume, 6) if cumulative_volume else price

            result_list.append({
                "date": date_str,
                "time": time_str,
                "price": price,
                "avg_price": avg_price,
                "volume": volume
            })
        return result_list if result_list else None
    except Exception as e:
        print(f"[ERROR] fetch_intraday_single failed: {e}")
        return None

def fetch_price_batch(codes_info: List[Tuple[str, str, str]]) -> Dict[str, Dict]:
    qmt_codes = [info[1] for info in codes_info if info[1]]
    if not qmt_codes:
        return {}
    try:
        result = send_qmt_request({"function": "get_full_tick", "params": {"stock_codes": qmt_codes}})
        if not result:
            return {}
        results = {}
        for orig_code, qmt_code, currency in codes_info:
            tick_data = result.get(qmt_code)
            if not tick_data:
                continue
            latest_price = tick_data.get("lastPrice")
            prev_close = tick_data.get("lastClose")
            if latest_price is None or prev_close is None:
                continue

            name = orig_code
            try:
                detail = send_qmt_request({
                    "function": "get_instrument_detail",
                    "params": {"stock_code": qmt_code, "iscomplete": False}
                })
                if detail and detail.get("InstrumentName"):
                    name = detail["InstrumentName"]
            except:
                pass

            change_amount = latest_price - prev_close
            results[orig_code] = {
                "name": name,
                "latestPrice": latest_price,
                "changePercent": round((change_amount / prev_close) * 100, 6),
                "changeAmount": change_amount,
                "source": "qmt",
                "currency": currency,
                "dailydata": None
            }
        return results
    except Exception as e:
        print(f"[ERROR] fetch_price_batch failed: {e}")
        return {}

def fetch_intraday_batch(codes_info: List[Tuple[str, str, str]]) -> Dict[str, List[Dict]]:
    trade_date = get_last_trade_date()
    qmt_codes = [info[1] for info in codes_info if info[1]]
    if not qmt_codes:
        return {}

    # 批量下载
    for qmt_code in qmt_codes:
        try:
            send_qmt_request({
                "function": "download_history_data",
                "params": {"stock_code": qmt_code, "period": "1m", "start_time": trade_date, "end_time": trade_date}
            })
        except:
            pass
    time.sleep(0.5)

    try:
        result = send_qmt_request({
            "function": "get_market_data_ex",
            "params": {
                "fields": ['time', 'open', 'high', 'low', 'close', 'volume', 'amount'],
                "stock_codes": qmt_codes,
                "period": "1m",
                "start_time": trade_date,
                "end_time": trade_date
            }
        })
        if not result:
            return {}

        # 获取昨收
        prev_closes = {}
        try:
            tick = send_qmt_request({"function": "get_full_tick", "params": {"stock_codes": qmt_codes}})
            for code in qmt_codes:
                prev_closes[code] = tick.get(code, {}).get("lastClose")
        except:
            pass

        import pandas as pd
        results = {}
        for orig_code, qmt_code, currency in codes_info:
            if qmt_code not in result:
                continue
            df_data = result[qmt_code]
            if not df_data or len(df_data) == 0:
                continue

            prev_close = prev_closes.get(qmt_code)
            result_list, cumulative_amount, cumulative_volume, is_first = [], 0.0, 0.0, True

            for row in df_data:
                time_val = row.get('time')
                if isinstance(time_val, str):
                    dt = datetime.datetime.strptime(time_val[:19], "%Y-%m-%dT%H:%M:%S" if 'T' in time_val else "%Y-%m-%d %H:%M:%S")
                else:
                    dt = pd.to_datetime(time_val).to_pydatetime()

                price = float(prev_close) if is_first and prev_close is not None else float(row['close'])
                is_first = False
                volume = float(row.get('volume', 0) or 0)
                amount = float(row.get('amount', 0) or 0)
                cumulative_amount += amount
                cumulative_volume += volume if volume > 0 else 0
                avg_price = round(cumulative_amount / cumulative_volume, 6) if cumulative_volume else price

                result_list.append({
                    "date": dt.strftime("%Y-%m-%d"),
                    "time": dt.strftime("%H:%M:%S"),
                    "price": price,
                    "avg_price": avg_price,
                    "volume": volume
                })

            if result_list:
                results[orig_code] = result_list
        return results
    except Exception as e:
        print(f"[ERROR] fetch_intraday_batch failed: {e}")
        return {}

# ==============================
# 路由
# ==============================
def handle_querylocal_single():
    code = request.args.get("code")
    type_ = request.args.get("type")
    if not code or not type_:
        return json_response({"detail": "Missing code or type"}, 400)

    qmt_code, currency = convert_code(code)
    if not qmt_code:
        return json_response({"detail": f"Unsupported code format: {code}"}, 400)

    try:
        if type_ == "price":
            result = fetch_price_single(qmt_code, code, currency)
            return json_response(result) if result else json_response({"detail": f"Price data not found for {code}"}, 404)
        elif type_ == "intraday":
            prev_close = None
            try:
                tick = send_qmt_request({"function": "get_full_tick", "params": {"stock_codes": [qmt_code]}})
                prev_close = tick.get(qmt_code, {}).get("lastClose")
            except:
                pass
            result = fetch_intraday_single(qmt_code, code, get_last_trade_date(), prev_close)
            return json_response(result) if result else json_response({"detail": f"Intraday data not found for {code}"}, 404)
        else:
            return json_response({"detail": "Invalid 'type'"}, 400)
    except Exception as e:
        return json_response({"detail": str(e)}, 500)

def handle_querylocal_batch():
    data = request.get_json()
    if not data:
        return json_response({"detail": "Missing JSON body"}, 400)
    codes = data.get("codes", [])
    type_ = data.get("type")
    if not codes or not type_:
        return json_response({"detail": "Missing codes or type"}, 400)
    if len(codes) > 50:
        return json_response({"detail": "Too many codes, max 50"}, 400)

    codes_info = []
    for code in codes:
        qmt_code, currency = convert_code(code)
        if qmt_code:
            codes_info.append((code, qmt_code, currency))

    if not codes_info:
        return json_response({"detail": "No valid codes"}, 400)

    try:
        if type_ == "price":
            return json_response(fetch_price_batch(codes_info))
        elif type_ == "intraday":
            return json_response(fetch_intraday_batch(codes_info))
        else:
            return json_response({"detail": "Batch only supports 'price' or 'intraday'"}, 400)
    except Exception as e:
        return json_response({"detail": str(e)}, 500)

@app.route("/querylocal", methods=["GET", "POST", "OPTIONS"])
@app.route("/api/querylocal", methods=["GET", "POST", "OPTIONS"])
def querylocal():
    if request.method == "OPTIONS":
        response = jsonify({})
        response.headers["Access-Control-Allow-Origin"] = request.headers.get("Origin", "*")
        response.headers["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS"
        response.headers["Access-Control-Allow-Headers"] = "Content-Type"
        return response, 204
    return handle_querylocal_batch() if request.method == "POST" else handle_querylocal_single()

@app.route("/health", methods=["GET"])
def health_check():
    return json_response({
        "status": "healthy",
        "service": "qmt_local_service",
        "timestamp": datetime.datetime.now().isoformat(),
        "qmt_connected": os.path.exists(QMT_COMMAND_FILE) or os.path.exists(QMT_COMMAND_FILE + ".notify"),
        "last_response_time": last_response_time
    })

if __name__ == "__main__":
    # 确保目录存在
    os.makedirs(os.path.dirname(QMT_COMMAND_FILE), exist_ok=True)

    monitor_thread = threading.Thread(target=response_monitor, daemon=True)
    monitor_thread.start()

    print("=" * 60)
    print("QMT Local Data Service - Final Stable Version")
    print("Listening on http://0.0.0.0:8787")
    print("=" * 60)
    app.run(host="0.0.0.0", port=8787, threaded=True)
