# qmt_api_server.py
# Windows 本地 miniQMT 数据服务
# 运行前请确保 miniQMT 终端已启动并登录

from flask import Flask, request, jsonify
from xtquant import xtdata
import datetime
import re
import pandas as pd

app = Flask(__name__)

# ==============================
# 代码转换：用户输入 -> miniQMT 格式
# ==============================
def convert_code(code):
    """
    将用户输入的代码转换为 miniQMT 格式，并返回币种
    600519 -> 600519.SH, CNY
    000001 -> 000001.SZ, CNY
    HK00700 -> 00700.HK, HKD
    """
    codeUpper = code.upper()

    if codeUpper.startswith("HK"):
        pure = codeUpper.replace("HK", "")
        return f"{pure}.HK", "HKD"
    elif re.match(r"^(60|68|51|56|58|55|900)", code):
        return f"{code}.SH", "CNY"
    elif re.match(r"^(00|30|15|200)", code):
        return f"{code}.SZ", "CNY"
    else:
        return None, None


# ==============================
# 统一 JSON 返回（对齐原接口缓存头）
# ==============================
def json_response(data, status=200):
    response = jsonify(data)
    response.status_code = status
    response.headers["Content-Type"] = "application/json"
    response.headers["Cache-Control"] = "max-age=5, stale-while-revalidate=10"
    return response


# ==============================
# ✅ Price：实时价格（完全对齐 Eastmoney 格式）
# ==============================
def fetch_price(qmt_code, orig_code, currency):
    tick = xtdata.get_full_tick([qmt_code])
    if not tick or qmt_code not in tick:
        return None

    data = tick[qmt_code]
    latest_price = data.get("lastPrice")
    prev_close = data.get("lastClose")

    if latest_price is None or prev_close is None:
        return None

    # 获取股票名称（失败时回退到原始代码）
    name = orig_code
    try:
        detail = xtdata.get_instrument_detail(qmt_code, iscomplete=False)
        if detail and detail.get("InstrumentName"):
            name = detail["InstrumentName"]
    except Exception:
        pass

    change_amount = latest_price - prev_close
    change_percent = round((change_amount / prev_close) * 100, 6) if prev_close else 0.0

    return {
        "name": name,
        "latestPrice": latest_price,
        "changePercent": change_percent,
        "changeAmount": change_amount,
        "source": "miniqmt",
        "currency": currency,
        "dailydata": None        # ✅ 与原接口完全一致
    }


# ==============================
# ✅ Intraday：当日分时数据（完全对齐 Eastmoney 格式）
# ==============================
def fetch_intraday(qmt_code, orig_code):
    today = datetime.datetime.now().strftime("%Y%m%d")

    # 1) 下载当日1分钟数据（幂等，已下载则快速返回）
    try:
        xtdata.download_history_data(qmt_code, period='1m', start_time=today, end_time=today)
    except Exception as e:
        print(f"[WARN] download_history_data: {e}")

    # 2) 获取数据（优先 get_market_data_ex，支持历史+实时拼接）
    df = None
    try:
        data_dict = xtdata.get_market_data_ex(
            ['time', 'open', 'high', 'low', 'close', 'volume', 'amount'],
            [qmt_code],
            period='1m',
            start_time=today,
            end_time=today
        )
        if data_dict and qmt_code in data_dict:
            df = data_dict[qmt_code]
    except Exception as e:
        print(f"[WARN] get_market_data_ex: {e}")

    if df is None or df.empty:
        return None

    if 'close' not in df.columns:
        return None

    result = []
    cumulative_amount = 0.0
    cumulative_volume = 0.0

    for idx, row in df.iterrows():
        # 时间解析（兼容多种格式）
        time_val = row.get('time')
        if pd.isna(time_val):
            time_val = idx

        if isinstance(time_val, pd.Timestamp):
            dt = time_val.to_pydatetime()
        elif isinstance(time_val, (int, float)):
            # 毫秒时间戳（miniQMT 常见格式）
            dt = datetime.datetime.fromtimestamp(time_val / 1000)
        elif isinstance(time_val, str):
            time_val = time_val.strip()
            if len(time_val) == 14:          # YYYYMMDDHHMMSS
                dt = datetime.datetime.strptime(time_val, "%Y%m%d%H%M%S")
            elif len(time_val) == 6:         # HHMMSS
                dt = datetime.datetime.strptime(f"{today} {time_val}", "%Y%m%d%H%M%S")
            elif len(time_val) == 8 and ':' in time_val:  # HH:MM:SS
                dt = datetime.datetime.strptime(f"{today} {time_val}", "%Y%m%d %H:%M:%S")
            elif len(time_val) == 19:        # YYYY-MM-DD HH:MM:SS
                dt = datetime.datetime.strptime(time_val, "%Y-%m-%d %H:%M:%S")
            else:
                try:
                    dt = pd.to_datetime(time_val).to_pydatetime()
                except Exception:
                    continue
        else:
            continue

        date_str = dt.strftime("%Y-%m-%d")
        time_str = dt.strftime("%H:%M:%S")

        price = float(row['close']) if 'close' in row else 0.0
        volume = float(row['volume']) if 'volume' in row else 0.0
        amount = float(row['amount']) if 'amount' in row else 0.0

        cumulative_amount += amount
        cumulative_volume += volume if volume > 0 else 0

        # ✅ 精度修复（关键）：与原 JS 逻辑完全一致
        avg_price = round(cumulative_amount / cumulative_volume, 6) if cumulative_volume else price

        result.append({
            "date": date_str,
            "time": time_str,
            "price": price,
            "avg_price": avg_price,
            "volume": float(volume)   # ✅ 类型对齐 Python float
        })

    return result if result else None


# ==============================
# 路由入口（与原 query.js 接口对齐）
# ==============================
@app.route("/query")
def query():
    code = request.args.get("code")
    type_ = request.args.get("type")

    if not code or not type_:
        return json_response({"detail": "Missing code or type"}, 400)

    qmt_code, currency = convert_code(code)
    if not qmt_code:
        return json_response({"detail": f"Unsupported code format: {code}"}, 400)

    try:
        if type_ == "price":
            result = fetch_price(qmt_code, code, currency)
            if result:
                return json_response(result)
            return json_response({"detail": f"Price data not found for {code}"}, 404)

        elif type_ == "intraday":
            result = fetch_intraday(qmt_code, code)
            if result:
                return json_response(result)
            return json_response({"detail": f"Intraday data not found for {code}"}, 404)

        elif type_ in ("info", "movingaveragedata"):
            return json_response(
                {"detail": f"{type_} not supported in local QMT service"}, 501
            )

        else:
            return json_response({
                "detail": "Invalid 'type' parameter. Use 'price', 'info', 'movingaveragedata', or 'intraday'."
            }, 400)

    except Exception as e:
        return json_response({"detail": str(e)}, 500)


# ==============================
# 启动服务
# ==============================
if __name__ == "__main__":
    print("=" * 60)
    print("QMT Local Data Service")
    print("Please make sure miniQMT is running before starting.")
    print("Listening on http://0.0.0.0:8787")
    print("=" * 60)
    # host=0.0.0.0 允许局域网访问，配合 Cloudflare Tunnel 使用
    app.run(host="0.0.0.0", port=8787, threaded=True)
