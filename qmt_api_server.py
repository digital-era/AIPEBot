# qmt_api_server.py

from flask import Flask, request, jsonify, Blueprint
from flask_cors import CORS
from xtquant import xtdata
import datetime
import re
import pandas as pd

app = Flask(__name__)
CORS(app, origins=[
    "https://aivibeinvestment.com",
])

# ==============================
# 代码转换：用户输入 -> miniQMT 格式
# ==============================
def convert_code(code):
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
# 统一 JSON 返回
# ==============================
def json_response(data, status=200):
    response = jsonify(data)
    response.status_code = status
    response.headers["Content-Type"] = "application/json"
    response.headers["Cache-Control"] = "max-age=5, stale-while-revalidate=10"
    return response


# ==============================
# 数据获取函数
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
        "dailydata": None
    }


from xtquant import xtdata
import datetime
import time
import re
import pandas as pd

# ... convert_code 函数不变 ...

def get_last_trade_date():
    now = datetime.datetime.now()
    today_str = now.strftime("%Y%m%d")
    
    try:
        dates = xtdata.get_trading_dates("SH", start_time="20260101", end_time=today_str, count=-1)
        if dates:
            last_ts = dates[-1]
            return time.strftime('%Y%m%d', time.localtime(last_ts / 1000))
    except Exception as e:
        print(f"[WARN] get_trading_dates failed: {e}")
    
    weekday = now.weekday()
    if weekday == 0:
        days_back = 3
    elif weekday == 6:
        days_back = 2
    else:
        days_back = 1
    return (now - datetime.timedelta(days=days_back)).strftime("%Y%m%d")


def fetch_intraday(qmt_code, orig_code):
    trade_date = get_last_trade_date()
    print(f"[DEBUG] Using trade date: {trade_date}")
    
    try:
        xtdata.download_history_data(qmt_code, period='1m', start_time=trade_date, end_time=trade_date)
    except Exception as e:
        print(f"[WARN] download_history_data: {e}")

    df = None
    try:
        data_dict = xtdata.get_market_data_ex(
            ['time', 'open', 'high', 'low', 'close', 'volume', 'amount'],
            [qmt_code],
            period='1m',
            start_time=trade_date,
            end_time=trade_date
        )
        if data_dict and qmt_code in data_dict:
            df = data_dict[qmt_code]
    except Exception as e:
        print(f"[WARN] get_market_data_ex: {e}")

    if df is None or df.empty or 'close' not in df.columns:
        return None

    prev_close = None
    try:
        tick = xtdata.get_full_tick([qmt_code])
        if tick and qmt_code in tick:
            prev_close = tick[qmt_code].get("lastClose")
    except Exception:
        pass

    result = []
    cumulative_amount = 0.0
    cumulative_volume = 0.0
    is_first = True

    for idx, row in df.iterrows():
        time_val = row.get('time')
        if pd.isna(time_val):
            time_val = idx

        if isinstance(time_val, pd.Timestamp):
            dt = time_val.to_pydatetime()
        elif isinstance(time_val, (int, float)):
            dt = datetime.datetime.fromtimestamp(time_val / 1000)
        elif isinstance(time_val, str):
            time_val = time_val.strip()
            if len(time_val) == 14:
                dt = datetime.datetime.strptime(time_val, "%Y%m%d%H%M%S")
            elif len(time_val) == 6:
                dt = datetime.datetime.strptime(f"{trade_date} {time_val}", "%Y%m%d%H%M%S")
            elif len(time_val) == 8 and ':' in time_val:
                dt = datetime.datetime.strptime(f"{trade_date} {time_val}", "%Y%m%d %H:%M:%S")
            elif len(time_val) == 19:
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

        if is_first and prev_close is not None:
            price = float(prev_close)
            is_first = False
        else:
            price = float(row['close'])

        volume = float(row['volume']) if 'volume' in row else 0.0
        amount = float(row['amount']) if 'amount' in row else 0.0

        cumulative_amount += amount
        cumulative_volume += volume if volume > 0 else 0
        avg_price = round(cumulative_amount / cumulative_volume, 6) if cumulative_volume else price

        result.append({
            "date": date_str,
            "time": time_str,
            "price": price,
            "avg_price": avg_price,
            "volume": float(volume)
        })

    return result if result else None


# ==============================
# 核心路由处理函数
# ==============================
def handle_querylocal():
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
# 注册路由：同时支持 /querylocal 和 /api/querylocal
# ==============================
@app.route("/querylocal", methods=["GET", "POST", "OPTIONS"])
@app.route("/api/querylocal", methods=["GET", "POST", "OPTIONS"])
def querylocal():
    # 处理 OPTIONS 预检请求
    if request.method == "OPTIONS":
        response = jsonify({})
        response.headers["Access-Control-Allow-Origin"] = request.headers.get("Origin", "*")
        response.headers["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS"
        response.headers["Access-Control-Allow-Headers"] = "Content-Type"
        return response, 204
    
    return handle_querylocal()


# ==============================
# 全局错误处理
# ==============================
@app.errorhandler(404)
def not_found(error):
    return json_response({"detail": "Not found"}, 404)


@app.errorhandler(500)
def server_error(error):
    return json_response({"detail": "Internal server error"}, 500)


# ==============================
# 启动服务
# ==============================
if __name__ == "__main__":
    print("=" * 60)
    print("QMT Local Data Service")
    print("Please make sure miniQMT is running before starting.")
    print("Listening on http://0.0.0.0:8787")
    print("Routes: /querylocal, /api/querylocal")
    print("=" * 60)
    app.run(host="0.0.0.0", port=8787, threaded=True)
