# ============================================================
# caidazi_mcp_server.py
# 财搭子 MCP 行情服务代理（Windows 本地版）
# 兼容原 QMT Flask API 格式
# ============================================================

import os
import re
import json
import time
from datetime import datetime, timezone, timedelta
from typing import Optional, List, Dict, Any

from flask import Flask, request, jsonify, make_response
from flask_cors import CORS
import requests

# ======================== 配置 ========================
CAIDAZI_MCP_URL = (os.environ.get("CAIDAZI_MCP_URL") or 
                   os.environ.get("CAIDAZI_BASE_URL") or 
                   "https://mcp.zhicepilot.com").rstrip("/")
CAIDAZI_API_KEY = os.environ.get("CAIDAZI_API_KEY", "")

# 简单内存缓存：key -> (expire_timestamp, data)
_CACHE: Dict[str, tuple] = {}

app = Flask(__name__)
CORS(app, resources={
    r"/*": {
        "origins": "*",
        "methods": ["GET", "POST", "OPTIONS"],
        "allow_headers": ["Content-Type"]
    }
})


# ============================================================
# 路由
# ============================================================
@app.route("/querylocal", methods=["GET", "POST", "OPTIONS"])
@app.route("/api/querylocal", methods=["GET", "POST", "OPTIONS"])
def querylocal():
    if request.method == "OPTIONS":
        return "", 204

    try:
        if request.method == "GET":
            return handle_single_query()
        elif request.method == "POST":
            return handle_batch_query()
        else:
            return error_response("Method Not Allowed", 405)
    except Exception as e:
        print(f"[Worker Error] {e}")
        return error_response(str(e), 500)


# ============================================================
# 单股票 GET
# ============================================================
def handle_single_query():
    code = request.args.get("code")
    type_ = request.args.get("type")
    date = request.args.get("date")

    if not code or not type_:
        return error_response("Missing code or type", 400)

    qmt_code, currency = convert_code(code)
    if not qmt_code:
        return error_response(f"Unsupported code format: {code}", 400)

    if type_ not in ("price", "intraday"):
        return error_response(f"{type_} not supported in local MCP service", 501)

    # 缓存 key（与原 Worker 一致）
    cache_key = request.url
    cached = get_from_cache(cache_key)
    if cached is not None:
        return jsonify(cached)

    codes_info = [{"orig": code, "qmt": qmt_code, "currency": currency}]

    if type_ == "price":
        results = fetch_price_batch(codes_info)
        result_data = results.get(code)
        if not result_data:
            return error_response(f"Price data not found for {code}", 404)
        ttl = 5
    else:
        trade_date = date or get_last_trade_date()
        results = fetch_intraday_batch(codes_info, trade_date)
        result_data = results.get(code)
        if not result_data:
            return error_response(f"Intraday data not found for {code}", 404)

        today = get_beijing_today()
        trade_date_clean = str(trade_date).replace("-", "")
        ttl = 86400 if trade_date_clean < today else 30

    set_cache(cache_key, result_data, ttl)
    resp = make_response(jsonify(result_data))
    resp.headers["Cache-Control"] = f"public, max-age={ttl}"
    return resp


# ============================================================
# 批量 POST
# ============================================================
def handle_batch_query():
    body = request.get_json(silent=True)
    if not body:
        return error_response("Missing JSON body", 400)

    codes = body.get("codes")
    type_ = body.get("type")
    date = body.get("date")

    if not codes or not type_ or not isinstance(codes, list):
        return error_response("Missing codes array or type", 400)
    if len(codes) > 50:
        return error_response("Too many codes, max 50", 400)
    if type_ not in ("price", "intraday"):
        return error_response("Batch only supports 'price' or 'intraday'", 400)

    codes_info = []
    for code in codes:
        qmt_code, currency = convert_code(str(code))
        if qmt_code:
            codes_info.append({"orig": str(code), "qmt": qmt_code, "currency": currency})

    if not codes_info:
        return error_response("No valid codes", 400)

    # 构造与原 Worker 类似的缓存 key
    sorted_codes = ",".join(sorted(set(c["orig"] for c in codes_info)))
    cache_key = f"{request.path}?codes={sorted_codes}&type={type_}"
    if date:
        cache_key += f"&date={date}"

    cached = get_from_cache(cache_key)
    if cached is not None:
        return jsonify(cached)

    if type_ == "price":
        results = fetch_price_batch(codes_info)
        ttl = 5
    else:
        results = fetch_intraday_batch(codes_info, date)
        today = get_beijing_today()
        trade_date = str(date or today).replace("-", "")
        ttl = 86400 if trade_date < today else 30

    set_cache(cache_key, results, ttl)
    resp = make_response(jsonify(results))
    resp.headers["Cache-Control"] = f"public, max-age={ttl}"
    return resp


# ============================================================
# 数据获取层
# ============================================================
def fetch_price_batch(codes_info: List[Dict]) -> Dict:
    symbols = deduplicate_symbols([c["orig"] for c in codes_info])
    if not symbols:
        return {}

    mcp_result = call_mcp_tool("get_a_share_realtime_1m_price", {
        "symbols": symbols,
        "include_incomplete": True,
    })

    items = (mcp_result or {}).get("data", {}).get("items", []) or []
    results = {}

    symbol_map = {}
    for c in codes_info:
        norm = normalize_a_share_code(c["orig"])
        if norm and norm not in symbol_map:
            symbol_map[norm] = c

    for item in items:
        mapping = symbol_map.get(item.get("symbol"))
        if not mapping or not item.get("bar"):
            continue

        bar = item["bar"]
        try:
            latest_price = float(bar["close"])
            prev_close = float(bar["prev_close"])
        except (KeyError, TypeError, ValueError):
            continue

        change_amount = latest_price - prev_close
        change_percent = round((change_amount / prev_close) * 100, 6) if prev_close else 0.0

        results[mapping["orig"]] = {
            "name": item.get("name") or mapping["orig"],
            "latestPrice": latest_price,
            "changePercent": change_percent,
            "changeAmount": change_amount,
            "source": "caidazi_mcp_local",   # 原为 caidazi_mcp_cf
            "currency": mapping["currency"],
            "dailydata": None,
        }

    return results


def fetch_intraday_batch(codes_info: List[Dict], end_date: Optional[str] = None) -> Dict:
    symbols = deduplicate_symbols([c["orig"] for c in codes_info])
    if not symbols:
        return {}

    args = {"symbols": symbols, "trading_days": 2}
    if end_date:
        args["end_date"] = str(end_date).replace("-", "")

    mcp_result = call_mcp_tool("get_a_share_history_1m_price", args)
    items = (mcp_result or {}).get("data", {}).get("items", []) or []
    results = {}

    symbol_map = {}
    for c in codes_info:
        norm = normalize_a_share_code(c["orig"])
        if norm and norm not in symbol_map:
            symbol_map[norm] = c["orig"]

    for item in items:
        orig_code = symbol_map.get(item.get("symbol"))
        if not orig_code:
            continue
        legacy = convert_history_item_to_legacy(item, end_date)
        if legacy:
            results[orig_code] = legacy

    return results


# ============================================================
# 数据转换逻辑（高度兼容原代码）
# ============================================================
def convert_history_item_to_legacy(item: Dict, preferred_trade_date: Optional[str] = None):
    days = item.get("days") or []
    if not days:
        return None

    selected_day = None
    if preferred_trade_date:
        target = str(preferred_trade_date).replace("-", "")
        for d in days:
            if str(d.get("trade_date", "")).replace("-", "") == target:
                selected_day = d
                break
    if not selected_day:
        selected_day = days[-1]

    bars = selected_day.get("bars") or []
    if not bars:
        return None

    bars = sorted(bars, key=lambda x: str(x.get("bar_time", "")))

    prev_close = None
    if bars and bars[0].get("prev_close") is not None:
        try:
            prev_close = float(bars[0]["prev_close"])
        except (TypeError, ValueError):
            prev_close = None

    result = []
    cumulative_amount = 0.0
    cumulative_volume_shares = 0.0
    is_first = True

    shanghai_tz = timezone(timedelta(hours=8))

    for bar in bars:
        bar_time_str = str(bar.get("bar_time", "")).replace("Z", "+00:00")
        try:
            if "T" in bar_time_str or "+" in bar_time_str or bar_time_str.endswith("Z"):
                dt = datetime.fromisoformat(bar_time_str.replace("Z", "+00:00"))
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                dt_sh = dt.astimezone(shanghai_tz)
            else:
                dt_sh = datetime.strptime(bar_time_str[:19], "%Y-%m-%d %H:%M:%S").replace(tzinfo=shanghai_tz)
            date_str = dt_sh.strftime("%Y-%m-%d")
            time_str = dt_sh.strftime("%H:%M:%S")
        except Exception:
            date_str = bar_time_str[:10]
            time_str = bar_time_str[11:19] if len(bar_time_str) >= 19 else "00:00:00"

        try:
            close = float(bar["close"])
        except (KeyError, TypeError, ValueError):
            continue

        price = prev_close if (is_first and prev_close is not None) else close
        volume_shares = float(bar.get("volume") or 0)
        volume_hands = volume_shares / 100.0
        amount = float(bar.get("amount") or 0)

        cumulative_amount += amount
        cumulative_volume_shares += volume_shares

        avg_price = round(cumulative_amount / cumulative_volume_shares, 6) if cumulative_volume_shares > 0 else price

        result.append({
            "date": date_str,
            "time": time_str,
            "price": price,
            "avg_price": avg_price,
            "volume": volume_hands,
        })
        is_first = False

    return result if result else None


# ============================================================
# MCP REST API 客户端
# ============================================================
def call_mcp_tool(tool_name: str, args: Dict) -> Any:
    if not CAIDAZI_API_KEY:
        raise RuntimeError("CAIDAZI_API_KEY is not configured. Please set environment variable CAIDAZI_API_KEY.")

    url = f"{CAIDAZI_MCP_URL}/api/tools/call"
    headers = {
        "Authorization": f"Bearer {CAIDAZI_API_KEY}",
        "Accept": "application/json",
        "Content-Type": "application/json",
    }
    payload = {
        "tool_name": tool_name,
        "parameters": args or {},
    }

    resp = requests.post(url, headers=headers, json=payload, timeout=30)
    text = resp.text

    try:
        body = json.loads(text) if text else {}
    except json.JSONDecodeError:
        body = {"raw": text}

    if not resp.ok:
        msg = ""
        if isinstance(body, dict):
            msg = body.get("detail") or body.get("error") or body.get("message") or ""
        if not msg:
            msg = (text[:200] if text else "") or resp.reason
        raise RuntimeError(f"MCP Server Error: {resp.status_code} {msg}")

    if isinstance(body, dict) and body.get("success") is False:
        raise RuntimeError(body.get("error") or f"Tool {tool_name} failed")

    # 官方客户端优先使用 body.result
    if isinstance(body, dict) and "result" in body:
        return body["result"]
    return body


# ============================================================
# 辅助方法
# ============================================================
def convert_code(code: str):
    if not code:
        return None, None
    text = str(code).strip().upper()

    if re.match(r"^\d{6}\.(SH|SZ|BJ)$", text):
        return text, "CNY"
    if re.match(r"^\d{5}\.HK$", text):
        return text, "HKD"
    if text.startswith("HK"):
        pure = text[2:]
        if pure.isdigit():
            return f"{pure}.HK", "HKD"
    if re.match(r"^(60|68|90)", text):
        return f"{text}.SH", "CNY"
    if re.match(r"^(00|30)", text):
        return f"{text}.SZ", "CNY"
    if re.match(r"^92", text):
        return f"{text}.BJ", "CNY"
    return None, None


def normalize_a_share_code(code: str) -> Optional[str]:
    qmt_code, currency = convert_code(code)
    if not qmt_code:
        return None
    if currency == "CNY" and re.match(r"^\d{6}\.(SH|SZ|BJ)$", qmt_code):
        return qmt_code
    if currency == "HKD" and re.match(r"^\d{5}\.HK$", qmt_code):
        return qmt_code
    return None


def deduplicate_symbols(symbols: List[str]) -> List[str]:
    seen = set()
    res = []
    for s in symbols:
        n = normalize_a_share_code(s)
        if n and n not in seen:
            seen.add(n)
            res.append(n)
    return res


def get_beijing_today() -> str:
    """上海时区当前日期，格式 YYYYMMDD"""
    shanghai = timezone(timedelta(hours=8))
    return datetime.now(shanghai).strftime("%Y%m%d")


def get_last_trade_date() -> str:
    return get_beijing_today()


def get_from_cache(key: str):
    if key in _CACHE:
        expire, data = _CACHE[key]
        if time.time() < expire:
            return data
        else:
            del _CACHE[key]
    return None


def set_cache(key: str, data, ttl: int):
    _CACHE[key] = (time.time() + ttl, data)


def error_response(detail: str, status: int = 400):
    return jsonify({"detail": detail}), status


# ============================================================
# 启动
# ============================================================
if __name__ == "__main__":
    if not CAIDAZI_API_KEY:
        print("=" * 60)
        print("警告：未设置 CAIDAZI_API_KEY 环境变量！")
        print("请先执行：")
        print('  set CAIDAZI_API_KEY=你的API密钥')
        print("然后再运行本程序。")
        print("=" * 60)

    print("财搭子 MCP 本地行情服务已启动")
    print("接口示例：")
    print("  GET  http://127.0.0.1:5000/querylocal?code=600519&type=price")
    print("  GET  http://127.0.0.1:5000/querylocal?code=600519&type=intraday")
    print("  POST http://127.0.0.1:5000/querylocal")
    print("监听地址：http://0.0.0.0:5000")
    app.run(host="0.0.0.0", port=5000, debug=False, threaded=True)
