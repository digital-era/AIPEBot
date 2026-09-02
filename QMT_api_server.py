# -*- coding: gbk -*-
# QMT内置Python环境 - 本地数据服务
# 替代原miniQMT HTTP服务，完全在QMT策略模型中运行

import os
import re
import json
import time
import datetime
import pandas as pd
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs

# ==============================
# 配置参数
# ==============================
PORT = 8787  # 服务端口
CORS_ORIGINS = [
    "https://aivibeinvestment.com",
    "http://localhost",
    "http://127.0.0.1"
]

# ==============================
# QMT数据接口适配层
# ==============================
class QMTDataAdapter:
    """QMT内置Python环境数据适配器"""
    
    def __init__(self, ContextInfo):
        self.ContextInfo = ContextInfo
        self._cache = {}  # 简单缓存
        self._cache_time = {}  # 缓存时间
        self.CACHE_TIMEOUT = 5  # 5秒缓存
        
    def _convert_code(self, code):
        """代码格式转换：用户输入 -> QMT格式"""
        codeUpper = code.upper()
        if codeUpper.startswith("HK"):
            pure = codeUpper.replace("HK", "")
            return f"{pure}.HK", "HKD"
        elif re.match(r"^(60|68|51|56|58|55|900)", code):
            return f"{code}.SH", "CNY"
        elif re.match(r"^(00|30|15|200)", code):
            return f"{code}.SZ", "CNY"
        elif re.match(r"^(4|8)", code):  # 北交所
            return f"{code}.BJ", "CNY"
        else:
            return None, None
    
    def _get_cache(self, key):
        """获取缓存数据"""
        if key in self._cache:
            if time.time() - self._cache_time.get(key, 0) < self.CACHE_TIMEOUT:
                return self._cache[key]
        return None
    
    def _set_cache(self, key, data):
        """设置缓存数据"""
        self._cache[key] = data
        self._cache_time[key] = time.time()
    
    def get_last_trade_date(self):
        """获取最近交易日"""
        now = datetime.datetime.now()
        today_str = now.strftime("%Y%m%d")
        
        try:
            # 使用QMT内置函数获取交易日历
            dates = self.ContextInfo.get_trading_dates("SH", start_time="20260101", end_time=today_str, count=-1)
            if dates:
                last_ts = dates[-1]
                return time.strftime('%Y%m%d', time.localtime(last_ts / 1000))
        except Exception as e:
            print(f"[WARN] get_trading_dates failed: {e}")
        
        # 备用逻辑
        weekday = now.weekday()
        if weekday == 0:  # 周一
            days_back = 3
        elif weekday == 6:  # 周日
            days_back = 2
        else:
            days_back = 1
        return (now - datetime.timedelta(days=days_back)).strftime("%Y%m%d")
    
    def fetch_price_single(self, qmt_code, orig_code, currency):
        """单只股票实时价格"""
        cache_key = f"price_{qmt_code}"
        cached = self._get_cache(cache_key)
        if cached:
            return cached
        
        try:
            # 使用QMT内置的get_full_tick获取实时数据
            tick = self.ContextInfo.get_full_tick([qmt_code])
            if not tick or qmt_code not in tick:
                return None
            
            data = tick[qmt_code]
            latest_price = data.get("lastPrice")
            prev_close = data.get("lastClose")
            
            if latest_price is None or prev_close is None:
                return None
            
            # 尝试获取股票名称
            name = orig_code
            try:
                # 使用QMT的get_instrument_detail
                detail = self.ContextInfo.get_instrument_detail(qmt_code, iscomplete=False)
                if detail and detail.get("InstrumentName"):
                    name = detail["InstrumentName"]
            except Exception:
                pass
            
            change_amount = latest_price - prev_close
            change_percent = round((change_amount / prev_close) * 100, 6) if prev_close else 0.0
            
            result = {
                "name": name,
                "latestPrice": latest_price,
                "changePercent": change_percent,
                "changeAmount": change_amount,
                "source": "qmt_builtin",
                "currency": currency,
                "dailydata": None
            }
            
            self._set_cache(cache_key, result)
            return result
            
        except Exception as e:
            print(f"[ERROR] fetch_price_single failed: {e}")
            return None
    
    def fetch_intraday_single(self, qmt_code, orig_code, trade_date, prev_close):
        """单只股票分时数据"""
        try:
            # 使用QMT内置的get_market_data_ex获取分时数据
            data_dict = self.ContextInfo.get_market_data_ex(
                ['time', 'open', 'high', 'low', 'close', 'volume', 'amount'],
                [qmt_code],
                period='1m',
                start_time=trade_date,
                end_time=trade_date
            )
            
            if not data_dict or qmt_code not in data_dict:
                return None
            
            df = data_dict[qmt_code]
            if df is None or df.empty or 'close' not in df.columns:
                return None
            
            result = []
            cumulative_amount = 0.0
            cumulative_volume = 0.0
            is_first = True
            
            for idx, row in df.iterrows():
                time_val = row.get('time')
                if pd.isna(time_val):
                    time_val = idx
                
                # 时间格式处理（与原逻辑保持一致）
                dt = self._parse_datetime(time_val, trade_date)
                if dt is None:
                    continue
                
                date_str = dt.strftime("%Y-%m-%d")
                time_str = dt.strftime("%H:%M:%S")
                
                # 核心逻辑：第一分钟用prev_close，后续用真实close
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
                    "volume": volume
                })
            
            return result if result else None
            
        except Exception as e:
            print(f"[ERROR] fetch_intraday_single failed: {e}")
            return None
    
    def _parse_datetime(self, time_val, trade_date):
        """解析时间值（兼容多种格式）"""
        if isinstance(time_val, pd.Timestamp):
            return time_val.to_pydatetime()
        elif isinstance(time_val, (int, float)):
            return datetime.datetime.fromtimestamp(time_val / 1000)
        elif isinstance(time_val, str):
            time_val = time_val.strip()
            if len(time_val) == 14:  # YYYYMMDDHHMMSS
                return datetime.datetime.strptime(time_val, "%Y%m%d%H%M%S")
            elif len(time_val) == 6:  # HHMMSS
                return datetime.datetime.strptime(f"{trade_date} {time_val}", "%Y%m%d%H%M%S")
            elif len(time_val) == 8 and ':' in time_val:  # HH:MM:SS
                return datetime.datetime.strptime(f"{trade_date} {time_val}", "%Y%m%d %H:%M:%S")
            elif len(time_val) == 19:  # YYYY-MM-DD HH:MM:SS
                return datetime.datetime.strptime(time_val, "%Y-%m-%d %H:%M:%S")
            else:
                try:
                    return pd.to_datetime(time_val).to_pydatetime()
                except Exception:
                    return None
        else:
            return None
    
    def fetch_price_batch(self, codes_info):
        """批量获取实时价格"""
        qmt_codes = [info[1] for info in codes_info if info[1]]
        if not qmt_codes:
            return {}
        
        try:
            # 使用QMT内置的get_full_tick批量获取
            ticks = self.ContextInfo.get_full_tick(qmt_codes)
            
            results = {}
            for orig_code, qmt_code, currency in codes_info:
                if not qmt_code:
                    continue
                
                tick_data = ticks.get(qmt_code) if ticks else None
                if not tick_data:
                    continue
                
                latest_price = tick_data.get("lastPrice")
                prev_close = tick_data.get("lastClose")
                
                if latest_price is None or prev_close is None:
                    continue
                
                name = orig_code
                try:
                    detail = self.ContextInfo.get_instrument_detail(qmt_code, iscomplete=False)
                    if detail and detail.get("InstrumentName"):
                        name = detail["InstrumentName"]
                except Exception:
                    pass
                
                change_amount = latest_price - prev_close
                change_percent = round((change_amount / prev_close) * 100, 6) if prev_close else 0.0
                
                results[orig_code] = {
                    "name": name,
                    "latestPrice": latest_price,
                    "changePercent": change_percent,
                    "changeAmount": change_amount,
                    "source": "qmt_builtin",
                    "currency": currency,
                    "dailydata": None
                }
            
            return results
            
        except Exception as e:
            print(f"[ERROR] fetch_price_batch failed: {e}")
            return {}
    
    def fetch_intraday_batch(self, codes_info):
        """批量获取分时数据"""
        trade_date = self.get_last_trade_date()
        
        qmt_codes = [info[1] for info in codes_info if info[1]]
        if not qmt_codes:
            return {}
        
        # 批量获取数据
        try:
            data_dict = self.ContextInfo.get_market_data_ex(
                ['time', 'open', 'high', 'low', 'close', 'volume', 'amount'],
                qmt_codes,
                period='1m',
                start_time=trade_date,
                end_time=trade_date
            )
        except Exception as e:
            print(f"[WARN] batch get_market_data_ex: {e}")
            return {}
        
        # 获取昨收价（使用单只获取更可靠）
        prev_closes = {}
        for qmt_code in qmt_codes:
            prev_close = None
            
            # 尝试1：单只tick
            try:
                tick = self.ContextInfo.get_full_tick([qmt_code])
                if tick and qmt_code in tick:
                    prev_close = tick[qmt_code].get("lastClose")
            except Exception as e:
                print(f"[WARN] get_full_tick {qmt_code}: {e}")
            
            # 尝试2：从instrument detail获取
            if prev_close is None:
                try:
                    detail = self.ContextInfo.get_instrument_detail(qmt_code, iscomplete=False)
                    if detail:
                        prev_close = detail.get("PreClose")
                except Exception as e:
                    print(f"[WARN] instrument_detail {qmt_code}: {e}")
            
            prev_closes[qmt_code] = prev_close
        
        # 处理每只股票结果
        results = {}
        for orig_code, qmt_code, currency in codes_info:
            if not qmt_code or not data_dict or qmt_code not in data_dict:
                continue
            
            df = data_dict[qmt_code]
            if df is None or df.empty or 'close' not in df.columns:
                continue
            
            prev_close = prev_closes.get(qmt_code)
            
            result = []
            cumulative_amount = 0.0
            cumulative_volume = 0.0
            is_first = True
            
            for idx, row in df.iterrows():
                time_val = row.get('time')
                if pd.isna(time_val):
                    time_val = idx
                
                dt = self._parse_datetime(time_val, trade_date)
                if dt is None:
                    continue
                
                date_str = dt.strftime("%Y-%m-%d")
                time_str = dt.strftime("%H:%M:%S")
                
                # 核心逻辑：与单只版本完全一致
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
                    "volume": volume
                })
            
            if result:
                results[orig_code] = result
        
        return results


# ==============================
# HTTP请求处理器
# ==============================
class QMTRequestHandler(BaseHTTPRequestHandler):
    """HTTP请求处理器"""
    
    def __init__(self, *args, adapter=None, **kwargs):
        self.adapter = adapter
        super().__init__(*args, **kwargs)
    
    def _set_headers(self, status=200, content_type="application/json"):
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Cache-Control", "max-age=5, stale-while-revalidate=10")
        
        # CORS支持
        origin = self.headers.get("Origin", "")
        if origin in CORS_ORIGINS:
            self.send_header("Access-Control-Allow-Origin", origin)
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()
    
    def _json_response(self, data, status=200):
        self._set_headers(status)
        self.wfile.write(json.dumps(data).encode("utf-8"))
    
    def do_OPTIONS(self):
        """处理CORS预检请求"""
        self._set_headers(204)
    
    def do_GET(self):
        """处理GET请求（单只查询）"""
        parsed = urlparse(self.path)
        path = parsed.path
        query = parse_qs(parsed.query)
        
        if path not in ["/querylocal", "/api/querylocal"]:
            self._json_response({"detail": "Not found"}, 404)
            return
        
        code = query.get("code", [None])[0]
        type_ = query.get("type", [None])[0]
        
        if not code or not type_:
            self._json_response({"detail": "Missing code or type"}, 400)
            return
        
        qmt_code, currency = self.adapter._convert_code(code)
        if not qmt_code:
            self._json_response({"detail": f"Unsupported code format: {code}"}, 400)
            return
        
        try:
            if type_ == "price":
                result = self.adapter.fetch_price_single(qmt_code, code, currency)
                if result:
                    self._json_response(result)
                else:
                    self._json_response({"detail": f"Price data not found for {code}"}, 404)
            
            elif type_ == "intraday":
                # 获取昨收价
                prev_close = None
                try:
                    tick = self.adapter.ContextInfo.get_full_tick([qmt_code])
                    if tick and qmt_code in tick:
                        prev_close = tick[qmt_code].get("lastClose")
                except Exception as e:
                    print(f"[WARN] get tick for prev_close {qmt_code}: {e}")
                
                if prev_close is None:
                    try:
                        detail = self.adapter.ContextInfo.get_instrument_detail(qmt_code, iscomplete=False)
                        if detail:
                            prev_close = detail.get("PreClose")
                    except Exception:
                        pass
                
                result = self.adapter.fetch_intraday_single(qmt_code, code, self.adapter.get_last_trade_date(), prev_close)
                if result:
                    self._json_response(result)
                else:
                    self._json_response({"detail": f"Intraday data not found for {code}"}, 404)
            
            elif type_ in ("info", "movingaveragedata"):
                self._json_response({"detail": f"{type_} not supported in local QMT service"}, 501)
            
            else:
                self._json_response({
                    "detail": "Invalid 'type' parameter. Use 'price', 'info', 'movingaveragedata', or 'intraday'."
                }, 400)
        
        except Exception as e:
            self._json_response({"detail": str(e)}, 500)
    
    def do_POST(self):
        """处理POST请求（批量查询）"""
        parsed = urlparse(self.path)
        path = parsed.path
        
        if path not in ["/querylocal", "/api/querylocal"]:
            self._json_response({"detail": "Not found"}, 404)
            return
        
        content_length = int(self.headers.get("Content-Length", 0))
        post_data = self.rfile.read(content_length)
        
        try:
            data = json.loads(post_data.decode("utf-8"))
        except json.JSONDecodeError:
            self._json_response({"detail": "Invalid JSON body"}, 400)
            return
        
        codes = data.get("codes", [])
        type_ = data.get("type")
        
        if not codes or not type_:
            self._json_response({"detail": "Missing codes or type"}, 400)
            return
        
        if len(codes) > 50:
            self._json_response({"detail": "Too many codes, max 50"}, 400)
            return
        
        # 转换代码
        codes_info = []
        for code in codes:
            qmt_code, currency = self.adapter._convert_code(code)
            if qmt_code:
                codes_info.append((code, qmt_code, currency))
        
        if not codes_info:
            self._json_response({"detail": "No valid codes"}, 400)
            return
        
        try:
            if type_ == "price":
                results = self.adapter.fetch_price_batch(codes_info)
                self._json_response(results)
            
            elif type_ == "intraday":
                results = self.adapter.fetch_intraday_batch(codes_info)
                self._json_response(results)
            
            else:
                self._json_response({
                    "detail": "Batch only supports 'price' or 'intraday'"
                }, 400)
        
        except Exception as e:
            self._json_response({"detail": str(e)}, 500)
    
    def log_message(self, format, *args):
        """重写日志方法，减少输出"""
        print(f"[HTTP] {self.address_string()} - {format % args}")


# ==============================
# QMT策略模型入口
# ==============================
def init(ContextInfo):
    """策略初始化"""
    ContextInfo.accID = "你的资金账号"  # 必须与策略绑定的资金账号一致
    
    # 初始化数据适配器
    ContextInfo.adapter = QMTDataAdapter(ContextInfo)
    
    # 启动HTTP服务
    print("=" * 60)
    print("QMT内置Python环境 - 本地数据服务")
    print("替代原miniQMT HTTP服务")
    print(f"Listening on http://0.0.0.0:{PORT}")
    print("Routes: /querylocal, /api/querylocal")
    print("Batch: POST /api/querylocal {codes: [...], type: 'intraday'}")
    print("=" * 60)
    
    # 启动HTTP服务器
    server_address = ("", PORT)
    httpd = HTTPServer(server_address, QMTRequestHandler)
    
    # 设置适配器
    def handler_factory(*args, **kwargs):
        return QMTRequestHandler(*args, adapter=ContextInfo.adapter, **kwargs)
    
    httpd.RequestHandlerClass = handler_factory
    
    # 在独立线程中运行
    import threading
    server_thread = threading.Thread(target=httpd.serve_forever)
    server_thread.daemon = True
    server_thread.start()
    
    print(f"HTTP服务已启动，端口: {PORT}")
    ContextInfo.httpd = httpd


def handlebar(ContextInfo):
    """K线驱动函数（本服务不依赖K线）"""
    pass


def stop(ContextInfo):
    """策略停止"""
    if hasattr(ContextInfo, "httpd"):
        ContextInfo.httpd.shutdown()
        print("HTTP服务已停止")
