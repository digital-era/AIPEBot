# -*- coding: gbk -*-
# QMT 数据服务策略 - 终极稳定版（支持名称 + 港股 + 1秒定时检测）
import os
import json
import time
from datetime import datetime
import pandas as pd

# ========== 配置 ==========
COMMAND_FILE = r"E:\AIPEQModelSIRIUS\QMTAPI\qmt_command.txt"
RESPONSE_FILE = r"E:\AIPEQModelSIRIUS\QMTAPI\qmt_response.txt"

# ========== 状态字典 ==========
G_STATE = {
    'init_done': False,
    'last_cmd_time': 0.0,
    'last_notify_time': 0.0
}

# ---------- 工具函数 ----------
def _norm_code(code):
    """更完善的代码标准化，特别支持港股"""
    code = str(code).strip().upper()
    
    # 已经是标准格式
    if '.' in code:
        return code
    
    # 港股处理（支持 HK00700、00700、700 等）
    if code.startswith('HK'):
        pure = code[2:].lstrip('0') or '0'
        return f"{pure.zfill(5)}.HK"
    
    # A股
    if code.startswith(('6', '5', '9')):
        return code + '.SH'
    if code.startswith(('0', '1', '2', '3')):
        return code + '.SZ'
    if code.startswith(('4', '8')):
        return code + '.BJ'
    
    # 纯数字港股尝试（4~5位）
    if code.isdigit() and 4 <= len(code) <= 5:
        return f"{code.zfill(5)}.HK"
    
    return code

def _write_response(response_data):
    try:
        with open(RESPONSE_FILE, 'w', encoding='utf-8') as f:
            json.dump(response_data, f, ensure_ascii=False)
            f.flush()
            os.fsync(f.fileno())

        with open(RESPONSE_FILE + '.notify', 'w', encoding='utf-8') as f:
            f.write(str(time.time()))
            f.flush()
            os.fsync(f.fileno())

        print(f"[WRITE] 响应已写入 | id={response_data.get('request_id')} | status={response_data.get('status')}")
    except Exception as e:
        print(f'[ERROR] _write_response: {e}')

def _get_full_tick(ContextInfo, stock_codes):
    result = {}
    try:
        for code in stock_codes:
            norm_code = _norm_code(code)
            tick = ContextInfo.get_full_tick([norm_code])
            if tick and norm_code in tick:
                d = tick[norm_code]
                result[norm_code] = {
                    'lastPrice': float(d.get('lastPrice', 0) or 0),
                    'lastClose': float(d.get('lastClose', 0) or 0),
                    'high': float(d.get('high', 0) or 0),
                    'low': float(d.get('low', 0) or 0),
                    'volume': float(d.get('volume', 0) or 0),
                    'amount': float(d.get('amount', 0) or 0)
                }
            else:
                print(f'[WARN] get_full_tick 无数据: {norm_code}')
    except Exception as e:
        print(f'[ERROR] _get_full_tick: {e}')
    return result

def _get_market_data_ex(ContextInfo, fields, stock_codes, period, start_time, end_time):
    try:
        norm_codes = [_norm_code(c) for c in stock_codes]
        data = ContextInfo.get_market_data_ex(
            fields=fields,
            stock_code=norm_codes,
            period=period,
            start_time=start_time,
            end_time=end_time,
            count=-1,
            dividend_type='none',
            fill_data=True,
            subscribe=True
        )
        result = {}
        if data:
            for code, df in data.items():
                if df is not None and not df.empty:
                    result[code] = []
                    for idx, row in df.iterrows():
                        row_dict = {}
                        for field in fields:
                            if field in row:
                                val = row[field]
                                if pd.notna(val):
                                    if isinstance(val, pd.Timestamp):
                                        row_dict[field] = val.isoformat()
                                    else:
                                        row_dict[field] = float(val) if isinstance(val, (int, float)) else val
                                else:
                                    row_dict[field] = None
                        result[code].append(row_dict)
        return result
    except Exception as e:
        print(f'[ERROR] _get_market_data_ex: {e}')
        return {}

def _get_trading_dates(ContextInfo, market, start_time, end_time, count):
    try:
        dates = ContextInfo.get_trading_dates(market, start_time, end_time, count)
        return dates if dates else []
    except Exception as e:
        print(f'[ERROR] _get_trading_dates: {e}')
        return []

def _get_instrument_detail(ContextInfo, stock_code, iscomplete=False):
    """兼容不同 QMT 版本的 get_instrument_detail"""
    try:
        norm_code = _norm_code(stock_code)
        
        # 先尝试不带 iscomplete 的调用（你的版本只支持2个参数）
        try:
            detail = ContextInfo.get_instrument_detail(norm_code)
        except TypeError:
            # 如果上面失败，再尝试带 iscomplete 的版本
            try:
                detail = ContextInfo.get_instrument_detail(norm_code, iscomplete)
            except Exception as e2:
                print(f'[ERROR] get_instrument_detail 两种方式都失败: {e2}')
                return {}
        
        if not detail:
            print(f'[WARN] get_instrument_detail 无数据: {norm_code}')
            return {}
        
        # 尝试多种可能的字段名
        name = (
            detail.get('InstrumentName') or 
            detail.get('instrument_name') or 
            detail.get('Name') or 
            detail.get('name') or 
            detail.get('InstrumentID') or 
            norm_code
        )
        
        return {
            'InstrumentName': name,
            'ExchangeID': detail.get('ExchangeID') or detail.get('exchange_id') or '',
            'ProductID': detail.get('ProductID') or detail.get('product_id') or '',
            'PreClose': detail.get('PreClose') or detail.get('pre_close') or detail.get('LastClose')
        }
    except Exception as e:
        print(f'[ERROR] _get_instrument_detail {stock_code}: {e}')
        return {}

def _process_command(ContextInfo, command):
    function = command.get('function')
    params = command.get('params', {})
    req_id = command.get('request_id', '')

    try:
        if function == 'get_full_tick':
            data = _get_full_tick(ContextInfo, params.get('stock_codes', []))
            return {'status': 'success', 'data': data, 'request_id': req_id}

        elif function == 'get_market_data_ex':
            data = _get_market_data_ex(
                ContextInfo,
                params.get('fields', []),
                params.get('stock_codes', []),
                params.get('period', '1d'),
                params.get('start_time', ''),
                params.get('end_time', '')
            )
            return {'status': 'success', 'data': data, 'request_id': req_id}

        elif function == 'get_trading_dates':
            data = _get_trading_dates(
                ContextInfo,
                params.get('market', 'SH'),
                params.get('start_time', ''),
                params.get('end_time', ''),
                params.get('count', -1)
            )
            return {'status': 'success', 'data': data, 'request_id': req_id}

        elif function == 'download_history_data':
            try:
                code = _norm_code(params.get('stock_code', ''))
                ContextInfo.download_history_data(
                    code,
                    params.get('period', '1d'),
                    params.get('start_time', ''),
                    params.get('end_time', '')
                )
                return {'status': 'success', 'data': {'success': True}, 'request_id': req_id}
            except Exception as e:
                print(f'[WARN] download_history_data 失败（已忽略）: {e}')
                # 即使下载失败也返回 success，让后续 get_market_data_ex 继续尝试
                return {'status': 'success', 'data': {'success': False, 'error': str(e)}, 'request_id': req_id}

        elif function == 'get_instrument_detail':
            data = _get_instrument_detail(
                ContextInfo,
                params.get('stock_code', ''),
                params.get('iscomplete', False)
            )
            return {'status': 'success', 'data': data, 'request_id': req_id}

        else:
            return {'status': 'error', 'message': f'Unknown function: {function}', 'request_id': req_id}

    except Exception as e:
        return {'status': 'error', 'message': str(e), 'request_id': req_id}

# ---------- 框架入口 ----------
def init(ContextInfo):
    print('=' * 50)
    print('[INIT] 数据服务策略启动 (终极稳定版 - 支持名称+港股)')
    ContextInfo.accID = ''   # ← 改成你自己的资金账号

    G_STATE['init_done'] = True
    G_STATE['last_cmd_time'] = 0.0
    G_STATE['last_notify_time'] = 0.0

    # 清理旧文件
    for f in [COMMAND_FILE, RESPONSE_FILE, COMMAND_FILE + '.notify', RESPONSE_FILE + '.notify']:
        if os.path.exists(f):
            try:
                os.remove(f)
            except:
                pass

    # ========== 关键修改：强制开启 1秒定时检测 ==========
    # 即使策略是日线模式，也能每秒主动检测一次命令文件
    try:
        ContextInfo.run_time("check_command", "1nSecond", "2010-01-01 00:00:00")
        print('[INIT] 已启动 1秒 定时检测（解决日线模式响应慢问题）')
    except Exception as e:
        print(f'[WARN] 启动定时器失败: {e}')
        print('[WARN] 请把策略周期改成 1分钟 或 5分钟 再试')

    print('[INIT] 初始化完成，等待命令...')

def check_command(ContextInfo):
    """定时器回调，每秒强制检测命令"""
    handlebar(ContextInfo)

def handlebar(ContextInfo):
    if not G_STATE.get('init_done'):
        return

    try:
        has_cmd = False

        # 检测命令文件修改时间
        if os.path.exists(COMMAND_FILE):
            mod_time = os.path.getmtime(COMMAND_FILE)
            if mod_time > G_STATE['last_cmd_time']:
                G_STATE['last_cmd_time'] = mod_time
                has_cmd = True

        # 检测 notify 文件
        notify_file = COMMAND_FILE + '.notify'
        if os.path.exists(notify_file):
            n_time = os.path.getmtime(notify_file)
            if n_time > G_STATE['last_notify_time']:
                G_STATE['last_notify_time'] = n_time
                has_cmd = True

        if not has_cmd:
            return

        if not os.path.exists(COMMAND_FILE):
            return

        with open(COMMAND_FILE, 'r', encoding='utf-8') as f:
            command = json.load(f)

        print(f'[HANDLEBAR] 收到命令: {command.get("function")} | id={command.get("request_id")}')

        response = _process_command(ContextInfo, command)
        _write_response(response)

        # 删除命令文件
        try:
            if os.path.exists(COMMAND_FILE):
                os.remove(COMMAND_FILE)
            if os.path.exists(notify_file):
                os.remove(notify_file)
        except Exception as e:
            print(f'[WARN] 删除命令文件失败: {e}')

        print(f'[HANDLEBAR] 命令处理完成: {command.get("function")}')

    except Exception as e:
        print(f'[ERROR] handlebar 处理异常: {e}')
        try:
            _write_response({
                'status': 'error',
                'message': str(e),
                'request_id': ''
            })
        except:
            pass

def stop(ContextInfo):
    G_STATE['init_done'] = False
    print('[STOP] 策略停止')
