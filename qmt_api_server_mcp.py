# ============================================================
# qmt_api_server_mcp.py
#
# Windows 本地行情数据服务
#
# 原版本：
#     Flask -> miniQMT / xtdata
#
# 当前版本：
#     Flask -> 财搭子 MCP

# 使用方法：
# pip uninstall mcp -y
# pip install -U "mcp"
# pip install flask flask-cors
# python -c "import mcp; print(mcp.__version__)"

# $env:CAIDAZI_API_KEY="你的API_KEY"
# python qmt_api_server.py
#
# ============================================================
#
# 【兼容原则】
#
# 1. 不修改原有 Flask API
# 2. 不修改 /querylocal
# 3. 不修改 /api/querylocal
# 4. GET 仍然是单股票查询
# 5. POST 仍然是批量查询
# 6. type=price 保持原返回结构
# 7. type=intraday 保持原返回结构
# 8. avg_price 保持原计算方式
# 9. 第一根分钟 price 继续使用 prev_close
# 10. 原来的 date 参数继续支持
# 11. 批量最多 50 个股票
# 12. miniQMT / xtdata 完全移除
#
# ============================================================

from flask import Flask, request, jsonify
from flask_cors import CORS

import asyncio
import datetime
import os
import re
import traceback

from mcp import ClientSession
from mcp.client.streamable_http import streamable_http_client


# ============================================================
# Flask
# ============================================================

app = Flask(__name__)

CORS(
    app,
    origins=[
        "https://aivibeinvestment.com",
    ],
)


# ============================================================
# 财搭子 MCP 配置
# ============================================================

MCP_SERVER_URL = os.environ.get(
    "CAIDAZI_MCP_URL",
    "https://mcp.zhicepilot.com/"
)

CAIDAZI_API_KEY = os.environ.get(
    "CAIDAZI_API_KEY"
)

# MCP 工具
MCP_TOOL_REALTIME_1M = (
    "get_a_share_realtime_1m_price"
)

MCP_TOOL_HISTORY_1M = (
    "get_a_share_history_1m_price"
)

# MCP 单次请求超时
MCP_TIMEOUT = float(
    os.environ.get(
        "CAIDAZI_MCP_TIMEOUT",
        "15"
    )
)


# ============================================================
# JSON 返回
# ============================================================

def json_response(data, status=200):

    response = jsonify(data)

    response.status_code = status

    response.headers[
        "Content-Type"
    ] = "application/json"

    response.headers[
        "Cache-Control"
    ] = "max-age=5, stale-while-revalidate=10"

    return response


# ============================================================
# 代码转换
#
# 保留原有转换逻辑，并增加 BJ。
#
# A股：
#   600519       -> 600519.SH
#   688xxx       -> 688xxx.SH
#   300750       -> 300750.SZ
#   000001       -> 000001.SZ
#   920xxx       -> 920xxx.BJ
#
# 港股：
#   HK00700      -> 00700.HK
#
# 注意：
# 财搭子分钟 Tool 只支持 A 股股票。
# ============================================================

def convert_code(code):

    if not code:
        return None, None

    code = str(code).strip()

    code_upper = code.upper()

    # --------------------------------------------------------
    # 已经是标准代码
    # --------------------------------------------------------

    if re.match(
        r"^\d{6}\.SH$",
        code_upper
    ):

        return code_upper, "CNY"

    if re.match(
        r"^\d{6}\.SZ$",
        code_upper
    ):

        return code_upper, "CNY"

    if re.match(
        r"^\d{6}\.BJ$",
        code_upper
    ):

        return code_upper, "CNY"

    if re.match(
        r"^\d{5}\.HK$",
        code_upper
    ):

        return code_upper, "HKD"

    # --------------------------------------------------------
    # 港股
    # --------------------------------------------------------

    if code_upper.startswith("HK"):

        pure = code_upper[2:]

        if pure.isdigit():

            return f"{pure}.HK", "HKD"

        return None, None

    # --------------------------------------------------------
    # 上海 A 股
    #
    # 保留原有：
    # 60 / 68 / 51 / 56 / 58 / 55 / 900
    #
    # 其中分钟 MCP 最终只接受 A 股股票。
    # 因此 ETF 等非股票在 MCP 层会被拒绝。
    # --------------------------------------------------------

    if re.match(
        r"^(60|68|90)",
        code
    ):

        return f"{code}.SH", "CNY"

    # --------------------------------------------------------
    # 深圳 A 股
    # --------------------------------------------------------

    if re.match(
        r"^(00|30)",
        code
    ):

        return f"{code}.SZ", "CNY"

    # --------------------------------------------------------
    # 北交所
    # --------------------------------------------------------

    if re.match(
        r"^92",
        code
    ):

        return f"{code}.BJ", "CNY"

    return None, None


# ============================================================
# 标准化 A 股代码
#
# 财搭子分钟 Tool 只接受：
#
# SH / SZ / BJ
#
# 并且只支持 A 股股票。
# ============================================================

def normalize_a_share_code(code):

    if not code:
        return None

    text = str(code).strip().upper()

    # 已经是标准 ts_code
    if re.match(
        r"^\d{6}\.(SH|SZ|BJ)$",
        text
    ):

        return text

    converted, currency = (
        convert_code(text)
    )

    if not converted:
        return None

    if currency != "CNY":
        return None

    if not re.match(
        r"^\d{6}\.(SH|SZ|BJ)$",
        converted
    ):

        return None

    return converted


# ============================================================
# 去重
#
# MCP 文档要求：
# 重复代码按首次出现去重。
# ============================================================

def deduplicate_symbols(symbols):

    result = []

    seen = set()

    for symbol in symbols:

        normalized = (
            normalize_a_share_code(
                symbol
            )
        )

        if not normalized:
            continue

        if normalized in seen:
            continue

        seen.add(normalized)

        result.append(normalized)

    return result


# ============================================================
# 当前上海时间
# ============================================================

def now_shanghai():

    try:

        from zoneinfo import ZoneInfo

        return datetime.datetime.now(
            ZoneInfo("Asia/Shanghai")
        )

    except Exception:

        # Windows / Python 旧环境备用
        return (
            datetime.datetime.utcnow()
            + datetime.timedelta(
                hours=8
            )
        )


# ============================================================
# 最近工作日 fallback
#
# 注意：
# 这只是 Flask 旧接口没有 date 时的 fallback。
#
# 真正交易日解析由 MCP history Tool 负责。
# ============================================================

def get_last_trade_date():

    now = now_shanghai()

    weekday = now.weekday()

    if weekday == 0:
        days_back = 3

    elif weekday == 6:
        days_back = 2

    elif weekday == 5:
        days_back = 1

    else:
        days_back = 1

    return (
        now -
        datetime.timedelta(
            days=days_back
        )
    ).strftime("%Y%m%d")


# ============================================================
# MCP HTTP Header
#
# 财搭子 README 要求使用：
#
# CAIDAZI_API_KEY
#
# 这里不把 Key 写死在程序。
# ============================================================

def build_mcp_headers():

    headers = {}

    if CAIDAZI_API_KEY:

        headers[
            "Authorization"
        ] = (
            "Bearer "
            + CAIDAZI_API_KEY
        )

    return headers


# ============================================================
# MCP 调用
#
# 使用官方 MCP Python SDK。
#
# 不自己手写：
#   initialize
#   tools/call
#   session-id
#   JSON-RPC
#
# 这样可以兼容 MCP 新旧协议。
# ============================================================

async def _mcp_call_tool_async(
    tool_name,
    arguments
):

    if not CAIDAZI_API_KEY:

        raise RuntimeError(
            "CAIDAZI_API_KEY is not configured. "
            "Please set the environment variable "
            "CAIDAZI_API_KEY."
        )

    # --------------------------------------------------------
    # MCP Streamable HTTP
    #
    # 使用 SDK 官方 transport。
    # --------------------------------------------------------

    try:

        # 当前 MCP SDK 推荐：
        #
        # streamable_http_client(
        #     url,
        #     http_client=...
        # )
        #
        # 但不同 MCP SDK 小版本在自定义
        # http client 上存在 API 差异。
        #
        # 这里优先尝试 SDK 1.x 兼容形式。
        # ----------------------------------------------------

        try:

            async with streamable_http_client(

                MCP_SERVER_URL,

                headers=build_mcp_headers(),

                timeout=MCP_TIMEOUT,

                sse_read_timeout=MCP_TIMEOUT

            ) as (
                read_stream,
                write_stream
            ):

                async with ClientSession(
                    read_stream,
                    write_stream
                ) as session:

                    await session.initialize()

                    result = (
                        await session.call_tool(
                            tool_name,
                            arguments
                        )
                    )

                    return result

        except TypeError as first_error:

            # ------------------------------------------------
            # MCP Python SDK 2.x：
            #
            # streamable_http_client()
            # 已取消 headers/timeout 参数。
            #
            # 这时使用 SDK 提供的
            # httpx2 client。
            # ------------------------------------------------

            try:

                import httpx2

                from mcp.shared._httpx_utils import (
                    create_mcp_http_client
                )

                http_client = (
                    create_mcp_http_client(
                        headers=build_mcp_headers(),
                        timeout=httpx2.Timeout(
                            MCP_TIMEOUT,
                            read=MCP_TIMEOUT
                        )
                    )
                )

                async with http_client:

                    async with (
                        streamable_http_client(

                            MCP_SERVER_URL,

                            http_client=
                                http_client

                        )
                        as (
                            read_stream,
                            write_stream
                        )
                    ):

                        async with ClientSession(

                            read_stream,
                            write_stream

                        ) as session:

                            await session.initialize()

                            result = (
                                await session.call_tool(
                                    tool_name,
                                    arguments
                                )
                            )

                            return result

            except ImportError as second_error:

                raise RuntimeError(
                    "MCP Python SDK 2.x requires "
                    "httpx2. "
                    "Please reinstall the latest "
                    "MCP Python SDK."
                ) from second_error

            except Exception:

                # 如果 2.x 方案失败，
                # 抛出真实错误而不是吞掉。
                raise

    except Exception as e:

        raise RuntimeError(
            f"MCP tool '{tool_name}' failed: {e}"
        ) from e


# ============================================================
# 同步 MCP 调用
#
# Flask 是同步框架。
#
# 每一次 HTTP 请求：
#     Flask thread
#          ↓
#     asyncio.run()
#          ↓
#     MCP
#
# 不保存跨请求 AsyncSession，
# 避免 event loop / thread 混用。
# ============================================================

def mcp_call_tool(
    tool_name,
    arguments
):

    try:

        return asyncio.run(
            _mcp_call_tool_async(
                tool_name,
                arguments
            )
        )

    except RuntimeError as e:

        # ----------------------------------------------------
        # 极少数情况下，如果 Flask 上下文已经存在
        # event loop，给出清晰错误。
        # ----------------------------------------------------

        raise RuntimeError(
            f"MCP call failed: {e}"
        ) from e


# ============================================================
# MCP 返回结果解析
#
# MCP ClientResult 通常：
#
# result.content
#
# 其中 text 内容可能是 JSON。
# ============================================================

def extract_mcp_result(result):

    if result is None:

        return None

    # --------------------------------------------------------
    # MCP CallToolResult
    # --------------------------------------------------------

    content = getattr(
        result,
        "content",
        None
    )

    if content:

        # 优先 structuredContent
        structured = getattr(
            result,
            "structuredContent",
            None
        )

        if structured is not None:

            return structured

        structured = getattr(
            result,
            "structured_content",
            None
        )

        if structured is not None:

            return structured

        # text
        for item in content:

            text = getattr(
                item,
                "text",
                None
            )

            if not text:
                continue

            text = str(text).strip()

            try:

                import json

                return json.loads(
                    text
                )

            except Exception:

                continue

    # --------------------------------------------------------
    # 如果 MCP SDK 返回 dict
    # --------------------------------------------------------

    if isinstance(
        result,
        dict
    ):

        if (
            "structuredContent"
            in result
        ):

            return result[
                "structuredContent"
            ]

        if (
            "structured_content"
            in result
        ):

            return result[
                "structured_content"
            ]

        if (
            "status"
            in result
        ):

            return result

    return result


# ============================================================
# MCP 工具结果标准化
# ============================================================

def validate_mcp_result(result):

    data = extract_mcp_result(
        result
    )

    if data is None:

        raise RuntimeError(
            "MCP returned empty result."
        )

    if not isinstance(
        data,
        dict
    ):

        raise RuntimeError(
            "MCP returned unexpected "
            "result structure."
        )

    # --------------------------------------------------------
    # MCP Tool 级错误
    # --------------------------------------------------------

    if data.get(
        "status"
    ) == "failed":

        error = data.get(
            "error"
        )

        if isinstance(
            error,
            dict
        ):

            message = error.get(
                "message",
                "MCP tool failed"
            )

        else:

            message = str(
                error
                or data.get(
                    "resp_for_llm",
                    "MCP tool failed"
                )
            )

        raise RuntimeError(
            message
        )

    return data


# ============================================================
# MCP：实时 1 分钟
#
# Tool：
# get_a_share_realtime_1m_price
#
# 一次批量调用。
# ============================================================

def fetch_mcp_realtime_1m(
    symbols,
    include_incomplete=True
):

    symbols = deduplicate_symbols(
        symbols
    )

    if not symbols:

        return None

    if len(symbols) > 50:

        raise ValueError(
            "symbols size must be between 1 and 50"
        )

    result = mcp_call_tool(

        MCP_TOOL_REALTIME_1M,

        {
            "symbols": symbols,

            "include_incomplete":
                bool(include_incomplete)
        }

    )

    return validate_mcp_result(
        result
    )


# ============================================================
# MCP：历史 2 个交易日
#
# Tool：
# get_a_share_history_1m_price
# ============================================================

def fetch_mcp_history_1m(
    symbols,
    end_date=None
):

    symbols = deduplicate_symbols(
        symbols
    )

    if not symbols:

        return None

    if len(symbols) > 50:

        raise ValueError(
            "symbols size must be between 1 and 50"
        )

    arguments = {

        "symbols": symbols,

        "trading_days": 2

    }

    if end_date:

        end_date = (
            str(end_date)
            .replace("-", "")
        )

        # ----------------------------------------------------
        # 基本格式检查
        # ----------------------------------------------------

        if not re.match(
            r"^\d{8}$",
            end_date
        ):

            raise ValueError(
                "date must be YYYYMMDD"
            )

        arguments[
            "end_date"
        ] = end_date

    result = mcp_call_tool(

        MCP_TOOL_HISTORY_1M,

        arguments

    )

    return validate_mcp_result(
        result
    )


# ============================================================
# MCP bar -> 原有前端分钟结构
#
# 原前端结构：
#
# {
#     date,
#     time,
#     price,
#     avg_price,
#     volume
# }
#
# 绝不返回 MCP 原始 bar。
# ============================================================

def convert_bar_to_legacy(
    bar,
    prev_close=None,
    is_first=False,
    cumulative_amount=0.0,
    cumulative_volume=0.0
):

    if not bar:

        return None

    bar_time = bar.get(
        "bar_time"
    )

    if not bar_time:

        return None

    # --------------------------------------------------------
    # ISO 8601
    # --------------------------------------------------------

    try:

        dt = datetime.datetime.fromisoformat(
            str(bar_time).replace(
                "Z",
                "+00:00"
            )
        )

        date_str = dt.strftime(
            "%Y-%m-%d"
        )

        time_str = dt.strftime(
            "%H:%M:%S"
        )

    except Exception:

        text = str(
            bar_time
        )

        if "T" in text:

            date_str = text[
                :10
            ]

            time_str = text[
                11:19
            ]

        else:

            return None

    # --------------------------------------------------------
    # close
    # --------------------------------------------------------

    close = bar.get(
        "close"
    )

    if close is None:

        return None

    close = float(
        close
    )

    # --------------------------------------------------------
    # 保留原来的特殊逻辑：
    #
    # 第一根：
    #     price = prev_close
    #
    # 后续：
    #     price = close
    # --------------------------------------------------------

    if (
        is_first
        and prev_close is not None
    ):

        price = float(
            prev_close
        )

    else:

        price = close

    # --------------------------------------------------------
    # volume
    # --------------------------------------------------------

    try:

        volume = float(
            bar.get(
                "volume",
                0
            ) or 0
        )

    except Exception:

        volume = 0.0

    # --------------------------------------------------------
    # amount
    # --------------------------------------------------------

    try:

        amount = float(
            bar.get(
                "amount",
                0
            ) or 0
        )

    except Exception:

        amount = 0.0

    # --------------------------------------------------------
    # 原 avg_price 算法
    # --------------------------------------------------------

    cumulative_amount += (
        amount
    )

    if volume > 0:

        cumulative_volume += (
            volume
        )

    if cumulative_volume:

        avg_price = round(
            cumulative_amount
            / cumulative_volume,
            6
        )

    else:

        avg_price = price

    return {

        "data": {

            "date": date_str,

            "time": time_str,

            "price": price,

            "avg_price": avg_price,

            "volume": volume

        },

        "cumulative_amount":
            cumulative_amount,

        "cumulative_volume":
            cumulative_volume
    }


# ============================================================
# MCP history item -> 原有 intraday
#
# 新 MCP：
#
# days:
#   day1
#   day2
#
# 原应用：
#
# intraday = 一个交易日的分钟数组
#
# 所以：
#     优先取 end_date
#     没指定则取 MCP 最后一个交易日
#
# 不把两个交易日拼在一起。
# ============================================================

def convert_history_item_to_legacy(
    item,
    preferred_trade_date=None
):

    if not item:

        return None

    days = item.get(
        "days",
        []
    )

    if not isinstance(
        days,
        list
    ):

        return None

    if not days:

        return None

    selected_day = None

    # --------------------------------------------------------
    # 如果指定日期：
    # 优先精确寻找
    # --------------------------------------------------------

    if preferred_trade_date:

        target = (
            str(
                preferred_trade_date
            )
            .replace(
                "-",
                ""
            )
        )

        for day in days:

            trade_date = str(
                day.get(
                    "trade_date",
                    ""
                )
            ).replace(
                "-",
                ""
            )

            if trade_date == target:

                selected_day = day

                break

    # --------------------------------------------------------
    # 没指定 / 指定日期没找到：
    #
    # 取最后一个 MCP 返回的交易日
    # --------------------------------------------------------

    if selected_day is None:

        selected_day = days[-1]

    bars = selected_day.get(
        "bars",
        []
    )

    if not isinstance(
        bars,
        list
    ):

        return None

    if not bars:

        return None

    # --------------------------------------------------------
    # 确保分钟升序
    #
    # MCP 文档已经保证升序，
    # 这里再次排序只是防御性处理。
    # 不改变数据值。
    # --------------------------------------------------------

    def bar_sort_key(bar):

        return str(
            bar.get(
                "bar_time",
                ""
            )
        )

    bars = sorted(
        bars,
        key=bar_sort_key
    )

    # --------------------------------------------------------
    # 第一根 bar 的 prev_close
    # --------------------------------------------------------

    prev_close = None

    if bars:

        try:

            prev_close = bars[0].get(
                "prev_close"
            )

            if prev_close is not None:

                prev_close = float(
                    prev_close
                )

        except Exception:

            prev_close = None

    result = []

    cumulative_amount = 0.0

    cumulative_volume = 0.0

    is_first = True

    for bar in bars:

        converted = (
            convert_bar_to_legacy(

                bar,

                prev_close=
                    prev_close,

                is_first=
                    is_first,

                cumulative_amount=
                    cumulative_amount,

                cumulative_volume=
                    cumulative_volume

            )
        )

        if not converted:

            continue

        result.append(
            converted["data"]
        )

        cumulative_amount = (
            converted[
                "cumulative_amount"
            ]
        )

        cumulative_volume = (
            converted[
                "cumulative_volume"
            ]
        )

        is_first = False

    return (
        result
        if result
        else None
    )


# ============================================================
# MCP history batch -> 原有 batch intraday
# ============================================================

def fetch_intraday_batch(
    codes_info,
    end_date=None
):

    # --------------------------------------------------------
    # 只取 A 股
    # --------------------------------------------------------

    symbols = []

    for (
        orig_code,
        qmt_code,
        currency
    ) in codes_info:

        normalized = (
            normalize_a_share_code(
                orig_code
            )
        )

        if normalized:

            symbols.append(
                normalized
            )

    symbols = deduplicate_symbols(
        symbols
    )

    if not symbols:

        return {}

    print(
        "[DEBUG] MCP history 1m "
        f"symbols={symbols}"
    )

    data = fetch_mcp_history_1m(

        symbols,

        end_date=end_date

    )

    if not data:

        return {}

    # --------------------------------------------------------
    # MCP 标准结构：
    #
    # {
    #     status,
    #     artifact_type,
    #     data: {
    #         timezone,
    #         requested_trading_days,
    #         resolved_trade_dates,
    #         items
    #     }
    # }
    # --------------------------------------------------------

    data_obj = data.get(
        "data"
    )

    if not isinstance(
        data_obj,
        dict
    ):

        return {}

    items = data_obj.get(
        "items",
        []
    )

    if not isinstance(
        items,
        list
    ):

        return {}

    # --------------------------------------------------------
    # 建立 MCP symbol -> 原始 code
    # --------------------------------------------------------

    symbol_map = {}

    for (
        orig_code,
        qmt_code,
        currency
    ) in codes_info:

        normalized = (
            normalize_a_share_code(
                orig_code
            )
        )

        if normalized:

            # 首次出现优先
            if normalized not in symbol_map:

                symbol_map[
                    normalized
                ] = orig_code

    results = {}

    for item in items:

        if not isinstance(
            item,
            dict
        ):

            continue

        symbol = item.get(
            "symbol"
        )

        if not symbol:

            continue

        orig_code = (
            symbol_map.get(
                symbol
            )
        )

        if not orig_code:

            continue

        try:

            result = (
                convert_history_item_to_legacy(

                    item,

                    preferred_trade_date=
                        end_date

                )
            )

            if result:

                results[
                    orig_code
                ] = result

                print(
                    "[DEBUG] "
                    f"{orig_code}: "
                    f"{len(result)} bars"
                )

        except Exception as e:

            print(
                "[WARN] convert "
                f"{symbol}: {e}"
            )

    return results


# ============================================================
# MCP realtime -> 原有 price
# ============================================================

def fetch_price_batch(
    codes_info
):

    symbols = []

    symbol_map = {}

    for (
        orig_code,
        qmt_code,
        currency
    ) in codes_info:

        normalized = (
            normalize_a_share_code(
                orig_code
            )
        )

        if not normalized:

            continue

        symbols.append(
            normalized
        )

        if normalized not in symbol_map:

            symbol_map[
                normalized
            ] = (
                orig_code,
                currency
            )

    symbols = deduplicate_symbols(
        symbols
    )

    if not symbols:

        return {}

    data = fetch_mcp_realtime_1m(

        symbols,

        include_incomplete=True

    )

    if not data:

        return {}

    data_obj = data.get(
        "data"
    )

    if not isinstance(
        data_obj,
        dict
    ):

        return {}

    items = data_obj.get(
        "items",
        []
    )

    if not isinstance(
        items,
        list
    ):

        return {}

    results = {}

    for item in items:

        if not isinstance(
            item,
            dict
        ):

            continue

        symbol = item.get(
            "symbol"
        )

        if not symbol:

            continue

        mapping = symbol_map.get(
            symbol
        )

        if not mapping:

            continue

        (
            orig_code,
            currency
        ) = mapping

        bar = item.get(
            "bar"
        )

        if not isinstance(
            bar,
            dict
        ):

            continue

        latest_price = bar.get(
            "close"
        )

        prev_close = bar.get(
            "prev_close"
        )

        if (
            latest_price is None
            or prev_close is None
        ):

            continue

        try:

            latest_price = float(
                latest_price
            )

            prev_close = float(
                prev_close
            )

        except Exception:

            continue

        change_amount = (
            latest_price
            - prev_close
        )

        if prev_close:

            change_percent = round(

                (
                    change_amount
                    / prev_close
                ) * 100,

                6

            )

        else:

            change_percent = 0.0

        name = (
            item.get(
                "name"
            )
            or orig_code
        )

        # ----------------------------------------------------
        # 保留原 price 返回结构
        # ----------------------------------------------------

        results[
            orig_code
        ] = {

            "name": name,

            "latestPrice":
                latest_price,

            "changePercent":
                change_percent,

            "changeAmount":
                change_amount,

            "source":
                "caidazi_mcp",

            "currency":
                currency,

            "dailydata":
                None
        }

    return results


# ============================================================
# 单只 price
# ============================================================

def fetch_price_single(
    qmt_code,
    orig_code,
    currency
):

    normalized = (
        normalize_a_share_code(
            orig_code
        )
    )

    if not normalized:

        return None

    results = fetch_price_batch(

        [
            (
                orig_code,
                normalized,
                currency
            )
        ]

    )

    return results.get(
        orig_code
    )


# ============================================================
# 单只 intraday
# ============================================================

def fetch_intraday_single(
    qmt_code,
    orig_code,
    trade_date=None,
    prev_close=None
):

    normalized = (
        normalize_a_share_code(
            orig_code
        )
    )

    if not normalized:

        return None

    data = fetch_mcp_history_1m(

        [normalized],

        end_date=trade_date

    )

    if not data:

        return None

    data_obj = data.get(
        "data"
    )

    if not isinstance(
        data_obj,
        dict
    ):

        return None

    items = data_obj.get(
        "items",
        []
    )

    if not items:

        return None

    item = items[0]

    return (
        convert_history_item_to_legacy(

            item,

            preferred_trade_date=
                trade_date

        )
    )


# ============================================================
# 单只查询
# ============================================================

def handle_querylocal_single():

    code = request.args.get(
        "code"
    )

    type_ = request.args.get(
        "type"
    )

    if not code or not type_:

        return json_response(
            {
                "detail":
                    "Missing code or type"
            },
            400
        )

    qmt_code, currency = (
        convert_code(code)
    )

    if not qmt_code:

        return json_response(
            {
                "detail":
                    f"Unsupported code format: {code}"
            },
            400
        )

    try:

        # ====================================================
        # price
        # ====================================================

        if type_ == "price":

            result = (
                fetch_price_single(

                    qmt_code,

                    code,

                    currency

                )
            )

            if result:

                return json_response(
                    result
                )

            return json_response(

                {
                    "detail":
                        f"Price data not found for {code}"
                },

                404

            )

        # ====================================================
        # intraday
        # ====================================================

        elif type_ == "intraday":

            trade_date = (
                request.args.get(
                    "date"
                )
            )

            if not trade_date:

                trade_date = (
                    get_last_trade_date()
                )

            result = (
                fetch_intraday_single(

                    qmt_code,

                    code,

                    trade_date=

                        trade_date

                )
            )

            if result:

                return json_response(
                    result
                )

            return json_response(

                {
                    "detail":
                        f"Intraday data not found for {code}"
                },

                404

            )

        # ====================================================
        # 保留原有行为
        # ====================================================

        elif type_ in (
            "info",
            "movingaveragedata"
        ):

            return json_response(

                {
                    "detail":
                        f"{type_} not supported "
                        "in local MCP service"
                },

                501

            )

        else:

            return json_response(

                {
                    "detail":
                        "Invalid 'type' parameter. "
                        "Use 'price', 'info', "
                        "'movingaveragedata', "
                        "or 'intraday'."
                },

                400

            )

    except ValueError as e:

        return json_response(

            {
                "detail": str(e)
            },

            400

        )

    except Exception as e:

        print(
            "[ERROR] single query:",
            e
        )

        traceback.print_exc()

        return json_response(

            {
                "detail":
                    str(e)
            },

            500

        )


# ============================================================
# 批量查询
# ============================================================

def handle_querylocal_batch():

    data = request.get_json(
        silent=True
    )

    if not data:

        return json_response(

            {
                "detail":
                    "Missing JSON body"
            },

            400

        )

    codes = data.get(
        "codes",
        []
    )

    type_ = data.get(
        "type"
    )

    if not codes or not type_:

        return json_response(

            {
                "detail":
                    "Missing codes or type"
            },

            400

        )

    if not isinstance(
        codes,
        list
    ):

        return json_response(

            {
                "detail":
                    "codes must be an array"
            },

            400

        )

    if len(codes) > 50:

        return json_response(

            {
                "detail":
                    "Too many codes, max 50"
            },

            400

        )

    # ========================================================
    # 转换所有代码
    # ========================================================

    codes_info = []

    for code in codes:

        qmt_code, currency = (
            convert_code(
                str(code)
            )
        )

        if qmt_code:

            codes_info.append(

                (
                    str(code),
                    qmt_code,
                    currency
                )

            )

    if not codes_info:

        return json_response(

            {
                "detail":
                    "No valid codes"
            },

            400

        )

    try:

        # ====================================================
        # price
        # ====================================================

        if type_ == "price":

            results = (
                fetch_price_batch(
                    codes_info
                )
            )

            return json_response(
                results
            )

        # ====================================================
        # intraday
        # ====================================================

        elif type_ == "intraday":

            end_date = data.get(
                "date"
            )

            results = (
                fetch_intraday_batch(

                    codes_info,

                    end_date=
                        end_date

                )
            )

            return json_response(
                results
            )

        else:

            return json_response(

                {
                    "detail":
                        "Batch only supports "
                        "'price' or 'intraday'"
                },

                400

            )

    except ValueError as e:

        return json_response(

            {
                "detail": str(e)
            },

            400

        )

    except Exception as e:

        print(
            "[ERROR] batch query:",
            e
        )

        traceback.print_exc()

        return json_response(

            {
                "detail":
                    str(e)
            },

            500

        )


# ============================================================
# 路由注册
# ============================================================

@app.route(
    "/querylocal",
    methods=[
        "GET",
        "POST",
        "OPTIONS"
    ]
)

@app.route(
    "/api/querylocal",
    methods=[
        "GET",
        "POST",
        "OPTIONS"
    ]
)

def querylocal():

    # --------------------------------------------------------
    # OPTIONS
    # --------------------------------------------------------

    if request.method == "OPTIONS":

        response = jsonify({})

        response.headers[
            "Access-Control-Allow-Origin"
        ] = request.headers.get(
            "Origin",
            "*"
        )

        response.headers[
            "Access-Control-Allow-Methods"
        ] = (
            "GET, POST, OPTIONS"
        )

        response.headers[
            "Access-Control-Allow-Headers"
        ] = "Content-Type"

        return response, 204

    # --------------------------------------------------------
    # POST
    # --------------------------------------------------------

    if request.method == "POST":

        return handle_querylocal_batch()

    # --------------------------------------------------------
    # GET
    # --------------------------------------------------------

    return handle_querylocal_single()


# ============================================================
# 404
# ============================================================

@app.errorhandler(404)

def not_found(error):

    return json_response(

        {
            "detail":
                "Not found"
        },

        404

    )


# ============================================================
# 500
# ============================================================

@app.errorhandler(500)

def server_error(error):

    return json_response(

        {
            "detail":
                "Internal server error"
        },

        500

    )


# ============================================================
# 启动
# ============================================================

if __name__ == "__main__":

    print(
        "=" * 70
    )

    print(
        "QMT Local Data Service "
        "-> Caidazi MCP"
    )

    print(
        "MCP Server:",
        MCP_SERVER_URL
    )

    print(
        "Realtime Tool:",
        MCP_TOOL_REALTIME_1M
    )

    print(
        "History Tool:",
        MCP_TOOL_HISTORY_1M
    )

    print(
        "CAIDAZI_API_KEY:",
        "configured"
        if CAIDAZI_API_KEY
        else "NOT CONFIGURED"
    )

    print(
        "Listening on "
        "http://0.0.0.0:8787"
    )

    print(
        "Routes:"
    )

    print(
        "  /querylocal"
    )

    print(
        "  /api/querylocal"
    )

    print(
        "Batch:"
    )

    print(
        "  POST /api/querylocal "
        "{codes: [...], type: 'intraday'}"
    )

    print(
        "=" * 70
    )

    app.run(
        host="0.0.0.0",
        port=8787,
        threaded=True
    )
