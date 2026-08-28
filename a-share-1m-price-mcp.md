# A 股 1 分钟价格数据 MCP 接口文档

## 1. 文档目标

为上层 Agent 提供两类 A 股 1 分钟价格数据：

1. **实时 1 分钟价格数据**：查询一个或多个标的当前交易日最新的 1 分钟 K 线。
2. **历史 1 分钟价格数据**：查询一个或多个标的最近 2 个有行情数据的交易日的完整 1 分钟 K 线。

本文档定义 MCP Tool 的输入、输出、数据口径和异常约定，不限定底层行情供应商。

## 2. 通用约定

### 2.1 标的代码

- 使用 `ts_code` 格式：上交所后缀 `.SH`，深交所后缀 `.SZ`，北交所后缀 `.BJ`。
- 示例：`600519.SH`、`300750.SZ`、`920000.BJ`。
- 本期仅支持 A 股股票；指数、ETF、可转债不在本接口范围内。

### 2.2 时间和交易日

- 时区固定为 `Asia/Shanghai`（UTC+8）。
- `trade_date` 格式为 `YYYYMMDD`。
- `bar_time` 格式为 ISO 8601，例如 `2026-08-18T09:31:00+08:00`。
- `bar_time` 表示分钟 K 线的**结束时间**；例如 `09:31` 表示 `[09:30:00, 09:31:00)`。
- 正常连续竞价时间为 `09:30–11:30`、`13:00–15:00`。集合竞价数据不生成独立分钟 K 线。
- 一个正常、未停牌的完整交易日最多返回 240 根 1 分钟 K 线。

### 2.3 行情口径

| 字段 | 口径 |
|------|------|
| `open` | 该分钟第一笔有效成交价 |
| `high` | 该分钟最高成交价 |
| `low` | 该分钟最低成交价 |
| `close` | 该分钟最后一笔有效成交价 |
| `volume` | 该分钟成交量，单位为股 |
| `amount` | 该分钟成交额，单位为元 |
| `prev_close` | 上一交易日收盘价 |
| `change` | `close - prev_close` |
| `change_pct` | `(close / prev_close - 1) * 100`，单位为 `%` |

- 默认返回**不复权**价格，`adjustment = "none"`。
- 无成交的分钟默认不补空 K 线；是否完整由 `is_complete` 和 `data_status` 表示。
- 所有价格和金额使用 JSON number；调用方不得依赖固定小数位。

### 2.4 批量和顺序

- 单次最多查询 50 个标的。
- 响应中的 `items` 与请求的 `symbols` 顺序一致。
- 单个标的失败不影响其他标的返回；失败信息写入该标的的 `error` 字段。
- 重复代码按首次出现去重。

## 3. Tool 一：实时 1 分钟价格

### 3.1 基本信息

| 属性 | 值 |
|------|----|
| Tool 名称 | `get_a_share_realtime_1m_price` |
| 分类 | `market_api` |
| 适用场景 | 盘中盯盘、实时价格判断、最新分钟行情展示 |
| 建议缓存 | 交易时段 5 秒；非交易时段 5 分钟 |

### 3.2 功能说明

返回每个标的当前交易日最新的一根 1 分钟 K 线。

- 交易时段内可返回正在形成的分钟 K 线，此时 `is_complete = false`。
- 午间休市或收盘后返回最近一根已完成 K 线，此时 `is_complete = true`。
- 非交易日返回最近交易日的最后一根 K 线，并通过 `market_status` 标识市场状态。

### 3.3 输入参数

| 参数 | 类型 | 必填 | 默认值 | 说明 |
|------|------|------|--------|------|
| `symbols` | `string[]` | 是 | - | A 股代码列表，1–50 个 |
| `include_incomplete` | `boolean` | 否 | `true` | 是否允许返回正在形成的分钟 K 线 |

请求示例：

```json
{
  "tool_name": "get_a_share_realtime_1m_price",
  "parameters": {
    "symbols": ["600519.SH", "300750.SZ"],
    "include_incomplete": true
  }
}
```

### 3.4 输出参数

统一外层结构：

| 字段 | 类型 | 说明 |
|------|------|------|
| `status` | `string` | `completed`、`partial` 或 `failed` |
| `artifact_type` | `string` | 固定为 `a_share_realtime_1m_price` |
| `resp_for_llm` | `string` | 面向 Agent 的简短结果摘要 |
| `data` | `object|null` | 结构化行情数据 |

`data` 结构：

| 字段 | 类型 | 说明 |
|------|------|------|
| `as_of` | `string` | 服务端生成响应的时间，ISO 8601 |
| `timezone` | `string` | 固定为 `Asia/Shanghai` |
| `market_status` | `string` | `pre_open`、`trading`、`lunch_break`、`closed`、`non_trading_day` |
| `items` | `array` | 各标的最新分钟行情 |

`items[]` 结构：

| 字段 | 类型 | 说明 |
|------|------|------|
| `symbol` | `string` | 标的代码 |
| `name` | `string|null` | 证券简称 |
| `trade_date` | `string|null` | 行情所属交易日 |
| `bar` | `object|null` | 最新 1 分钟 K 线，字段见通用行情口径 |
| `data_status` | `string` | `normal`、`delayed`、`suspended`、`no_data` |
| `delay_seconds` | `integer|null` | 数据相对当前时间的延迟秒数 |
| `error` | `object|null` | 单标的错误，成功时为 `null` |

`bar` 除通用行情字段外，还包含：

| 字段 | 类型 | 说明 |
|------|------|------|
| `bar_time` | `string` | 分钟结束时间 |
| `is_complete` | `boolean` | 该分钟是否已结束 |
| `adjustment` | `string` | 固定为 `none` |

响应示例：

```json
{
  "status": "completed",
  "artifact_type": "a_share_realtime_1m_price",
  "resp_for_llm": "已返回 2 个 A 股标的的最新 1 分钟行情",
  "data": {
    "as_of": "2026-08-18T10:08:37+08:00",
    "timezone": "Asia/Shanghai",
    "market_status": "trading",
    "items": [
      {
        "symbol": "600519.SH",
        "name": "贵州茅台",
        "trade_date": "20260818",
        "bar": {
          "bar_time": "2026-08-18T10:09:00+08:00",
          "open": 1421.50,
          "high": 1422.80,
          "low": 1421.20,
          "close": 1422.30,
          "volume": 12600,
          "amount": 17919420.00,
          "prev_close": 1415.00,
          "change": 7.30,
          "change_pct": 0.5159,
          "is_complete": false,
          "adjustment": "none"
        },
        "data_status": "normal",
        "delay_seconds": 2,
        "error": null
      }
    ]
  }
}
```

## 4. Tool 二：历史 2 日 1 分钟价格

### 4.1 基本信息

| 属性 | 值 |
|------|----|
| Tool 名称 | `get_a_share_history_1m_price` |
| 分类 | `market_api` |
| 适用场景 | 日内走势分析、跨两日价格比较、短周期特征计算 |
| 建议缓存 | 已结束交易日永久缓存；当日数据 30 秒 |

### 4.2 功能说明

为每个标的返回最近 2 个**市场交易日**的 1 分钟 K 线。目标日期由全市场交易日历确定，而不是简单取两个自然日。

- 默认 `end_date` 为最近交易日；盘中调用时包含当日已生成的分钟 K 线和上一交易日完整数据。
- 每个标的最多返回 480 根 K 线。
- 停牌日仍计入市场交易日，但该标的当天 `bars` 为空，并标记 `suspended`，不会继续向前补第三天。
- 新股上市不足 2 个交易日时按实际可用天数返回。

### 4.3 输入参数

| 参数 | 类型 | 必填 | 默认值 | 说明 |
|------|------|------|--------|------|
| `symbols` | `string[]` | 是 | - | A 股代码列表，1–50 个 |
| `end_date` | `string` | 否 | 最近交易日 | 截止交易日，格式 `YYYYMMDD`；若为非交易日则回退至此前最近交易日 |
| `trading_days` | `integer` | 否 | `2` | 固定只允许为 `2`，预留参数便于 schema 表意 |

请求示例：

```json
{
  "tool_name": "get_a_share_history_1m_price",
  "parameters": {
    "symbols": ["600519.SH", "300750.SZ"],
    "end_date": "20260818",
    "trading_days": 2
  }
}
```

### 4.4 输出参数

外层结构与实时接口一致，`artifact_type` 固定为 `a_share_history_1m_price`。

`data` 结构：

| 字段 | 类型 | 说明 |
|------|------|------|
| `timezone` | `string` | 固定为 `Asia/Shanghai` |
| `requested_trading_days` | `integer` | 固定为 `2` |
| `resolved_trade_dates` | `string[]` | 实际解析出的两个市场交易日，升序 |
| `items` | `array` | 各标的历史分钟行情 |

`items[]` 结构：

| 字段 | 类型 | 说明 |
|------|------|------|
| `symbol` | `string` | 标的代码 |
| `name` | `string|null` | 证券简称 |
| `days` | `array` | 按交易日升序排列的数据 |
| `error` | `object|null` | 单标的错误 |

`days[]` 结构：

| 字段 | 类型 | 说明 |
|------|------|------|
| `trade_date` | `string` | 交易日 |
| `data_status` | `string` | `complete`、`partial`、`suspended`、`no_data` |
| `bars` | `array` | 按 `bar_time` 升序排列的 1 分钟 K 线 |

历史 `bars[]` 字段与实时接口的 `bar` 相同。历史完整分钟的 `is_complete` 为 `true`；盘中当日最后一分钟可为 `false`。

响应示例（节选）：

```json
{
  "status": "completed",
  "artifact_type": "a_share_history_1m_price",
  "resp_for_llm": "已返回 2 个标的最近 2 个交易日的 1 分钟行情",
  "data": {
    "timezone": "Asia/Shanghai",
    "requested_trading_days": 2,
    "resolved_trade_dates": ["20260817", "20260818"],
    "items": [
      {
        "symbol": "600519.SH",
        "name": "贵州茅台",
        "days": [
          {
            "trade_date": "20260817",
            "data_status": "complete",
            "bars": [
              {
                "bar_time": "2026-08-17T09:31:00+08:00",
                "open": 1410.00,
                "high": 1411.20,
                "low": 1409.80,
                "close": 1411.00,
                "volume": 15800,
                "amount": 22286100.00,
                "prev_close": 1402.50,
                "change": 8.50,
                "change_pct": 0.6061,
                "is_complete": true,
                "adjustment": "none"
              }
            ]
          }
        ],
        "error": null
      }
    ]
  }
}
```

## 5. 错误约定

### 5.1 Tool 级错误

| 错误码 | 场景 |
|--------|------|
| `INVALID_ARGUMENT` | 参数格式错误、`symbols` 为空或超过 50 个 |
| `UNAUTHORIZED` | 未通过认证 |
| `FORBIDDEN` | 调用方无行情权限 |
| `RATE_LIMITED` | 超过调用频率限制 |
| `UPSTREAM_UNAVAILABLE` | 上游行情源不可用 |
| `INTERNAL_ERROR` | 服务内部异常 |

Tool 级失败示例：

```json
{
  "status": "failed",
  "artifact_type": "a_share_history_1m_price",
  "resp_for_llm": "symbols 最多支持 50 个标的",
  "data": null,
  "error": {
    "code": "INVALID_ARGUMENT",
    "message": "symbols size must be between 1 and 50",
    "retryable": false
  }
}
```

### 5.2 单标的错误

| 错误码 | 场景 |
|--------|------|
| `INVALID_SYMBOL` | 代码格式错误或不属于支持的 A 股股票范围 |
| `SYMBOL_NOT_FOUND` | 标的不存在 |
| `NO_DATA` | 查询区间无行情数据 |

批量请求中部分标的失败时，外层 `status = "partial"`，成功标的正常返回数据。

## 6. 非功能要求

| 项目 | 实时接口 | 历史接口 |
|------|----------|----------|
| 服务端 P95 延迟 | 单标的 ≤ 300 ms；50 标的 ≤ 1 s | 单标的 ≤ 500 ms；50 标的 ≤ 3 s |
| 数据新鲜度 | 交易时段延迟 ≤ 10 秒 | 已结束分钟延迟 ≤ 60 秒 |
| 可用性 | ≥ 99.9% | ≥ 99.9% |
| 默认限流建议 | 60 次/分钟/调用方 | 20 次/分钟/调用方 |
| 最大响应量 | 50 根 bar | 50 × 480 根 bar |

建议历史接口启用 gzip/br 压缩；服务端应按标的批量查询，禁止循环逐标的访问数据库。

## 7. 验收标准

1. 正常交易日完整数据每个未停牌标的返回 240 根 K 线，两个交易日最多 480 根。
2. OHLC 满足 `low <= open/close <= high`，`volume >= 0`，`amount >= 0`。
3. 同一标的同一分钟唯一，且 `bar_time` 严格升序。
4. 交易日期解析能正确跳过周末、法定休市日，并正确表示停牌日。
5. 实时接口能区分正在形成和已经完成的分钟 K 线。
6. 批量请求中单个无效代码不会导致整批失败。
7. 价格、成交量和成交额与权威行情源抽样核对，差异满足供应商约定精度。

## 8. 与现有 Tool 的关系

仓库当前已有 `get_a_minute_quote`，可查询单标的 A 股近 2 日 1 分钟 K 线。本文定义的两个 Tool 是面向本需求的清晰分层版本：

- `get_a_share_realtime_1m_price` 专注最新分钟行情，并明确市场状态和数据延迟。
- `get_a_share_history_1m_price` 专注每个标的固定 2 个交易日的完整序列，并支持批量标的。

实现时可复用 `AMinuteQuoteService` 的分钟 K 线重构逻辑，但应统一本文的分钟结束时间、批量查询、交易日和停牌口径。
