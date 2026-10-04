#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
4 份盘前分析报告（大流/行业/红利/自选）自动化生成脚本
基于 auto_hot.py 的 AI Studio 自动化框架 + TradeAgent 页面的提示词组装与报告组装逻辑。
流程：
1. 拉取 AIPEBot/tradeagent/ 下每个来源的两份 5 日 JSON（代理优先，raw 直连兜底）
2. 动态组装三段式提示词（同 TradeAgent buildPreMarketPrompt）
3. 连接已登录的 AI Studio（CDP 9222），每个来源新建会话、开联网、发送
4. 等待流式输出结束 → 提取模型 Markdown 回复
5. Python 版 markdown_to_html + build_report_html 组装深色报告页（同 TradeAgent）
6. 校验 → 保存为 {source}盘前分析报告_YYYY-MM-DD.html
7. 检查 GitHub Trend/ta/ 是否已有同名报告：有则人工判断，无才提交（pull --rebase + push）
"""
import asyncio
import json
import re
import subprocess
import sys
from datetime import datetime
from pathlib import Path
import pytz
import requests
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeout
# ===================== 配置区 =====================
AI_STUDIO_URL = "https://aistudio.tencent.com/"
OUTPUT_DIR = Path("./output")
BROWSER_DATA_DIR = Path("./browser_data")
GITHUB_USERNAME = "digital-era"
GITHUB_REPO_NAME = "AIPEBot"
GITHUB_BRANCH = "main"
GITHUB_TARGET_DIR = "tradeagent"
PROXY_BASE_URL = "https://githubproxy.aivibeinvest.com"
RAW_BASE_URL = "https://raw.githubusercontent.com/digital-era/AIPEBot/main/tradeagent"
TREND_RAW_BASE = "https://raw.githubusercontent.com/digital-era/Trend/main/ta"
SOURCES = ["大流", "行业", "红利", "自选"]
ENABLE_GIT_PUSH = True
TREND_REPO_PATH = Path("E:/Trend")
GIT_COMMIT_MSG_PREFIX = "add pre-market report"
GIT_USER_NAME = "digital-era"
GIT_USER_EMAIL = "digital_era@sina.com"
INTERACTIVE_CONFIRM = False
GENERATION_TIMEOUT = 900
STABLE_SECONDS = 45
HEADLESS = False
USE_EXISTING_CHROME = True
CDP_URL = "http://127.0.0.1:9222"
ERROR_KEYWORDS = ["服务异常", "服务出现异常", "出现错误", "请求失败", "请稍后重试", "请重试", "网络错误"]
NEW_CHAT_SELECTOR = '.layout-menu__menu-item:has-text("对话")'
BEIJING_TZ = pytz.timezone("Asia/Shanghai")
# ==================================================
def get_beijing_date() -> str:
    return datetime.now(BEIJING_TZ).strftime("%Y-%m-%d")
def fetch_github_json(filename: str) -> dict:
    url = "{}/{}/{}/{}/{}/{}".format(
        PROXY_BASE_URL, GITHUB_USERNAME, GITHUB_REPO_NAME,
        GITHUB_BRANCH, GITHUB_TARGET_DIR, filename
    )
    try:
        r = requests.get(url, timeout=30, headers={"Cache-Control": "no-store"})
        r.raise_for_status()
        print("      [代理] {} HTTP {}".format(filename, r.status_code))
    except Exception as e:
        print("      [代理失败，降级直连] {}".format(e))
        url = "{}/{}".format(RAW_BASE_URL, filename)
        r = requests.get(url, timeout=30, headers={"Cache-Control": "no-store"})
        r.raise_for_status()
        print("      [直连] {} HTTP {}".format(filename, r.status_code))
    return r.json()
def extract_weight_items(day, source):
    out = []
    if not day or not isinstance(day, dict):
        return out
    if isinstance(day.get(source), list):
        for group in day[source]:
            if not isinstance(group, dict):
                continue
            for it in (group.get("权重标的") or []):
                out.append(it)
        return out
    if isinstance(day.get("权重标的"), list):
        return list(day["权重标的"])
    return out
def simplify_pairs(raw, source):
    data = raw.get("数据") or raw.get("data") or raw
    out = {}
    for date, day in data.items():
        if not re.match(r"^\d{4}-\d{2}-\d{2}$", date):
            continue
        items = []
        for it in extract_weight_items(day, source):
            items.append({
                "代码": it.get("代码"),
                "名称": it.get("名称"),
                "最优权重": it.get("最优权重(%)")
            })
        out[date] = {
            "综合建议仓位因子": day.get("综合建议仓位因子"),
            "权重标的": items
        }
    return out
def simplify_flow(raw):
    data = raw.get("数据") or raw.get("data") or raw
    if not isinstance(data, list):
        data = []
    keys = ["代码", "名称", "日期", "收盘价", "涨跌幅", "PotScore",
            "超大单净流入-净占比", "主力净流入-净占比", "大单净流入-净占比"]
    result = []
    for it in data:
        if isinstance(it, dict):
            row = {}
            for k in keys:
                row[k] = it.get(k)
            result.append(row)
    return result
def build_prompt(source: str) -> str:
    print("   组装 {} 提示词...".format(source))
    pairs_raw = fetch_github_json("{}_trade_code_pairs_5days.json".format(source))
    flow_raw = fetch_github_json("{}_EEIFlowTrade5days.json".format(source))
    pairs_json = simplify_pairs(pairs_raw, source)
    flow_json = simplify_flow(flow_raw)
    json1 = json.dumps(pairs_json, ensure_ascii=False, indent=2)
    json2 = json.dumps(flow_json, ensure_ascii=False, indent=2)
    head = "盘前分析：\n"
    head += "1、下面文件是备选标的信息（JSON），请关注\"代码\"和\"名称\"两个字段用来唯一标识备选标的，\"日期\"字段标注时间：\n"
    head += "```json\n" + json1 + "\n```\n\n"
    head += "2、下面文件是备选标的5日主力资金流入（请关注\"超大单净流入-净占比\",\"主力净流入-净占比\",\"大单净流入-净占比\"三个字段，\"主力净流入-净占比\"=\"超大单净流入-净占比\"+\"大单净流入-净占比\"）和动量（请关注\"PotScore\"字段，大于0即为正动量，数值越大动量越大）以及价格变动（请关注\"涨跌幅\"字段，与主力资金流入和动量相关）：\n"
    head += "```json\n" + json2 + "\n```\n\n"
    head += "3、请基于上述信息，结合网络搜索对标的经营最新发展态势、所在行业最新发展态势，最新宏观、中观态势，和实时相关重大事件进行洞见分析，输出下一个交易日的交易策略和推荐标的。注意：请忽略原始信息中的权重，依据自己的分析而不是原始信息的建议。\n\n"
    head += "请严格按照以下结构输出（可参考风格，使用 emoji 和 Markdown）：\n\n"
    head += "根据您提供的备选标的、资金流、动量数据，并结合对最新市场动态、行业趋势及宏观中观政策的分析，我为您梳理了下一个交易日的交易策略与推荐标的。核心思路是：聚焦资金持续流入、动量强劲、且处于政策与产业风口上的标的，同时警惕短期涨幅过大带来的回调风险。\n\n"
    head += "📊 一、核心交易策略\n（列出 3~5 条核心策略）\n\n"
    head += "🔍 二、推荐标的及分析\n（用表格或列表给出重点推荐标的）\n\n"
    head += "📈 三、重点标的深度解读\n（对 2~4 只重点标的做资金面 + 消息面 + 行业面 + 策略建议的深度解读）\n\n"
    head += "🌐 四、宏观与行业背景洞察\n（政策、行业景气、市场情绪）\n\n"
    head += "⚠️ 五、风险提示与操作建议\n（短期涨幅过大、基本面验证、系统性风险、分散投资、止损纪律）\n\n"
    head += "📝 总结与下一步行动\n（明确下一个交易日的首选 / 次选 / 备选 / 警惕标的）\n\n"
    head += "请务必结合自身的风险承受能力和投资目标，参考上述分析做出决策。投资有风险，入市需谨慎。"
    print("      提示词长度: {} 字符".format(len(head)))
    return head
def markdown_to_html(md: str) -> str:
    if not md:
        return ""
    text = md.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    text = re.sub(r"^### (.*)$", r"<h3>\1</h3>", text, flags=re.M)
    text = re.sub(r"^## (.*)$", r"<h2>\1</h2>", text, flags=re.M)
    text = re.sub(r"^# (.*)$", r"<h1>\1</h1>", text, flags=re.M)
    text = re.sub(r"\*\*(.*?)\*\*", r"<strong>\1</strong>", text, flags=re.S)
    text = re.sub(r"\*(.*?)\*", r"<em>\1</em>", text, flags=re.S)
    def _codeblock(m):
        code = re.sub(r"^```\w*\n?", "", m.group(0)).replace("```", "")
        style = "background:#1a2332;padding:12px;border-radius:6px;overflow:auto;"
        return "<pre style=\"" + style + "\"><code>" + code + "</code></pre>"
    text = re.sub(r"```[\s\S]*?```", _codeblock, text)
    text = re.sub(
        r"`([^`]+)`",
        "<code style='background:#1a2332;padding:2px 5px;border-radius:3px;'>\\1</code>",
        text
    )
    text = re.sub(r"^\s*[-*] (.*)$", r"<li>\1</li>", text, flags=re.M)
    text = re.sub(r"((?:<li>.*</li>\n?)+)", r"<ul>\1</ul>", text)
    text = text.replace("\n\n", "</p><p>").replace("\n", "<br>")
    return "<p>" + text + "</p>"
REPORT_CSS = (
    "html { -webkit-text-size-adjust: 100%; text-size-adjust: 100%; } "
    ":root { color-scheme: dark; } "
    "body { font-family: system-ui, -apple-system, 'Segoe UI', Roboto, "
    "'PingFang SC', 'Microsoft YaHei', sans-serif; max-width: 860px; "
    "margin: 0 auto; padding: 32px 20px 60px; background: #0f1419; "
    "color: #e7e9ea; line-height: 1.7; font-size: 15px; } "
    "h1, h2, h3 { color: #1d9bf0; margin-top: 1.6em; } "
    "h1 { font-size: 1.6rem; border-bottom: 1px solid #38444d; padding-bottom: 10px; } "
    "h2 { font-size: 1.25rem; } h3 { font-size: 1.1rem; } "
    "strong { color: #f4212e; } "
    "ul { padding-left: 1.4em; } li { margin: 0.35em 0; } "
    "pre { background: #1a2332; padding: 14px; border-radius: 8px; "
    "overflow-x: auto; -webkit-overflow-scrolling: touch; } "
    "code { font-family: ui-monospace, 'Cascadia Code', monospace; font-size: 0.9em; } "
    ".meta { color: #8b98a5; font-size: 0.9rem; margin-bottom: 24px; } "
    ".footer { margin-top: 48px; padding-top: 16px; border-top: 1px solid #38444d; "
    "color: #8b98a5; font-size: 0.85rem; } "
    "table { width: 100%; border-collapse: collapse; margin: 16px 0; min-width: 480px; } "
    "th, td { border: 1px solid #38444d; padding: 8px 12px; text-align: left; white-space: nowrap; } "
    "th { background: #1a2332; } "
    ".table-wrap { width: 100%; overflow-x: auto; -webkit-overflow-scrolling: touch; } "
    "@media (max-width: 640px) { "
    "body { padding: 16px 12px 40px; font-size: 14px; line-height: 1.65; } "
    "h1 { font-size: 1.35rem; } h2 { font-size: 1.15rem; } h3 { font-size: 1.02rem; } "
    "pre { padding: 10px; font-size: 0.8em; } "
    ".meta { margin-bottom: 16px; } .footer { margin-top: 32px; } "
    "}"
)
def build_report_html(source: str, content_md: str) -> str:
    now = datetime.now(BEIJING_TZ).strftime("%Y/%m/%d %H:%M:%S")
    body_html = markdown_to_html(content_md)
    title = "{}盘前分析报告 - {}".format(source, now)
    page = "<!DOCTYPE html>\n"
    page += "<html lang=\"zh-CN\">\n"
    page += "<head>\n"
    page += "  <meta charset=\"UTF-8\" />\n"
    page += "  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />\n"
    page += "  <title>" + title + "</title>\n"
    page += "  <style>" + REPORT_CSS + "</style>\n"
    page += "</head>\n"
    page += "<body>\n"
    page += "  <h1>📊 " + source + "盘前分析报告</h1>\n"
    page += "  <div class=\"meta\">生成时间：" + now + " &nbsp;|&nbsp; 来源：量子TradeAgent（自动化）</div>\n"
    page += "  <div class=\"content\">\n"
    page += body_html + "\n"
    page += "  </div>\n"
    page += "  <div class=\"footer\">\n"
    page += "    本报告由大模型结合资金流、动量与公开信息生成，仅供参考，不构成投资建议。<br>\n"
    page += "    投资有风险，入市需谨慎。\n"
    page += "  </div>\n"
    page += "</body>\n"
    page += "</html>"
    return page
def validate_report_html(html: str):
    errors = []
    if not html.lstrip().lower().startswith("<!doctype html"):
        errors.append("不是以 <!DOCTYPE html> 开头的完整 HTML")
    if ("服务出现异常" in html) or ("请稍后重试" in html):
        errors.append("检出模型报错文案（服务异常/请稍后重试）")
    if re.search(r"示例|EXAMPLE|example\.com", html[:5000]):
        errors.append("头部检出示例页特征")
    for kw in ("<style", "<body", "</html>"):
        if kw not in html.lower():
            errors.append("缺少关键标签 " + kw)
    if len(html) < 8000:
        errors.append("HTML 过短（{} 字符）".format(len(html)))
    today_full = datetime.now(BEIJING_TZ).strftime("%Y.%m.%d")
    today_compact = today_full.replace(".", "")
    today_dash = get_beijing_date()
    if (today_full not in html) and (today_compact not in html) and (today_dash not in html):
        print("      ⚠️ 警告: 未找到今日日期（{} / {}），请人工确认".format(today_full, today_dash))
    return (len(errors) == 0, errors)
def github_report_exists(filename: str):
    urls = [
        "{}/{}/Trend/main/ta/{}".format(PROXY_BASE_URL, GITHUB_USERNAME, filename),
        "{}/{}".format(TREND_RAW_BASE, filename),
    ]
    for url in urls:
        try:
            r = requests.head(url, timeout=15, allow_redirects=True)
            if r.status_code == 200:
                return True
            if r.status_code == 404:
                return False
        except Exception:
            pass
        try:
            r = requests.get(url, timeout=15, stream=True)
            if r.status_code == 200:
                r.close()
                return True
            if r.status_code == 404:
                r.close()
                return False
        except Exception:
            continue
    return None
async def is_logged_in(page) -> bool:
    login_btn = page.locator(".side-menu-layout__r__btn", has_text="登录")
    if await login_btn.count() > 0:
        try:
            if await login_btn.first.is_visible(timeout=2000):
                return False
        except Exception:
            pass
    return True
async def is_generation_active(page) -> bool:
    try:
        send_btn = page.locator(".hy-chat-input-send-btn")
        if await send_btn.count() == 0:
            return True
        g_el = send_btn.first.locator("svg g").first
        if await g_el.count() == 0:
            return True
        g_id = (await g_el.get_attribute("id") or "").strip()
        return g_id == "Stop generating"
    except Exception:
        return True
async def wait_for_generation_finish(page, max_wait: int) -> bool:
    print("      等待模型流式输出结束...")
    finish_rounds = 0
    finish_confirm_rounds = 3
    waited = 0
    while waited < max_wait:
        active = await is_generation_active(page)
        if not active:
            finish_rounds += 1
            if finish_rounds >= finish_confirm_rounds:
                print("      模型输出已结束 ✓（耗时约 {} 秒）".format(waited))
                return True
        else:
            finish_rounds = 0
            if waited > 0 and waited % 30 < 3:
                print("      模型仍在输出中...（已等待 {} 秒）".format(waited))
        await page.wait_for_timeout(3000)
        waited += 3
    print("      ⚠️ 等待流式输出结束超时（{} 秒），继续尝试提取".format(max_wait))
    return False
async def detect_error_on_page(page):
    try:
        body_text = await page.locator("body").text_content() or ""
        tail = body_text[-3000:]
        for kw in ERROR_KEYWORDS:
            if kw in tail:
                return kw
    except Exception:
        pass
    return None
async def start_new_chat(page) -> bool:
    print("      尝试新建对话（点击「对话」菜单项）...")
    try:
        chat_menu = page.locator(NEW_CHAT_SELECTOR)
        n = await chat_menu.count()
        if n == 0:
            print("      ⚠️ 未找到「对话」菜单项，将依赖基线兜底")
            return False
        for i in range(n):
            item = chat_menu.nth(i)
            try:
                if await item.is_visible():
                    await item.click()
                    print("      已点击「对话」，进入新会话 ✓")
                    await page.wait_for_timeout(3000)
                    return True
            except Exception:
                continue
        print("      ⚠️ 「对话」菜单项存在但均不可见，将依赖基线兜底")
        return False
    except Exception as e:
        print("      ⚠️ 点击「对话」异常（忽略）: {}".format(e))
        return False
async def ensure_online_search(page):
    online_btn = page.locator('[data-tool-key="online"]')
    await online_btn.wait_for(state="visible", timeout=10000)
    active_icon = page.locator(".online-tool-icon--active")
    if await active_icon.count() == 0:
        await online_btn.click()
        await page.wait_for_timeout(600)
        try:
            await active_icon.wait_for(state="visible", timeout=3000)
            print("      联网搜索已开启 ✓")
        except PlaywrightTimeout:
            print("      警告：未能确认联网搜索激活状态，继续尝试...")
    else:
        print("      联网搜索已处于开启状态 ✓")


REPLY_START_MARK = "📊 一、核心交易策略"
REPLY_END_MARK = "投资有风险，入市需谨慎。"


async def pick_reply_text(page, baseline_body_len):
    """
    提取模型输出正文：页面全文相对发送前基线的增量（新会话下增量
    含提示词回显、思考过程、正文、页脚），再做首尾裁剪：
    - 起点：增量中最后一次出现「一、核心交易策略」（提示词回显里也有
      这句，但提示词在增量起点之前？不一定——若新会话仍回显提示词，
      则增量内可能出现两次，取最后一次，之后才是正文）
    - 终点：「投资有风险，入市需谨慎。」之后全部截掉（思考过程、
      页脚、模型介绍等）
    """
    try:
        body_text = (await page.locator("body").text_content() or "").strip()
    except Exception:
        return None
    if len(body_text) <= baseline_body_len + 2000:
        return None

    delta = body_text[baseline_body_len:]

    # 起点：增量中最后一次出现起始标记
    pos = delta.rfind(REPLY_START_MARK)
    if pos < 0:
        return None   # 正文尚未生成到该处
    reply = delta[pos:]

    # 终点：起始标记之后第一次出现的结尾标记（正文自己的结尾）
    end = reply.find(REPLY_END_MARK)
    if end < 0:
        # 正文还没输出完，返回目前截到的部分（调用方靠长度增长判断继续等待）
        return reply.strip()
    reply = reply[:end + len(REPLY_END_MARK)]

    return reply.strip()



async def generate_report(page, prompt: str, source: str):
    """
    在全新会话中发送提示词，提取模型输出全部文本（页面全文增量），
    组装为完整报告 HTML。失败返回 None。
    """
    # 每个来源强制新建对话，避免旧会话上下文互相影响
    ok = await start_new_chat(page)
    if not ok:
        print("      ❌ 无法新建对话，跳过本来源（避免旧会话上下文干扰）")
        return None
    await ensure_online_search(page)

    # 基线：发送前页面全文长度（新会话理论上接近 0，取实测值兜底）
    try:
        baseline_body_len = len((await page.locator("body").text_content() or "").strip())
    except Exception:
        baseline_body_len = 0
    print("      [基线] 页面全文长度: {}".format(baseline_body_len))

    textarea = page.locator("textarea.t-textarea__inner")
    await textarea.wait_for(state="visible", timeout=30000)

    filled = False
    for attempt in range(1, 4):          # 常规 fill 最多试 3 次
        try:
            await textarea.click(timeout=10000)
            await textarea.fill(prompt, timeout=30000)
            filled = True
            break
        except Exception as e:
            print("      fill 第 {} 次尝试失败: {}".format(
                attempt, str(e).split("\n")[0][:80]))
            await page.wait_for_timeout(3000)

    if not filled:
        print("      常规 fill 连续失败，改用 JS 直接写入...")
        await textarea.evaluate(
            "(el, text) => { el.focus(); el.value = text;"
            " el.dispatchEvent(new Event('input', { bubbles: true })); }",
            prompt,
        )
    await page.wait_for_timeout(500)


    send_btn = page.locator(".hy-chat-input-send-btn")
    try:
        await page.wait_for_selector(
            ".hy-chat-input-send-btn:not(.hy-chat-input-send-btn--disabled)",
            timeout=5000
        )
    except PlaywrightTimeout:
        print("      发送按钮仍显示 disabled，尝试强制点击...")
    await send_btn.click()
    print("      已点击发送，等待模型生成（最长 {} 秒）...".format(GENERATION_TIMEOUT))

    gen_ok = await wait_for_generation_finish(page, GENERATION_TIMEOUT)

    # 流式结束后，轮询提取回复全文，直到内容稳定（最多再等 GENERATION_TIMEOUT 秒）
    last_good = None
    last_change_time = asyncio.get_event_loop().time()
    start_time = asyncio.get_event_loop().time()
    error_seen = None

    while asyncio.get_event_loop().time() - start_time < GENERATION_TIMEOUT:
        await page.wait_for_timeout(4000)

        reply = await pick_reply_text(page, baseline_body_len)

        if reply and len(reply) > 500:
            if last_good is None or len(reply) > len(last_good) + 50:
                last_good = reply
                last_change_time = asyncio.get_event_loop().time()
                print("      检测到模型回复，当前长度 {} …".format(len(reply)))
            elif (asyncio.get_event_loop().time() - last_change_time) >= STABLE_SECONDS:
                print("      内容已稳定约 {} 秒，判定生成完成".format(STABLE_SECONDS))
                break
        else:
            err = await detect_error_on_page(page)
            if err and err != error_seen:
                error_seen = err
                print("      ⚠️ 检测到页面报错提示：「{}」，且无新回复生成".format(err))

        elapsed = int(asyncio.get_event_loop().time() - start_time)
        if elapsed > 0 and elapsed % 30 < 4:
            print("      已等待 {} 秒...".format(elapsed))

    if (not last_good) or (len(last_good) < 500):
        screenshot_path = OUTPUT_DIR / ("debug_盘前_{}.png".format(get_beijing_date()))
        try:
            await page.screenshot(path=str(screenshot_path), full_page=True)
            print("      已保存截图: {}".format(screenshot_path))
        except Exception:
            pass
        if error_seen:
            print("      ❌ 模型返回异常（「{}」），未生成有效内容".format(error_seen))
        return None

    print("      成功提取模型回复，长度: {} 字符".format(len(last_good)))

    # 模型输出全文 → Python 组装完整报告 HTML（同 TradeAgent 页面）
    html_code = build_report_html(source, last_good)

    ok2, reasons = validate_report_html(html_code)
    if not ok2:
        print("      ❌ HTML 校验失败：")
        for r in reasons:
            print("         - " + r)
        debug_path = OUTPUT_DIR / ("debug_invalid_盘前_{}.html".format(get_beijing_date()))
        debug_path.write_text(html_code, encoding="utf-8")
        print("      无效 HTML 已另存: {}".format(debug_path))
        return None

    print("      HTML 校验通过 ✓")
    return html_code

def save_and_push(source: str, html_code: str, date_str: str) -> bool:
    filename = "{}盘前分析报告_{}.html".format(source, date_str)
    output_path = OUTPUT_DIR / filename
    output_path.write_text(html_code, encoding="utf-8")
    print("      已保存: {}".format(output_path))
    if not ENABLE_GIT_PUSH:
        return True
    if not TREND_REPO_PATH.exists():
        print("      ⚠️ 未找到本地仓库 {}，跳过 git 提交".format(TREND_REPO_PATH))
        return False
    print("      检查 GitHub 是否已有 {} ...".format(filename))
    exists = github_report_exists(filename)
    if exists is True:
        print("      ⚠️ GitHub 已存在 {}，跳过自动提交。".format(filename))
        print("         本次新生成的报告在本地: {}".format(output_path))
        if INTERACTIVE_CONFIRM:
            ans = input(">>> 是否强制覆盖提交？(y/N)：").strip().lower()
            if ans != "y":
                print("      已跳过提交（人工选择保留远端版本）")
                return True
            print("      人工确认覆盖，继续提交...")
        else:
            print("         （INTERACTIVE_CONFIRM=False，自动跳过）")
            return True
    elif exists is None:
        print("      ⚠️ 无法确认远端是否存在该文件（网络原因），按不存在处理，继续提交")
    else:
        print("      远端不存在同名报告 ✓ 继续提交")
    ta_dir = TREND_REPO_PATH / "ta"
    ta_dir.mkdir(parents=True, exist_ok=True)
    target = ta_dir / filename
    target.write_text(html_code, encoding="utf-8")
    git_identity = ["-c", "user.name=" + GIT_USER_NAME, "-c", "user.email=" + GIT_USER_EMAIL]
    try:
        subprocess.run(
            ["git", "-C", str(TREND_REPO_PATH), "add", "ta/" + filename],
            check=True
        )
        subprocess.run(
            ["git"] + git_identity + [
                "-C", str(TREND_REPO_PATH), "commit", "-m",
                GIT_COMMIT_MSG_PREFIX + " " + filename
            ],
            check=True
        )
        pull = subprocess.run(
            ["git"] + git_identity + [
                "-C", str(TREND_REPO_PATH),
                "pull", "--rebase", "origin", "main"
            ],
            capture_output=True, text=True
        )
        if pull.returncode == 0:
            print("      已同步远端更新（pull --rebase）")
        else:
            msg = (pull.stdout or "") + (pull.stderr or "")
            print("      ⚠️ pull --rebase 失败: " + msg)
            subprocess.run(
                ["git", "-C", str(TREND_REPO_PATH), "rebase", "--abort"],
                capture_output=True
            )
            print("      已中止 rebase，放弃本次推送（本地文件未受影响）")
            return False
        subprocess.run(["git", "-C", str(TREND_REPO_PATH), "push"], check=True)
        print("      已推送到 digital-era/Trend/ta/" + filename)
        return True
    except subprocess.CalledProcessError as e:
        print("      ⚠️ Git 操作失败: {}".format(e))
        return False
        
async def main():
    date_str = get_beijing_date()
    print("===== 开始生成 {} 份盘前分析报告 ({}) =====\n".format(len(SOURCES), date_str))
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    results = {}
    browser = None          # ← 补：统一初始化
    context = None          # ← 补：统一初始化
    p = await async_playwright().start()

    # 嵌套清理函数：能访问 main 的局部变量 p/browser/context
    async def cleanup():
        if context is not None:
            try:
                await context.close()
            except Exception:
                pass
        if browser is not None:
            try:
                await browser.close()
            except Exception:
                pass
        try:
            await p.stop()
        except Exception:
            pass

    if USE_EXISTING_CHROME:
        print("[1] 连接到已启动的 Chrome (CDP 9222)...")
        try:
            browser = await p.chromium.connect_over_cdp(CDP_URL)
        except Exception as e:
            print("❌ 无法连接 Chrome：{}".format(e))
            print()
            print("请先用以下命令启动 Chrome，并在该窗口登录 https://aistudio.tencent.com/ ：")
            print()
            print("  Windows:")
            print('    "C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe" '
                  '--remote-debugging-port=9222 --user-data-dir="%TEMP%\\chrome-aistudio"')
            print()
            await cleanup()
            sys.exit(1)
        if browser.contexts:
            context = browser.contexts[0]
        else:
            context = await browser.new_context()
    else:
        print("[1] 启动 Playwright 独立浏览器...")
        BROWSER_DATA_DIR.mkdir(parents=True, exist_ok=True)
        context = await p.chromium.launch_persistent_context(
            user_data_dir=str(BROWSER_DATA_DIR),
            headless=HEADLESS,
            viewport={"width": 1440, "height": 900},
            locale="zh-CN",
            channel="chrome",
            args=["--disable-blink-features=AutomationControlled", "--no-sandbox"]
        )

    page = None
    for pg in context.pages:
        if "aistudio.tencent.com" in (pg.url or ""):
            page = pg
            break
    if page is None:
        if context.pages:
            page = context.pages[0]
        else:
            page = await context.new_page()

    print("[2] 打开 AI Studio...")
    if "aistudio.tencent.com" not in (page.url or ""):
        await page.goto(AI_STUDIO_URL, wait_until="domcontentloaded", timeout=60000)
    else:
        await page.reload(wait_until="domcontentloaded")
    await page.wait_for_timeout(2000)
    try:
        stay_btn = page.get_by_role(
            "button", name=re.compile(r"Stay here|留在此处|留在这里", re.I)
        )
        if await stay_btn.count() > 0:
            await stay_btn.first.click()
            await page.wait_for_timeout(800)
    except Exception:
        pass

    if not await is_logged_in(page):
        print("\n⚠️  检测到未登录！请在浏览器窗口中手动登录（微信/QQ/手机号）后回车继续...")
        input(">>> 登录完成后按回车继续 <<<")
        await page.reload(wait_until="domcontentloaded")
        await page.wait_for_timeout(2000)
        if not await is_logged_in(page):
            print("❌ 仍未检测到登录状态，退出。")
            await cleanup()
            sys.exit(1)
    print("      已登录 ✓\n")

    for idx, source in enumerate(SOURCES, 1):
        print("\n===== [{}/{}] 来源：{} =====".format(idx, len(SOURCES), source))
        try:
            prompt = build_prompt(source)
            html_code = await generate_report(page, prompt, source)
            if not html_code:
                print("❌ [{}] 生成无效，跳过推送".format(source))
                results[source] = False
                continue
            results[source] = save_and_push(source, html_code, date_str)
        except Exception as e:
            print("❌ [{}] 异常: {}".format(source, e))
            results[source] = False

    print("\n===== 执行汇总 =====")
    for s, ok in results.items():
        mark = "✅" if ok else "❌"
        print("  {} {}盘前分析报告_{}.html".format(mark, s, date_str))

    await cleanup()   # ← 末尾统一调用嵌套函数

    failed = [s for s, ok in results.items() if not ok]
    if failed:
        print("\n⚠️ 失败来源: {}（可单独重跑：修改 SOURCES = {}）".format("、".join(failed), failed))
        sys.exit(2)
    print("\n✅ 全部完成！")


if __name__ == "__main__":
    asyncio.run(main())
