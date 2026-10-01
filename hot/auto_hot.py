#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
24小时热点图谱 自动化生成脚本
每天北京时间 8:00 自动执行：
1. 拉取 genhot.txt 提示词
2. 打开腾讯 AI Studio，开启联网搜索
3. 提交提示词并等待生成
4. 提取 HTML 代码
5. 保存为 24小时热点_YYYY-MM-DD.html
6. （可选）提交到 digital-era/Trend 仓库的 ht 目录

登录态说明：
- USE_EXISTING_CHROME = True（推荐）：连接已用 --remote-debugging-port=9222 启动的 Chrome，直接复用登录态
- USE_EXISTING_CHROME = False：使用 Playwright 独立 browser_data，需在该窗口登录一次
"""

import asyncio
import re
import subprocess
import sys
from datetime import datetime
from pathlib import Path

import pytz
import requests
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeout


# ===================== 配置区 =====================
PROMPT_URL = "https://raw.githubusercontent.com/digital-era/AIPEBot/main/hot/genhot.txt"
AI_STUDIO_URL = "https://aistudio.tencent.com/"

# 浏览器数据目录（仅 USE_EXISTING_CHROME=False 时使用）
BROWSER_DATA_DIR = Path("./browser_data")

import shutil
import glob
import os

# 生成结果保存目录
OUTPUT_DIR = Path("./output")

def clean_output_dir():
    """运行前清空 output 目录下的所有文件（保留目录本身）。"""
    print(f"[0/6] 清空输出目录 {OUTPUT_DIR} ...")
    if not os.path.isdir(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        print("      目录不存在，已创建 ✓")
        return
    removed = 0
    for f in glob.glob(os.path.join(OUTPUT_DIR, "*")):
        try:
            if os.path.isfile(f) or os.path.islink(f):
                os.remove(f)
                removed += 1
            elif os.path.isdir(f):
                shutil.rmtree(f)
                removed += 1
        except Exception as e:
            print(f"      ⚠️ 无法删除 {f}: {e}")
    print(f"      已清理 {removed} 个历史文件 ✓")
    
    
import re
from datetime import datetime, timedelta

BEIJING_TZ = pytz.timezone("Asia/Shanghai")

def validate_html(html: str) -> tuple[bool, list[str]]:
    """校验生成的 HTML 是否为有效产出。返回 (是否通过, 失败原因列表)。"""
    errors = []

    # ① 必须是完整 HTML
    if not html.lstrip().lower().startswith("<!doctype html"):
        errors.append("不是以 <!DOCTYPE html> 开头的完整 HTML")

    # ② 模型报错特征（报错时模型会复述提示词里的示例页）
    if "服务出现异常" in html or "请稍后重试" in html:
        errors.append("检出模型报错文案（服务异常/请稍后重试）")
    if re.search(r"示例|EXAMPLE|example\.com", html[:5000]):
        errors.append("头部检出示例页特征")

    # ③ 编报日期 = 今日北京时间
    now_bj = datetime.now(BEIJING_TZ)
    today_full = now_bj.strftime("%Y.%m.%d")
    today_compact = now_bj.strftime("%Y%m%d")
    if (today_full not in html) and (today_compact not in html):
        errors.append(f"未找到今日编报日期 {today_full}")

    # ④ 昨日日期应存在（时间窗口最近 24~48 小时）
    yest = now_bj - timedelta(days=1)
    y_str = yest.strftime("%Y.%m.%d")
    y2_str = yest.strftime("%m.%d")
    if (y_str not in html) and (y2_str not in html):
        errors.append(f"未找到昨日日期 {y_str}")

    # ⑤ 内容量
    if len(html) < 15000:
        errors.append(f"HTML 过短（{len(html)} 字符）")

    # ⑥ 关键结构
    for kw in ("<style", "<body", "</html>"):
        if kw not in html.lower():
            errors.append(f"缺少关键标签 {kw}")

    return (len(errors) == 0, errors)



# 是否提交到 GitHub（需要先 git clone digital-era/Trend 到本地）
ENABLE_GIT_PUSH = False
TREND_REPO_PATH = Path("E:/Trend")
GIT_COMMIT_MSG_PREFIX = "add hot topics"

# 生成超时（秒）—— 建议 15 分钟，可再加大
GENERATION_TIMEOUT = 900  # 15 分钟
# 认为「生成结束」的稳定时间：连续这么多秒抽到的 HTML 长度不再明显增加
STABLE_SECONDS = 45

# 无头模式（仅独立浏览器模式有效；连接已有 Chrome 时忽略）
HEADLESS = False

# ---------- 登录复用开关 ----------
# True  = 连接已启动的 Chrome（推荐，无需重复登录）
# False = 使用 Playwright 独立配置目录
USE_EXISTING_CHROME = True
CDP_URL = "http://127.0.0.1:9222"
# ==================================================
# 报错关键词（出现且无新 HTML 时判定失败）
ERROR_KEYWORDS = ["服务异常", "服务出现异常", "出现错误", "请求失败", "请稍后重试", "请重试", "网络错误"]
# ★ 简化：唯一的新建对话入口（来自实际 DOM，混元「对话」即新建会话按钮）
NEW_CHAT_SELECTOR = '.layout-menu__menu-item:has-text("对话")'


def get_beijing_date() -> str:
    """返回今天北京时间日期 YYYY-MM-DD"""
    beijing = pytz.timezone("Asia/Shanghai")
    return datetime.now(beijing).strftime("%Y-%m-%d")


def fetch_prompt() -> str:
    """从 GitHub 拉取提示词"""
    print("[1/6] 正在拉取提示词...")
    resp = requests.get(PROMPT_URL, timeout=30)
    resp.raise_for_status()
    prompt = resp.text.strip()
    print(f"      提示词长度: {len(prompt)} 字符")
    return prompt


def extract_html_from_text(text: str) -> str | None:
    """从模型回复中提取完整 HTML"""
    patterns = [
        r"```(?:html)?\s*(<!DOCTYPE html>[\s\S]*?</html>)\s*```",
        r"(<!DOCTYPE html>[\s\S]*?</html>)",
    ]
    for pat in patterns:
        m = re.search(pat, text, re.IGNORECASE)
        if m:
            return m.group(1).strip()
    return None


def strip_line_numbers(html: str) -> str:
    """
    清洗 text_content() 混入的代码行号。
    特征：大量行的行首是 1~5 位数字（行号栏）。
    若超过 30% 的行符合该特征，则把所有行的行首数字删掉。
    """
    lines = html.split("\n")
    numbered = sum(1 for ln in lines if re.match(r"^\s*\d{1,5}\s", ln))
    if len(lines) > 0 and numbered / len(lines) > 0.30:
        cleaned = [re.sub(r"^\s*\d{1,5}\s", "", ln) for ln in lines]
        return "\n".join(cleaned)
    return html



async def is_logged_in(page) -> bool:
    """更稳健的登录判断：存在可见的「登录」按钮则视为未登录"""
    login_btn = page.locator(".side-menu-layout__r__btn", has_text="登录")
    if await login_btn.count() > 0:
        try:
            if await login_btn.first.is_visible(timeout=2000):
                return False
        except Exception:
            pass
    return True

# ★ 修复：判断模型是否正在流式生成中
# 已确认的三态信号（发送按钮内 SVG 的 <g id>）：
#   "Send Highlight"   → 输入框有内容、可发送（空闲）
#   "Stop generating"  → 模型正在生成
#   "Send Default"     → 生成完成 / 输入框为空
async def is_generation_active(page) -> bool:
    """
    返回 True = 仍在生成；False = 生成已结束。
    唯一判据：按钮内 SVG 的 <g id> 是否为 "Stop generating"。
    检测失败时保守返回 True（宁可多等，不可提前截断）。
    """
    try:
        send_btn = page.locator(".hy-chat-input-send-btn")
        if await send_btn.count() == 0:
            return True  # 找不到按钮，保守认为在生成
        g_el = send_btn.first.locator("svg g").first
        if await g_el.count() == 0:
            return True  # 结构异常，保守处理
        g_id = (await g_el.get_attribute("id") or "").strip()
        return g_id == "Stop generating"
    except Exception:
        return True  # 任何异常都保守处理：宁可多等
	
# ★ 修复：等待模型真正结束流式输出
async def wait_for_generation_finish(page, max_wait: int) -> bool:
    """
    轮询等待模型输出真正结束（图标从 "Stop generating" 切走），
    且额外要求连续 FINISH_CONFIRM_ROUNDS 轮都处于「已结束」状态，防止图标闪烁误判。
    返回 True=已确认结束；False=超时仍未结束。
    """
    print("      等待模型流式输出真正结束（以图标离开 Stop generating 为准）...")
    finish_rounds = 0
    FINISH_CONFIRM_ROUNDS = 3   # 连续 3 轮（每轮 3 秒）都检测到已结束才确认
    waited = 0
    while waited < max_wait:
        active = await is_generation_active(page)
        if not active:
            finish_rounds += 1
            if finish_rounds >= FINISH_CONFIRM_ROUNDS:
                print(f"      模型输出已结束 ✓（耗时约 {waited} 秒）")
                return True
        else:
            finish_rounds = 0
            if waited > 0 and waited % 30 < 3:
                print(f"      模型仍在输出中...（已等待 {waited} 秒）")
        await page.wait_for_timeout(3000)
        waited += 3
    print(f"      ⚠️ 等待流式输出结束超时（{max_wait} 秒），继续尝试提取现有内容")
    return False


async def detect_error_on_page(page) -> str | None:
    """若页面文本包含报错关键词则返回命中的关键词，否则 None"""
    try:
        body_text = await page.locator("body").text_content() or ""
        tail = body_text[-3000:]
        for kw in ERROR_KEYWORDS:
            if kw in tail:
                return kw
    except Exception:
        pass
    return None
    
    
# ★ 简化后的 start_new_chat()：直接点击「对话」即可
async def start_new_chat(page) -> bool:
    """
    点击侧边栏「对话」菜单项（混元中它就是新建会话按钮）：
    - 原窗口有内容 → 自动生成一个空白对话窗口
    - 原窗口本来就是空的 → 界面无可见变化（正常）
    返回 True=点击成功；False=未找到按钮。
    """
    print("      尝试新建对话（点击「对话」菜单项）...")
    try:
        chat_menu = page.locator(NEW_CHAT_SELECTOR)
        n = await chat_menu.count()
        if n == 0:
            print("      ⚠️ 未找到「对话」菜单项（可能侧边栏收起），将依赖基线兜底")
            return False
        # 取第一个可见的点击
        for i in range(n):
            item = chat_menu.nth(i)
            try:
                if await item.is_visible():
                    await item.click()
                    print("      已点击「对话」，进入新会话 ✓")
                    await page.wait_for_timeout(1500)  # 等待窗口切换/清空
                    return True
            except Exception:
                continue
        print("      ⚠️ 「对话」菜单项存在但均不可见，将依赖基线兜底")
        return False
    except Exception as e:
        print(f"      ⚠️ 点击「对话」异常（忽略，依赖基线兜底）: {e}")
        return False


async def generate_hot_page(prompt: str) -> str:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    async with async_playwright() as p:
        context = None
        browser = None
        own_context = False
        if USE_EXISTING_CHROME:
            print("[2/6] 连接到已启动的 Chrome (CDP 9222)...")
            try:
                browser = await p.chromium.connect_over_cdp(CDP_URL)
            except Exception as e:
                print(f"❌ 无法连接 Chrome：{e}")
                print()
                print("请先用以下命令启动 Chrome，并在该窗口登录 https://aistudio.tencent.com/ ：")
                print()
                print("  Windows:")
                print('    "C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe" --remote-debugging-port=9222 --user-data-dir="%TEMP%\\chrome-aistudio"')
                print()
                print("  macOS:")
                print('    /Applications/Google\\ Chrome.app/Contents/MacOS/Google\\ Chrome --remote-debugging-port=9222 --user-data-dir="/tmp/chrome-aistudio"')
                print()
                print("  Linux:")
                print("    google-chrome --remote-debugging-port=9222 --user-data-dir=/tmp/chrome-aistudio")
                print()
                sys.exit(1)
            context = browser.contexts[0] if browser.contexts else await browser.new_context()
            page = None
            for pg in context.pages:
                if "aistudio.tencent.com" in (pg.url or ""):
                    page = pg
                    break
            if page is None:
                page = context.pages[0] if context.pages else await context.new_page()
            own_context = False
        else:
            print("[2/6] 启动 Playwright 独立浏览器...")
            BROWSER_DATA_DIR.mkdir(parents=True, exist_ok=True)
            context = await p.chromium.launch_persistent_context(
                user_data_dir=str(BROWSER_DATA_DIR),
                headless=HEADLESS,
                viewport={"width": 1440, "height": 900},
                locale="zh-CN",
                channel="chrome",
                args=[
                    "--disable-blink-features=AutomationControlled",
                    "--no-sandbox",
                ],
            )
            page = context.pages[0] if context.pages else await context.new_page()
            own_context = True
        print("[3/6] 打开 AI Studio...")
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
            print("\n⚠️  检测到未登录！")
            if USE_EXISTING_CHROME:
                print("   请在已连接的 Chrome 窗口中手动登录（微信/QQ/手机号），")
            else:
                print("   请在打开的浏览器窗口中手动登录（微信/QQ/手机号），")
            print("   登录成功后回到终端按回车继续...\n")
            if (not USE_EXISTING_CHROME) and HEADLESS:
                print("无头模式下无法登录，请先将 HEADLESS = False 运行一次完成登录。")
                if own_context:
                    await context.close()
                sys.exit(1)
            input(">>> 登录完成后按回车继续 <<<")
            await page.reload(wait_until="domcontentloaded")
            await page.wait_for_timeout(2000)
            if not await is_logged_in(page):
                print("❌ 仍未检测到登录状态，退出。")
                if own_context:
                    await context.close()
                sys.exit(1)
        print("      已登录 ✓")
        textarea = page.locator("textarea.t-textarea__inner")
        await textarea.wait_for(state="visible", timeout=30000)
        # ★ 新增：新建对话，清掉上一次会话的残留内容
        await start_new_chat(page)
        print("[4/6] 开启联网搜索...")
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
        # 基线（双保险）：新建对话后正常应为 None
        try:
            body_before = await page.locator("body").text_content() or ""
        except Exception:
            body_before = ""
        baseline_html = extract_html_from_text(body_before)    
        
        if baseline_html:
            print(f"      [基线] 页面仍有旧 HTML（长度 {len(baseline_html)}），提取时将排除")
        else:
            print("      [基线] 当前会话干净，无旧 HTML ✓")
        print("[5/6] 填入提示词并发送...")
        await textarea.click()
        await textarea.fill("")
        await textarea.fill(prompt)
        await page.wait_for_timeout(500)
        send_btn = page.locator(".hy-chat-input-send-btn")
        try:
            await page.wait_for_selector(
                ".hy-chat-input-send-btn:not(.hy-chat-input-send-btn--disabled)",
                timeout=5000,
            )
        except PlaywrightTimeout:
            print("      发送按钮仍显示 disabled，尝试强制点击...")
        await send_btn.click()
        print(f"      已点击发送，等待模型生成（最长 {GENERATION_TIMEOUT} 秒）...")
        await wait_for_generation_finish(page, GENERATION_TIMEOUT)

        html_code = None
        last_good = None
        last_change_time = asyncio.get_event_loop().time()
        start_time = asyncio.get_event_loop().time()
        diag_rounds = 0
        error_seen = None       

        while asyncio.get_event_loop().time() - start_time < GENERATION_TIMEOUT:
            await page.wait_for_timeout(4000)

            candidates = []

            code_blocks = page.locator("pre, code, .markdown-body, [class*='code']")
            count = await code_blocks.count()
            for i in range(count):
                try:
                    text = await code_blocks.nth(i).text_content() or ""
                    extracted = extract_html_from_text(text)
                    if extracted and len(extracted) > 800:
                        extracted = strip_line_numbers(extracted)   # ★ 第1处：加这一行
                        candidates.append(extracted)
                except Exception:
                    pass


            try:
                body_text = await page.locator("body").text_content() or ""
                extracted = extract_html_from_text(body_text)
                if extracted and len(extracted) > 800:
                    candidates.append(extracted)
            except Exception:
                pass
            if diag_rounds < 2:
                doctype_pos = body_text.find("DOCTYPE")
                print(f"      [诊断] 代码块数量: {count}, 候选数: {len(candidates)}, "
                      f"页面文本长度: {len(body_text)}, DOCTYPE位置: {doctype_pos}")
                diag_rounds += 1
            # 过滤：与基线相同的旧内容（新对话成功时基线为 None，此过滤不生效）
            filtered = []
            for c in candidates:
                if baseline_html and abs(len(c) - len(baseline_html)) < 300 and c[:300] == baseline_html[:300]:
                    continue
                filtered.append(c)
            if filtered:
                best = max(filtered, key=len)
                if last_good is None or len(best) > len(last_good) + 50:
                    last_good = best
                    last_change_time = asyncio.get_event_loop().time()
                    print(f"      检测到新 HTML 候选，当前长度 {len(best)} …")
                elif (asyncio.get_event_loop().time() - last_change_time) >= STABLE_SECONDS:
                    html_code = last_good
                    print(f"      内容已稳定约 {STABLE_SECONDS} 秒，判定生成完成")
                    break
            else:
                err = await detect_error_on_page(page)
                if err and err != error_seen:
                    error_seen = err
                    print(f"      ⚠️ 检测到页面报错提示：「{err}」，且无新 HTML 生成")
            elapsed = int(asyncio.get_event_loop().time() - start_time)
            if elapsed > 0 and elapsed % 30 < 4:
                print(f"      已等待 {elapsed} 秒...")
        # 超时兜底
        if not html_code:
            if last_good and len(last_good) > 800:
                html_code = last_good
                print(f"      超时，使用最后一次候选 HTML（长度 {len(html_code)}）")
            else:
                if error_seen:
                    raise RuntimeError(
                        f"模型返回异常（「{error_seen}」），未生成新内容。"
                        f"请检查 AI Studio 网页后重试。"
                    )
                screenshot_path = OUTPUT_DIR / f"debug_{get_beijing_date()}.png"
                await page.screenshot(path=str(screenshot_path), full_page=True)
                try:
                    diag_txt = OUTPUT_DIR / f"debug_{get_beijing_date()}.txt"
                    diag_txt.write_text(body_text[:200000], encoding="utf-8")
                    print(f"      已保存页面文本: {diag_txt}")
                except Exception:
                    pass
                print(f"❌ 未能提取到完整 HTML，已保存截图: {screenshot_path}")
                if own_context:
                    await context.close()
                raise RuntimeError("生成超时或未能提取 HTML")
        if not isinstance(html_code, str) or len(html_code) < 500:
            if own_context:
                await context.close()
            raise RuntimeError(f"提取结果无效: type={type(html_code)}, 预览={repr(html_code)[:100]}")
        print(f"      成功提取 HTML，长度: {len(html_code)} 字符")

        # ★ 保存前有效性校验：拦截模型报错时复读的示例页/旧页面
        ok, reasons = validate_html(html_code)
        if not ok:
            print("      ❌ HTML 校验失败：")
            for r in reasons:
                print(f"         - {r}")
            # 保存失败现场，便于排查
            debug_path = OUTPUT_DIR / f"debug_invalid_{get_beijing_date()}.html"
            debug_path.write_text(html_code, encoding="utf-8")
            print(f"      无效 HTML 已另存: {debug_path}")
            if own_context:
                await context.close()
            return None                    # main() 会接住 None → 任务失败

        print("      HTML 校验通过 ✓")
        if own_context:
            await context.close()
        return html_code


def save_and_optional_push(html_code: str, date_str: str) -> Path:
    """保存文件，可选提交到 GitHub"""
    if not html_code or not isinstance(html_code, str):
        raise TypeError(f"html_code 无效，无法保存: type={type(html_code)}")

    filename = f"24小时热点_{date_str}.html"
    output_path = OUTPUT_DIR / filename
    output_path.write_text(html_code, encoding="utf-8")
    print(f"[6/6] 已保存: {output_path}")
    
    if ENABLE_GIT_PUSH:
        if not TREND_REPO_PATH.exists():
            print(f"⚠️  未找到本地仓库 {TREND_REPO_PATH}，跳过 git 提交")
            return output_path

        ht_dir = TREND_REPO_PATH / "ht"
        ht_dir.mkdir(parents=True, exist_ok=True)
        target = ht_dir / filename
        target.write_text(html_code, encoding="utf-8")

        try:
            subprocess.run(
                ["git", "-C", str(TREND_REPO_PATH), "add", f"ht/{filename}"],
                check=True,
            )
            subprocess.run(
                [
                    "git",
                    "-C",
                    str(TREND_REPO_PATH),
                    "commit",
                    "-m",
                    f"{GIT_COMMIT_MSG_PREFIX} {filename}",
                ],
                check=True,
            )
            subprocess.run(
                ["git", "-C", str(TREND_REPO_PATH), "push"],
                check=True,
            )
            print(f"      已推送到 digital-era/Trend/ht/{filename}")
        except subprocess.CalledProcessError as e:
            print(f"⚠️  Git 操作失败: {e}")

    return output_path


async def main():
    date_str = get_beijing_date()
    print(f"===== 开始生成 24小时热点图谱 ({date_str}) =====\n")
    clean_output_dir()
    prompt = fetch_prompt()
    html_code = await generate_hot_page(prompt)
    if not html_code:
        # 生成无效（校验失败/提取失败）→ 退出码 2，与其他异常区分
        print("❌ 本次生成无效，任务判定为失败")
        sys.exit(2)
    save_and_optional_push(html_code, date_str)
    print("\n✅ 全部完成！")


if __name__ == "__main__":
    asyncio.run(main())
