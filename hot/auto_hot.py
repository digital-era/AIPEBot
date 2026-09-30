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

# 生成结果保存目录
OUTPUT_DIR = Path("./output")

# 是否提交到 GitHub（需要先 git clone digital-era/Trend 到本地）
ENABLE_GIT_PUSH = False
TREND_REPO_PATH = Path("./Trend")
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


async def generate_hot_page(prompt: str) -> str:
    """核心：打开 AI Studio → 开启联网搜索 → 提交 → 提取 HTML"""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    async with async_playwright() as p:
        context = None
        browser = None
        own_context = False  # 是否由本脚本创建的 context（决定结束时是否关闭）

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
            # 优先复用已打开的 aistudio 标签页
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

        # 打开 / 刷新页面
        print("[3/6] 打开 AI Studio...")
        if "aistudio.tencent.com" not in (page.url or ""):
            await page.goto(AI_STUDIO_URL, wait_until="domcontentloaded", timeout=60000)
        else:
            await page.reload(wait_until="domcontentloaded")
        await page.wait_for_timeout(2000)

        # 关闭可能出现的国际站弹窗
        try:
            stay_btn = page.get_by_role(
                "button", name=re.compile(r"Stay here|留在此处|留在这里", re.I)
            )
            if await stay_btn.count() > 0:
                await stay_btn.first.click()
                await page.wait_for_timeout(800)
        except Exception:
            pass

        # 登录检查
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

        # 等待输入框出现
        textarea = page.locator("textarea.t-textarea__inner")
        await textarea.wait_for(state="visible", timeout=30000)

        # 开启联网搜索
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

        # 清空并填入提示词
        print("[5/6] 填入提示词并发送...")
        await textarea.click()
        await textarea.fill("")
        await textarea.fill(prompt)
        await page.wait_for_timeout(500)

        # 点击发送按钮
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

        # 记录发送前页面上已有的 HTML（避免抽到提示词里的样例）
        try:
            body_before = await page.locator("body").inner_text()
        except Exception:
            body_before = ""
        prompt_sample = extract_html_from_text(body_before)  # 可能是提示词里的模板

        html_code = None
        last_good = None
        last_change_time = asyncio.get_event_loop().time()
        start_time = asyncio.get_event_loop().time()

        while asyncio.get_event_loop().time() - start_time < GENERATION_TIMEOUT:
            await page.wait_for_timeout(4000)

            candidates = []

            # 方法1：代码块（优先最后几个大块）
            code_blocks = page.locator("pre, code, .markdown-body, [class*='code']")
            count = await code_blocks.count()
            for i in range(max(0, count - 8), count):
                try:
                    text = await code_blocks.nth(i).inner_text()
                    extracted = extract_html_from_text(text)
                    if extracted and len(extracted) > 800:
                        candidates.append(extracted)
                except Exception:
                    pass

            # 方法2：整页文本
            try:
                body_text = await page.locator("body").inner_text()
                extracted = extract_html_from_text(body_text)
                if extracted and len(extracted) > 800:
                    candidates.append(extracted)
            except Exception:
                pass

            # 过滤：去掉和「发送前样例」几乎一样的内容（提示词模板）
            filtered = []
            for c in candidates:
                if prompt_sample and len(prompt_sample) > 500:
                    # 相似度粗判：前 200 字符相同且总长度接近 → 视为样例
                    if c[:200] == prompt_sample[:200] and abs(len(c) - len(prompt_sample)) < 300:
                        continue
                filtered.append(c)

            if filtered:
                # 取最长的一份（通常是完整页面）
                best = max(filtered, key=len)
                if last_good is None or len(best) > len(last_good) + 50:
                    last_good = best
                    last_change_time = asyncio.get_event_loop().time()
                    print(f"      检测到 HTML 候选，当前长度 {len(best)} …")
                elif last_good and (asyncio.get_event_loop().time() - last_change_time) >= STABLE_SECONDS:
                    # 内容已稳定一段时间，认为生成结束
                    html_code = last_good
                    print(f"      内容已稳定约 {STABLE_SECONDS} 秒，判定生成完成")
                    break

            elapsed = int(asyncio.get_event_loop().time() - start_time)
            if elapsed > 0 and elapsed % 30 == 0:
                print(f"      已等待 {elapsed} 秒...")

        # 超时兜底
        if not html_code:
            if last_good and len(last_good) > 800:
                html_code = last_good
                print(f"      超时，使用最后一次候选 HTML（长度 {len(html_code)}）")
            else:
                screenshot_path = OUTPUT_DIR / f"debug_{get_beijing_date()}.png"
                await page.screenshot(path=str(screenshot_path), full_page=True)
                print(f"❌ 未能提取到完整 HTML，已保存截图: {screenshot_path}")
                if own_context:
                    await context.close()
                raise RuntimeError("生成超时或未能提取 HTML")

        # ---- 关键：统一确认类型再返回 ----
        if not isinstance(html_code, str) or len(html_code) < 500:
            if own_context:
                await context.close()
            raise RuntimeError(f"提取结果无效: type={type(html_code)}, 预览={repr(html_code)[:100]}")

        print(f"      成功提取 HTML，长度: {len(html_code)} 字符")

        if own_context:
            await context.close()

        return html_code   # 必须有这一行，且不要写成 return 或 return None

        print(f"      成功提取 HTML，长度: {len(html_code)} 字符")


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
    prompt = fetch_prompt()
    html_code = await generate_hot_page(prompt)
    if not html_code:
        raise RuntimeError("generate_hot_page 返回了空结果")
    save_and_optional_push(html_code, date_str)
    print("\n✅ 全部完成！")

if __name__ == "__main__":
    asyncio.run(main())
