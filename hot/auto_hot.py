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

# 浏览器数据目录（第一次运行会在此保存登录态）
BROWSER_DATA_DIR = Path("./browser_data")

# 生成结果保存目录
OUTPUT_DIR = Path("./output")

# 是否提交到 GitHub（需要先 git clone digital-era/Trend 到本地）
ENABLE_GIT_PUSH = False
TREND_REPO_PATH = Path("./Trend")          # 本地 Trend 仓库路径
GIT_COMMIT_MSG_PREFIX = "add hot topics"

# 生成超时（秒），热点生成可能较慢
GENERATION_TIMEOUT = 600  # 10 分钟

# 无头模式：调试时设为 False，正式运行可设为 True
HEADLESS = False
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
    # 优先匹配 markdown 代码块
    patterns = [
        r"```(?:html)?\s*(<!DOCTYPE html>[\s\S]*?</html>)\s*```",
        r"(<!DOCTYPE html>[\s\S]*?</html>)",
    ]
    for pat in patterns:
        m = re.search(pat, text, re.IGNORECASE)
        if m:
            return m.group(1).strip()
    return None


async def generate_hot_page(prompt: str) -> str:
    """核心：打开 AI Studio → 开启联网搜索 → 提交 → 提取 HTML"""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    BROWSER_DATA_DIR.mkdir(parents=True, exist_ok=True)

    async with async_playwright() as p:
        print("[2/6] 启动浏览器（首次请手动登录）...")
        context = await p.chromium.launch_persistent_context(
            user_data_dir=str(BROWSER_DATA_DIR),
            headless=HEADLESS,
            viewport={"width": 1440, "height": 900},
            locale="zh-CN",
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
            ],
        )

        page = context.pages[0] if context.pages else await context.new_page()

        # 打开页面
        print("[3/6] 打开 AI Studio...")
        await page.goto(AI_STUDIO_URL, wait_until="domcontentloaded", timeout=60000)
        await page.wait_for_timeout(2000)

        # 关闭可能出现的国际站弹窗
        try:
            stay_btn = page.get_by_role("button", name=re.compile("Stay here|留在此处|留在这里", re.I))
            if await stay_btn.count() > 0:
                await stay_btn.first.click()
                await page.wait_for_timeout(800)
        except Exception:
            pass

        # 检查是否已登录（右上角没有「登录」按钮）
        login_btn = page.locator(".side-menu-layout__r__btn", has_text="登录")
        if await login_btn.count() > 0 and await login_btn.is_visible():
            print("\n⚠️  检测到未登录！")
            print("   请在打开的浏览器窗口中手动登录（微信/QQ/手机号），")
            print("   登录成功后回到终端按回车继续...\n")
            if HEADLESS:
                print("无头模式下无法登录，请先将 HEADLESS = False 运行一次完成登录。")
                await context.close()
                sys.exit(1)
            input(">>> 登录完成后按回车继续 <<<")
            await page.reload(wait_until="domcontentloaded")
            await page.wait_for_timeout(2000)

        # 等待输入框出现
        textarea = page.locator("textarea.t-textarea__inner")
        await textarea.wait_for(state="visible", timeout=30000)

        # 开启联网搜索
        print("[4/6] 开启联网搜索...")
        online_btn = page.locator('[data-tool-key="online"]')
        await online_btn.wait_for(state="visible", timeout=10000)

        # 如果还没激活就点击
        active_icon = page.locator(".online-tool-icon--active")
        if await active_icon.count() == 0:
            await online_btn.click()
            await page.wait_for_timeout(600)
            # 再次确认
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
        await textarea.fill("")  # 先清空
        await textarea.fill(prompt)
        await page.wait_for_timeout(500)

        # 点击发送按钮
        send_btn = page.locator(".hy-chat-input-send-btn")
        # 等待按钮变为可用状态
        try:
            await page.wait_for_selector(
                ".hy-chat-input-send-btn:not(.hy-chat-input-send-btn--disabled)",
                timeout=5000,
            )
        except PlaywrightTimeout:
            print("      发送按钮仍显示 disabled，尝试强制点击...")

        await send_btn.click()
        print("      已点击发送，等待模型生成（最长 {} 秒）...".format(GENERATION_TIMEOUT))

        # 等待生成完成
        # 策略：等待发送按钮重新出现 disabled（表示本轮生成结束），
        # 或页面出现新的 HTML 代码块
        html_code = None
        start_time = asyncio.get_event_loop().time()

        while asyncio.get_event_loop().time() - start_time < GENERATION_TIMEOUT:
            await page.wait_for_timeout(3000)

            # 尝试从页面提取最新回复中的 HTML
            # 方法1：查找所有 pre / code 块
            code_blocks = page.locator("pre, code, .markdown-body, [class*='code']")
            count = await code_blocks.count()
            if count > 0:
                # 取最后一个较大的代码块
                for i in range(count - 1, -1, -1):
                    text = await code_blocks.nth(i).inner_text()
                    extracted = extract_html_from_text(text)
                    if extracted and len(extracted) > 500:
                        html_code = extracted
                        break

            if html_code:
                break

            # 方法2：获取整个对话区域文本再提取
            try:
                body_text = await page.locator("body").inner_text()
                extracted = extract_html_from_text(body_text)
                if extracted and len(extracted) > 500:
                    # 再等一会儿确保生成结束
                    await page.wait_for_timeout(5000)
                    body_text = await page.locator("body").inner_text()
                    extracted2 = extract_html_from_text(body_text)
                    if extracted2 and len(extracted2) >= len(extracted):
                        html_code = extracted2
                        break
            except Exception:
                pass

            # 简单进度提示
            elapsed = int(asyncio.get_event_loop().time() - start_time)
            if elapsed % 30 == 0:
                print(f"      已等待 {elapsed} 秒...")

        if not html_code:
            # 最后一次尝试：截图保存方便排查
            screenshot_path = OUTPUT_DIR / f"debug_{get_beijing_date()}.png"
            await page.screenshot(path=str(screenshot_path), full_page=True)
            print(f"❌ 未能提取到完整 HTML，已保存截图: {screenshot_path}")
            print("   请手动检查页面或增加 GENERATION_TIMEOUT")
            await context.close()
            raise RuntimeError("生成超时或未能提取 HTML")

        print(f"      成功提取 HTML，长度: {len(html_code)} 字符")
        await context.close()
        return html_code


def save_and_optional_push(html_code: str, date_str: str) -> Path:
    """保存文件，可选提交到 GitHub"""
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
            subprocess.run(["git", "-C", str(TREND_REPO_PATH), "add", f"ht/{filename}"], check=True)
            subprocess.run(
                ["git", "-C", str(TREND_REPO_PATH), "commit", "-m", f"{GIT_COMMIT_MSG_PREFIX} {filename}"],
                check=True,
            )
            subprocess.run(["git", "-C", str(TREND_REPO_PATH), "push"], check=True)
            print(f"      已推送到 digital-era/Trend/ht/{filename}")
        except subprocess.CalledProcessError as e:
            print(f"⚠️  Git 操作失败: {e}")

    return output_path


async def main():
    date_str = get_beijing_date()
    print(f"===== 开始生成 24小时热点图谱 ({date_str}) =====\n")

    prompt = fetch_prompt()
    html_code = await generate_hot_page(prompt)
    save_and_optional_push(html_code, date_str)

    print("\n✅ 全部完成！")


if __name__ == "__main__":
    asyncio.run(main())
