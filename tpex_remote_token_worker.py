#!/usr/bin/env python3
"""
遠端 Turnstile Token 生產器 — 在 GitHub Actions / Codespaces 執行
生成的 token 透過 HTTP POST 傳回本地主程式的 Token 接收器

用法:
    python tpex_remote_token_worker.py <RELAY_URL> [NUM_WORKERS]
    python tpex_remote_token_worker.py https://xxxx.ngrok-free.app 5

環境需求:
    pip install playwright aiohttp
    playwright install chromium
"""

import asyncio
import sys
import time
import aiohttp
from playwright.async_api import async_playwright

# ===== TPEX Turnstile 設定 (與主程式一致) =====
TPEX_URL = "https://www.tpex.org.tw/zh-tw/mainboard/trading/info/brokerBS.html"
SITEKEY = "0x4AAAAAAA5dUtKdGUVDW9i-"
SOLVER_HTML = f"""<!DOCTYPE html>
<html><head><meta charset="UTF-8"><title>S</title>
<script src="https://challenges.cloudflare.com/turnstile/v0/api.js" async></script>
</head><body><div class="cf-turnstile" data-sitekey="{SITEKEY}"></div></body></html>"""
TURNSTILE_TIMEOUT = 40  # 秒


async def setup_solver_route(page):
    """攔截 TPEX 頁面請求, 回傳極簡 Turnstile HTML"""
    async def intercept(route):
        req = route.request
        if req.resource_type == 'document' and 'tpex.org.tw' in req.url:
            await route.fulfill(
                status=200,
                content_type='text/html; charset=utf-8',
                body=SOLVER_HTML
            )
        else:
            await route.continue_()
    await page.route("**/*", intercept)


async def get_token(page, worker_id, first_time=False):
    """導航/reload 極簡頁面並等待 Turnstile token"""
    try:
        if first_time:
            await page.goto(TPEX_URL, wait_until='domcontentloaded', timeout=60000)
        else:
            await page.reload(wait_until='domcontentloaded')

        for _ in range(TURNSTILE_TIMEOUT):
            token = await page.evaluate(
                '() => { const e = document.querySelector("[name=cf-turnstile-response]"); return e ? e.value : ""; }'
            )
            if token:
                return token
            await asyncio.sleep(1)

        print(f"[W{worker_id}] Turnstile 超時")
        return None
    except Exception as e:
        print(f"[W{worker_id}] 取 token 失敗: {e}")
        return None


async def token_worker(browser, relay_url, worker_id, session, stop_event):
    """單個 Token 生產者: 生 token → POST 到 relay"""
    context = await browser.new_context(viewport={'width': 1280, 'height': 900})
    page = await context.new_page()
    await setup_solver_route(page)
    token_count = 0
    consecutive_fails = 0

    try:
        while not stop_event.is_set():
            token = await get_token(page, worker_id, token_count == 0)
            if token:
                try:
                    async with session.post(
                        f"{relay_url}/token",
                        json={"token": token}
                    ) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            token_count += 1
                            consecutive_fails = 0
                            if token_count % 5 == 0:
                                print(f"[W{worker_id}] ✓ 已送出 {token_count} 個 token (queue={data.get('queue', '?')})")
                        else:
                            print(f"[W{worker_id}] 伺服器拒絕: HTTP {resp.status}")
                except aiohttp.ClientError as e:
                    print(f"[W{worker_id}] 傳送失敗: {e}")
                    # 如果連不上 relay, 等一下再試
                    await asyncio.sleep(5)
            else:
                consecutive_fails += 1
                if consecutive_fails >= 5:
                    print(f"[W{worker_id}] 連續5次失敗, 重建分頁...")
                    try:
                        await page.close()
                        await context.close()
                    except:
                        pass
                    context = await browser.new_context(viewport={'width': 1280, 'height': 900})
                    page = await context.new_page()
                    await setup_solver_route(page)
                    consecutive_fails = 0
                    await asyncio.sleep(5)

            # 定期檢查主程式是否已完成
            if token_count > 0 and token_count % 10 == 0:
                try:
                    async with session.get(f"{relay_url}/status") as resp:
                        data = await resp.json()
                        if data.get('done', False):
                            print(f"[W{worker_id}] 主程式已完成, 停止生產")
                            stop_event.set()
                            break
                except:
                    pass

    except Exception as e:
        print(f"[W{worker_id}] 異常: {e}")
    finally:
        try:
            await page.close()
            await context.close()
        except:
            pass
        print(f"[W{worker_id}] 結束 (共產生 {token_count} 個 token)")


async def main(relay_url, num_workers=5):
    print("=" * 60)
    print(f"TPEX 遠端 Token 生產器")
    print(f"  Relay URL:  {relay_url}")
    print(f"  Workers:    {num_workers}")
    print(f"  Sitekey:    {SITEKEY[:15]}...")
    print("=" * 60)

    stop_event = asyncio.Event()

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)
        print("✓ Chromium 瀏覽器已啟動 (headless)")

        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=15)
        ) as session:
            # 先測試 relay 連線
            print(f"測試連線 {relay_url}/status ...")
            try:
                async with session.get(f"{relay_url}/status") as resp:
                    data = await resp.json()
                    print(f"✓ 連線成功! 目前進度: {data.get('progress', 0)}/{data.get('total', '?')}")
                    if data.get('done', False):
                        print("主程式已完成, 無需生產 token")
                        await browser.close()
                        return
            except Exception as e:
                print(f"✗ 無法連線到 relay: {e}")
                print("  請確認主程式已啟動且 ngrok 正在運行")
                await browser.close()
                return

            # 啟動 workers
            tasks = []
            for i in range(num_workers):
                t = asyncio.create_task(
                    token_worker(browser, relay_url, i + 1, session, stop_event)
                )
                tasks.append(t)
                await asyncio.sleep(2)  # 錯開啟動

            print(f"✓ {num_workers} 個 token worker 已啟動")
            start_time = time.time()

            await asyncio.gather(*tasks, return_exceptions=True)

            elapsed = time.time() - start_time
            print(f"\n全部 worker 結束 (運行 {elapsed:.0f} 秒)")

        await browser.close()
        print("✓ 瀏覽器已關閉")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("用法: python tpex_remote_token_worker.py <RELAY_URL> [NUM_WORKERS]")
        print("範例: python tpex_remote_token_worker.py https://xxxx.ngrok-free.app 5")
        print()
        print("步驟:")
        print("  1. 在本地電腦啟動主程式 (會自動開啟 port 9999 的 token 接收器)")
        print("  2. 在本地電腦執行: ngrok http 9999")
        print("  3. 複製 ngrok 給的 URL (如 https://xxxx.ngrok-free.app)")
        print("  4. 在 GitHub Actions 或 Codespaces 執行此腳本, 傳入 ngrok URL")
        sys.exit(1)

    relay_url = sys.argv[1].rstrip('/')
    num_workers = int(sys.argv[2]) if len(sys.argv) > 2 else 5

    asyncio.run(main(relay_url, num_workers))
