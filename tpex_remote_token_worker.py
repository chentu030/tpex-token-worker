#!/usr/bin/env python3
"""
TPEX 遠端下載 Worker — 在 GitHub Actions 執行
流程: 取股票代碼 → 生成 Turnstile token → 直接下載 CSV → 上傳 CSV 到本地
(token IP = 下載 IP, 不會有 IP 不匹配問題)

用法:
    python tpex_remote_token_worker.py <RELAY_URL> [NUM_WORKERS]
    python tpex_remote_token_worker.py https://xxxx.ngrok-free.app 5

環境需求:
    pip install patchright aiohttp
    patchright install chromium
"""

import asyncio
import sys
import time
import aiohttp
from patchright.async_api import async_playwright

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


async def download_csv(page, download_url, code, token):
    """用 page.evaluate(fetch) 下載 CSV (確保 IP 一致 + 瀏覽器 TLS 指紋)"""
    url = f"{download_url}?cf-turnstile-response={token}&code={code}&id=&response=utf-8"
    result = await page.evaluate("""
        async (url) => {
            try {
                const resp = await fetch(url);
                const text = await resp.text();
                return { ok: resp.ok, status: resp.status, text: text };
            } catch(e) {
                return { ok: false, error: e.message, text: "" };
            }
        }
    """, url)
    return result


async def download_worker(browser, relay_url, worker_id, session, stop_event):
    """下載 Worker: 取股票 → 生成token → 下載CSV → 上傳結果"""
    context = await browser.new_context(viewport={'width': 1280, 'height': 900})
    page = await context.new_page()
    await setup_solver_route(page)

    download_count = 0
    fail_count = 0
    empty_count = 0  # 空回應次數 (歸入 fail, 不標記 nodata)
    token_count = 0
    consecutive_token_fails = 0
    idle_count = 0

    try:
        while not stop_event.is_set():
            # 1. 向 relay 取得下一個待下載的股票
            try:
                async with session.get(f"{relay_url}/next_stock") as resp:
                    data = await resp.json()
            except aiohttp.ClientError as e:
                print(f"[W{worker_id}] 無法連線 relay: {e}")
                await asyncio.sleep(5)
                continue

            code = data.get('code')
            if data.get('done', False):
                print(f"[W{worker_id}] 主程式已完成")
                break

            if not code:
                idle_count += 1
                if idle_count > 30:  # 60秒無股票 → 結束
                    print(f"[W{worker_id}] 長時間無待下載股票, 結束")
                    break
                await asyncio.sleep(2)
                continue

            idle_count = 0
            download_url = data.get('download_url', '')

            # 2. 生成 Turnstile token
            token = await get_token(page, worker_id, token_count == 0)
            if not token:
                consecutive_token_fails += 1
                # 放回佇列
                try:
                    async with session.post(
                        f"{relay_url}/upload",
                        json={'code': code, 'status': 'fail'}
                    ):
                        pass
                except:
                    pass

                if consecutive_token_fails >= 5:
                    print(f"[W{worker_id}] 連續{consecutive_token_fails}次 token 失敗, 重建分頁...")
                    try:
                        await page.close()
                        await context.close()
                    except:
                        pass
                    context = await browser.new_context(viewport={'width': 1280, 'height': 900})
                    page = await context.new_page()
                    await setup_solver_route(page)
                    consecutive_token_fails = 0
                    await asyncio.sleep(5)
                continue

            consecutive_token_fails = 0
            token_count += 1

            # 3. 用 page.evaluate(fetch) 下載 CSV
            try:
                result = await download_csv(page, download_url, code, token)
            except Exception as e:
                print(f"[W{worker_id}] 下載異常 {code}: {e}")
                try:
                    async with session.post(
                        f"{relay_url}/upload",
                        json={'code': code, 'status': 'fail'}
                    ):
                        pass
                except:
                    pass
                fail_count += 1
                continue

            csv_text = result.get('text', '')
            stripped = csv_text.strip()

            # 檢查結果
            # 遠端 worker 不標記 nodata (GitHub IP 可能收到假空回應)
            # 只有包含正確 CSV 標頭的才算 ok, 其餘都 fail 放回佇列
            if not result.get('ok'):
                status = 'fail'
                fail_count += 1
            elif stripped.startswith('<!DOCTYPE') or stripped.startswith('<html'):
                status = 'html'
                fail_count += 1
            elif '券商' in stripped and len(stripped) >= 10:
                status = 'ok'
                download_count += 1
            else:
                # 空回應或無法辨識 → 當作失敗, 放回佇列讓本地處理
                status = 'fail'
                fail_count += 1
                if len(stripped) < 10:
                    empty_count += 1

            # 4. 上傳結果到 relay
            try:
                upload_data = {'code': code, 'status': status}
                if status == 'ok':
                    upload_data['csv'] = csv_text

                async with session.post(
                    f"{relay_url}/upload",
                    json=upload_data,
                    timeout=aiohttp.ClientTimeout(total=30)
                ) as resp:
                    resp_data = await resp.json()
                    if status == 'html':
                        print(f"[W{worker_id}] ⚠ {code} 收到HTML (token可能過期)")
            except aiohttp.ClientError as e:
                print(f"[W{worker_id}] 上傳失敗 {code}: {e}")
                fail_count += 1
                continue

            total = download_count + fail_count
            if total % 5 == 0 or total <= 3:
                tag = '✓' if status == 'ok' else '✗'
                print(f"[W{worker_id}] {tag} {code} (成功{download_count}/失敗{fail_count}/空回應{empty_count})")

    except Exception as e:
        print(f"[W{worker_id}] 異常: {e}")
    finally:
        try:
            await page.close()
            await context.close()
        except:
            pass
        print(f"[W{worker_id}] 結束: 成功{download_count} 失敗{fail_count} (空回應{empty_count})")


async def main(relay_url, num_workers=5):
    print("=" * 60)
    print("TPEX 遠端下載 Worker (GitHub Actions)")
    print(f"  Relay URL:  {relay_url}")
    print(f"  Workers:    {num_workers}")
    print(f"  模式:       取股票→生成token→下載CSV→上傳結果")
    print("=" * 60)

    stop_event = asyncio.Event()

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=False)
        print("✓ Chromium 瀏覽器已啟動 (headed + Xvfb 虛擬螢幕)")

        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=30)
        ) as session:
            # 先測試 relay 連線
            print(f"測試連線 {relay_url}/status ...")
            try:
                async with session.get(f"{relay_url}/status") as resp:
                    data = await resp.json()
                    remaining = data.get('stocks_remaining', '?')
                    progress = data.get('progress', 0)
                    total = data.get('total', '?')
                    print(f"✓ 連線成功! 進度: {progress}/{total}, 剩餘: {remaining}")
                    if data.get('done', False):
                        print("主程式已完成, 無需下載")
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
                    download_worker(browser, relay_url, i + 1, session, stop_event)
                )
                tasks.append(t)
                await asyncio.sleep(2)  # 錯開啟動

            print(f"✓ {num_workers} 個下載 worker 已啟動")
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
        print("流程:")
        print("  GitHub Actions: 取股票 → 生成token → 下載CSV → 上傳CSV")
        print("  本地主程式:     分配股票 ← ─ ─ ─ ─ ─ ─ ─ ─ → 儲存檔案")
        print()
        print("步驟:")
        print("  1. 在本地電腦啟動主程式 (會自動開啟 port 9999)")
        print("  2. 在本地電腦執行: ngrok http 9999")
        print("  3. 複製 ngrok 給的 URL (如 https://xxxx.ngrok-free.app)")
        print("  4. 到 GitHub → Actions → Run workflow → 貼入 URL")
        sys.exit(1)

    relay_url = sys.argv[1].rstrip('/')
    num_workers = int(sys.argv[2]) if len(sys.argv) > 2 else 5

    asyncio.run(main(relay_url, num_workers))
