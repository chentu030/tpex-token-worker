#!/usr/bin/env python3
"""
遠端 TPEX 下載 Worker — 在 GitHub Actions 執行
取得股票代碼 → 生成 Turnstile token → 下載 CSV → 回傳結果

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
import traceback
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
        current_url = page.url or ''
        need_navigate = first_time or 'about:blank' in current_url or 'tpex.org.tw' not in current_url

        if need_navigate:
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
        print(f"[W{worker_id}] 取 token 失敗: {type(e).__name__}: {e}")
        return None


FETCH_JS_TEMPLATE = """
async (url) => {
    try {
        const resp = await fetch(url, {credentials: 'include'});
        if (!resp.ok) return {ok: false, status: resp.status, text: ''};
        const text = await resp.text();
        return {ok: true, status: resp.status, text: text};
    } catch(e) {
        return {ok: false, status: 0, text: e.toString()};
    }
}
"""

# Turnstile token 是一次性的, 每次下載都要新 token
BATCH_PER_TOKEN = 1

# ===== 頻率限制偵測 & 冷却機制 =====
RATE_LIMIT_EMPTY_THRESHOLD = 3   # 單一 worker 連續空回應次數 → 觤發全局冷却
COOLDOWN_FIXED = 900             # 固定冷却秒數 (15分鐘)


PAGE_REBUILD_INTERVAL = 50  # 每 N 次 token 生成後重建分頁 (防止記憶體洩漏)
MAX_BROWSER_CRASHES = 3     # 瀏覽器崩潰恢復上限

# ===== 批次暫停機制 (避免 TPEX 頻率封鎖) =====
FIRST_BATCH_SIZE = 1300       # 第一批處理數量
SUBSEQUENT_BATCH_SIZE = 1300  # 後續每批處理數量
BATCH_PAUSE_SECONDS = 15 * 60 # 批次間暫停時間 (15分鐘)


async def download_worker(browser, relay_url, worker_id, session, dl_session, stop_event, stats, cooldown_state, batch_state=None):
    """單個下載 Worker: 用瀏覽器 fetch() 下載, 一個 token 批量處理多支
    cooldown_state: [cooldown_until] 共享的冷却狀態 (固定 15 分鐘)
    batch_state: [batch_total, batch_limit, batch_pause_until] 共享的批次暫停狀態"""
    cooldown_until = cooldown_state
    batch_total, batch_limit, batch_pause_until = batch_state if batch_state else ([0], [999999], [0.0])
    ok_count = 0
    nodata_count = 0
    fail_count = 0
    html_rejects = 0
    token_count = 0
    consecutive_token_fails = 0
    consecutive_empty = 0  # 連續空回應計數 (頻率限制偵測)
    browser_crashes = 0

    context = None
    page = None

    async def rebuild_page(reason=""):
        """重建瀏覽器分頁 (釋放記憶體)"""
        nonlocal context, page
        if reason:
            print(f"[W{worker_id}] 重建分頁: {reason}")
        try:
            if page:
                await page.close()
        except:
            pass
        try:
            if context:
                await context.close()
        except:
            pass
        context = await browser.new_context(viewport={'width': 1280, 'height': 900})
        page = await context.new_page()
        await setup_solver_route(page)
        return context, page

    try:
        context, page = await rebuild_page()
        token = None
        batch_count = 0

        while not stop_event.is_set():
            # ---- 批次暫停檢查 ----
            now_ts = time.time()
            if now_ts < batch_pause_until[0]:
                wait_secs = batch_pause_until[0] - now_ts
                if wait_secs > 2:
                    mins_left = int(wait_secs) // 60 + 1
                    print(f"[W{worker_id}] 批次暫停中, 剩餘 {mins_left} 分鐘...")
                await asyncio.sleep(min(wait_secs, 30))
                continue

            # ---- 頻率限制冷却檢查 ----
            now_ts = time.time()
            if now_ts < cooldown_until[0]:
                wait_secs = cooldown_until[0] - now_ts
                if wait_secs > 2:
                    print(f"[W{worker_id}] 頻率限制冷却中, 等待 {wait_secs:.0f} 秒...")
                await asyncio.sleep(min(wait_secs, 10))
                consecutive_empty = 0  # 冷却結束後重置, 給新機會
                continue

            try:  # ★ 包住整個迴圈體, 捕捉瀏覽器崩潰

                # ---- Step 1: 確保有可用的 token ----
                if token is None or batch_count >= BATCH_PER_TOKEN:
                    # 定期重建分頁 (防止記憶體洩漏導致瀏覽器崩潰)
                    if token_count > 0 and token_count % PAGE_REBUILD_INTERVAL == 0:
                        context, page = await rebuild_page(f"定期重建 (第{token_count}次token)")
                        consecutive_token_fails = 0
                        await asyncio.sleep(2)

                    token = await get_token(page, worker_id, token_count == 0)
                    if not token:
                        consecutive_token_fails += 1
                        if consecutive_token_fails >= 5:
                            context, page = await rebuild_page(f"連續{consecutive_token_fails}次token失敗")
                            consecutive_token_fails = 0
                            await asyncio.sleep(3)
                        continue
                    token_count += 1
                    batch_count = 0
                    consecutive_token_fails = 0

                # ---- Step 2: 從 relay 取得股票代碼 ----
                try:
                    async with session.get(f"{relay_url}/next_stock") as resp:
                        data = await resp.json()
                        if data.get('done', False) or not data.get('code'):
                            print(f"[W{worker_id}] 沒有更多股票, 結束")
                            break
                        code = data['code']
                        download_url = data.get('download_url', '')
                except aiohttp.ClientError as e:
                    print(f"[W{worker_id}] 取股票失敗: {e}")
                    await asyncio.sleep(5)
                    continue

                # ---- Step 3: 用瀏覽器 fetch() 下載 CSV (帶完整 cookies) ----
                dl_url = f"{download_url}?cf-turnstile-response={token}&code={code}&id=&response=utf-8"
                status = 'fail'
                csv_text = ''
                try:
                    result = await page.evaluate(FETCH_JS_TEMPLATE, dl_url)
                    if result and result.get('ok'):
                        csv_text = result.get('text', '')
                        stripped = csv_text.strip()
                        if stripped.startswith('<!DOCTYPE') or stripped.startswith('<html'):
                            status = 'html'
                            csv_text = ''
                            html_rejects += 1
                            token = None  # token 過期, 下次取新的
                        elif len(stripped) < 10:
                            # 空回應: 可能是真的無資料, 也可能是 TPEX 頻率限制
                            consecutive_empty += 1
                            if consecutive_empty >= RATE_LIMIT_EMPTY_THRESHOLD:
                                # 連續多次空回應 → 判定為頻率限制, 回報 fail 讓主程式重新分配
                                status = 'fail'
                                csv_text = ''
                                token = None
                                # 觸發全局冷却 (固定 15 分鐘)
                                cooldown_until[0] = time.time() + COOLDOWN_FIXED
                                print(f"[W{worker_id}] ⚠ 頻率限制 (連續{consecutive_empty}次空回應)"
                                      f" → 全部冷却 {COOLDOWN_FIXED}秒 ({COOLDOWN_FIXED//60}分鐘)")
                            else:
                                # 前幾次空回應當作 nodata (可能真的無資料)
                                status = 'nodata'
                                csv_text = stripped
                        else:
                            status = 'ok'
                            # 成功下載 → 重置空回應計數
                            consecutive_empty = 0
                    else:
                        status = 'fail'
                        token = None  # 請求失敗, 換新 token
                except Exception as e:
                    print(f"[W{worker_id}] fetch 失敗 {code}: {e}")
                    status = 'fail'
                    token = None

                batch_count += 1

                # ---- Step 4: 上傳結果到 relay ----
                try:
                    async with session.post(f"{relay_url}/upload",
                        json={'code': code, 'csv': csv_text, 'status': status}
                    ) as resp:
                        res = await resp.json()
                        if res.get('ok'):
                            if status == 'ok':
                                ok_count += 1
                            elif status == 'nodata':
                                nodata_count += 1
                            else:
                                fail_count += 1
                        else:
                            fail_count += 1
                except Exception as e:
                    print(f"[W{worker_id}] 上傳失敗 {code}: {e}")
                    fail_count += 1

                # ---- 批次計數 & 暫停觸發 ----
                batch_total[0] += 1
                if batch_total[0] >= batch_limit[0] and batch_pause_until[0] < time.time():
                    pause_mins = BATCH_PAUSE_SECONDS // 60
                    next_batch = SUBSEQUENT_BATCH_SIZE
                    batch_pause_until[0] = time.time() + BATCH_PAUSE_SECONDS
                    batch_limit[0] += next_batch
                    print(f"[W{worker_id}] ✘ 已完成 {batch_total[0]} 支, 暫停 {pause_mins} 分鐘, 下批 {next_batch} 支")

                total_done = ok_count + nodata_count + fail_count
                if total_done % 5 == 0:
                    print(f"[W{worker_id}] 已處理 {total_done} 支 (✓{ok_count} ○{nodata_count} ✗{fail_count})")

                # 定期檢查主程式是否已完成
                if total_done % 20 == 0:
                    try:
                        async with session.get(f"{relay_url}/status") as resp:
                            sdata = await resp.json()
                            if sdata.get('done', False):
                                print(f"[W{worker_id}] 主程式已完成, 停止")
                                stop_event.set()
                                break
                    except:
                        pass

            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                # relay 連線問題, 等一下再試
                print(f"[W{worker_id}] 連線問題: {type(e).__name__}: {e}")
                await asyncio.sleep(5)
                continue

            except Exception as e:
                # ★ 瀏覽器崩潰或其他嚴重錯誤 → 嘗試重建
                browser_crashes += 1
                err_name = type(e).__name__
                print(f"[W{worker_id}] ⚠ 瀏覽器異常 ({err_name}: {e}), 嘗試恢復 ({browser_crashes}/{MAX_BROWSER_CRASHES})")
                if browser_crashes >= MAX_BROWSER_CRASHES:
                    print(f"[W{worker_id}] 瀏覽器崩潰次數過多, 放棄")
                    break
                try:
                    context, page = await rebuild_page("瀏覽器崩潰恢復")
                    token = None
                    consecutive_token_fails = 0
                    await asyncio.sleep(5)
                except Exception as e2:
                    print(f"[W{worker_id}] 重建失敗: {type(e2).__name__}: {e2}, 放棄")
                    break

    except Exception as e:
        print(f"[W{worker_id}] 致命異常: {type(e).__name__}: {e}")
        traceback.print_exc()
    finally:
        try:
            if page:
                await page.close()
            if context:
                await context.close()
        except:
            pass
        stats[worker_id] = (ok_count, nodata_count, fail_count)
        print(f"[W{worker_id}] 結束 (✓{ok_count} ○{nodata_count} ✗{fail_count}, token={token_count}, html拒絕={html_rejects})")


async def main(relay_url, num_workers=5):
    print("=" * 60)
    print(f"TPEX 遠端下載 Worker (GitHub Actions)")
    print(f"  Relay URL:  {relay_url}")
    print(f"  Workers:    {num_workers}")
    print("=" * 60)

    stop_event = asyncio.Event()
    stats = {}  # {worker_id: (ok, nodata, fail)}
    MAX_BROWSER_RESTARTS = 3  # 瀏覽器崩潰後最多重啟幾次

    relay_session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15))

    try:
        # 先測試 relay 連線
        print(f"測試連線 {relay_url}/status ...")
        try:
            async with relay_session.get(f"{relay_url}/status") as resp:
                data = await resp.json()
                print(f"✓ 連線成功! 進度: {data.get('progress', 0)}/{data.get('total', '?')}")
                remaining = data.get('stocks_remaining', '?')
                print(f"  剩餘待下載: {remaining} 支")
                if data.get('done', False):
                    print("主程式已完成, 無需下載")
                    return
        except Exception as e:
            print(f"✗ 無法連線到 relay: {type(e).__name__}: {e}")
            print("  請確認主程式已啟動且 ngrok 正在運行")
            return

        # 共享的頻率限制冷却狀態 (固定 15 分鐘, 所有 worker 一起等)
        cooldown_until = [0.0]
        cooldown_state = cooldown_until

        # 共享的批次暫停狀態 (每 1300 支 → 暫停 15 分鐘)
        batch_total = [0]                    # 所有 worker 累計處理數
        batch_limit = [FIRST_BATCH_SIZE]     # 下次暫停的閾值
        batch_pause_until = [0.0]            # 暫停到的時間戳
        batch_state = (batch_total, batch_limit, batch_pause_until)
        print(f"  批次暫停: 每 {FIRST_BATCH_SIZE}(首批)/{SUBSEQUENT_BATCH_SIZE}(後續) 支暫停 {BATCH_PAUSE_SECONDS//60} 分鐘")

        start_time = time.time()
        browser_restarts = 0

        async with async_playwright() as pw:
            while browser_restarts <= MAX_BROWSER_RESTARTS and not stop_event.is_set():
                browser = None
                try:
                    browser = await pw.chromium.launch(headless=False)
                    restart_label = f" (第{browser_restarts}次重啟)" if browser_restarts > 0 else ""
                    print(f"✓ Chromium 瀏覽器已啟動{restart_label}")

                    # 啟動 workers
                    tasks = []
                    for i in range(num_workers):
                        t = asyncio.create_task(
                            download_worker(browser, relay_url, i + 1, relay_session, None, stop_event, stats, cooldown_state, batch_state)
                        )
                        tasks.append(t)
                        await asyncio.sleep(3)

                    print(f"✓ {num_workers} 個下載 worker 已啟動")
                    results = await asyncio.gather(*tasks, return_exceptions=True)

                    # 檢查是否全員崩潰 (非正常結束)
                    all_crashed = all(isinstance(r, Exception) for r in results if r is not None)
                    any_work_done = any(s[0] > 0 for s in stats.values())

                    # 檢查 relay 是否已完成
                    relay_done = False
                    try:
                        async with relay_session.get(f"{relay_url}/status") as resp:
                            sdata = await resp.json()
                            relay_done = sdata.get('done', False)
                            remaining = sdata.get('stocks_remaining', 0)
                    except:
                        relay_done = True  # 連不上 relay 就當作完成

                    if relay_done or stop_event.is_set():
                        print("主程式已完成, 停止")
                        break

                    if all_crashed and not relay_done:
                        browser_restarts += 1
                        if browser_restarts <= MAX_BROWSER_RESTARTS:
                            print(f"\n⚠ 所有 worker 崩潰! 重啟瀏覽器 ({browser_restarts}/{MAX_BROWSER_RESTARTS})...")
                            await asyncio.sleep(5)
                            continue
                        else:
                            print(f"\n✗ 瀏覽器崩潰次數過多 ({MAX_BROWSER_RESTARTS}), 放棄")
                            break
                    else:
                        break  # 正常結束

                except Exception as e:
                    print(f"\n✗ 瀏覽器層異常: {type(e).__name__}: {e}")
                    traceback.print_exc()
                    browser_restarts += 1
                    if browser_restarts <= MAX_BROWSER_RESTARTS:
                        print(f"重啟瀏覽器 ({browser_restarts}/{MAX_BROWSER_RESTARTS})...")
                        await asyncio.sleep(5)
                    else:
                        break
                finally:
                    if browser:
                        try:
                            await browser.close()
                        except:
                            pass

        elapsed = time.time() - start_time

        # 總結
        total_ok = sum(s[0] for s in stats.values())
        total_nodata = sum(s[1] for s in stats.values())
        total_fail = sum(s[2] for s in stats.values())
        print(f"\n{'='*60}")
        print(f"全部 worker 結束 (運行 {elapsed:.0f} 秒, 瀏覽器重啟 {browser_restarts} 次)")
        print(f"  ✓ 成功: {total_ok}")
        print(f"  ○ 空值: {total_nodata}")
        print(f"  ✗ 失敗: {total_fail}")
        print(f"  總計: {total_ok + total_nodata + total_fail}")
        print(f"{'='*60}")

    finally:
        await relay_session.close()
        print("✓ 已清理完畢")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("用法: python tpex_remote_token_worker.py <RELAY_URL> [NUM_WORKERS]")
        print("範例: python tpex_remote_token_worker.py https://xxxx.ngrok-free.app 5")
        print()
        print("步驟:")
        print("  1. 在本地電腦啟動主程式 (會自動開啟 port 9999 的 relay 伺服器)")
        print("  2. 在本地電腦執行: ngrok http 9999")
        print("  3. 複製 ngrok 給的 URL (如 https://xxxx.ngrok-free.app)")
        print("  4. 在 GitHub Actions 執行此腳本, 傳入 ngrok URL")
        sys.exit(1)

    relay_url = sys.argv[1].rstrip('/')
    num_workers = int(sys.argv[2]) if len(sys.argv) > 2 else 3

    asyncio.run(main(relay_url, num_workers))
