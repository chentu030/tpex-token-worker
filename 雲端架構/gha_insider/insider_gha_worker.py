"""
內部人持股異動事後申報 ── GitHub Actions 分片 worker（自給自足，只用 requests + 標準函式庫）
==========================================================================================
在每個 GitHub Actions runner 上跑一片公司清單（runner 自己的 IP、直連、不用代理），
結果寫成 JSONL + 進度 JSON，由 workflow 上傳成 artifact，VM B 的 merge_results.py 合併。

解析邏輯逐行照抄 董監持股與設質爬蟲\\insider_changes_crawler.py 的 _fetch_once()
（不 import 原程式：GitHub repo 只放這支檔案，runner 上沒有 StockDB、proxy_pool、pandas）。
原程式若修改解析規則，這裡要同步（verify_against_existing() 測試會抓到差異）。

每家公司 = 1 次 POST（直連一次就算數，和原程式 --direct 相同；代理模式才需要「連抓兩次相同」驗貨）。

禮貌與退避
  ‧每次請求間隔 --min-delay～--max-delay 秒（預設 1.5～3 秒，隨機）
  ‧回應含「查詢過於頻繁」/ HTTP 429：退避 30→60→120→240 秒後重試同一家
  ‧回應是安全機制頁（FOR SECURITY REASONS / 安全性考量）或 HTTP 403：退避 120 秒再試一次，
    還是擋 → 整片停止（blocked=true），剩下的公司不記錄，下一輪/VM B 本機補
  ‧連線錯誤/逾時/5xx：同一家最多 3 次（10、20、40 秒）；仍失敗 → 記「失敗」（不算完成）
  ‧--max-minutes 到點就收工（比 workflow timeout 早），保證進度檔與 artifact 一定寫得出來

輸出（--out 目錄）
  rows.jsonl      每位內部人一列（欄位同原程式 COLS，市場欄留空，由合併時補）
  progress.json   {代號: {"狀態": "ok"|"查無資料"|"失敗", "日期": "YYYY-MM-DD", "時間": "HH:MM:SS", "次數": n}}
  summary.json    片號、IP、筆數、blocked、耗時

用法
  python insider_gha_worker.py --month 202609 --codes 2330,1101 --out out/shard_00
  python insider_gha_worker.py --month 202609 --codes-file todo.txt --shard 3 --shards 20 --out out/shard_03
"""
import argparse, html, json, os, random, re, sys, time
from datetime import datetime, timedelta, timezone

import requests

URL = "https://mopsov.twse.com.tw/mops/web/ajax_query6_1"
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                         "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"}
TW = timezone(timedelta(hours=8))          # 時間一律台灣時間（runner 是 UTC）
NUMS = ["選任持股", "上月持股", "上月底信託", "上月底累計設質", "上月底私募",
        "增加_集中", "增加_其他", "增加_私募", "增加_信託", "設定質權",
        "減少_集中", "減少_其他", "減少_私募", "減少_信託", "解除質權",
        "本月持股", "本月底分戶集保", "本月底信託", "本月底累計設質", "本月底私募"]
COLS = ["年月", "代號", "名稱", "市場", "身份別", "姓名", "本人", "持股種類"] + NUMS + ["身份別備註", "來源"]
_ROW = re.compile(r"<tr class='(?:odd|even)'>(.*?)</tr>", re.S | re.I)
_TD = re.compile(r"<td[^>]*>(.*?)</td>", re.S | re.I)
_TOO_FAST = ("查詢過於頻繁", "過於頻繁", "too many requests")
FREQ_BACKOFF = [30, 60, 120, 240]
NET_BACKOFF = [10, 20, 40]


def now_tw():
    return datetime.now(TW)


def to_int(x):
    s = str(x or "").replace(",", "").strip()
    try:
        return int(float(s))
    except ValueError:
        return None


def roc_ym(s):
    s = str(s).strip()
    return (int(s[:-2]) + 1911) * 100 + int(s[-2:])


def is_block(text):
    return "FOR SECURITY REASONS" in text or "安全性考量" in text


def is_too_fast(text):
    t = text.lower()
    return any(k.lower() in t for k in _TOO_FAST)


def parse(s, code, ym):
    """回應 HTML → rows（list）；[] = 查無資料 / 資料年月不符。與 insider_changes_crawler._fetch_once 相同。"""
    if "查無資料" in s:
        return []
    mym = re.search(r"資料年月[:：](\d{4,5})", s)
    if not mym or roc_ym(mym.group(1)) != ym:
        return []
    mn = re.search(r"\n([^\n<]+?)　公司提供", s)
    name = mn.group(1).strip() if mn else ""
    rows, owner = [], ""
    for tr in _ROW.findall(s):
        tds = _TD.findall(tr)
        if len(tds) < 7:
            continue
        txt = lambda x: html.unescape(re.sub(r"<[^>]+>", "", x)).strip()
        ident, person, kind = txt(tds[0]), txt(tds[1]), txt(tds[2])
        nums, remark = [], ""
        for cell in tds[3:7]:
            parts = re.split(r"<br\s*/?>", cell, flags=re.I)
            for p in parts:
                t = txt(p)
                if t.startswith("身份別備註"):
                    remark = re.sub(r"\s+", " ", t.replace("身份別備註", "").lstrip("：: ")).strip()
                    break
                if t != "":
                    nums.append(to_int(t))
        if len(nums) < 20:
            continue
        self_row = "配偶" not in ident and "子女" not in ident
        if self_row and person:
            owner = person
        rows.append(dict(zip(NUMS, nums[:20]), 年月=ym, 代號=code, 名稱=name, 市場="", 身份別=ident,
                         姓名=person or owner, 本人=self_row, 持股種類=kind, 身份別備註=remark, 來源="MOPS"))
    return rows


class Client:
    def __init__(self, min_delay, max_delay, timeout=60):
        self.s = requests.Session()
        self.s.headers.update(HEADERS)
        self.min_delay, self.max_delay, self.timeout = min_delay, max_delay, timeout
        self.last = 0.0
        self.n_req = 0
        self.n_backoff = 0

    def _pace(self):
        wait = random.uniform(self.min_delay, self.max_delay) - (time.time() - self.last)
        if wait > 0:
            time.sleep(wait)

    def post(self, data):
        self._pace()
        try:
            r = self.s.post(URL, data=data, timeout=self.timeout)
            return r.status_code, r.content.decode("utf-8", "replace")
        except requests.RequestException as e:
            return -1, f"{type(e).__name__}: {e}"
        finally:
            self.last = time.time()
            self.n_req += 1

    def fetch(self, code, ym, log=print):
        """回傳 (狀態, rows, 次數)；狀態 = ok / 查無資料 / 失敗 / blocked"""
        y, m = ym // 100 - 1911, ym % 100
        data = {"encodeURIComponent": "1", "step": "1", "firstin": "1", "off": "1",
                "queryName": "co_id", "inpuType": "co_id", "TYPEK": "all", "isnew": "false",
                "co_id": code, "year": str(y), "month": f"{m:02d}"}
        tries, freq_i, net_i, blocked_once = 0, 0, 0, False
        while True:
            tries += 1
            st, s = self.post(data)
            if st == 200 and not is_block(s) and not is_too_fast(s) and "公開資訊觀測站" in s:
                rows = parse(s, code, ym)
                return ("ok" if rows else "查無資料"), rows, tries
            if st == 429 or (st == 200 and is_too_fast(s)):
                if freq_i >= len(FREQ_BACKOFF):
                    return "失敗", [], tries
                w = FREQ_BACKOFF[freq_i]; freq_i += 1; self.n_backoff += 1
                log(f"  [退避] {code} 查詢過於頻繁，等 {w} 秒")
                time.sleep(w)
                continue
            if st == 403 or (st == 200 and is_block(s)):
                if blocked_once:
                    return "blocked", [], tries
                blocked_once = True; self.n_backoff += 1
                log(f"  [退避] {code} 被安全機制擋（HTTP {st}），等 120 秒再試一次")
                time.sleep(120)
                continue
            # 連線錯誤 / 逾時 / 5xx / 內容不像 MOPS
            if net_i >= len(NET_BACKOFF):
                log(f"  [失敗] {code} HTTP {st} {s[:80]!r}")
                return "失敗", [], tries
            w = NET_BACKOFF[net_i]; net_i += 1
            log(f"  [重試] {code} HTTP {st}，{w} 秒後重試")
            time.sleep(w)


def runner_ip():
    try:
        return requests.get("https://api.ipify.org", timeout=10).text.strip()
    except Exception:
        return ""


def write_json(obj, path):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=1)
    os.replace(tmp, path)


def select_codes(a):
    if a.codes:
        codes = [c.strip() for c in re.split(r"[,\s]+", a.codes) if c.strip()]
    elif a.codes_file:
        codes = [c.strip() for c in re.split(r"[,\s]+", open(a.codes_file, encoding="utf-8").read()) if c.strip()]
    else:
        codes = []
    codes = [c for c in codes if re.fullmatch(r"[0-9A-Z]{4,6}", c)]
    if a.shards > 1:                       # 交錯分片：每片都拿到大/小公司、上市/上櫃混合，負載較平均
        codes = codes[a.shard::a.shards]
    return codes


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--month", type=int, required=True, help="資料年月 YYYYMM（西元）")
    ap.add_argument("--codes", help="逗號或空白分隔的公司代號")
    ap.add_argument("--codes-file")
    ap.add_argument("--shard", type=int, default=0)
    ap.add_argument("--shards", type=int, default=1)
    ap.add_argument("--out", required=True)
    ap.add_argument("--min-delay", type=float, default=1.5)
    ap.add_argument("--max-delay", type=float, default=3.0)
    ap.add_argument("--max-minutes", type=float, default=25)
    ap.add_argument("--no-ip", action="store_true", help="不查 runner IP（本機測試用）")
    a = ap.parse_args()

    os.makedirs(a.out, exist_ok=True)
    codes = select_codes(a)
    t0 = time.time()
    ip = "" if a.no_ip else runner_ip()
    print(f"[分片 {a.shard}/{a.shards}] 月份 {a.month}，{len(codes)} 家，IP {ip or '-'}", flush=True)
    cli = Client(a.min_delay, a.max_delay)
    prog, n_rows, blocked, stopped = {}, 0, False, False
    rows_path = os.path.join(a.out, "rows.jsonl")
    with open(rows_path, "w", encoding="utf-8") as fo:
        for i, code in enumerate(codes, 1):
            if (time.time() - t0) / 60 > a.max_minutes:
                print(f"  [收工] 已到 {a.max_minutes} 分鐘上限，剩 {len(codes) - i + 1} 家留給下一輪", flush=True)
                stopped = True
                break
            st, rows, tries = cli.fetch(code, a.month, log=lambda s: print(s, flush=True))
            if st == "blocked":
                print(f"  [停止] MOPS 擋下本 runner IP（{ip}），剩 {len(codes) - i + 1} 家留給下一輪", flush=True)
                blocked = True
                break
            t = now_tw()
            prog[code] = {"狀態": st, "日期": t.date().isoformat(), "時間": t.strftime("%H:%M:%S"), "次數": tries}
            for r in rows:
                fo.write(json.dumps(r, ensure_ascii=False) + "\n")
            n_rows += len(rows)
            fo.flush()
            if i % 20 == 0:
                write_json(prog, os.path.join(a.out, "progress.json"))
                ok = sum(1 for v in prog.values() if v["狀態"] == "ok")
                print(f"  {i}/{len(codes)}，有資料 {ok} 家，{n_rows} 列，{(time.time() - t0) / 60:.1f} 分", flush=True)
    write_json(prog, os.path.join(a.out, "progress.json"))
    cnt = {k: sum(1 for v in prog.values() if v["狀態"] == k) for k in ("ok", "查無資料", "失敗")}
    summ = {"shard": a.shard, "shards": a.shards, "month": a.month, "ip": ip, "codes": len(codes),
            "done": len(prog), **cnt, "rows": n_rows, "requests": cli.n_req, "backoffs": cli.n_backoff,
            "blocked": blocked, "stopped_by_time": stopped, "minutes": round((time.time() - t0) / 60, 2),
            "finished_at": now_tw().isoformat(timespec="seconds")}
    write_json(summ, os.path.join(a.out, "summary.json"))
    print(json.dumps(summ, ensure_ascii=False), flush=True)
    # 被擋也回 0：讓 artifact 一定上傳；是否完整由 VM B 看 summary/progress 決定
    return 0


if __name__ == "__main__":
    sys.exit(main())
