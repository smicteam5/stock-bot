import os
import asyncio
import aiohttp
import feedparser
from datetime import datetime, time
import pytz
from telegram import Bot, Update
from telegram.ext import Application, CommandHandler, ContextTypes
from telegram.error import TelegramError
from apscheduler.schedulers.asyncio import AsyncIOScheduler

# ─── 환경변수에서 키 불러오기 ───────────────────────────────────────────
TELEGRAM_TOKEN   = os.environ["TELEGRAM_TOKEN"]
TELEGRAM_CHAT_ID = os.environ["TELEGRAM_CHAT_ID"]
DART_API_KEY     = os.environ["DART_API_KEY"]

KST = pytz.timezone("Asia/Seoul")
bot = Bot(token=TELEGRAM_TOKEN)

# ════════════════════════════════════════════════════════════════════════
# 1) 오전 7:30 – 미국 시장 브리핑
# ════════════════════════════════════════════════════════════════════════

NEWS_FEEDS_GLOBAL = {
    "WSJ 마켓":     "https://feeds.a.dj.com/rss/RSSMarketsMain.xml",
    "Reuters 비즈": "https://feeds.reuters.com/reuters/businessNews",
    "Bloomberg":   "https://feeds.bloomberg.com/markets/news.rss",
}

NEWS_FEEDS_KOREA = {
    "한국경제":      "https://www.hankyung.com/feed/industry",
    "매일경제":      "https://www.mk.co.kr/rss/30000001/",
    "연합뉴스 산업": "https://www.yna.co.kr/rss/industry.xml",
}

SECTORS = {
    "기술 (XLK)":        "XLK",
    "에너지 (XLE)":      "XLE",
    "금융 (XLF)":        "XLF",
    "헬스케어 (XLV)":    "XLV",
    "반도체 (SOXX)":     "SOXX",
    "소비재 (XLY)":      "XLY",
    "유틸리티 (XLU)":    "XLU",
}

async def fetch_yahoo_quote(session, symbol: str) -> dict:
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?interval=1d&range=2d"
    headers = {"User-Agent": "Mozilla/5.0"}
    async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=10)) as r:
        data = await r.json()
    meta   = data["chart"]["result"][0]["meta"]
    price  = meta["regularMarketPrice"]
    prev   = meta["chartPreviousClose"]
    chg    = ((price - prev) / prev) * 100
    return {"symbol": symbol, "price": price, "change": chg}

async def fetch_fear_greed(session) -> str:
    urls = [
        "https://production.dataviz.cnn.io/index/fearandgreed/graphdata",
        "https://fear-and-greed-index.p.rapidapi.com/v1/fgi",
    ]
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    }
    for url in urls:
        try:
            async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=10)) as r:
                data = await r.json(content_type=None)
            if "fear_and_greed" in data:
                score  = float(data["fear_and_greed"]["score"])
                rating = data["fear_and_greed"]["rating"]
            elif "fgi" in data:
                score  = float(data["fgi"]["now"]["value"])
                rating = data["fgi"]["now"]["valueText"]
            else:
                continue
            rating_kr = {
                "Extreme Fear": "극도의 공포",
                "Fear":         "공포",
                "Neutral":      "중립",
                "Greed":        "탐욕",
                "Extreme Greed":"극도의 탐욕",
            }.get(rating, rating)
            return f"{score:.1f} / 100  ({rating_kr})"
        except Exception as e:
            print(f"Fear&Greed 요청 실패 ({url}): {e}")
            continue
    return "데이터 조회 실패"

async def fetch_news_headlines(feeds: dict, count: int = 3) -> list:
    headlines = []
    for source, url in feeds.items():
        try:
            feed = feedparser.parse(url)
            if feed.entries:
                entry = feed.entries[0]
                title = entry.get("title", "").strip()
                link  = entry.get("link", "")
                if title:
                    headlines.append(f"  • [{source}] [{title}]({link})")
        except Exception as e:
            print(f"뉴스 피드 오류 ({source}): {e}")
    return headlines[:count]

def arrow(chg: float) -> str:
    return "🔴▼" if chg < 0 else "🟢▲"

async def send_morning_briefing():
    async with aiohttp.ClientSession() as session:
        nasdaq = await fetch_yahoo_quote(session, "^IXIC")
        snp    = await fetch_yahoo_quote(session, "^GSPC")
        fg = await fetch_fear_greed(session)
        sector_lines = []
        for name, sym in SECTORS.items():
            q = await fetch_yahoo_quote(session, sym)
            sector_lines.append(f"  {arrow(q['change'])} {name}: {q['change']:+.2f}%")

    news_lines = await fetch_news_headlines(NEWS_FEEDS_GLOBAL, count=3)
    if not news_lines:
        news_lines = ["  뉴스를 불러오지 못했어요"]

    lines = [
        "━━━━━━━━━━━━━━━━━━━━━━━━",
        "📊 *미국 시장 모닝 브리핑*",
        f"🕐 {datetime.now(KST).strftime('%Y-%m-%d %H:%M')} KST",
        "━━━━━━━━━━━━━━━━━━━━━━━━",
        "",
        "*📈 주요 지수*",
        f"  {arrow(nasdaq['change'])} 나스닥:  {nasdaq['price']:,.2f}  ({nasdaq['change']:+.2f}%)",
        f"  {arrow(snp['change'])} S&P 500: {snp['price']:,.2f}  ({snp['change']:+.2f}%)",
        "",
        "*😨 Fear & Greed 지수*",
        f"  {fg}",
        "",
        "*🏭 섹터별 변동률*",
    ] + sector_lines + [
        "",
        "*📰 주요 뉴스 헤드라인*",
    ] + news_lines + ["━━━━━━━━━━━━━━━━━━━━━━━━"]

    await bot.send_message(
        chat_id=TELEGRAM_CHAT_ID,
        text="\n".join(lines),
        parse_mode="Markdown"
    )
    print(f"[{datetime.now(KST)}] 모닝 브리핑 전송 완료")


# ════════════════════════════════════════════════════════════════════════
# 2) 뉴스 헤드라인 별도 알림 (3시간마다)
# ════════════════════════════════════════════════════════════════════════

async def send_news_alert():
    global_lines = await fetch_news_headlines(NEWS_FEEDS_GLOBAL, count=3)
    korea_lines  = await fetch_news_headlines(NEWS_FEEDS_KOREA, count=3)

    if not global_lines and not korea_lines:
        return

    now_str = datetime.now(KST).strftime("%Y-%m-%d %H:%M")
    lines = [
        "━━━━━━━━━━━━━━━━━━━━━━━━",
        "📰 *주요 뉴스 헤드라인*",
        f"🕐 {now_str} KST",
        "━━━━━━━━━━━━━━━━━━━━━━━━",
        "",
        "*🌐 해외 뉴스*",
    ] + (global_lines if global_lines else ["  뉴스를 불러오지 못했어요"]) + [
        "",
        "*🇰🇷 국내 산업 뉴스*",
    ] + (korea_lines if korea_lines else ["  뉴스를 불러오지 못했어요"]) + [
        "━━━━━━━━━━━━━━━━━━━━━━━━"
    ]

    await bot.send_message(
        chat_id=TELEGRAM_CHAT_ID,
        text="\n".join(lines),
        parse_mode="Markdown",
        disable_web_page_preview=True
    )
    print(f"[{datetime.now(KST)}] 뉴스 알림 전송 완료")


# ════════════════════════════════════════════════════════════════════════
# 3) DART – 주요 기관 지분 변동 실시간 감시
# ════════════════════════════════════════════════════════════════════════

WATCH_INSTITUTIONS = [
    "국민연금",
    "삼성자산운용",
    "미래에셋자산운용",
    "한국투자",
    "KB자산운용",
    "신한자산운용",
]

seen_dart_ids: set = set()

async def fetch_dart_major_holdings(session) -> list:
    url = "https://opendart.fss.or.kr/api/majorstock.json"
    params = {
        "crtfc_key": DART_API_KEY,
        "page_no":   "1",
        "page_count":"40",
    }
    async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=15)) as r:
        data = await r.json(content_type=None)
    if data.get("status") != "000":
        return []
    return data.get("list", [])

async def check_dart():
    global seen_dart_ids
    async with aiohttp.ClientSession() as session:
        items = await fetch_dart_major_holdings(session)

    for item in items:
        doc_id    = item.get("rcept_no", "")
        reporter  = item.get("repror_nm", "")
        corp_name = item.get("corp_name", "")
        stock_type= item.get("stkqy_irds_nm", "")
        hold_ratio= item.get("stkqy_irds_rate", "")
        rcept_dt  = item.get("rcept_dt", "")

        if doc_id in seen_dart_ids:
            continue
        if not any(kw in reporter for kw in WATCH_INSTITUTIONS):
            continue

        seen_dart_ids.add(doc_id)

        msg = (
            "━━━━━━━━━━━━━━━━━━━━━━━━\n"
            "🏦 *DART 기관 지분 변동 알림*\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"📌 보고 기관: *{reporter}*\n"
            f"🏢 대상 종목: {corp_name}\n"
            f"📊 주식 종류: {stock_type}\n"
            f"📉 보유 변동: {hold_ratio}%\n"
            f"📅 접수일: {rcept_dt}\n"
            f"🔗 [공시 보기](https://dart.fss.or.kr/dsaf001/main.do?rcpNo={doc_id})\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━"
        )
        try:
            await bot.send_message(
                chat_id=TELEGRAM_CHAT_ID,
                text=msg,
                parse_mode="Markdown",
                disable_web_page_preview=True
            )
            print(f"[{datetime.now(KST)}] DART 알림 전송: {reporter} / {corp_name}")
        except TelegramError as e:
            print(f"DART 전송 오류: {e}")


# ════════════════════════════════════════════════════════════════════════
# 4) KITA – 한국 메모리 반도체 수출 데이터
# ════════════════════════════════════════════════════════════════════════

last_kita_data: dict = {}

async def fetch_kita_memory_export(session) -> dict | None:
    url = "https://www.kita.net/openApi/tradeStats/getExportStats.do"
    params = {
        "serviceKey": "KITA_FREE",
        "hsSgn":      "854232",
        "statYymm":   datetime.now(KST).strftime("%Y%m"),
        "natCd":      "000",
        "type":       "json",
    }
    try:
        async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=15)) as r:
            data = await r.json(content_type=None)
        return data
    except Exception as e:
        print(f"KITA 요청 오류: {e}")
        return None

async def check_kita():
    global last_kita_data
    async with aiohttp.ClientSession() as session:
        data = await fetch_kita_memory_export(session)

    if not data:
        return

    key = str(data)
    if key == str(last_kita_data):
        return
    last_kita_data = data

    try:
        items = data.get("data") or data.get("list") or []
        if not items:
            return
        latest = items[0]

        export_amt   = latest.get("expAmt",   latest.get("expDlr",  "N/A"))
        export_qty   = latest.get("expQty",   latest.get("expKg",   "N/A"))
        unit_price   = latest.get("unitPrice","N/A")
        period       = latest.get("statYymm", datetime.now(KST).strftime("%Y%m"))

        msg = (
            "━━━━━━━━━━━━━━━━━━━━━━━━\n"
            "🇰🇷 *KITA 메모리 반도체 수출 업데이트*\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"📅 기준월: {period[:4]}년 {period[4:]}월\n"
            f"💵 수출 금액: {float(export_amt):,.0f} 천달러\n"
            f"📦 수출 중량: {float(export_qty):,.0f} kg\n"
            f"💲 수출 단가: {unit_price}\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━"
        )
        await bot.send_message(
            chat_id=TELEGRAM_CHAT_ID,
            text=msg,
            parse_mode="Markdown"
        )
        print(f"[{datetime.now(KST)}] KITA 알림 전송 완료")
    except Exception as e:
        print(f"KITA 파싱 오류: {e}")


# ════════════════════════════════════════════════════════════════════════
# 5) 기관/외국인 시총 대비 순매수 수급 알림 (13:00, 15:40)
# ════════════════════════════════════════════════════════════════════════

async def fetch_investor_flow(session, investor: str, trade: str, top_n: int = 7) -> list:
    import re
    from datetime import date

    today = date.today().strftime("%Y%m%d")
    krx_url = "https://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd"
    form = {
        "bld": "dbms/MDC/STAT/standard/MDCSTAT02203",
        "locale": "ko_KR",
        "trdDd": today,
        "money": "1",
        "idxIndMidclssCd": "00",
        "sortParamColumn": "NETBID_TRDVAL",
        "sortType": "DESC" if trade == "buy" else "ASC",
        "askBid": "0",
        "codeNmSearchText": "",
        "page": "1",
        "pageSize": "30",
    }
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Referer": "https://data.krx.co.kr/",
        "Content-Type": "application/x-www-form-urlencoded",
    }

    results = []
    try:
        async with session.post(krx_url, data=form, headers=headers,
                                timeout=aiohttp.ClientTimeout(total=15)) as r:
            data = await r.json(content_type=None)

        items = data.get("output", [])
        for item in items:
            name   = item.get("ISU_ABBRV", "")
            mktcap = float(item.get("MKTCAP", 0) or 0)
            if investor == "foreign":
                net_val = float(item.get("FRGN_NETBID_TRDVAL", 0) or 0)
            else:
                net_val = float(item.get("INST_NETBID_TRDVAL", 0) or 0)

            if mktcap <= 0:
                continue
            ratio = (net_val / mktcap) * 100
            if trade == "buy" and net_val <= 0:
                continue
            if trade == "sell" and net_val >= 0:
                continue

            change_rate = item.get("CMPPREVDD_PRC", "")
            isin = item.get("ISU_CD", item.get("MKT_ID", ""))
            results.append({
                "name": name, "ratio": ratio,
                "net": net_val, "change": change_rate, "isin": isin,
            })

        results.sort(key=lambda x: abs(x["ratio"]), reverse=True)
        top = results[:top_n]
        for item_r in top:
            isin_code = item_r.get("isin", "")
            if isin_code:
                item_r["sector"] = await fetch_sector(session, isin_code)
            else:
                item_r["sector"] = ""
        return top

    except Exception as e:
        print(f"KRX 수급 오류 ({investor}/{trade}): {e}")
    return results[:top_n]

SECTOR_CACHE: dict = {}

async def fetch_sector(session, isin_code: str) -> str:
    if isin_code in SECTOR_CACHE:
        return SECTOR_CACHE[isin_code]
    try:
        url = "https://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd"
        form = {
            "bld": "dbms/MDC/STAT/standard/MDCSTAT03901",
            "locale": "ko_KR",
            "isuCd": isin_code,
            "isuCd2": "",
            "codeNmSearchText": "",
            "pageSize": "1",
            "page": "1",
        }
        headers = {
            "User-Agent": "Mozilla/5.0",
            "Referer": "https://data.krx.co.kr/",
            "Content-Type": "application/x-www-form-urlencoded",
        }
        async with session.post(url, data=form, headers=headers,
                                timeout=aiohttp.ClientTimeout(total=8)) as r:
            data = await r.json(content_type=None)
        items = data.get("output", [])
        if items:
            sector = items[0].get("IDX_IND_NM", "") or items[0].get("SECT_TP_NM", "")
            if sector:
                SECTOR_CACHE[isin_code] = sector
                return sector
    except Exception:
        pass
    try:
        import re
        naver_url = f"https://finance.naver.com/item/main.naver?code={isin_code[-6:]}"
        headers2 = {"User-Agent": "Mozilla/5.0", "Accept-Language": "ko-KR"}
        async with session.get(naver_url, headers=headers2,
                               timeout=aiohttp.ClientTimeout(total=8)) as r:
            html = await r.text(encoding="euc-kr", errors="replace")
        m = re.search(r"업종</th>.*?<td[^>]*>(.*?)</td>", html, re.DOTALL)
        if m:
            sector = re.sub(r"<[^>]+>", "", m.group(1)).strip()
            SECTOR_CACHE[isin_code] = sector
            return sector
    except Exception:
        pass
    return "기타"

def format_flow_lines(items: list, trade: str) -> list:
    lines = []
    emoji = "🟢" if trade == "buy" else "🔴"
    for i, s in enumerate(items, 1):
        ratio_str  = f"{s['ratio']:+.3f}%"
        net_str    = f"{s['net']:+,.0f}백만원"
        chg        = s.get("change", "")
        chg_str    = f" | 등락 {chg}" if chg else ""
        sector     = s.get("sector", "")
        sector_str = f" ({sector})" if sector else ""
        lines.append(f"  {emoji} {i}. *{s['name']}*{sector_str} 시총대비 {ratio_str} ({net_str}{chg_str})")
    return lines if lines else ["  데이터 없음"]

async def send_supply_demand_alert(label: str):
    async with aiohttp.ClientSession() as session:
        inst_buy  = await fetch_investor_flow(session, "institution", "buy",  top_n=10)
        inst_sell = await fetch_investor_flow(session, "institution", "sell", top_n=10)
        frgn_buy  = await fetch_investor_flow(session, "foreign",     "buy",  top_n=10)
        frgn_sell = await fetch_investor_flow(session, "foreign",     "sell", top_n=10)

    now_str = datetime.now(KST).strftime("%Y-%m-%d %H:%M")
    lines = [
        "━━━━━━━━━━━━━━━━━━━━━━━━",
        f"💹 *기관/외국인 수급 ({label})*",
        f"🕐 {now_str} KST  |  시총 대비 순매수 비율 기준",
        "━━━━━━━━━━━━━━━━━━━━━━━━",
        "",
        "*🏦 기관 순매수 상위*",
    ] + format_flow_lines(inst_buy, "buy") + [
        "",
        "*🏦 기관 순매도 상위*",
    ] + format_flow_lines(inst_sell, "sell") + [
        "",
        "*🌐 외국인 순매수 상위*",
    ] + format_flow_lines(frgn_buy, "buy") + [
        "",
        "*🌐 외국인 순매도 상위*",
    ] + format_flow_lines(frgn_sell, "sell") + [
        "━━━━━━━━━━━━━━━━━━━━━━━━"
    ]

    await bot.send_message(
        chat_id=TELEGRAM_CHAT_ID,
        text="\n".join(lines),
        parse_mode="Markdown",
        disable_web_page_preview=True
    )
    print(f"[{datetime.now(KST)}] 수급 알림 전송 완료 ({label})")

async def send_supply_demand_midday():
    await send_supply_demand_alert("장중 잠정")

async def send_supply_demand_close():
    await send_supply_demand_alert("장 마감")


# ════════════════════════════════════════════════════════════════════════
# 6) 52주 신고가 / 신저가 알림
# ════════════════════════════════════════════════════════════════════════

async def fetch_52week_stocks(session, mode: str) -> list:
    import re
    if mode == "high":
        url = "https://finance.naver.com/sise/sise_high.naver"
    else:
        url = "https://finance.naver.com/sise/sise_low.naver"

    headers = {"User-Agent": "Mozilla/5.0", "Accept-Language": "ko-KR,ko;q=0.9"}
    results = []
    try:
        async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=15)) as r:
            html = await r.text(encoding="euc-kr", errors="replace")

        name_pattern = re.compile(r'sise_item\.naver\?code=(\w+)[^>]*>([\w\s가-힣·&;]+)</a>')
        rate_pattern  = re.compile(r'([\+\-]?\d+\.\d+)%')
        names = name_pattern.findall(html)
        rates = rate_pattern.findall(html)

        for i, (code, name) in enumerate(names[:10]):
            name = name.strip()
            rate = rates[i] if i < len(rates) else "N/A"
            if name:
                results.append({"code": code, "name": name, "rate": rate})
    except Exception as e:
        print(f"52주 스크래핑 오류: {e}")
    return results[:10]

async def fetch_stock_news(session, stock_name: str) -> str:
    try:
        url = f"https://m.stock.naver.com/api/json/search/searchNews.nhn?query={stock_name}&pageSize=1"
        headers = {"User-Agent": "Mozilla/5.0"}
        async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=8)) as r:
            data = await r.json(content_type=None)
        items = data.get("result", {}).get("newsList", [])
        if items:
            return items[0].get("title", "").strip()
    except Exception:
        pass
    return ""

async def send_52week_alert():
    async with aiohttp.ClientSession() as session:
        highs = await fetch_52week_stocks(session, "high")
        lows  = await fetch_52week_stocks(session, "low")

        high_lines = []
        for s in highs[:7]:
            news = await fetch_stock_news(session, s["name"])
            line = f"  📌 *{s['name']}* ({s['rate']}%)"
            if news:
                line += f"\n      📰 {news[:40]}..."
            high_lines.append(line)

        low_lines = []
        for s in lows[:7]:
            news = await fetch_stock_news(session, s["name"])
            line = f"  📌 *{s['name']}* ({s['rate']}%)"
            if news:
                line += f"\n      📰 {news[:40]}..."
            low_lines.append(line)

    now_str = datetime.now(KST).strftime("%Y-%m-%d")
    lines = [
        "━━━━━━━━━━━━━━━━━━━━━━━━",
        "📊 *52주 신고가 / 신저가*",
        f"📅 {now_str} 장 마감",
        "━━━━━━━━━━━━━━━━━━━━━━━━",
        "",
        "*🚀 52주 신고가 종목*",
    ] + (high_lines if high_lines else ["  데이터 없음"]) + [
        "",
        "*📉 52주 신저가 종목*",
    ] + (low_lines if low_lines else ["  데이터 없음"]) + [
        "━━━━━━━━━━━━━━━━━━━━━━━━"
    ]

    await bot.send_message(
        chat_id=TELEGRAM_CHAT_ID,
        text="\n".join(lines),
        parse_mode="Markdown",
        disable_web_page_preview=True
    )
    print(f"[{datetime.now(KST)}] 52주 신고가/신저가 알림 전송 완료")


# ════════════════════════════════════════════════════════════════════════
# 7) 기관/외국인 수급 알림 (15:45)
# ════════════════════════════════════════════════════════════════════════

async def fetch_investor_trading(session, investor: str, trade_type: str) -> list:
    import re
    if investor == "institution":
        ftype = "1"
    else:
        ftype = "2"

    if trade_type == "buy":
        url = f"https://finance.naver.com/fund/sise_by_investor.naver?bizdate=&sosok=0&ftype={ftype}&order=1"
    else:
        url = f"https://finance.naver.com/fund/sise_by_investor.naver?bizdate=&sosok=0&ftype={ftype}&order=2"

    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept-Language": "ko-KR,ko;q=0.9",
        "Referer": "https://finance.naver.com",
    }
    results = []
    try:
        async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=15)) as r:
            html = await r.text(encoding="euc-kr", errors="replace")

        name_pattern   = re.compile(r'itemDetail\.naver\?code=\w+[^>]*>([\w\s가-힣·&;]+)</a>')
        amount_pattern = re.compile(r'<td[^>]*class="[^"]*num[^"]*"[^>]*>([\-\d,]+)</td>')
        names   = name_pattern.findall(html)
        amounts = amount_pattern.findall(html)

        for i, name in enumerate(names[:7]):
            name = name.strip()
            amt  = amounts[i].replace(",", "").strip() if i < len(amounts) else "N/A"
            try:
                amt_int = int(amt)
                amt_str = f"{amt_int:+,}백만원"
            except ValueError:
                amt_str = amt
            if name:
                results.append({"name": name, "amount": amt_str})
    except Exception as e:
        print(f"수급 스크래핑 오류 ({investor}/{trade_type}): {e}")
    return results[:7]

async def send_investor_flow_alert():
    async with aiohttp.ClientSession() as session:
        inst_buy  = await fetch_investor_trading(session, "institution", "buy")
        inst_sell = await fetch_investor_trading(session, "institution", "sell")
        for_buy   = await fetch_investor_trading(session, "foreign", "buy")
        for_sell  = await fetch_investor_trading(session, "foreign", "sell")

    def fmt_list(items, emoji):
        if not items:
            return ["  데이터 없음"]
        return [f"  {emoji} *{s['name']}* {s['amount']}" for s in items]

    now_str = datetime.now(KST).strftime("%Y-%m-%d %H:%M")
    lines = [
        "━━━━━━━━━━━━━━━━━━━━━━━━",
        "💰 *기관/외국인 수급 (잠정)*",
        f"📅 {now_str} 기준",
        "━━━━━━━━━━━━━━━━━━━━━━━━",
        "",
        "*🏦 기관 순매수 상위*",
    ] + fmt_list(inst_buy, "✅") + [
        "",
        "*🏦 기관 순매도 상위*",
    ] + fmt_list(inst_sell, "🔻") + [
        "",
        "*🌏 외국인 순매수 상위*",
    ] + fmt_list(for_buy, "✅") + [
        "",
        "*🌏 외국인 순매도 상위*",
    ] + fmt_list(for_sell, "🔻") + [
        "━━━━━━━━━━━━━━━━━━━━━━━━"
    ]

    await bot.send_message(
        chat_id=TELEGRAM_CHAT_ID,
        text="\n".join(lines),
        parse_mode="Markdown",
        disable_web_page_preview=True
    )
    print(f"[{datetime.now(KST)}] 수급 알림 전송 완료")


# ════════════════════════════════════════════════════════════════════════
# ★ 8) 코스피/코스닥 수급 누적 테이블 (신규 추가)
#    - 매일 18:00 자동 전송
#    - /수급누적 명령어로 즉시 조회
# ════════════════════════════════════════════════════════════════════════

import pandas as pd
try:
    from pykrx import stock as krx_stock
    PYKRX_OK = True
except ImportError:
    PYKRX_OK = False
    print("⚠️  pykrx 미설치 — 수급 누적 기능 비활성화 (pip install pykrx)")

SUPPLY_START_YEAR = 2020   # 연간 집계 시작 연도

def _fetch_supply_df(market: str) -> pd.DataFrame:
    """pykrx로 지수 + 투자자별 순매수 수집"""
    today = datetime.now(KST).strftime("%Y%m%d")
    start = f"{SUPPLY_START_YEAR}0101"
    code  = "1001" if market == "KOSPI" else "2001"

    idx = krx_stock.get_index_ohlcv(start, today, code)[["종가"]]
    idx.index = pd.to_datetime(idx.index)
    idx.rename(columns={"종가": "지수"}, inplace=True)

    raw = krx_stock.get_market_trading_value_by_date(start, today, market, detail=True)
    raw.index = pd.to_datetime(raw.index)
    raw = raw / 1e8  # 원 → 억 원

    col_map = {"외국인합계": "외국인", "기관합계": "기관", "연기금등": "연기금", "개인": "개인"}
    raw = raw.rename(columns=col_map)
    cols = [c for c in col_map.values() if c in raw.columns]
    return idx.join(raw[cols], how="left").fillna(0)


def _f(v: float) -> str:
    """억 원 포매팅"""
    v = int(round(v))
    if v == 0: return "0"
    return f"-{abs(v):,}" if v < 0 else f"{v:,}"

def _pct(v: float) -> str:
    return f"{'+' if v > 0 else ''}{v:.2f}%"

HDR  = f"{'기간':<9}{'지수':>8}{'등락':>8}{'외국인':>10}{'기관':>9}{'연기금':>8}{'개인':>9}"
LINE = "─" * 61

def _make_row(name, jisu, pct, fg, inst, pens, ind) -> str:
    return (
        f"{name:<9}{jisu:>8.2f}{_pct(pct):>8}"
        f"{_f(fg):>10}{_f(inst):>9}{_f(pens):>8}{_f(ind):>9}"
    )

def _build_supply_msg(market: str, df: pd.DataFrame) -> str:
    sup = ["외국인", "기관", "연기금", "개인"]

    # ── 연간
    yr = df.resample("YE").agg({"지수": "last", **{c: "sum" for c in sup}})
    yr_rows = []
    for i, r in yr.iterrows():
        d = df[df.index.year == i.year]["지수"]
        p = (d.iloc[-1] / d.iloc[0] - 1) * 100 if len(d) >= 2 else 0.0
        yr_rows.append(_make_row(f"{i.year}년", r["지수"], p,
                                 r["외국인"], r["기관"], r["연기금"], r["개인"]))

    # ── 월간 최근 6개월
    mo = df.resample("ME").agg({"지수": "last", **{c: "sum" for c in sup}}).tail(6)
    mo_rows = []
    for i, r in mo.iterrows():
        d = df[(df.index.year == i.year) & (df.index.month == i.month)]["지수"]
        p = (d.iloc[-1] / d.iloc[0] - 1) * 100 if len(d) >= 2 else 0.0
        mo_rows.append(_make_row(f"{i.year%100:02d}.{i.month:02d}", r["지수"], p,
                                 r["외국인"], r["기관"], r["연기금"], r["개인"]))

    # ── 주간 (이번 주)
    wk = df.resample("W-FRI").agg({"지수": "last", **{c: "sum" for c in sup}}).tail(1)
    wk_rows = []
    for i, r in wk.iterrows():
        d = df[df.index >= (i - pd.Timedelta(days=6))]["지수"]
        p = (d.iloc[-1] / d.iloc[0] - 1) * 100 if len(d) >= 2 else 0.0
        wk_rows.append(_make_row("주간", r["지수"], p,
                                 r["외국인"], r["기관"], r["연기금"], r["개인"]))

    # ── 일별 최근 5거래일
    daily = df.tail(5).copy()
    daily["pct"] = daily["지수"].pct_change() * 100
    dy_rows = []
    for i, r in daily.iterrows():
        dy_rows.append(_make_row(i.strftime("%m/%d"), r["지수"], r.get("pct", 0),
                                 r["외국인"], r["기관"], r["연기금"], r["개인"]))

    icon  = "📈" if market == "KOSPI" else "📊"
    mname = "코스피" if market == "KOSPI" else "코스닥"
    now   = datetime.now(KST).strftime("%Y-%m-%d %H:%M")

    def block(label, rows):
        return "\n".join([f"▸ {label}", HDR, LINE] + rows)

    body = (
        f"{icon} {mname} 수급 누적  (단위: 억원)\n"
        f"업데이트: {now}\n\n"
        + block("연간", yr_rows)               + "\n\n"
        + block("월간 — 최근 6개월", mo_rows)  + "\n\n"
        + block("주간", wk_rows)               + "\n\n"
        + block("일별 — 최근 5거래일", dy_rows)
    )
    return f"```\n{body}\n```"


async def send_supply_cumulative(chat_id: str = TELEGRAM_CHAT_ID):
    """코스피 + 코스닥 수급 누적 테이블 전송"""
    if not PYKRX_OK:
        await bot.send_message(chat_id, "⚠️ pykrx 미설치로 수급 누적 기능을 사용할 수 없어요.\n`pip install pykrx` 후 재배포해주세요.")
        return

    await bot.send_message(chat_id, "⏳ 수급 누적 데이터 수집 중... (1~2분 소요)")
    loop = asyncio.get_event_loop()

    for market in ["KOSPI", "KOSDAQ"]:
        try:
            # pykrx는 동기 라이브러리 → 별도 스레드에서 실행
            df  = await loop.run_in_executor(None, _fetch_supply_df, market)
            msg = _build_supply_msg(market, df)
            # 4096자 초과 시 분할
            for chunk in [msg[i:i+4000] for i in range(0, len(msg), 4000)]:
                await bot.send_message(chat_id, chunk, parse_mode="Markdown")
        except Exception as e:
            await bot.send_message(chat_id, f"❌ {market} 수급 누적 오류: {e}")

    print(f"[{datetime.now(KST)}] 수급 누적 테이블 전송 완료")


# /수급누적 명령어 핸들러
async def cmd_supply_cumulative(update: Update, context: ContextTypes.DEFAULT_TYPE):
    asyncio.create_task(send_supply_cumulative(str(update.effective_chat.id)))


# ════════════════════════════════════════════════════════════════════════
# 스케줄러 설정 & 실행
# ════════════════════════════════════════════════════════════════════════

async def main():
    scheduler = AsyncIOScheduler(timezone=KST)

    # 매일 오전 7:30 – 모닝 브리핑
    scheduler.add_job(send_morning_briefing, "cron", hour=7,  minute=30)

    # 매 3시간마다 – 뉴스 헤드라인
    scheduler.add_job(send_news_alert, "interval", hours=3)

    # 매 10분마다 – DART 지분 변동 체크
    scheduler.add_job(check_dart, "interval", minutes=10)

    # 매 30분마다 – KITA 수출 데이터 체크
    scheduler.add_job(check_kita, "interval", minutes=30)

    # 매일 13:00 – 장중 수급 알림
    scheduler.add_job(send_supply_demand_midday, "cron", hour=13, minute=0)

    # 매일 15:40 – 장 마감 수급 + 52주 신고가/신저가
    scheduler.add_job(send_supply_demand_close, "cron", hour=15, minute=40)
    scheduler.add_job(send_52week_alert,        "cron", hour=15, minute=40)

    # 매일 15:45 – 기관/외국인 수급
    scheduler.add_job(send_investor_flow_alert, "cron", hour=15, minute=45)

    # ★ 매일 18:00 – 코스피/코스닥 수급 누적 테이블 (신규)
    scheduler.add_job(send_supply_cumulative,   "cron", hour=18, minute=0)

    scheduler.start()
    print(f"[{datetime.now(KST)}] ✅ 봇 시작됨 – 스케줄러 실행 중")

    # 시작 시 즉시 실행
    await send_morning_briefing()
    await check_dart()
    await check_kita()

    # ★ /수급누적 명령어 등록
    app = Application.builder().token(TELEGRAM_TOKEN).build()
    app.add_handler(CommandHandler("수급누적", cmd_supply_cumulative))
    await app.initialize()
    await app.start()
    await app.updater.start_polling()

    print("📌 명령어 /수급누적 등록 완료")

    while True:
        await asyncio.sleep(60)


if __name__ == "__main__":
    asyncio.run(main())
