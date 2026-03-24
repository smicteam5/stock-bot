import os
import asyncio
import aiohttp
import feedparser
from datetime import datetime, time
import pytz
from telegram import Bot
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
            # CNN 응답 구조
            if "fear_and_greed" in data:
                score  = float(data["fear_and_greed"]["score"])
                rating = data["fear_and_greed"]["rating"]
            # 대체 API 응답 구조
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

def arrow(chg: float) -> str:
    return "🔴▼" if chg < 0 else "🟢▲"

async def send_morning_briefing():
    async with aiohttp.ClientSession() as session:
        # 지수
        nasdaq = await fetch_yahoo_quote(session, "^IXIC")
        snp    = await fetch_yahoo_quote(session, "^GSPC")
        # Fear & Greed
        fg = await fetch_fear_greed(session)
        # 섹터
        sector_lines = []
        for name, sym in SECTORS.items():
            q = await fetch_yahoo_quote(session, sym)
            sector_lines.append(f"  {arrow(q['change'])} {name}: {q['change']:+.2f}%")

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
    ] + sector_lines + ["━━━━━━━━━━━━━━━━━━━━━━━━"]

    await bot.send_message(
        chat_id=TELEGRAM_CHAT_ID,
        text="\n".join(lines),
        parse_mode="Markdown"
    )
    print(f"[{datetime.now(KST)}] 모닝 브리핑 전송 완료")


# ════════════════════════════════════════════════════════════════════════
# 2) DART – 주요 기관 지분 변동 실시간 감시
# ════════════════════════════════════════════════════════════════════════

# 감시할 기관 목록 (법인명 키워드)
WATCH_INSTITUTIONS = [
    "국민연금",
    "삼성자산운용",
    "미래에셋자산운용",
    "한국투자",
    "KB자산운용",
    "신한자산운용",
]

seen_dart_ids: set = set()   # 이미 알림 보낸 공시 ID 저장

async def fetch_dart_major_holdings(session) -> list:
    """5% 이상 대량보유 상황보고 (주요주주)"""
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
        reporter  = item.get("repror_nm", "")       # 보고자(기관명)
        corp_name = item.get("corp_name", "")        # 대상 법인
        stock_type= item.get("stkqy_irds_nm", "")   # 주식 종류
        hold_ratio= item.get("stkqy_irds_rate", "")  # 보유 비율 변동
        rcept_dt  = item.get("rcept_dt", "")         # 접수일

        if doc_id in seen_dart_ids:
            continue

        # 감시 기관이 포함된 공시만 알림
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
# 3) KITA – 한국 메모리 반도체 수출 데이터
# ════════════════════════════════════════════════════════════════════════

last_kita_data: dict = {}   # 마지막으로 받은 데이터 캐시

async def fetch_kita_memory_export(session) -> dict | None:
    """
    KITA 무역통계 API – 메모리 반도체 (HS 8542 기반 품목)
    실제 KITA Open API 엔드포인트 사용
    """
    url = "https://www.kita.net/openApi/tradeStats/getExportStats.do"
    params = {
        "serviceKey": "KITA_FREE",   # KITA 무료 공개 엔드포인트
        "hsSgn":      "854232",      # HS코드: 메모리 반도체
        "statYymm":   datetime.now(KST).strftime("%Y%m"),
        "natCd":      "000",         # 전체 국가
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

    # 데이터가 이전과 달라졌을 때만 알림
    key = str(data)
    if key == str(last_kita_data):
        return
    last_kita_data = data

    try:
        items = data.get("data") or data.get("list") or []
        if not items:
            return
        latest = items[0]

        export_amt   = latest.get("expAmt",   latest.get("expDlr",  "N/A"))   # 수출 금액(천달러)
        export_qty   = latest.get("expQty",   latest.get("expKg",   "N/A"))   # 수출 중량(kg)
        unit_price   = latest.get("unitPrice","N/A")                           # 단가
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
# 스케줄러 설정 & 실행
# ════════════════════════════════════════════════════════════════════════

async def main():
    scheduler = AsyncIOScheduler(timezone=KST)

    # 매일 오전 7:30 – 모닝 브리핑
    scheduler.add_job(send_morning_briefing, "cron", hour=7, minute=30)

    # 매 10분마다 – DART 기관 지분 변동 체크
    scheduler.add_job(check_dart, "interval", minutes=10)

    # 매 30분마다 – KITA 수출 데이터 체크
    scheduler.add_job(check_kita, "interval", minutes=30)

    scheduler.start()
    print(f"[{datetime.now(KST)}] ✅ 봇 시작됨 – 스케줄러 실행 중")

    # 시작하자마자 한 번 실행 (테스트용)
    await send_morning_briefing()
    await check_dart()
    await check_kita()

    # 무한 대기
    while True:
        await asyncio.sleep(60)

if __name__ == "__main__":
    asyncio.run(main())
