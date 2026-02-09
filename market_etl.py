import sqlite3
import requests
import os
import math
import datetime as dt
from concurrent.futures import ThreadPoolExecutor, as_completed
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from utils import get_connection, DB_FILE, safe_float, safe_int

load_dotenv()

# ==========================================
# 1. 설정 정보
# ==========================================
AUTH_KEY = os.getenv("HRD_API_KEY")

ARCHIVE_START = dt.date(2023, 1, 1)  # 최초 수집 시 시작점
REFRESH_MONTHS = 12                   # 갱신 구간 (취업률 확정 대기)
PAGE_SIZE  = 100
MAX_WORKERS= 4

BASE_URL = "https://hrd.work24.go.kr/jsp/HRDP/HRDPO00/HRDPOA60/HRDPOA60_1.jsp"

if not AUTH_KEY:
    print("오류: .env 파일에서 HRD_API_KEY를 찾을 수 없습니다.")
    exit()

# ==========================================
# 2. 유틸리티 함수
# ==========================================
def ymd(d: dt.date) -> str:
    return d.strftime("%Y%m%d")

def get_retry_session():
    session = requests.Session()
    retry = Retry(
        total=5,
        backoff_factor=1,
        status_forcelist=[500, 502, 503, 504],
        allowed_methods=["GET"]
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session

def month_shards(start: dt.date, end: dt.date):
    cur = dt.date(start.year, start.month, 1)
    end_m = dt.date(end.year, end.month, 1)
    while cur <= end_m:
        last = (dt.date(cur.year, 12, 31) if cur.month==12
                else dt.date(cur.year, cur.month+1, 1) - dt.timedelta(days=1))
        yield max(cur, start), min(last, end)
        cur = (dt.date(cur.year+1, 1, 1) if cur.month==12
               else dt.date(cur.year, cur.month+1, 1))

def week_shards(start: dt.date, end: dt.date):
    cur = start
    while cur <= end:
        w_end = min(cur + dt.timedelta(days=6), end)
        yield cur, w_end
        cur = w_end + dt.timedelta(days=1)

def get_collect_range():
    """DB 상태를 확인하여 수집 범위를 결정합니다."""
    today = dt.date.today()
    refresh_start = today - dt.timedelta(days=REFRESH_MONTHS * 30)

    conn = get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM TB_MARKET_TREND")
        count = cursor.fetchone()[0]
    except Exception:
        count = 0
    finally:
        conn.close()

    if count == 0:
        # 첫 실행: 전체 수집
        start = ARCHIVE_START
        print(f"[모드] 첫 실행 - 전체 수집 ({start} ~ {today})")
    else:
        # 이후 실행: 갱신 구간만
        start = refresh_start
        print(f"[모드] 증분 수집 - 최근 {REFRESH_MONTHS}개월 ({start} ~ {today})")
        print(f"       (DB 기존 데이터: {count:,}건)")

    return start, today

def parse_rows_xml(soup: BeautifulSoup):
    out = []
    sl = soup.find("srchList")
    if not sl: return out

    for scn in sl.find_all("scn_list"):
        def g(tag):
            el = scn.find(tag)
            return el.text.strip() if el and el.text else ""

        out.append((
            g("trprId"), safe_int(g("trprDegr")),
            g("title"), g("subTitle"), g("traStartDate"), g("traEndDate"),
            g("ncsCd"), g("trngAreaCd"),
            safe_int(g("yardMan")), safe_float(g("realMan"), default=None), safe_float(g("courseMan"), default=None), safe_int(g("regCourseMan")),
            safe_float(g("eiEmplRate3"), default=None), safe_float(g("eiEmplRate6"), default=None), safe_int(g("eiEmplCnt3")), g("eiEmplCnt3Gt10"),
            safe_float(g("stdgScor"), default=None), g("grade"),
            g("certificate"), g("contents"), g("address"), g("telNo"),
            g("instCd"), g("trainstCstId"), g("trainTarget"), g("trainTargetCd"),
            g("wkendSe"),
            g("titleIcon"), g("titleLink"), g("subTitleLink")
        ))
    return out

# ==========================================
# 3. 수집 로직
# ==========================================
def collect_one_month(idx: int, m_start: dt.date, m_end: dt.date):
    session = get_retry_session()

    params = {
        "authKey": AUTH_KEY, "returnType": "XML", "outType": "1",
        "pageNum": "1", "pageSize": str(PAGE_SIZE),
        "sort": "ASC", "sortCol": "2", "srchNcs1": "20", "srchTraArea1": "00"
    }
    params["srchTraStDt"], params["srchTraEndDt"] = ymd(m_start), ymd(m_end)

    try:
        r = session.get(BASE_URL, params=params, timeout=60)
        soup1 = BeautifulSoup(r.content, "lxml-xml")

        cnt_tag = soup1.find("scn_cnt")
        scn_cnt = int(cnt_tag.text) if cnt_tag and cnt_tag.text else 0
        total_pages = math.ceil(scn_cnt / PAGE_SIZE) if scn_cnt else 0

        print(f"[M{idx:02d}] {m_start} ~ {m_end} | 대상: {scn_cnt:,}건")

        rows = []
        if total_pages == 0: return rows

        if total_pages <= 1000:
            rows.extend(parse_rows_xml(soup1))
            for pg in range(2, total_pages + 1):
                params["pageNum"] = str(pg)
                r = session.get(BASE_URL, params=params, timeout=60)
                rows.extend(parse_rows_xml(BeautifulSoup(r.content, "lxml-xml")))
        else:
            print(f"  [M{idx:02d}] 데이터 과다({scn_cnt}건) -> 주간 단위로 상세 수집")
            for w_start, w_end in week_shards(m_start, m_end):
                w_params = params.copy()
                w_params["srchTraStDt"], w_params["srchTraEndDt"] = ymd(w_start), ymd(w_end)
                w_params["pageNum"] = "1"

                r_w = session.get(BASE_URL, params=w_params, timeout=60)
                soup_w = BeautifulSoup(r_w.content, "lxml-xml")

                w_cnt = int(soup_w.find("scn_cnt").text) if soup_w.find("scn_cnt") else 0
                w_pages = math.ceil(w_cnt / PAGE_SIZE)

                if w_pages > 0:
                    rows.extend(parse_rows_xml(soup_w))
                    for pg in range(2, w_pages + 1):
                        w_params["pageNum"] = str(pg)
                        r_next = session.get(BASE_URL, params=w_params, timeout=60)
                        rows.extend(parse_rows_xml(BeautifulSoup(r_next.content, "lxml-xml")))
        return rows

    except Exception as e:
        print(f"[M{idx:02d}] 최종 실패: {e}")
        return []

# ==========================================
# 4. DB 저장
# ==========================================
UPSERT_QUERY = '''
    INSERT INTO TB_MARKET_TREND (
        TRPR_ID, TRPR_DEGR, TRPR_NM, TRAINST_NM, TR_STA_DT, TR_END_DT, NCS_CD, TRNG_AREA_CD,
        TOT_FXNUM, TOT_TRCO, COURSE_MAN, REG_COURSE_MAN,
        EI_EMPL_RATE_3, EI_EMPL_RATE_6, EI_EMPL_CNT_3, EI_EMPL_CNT_3_GT10,
        STDG_SCOR, GRADE,
        CERTIFICATE, CONTENTS, ADDRESS, TEL_NO,
        INST_INO, TRAINST_CST_ID, TRAIN_TARGET, TRAIN_TARGET_CD,
        WKEND_SE, TITLE_ICON, TITLE_LINK, SUB_TITLE_LINK
    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    ON CONFLICT(TRPR_ID, TRPR_DEGR) DO UPDATE SET
        TRPR_NM=excluded.TRPR_NM,
        TRAINST_NM=excluded.TRAINST_NM,
        TR_END_DT=excluded.TR_END_DT,
        TOT_FXNUM=excluded.TOT_FXNUM,
        TOT_TRCO=excluded.TOT_TRCO,
        COURSE_MAN=excluded.COURSE_MAN,
        REG_COURSE_MAN=excluded.REG_COURSE_MAN,
        EI_EMPL_RATE_3=excluded.EI_EMPL_RATE_3,
        EI_EMPL_RATE_6=excluded.EI_EMPL_RATE_6,
        EI_EMPL_CNT_3=excluded.EI_EMPL_CNT_3,
        EI_EMPL_CNT_3_GT10=excluded.EI_EMPL_CNT_3_GT10,
        STDG_SCOR=excluded.STDG_SCOR,
        GRADE=excluded.GRADE,
        CERTIFICATE=excluded.CERTIFICATE,
        CONTENTS=excluded.CONTENTS,
        ADDRESS=excluded.ADDRESS,
        TEL_NO=excluded.TEL_NO,
        TRAIN_TARGET=excluded.TRAIN_TARGET,
        TRAIN_TARGET_CD=excluded.TRAIN_TARGET_CD,
        WKEND_SE=excluded.WKEND_SE,
        COLLECTED_AT=CURRENT_TIMESTAMP
'''

def save_rows(rows):
    """수집된 rows를 DB에 배치 저장합니다."""
    if not rows:
        return 0
    conn = get_connection()
    cursor = conn.cursor()
    try:
        batch_size = 1000
        for i in range(0, len(rows), batch_size):
            batch = rows[i:i+batch_size]
            cursor.executemany(UPSERT_QUERY, batch)
            conn.commit()
        return len(rows)
    except Exception as e:
        print(f"DB 저장 중 오류: {e}")
        return 0
    finally:
        conn.close()

# ==========================================
# 5. 메인 실행
# ==========================================
def main():
    start_date, end_date = get_collect_range()
    print(f"[Market ETL] 수집 기간: {start_date} ~ {end_date}")

    months = list(month_shards(start_date, end_date))
    total_saved = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futs = {ex.submit(collect_one_month, i, m_start, m_end): i
                for i, (m_start, m_end) in enumerate(months, start=1)}

        for fut in as_completed(futs):
            rows = fut.result()
            if rows:
                saved = save_rows(rows)
                total_saved += saved
                print(f"   >> {saved:,}건 저장 (누적: {total_saved:,}건)")

    if total_saved == 0:
        print("수집된 데이터가 없습니다.")
    else:
        print(f"\n[Success] 총 {total_saved:,}건 저장 완료!")

if __name__ == "__main__":
    main()
