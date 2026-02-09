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

# 🚀 리팩토링: utils에서 공통 기능 가져오기
from utils import get_connection, DB_FILE, safe_float, safe_int

load_dotenv()

# ==========================================
# 1. 설정 정보
# ==========================================
AUTH_KEY = os.getenv("HRD_API_KEY")
# DB_FILE 정의 제거됨 (utils 사용)

START_DATE = dt.date(2023, 1, 1)
END_DATE   = dt.date(2026, 1, 31)
PAGE_SIZE  = 100
MAX_WORKERS= 4

BASE_URL = "https://hrd.work24.go.kr/jsp/HRDP/HRDPO00/HRDPOA60/HRDPOA60_1.jsp"

if not AUTH_KEY:
    print("❌ 오류: .env 파일에서 HRD_API_KEY를 찾을 수 없습니다.")
    exit()

# ==========================================
# 2. 유틸리티 함수
# ==========================================
# get_connection() 함수 제거됨 (utils 사용)

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
            print(f"  ⚠️ [M{idx:02d}] 데이터 과다({scn_cnt}건) -> 주간 단위로 상세 수집")
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
        print(f"❌ [M{idx:02d}] 최종 실패: {e}")
        return []

# ==========================================
# 4. 메인 실행
# ==========================================
def main():
    print(f"🚀 [Market ETL - Refactored] 시장 데이터 수집 시작 (기간: {START_DATE} ~ {END_DATE})")
    
    months = list(month_shards(START_DATE, END_DATE))
    total_rows = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futs = {ex.submit(collect_one_month, i, m_start, m_end): i 
                for i, (m_start, m_end) in enumerate(months, start=1)}
        
        for fut in as_completed(futs):
            res = fut.result()
            if res: total_rows.extend(res)

    print(f"\n📊 총 수집된 데이터: {len(total_rows):,}건")
    if len(total_rows) == 0:
        print("⚠️ 수집된 데이터가 없습니다.")
        return

    print("💾 DB 저장 시작...")
    conn = get_connection() # utils 함수 사용
    cursor = conn.cursor()
    
    query = '''
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
            EI_EMPL_RATE_3=excluded.EI_EMPL_RATE_3,
            EI_EMPL_CNT_3=excluded.EI_EMPL_CNT_3,
            STDG_SCOR=excluded.STDG_SCOR,
            COLLECTED_AT=CURRENT_TIMESTAMP
    '''
    
    try:
        batch_size = 1000
        for i in range(0, len(total_rows), batch_size):
            batch = total_rows[i:i+batch_size]
            cursor.executemany(query, batch)
            conn.commit()
            print(f"   >> {min(i+batch_size, len(total_rows))}/{len(total_rows)} 저장 완료")
    except Exception as e:
        print(f"❌ DB 저장 중 오류: {e}")
    finally:
        conn.close()

    print("\n✅ [Success] 모든 시장 데이터가 DB에 저장되었습니다!")

if __name__ == "__main__":
    main()