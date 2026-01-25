import sqlite3
import requests
import os
import math
import time
import datetime as dt
from concurrent.futures import ThreadPoolExecutor, as_completed
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# .env 로드
load_dotenv()

# ==========================================
# 1. 설정 정보
# ==========================================
AUTH_KEY = os.getenv("HRD_API_KEY")
DB_FILE = "hrd_analysis.db"

# 수집 기간 (2023.01 ~ 2026.01)
START_DATE = dt.date(2023, 1, 1)
END_DATE   = dt.date(2026, 1, 31)
PAGE_SIZE  = 100
MAX_WORKERS= 4   # ⚠️ 안정성을 위해 6 -> 4로 하향 조정

BASE_URL = "https://hrd.work24.go.kr/jsp/HRDP/HRDPO00/HRDPOA60/HRDPOA60_1.jsp"

if not AUTH_KEY:
    print("❌ 오류: .env 파일에서 HRD_API_KEY를 찾을 수 없습니다.")
    exit()

# ==========================================
# 2. 유틸리티 함수 (재시도 로직 추가)
# ==========================================
def get_connection():
    return sqlite3.connect(DB_FILE)

def ymd(d: dt.date) -> str:
    return d.strftime("%Y%m%d")

# 🚀 [핵심] 끊겨도 다시 붙는 끈질긴 세션 생성
def get_retry_session():
    session = requests.Session()
    retry = Retry(
        total=5,                # 최대 5번 재시도
        backoff_factor=1,       # 1초, 2초, 4초... 기다렸다 재시도
        status_forcelist=[500, 502, 503, 504], # 서버 에러시 재시도
        allowed_methods=["GET"]
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session

# 월 단위 샤딩
def month_shards(start: dt.date, end: dt.date):
    cur = dt.date(start.year, start.month, 1)
    end_m = dt.date(end.year, end.month, 1)
    while cur <= end_m:
        last = (dt.date(cur.year, 12, 31) if cur.month==12
                else dt.date(cur.year, cur.month+1, 1) - dt.timedelta(days=1))
        yield max(cur, start), min(last, end)
        cur = (dt.date(cur.year+1, 1, 1) if cur.month==12
               else dt.date(cur.year, cur.month+1, 1))

# 주 단위 샤딩
def week_shards(start: dt.date, end: dt.date):
    cur = start
    while cur <= end:
        w_end = min(cur + dt.timedelta(days=6), end)
        yield cur, w_end
        cur = w_end + dt.timedelta(days=1)

def safe_float(val):
    if not val or val == "": return None
    try: return float(val)
    except: return None

def safe_int(val):
    if not val or val == "": return None
    try: return int(val)
    except: return None

# XML 파싱
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
            safe_int(g("yardMan")), safe_float(g("realMan")), safe_float(g("courseMan")), safe_int(g("regCourseMan")),
            safe_float(g("eiEmplRate3")), safe_float(g("eiEmplRate6")), safe_int(g("eiEmplCnt3")), g("eiEmplCnt3Gt10"),
            safe_float(g("stdgScor")), g("grade"),
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
    session = get_retry_session() # 👈 재시도 세션 장착
    
    params = {
        "authKey": AUTH_KEY, 
        "returnType": "XML", 
        "outType": "1",
        "pageNum": "1",
        "pageSize": str(PAGE_SIZE), 
        "sort": "ASC", 
        "sortCol": "2",
        "srchNcs1": "20",       
        "srchTraArea1": "00"    
    }
    params["srchTraStDt"], params["srchTraEndDt"] = ymd(m_start), ymd(m_end)

    try:
        # 1. 건수 확인 (재시도 기능 자동 적용됨)
        r = session.get(BASE_URL, params=params, timeout=60) # 타임아웃 60초로 넉넉하게
        soup1 = BeautifulSoup(r.content, "lxml-xml")
        
        cnt_tag = soup1.find("scn_cnt")
        scn_cnt = int(cnt_tag.text) if cnt_tag and cnt_tag.text else 0
        total_pages = math.ceil(scn_cnt / PAGE_SIZE) if scn_cnt else 0
        
        print(f"[M{idx:02d}] {m_start} ~ {m_end} | 대상: {scn_cnt:,}건")

        rows = []
        if total_pages == 0: return rows

        # 2. 1000페이지 이하면 바로 수집
        if total_pages <= 1000:
            rows.extend(parse_rows_xml(soup1))
            for pg in range(2, total_pages + 1):
                params["pageNum"] = str(pg)
                r = session.get(BASE_URL, params=params, timeout=60)
                rows.extend(parse_rows_xml(BeautifulSoup(r.content, "lxml-xml")))
        
        # 3. 1000페이지 초과면 주 단위로 쪼개기
        else:
            print(f"  ⚠️ [M{idx:02d}] 데이터 과다({scn_cnt}건) -> 주간 단위로 상세 수집")
            for w_start, w_end in week_shards(m_start, m_end):
                w_params = params.copy()
                w_params["srchTraStDt"], w_params["srchTraEndDt"] = ymd(w_start), ymd(w_end)
                w_params["pageNum"] = "1"
                
                r_w = session.get(BASE_URL, params=w_params, timeout=60)
                soup_w = BeautifulSoup(r_w.content, "lxml-xml")
                
                w_cnt_tag = soup_w.find("scn_cnt")
                w_cnt = int(w_cnt_tag.text) if w_cnt_tag and w_cnt_tag.text else 0
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
        return [] # 빈 리스트 반환 (이 달은 건너뜀)

# ==========================================
# 4. 메인 실행
# ==========================================
def main():
    print(f"🚀 [Market ETL - Robust] 시장 데이터 수집 시작 (기간: {START_DATE} ~ {END_DATE})")
    print(f"   - 재시도 로직: 탑재 완료 (Max 5회)")
    print(f"   - 병렬 프로세스: {MAX_WORKERS}개 (안정성 강화)")

    months = list(month_shards(START_DATE, END_DATE))
    total_rows = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futs = {ex.submit(collect_one_month, i, m_start, m_end): i 
                for i, (m_start, m_end) in enumerate(months, start=1)}
        
        for fut in as_completed(futs):
            res = fut.result()
            if res:
                total_rows.extend(res)

    print(f"\n📊 총 수집된 데이터: {len(total_rows):,}건")
    
    if len(total_rows) == 0:
        print("⚠️ 수집된 데이터가 없습니다. 다시 확인해주세요.")
        return

    print("💾 DB 저장 시작... (잠시만 기다려주세요)")

    conn = get_connection()
    cursor = conn.cursor()
    
    query = '''
        INSERT INTO TB_MARKET_TREND (
            TRPR_ID, TRPR_DEGR, 
            TRPR_NM, TRAINST_NM, TR_STA_DT, TR_END_DT, NCS_CD, TRNG_AREA_CD,
            TOT_FXNUM, TOT_TRCO, COURSE_MAN, REG_COURSE_MAN,
            EI_EMPL_RATE_3, EI_EMPL_RATE_6, EI_EMPL_CNT_3, EI_EMPL_CNT_3_GT10,
            STDG_SCOR, GRADE,
            CERTIFICATE, CONTENTS, ADDRESS, TEL_NO,
            INST_INO, TRAINST_CST_ID, TRAIN_TARGET, TRAIN_TARGET_CD,
            WKEND_SE, 
            TITLE_ICON, TITLE_LINK, SUB_TITLE_LINK
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