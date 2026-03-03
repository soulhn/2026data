import sqlite3
import requests
import os
import math
import json
import datetime as dt
from concurrent.futures import ThreadPoolExecutor, as_completed
from bs4 import BeautifulSoup
from dotenv import load_dotenv

from utils import get_connection, DB_FILE, safe_float, safe_int, get_retry_session, adapt_query, is_pg
from init_db import init_all_tables
from config import ETL_ARCHIVE_START, ETL_REFRESH_MONTHS, ETL_PAGE_SIZE, ETL_MAX_WORKERS, ETL_BATCH_SIZE, ETL_BATCH_PAGE_SIZE, CacheKey

import logging
import time

logger = logging.getLogger(__name__)
logging.basicConfig(
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
    level=logging.INFO,
)

load_dotenv()

# ==========================================
# 1. 설정 정보
# ==========================================
AUTH_KEY = os.getenv("HRD_API_KEY")

ARCHIVE_START = ETL_ARCHIVE_START
REFRESH_MONTHS = ETL_REFRESH_MONTHS
PAGE_SIZE  = ETL_PAGE_SIZE
MAX_WORKERS= ETL_MAX_WORKERS

BASE_URL = "https://hrd.work24.go.kr/jsp/HRDP/HRDPO00/HRDPOA60/HRDPOA60_1.jsp"

if not AUTH_KEY:
    logger.warning("HRD_API_KEY를 찾을 수 없습니다. ETL 실행 시 오류가 발생합니다.")

# ==========================================
# 2. 유틸리티 함수
# ==========================================
def ymd(d: dt.date) -> str:
    return d.strftime("%Y%m%d")


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
        cursor.execute("SELECT COUNT(*), MIN(TR_STA_DT) FROM TB_MARKET_TREND")
        row = cursor.fetchone()
        count = row[0] if row else 0
        min_dt_str = str(row[1]) if row and row[1] else None
    except Exception:
        count = 0
        min_dt_str = None
    finally:
        conn.close()

    if count == 0:
        # 첫 실행: 전체 수집
        start = ARCHIVE_START
        logger.info(f"[모드] 첫 실행 - 전체 수집 ({start} ~ {today})")
    else:
        # 아카이브 기준 연도 데이터가 없으면 전체 재수집 (Supabase 누락 데이터 복구)
        has_archive = min_dt_str and min_dt_str[:4] <= str(ARCHIVE_START.year)
        if not has_archive:
            start = ARCHIVE_START
            logger.info(f"[모드] 아카이브 보완 - 전체 수집 ({start} ~ {today})")
            logger.info(f"DB 최초 데이터: {min_dt_str}, 기준: {ARCHIVE_START}")
        else:
            start = refresh_start
            logger.info(f"[모드] 증분 수집 - 최근 {REFRESH_MONTHS}개월 ({start} ~ {today})")
        logger.info(f"DB 기존 데이터: {count:,}건")

    return start, today

def _normalize_stdg_scor(val):
    """API 스케일 변경 대응: 100점 스케일(≤100) → 10000점 스케일로 통일."""
    if val is not None and 0 < val <= 100:
        return val * 100
    return val


def parse_rows_xml(soup: BeautifulSoup):
    out = []
    sl = soup.find("srchList")
    if not sl: return out

    for scn in sl.find_all("scn_list"):
        def g(tag):
            el = scn.find(tag)
            return el.text.strip() if el and el.text else ""

        sta_dt = g("traStartDate")
        address = g("address")
        if len(sta_dt) >= 7 and sta_dt[4] == '-':
            year_month = sta_dt[:7]          # YYYY-MM-DD → YYYY-MM
        elif len(sta_dt) >= 6:
            year_month = f"{sta_dt[:4]}-{sta_dt[4:6]}"  # YYYYMMDD → YYYY-MM
        else:
            year_month = None
        region = address.split()[0] if address and address.strip() else None

        out.append((
            g("trprId"), safe_int(g("trprDegr")),
            g("title"), g("subTitle"), sta_dt, g("traEndDate"),
            g("ncsCd"), g("trngAreaCd"),
            safe_int(g("yardMan")), safe_float(g("realMan"), default=None), safe_float(g("courseMan"), default=None), safe_int(g("regCourseMan")),
            safe_float(g("eiEmplRate3"), default=None), safe_float(g("eiEmplRate6"), default=None), safe_int(g("eiEmplCnt3")), g("eiEmplCnt3Gt10"),
            _normalize_stdg_scor(safe_float(g("stdgScor"), default=None)), g("grade"),
            g("certificate"), g("contents"), address, g("telNo"),
            g("instCd"), g("trainstCstId"), g("trainTarget"), g("trainTargetCd"),
            g("wkendSe"),
            g("titleIcon"), g("titleLink"), g("subTitleLink"),
            year_month, region
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

        logger.info(f"[M{idx:02d}] {m_start} ~ {m_end} | 대상: {scn_cnt:,}건")

        rows = []
        if total_pages == 0: return rows

        if total_pages <= 1000:
            rows.extend(parse_rows_xml(soup1))
            for pg in range(2, total_pages + 1):
                params["pageNum"] = str(pg)
                r = session.get(BASE_URL, params=params, timeout=60)
                rows.extend(parse_rows_xml(BeautifulSoup(r.content, "lxml-xml")))
        else:
            logger.info(f"[M{idx:02d}] 데이터 과다({scn_cnt}건) -> 주간 단위로 상세 수집")
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
        logger.error(f"[M{idx:02d}] 최종 실패: {e}")
        return []

# ==========================================
# 4. DB 저장
# ==========================================
_UPSERT_QUERY_RAW = '''
    INSERT INTO TB_MARKET_TREND (
        TRPR_ID, TRPR_DEGR, TRPR_NM, TRAINST_NM, TR_STA_DT, TR_END_DT, NCS_CD, TRNG_AREA_CD,
        TOT_FXNUM, TOT_TRCO, COURSE_MAN, REG_COURSE_MAN,
        EI_EMPL_RATE_3, EI_EMPL_RATE_6, EI_EMPL_CNT_3, EI_EMPL_CNT_3_GT10,
        STDG_SCOR, GRADE,
        CERTIFICATE, CONTENTS, ADDRESS, TEL_NO,
        INST_INO, TRAINST_CST_ID, TRAIN_TARGET, TRAIN_TARGET_CD,
        WKEND_SE, TITLE_ICON, TITLE_LINK, SUB_TITLE_LINK,
        YEAR_MONTH, REGION
    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
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
        YEAR_MONTH=excluded.YEAR_MONTH,
        REGION=excluded.REGION,
        COLLECTED_AT=CURRENT_TIMESTAMP
'''

def save_rows(rows):
    """수집된 rows를 DB에 배치 저장합니다."""
    if not rows:
        return 0
    conn = get_connection()
    cursor = conn.cursor()
    upsert_query = adapt_query(_UPSERT_QUERY_RAW)
    try:
        batch_size = ETL_BATCH_SIZE
        if is_pg():
            from psycopg2.extras import execute_batch
            for i in range(0, len(rows), batch_size):
                batch = rows[i:i+batch_size]
                execute_batch(cursor, upsert_query, batch, page_size=ETL_BATCH_PAGE_SIZE)
                conn.commit()
        else:
            for i in range(0, len(rows), batch_size):
                batch = rows[i:i+batch_size]
                cursor.executemany(upsert_query, batch)
                conn.commit()
        return len(rows)
    except Exception as e:
        logger.error(f"DB 저장 중 오류: {e}")
        return 0
    finally:
        conn.close()

# ==========================================
# 5. 집계 캐시 (ETL 완료 후 자동 실행)
# ==========================================
def compute_and_cache_aggregations():
    """ETL 완료 후 주요 집계를 TB_MARKET_CACHE에 저장합니다."""
    logger.info("시장 데이터 집계 시작...")
    conn = get_connection()
    cursor = conn.cursor()

    upsert_sql = adapt_query("""
        INSERT INTO TB_MARKET_CACHE (CACHE_KEY, CACHE_DATA, COMPUTED_AT)
        VALUES (?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(CACHE_KEY) DO UPDATE SET
            CACHE_DATA=excluded.CACHE_DATA,
            COMPUTED_AT=excluded.COMPUTED_AT
    """)

    def run_agg(key, sql, params=()):
        try:
            if params:
                cursor.execute(sql, params)
            else:
                cursor.execute(sql)
            cols = [d[0].upper() for d in cursor.description]
            rows = [dict(zip(cols, row)) for row in cursor.fetchall()]
            data_json = json.dumps(rows, ensure_ascii=False, default=str)
            cursor.execute(upsert_sql, (key, data_json))
            conn.commit()
            logger.info(f"[캐시] {key}: {len(rows)}행 저장")
            return True
        except Exception as e:
            logger.error(f"[캐시] {key} 실패: {e}")
            if is_pg():
                conn.rollback()
            return False

    aggs = [
        (CacheKey.KPI, adapt_query("""
            SELECT COUNT(*) as CNT,
                   AVG(CASE WHEN TOT_TRCO > 0 THEN TOT_TRCO END) as AVG_TRCO,
                   AVG(TOT_FXNUM) as AVG_FXNUM,
                   AVG(CASE WHEN EI_EMPL_RATE_3 > 0 THEN EI_EMPL_RATE_3 END) as AVG_EMPL,
                   AVG(CASE WHEN STDG_SCOR > 0 THEN STDG_SCOR END) as AVG_SCORE
            FROM TB_MARKET_TREND
        """), ()),
        (CacheKey.MONTHLY_COUNTS, adapt_query("""
            SELECT YEAR_MONTH, COUNT(*) as COUNT
            FROM TB_MARKET_TREND WHERE YEAR_MONTH IS NOT NULL
            GROUP BY YEAR_MONTH ORDER BY YEAR_MONTH
        """), ()),
        (CacheKey.REGION_COUNTS, adapt_query("""
            SELECT REGION as 지역, COUNT(*) as 개수
            FROM TB_MARKET_TREND WHERE REGION IS NOT NULL
            GROUP BY REGION ORDER BY 개수 DESC
        """), ()),
        (CacheKey.INST_STATS, adapt_query("""
            SELECT TRAINST_NM,
                   COUNT(*) as TRPR_CNT,
                   COALESCE(SUM(TOT_FXNUM), 0) as TOT_FXNUM,
                   COALESCE(SUM(REG_COURSE_MAN), 0) as REG_COURSE_MAN,
                   AVG(EI_EMPL_RATE_3) as AVG_EMPL,
                   AVG(STDG_SCOR) as AVG_SCORE
            FROM TB_MARKET_TREND
            GROUP BY TRAINST_NM ORDER BY TRPR_CNT DESC
        """), ()),
        (CacheKey.NCS_AGG, adapt_query("""
            SELECT NCS_CD,
                   COUNT(*) as CNT,
                   COALESCE(SUM(TOT_FXNUM), 0) as TOT_FXNUM,
                   COALESCE(SUM(REG_COURSE_MAN), 0) as REG_COURSE_MAN,
                   AVG(CASE WHEN REG_COURSE_MAN > 0 AND TOT_FXNUM > 0
                       THEN CAST(REG_COURSE_MAN AS REAL) / TOT_FXNUM * 100 END) as AVG_RECRUIT
            FROM TB_MARKET_TREND
            GROUP BY NCS_CD HAVING COUNT(*) >= 5
            ORDER BY CNT DESC
        """), ()),
        (CacheKey.MONTHLY_EMPL, adapt_query("""
            SELECT YEAR_MONTH as 월, AVG(EI_EMPL_RATE_3) as 평균취업률
            FROM TB_MARKET_TREND
            WHERE EI_EMPL_RATE_3 > 0 AND YEAR_MONTH IS NOT NULL
            GROUP BY YEAR_MONTH ORDER BY YEAR_MONTH
        """), ()),
        (CacheKey.MONTHLY_RECRUIT, adapt_query("""
            SELECT YEAR_MONTH,
                   AVG(CASE WHEN TOT_FXNUM > 0
                       THEN CAST(REG_COURSE_MAN AS REAL) / TOT_FXNUM * 100 END) as 모집률
            FROM TB_MARKET_TREND WHERE YEAR_MONTH IS NOT NULL
            GROUP BY YEAR_MONTH ORDER BY YEAR_MONTH
        """), ()),
        (CacheKey.REGION_OPP, adapt_query("""
            SELECT REGION,
                   COUNT(*) as 과정수,
                   SUM(COALESCE(TOT_FXNUM, 0)) as 총신청인원,
                   AVG(CASE WHEN TOT_FXNUM > 0
                       THEN CAST(REG_COURSE_MAN AS REAL) / TOT_FXNUM * 100 END) as 평균모집률,
                   AVG(CASE WHEN EI_EMPL_RATE_3 > 0 THEN EI_EMPL_RATE_3 END) as 평균취업률
            FROM TB_MARKET_TREND
            WHERE REGION IS NOT NULL
            GROUP BY REGION HAVING COUNT(*) >= 5
        """), ()),
        (CacheKey.NCS_OPP_MATRIX, adapt_query("""
            SELECT NCS_CD,
                   COUNT(*) as 경쟁과정수,
                   AVG(CASE WHEN EI_EMPL_RATE_3 > 0 THEN EI_EMPL_RATE_3 END) as 평균취업률,
                   AVG(CASE WHEN TOT_FXNUM > 0
                       THEN CAST(REG_COURSE_MAN AS REAL) / TOT_FXNUM * 100 END) as 평균모집률
            FROM TB_MARKET_TREND
            GROUP BY NCS_CD HAVING COUNT(*) >= 3
        """), ()),
    ]

    saved_count = sum(run_agg(key, sql, params) for key, sql, params in aggs)

    # DB 명세 페이지용 시장 분포 캐시
    db_dist_aggs = [
        (CacheKey.DB_MARKET_TYPE, adapt_query("""
            SELECT TRAIN_TARGET AS 훈련유형, COUNT(*) AS 건수
            FROM TB_MARKET_TREND
            WHERE TRAIN_TARGET IS NOT NULL AND TRAIN_TARGET != ''
            GROUP BY TRAIN_TARGET ORDER BY 건수 DESC
        """), ()),
        (CacheKey.DB_MARKET_REGION, adapt_query("""
            SELECT REGION AS 지역, COUNT(*) AS 건수
            FROM TB_MARKET_TREND
            WHERE REGION IS NOT NULL AND REGION != ''
            GROUP BY REGION ORDER BY 건수 DESC
        """), ()),
        (CacheKey.DB_MARKET_YEAR, adapt_query("""
            SELECT SUBSTR(YEAR_MONTH,1,4) AS 연도, COUNT(*) AS 건수
            FROM TB_MARKET_TREND
            WHERE YEAR_MONTH IS NOT NULL
            GROUP BY SUBSTR(YEAR_MONTH,1,4) ORDER BY 연도
        """), ()),
    ]
    saved_count += sum(run_agg(key, sql, params) for key, sql, params in db_dist_aggs)

    # ncs_growth: 최근 6개월 vs 이전 6개월 비교 (동적 날짜 계산)
    try:
        cursor.execute(adapt_query(
            "SELECT MAX(YEAR_MONTH) as MAX_YM FROM TB_MARKET_TREND WHERE YEAR_MONTH IS NOT NULL"
        ))
        row = cursor.fetchone()
        max_ym_str = str(row[0])[:7] if row and row[0] else None
    except Exception:
        max_ym_str = None

    if max_ym_str:
        try:
            max_y, max_m = int(max_ym_str[:4]), int(max_ym_str[5:7])
            mid_m = max_m - 6
            mid_y = max_y
            if mid_m <= 0:
                mid_m += 12
                mid_y -= 1
            start_m = max_m - 12
            start_y = max_y
            if start_m <= 0:
                start_m += 12
                start_y -= 1
            mid_ym = f"{mid_y:04d}-{mid_m:02d}"
            start_ym = f"{start_y:04d}-{start_m:02d}"

            period_sql = adapt_query("""
                SELECT NCS_CD, COUNT(*) as cnt
                FROM TB_MARKET_TREND
                WHERE YEAR_MONTH > ? AND YEAR_MONTH <= ? AND YEAR_MONTH IS NOT NULL
                GROUP BY NCS_CD HAVING COUNT(*) >= 3
            """)
            cursor.execute(period_sql, [start_ym, mid_ym])
            prev_dict = {str(r[0]): int(r[1]) for r in cursor.fetchall()}

            cursor.execute(period_sql, [mid_ym, max_ym_str])
            growth_rows = []
            for r in cursor.fetchall():
                ncs = str(r[0])
                recent_cnt = int(r[1])
                prev_cnt = prev_dict.get(ncs, 0)
                growth_rate = round(
                    (recent_cnt - prev_cnt) / (prev_cnt if prev_cnt > 0 else 1) * 100, 1
                )
                growth_rows.append({
                    'NCS_CD': ncs,
                    '최근6개월': recent_cnt,
                    '이전6개월': prev_cnt,
                    '증가율(%)': growth_rate,
                })
            growth_rows.sort(key=lambda x: x['증가율(%)'], reverse=True)

            data_json = json.dumps(growth_rows, ensure_ascii=False, default=str)
            cursor.execute(upsert_sql, (CacheKey.NCS_GROWTH, data_json))
            conn.commit()
            logger.info(f"[캐시] ncs_growth: {len(growth_rows)}행 저장")
            saved_count += 1
        except Exception as e:
            logger.error(f"[캐시] ncs_growth 실패: {e}")
            if is_pg():
                conn.rollback()

    conn.close()
    logger.info(f"[집계 캐시] {saved_count}개 집계 완료")


# ==========================================
# 6. 메인 실행
# ==========================================
def main():
    init_all_tables()
    _t0 = time.monotonic()
    start_date, end_date = get_collect_range()
    logger.info(f"[Market ETL] 수집 기간: {start_date} ~ {end_date}")

    months = list(month_shards(start_date, end_date))
    total_saved = 0
    success_months, failed_months = 0, 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futs = {ex.submit(collect_one_month, i, m_start, m_end): i
                for i, (m_start, m_end) in enumerate(months, start=1)}

        for fut in as_completed(futs):
            month_idx = futs[fut]
            try:
                rows = fut.result()
                if rows:
                    saved = save_rows(rows)
                    total_saved += saved
                    logger.info(f"{saved:,}건 저장 (누적: {total_saved:,}건)")
                success_months += 1
            except Exception as e:
                failed_months += 1
                logger.error(f"[M{month_idx:02d}] 수집 실패: {e}")

    logger.info(f"[Summary] 월별 성공: {success_months}, 실패: {failed_months}, 총 저장: {total_saved:,}건")
    if total_saved == 0:
        logger.warning("수집된 데이터가 없습니다.")
    else:
        logger.info(f"[Success] 총 {total_saved:,}건 저장 완료!")

    logger.info(f"총 소요: {time.monotonic() - _t0:.1f}초")
    compute_and_cache_aggregations()

if __name__ == "__main__":
    main()
