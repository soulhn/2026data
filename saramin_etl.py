"""사람인 채용공고 ETL — 키워드별 IT 채용공고 수집 + 캐시 집계"""
import json
import logging
import os
import time
import datetime as dt

from dotenv import load_dotenv

from utils import get_connection, get_retry_session, adapt_query, is_pg
from init_db import init_all_tables
from config import (
    SARAMIN_PAGE_SIZE, SARAMIN_MAX_PAGES, SARAMIN_API_CALL_LIMIT,
    SARAMIN_SLEEP_INTERVAL, SARAMIN_KEYWORDS, ETL_BATCH_SIZE, CacheKey,
)

logger = logging.getLogger(__name__)
logging.basicConfig(
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
    level=logging.INFO,
)

load_dotenv()

BASE_URL = "https://oapi.saramin.co.kr/job-search"
API_KEY = os.getenv("SARAMIN_API_KEY")

if not API_KEY:
    logger.warning("SARAMIN_API_KEY를 찾을 수 없습니다. ETL 실행 시 오류가 발생합니다.")

# ── 1차 지역코드 → 지역명 매핑 ──
LOC_CODE_TO_REGION = {
    '101': '서울', '102': '경기', '103': '광주', '104': '대구',
    '105': '대전', '106': '부산', '107': '울산', '108': '인천',
    '109': '강원', '110': '경남', '111': '경북', '112': '전남',
    '113': '전북', '114': '충북', '115': '충남', '116': '제주',
    '117': '세종', '118': '전국',
}


def _ts_to_date(ts_val):
    """Unix timestamp (int/str) → YYYY-MM-DD. 실패 시 None."""
    if not ts_val:
        return None
    try:
        return dt.datetime.fromtimestamp(int(ts_val)).strftime('%Y-%m-%d')
    except (ValueError, TypeError, OSError):
        return None


def _extract_region(loc_cd):
    """지역코드에서 1차 지역명 추출."""
    if not loc_cd:
        return None
    code_prefix = str(loc_cd).split(',')[0][:3]
    return LOC_CODE_TO_REGION.get(code_prefix)


def _safe_int(val):
    """안전한 int 변환."""
    if val is None or val == '':
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        return None


def _get_nested(d, *keys, default=''):
    """중첩 dict에서 안전하게 값 추출."""
    for k in keys:
        if not isinstance(d, dict):
            return default
        d = d.get(k, default)
    return d if d is not None else default


def parse_jobs_json(data):
    """JSON 응답에서 채용공고 목록을 파싱합니다."""
    if isinstance(data, (bytes, str)):
        data = json.loads(data)

    jobs_wrapper = data.get('jobs', {})
    total = int(jobs_wrapper.get('total', 0))
    job_list = jobs_wrapper.get('job', [])

    if not job_list:
        return [], total

    rows = []
    for job in job_list:
        job_id = str(job.get('id', ''))
        active = int(job.get('active', 0))
        url = job.get('url', '')

        company_nm = _get_nested(job, 'company', 'detail', 'name')
        position = job.get('position', {})
        title = position.get('title', '')

        industry = position.get('industry', {})
        ind_cd = str(industry.get('code', ''))
        ind_nm = industry.get('name', '')

        location = position.get('location', {})
        loc_cd = str(location.get('code', ''))
        loc_nm = location.get('name', '')

        job_type = position.get('job-type', {})
        job_type_cd = str(job_type.get('code', ''))
        job_type_nm = job_type.get('name', '')

        job_mid = position.get('job-mid-code', {})
        job_mid_cd = str(job_mid.get('code', ''))
        job_mid_nm = job_mid.get('name', '')

        job_code = position.get('job-code', {})
        job_cd = str(job_code.get('code', ''))
        job_nm = job_code.get('name', '')

        exp = position.get('experience-level', {})
        exp_cd = str(exp.get('code', ''))
        exp_min = _safe_int(exp.get('min'))
        exp_max = _safe_int(exp.get('max'))
        exp_nm = exp.get('name', '')

        edu = position.get('required-education-level', {})
        edu_cd = str(edu.get('code', ''))
        edu_nm = edu.get('name', '')

        keyword = job.get('keyword', '')

        salary = job.get('salary', {})
        salary_cd = str(salary.get('code', ''))
        salary_nm = salary.get('name', '')

        close_type = job.get('close-type', {})
        close_cd = str(close_type.get('code', ''))
        close_nm = close_type.get('name', '')

        posting_dt = _ts_to_date(job.get('posting-timestamp'))
        expiration_dt = _ts_to_date(job.get('expiration-timestamp'))
        opening_dt = _ts_to_date(job.get('opening-timestamp'))
        modification_dt = _ts_to_date(job.get('modification-timestamp'))

        year_month = posting_dt[:7] if posting_dt and len(posting_dt) >= 7 else None
        region = _extract_region(loc_cd)

        rows.append((
            job_id, active, company_nm, title,
            ind_cd, ind_nm, job_mid_cd, job_mid_nm, job_cd, job_nm,
            loc_cd, loc_nm, job_type_cd, job_type_nm,
            edu_cd, edu_nm, exp_cd, exp_min, exp_max, exp_nm,
            salary_cd, salary_nm, close_cd, close_nm,
            posting_dt, expiration_dt, opening_dt, modification_dt,
            keyword, url,
        ))

    return rows, total


def collect_keyword(session, keyword, api_call_count):
    """단일 키워드로 채용공고 수집. (api_call_count, rows) 반환."""
    all_rows = []
    for page in range(SARAMIN_MAX_PAGES):
        if api_call_count >= SARAMIN_API_CALL_LIMIT:
            logger.warning(f"API 호출 한도 도달 ({api_call_count}회). 수집 조기 종료.")
            break

        params = {
            'access-key': API_KEY,
            'keywords': keyword,
            'count': str(SARAMIN_PAGE_SIZE),
            'start': str(page * SARAMIN_PAGE_SIZE),
            'sort': 'pd',
            'published': '1',
        }

        try:
            resp = session.get(BASE_URL, params=params, timeout=30)
            api_call_count += 1
            resp.raise_for_status()
        except Exception as e:
            logger.error(f"[{keyword}] 페이지 {page} 요청 실패: {e}")
            break

        try:
            data = resp.json()
        except Exception:
            preview = resp.content[:500].decode('utf-8', errors='replace')
            logger.warning(f"[{keyword}] JSON 파싱 실패. 응답 미리보기: {preview}")
            break

        rows, total = parse_jobs_json(data)
        if not rows:
            break

        # search_keyword, year_month, region 추가
        extended = []
        for r in rows:
            posting_dt = r[24]
            ym = posting_dt[:7] if posting_dt and len(posting_dt) >= 7 else None
            rgn = _extract_region(r[10])
            extended.append(r + (keyword, ym, rgn))
        all_rows.extend(extended)

        logger.info(f"[{keyword}] 페이지 {page}: {len(rows)}건 (전체 {total}건)")

        # 더 이상 데이터 없으면 종료
        if (page + 1) * SARAMIN_PAGE_SIZE >= total:
            break

        time.sleep(SARAMIN_SLEEP_INTERVAL)

    return api_call_count, all_rows


# ── DB 저장 ──
_UPSERT_QUERY_RAW = '''
    INSERT INTO TB_JOB_POSTING (
        JOB_ID, ACTIVE, COMPANY_NM, POSITION_TITLE,
        IND_CD, IND_NM, JOB_MID_CD, JOB_MID_NM, JOB_CD, JOB_NM,
        LOC_CD, LOC_NM, JOB_TYPE_CD, JOB_TYPE_NM,
        EDU_LV_CD, EDU_LV_NM, EXPERIENCE_CD, EXPERIENCE_MIN, EXPERIENCE_MAX, EXPERIENCE_NM,
        SALARY_CD, SALARY_NM, CLOSE_TYPE_CD, CLOSE_TYPE_NM,
        POSTING_DT, EXPIRATION_DT, OPENING_DT, MODIFICATION_DT,
        KEYWORD, POSITION_URL,
        SEARCH_KEYWORD, YEAR_MONTH, REGION
    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    ON CONFLICT(JOB_ID) DO UPDATE SET
        ACTIVE=excluded.ACTIVE,
        COMPANY_NM=excluded.COMPANY_NM,
        POSITION_TITLE=excluded.POSITION_TITLE,
        IND_CD=excluded.IND_CD,
        IND_NM=excluded.IND_NM,
        JOB_MID_CD=excluded.JOB_MID_CD,
        JOB_MID_NM=excluded.JOB_MID_NM,
        JOB_CD=excluded.JOB_CD,
        JOB_NM=excluded.JOB_NM,
        LOC_CD=excluded.LOC_CD,
        LOC_NM=excluded.LOC_NM,
        JOB_TYPE_CD=excluded.JOB_TYPE_CD,
        JOB_TYPE_NM=excluded.JOB_TYPE_NM,
        EDU_LV_CD=excluded.EDU_LV_CD,
        EDU_LV_NM=excluded.EDU_LV_NM,
        EXPERIENCE_CD=excluded.EXPERIENCE_CD,
        EXPERIENCE_MIN=excluded.EXPERIENCE_MIN,
        EXPERIENCE_MAX=excluded.EXPERIENCE_MAX,
        EXPERIENCE_NM=excluded.EXPERIENCE_NM,
        SALARY_CD=excluded.SALARY_CD,
        SALARY_NM=excluded.SALARY_NM,
        CLOSE_TYPE_CD=excluded.CLOSE_TYPE_CD,
        CLOSE_TYPE_NM=excluded.CLOSE_TYPE_NM,
        POSTING_DT=excluded.POSTING_DT,
        EXPIRATION_DT=excluded.EXPIRATION_DT,
        OPENING_DT=excluded.OPENING_DT,
        MODIFICATION_DT=excluded.MODIFICATION_DT,
        KEYWORD=excluded.KEYWORD,
        POSITION_URL=excluded.POSITION_URL,
        SEARCH_KEYWORD=excluded.SEARCH_KEYWORD,
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
        if is_pg():
            from psycopg2.extras import execute_batch
            for i in range(0, len(rows), ETL_BATCH_SIZE):
                batch = rows[i:i + ETL_BATCH_SIZE]
                execute_batch(cursor, upsert_query, batch, page_size=100)
                conn.commit()
        else:
            for i in range(0, len(rows), ETL_BATCH_SIZE):
                batch = rows[i:i + ETL_BATCH_SIZE]
                cursor.executemany(upsert_query, batch)
                conn.commit()
        return len(rows)
    except Exception as e:
        logger.error(f"DB 저장 중 오류: {e}")
        return 0
    finally:
        conn.close()


# ── 집계 캐시 ──
def compute_and_cache_aggregations():
    """ETL 완료 후 주요 집계를 TB_MARKET_CACHE에 저장합니다."""
    logger.info("채용 데이터 집계 시작...")
    conn = get_connection()
    cursor = conn.cursor()

    upsert_sql = adapt_query("""
        INSERT INTO TB_MARKET_CACHE (CACHE_KEY, CACHE_DATA, COMPUTED_AT)
        VALUES (?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(CACHE_KEY) DO UPDATE SET
            CACHE_DATA=excluded.CACHE_DATA,
            COMPUTED_AT=excluded.COMPUTED_AT
    """)

    def run_agg(key, sql):
        try:
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

    today_str = dt.date.today().isoformat()

    # 만료 판정: EXPIRATION_DT < 오늘 OR ACTIVE = 0
    if is_pg():
        expired_cond = f"(EXPIRATION_DT < '{today_str}' OR ACTIVE = 0)"
        active_cond = f"((EXPIRATION_DT IS NULL OR EXPIRATION_DT >= '{today_str}') AND ACTIVE = 1)"
        duration_expr = "(EXPIRATION_DT::date - POSTING_DT::date)"
        exp_month_expr = "TO_CHAR(EXPIRATION_DT::date, 'YYYY-MM')"
    else:
        expired_cond = f"(EXPIRATION_DT < '{today_str}' OR ACTIVE = 0)"
        active_cond = f"((EXPIRATION_DT IS NULL OR EXPIRATION_DT >= '{today_str}') AND ACTIVE = 1)"
        duration_expr = "(JULIANDAY(EXPIRATION_DT) - JULIANDAY(POSTING_DT))"
        exp_month_expr = "SUBSTR(EXPIRATION_DT, 1, 7)"

    aggs = [
        (CacheKey.SARAMIN_KPI, adapt_query(f"""
            SELECT COUNT(*) AS CNT,
                   COALESCE(SUM(CASE WHEN {active_cond} THEN 1 ELSE 0 END), 0) AS ACTIVE_CNT,
                   COALESCE(SUM(CASE WHEN {expired_cond} THEN 1 ELSE 0 END), 0) AS EXPIRED_CNT,
                   COUNT(DISTINCT COMPANY_NM) AS COMPANY_CNT
            FROM TB_JOB_POSTING
        """)),
        (CacheKey.SARAMIN_MONTHLY, adapt_query("""
            SELECT YEAR_MONTH, COUNT(*) AS CNT
            FROM TB_JOB_POSTING
            WHERE YEAR_MONTH IS NOT NULL
            GROUP BY YEAR_MONTH ORDER BY YEAR_MONTH
        """)),
        (CacheKey.SARAMIN_JOB_CD, adapt_query("""
            SELECT JOB_MID_NM, COUNT(*) AS CNT
            FROM TB_JOB_POSTING
            WHERE JOB_MID_NM IS NOT NULL AND JOB_MID_NM != ''
            GROUP BY JOB_MID_NM ORDER BY CNT DESC
        """)),
        (CacheKey.SARAMIN_LOC, adapt_query("""
            SELECT REGION, COUNT(*) AS CNT
            FROM TB_JOB_POSTING
            WHERE REGION IS NOT NULL
            GROUP BY REGION ORDER BY CNT DESC
        """)),
        (CacheKey.SARAMIN_KEYWORD_TREND, adapt_query("""
            SELECT SEARCH_KEYWORD, YEAR_MONTH, COUNT(*) AS CNT
            FROM TB_JOB_POSTING
            WHERE SEARCH_KEYWORD IS NOT NULL AND YEAR_MONTH IS NOT NULL
            GROUP BY SEARCH_KEYWORD, YEAR_MONTH
            ORDER BY SEARCH_KEYWORD, YEAR_MONTH
        """)),
        # ── 신규 5종: 진행중/종료 분리 집계 ──
        (CacheKey.SARAMIN_ACTIVE_LOC, adapt_query(f"""
            SELECT REGION, COUNT(*) AS CNT
            FROM TB_JOB_POSTING
            WHERE REGION IS NOT NULL AND {active_cond}
            GROUP BY REGION ORDER BY CNT DESC
        """)),
        (CacheKey.SARAMIN_ACTIVE_JOB_CD, adapt_query(f"""
            SELECT JOB_MID_NM, COUNT(*) AS CNT
            FROM TB_JOB_POSTING
            WHERE JOB_MID_NM IS NOT NULL AND JOB_MID_NM != '' AND {active_cond}
            GROUP BY JOB_MID_NM ORDER BY CNT DESC
        """)),
        (CacheKey.SARAMIN_EXPIRED_MONTHLY, adapt_query(f"""
            SELECT {exp_month_expr} AS YEAR_MONTH, COUNT(*) AS CNT
            FROM TB_JOB_POSTING
            WHERE EXPIRATION_DT IS NOT NULL AND {expired_cond}
            GROUP BY {exp_month_expr} ORDER BY {exp_month_expr}
        """)),
        (CacheKey.SARAMIN_EXPIRED_JOB_CD, adapt_query(f"""
            SELECT JOB_MID_NM, COUNT(*) AS CNT
            FROM TB_JOB_POSTING
            WHERE JOB_MID_NM IS NOT NULL AND JOB_MID_NM != '' AND {expired_cond}
            GROUP BY JOB_MID_NM ORDER BY CNT DESC
        """)),
        (CacheKey.SARAMIN_POSTING_DURATION, adapt_query(f"""
            SELECT JOB_MID_NM, AVG_DAYS, CNT FROM (
                SELECT JOB_MID_NM,
                       ROUND(AVG({duration_expr}), 1) AS AVG_DAYS,
                       COUNT(*) AS CNT
                FROM TB_JOB_POSTING
                WHERE JOB_MID_NM IS NOT NULL AND JOB_MID_NM != ''
                  AND POSTING_DT IS NOT NULL AND EXPIRATION_DT IS NOT NULL
                  AND {expired_cond}
                GROUP BY JOB_MID_NM
            ) sub WHERE CNT >= 3
            ORDER BY AVG_DAYS
        """)),
    ]

    saved = sum(run_agg(key, sql) for key, sql in aggs)
    conn.close()
    logger.info(f"[집계 캐시] {saved}개 집계 완료")


# ── 메인 ──
def main():
    init_all_tables()
    _t0 = time.monotonic()

    if not API_KEY:
        logger.error("SARAMIN_API_KEY가 설정되지 않았습니다. ETL 종료.")
        return

    session = get_retry_session()
    api_call_count = 0
    total_saved = 0

    for keyword in SARAMIN_KEYWORDS:
        if api_call_count >= SARAMIN_API_CALL_LIMIT:
            logger.warning("API 호출 한도 도달. 남은 키워드 건너뜀.")
            break

        api_call_count, rows = collect_keyword(session, keyword, api_call_count)
        if rows:
            saved = save_rows(rows)
            total_saved += saved
            logger.info(f"[{keyword}] {saved}건 저장 (누적: {total_saved:,}건)")

    logger.info(f"[Summary] API 호출: {api_call_count}회, 총 저장: {total_saved:,}건")
    logger.info(f"총 소요: {time.monotonic() - _t0:.1f}초")
    compute_and_cache_aggregations()


if __name__ == "__main__":
    main()
