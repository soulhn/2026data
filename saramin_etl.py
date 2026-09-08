"""사람인 채용공고 ETL — 과정별 취업 방향(트랙) 기준 채용공고 수집 + 트랙 태깅 + 캐시 집계

수집(직무 코드·키워드 쿼리)과 분류(트랙 규칙)는 분리돼 있다. 어떤 쿼리로 들어왔든
`tag_tracks()`가 모든 보유 공고를 같은 규칙으로 다시 태깅한다 (설계: docs/track_job_mapping.md).
"""
import json
import logging
import os
import re
import time
import datetime as dt
from collections import Counter

from dotenv import load_dotenv

from utils import get_connection, get_retry_session, adapt_query, is_pg
from init_db import init_all_tables
from config import (
    SARAMIN_PAGE_SIZE, SARAMIN_API_CALL_LIMIT,
    SARAMIN_QUERIES, SARAMIN_PUBLISHED_DAYS,
    SARAMIN_RETENTION_EXPIRED_DAYS, SARAMIN_EVERGREEN_MIN_AHEAD_DAYS,
    SARAMIN_RETENTION_EVERGREEN_DAYS,
    SARAMIN_TRACK_RULES, SARAMIN_TRACK_ORDER, SARAMIN_ENTRY_LEVEL_CODES,
    SARAMIN_EXCLUDE_JOB_TYPES, SARAMIN_EXCLUDE_TITLE_RE, SARAMIN_CODE_ONLY_MIN_SCORE,
    SARAMIN_CODE_ONLY_TITLE_RE,
    ETL_BATCH_SIZE, CacheKey,
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
    """지역코드에서 대표(첫 번째) 지역명 추출."""
    if not loc_cd:
        return None
    code_prefix = str(loc_cd).split(',')[0][:3]
    return LOC_CODE_TO_REGION.get(code_prefix)


def _extract_regions(loc_cd):
    """지역코드에서 모든 지역명 추출 (다중 지역 지원)."""
    if not loc_cd:
        return []
    regions = []
    for code in str(loc_cd).split(','):
        prefix = code.strip()[:3]
        region = LOC_CODE_TO_REGION.get(prefix)
        if region and region not in regions:
            regions.append(region)
    return regions


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


def _published_range(days):
    """SARAMIN_PUBLISHED_DAYS → (published_min, published_max) unix timestamp 쌍."""
    now = dt.datetime.now()
    end = now.replace(hour=23, minute=59, second=59)
    start = (now - dt.timedelta(days=days)).replace(hour=0, minute=0, second=0)
    return str(int(start.timestamp())), str(int(end.timestamp()))


def _daily_ranges(days):
    """SARAMIN_PUBLISHED_DAYS를 1일 단위로 분할하여 (min, max) 쌍 리스트 반환."""
    now = dt.datetime.now()
    ranges = []
    for d in range(days, 0, -1):
        day_start = (now - dt.timedelta(days=d)).replace(hour=0, minute=0, second=0)
        day_end = (now - dt.timedelta(days=d)).replace(hour=23, minute=59, second=59)
        ranges.append((str(int(day_start.timestamp())), str(int(day_end.timestamp()))))
    # 오늘
    today_start = now.replace(hour=0, minute=0, second=0)
    today_end = now.replace(hour=23, minute=59, second=59)
    ranges.append((str(int(today_start.timestamp())), str(int(today_end.timestamp()))))
    return ranges


def build_query_params(query, pub_min, pub_max):
    """SARAMIN_QUERIES 항목 → API 요청 파라미터. `keywords` 또는 `job_cd` 중 있는 것만 싣는다."""
    params = {
        'access-key': API_KEY,
        'count': str(SARAMIN_PAGE_SIZE),
        'start': '0',
        'sort': 'pd',
        'published_min': pub_min,
        'published_max': pub_max,
    }
    if query.get('keywords'):
        params['keywords'] = query['keywords']
    if query.get('job_cd'):
        params['job_cd'] = query['job_cd']
    return params


def collect_query(session, query, api_call_count):
    """수집 쿼리 하나(키워드 또는 직무 코드)로 채용공고 수집. 1일 단위 분할로 110건 한계 대응.

    사람인 API는 1회 호출당 최대 110건만 반환하며 페이징을 지원하지 않음.
    SARAMIN_PUBLISHED_DAYS 기간을 1일씩 나누어 호출하면 수집량이 N배 증가.
    """
    label = query['label']
    all_extended = []
    truncated_days = 0

    for pub_min, pub_max in _daily_ranges(SARAMIN_PUBLISHED_DAYS):
        if api_call_count >= SARAMIN_API_CALL_LIMIT:
            logger.warning(f"API 호출 한도 도달 ({api_call_count}회). 수집 조기 종료.")
            break

        params = build_query_params(query, pub_min, pub_max)

        try:
            resp = session.get(BASE_URL, params=params, timeout=30)
            api_call_count += 1
            resp.raise_for_status()
        except Exception as e:
            logger.error(f"[{label}] 요청 실패: {e}")
            continue

        try:
            data = resp.json()
        except Exception:
            preview = resp.content[:500].decode('utf-8', errors='replace')
            logger.warning(f"[{label}] JSON 파싱 실패. 응답 미리보기: {preview}")
            continue

        rows, total = parse_jobs_json(data)
        if not rows:
            continue

        if len(rows) >= SARAMIN_PAGE_SIZE:
            truncated_days += 1

        for r in rows:
            _ym_src = r[24] or r[26] or r[27]  # POSTING_DT or OPENING_DT or MODIFICATION_DT
            ym = _ym_src[:7] if _ym_src and len(_ym_src) >= 7 else None
            rgn = _extract_region(r[10])
            all_extended.append(r + (label, ym, rgn))

    if all_extended:
        logger.info(f"[{label}] {len(all_extended)}건 수집 ({SARAMIN_PUBLISHED_DAYS + 1}일 분할)")
    if truncated_days:
        logger.warning(
            f"[{label}] {truncated_days}일이 PAGE_SIZE({SARAMIN_PAGE_SIZE})에 도달 — "
            f"수집 쿼리 세분화를 검토하세요."
        )

    return api_call_count, all_extended


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
        YEAR_MONTH=excluded.YEAR_MONTH,
        REGION=excluded.REGION,
        COLLECTED_AT=CURRENT_TIMESTAMP
'''


_KW_INSERT_RAW = '''
    INSERT OR IGNORE INTO TB_JOB_POSTING_KEYWORD (JOB_ID, SEARCH_KEYWORD)
    VALUES (?, ?)
'''

_RGN_INSERT_RAW = '''
    INSERT OR IGNORE INTO TB_JOB_POSTING_REGION (JOB_ID, REGION)
    VALUES (?, ?)
'''


def save_keyword_mappings(rows):
    """수집된 rows에서 (JOB_ID, SEARCH_KEYWORD) 쌍을 junction 테이블에 저장."""
    if not rows:
        return 0
    pairs = list({(r[0], r[30]) for r in rows})
    conn = get_connection()
    cursor = conn.cursor()
    kw_query = adapt_query(_KW_INSERT_RAW)
    try:
        if is_pg():
            from psycopg2.extras import execute_batch
            execute_batch(cursor, kw_query, pairs, page_size=100)
        else:
            cursor.executemany(kw_query, pairs)
        conn.commit()
        return len(pairs)
    except Exception as e:
        logger.error(f"키워드 매핑 저장 중 오류: {e}")
        return 0
    finally:
        conn.close()


def save_region_mappings(rows):
    """수집된 rows에서 (JOB_ID, REGION) 쌍을 junction 테이블에 저장 (다중 지역)."""
    if not rows:
        return 0
    pairs = []
    for r in rows:
        job_id = r[0]
        loc_cd = r[10]  # LOC_CD
        for region in _extract_regions(loc_cd):
            pairs.append((job_id, region))
    if not pairs:
        return 0
    pairs = list(set(pairs))
    conn = get_connection()
    cursor = conn.cursor()
    rgn_query = adapt_query(_RGN_INSERT_RAW)
    try:
        if is_pg():
            from psycopg2.extras import execute_batch
            execute_batch(cursor, rgn_query, pairs, page_size=100)
        else:
            cursor.executemany(rgn_query, pairs)
        conn.commit()
        return len(pairs)
    except Exception as e:
        logger.error(f"지역 매핑 저장 중 오류: {e}")
        return 0
    finally:
        conn.close()


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


# ── 트랙 분류 ──
_COMPILED_RULES = {
    track: {
        'strong_codes': rule['strong_codes'],
        'weak_codes': rule['weak_codes'],
        'strong_re': re.compile(rule['strong_re']) if rule['strong_re'] else None,
        'weak_re': re.compile(rule['weak_re']) if rule['weak_re'] else None,
        'threshold': rule['threshold'],
        'entry_only': rule.get('entry_only', False),
    }
    for track, rule in SARAMIN_TRACK_RULES.items()
}
_EXCLUDE_TITLE_RE = re.compile(SARAMIN_EXCLUDE_TITLE_RE)
_CODE_ONLY_TITLE_RE = re.compile(SARAMIN_CODE_ONLY_TITLE_RE)


def _split_codes(value):
    return {c.strip() for c in str(value or '').split(',') if c.strip()}


def classify_posting(title, keyword, job_cd, job_mid_cd, job_type_cd, experience_cd):
    """공고 하나를 트랙별로 채점한다. 반환: {TRACK: (score, match_source, entry_level)}.

    강한 신호 3점(코드 하나·정규식 매치 1회), 보조 신호 1점, threshold 이상이면 태깅.
    단, 제목·키워드 근거 없이 코드만으로는 SARAMIN_CODE_ONLY_MIN_SCORE 이상이어야 한다.
    IT개발·데이터(상위 직무 2)가 아니거나 알바·파견·교육생, 강사·헤드헌팅 등 제외 제목은 제외.
    """
    mid_codes = _split_codes(job_mid_cd)
    if '2' not in mid_codes:
        return {}
    if str(job_type_cd or '') in SARAMIN_EXCLUDE_JOB_TYPES:
        return {}
    title = title or ''
    if _EXCLUDE_TITLE_RE.search(title):
        return {}

    codes = _split_codes(job_cd)
    text = f"{title} {keyword or ''}"
    entry_level = 1 if str(experience_cd or '') in SARAMIN_ENTRY_LEVEL_CODES else 0
    title_has_role = bool(_CODE_ONLY_TITLE_RE.search(title))

    result = {}
    for track, rule in _COMPILED_RULES.items():
        if rule['entry_only'] and not entry_level:
            continue
        strong_code_hits = len(codes & rule['strong_codes'])
        code_score = 3 * strong_code_hits + len(codes & rule['weak_codes'])
        text_score = 0
        if rule['strong_re'] and rule['strong_re'].search(text):
            text_score += 3
        if rule['weak_re'] and rule['weak_re'].search(text):
            text_score += 1
        score = code_score + text_score
        if score < rule['threshold']:
            continue
        if not text_score and (not strong_code_hits or code_score < SARAMIN_CODE_ONLY_MIN_SCORE
                               or not title_has_role):
            continue
        if code_score and text_score:
            source = 'both'
        elif code_score:
            source = 'code'
        else:
            source = 'keyword'
        result[track] = (score, source, entry_level)
    return result


_TRACK_INSERT_RAW = '''
    INSERT INTO TB_JOB_POSTING_TRACK (JOB_ID, TRACK, SCORE, MATCH_SOURCE, ENTRY_LEVEL)
    VALUES (?, ?, ?, ?, ?)
'''


def tag_tracks():
    """보유 공고 전량을 트랙 규칙으로 다시 태깅한다 (전량 삭제 후 재생성 — 규칙 변경 시 소급 반영)."""
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(adapt_query(
            "SELECT JOB_ID, POSITION_TITLE, KEYWORD, JOB_CD, JOB_MID_CD, JOB_TYPE_CD, EXPERIENCE_CD "
            "FROM TB_JOB_POSTING"
        ))
        postings = cursor.fetchall()
        tags = []
        for job_id, title, keyword, job_cd, job_mid_cd, job_type_cd, exp_cd in postings:
            for track, (score, source, entry) in classify_posting(
                    title, keyword, job_cd, job_mid_cd, job_type_cd, exp_cd).items():
                tags.append((job_id, track, score, source, entry))

        cursor.execute("DELETE FROM TB_JOB_POSTING_TRACK")
        insert_sql = adapt_query(_TRACK_INSERT_RAW)
        if is_pg():
            from psycopg2.extras import execute_batch
            for i in range(0, len(tags), ETL_BATCH_SIZE):
                execute_batch(cursor, insert_sql, tags[i:i + ETL_BATCH_SIZE], page_size=200)
        else:
            cursor.executemany(insert_sql, tags)
        conn.commit()

        counts = Counter(t[1] for t in tags)
        entry_counts = Counter(t[1] for t in tags if t[4])
        summary = ", ".join(
            f"{tr} {counts.get(tr, 0):,}건(신입가능 {entry_counts.get(tr, 0):,})"
            for tr in SARAMIN_TRACK_ORDER
        )
        logger.info(f"[트랙 태깅] 공고 {len(postings):,}건 → {summary}")
        return dict(counts)
    except Exception as e:
        logger.error(f"[트랙 태깅] 실패: {e}")
        if is_pg():
            conn.rollback()
        return {}
    finally:
        conn.close()


# ── 누적 캐시 병합 ──

# 시계열 캐시는 전체 재계산이 아니라 누적 병합한다. 보존 정책이 옛 원본을 지우면
# 재계산 값이 실제보다 작아지므로, 과거 월은 캐시에 남은 값을 지켜야 추이가 유지된다.
CUMULATIVE_CACHE_KEYS = {
    CacheKey.SARAMIN_TRACK_MONTHLY: ("TRACK", "YEAR_MONTH"),
}

# 2026-09 트랙 개편 이전 캐시 키 — 페이지가 더 이상 읽지 않으므로 집계 때 정리한다
_LEGACY_CACHE_KEYS = (
    "saramin_kpi", "saramin_monthly", "saramin_job_cd", "saramin_loc",
    "saramin_keyword_trend", "saramin_keyword_dist", "saramin_active_loc",
    "saramin_active_job_cd", "saramin_expired_monthly", "saramin_expired_job_cd",
    "saramin_posting_duration",
)


def merge_cumulative(existing_rows, fresh_rows, key_cols):
    """월별 카운트 캐시를 누적 병합한다.

    같은 키(월)는 큰 CNT를 채택한다 — 월 카운트는 수집 직후 며칠만 늘고(게시일 3일 창)
    이후에는 보존 삭제로 줄기만 하므로, max가 곧 그 월의 완전한 값이다.
    날짜 비교가 없어 월 경계·실행 순서와 무관하게 안전하다.
    주의: 원본을 의도적으로 정정(오염 데이터 삭제 등)해도 캐시 값은 남는다 → 그때는 캐시를 수동 삭제.
    """
    merged = {tuple(r.get(c) for c in key_cols): r for r in existing_rows}
    for r in fresh_rows:
        k = tuple(r.get(c) for c in key_cols)
        if k not in merged or (r.get("CNT") or 0) > (merged[k].get("CNT") or 0):
            merged[k] = r
    return sorted(merged.values(), key=lambda r: tuple(str(r.get(c)) for c in key_cols))


# ── 보존 정책 ──
def cleanup_old_postings():
    """보존 기간이 지난 공고를 삭제한다 (Supabase 500MB 한도 대응).

    - 일반 공고: 마감 후 SARAMIN_RETENTION_EXPIRED_DAYS 경과 시 삭제
    - 상시채용(마감일이 1년 이상 먼 미래): 마감 기준으로는 영원히 안 지워지므로
      게시 후 SARAMIN_RETENTION_EVERGREEN_DAYS 경과 시 삭제
    날짜가 'YYYY-MM-DD' 문자열이라 커트오프를 파이썬에서 계산해 문자열 비교만 한다
    (PG/SQLite 공통 문법 — 엔진 분기 불필요).
    """
    today = dt.date.today()
    expired_cutoff = (today - dt.timedelta(days=SARAMIN_RETENTION_EXPIRED_DAYS)).isoformat()
    evergreen_horizon = (today + dt.timedelta(days=SARAMIN_EVERGREEN_MIN_AHEAD_DAYS)).isoformat()
    posted_cutoff = (today - dt.timedelta(days=SARAMIN_RETENTION_EVERGREEN_DAYS)).isoformat()

    doomed_where = (
        "((EXPIRATION_DT IS NOT NULL AND EXPIRATION_DT < ?) "
        "OR (EXPIRATION_DT > ? AND POSTING_DT IS NOT NULL AND POSTING_DT < ?))"
    )
    params = (expired_cutoff, evergreen_horizon, posted_cutoff)

    conn = get_connection()
    cursor = conn.cursor()
    try:
        # junction 먼저 삭제해야 고아 행이 안 남는다 (FK 미설정)
        deleted = {}
        for table in ("TB_JOB_POSTING_KEYWORD", "TB_JOB_POSTING_REGION", "TB_JOB_POSTING_TRACK"):
            cursor.execute(adapt_query(
                f"DELETE FROM {table} WHERE JOB_ID IN "
                f"(SELECT JOB_ID FROM TB_JOB_POSTING WHERE {doomed_where})"
            ), params)
            deleted[table] = cursor.rowcount
        cursor.execute(adapt_query(
            f"DELETE FROM TB_JOB_POSTING WHERE {doomed_where}"
        ), params)
        deleted["TB_JOB_POSTING"] = cursor.rowcount
        conn.commit()
        logger.info(
            f"[보존 정책] 마감<{expired_cutoff} 또는 상시채용 게시<{posted_cutoff} 삭제: "
            + ", ".join(f"{t} {n}건" for t, n in deleted.items())
        )
        return deleted
    except Exception as e:
        logger.error(f"[보존 정책] 삭제 실패: {e}")
        if is_pg():
            conn.rollback()
        return {}
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
            if key in CUMULATIVE_CACHE_KEYS:
                # 보존 삭제로 옛 원본이 사라져도 과거 월 추이는 기존 캐시 값으로 유지
                cursor.execute(
                    adapt_query("SELECT CACHE_DATA FROM TB_MARKET_CACHE WHERE CACHE_KEY = ?"),
                    (key,),
                )
                prev = cursor.fetchone()
                existing = json.loads(prev[0]) if prev and prev[0] else []
                rows = merge_cumulative(existing, rows, CUMULATIVE_CACHE_KEYS[key])
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

    # 진행중 분포·목록은 페이지가 PG에서 직접 조회한다(트랙·신입 필터 조합이 많아 캐시 부적합).
    # 캐시는 보존 삭제 후에도 남아야 하는 월별 추이(누적 병합)와 수집 쿼리별 보유 현황만 담는다.
    # '전체'(ALL)는 어느 트랙이든 붙은 공고의 중복 제거 합.
    aggs = [
        (CacheKey.SARAMIN_TRACK_MONTHLY, adapt_query("""
            SELECT t.TRACK AS TRACK, jp.YEAR_MONTH AS YEAR_MONTH, COUNT(*) AS CNT,
                   COALESCE(SUM(t.ENTRY_LEVEL), 0) AS ENTRY_CNT
            FROM TB_JOB_POSTING_TRACK t JOIN TB_JOB_POSTING jp ON t.JOB_ID = jp.JOB_ID
            WHERE jp.YEAR_MONTH IS NOT NULL
            GROUP BY t.TRACK, jp.YEAR_MONTH
            UNION ALL
            SELECT 'ALL' AS TRACK, jp.YEAR_MONTH AS YEAR_MONTH, COUNT(*) AS CNT,
                   COALESCE(SUM(CASE WHEN jp.JOB_ID IN
                        (SELECT JOB_ID FROM TB_JOB_POSTING_TRACK WHERE ENTRY_LEVEL = 1)
                        THEN 1 ELSE 0 END), 0) AS ENTRY_CNT
            FROM TB_JOB_POSTING jp
            WHERE jp.JOB_ID IN (SELECT JOB_ID FROM TB_JOB_POSTING_TRACK)
              AND jp.YEAR_MONTH IS NOT NULL
            GROUP BY jp.YEAR_MONTH
        """)),
        (CacheKey.SARAMIN_QUERY_HITS, adapt_query("""
            SELECT SEARCH_KEYWORD, COUNT(*) AS CNT
            FROM TB_JOB_POSTING_KEYWORD
            GROUP BY SEARCH_KEYWORD ORDER BY CNT DESC
        """)),
    ]

    saved = sum(run_agg(key, sql) for key, sql in aggs)

    try:
        placeholders = ",".join("?" for _ in _LEGACY_CACHE_KEYS)
        cursor.execute(adapt_query(
            f"DELETE FROM TB_MARKET_CACHE WHERE CACHE_KEY IN ({placeholders})"), _LEGACY_CACHE_KEYS)
        conn.commit()
    except Exception as e:
        logger.warning(f"[캐시] 구 캐시 정리 실패 (무시): {e}")
        if is_pg():
            conn.rollback()

    conn.close()
    logger.info(f"[집계 캐시] {saved}개 집계 완료")


# ── 메인 ──
def main():
    init_all_tables()
    _t0 = time.monotonic()

    if not API_KEY:
        logger.error("SARAMIN_API_KEY가 설정되지 않았습니다. ETL 종료.")
        return

    daily_calls = SARAMIN_PUBLISHED_DAYS + 1
    planned = len(SARAMIN_QUERIES) * daily_calls
    logger.info(
        f"[설정] published={SARAMIN_PUBLISHED_DAYS}일 (1일 단위 분할, "
        f"쿼리당 {daily_calls}회), 수집 쿼리 {len(SARAMIN_QUERIES)}개 → 예정 호출 {planned}회 "
        f"(한도 {SARAMIN_API_CALL_LIMIT}), 최대 {SARAMIN_PAGE_SIZE}건/호출"
    )
    session = get_retry_session()
    api_call_count = 0
    total_saved = 0

    for query in SARAMIN_QUERIES:
        if api_call_count >= SARAMIN_API_CALL_LIMIT:
            logger.warning("API 호출 한도 도달. 남은 수집 쿼리 건너뜀.")
            break

        api_call_count, rows = collect_query(session, query, api_call_count)
        if rows:
            saved = save_rows(rows)
            save_keyword_mappings(rows)
            save_region_mappings(rows)
            total_saved += saved
            logger.info(f"[{query['label']}] {saved}건 저장 (누적: {total_saved:,}건)")

    logger.info(f"[Summary] API 호출: {api_call_count}회, 총 저장: {total_saved:,}건")
    logger.info(f"총 소요: {time.monotonic() - _t0:.1f}초")
    # 순서 중요: 삭제 → 태깅 → 집계. 과거 월 추이는 누적 병합(merge_cumulative)이 지켜준다
    cleanup_old_postings()
    tag_tracks()
    compute_and_cache_aggregations()


def cleanup_only():
    """API 수집 없이 보존 삭제 + 트랙 태깅 + 캐시 재집계만 수행 (일일 API 쿼터 소모 없음)."""
    init_all_tables()
    cleanup_old_postings()
    tag_tracks()
    compute_and_cache_aggregations()


def tag_only():
    """API 수집·삭제 없이 트랙 태깅 + 캐시 재집계만 수행 (규칙 조정 후 소급 반영용)."""
    init_all_tables()
    tag_tracks()
    compute_and_cache_aggregations()


if __name__ == "__main__":
    import sys
    if "--cleanup-only" in sys.argv:
        cleanup_only()
    elif "--tag-only" in sys.argv:
        tag_only()
    else:
        main()
