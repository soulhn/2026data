# utils.py
import sqlite3
import hmac
import re
import pandas as pd
import os
from datetime import datetime
from dotenv import load_dotenv

# .env 파일 로드
load_dotenv()

# ✅ [핵심] DB 설정 중앙화: 모든 파일이 이 변수를 가져다 씁니다.
DB_FILE = "hrd_analysis.db"

def get_database_url():
    """os.getenv → st.secrets 순으로 DATABASE_URL을 매번 동적 탐색"""
    url = os.getenv("DATABASE_URL")
    if url:
        return url
    try:
        import streamlit as st
        return st.secrets.get("DATABASE_URL")
    except Exception:
        return None


def is_pg():
    """현재 PostgreSQL 모드인지 반환"""
    return get_database_url() is not None


def adapt_query(sql):
    """SQLite 쿼리를 현재 DB 엔진에 맞게 변환"""
    if not is_pg():
        return sql
    # ? → %s
    sql = sql.replace("?", "%s")
    # INSERT OR IGNORE INTO → INSERT INTO ... (+ ON CONFLICT DO NOTHING 자동 추가)
    if re.search(r"INSERT\s+OR\s+IGNORE\s+INTO", sql, re.IGNORECASE):
        sql = re.sub(
            r"INSERT\s+OR\s+IGNORE\s+INTO",
            "INSERT INTO",
            sql,
            flags=re.IGNORECASE,
        )
        # ON CONFLICT DO NOTHING 이 아직 없으면 VALUES(...) 뒤에 추가
        if "ON CONFLICT" not in sql.upper():
            sql = sql.rstrip().rstrip(";")
            sql += " ON CONFLICT DO NOTHING"
    return sql

def check_password():
    """Streamlit 비밀번호 인증. 인증 실패 시 st.stop()으로 앱을 중단합니다."""
    import streamlit as st

    if st.session_state.get("authenticated"):
        return True

    pwd = st.text_input("비밀번호를 입력하세요", type="password")
    if pwd and hmac.compare_digest(pwd, st.secrets.passwords.admin):
        st.session_state.authenticated = True
        st.rerun()
    elif pwd:
        st.error("비밀번호가 틀렸습니다")

    st.stop()

def _get_pg_pool():
    """PG 읽기 전용 커넥션을 캐싱하여 반환 (Streamlit 환경에서만 캐싱)."""
    try:
        import streamlit as st
        return _get_pg_pool_cached()
    except Exception:
        # Streamlit 없는 환경 (ETL, 테스트 등) → 일반 커넥션
        return None


def _get_pg_pool_cached():
    """@st.cache_resource 로 PG 커넥션 재사용."""
    import streamlit as st

    @st.cache_resource
    def _create():
        import psycopg2
        conn = psycopg2.connect(get_database_url(), connect_timeout=5)
        conn.autocommit = True
        return conn

    return _create()


def get_connection(timeout=5, row_factory=None):
    """DB 연결 객체를 반환합니다. DATABASE_URL이 있으면 PostgreSQL, 없으면 SQLite."""
    if is_pg():
        import psycopg2
        import psycopg2.extras
        conn = psycopg2.connect(get_database_url(), connect_timeout=timeout)
        conn.autocommit = False
        if row_factory == sqlite3.Row:
            conn.cursor_factory = psycopg2.extras.RealDictCursor
        return conn
    conn = sqlite3.connect(DB_FILE, timeout=timeout)
    if row_factory:
        conn.row_factory = row_factory
    return conn

def load_data(query, params=None):
    """SQL 쿼리를 받아 Pandas DataFrame으로 반환합니다.
    PG 읽기 시 캐싱된 커넥션을 사용하여 TCP 연결 오버헤드를 줄입니다."""
    pool_conn = _get_pg_pool() if is_pg() else None
    if pool_conn is not None:
        try:
            df = pd.read_sql(adapt_query(query), pool_conn, params=params)
            df.columns = [c.upper() for c in df.columns]
            return df
        except Exception:
            # 커넥션이 끊어진 경우 폴백
            pass
    conn = get_connection()
    try:
        df = pd.read_sql(adapt_query(query), conn, params=params)
        if is_pg():
            df.columns = [c.upper() for c in df.columns]
        return df
    finally:
        conn.close()

def calculate_age_at_training(birth_date_str, training_start_date_str):
    """
    생년월일과 훈련 시작일을 기준으로 '훈련 당시 나이'를 계산합니다.
    """
    if not birth_date_str or len(str(birth_date_str)) < 4:
        return None
    
    try:
        birth_year = int(str(birth_date_str)[:4])
        if not training_start_date_str:
            target_year = datetime.now().year
        else:
            target_year = int(str(training_start_date_str)[:4])
        return target_year - birth_year + 1
    except (ValueError, TypeError):
        return None

def get_retry_session(retries=5, backoff_factor=1):
    """재시도 로직이 포함된 requests Session을 반환합니다."""
    import requests
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry

    session = requests.Session()
    retry = Retry(
        total=retries,
        backoff_factor=backoff_factor,
        status_forcelist=[500, 502, 503, 504],
        allowed_methods=["GET"]
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session

def safe_float(val, default=0.0):
    """안전한 float 변환. 'A', 'B', 'null' 등 비숫자도 처리."""
    if not val or val == "":
        return default
    try:
        return float(val)
    except (ValueError, TypeError):
        return default

def safe_int(val, default=None):
    """안전한 int 변환."""
    if not val or val == "":
        return default
    try:
        return int(val)
    except (ValueError, TypeError):
        return default


# ── 출석률 표준 계산 (매출_분석.py 기준) ──
# 출석 불인정 상태만 제외 → 나머지 전체 출석으로 인정
# (공가 종류가 다양해서 포함 방식보다 제외 방식이 정확)
NOT_ATTEND_STATUSES = frozenset({'결석', '중도탈락미출석', '100분의50미만출석'})


def _attendance_penalty(status: str) -> int:
    """지각/조퇴/외출 패널티 포인트 반환 (3개 누적 = 가상 결석 1일)."""
    if not isinstance(status, str):
        return 0
    return ('지각' in status) + ('조퇴' in status) + ('외출' in status)


def calc_attendance_rate(att_df) -> float:
    """출석률 계산 (매출_분석.py 기준).

    groupby().apply() 패턴으로 기수/학생 단위 모두 사용 가능.

    Args:
        att_df: TB_ATTENDANCE_LOG 서브셋. 컬럼: ATEND_STATUS, ATEND_DT 필수.

    Returns:
        float: 0.0~100.0 범위 출석률. 훈련일이 없으면 0.0.

    Note:
        - NOT_ATTEND_STATUSES(결석/중도탈락미출석/100분의50미만출석) 제외 방식 사용
        - 패널티: 지각+조퇴+외출 3개 누적 → 1일 차감
        - 분모: 중도탈락미출석 제외한 고유 훈련일수
    """
    if att_df.empty:
        return 0.0
    dates = pd.to_datetime(att_df['ATEND_DT'], errors='coerce').dt.date
    statuses = att_df['ATEND_STATUS']
    training_days = dates[statuses != '중도탈락미출석'].nunique()
    if training_days == 0:
        return 0.0
    present_mask = ~statuses.isin(NOT_ATTEND_STATUSES)
    base_attend = dates[present_mask].nunique()
    penalty = int(statuses[present_mask].apply(_attendance_penalty).sum())
    attend_days = max(0, base_attend - penalty // 3)
    return round(attend_days / training_days * 100, 1)


def calc_attendance_rate_from_counts(
    total: int,
    absent: int,
    dropout_absent: int,
    lt50: int,
    late: int,
    early_leave: int,
    out: int,
) -> float:
    """집계된 상태별 카운트로 출석률 계산 (매출_분석.py 기준).

    get_all_degr_attendance() 등 사전 집계 결과에 사용.

    Args:
        total:          전체 기록 수
        absent:         결석 수
        dropout_absent: 중도탈락미출석 수 (분모에서도 제외)
        lt50:           100분의50미만출석 수
        late:           지각 수 (패널티 포인트)
        early_leave:    조퇴 수 (패널티 포인트)
        out:            외출 수 (패널티 포인트)

    Returns:
        float: 0.0~100.0 범위 출석률.
    """
    training_days = total - dropout_absent
    if training_days <= 0:
        return 0.0
    base = total - absent - dropout_absent - lt50
    penalty_points = late + early_leave + out
    attend_days = max(0, base - penalty_points // 3)
    return round(attend_days / training_days * 100, 1)


def calc_employment_rate_6(ei6_val, hrd6_val):
    """6개월 총 취업률 = EI_EMPL_RATE_6 + HRD_EMPL_RATE_6 합산.

    home.py, 시장_분석.py, 종료과정_성과.py에서 각각 중복 구현하던 로직을 통일.

    Args:
        ei6_val:  EI_EMPL_RATE_6 원본값 (str/float/NaN/None 모두 수용)
        hrd6_val: HRD_EMPL_RATE_6 원본값

    Returns:
        float | pd.NA:
            - 어느 한쪽이라도 A/B/C/D 특수코드면 pd.NA
            - 둘 다 결측이면 pd.NA
            - 나머지는 float 합산값
    """
    from config import EMPL_CODE_MAP
    ei6_s = str(ei6_val).strip() if ei6_val is not None else ''
    hrd6_s = str(hrd6_val).strip() if hrd6_val is not None else ''
    if ei6_s in EMPL_CODE_MAP or hrd6_s in EMPL_CODE_MAP:
        return pd.NA
    _EMPTY = ('', 'nan', 'None', 'NaN', '<NA>')
    if ei6_s in _EMPTY and hrd6_s in _EMPTY:
        return pd.NA
    return safe_float(ei6_s) + safe_float(hrd6_s)


def parse_empl_rate(val):
    """취업률 단일 컬럼 값 파싱.

    Args:
        val: EI_EMPL_RATE_3 / EI_EMPL_RATE_6 / HRD_EMPL_RATE_6 원본값

    Returns:
        tuple: (numeric_or_None, label_or_None)
               숫자 → (float, None)
               A/B/C/D 특수코드 → (None, '진행중' 등 레이블)
               빈값/NaN → (None, None)
    """
    from config import EMPL_CODE_MAP
    s = str(val).strip() if val is not None else ''
    if s in EMPL_CODE_MAP:
        return (None, EMPL_CODE_MAP[s])
    f = safe_float(s, default=None)
    return (f, None)


def calc_recruit_rate(reg_col, fxnum_col):
    """모집률(%) 계산: (신청인원 / 정원 × 100), 정원 0이면 NA, 상한 100%"""
    return (reg_col / fxnum_col.replace(0, pd.NA) * 100).fillna(0).clip(upper=100)


def is_completed(status):
    """수료 판정: '수료' 또는 '조기취업' 포함 여부 (정확일치 금지)"""
    return status.str.contains('수료|조기취업', na=False)


def parse_time_to_minutes(t):
    """HH:MM 형태의 시간을 분으로 변환"""
    try:
        parts = str(t).split(':')
        if len(parts) == 2 and parts[0].isdigit() and parts[1].isdigit():
            return int(parts[0]) * 60 + int(parts[1])
    except Exception:
        pass
    return None


def get_billing_periods(start_date, end_date):
    """개강일 기준 월 단위 청구 기간 목록 반환.

    청구 기간은 30일 고정이 아니라 개강일과 같은 날짜 기준 월 단위:
    예) 5/19 개강 → 1단위: 5/19~6/18, 2단위: 6/19~7/18, ...

    Args:
        start_date: str(YYYY-MM-DD) 또는 date 객체 (훈련 시작일)
        end_date:   str(YYYY-MM-DD) 또는 date 객체 (훈련 종료일)

    Returns:
        list[dict]: [{'period_num':1, 'start':date, 'end':date,
                      'label':str, 'status':str}, ...]
        status: '완료' | '진행중' | '예정'
    """
    import datetime as _dt
    import calendar

    def _to_date(v):
        if isinstance(v, _dt.datetime):
            return v.date()
        if isinstance(v, _dt.date):
            return v
        return _dt.date.fromisoformat(str(v)[:10])

    def _add_one_month(d):
        """1개월 후 같은 날짜 반환 (월말 초과 시 말일로 클램프)"""
        month = d.month + 1
        year = d.year + (month - 1) // 12
        month = ((month - 1) % 12) + 1
        max_day = calendar.monthrange(year, month)[1]
        return _dt.date(year, month, min(d.day, max_day))

    start = _to_date(start_date)
    end = _to_date(end_date)
    today = _dt.date.today()

    periods = []
    period_start = start
    period_num = 1
    while period_start <= end:
        next_start = _add_one_month(period_start)
        period_end = min(next_start - _dt.timedelta(days=1), end)
        if period_end < today:
            status = "완료"
        elif period_start <= today <= period_end:
            status = "진행중"
        else:
            status = "예정"
        label = f"{period_num}단위 ({period_start.strftime('%m/%d')}~{period_end.strftime('%m/%d')})"
        periods.append({
            "period_num": period_num,
            "start": period_start,
            "end": period_end,
            "label": label,
            "status": status,
        })
        period_start = next_start
        period_num += 1
    return periods


def calc_revenue(attend_days, training_days, period_training_days=None):
    """단위기간 수강생 매출 계산.

    Args:
        attend_days:          해당 기간 출석 일수 (출석+지각+입실중, 조퇴 제외)
        training_days:        수강생 개인 훈련일수 (출석률 계산 분모)
                              중도 입과자는 실제 수강 개시일 이후 훈련일수로 계산.
        period_training_days: 단위기간 전체 훈련일수 (전액 기준; None이면 training_days 사용)
                              full_fee = period_training_days × DAILY_TRAINING_FEE

    Returns:
        tuple: (fee:int, rate:float, status:str)
        status: '전액' | '비례' | '미청구' | '해당없음'
    """
    from config import DAILY_TRAINING_FEE, REVENUE_FULL_THRESHOLD

    eff_period = period_training_days if period_training_days is not None else training_days

    if eff_period <= 0:
        return (0, 0.0, "해당없음")

    if training_days <= 0:
        return (0, 0.0, "미청구")

    rate = round(attend_days / training_days, 3)
    full_fee = eff_period * DAILY_TRAINING_FEE

    if rate >= REVENUE_FULL_THRESHOLD:
        fee = full_fee
        status = "전액"
    elif rate > 0:
        # 정수 산술로 부동소수점 오차 방지
        # int(full_fee * 0.35) = 1,016,399 (버그) vs full_fee * 350 // 1000 = 1,016,400
        rate_per_mille = round(attend_days * 1000 / training_days)
        fee = full_fee * rate_per_mille // 1000
        status = "비례"
    else:
        fee = 0
        status = "미청구"

    return (fee, rate, status)
