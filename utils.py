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
    """SQL 쿼리를 받아 Pandas DataFrame으로 반환합니다."""
    conn = get_connection()
    try:
        df = pd.read_sql(adapt_query(query), conn, params=params)
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