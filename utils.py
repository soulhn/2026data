# utils.py
import sqlite3
import pandas as pd
import os
from datetime import datetime
from dotenv import load_dotenv

# .env 파일 로드
load_dotenv()

# ✅ [핵심] DB 설정 중앙화: 모든 파일이 이 변수를 가져다 씁니다.
DB_FILE = "hrd_analysis.db"

def get_connection(timeout=5, row_factory=None):
    """DB 연결 객체를 반환합니다."""
    conn = sqlite3.connect(DB_FILE, timeout=timeout)
    if row_factory:
        conn.row_factory = row_factory
    return conn

def load_data(query, params=None):
    """SQL 쿼리를 받아 Pandas DataFrame으로 반환합니다."""
    conn = get_connection()
    try:
        df = pd.read_sql(query, conn, params=params)
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