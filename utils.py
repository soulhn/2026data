# utils.py
import sqlite3
import pandas as pd
import os
from datetime import datetime
from dotenv import load_dotenv

# .env 파일 로드 (API 키 등 환경변수 불러오기)
load_dotenv()

# 상수 설정
DB_FILE = "hrd_analysis.db"

def get_connection():
    """DB 연결 객체를 반환합니다."""
    return sqlite3.connect(DB_FILE)

def load_data(query):
    """SQL 쿼리를 받아 Pandas DataFrame으로 반환합니다."""
    conn = get_connection()
    try:
        df = pd.read_sql(query, conn)
        return df
    finally:
        conn.close()

def calculate_age_at_training(birth_date_str, training_start_date_str):
    """
    생년월일과 훈련 시작일을 기준으로 '훈련 당시 나이'를 계산합니다.
    (연도 기준 계산: 훈련시작연도 - 출생연도 + 1)
    """
    if not birth_date_str or len(str(birth_date_str)) < 4:
        return None
    
    try:
        birth_year = int(str(birth_date_str)[:4])
        
        # 훈련 시작일이 없으면 현재 연도 기준
        if not training_start_date_str:
            target_year = datetime.now().year
        else:
            target_year = int(str(training_start_date_str)[:4])
            
        return target_year - birth_year + 1
    except:
        return None