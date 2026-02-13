"""
전체 DB 테이블 초기화 및 마이그레이션 스크립트
모든 테이블의 CREATE/ALTER를 한 곳에서 관리합니다.
"""
import sqlite3
from utils import get_connection, DB_FILE, is_pg


def init_all_tables():
    conn = get_connection(timeout=30)
    cursor = conn.cursor()

    # ==========================================
    # [내부] 과정 마스터
    # ==========================================
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS TB_COURSE_MASTER (
            TRPR_ID TEXT, TRPR_DEGR INTEGER, TRPR_NM TEXT,
            TR_STA_DT TEXT, TR_END_DT TEXT,
            TOT_TRCO INTEGER, FINI_CNT INTEGER,
            TOT_FXNUM INTEGER, TOT_PAR_MKS INTEGER, TOT_TRP_CNT INTEGER, INST_INO TEXT,
            EI_EMPL_RATE_3 TEXT, EI_EMPL_CNT_3 INTEGER, EI_EMPL_RATE_6 TEXT, EI_EMPL_CNT_6 INTEGER,
            HRD_EMPL_RATE_6 TEXT, HRD_EMPL_CNT_6 INTEGER, REAL_EMPL_RATE REAL,
            COLLECTED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (TRPR_ID, TRPR_DEGR)
        )
    ''')

    # ==========================================
    # [내부] 훈련생 정보
    # ==========================================
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS TB_TRAINEE_INFO (
            TRPR_ID TEXT, TRPR_DEGR INTEGER, TRNEE_ID TEXT, TRNEE_NM TEXT,
            TRNEE_STATUS TEXT, TRNEE_TYPE TEXT, BIRTH_DATE TEXT,
            TOTAL_DAYS INTEGER, OFLHD_CNT INTEGER, VCATN_CNT INTEGER,
            ABSENT_CNT INTEGER, ATEND_CNT INTEGER,
            COLLECTED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (TRPR_ID, TRPR_DEGR, TRNEE_ID)
        )
    ''')

    # ==========================================
    # [내부] 출결 로그
    # ==========================================
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS TB_ATTENDANCE_LOG (
            TRPR_ID TEXT, TRPR_DEGR INTEGER, TRNEE_ID TEXT,
            ATEND_DT TEXT, DAY_NM TEXT, IN_TIME TEXT, OUT_TIME TEXT,
            ATEND_STATUS TEXT, ATEND_STATUS_CD TEXT,
            COLLECTED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(TRPR_ID, TRPR_DEGR, TRNEE_ID, ATEND_DT)
        )
    ''')

    # ==========================================
    # [외부] 시장 동향
    # ==========================================
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS TB_MARKET_TREND (
            TRPR_ID TEXT,           -- 훈련과정ID
            TRPR_DEGR INTEGER,      -- 훈련과정 순차
            TRPR_NM TEXT,           -- 제목
            TRAINST_NM TEXT,        -- 훈련기관명
            TR_STA_DT TEXT,         -- 훈련시작일자
            TR_END_DT TEXT,         -- 훈련종료일자
            NCS_CD TEXT,            -- NCS 코드
            TRNG_AREA_CD TEXT,      -- 지역코드
            TOT_FXNUM INTEGER,      -- 정원
            TOT_TRCO REAL,          -- 훈련비
            COURSE_MAN REAL,        -- 수강비
            REG_COURSE_MAN INTEGER, -- 등록인원
            EI_EMPL_RATE_3 REAL,    -- 3개월 취업률
            EI_EMPL_RATE_6 REAL,    -- 6개월 취업률
            EI_EMPL_CNT_3 INTEGER,  -- 3개월 취업인원
            EI_EMPL_CNT_3_GT10 TEXT,-- 10인 미만 여부
            STDG_SCOR REAL,         -- 만족도 점수
            GRADE TEXT,             -- 등급
            CERTIFICATE TEXT,       -- 자격증
            CONTENTS TEXT,          -- 컨텐츠
            ADDRESS TEXT,           -- 주소
            TEL_NO TEXT,            -- 전화번호
            INST_INO TEXT,          -- 기관 코드
            TRAINST_CST_ID TEXT,    -- 기관ID
            TRAIN_TARGET TEXT,      -- 훈련유형
            TRAIN_TARGET_CD TEXT,   -- 유형코드
            WKEND_SE TEXT,          -- 주말구분
            TITLE_ICON TEXT,        -- 아이콘
            TITLE_LINK TEXT,        -- 링크
            SUB_TITLE_LINK TEXT,    -- 부제목 링크
            YEAR_MONTH TEXT,        -- 파생: 개설연월 (YYYY-MM)
            REGION TEXT,            -- 파생: 지역 (ADDRESS 첫 단어)
            COLLECTED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (TRPR_ID, TRPR_DEGR)
        )
    ''')

    # ==========================================
    # 마이그레이션 (기존 DB 호환) — 인덱스보다 먼저 실행
    # ==========================================
    migrations = [
        "ALTER TABLE TB_TRAINEE_INFO ADD COLUMN ABSENT_CNT INTEGER",
        "ALTER TABLE TB_TRAINEE_INFO ADD COLUMN ATEND_CNT INTEGER",
        "ALTER TABLE TB_MARKET_TREND ADD COLUMN YEAR_MONTH TEXT",
        "ALTER TABLE TB_MARKET_TREND ADD COLUMN REGION TEXT",
    ]
    for sql in migrations:
        try:
            if is_pg():
                conn.commit()  # PG: 이전 작업 커밋 후 개별 실행
            cursor.execute(sql)
            if is_pg():
                conn.commit()
        except Exception:
            if is_pg():
                conn.rollback()  # PG: 실패한 트랜잭션 롤백 필수
            pass  # 이미 존재하거나 지원하지 않는 ALTER

    # ==========================================
    # 인덱스 (마이그레이션 이후 실행)
    # ==========================================
    indexes = [
        ('IDX_MARKET_NCS',  'TB_MARKET_TREND', 'NCS_CD'),
        ('IDX_MARKET_DATE', 'TB_MARKET_TREND', 'TR_STA_DT'),
        ('IDX_MARKET_AREA', 'TB_MARKET_TREND', 'TRNG_AREA_CD'),
        ('IDX_MARKET_TRAINST',    'TB_MARKET_TREND', 'TRAINST_NM'),
        ('IDX_MARKET_YEAR_MONTH', 'TB_MARKET_TREND', 'YEAR_MONTH'),
        ('IDX_MARKET_REGION',     'TB_MARKET_TREND', 'REGION'),
        ('IDX_MARKET_TARGET',     'TB_MARKET_TREND', 'TRAIN_TARGET'),
        ('IDX_ATTEND_DEGR',   'TB_ATTENDANCE_LOG', 'TRPR_DEGR'),
        ('IDX_ATTEND_DATE',   'TB_ATTENDANCE_LOG', 'ATEND_DT'),
        ('IDX_COURSE_END_DT', 'TB_COURSE_MASTER',  'TR_END_DT'),
    ]
    for idx_name, table, col in indexes:
        try:
            if is_pg():
                conn.commit()
                cursor.execute(f'CREATE INDEX IF NOT EXISTS {idx_name} ON {table} ({col})')
                conn.commit()
            else:
                cursor.execute(f'CREATE INDEX IF NOT EXISTS {idx_name} ON {table} ({col})')
        except Exception:
            if is_pg():
                conn.rollback()
            pass  # 이미 존재하거나 컬럼 미존재 시 무시

    # ==========================================
    # 백필: YEAR_MONTH, REGION (1회성)
    # ==========================================
    backfill_queries = []
    if is_pg():
        backfill_queries = [
            "UPDATE TB_MARKET_TREND SET YEAR_MONTH = LEFT(TR_STA_DT, 4) || '-' || SUBSTR(TR_STA_DT, 5, 2) WHERE YEAR_MONTH IS NULL AND TR_STA_DT IS NOT NULL",
            "UPDATE TB_MARKET_TREND SET REGION = SPLIT_PART(ADDRESS, ' ', 1) WHERE REGION IS NULL AND ADDRESS IS NOT NULL",
        ]
    else:
        backfill_queries = [
            "UPDATE TB_MARKET_TREND SET YEAR_MONTH = substr(TR_STA_DT, 1, 4) || '-' || substr(TR_STA_DT, 5, 2) WHERE YEAR_MONTH IS NULL AND TR_STA_DT IS NOT NULL",
            "UPDATE TB_MARKET_TREND SET REGION = substr(ADDRESS, 1, instr(ADDRESS || ' ', ' ') - 1) WHERE REGION IS NULL AND ADDRESS IS NOT NULL",
        ]
    for sql in backfill_queries:
        try:
            if is_pg():
                conn.commit()
            cursor.execute(sql)
            if is_pg():
                conn.commit()
        except Exception:
            if is_pg():
                conn.rollback()

    conn.commit()
    conn.close()
    db_label = "PostgreSQL" if is_pg() else DB_FILE
    print(f"[init_db] 전체 테이블 초기화 완료 (DB: {db_label})")


if __name__ == "__main__":
    init_all_tables()
