"""
전체 DB 테이블 초기화 및 마이그레이션 스크립트
모든 테이블의 CREATE/ALTER를 한 곳에서 관리합니다.
"""
import sqlite3
from utils import get_connection, DB_FILE, is_pg, adapt_query


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
            HRD_EMPL_RATE_6 TEXT, HRD_EMPL_CNT_6 INTEGER,
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
    # [외부] 채용공고 (사람인 API)
    # ==========================================
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS TB_JOB_POSTING (
            JOB_ID          TEXT PRIMARY KEY,
            ACTIVE          INTEGER,
            COMPANY_NM      TEXT,
            POSITION_TITLE  TEXT,
            IND_CD          TEXT,
            IND_NM          TEXT,
            JOB_MID_CD      TEXT,
            JOB_MID_NM      TEXT,
            JOB_CD          TEXT,
            JOB_NM          TEXT,
            LOC_CD          TEXT,
            LOC_NM          TEXT,
            JOB_TYPE_CD     TEXT,
            JOB_TYPE_NM     TEXT,
            EDU_LV_CD       TEXT,
            EDU_LV_NM       TEXT,
            EXPERIENCE_CD   TEXT,
            EXPERIENCE_MIN  INTEGER,
            EXPERIENCE_MAX  INTEGER,
            EXPERIENCE_NM   TEXT,
            SALARY_CD       TEXT,
            SALARY_NM       TEXT,
            CLOSE_TYPE_CD   TEXT,
            CLOSE_TYPE_NM   TEXT,
            POSTING_DT      TEXT,
            EXPIRATION_DT   TEXT,
            OPENING_DT      TEXT,
            MODIFICATION_DT TEXT,
            KEYWORD         TEXT,
            POSITION_URL    TEXT,
            SEARCH_KEYWORD  TEXT,
            YEAR_MONTH      TEXT,
            REGION          TEXT,
            COLLECTED_AT    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')

    # ==========================================
    # [매핑] 채용공고 ↔ 검색 키워드 (다대다)
    # ==========================================
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS TB_JOB_POSTING_KEYWORD (
            JOB_ID          TEXT NOT NULL,
            SEARCH_KEYWORD  TEXT NOT NULL,
            COLLECTED_AT    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (JOB_ID, SEARCH_KEYWORD)
        )
    ''')

    # ==========================================
    # [매핑] 채용공고 ↔ 지역 (다대다)
    # ==========================================
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS TB_JOB_POSTING_REGION (
            JOB_ID          TEXT NOT NULL,
            REGION          TEXT NOT NULL,
            COLLECTED_AT    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (JOB_ID, REGION)
        )
    ''')

    # ==========================================
    # [캐시] 시장 동향 집계 캐시 (ETL 후 pre-compute)
    # ==========================================
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS TB_MARKET_CACHE (
            CACHE_KEY TEXT PRIMARY KEY,
            CACHE_DATA TEXT,
            COMPUTED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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
        "ALTER TABLE TB_COURSE_MASTER DROP COLUMN REAL_EMPL_RATE",
        "ALTER TABLE TB_MARKET_TREND ADD COLUMN CERTIFICATE TEXT",
        "ALTER TABLE TB_MARKET_TREND ADD COLUMN CONTENTS TEXT",
        "ALTER TABLE TB_MARKET_TREND ADD COLUMN ADDRESS TEXT",
        "ALTER TABLE TB_MARKET_TREND ADD COLUMN TEL_NO TEXT",
        "ALTER TABLE TB_MARKET_TREND ADD COLUMN INST_INO TEXT",
        "ALTER TABLE TB_MARKET_TREND ADD COLUMN TRAINST_CST_ID TEXT",
        "ALTER TABLE TB_MARKET_TREND ADD COLUMN TRAIN_TARGET TEXT",
        "ALTER TABLE TB_MARKET_TREND ADD COLUMN TRAIN_TARGET_CD TEXT",
        "ALTER TABLE TB_MARKET_TREND ADD COLUMN WKEND_SE TEXT",
        "ALTER TABLE TB_MARKET_TREND ADD COLUMN TITLE_ICON TEXT",
        "ALTER TABLE TB_MARKET_TREND ADD COLUMN TITLE_LINK TEXT",
        "ALTER TABLE TB_MARKET_TREND ADD COLUMN SUB_TITLE_LINK TEXT",
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
        ('IDX_JOB_JOB_CD',       'TB_JOB_POSTING', 'JOB_CD'),
        ('IDX_JOB_JOB_MID_CD',   'TB_JOB_POSTING', 'JOB_MID_CD'),
        ('IDX_JOB_LOC_CD',       'TB_JOB_POSTING', 'LOC_CD'),
        ('IDX_JOB_IND_CD',       'TB_JOB_POSTING', 'IND_CD'),
        ('IDX_JOB_YEAR_MONTH',   'TB_JOB_POSTING', 'YEAR_MONTH'),
        ('IDX_JOB_POSTING_DT',   'TB_JOB_POSTING', 'POSTING_DT'),
        ('IDX_JOB_SEARCH_KW',    'TB_JOB_POSTING', 'SEARCH_KEYWORD'),
        ('IDX_JOB_KW_KEYWORD',   'TB_JOB_POSTING_KEYWORD', 'SEARCH_KEYWORD'),
        ('IDX_JOB_RGN_REGION',   'TB_JOB_POSTING_REGION',  'REGION'),
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
    # 컬럼 마이그레이션 (기존 테이블에 새 컬럼 추가)
    # ==========================================
    migrations = [
        ('TB_JOB_POSTING', 'MODIFICATION_DT', 'TEXT'),
    ]
    for table, col, col_type in migrations:
        try:
            if is_pg():
                conn.commit()
            cursor.execute(f'ALTER TABLE {table} ADD COLUMN {col} {col_type}')
            if is_pg():
                conn.commit()
        except Exception:
            if is_pg():
                conn.rollback()
            pass  # 이미 존재하는 컬럼이면 무시

    # ==========================================
    # 백필: YEAR_MONTH, REGION (1회성)
    # ==========================================
    backfill_queries = []
    if is_pg():
        backfill_queries = [
            "UPDATE TB_MARKET_TREND SET YEAR_MONTH = LEFT(TR_STA_DT, 7) WHERE TR_STA_DT IS NOT NULL AND (YEAR_MONTH IS NULL OR YEAR_MONTH !~ '^[0-9]{4}-[0-9]{2}$')",
            "UPDATE TB_MARKET_TREND SET REGION = SPLIT_PART(ADDRESS, ' ', 1) WHERE REGION IS NULL AND ADDRESS IS NOT NULL",
        ]
    else:
        backfill_queries = [
            "UPDATE TB_MARKET_TREND SET YEAR_MONTH = substr(TR_STA_DT, 1, 7) WHERE TR_STA_DT IS NOT NULL AND (YEAR_MONTH IS NULL OR length(YEAR_MONTH) != 7 OR YEAR_MONTH LIKE '%-%-%')",
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

    # ==========================================
    # 백필: TB_JOB_POSTING_KEYWORD (기존 SEARCH_KEYWORD → junction)
    # ==========================================
    kw_backfill = (
        "INSERT OR IGNORE INTO TB_JOB_POSTING_KEYWORD (JOB_ID, SEARCH_KEYWORD) "
        "SELECT JOB_ID, SEARCH_KEYWORD FROM TB_JOB_POSTING "
        "WHERE SEARCH_KEYWORD IS NOT NULL AND SEARCH_KEYWORD != ''"
    )
    try:
        if is_pg():
            conn.commit()
        cursor.execute(adapt_query(kw_backfill))
        if is_pg():
            conn.commit()
    except Exception:
        if is_pg():
            conn.rollback()

    # ==========================================
    # 백필: TB_JOB_POSTING_REGION (기존 REGION → junction)
    # ==========================================
    rgn_backfill = (
        "INSERT OR IGNORE INTO TB_JOB_POSTING_REGION (JOB_ID, REGION) "
        "SELECT JOB_ID, REGION FROM TB_JOB_POSTING "
        "WHERE REGION IS NOT NULL AND REGION != ''"
    )
    try:
        if is_pg():
            conn.commit()
        cursor.execute(adapt_query(rgn_backfill))
        if is_pg():
            conn.commit()
    except Exception:
        if is_pg():
            conn.rollback()

    # ==========================================
    # 1회성 정리: 2026-04 이전 사람인 데이터 삭제
    # (수집 조건 변경으로 구 데이터 신뢰도 낮음)
    # ==========================================
    cleanup_sqls = [
        "DELETE FROM TB_JOB_POSTING_KEYWORD WHERE JOB_ID IN (SELECT JOB_ID FROM TB_JOB_POSTING WHERE YEAR_MONTH < '2026-04')",
        "DELETE FROM TB_JOB_POSTING_REGION WHERE JOB_ID IN (SELECT JOB_ID FROM TB_JOB_POSTING WHERE YEAR_MONTH < '2026-04')",
        "DELETE FROM TB_JOB_POSTING WHERE YEAR_MONTH < '2026-04'",
    ]
    for sql in cleanup_sqls:
        try:
            if is_pg():
                conn.commit()
            cursor.execute(adapt_query(sql))
            affected = cursor.rowcount
            if affected:
                print(f"[init_db] 정리: {affected}건 삭제")
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
