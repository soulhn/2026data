"""
전체 DB 테이블 초기화 및 마이그레이션 스크립트
모든 테이블의 CREATE/ALTER를 한 곳에서 관리합니다.

DB는 둘이다 (2026-09 분리):
  - 메인 DB  (DATABASE_URL)        : 과정·명부·출결·채용공고·집계 캐시(TB_MARKET_CACHE)
  - 시장 DB  (DATABASE_URL_MARKET) : TB_MARKET_TREND (46만 행). 미설정 시 메인 DB로 폴백
"""
from utils import get_connection, is_pg, adapt_query, MARKET_DB


def _exec_ignore(conn, cursor, sql):
    """이미 존재하거나 지원하지 않는 DDL은 무시. PG는 실패한 트랜잭션 롤백 필수."""
    try:
        if is_pg():
            conn.commit()
        cursor.execute(sql)
        if is_pg():
            conn.commit()
        return cursor.rowcount
    except Exception:
        if is_pg():
            conn.rollback()
        return 0


def init_main_tables():
    """메인 DB 테이블 — 내부 과정·명부·출결, 채용공고, 집계 캐시."""
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
    # [매핑] 채용공고 ↔ 과정 트랙 (다대다) — saramin_etl.tag_tracks() 가 전량 재생성
    # ==========================================
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS TB_JOB_POSTING_TRACK (
            JOB_ID          TEXT NOT NULL,
            TRACK           TEXT NOT NULL,
            SCORE           INTEGER,
            MATCH_SOURCE    TEXT,
            ENTRY_LEVEL     INTEGER,
            TAGGED_AT       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (JOB_ID, TRACK)
        )
    ''')

    # ==========================================
    # [KPI] 회차별 HRD-Net 집계 스냅샷 — kpi_etl.py 가 매시간 수집, 값이 바뀔 때만 행 추가
    # 수강신청·승인·명부 상태의 시간별 변화를 남긴다 (모집 KPI 1단계, recruit-kpi/docs/PLAN.md)
    # ==========================================
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS TB_COURSE_SNAPSHOT (
            TRPR_ID TEXT NOT NULL,
            TRPR_DEGR INTEGER NOT NULL,
            SNAP_AT TIMESTAMP NOT NULL,      -- 수집 시각 (UTC)
            TR_STA_DT TEXT, TR_END_DT TEXT,
            TOT_FXNUM INTEGER,               -- 정원
            TOT_TRP_CNT INTEGER,             -- 수강신청 (HRD 등록)
            TOT_PAR_MKS INTEGER,             -- 승인 = 확정 신고 인원
            FINI_CNT INTEGER,                -- 수료
            ROSTER_CNT INTEGER,              -- 명부 행 수
            ACTIVE_CNT INTEGER,              -- 훈련중
            DROPOUT_CNT INTEGER,             -- 중도탈락·제적
            PARTIAL_FINI_CNT INTEGER,        -- 80%이상수료
            EARLY_EMPL_CNT INTEGER,          -- 조기취업
            CHANGED TEXT,                    -- 'first' | 'daily' | 직전 대비 바뀐 컬럼 목록 (쉼표)
            PRIMARY KEY (TRPR_ID, TRPR_DEGR, SNAP_AT)
        )
    ''')

    # ==========================================
    # [KPI] 명부 사람 단위 스냅샷 — kpi_etl.py 가 매시간. 승인 감지 시각·상태 변화·첫 참석일을 사람마다 남긴다
    # 이름은 해시·마스킹만 (HRD 명부 원본 이름은 TB_TRAINEE_INFO에 이미 있으나 KPI 테이블엔 복제하지 않음)
    # ==========================================
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS TB_ROSTER_MEMBER (
            TRPR_ID TEXT NOT NULL, TRPR_DEGR INTEGER NOT NULL, TRNEE_ID TEXT NOT NULL,
            NAME_HASH TEXT, NAME_MASKED TEXT,
            TR_STA_DT TEXT,                  -- 회차 개강일 (등록 지연 일수 계산용)
            STATUS TEXT,                     -- 현재 명부 상태 (훈련중·중도탈락·정상수료 …)
            FIRST_SEEN_AT TIMESTAMP,         -- 명부에 처음 나타난 시각 = HRD 승인 감지
            LAST_SEEN_AT TIMESTAMP,          -- 마지막으로 명부에 있었던 시각
            GONE_AT TIMESTAMP,               -- 명부에서 사라진 것을 감지한 시각 (승인 취소 등)
            STATUS_CHANGED_AT TIMESTAMP,     -- 마지막 상태 변화 감지 시각
            FIRST_ATTEND_DT TEXT,            -- 첫 참석일 (YYYYMMDD). 입실 시간이 있거나 출석 계열 상태
            FIRST_IN_TIME TEXT,              -- 첫 참석일 입실 시각
            DAY1_STATUS TEXT,                -- 개강일 출결 상태 (출석·결석·지각 …). 행 자체가 없으면 NULL = 기록 없음
            PRIMARY KEY (TRPR_ID, TRPR_DEGR, TRNEE_ID)
        )
    ''')
    _exec_ignore(conn, cursor, "ALTER TABLE TB_ROSTER_MEMBER ADD COLUMN DAY1_STATUS TEXT")   # 2026-09-17 추가 (기존 DB)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS TB_ROSTER_MEMBER_LOG (
            TRPR_ID TEXT NOT NULL, TRPR_DEGR INTEGER NOT NULL, TRNEE_ID TEXT NOT NULL,
            DETECTED_AT TIMESTAMP NOT NULL,
            EVENT TEXT NOT NULL,             -- JOINED · STATUS · FIRST_ATTEND · LEFT
            OLD_VALUE TEXT, NEW_VALUE TEXT,
            PRIMARY KEY (TRPR_ID, TRPR_DEGR, TRNEE_ID, DETECTED_AT, EVENT)
        )
    ''')

    # ==========================================
    # [KPI] 노션 신청자 리스트 미러 — notion_applicants_etl.py 가 매시간 증분 폴링
    # 한 사람 한 줄(노션 페이지 ID가 키). 이름은 sha256 해시·마스킹만, 연락처는 저장하지 않는다.
    # ==========================================
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS TB_APPLICANT (
            NOTION_PAGE_ID TEXT PRIMARY KEY,
            NOTION_URL TEXT,
            NAME_HASH TEXT,                  -- sha256(이름). HRD 명부 이름을 즉석 해시해 대조
            NAME_MASKED TEXT,                -- 홍*동
            COHORT TEXT,                     -- 최종기수 (AIO1 …)
            COHORT_TEXT TEXT,                -- 기수 (자유 입력)
            STATUS TEXT,                     -- 최종결과 (HRD신청 · HRD등록 · 합격취소 …)
            PROCESS_RESULT TEXT,             -- 처리결과
            APPLIED_AT TEXT,                 -- 신청일시 (원문)
            SOURCE TEXT,                     -- 유입경로
            INTERVIEW_AT TEXT, INTERVIEW_GRADE TEXT,
            PASS_NOTICE_AT TEXT, PASS_REG_AT TEXT, PASS_REG INTEGER,
            HRD_APPLY_AT TEXT,               -- HRD 신청 일시 (신규 속성, 없으면 NULL)
            HRD_REG_AT TEXT,                 -- HRD 등록 일시
            HRD_ONSITE INTEGER,              -- HRD 신청(현장)
            OT_ATTEND TEXT,                  -- OT참석 O/X
            ATTEND_DT TEXT,                  -- 개강 참석일 (신규 속성, 없으면 NULL)
            CANCEL_REASON TEXT, TRANSFER_TO TEXT,
            NOTION_CREATED_AT TEXT, NOTION_EDITED_AT TEXT,
            SYNCED_AT TIMESTAMP
        )
    ''')
    # 상태 전이 로그 — 추적 필드가 바뀔 때마다 한 줄. 노션 API엔 이력이 없어 스냅샷 차분이 유일한 이력
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS TB_APPLICANT_STATUS_LOG (
            NOTION_PAGE_ID TEXT NOT NULL,
            DETECTED_AT TIMESTAMP NOT NULL,  -- 우리가 감지한 시각 (폴링 주기 단위)
            FIELD TEXT NOT NULL,             -- STATUS · COHORT · HRD_REG_AT · ATTEND_DT …
            OLD_VALUE TEXT, NEW_VALUE TEXT,
            NOTION_EDITED_AT TEXT,           -- 그 시점 노션 last_edited_time
            PRIMARY KEY (NOTION_PAGE_ID, DETECTED_AT, FIELD)
        )
    ''')
    # 노션 KPI 페이지 발행 기록 — 행별 노션 페이지 ID와 내용 해시. 내용이 같으면 API 호출을 건너뛴다
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS TB_NOTION_PUBLISH (
            DB_KEY TEXT NOT NULL,            -- 'cohort' | 'person'
            ROW_KEY TEXT NOT NULL,           -- 기수 또는 신청자 페이지 ID
            NOTION_PAGE_ID TEXT,
            CONTENT_HASH TEXT,
            UPDATED_AT TIMESTAMP,
            PRIMARY KEY (DB_KEY, ROW_KEY)
        )
    ''')
    # 동기화 상태 — 마지막 폴링 시각 등 작은 키·값
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS TB_SYNC_STATE (
            SYNC_KEY TEXT PRIMARY KEY,
            SYNC_VALUE TEXT,
            UPDATED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')

    # ==========================================
    # [캐시] 집계 캐시 — 시장·출결·채용 ETL 3종이 공유하므로 메인 DB에 둔다
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
    for sql in [
        "ALTER TABLE TB_TRAINEE_INFO ADD COLUMN ABSENT_CNT INTEGER",
        "ALTER TABLE TB_TRAINEE_INFO ADD COLUMN ATEND_CNT INTEGER",
        "ALTER TABLE TB_COURSE_MASTER DROP COLUMN REAL_EMPL_RATE",
        "ALTER TABLE TB_JOB_POSTING ADD COLUMN MODIFICATION_DT TEXT",
    ]:
        _exec_ignore(conn, cursor, sql)

    # ==========================================
    # 인덱스 (마이그레이션 이후 실행)
    # ==========================================
    indexes = [
        ('IDX_ATTEND_DEGR',   'TB_ATTENDANCE_LOG', 'TRPR_DEGR'),
        ('IDX_ATTEND_DATE',   'TB_ATTENDANCE_LOG', 'ATEND_DT'),
        ('IDX_ATTEND_TRNEE',  'TB_ATTENDANCE_LOG', 'TRNEE_ID'),
        ('IDX_TRAINEE_DEGR',  'TB_TRAINEE_INFO',   'TRPR_DEGR'),
        ('IDX_COURSE_END_DT', 'TB_COURSE_MASTER',  'TR_END_DT'),
        ('IDX_COURSE_STA_DT', 'TB_COURSE_MASTER',  'TR_STA_DT'),
        ('IDX_SNAP_AT',       'TB_COURSE_SNAPSHOT', 'SNAP_AT'),
        ('IDX_ROSTER_MEMBER_NAME', 'TB_ROSTER_MEMBER', 'NAME_HASH'),
        ('IDX_ROSTER_LOG_DETECTED', 'TB_ROSTER_MEMBER_LOG', 'DETECTED_AT'),
        ('IDX_APPLICANT_COHORT', 'TB_APPLICANT', 'COHORT'),
        ('IDX_APPLICANT_STATUS', 'TB_APPLICANT', 'STATUS'),
        ('IDX_APPLOG_DETECTED',  'TB_APPLICANT_STATUS_LOG', 'DETECTED_AT'),
        ('IDX_JOB_JOB_CD',       'TB_JOB_POSTING', 'JOB_CD'),
        ('IDX_JOB_JOB_MID_CD',   'TB_JOB_POSTING', 'JOB_MID_CD'),
        ('IDX_JOB_LOC_CD',       'TB_JOB_POSTING', 'LOC_CD'),
        ('IDX_JOB_IND_CD',       'TB_JOB_POSTING', 'IND_CD'),
        ('IDX_JOB_YEAR_MONTH',   'TB_JOB_POSTING', 'YEAR_MONTH'),
        ('IDX_JOB_POSTING_DT',   'TB_JOB_POSTING', 'POSTING_DT'),
        ('IDX_JOB_SEARCH_KW',    'TB_JOB_POSTING', 'SEARCH_KEYWORD'),
        ('IDX_JOB_KW_KEYWORD',   'TB_JOB_POSTING_KEYWORD', 'SEARCH_KEYWORD'),
        ('IDX_JOB_RGN_REGION',   'TB_JOB_POSTING_REGION',  'REGION'),
        ('IDX_JOB_TRK_TRACK',    'TB_JOB_POSTING_TRACK',   'TRACK'),
    ]
    for idx_name, table, col in indexes:
        _exec_ignore(conn, cursor, f'CREATE INDEX IF NOT EXISTS {idx_name} ON {table} ({col})')

    # ==========================================
    # 백필: TB_JOB_POSTING_KEYWORD / TB_JOB_POSTING_REGION (기존 컬럼 → junction)
    # ==========================================
    _exec_ignore(conn, cursor, adapt_query(
        "INSERT OR IGNORE INTO TB_JOB_POSTING_KEYWORD (JOB_ID, SEARCH_KEYWORD) "
        "SELECT JOB_ID, SEARCH_KEYWORD FROM TB_JOB_POSTING "
        "WHERE SEARCH_KEYWORD IS NOT NULL AND SEARCH_KEYWORD != ''"
    ))
    _exec_ignore(conn, cursor, adapt_query(
        "INSERT OR IGNORE INTO TB_JOB_POSTING_REGION (JOB_ID, REGION) "
        "SELECT JOB_ID, REGION FROM TB_JOB_POSTING "
        "WHERE REGION IS NOT NULL AND REGION != ''"
    ))

    # ==========================================
    # 1회성 정리: 2026-04 이전 사람인 데이터 삭제
    # (수집 조건 변경으로 구 데이터 신뢰도 낮음)
    # ==========================================
    for sql in [
        "DELETE FROM TB_JOB_POSTING_KEYWORD WHERE JOB_ID IN (SELECT JOB_ID FROM TB_JOB_POSTING WHERE YEAR_MONTH < '2026-04')",
        "DELETE FROM TB_JOB_POSTING_REGION WHERE JOB_ID IN (SELECT JOB_ID FROM TB_JOB_POSTING WHERE YEAR_MONTH < '2026-04')",
        "DELETE FROM TB_JOB_POSTING WHERE YEAR_MONTH < '2026-04'",
    ]:
        affected = _exec_ignore(conn, cursor, adapt_query(sql))
        if affected and affected > 0:
            print(f"[init_db] 정리: {affected}건 삭제")

    conn.commit()
    conn.close()


def init_market_tables():
    """시장 DB 테이블 — TB_MARKET_TREND (DATABASE_URL_MARKET, 미설정 시 메인 DB)."""
    conn = get_connection(timeout=30, db=MARKET_DB)
    cursor = conn.cursor()

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
            YEAR_MONTH TEXT,        -- 파생: 개설연월 (YYYY-MM)
            REGION TEXT,            -- 파생: 지역 (ADDRESS 첫 단어)
            COLLECTED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (TRPR_ID, TRPR_DEGR)
        )
    ''')

    # 마이그레이션 (기존 DB 호환) — 인덱스보다 먼저
    for col in ["YEAR_MONTH", "REGION", "CERTIFICATE", "CONTENTS", "ADDRESS", "TEL_NO",
                "INST_INO", "TRAINST_CST_ID", "TRAIN_TARGET", "TRAIN_TARGET_CD", "WKEND_SE"]:
        _exec_ignore(conn, cursor, f"ALTER TABLE TB_MARKET_TREND ADD COLUMN {col} TEXT")

    # 인덱스. IDX_MARKET_AREA·IDX_MARKET_TRAINST 는 2026-08 제거 — 누적 스캔 1·4회의 미사용
    # 인덱스로 12.6MB + 매일 UPSERT 쓰기 비용만 소모 (기관 분석은 inst_stats 캐시,
    # 지역 필터는 REGION 컬럼의 IDX_MARKET_REGION 사용)
    for idx_name, col in [
        ('IDX_MARKET_NCS', 'NCS_CD'),
        ('IDX_MARKET_DATE', 'TR_STA_DT'),
        ('IDX_MARKET_YEAR_MONTH', 'YEAR_MONTH'),
        ('IDX_MARKET_REGION', 'REGION'),
        ('IDX_MARKET_TARGET', 'TRAIN_TARGET'),
    ]:
        _exec_ignore(conn, cursor, f'CREATE INDEX IF NOT EXISTS {idx_name} ON TB_MARKET_TREND ({col})')

    # 백필: YEAR_MONTH, REGION (1회성)
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
        _exec_ignore(conn, cursor, sql)

    conn.commit()
    conn.close()


def init_all_tables(include_market=True):
    """메인 DB 테이블 초기화. include_market=True면 시장 DB 테이블까지.

    hrd_etl·saramin_etl은 시장 테이블을 쓰지 않으므로 include_market=False로 부른다 —
    DATABASE_URL_MARKET이 없는 환경에서 메인 DB에 빈 TB_MARKET_TREND를 되살리지 않기 위함.
    """
    init_main_tables()
    if include_market:
        init_market_tables()
    print("[init_db] 테이블 초기화 완료 (DB: PostgreSQL"
          + (", 시장 DB 포함)" if include_market else ")"))


if __name__ == "__main__":
    init_all_tables()
