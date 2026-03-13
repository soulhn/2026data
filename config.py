"""프로젝트 전역 설정 상수"""
import datetime as dt
import os

# ── 출결 기준 ──
LATE_CUTOFF_HHMM = 910          # 9시 10분 이후 입실 → 지각
ATTENDANCE_TARGET = 90           # 목표 출석률 (%)

# ── 누적 위험군 임계값 ──
RISK_ABSENT = 3                  # 결석 N회 이상
RISK_LATE = 5                    # 지각 N회 이상
RISK_EARLY_LEAVE = 5             # 조퇴 N회 이상

# ── 캐시 TTL (초) ──
CACHE_TTL_DEFAULT = 600          # 일반 페이지 (10분)
CACHE_TTL_REALTIME = 300         # 실시간 출결 (5분)
CACHE_TTL_API = 60               # 실시간 API 캐시 (운영 현황)
CACHE_TTL_MARKET = 86400         # 시장 동향 (24시간 — ETL 주기와 동기화)
CACHE_TTL_SARAMIN = 86400        # 채용 동향 (24시간 — ETL 주기와 동기화)

# ── 실시간 API 파라미터 ──
API_MAX_WORKERS = 8              # ThreadPoolExecutor 워커 수
API_TIMEOUT = 30                 # API 요청 타임아웃 (초)

# ── ETL 파라미터 ──
ETL_ARCHIVE_START = dt.date(2023, 1, 1)
ETL_REFRESH_MONTHS = 12
ETL_PAGE_SIZE = 100
ETL_MAX_WORKERS = 4
ETL_BATCH_SIZE = 1000
ETL_UPDATE_CUTOFF_DAYS = 7      # 종료 후 N일 초과 → 출결 수집 스킵
ETL_FULL_SKIP_MONTHS = 7        # 종료 후 N개월 초과 → API 호출 완전 스킵 (취업률 6개월 확정 이후)
ETL_BATCH_PAGE_SIZE = 100

# ── 시장 동향 분석 ──
COST_BINS = [0, 1_000_000, 3_000_000, 5_000_000, 10_000_000, float("inf")]
COST_BIN_LABELS = ["~100만", "100~300만", "300~500만", "500~1000만", "1000만~"]
SCATTER_SAMPLE_LIMIT = 3000
REGRESSION_SAMPLE_LIMIT = 2000
MARKET_PREVIEW_LIMIT = 500
NCS_MIN_COURSES = 5
CERT_MIN_COURSES = 5
CERT_EMPL_MIN_COURSES = 10
TOP_CERTS_LIMIT = 20
RECENT_TREND_DAYS = 10

# ── 사람인 채용공고 ETL ──
SARAMIN_PAGE_SIZE = 110
SARAMIN_API_CALL_LIMIT = 480
SARAMIN_PUBLISHED_DAYS = int(os.environ.get("SARAMIN_PUBLISHED_DAYS", "7"))
SARAMIN_MAX_PAGES = int(os.environ.get("SARAMIN_MAX_PAGES", "5"))
SARAMIN_KEYWORDS = [
    'Python', 'Java', 'JavaScript', 'React', 'Spring',
    'AI', '백엔드', '프론트엔드', 'DevOps', '데이터',
    '클라우드', 'Flutter', '보안', 'DBA', '쿠버네티스',
]

# ── 매출 분석 상수 ──
DAILY_TRAINING_FEE = 145_200        # 일 훈련비 단가 (원)
REVENUE_FULL_THRESHOLD = 0.80       # 전액 청구 최소 출석률

# ── 취업률 특수값 코드 매핑 ──
# EI_EMPL_RATE_3 / EI_EMPL_RATE_6 / HRD_EMPL_RATE_6 (TB_COURSE_MASTER TEXT 컬럼)
EMPL_CODE_MAP = {
    'A': '개설예정',
    'B': '진행중',
    'C': '미실시',
    'D': '수료자없음',
}

# ── 훈련생 유형 코드 → 한글 레이블 (trneeTracseSe) ──
TRNEE_TYPE_MAP = {
    'C0031':  '근로자',
    'C0031C': '돌봄서비스',
    'C0031F': '근로자외국어',
    'C0054':  '국가기간전략',
    'C0054G': '기업맞춤형',
    'C0054S': '일반고특화',
    'C0054Y': '스마트혼합',
    'C0055':  '실업자',
    'C0055C': '과정평가형',
    'C0061':  '내일배움카드',
    'C0061I': '내일배움(재직자)',
    'C0061S': '내일배움(구직자)',
    'C0102':  '산업구조변화',
    'C0104':  'K-디지털트레이닝',
    'C0105':  'K-디지털기초',
}

# ── AI 리포트 ──
GEMINI_MODEL = "gemini-2.5-flash"
CACHE_TTL_AI_REPORT = 1800               # 30분
AI_REPORT_MAX_TOKENS = 8192


# ── 캐시 키 상수 (TB_MARKET_CACHE.CACHE_KEY) ──
class CacheKey:
    """ETL이 TB_MARKET_CACHE에 저장하는 사전 집계 캐시 키."""
    # hrd_etl.py 에서 생성
    ATTENDANCE_STATS = "attendance_stats"
    DB_ATTEND_DIST = "db_attend_dist"
    DB_TRAINEE_DIST = "db_trainee_dist"
    REVENUE_ALL_TERMS = "revenue_all_terms"
    DB_FILL_RATES = "db_fill_rates"
    DB_SAMPLE_VALUES = "db_sample_values"
    # market_etl.py 에서 생성
    KPI = "kpi"
    MONTHLY_COUNTS = "monthly_counts"
    REGION_COUNTS = "region_counts"
    INST_STATS = "inst_stats"
    NCS_AGG = "ncs_agg"
    MONTHLY_EMPL = "monthly_empl"
    MONTHLY_RECRUIT = "monthly_recruit"
    REGION_OPP = "region_opp"
    NCS_OPP_MATRIX = "ncs_opp_matrix"
    NCS_GROWTH = "ncs_growth"
    DB_MARKET_TYPE = "db_market_type"
    DB_MARKET_REGION = "db_market_region"
    DB_MARKET_YEAR = "db_market_year"
    # saramin_etl.py 에서 생성
    SARAMIN_KPI = "saramin_kpi"
    SARAMIN_MONTHLY = "saramin_monthly"
    SARAMIN_JOB_CD = "saramin_job_cd"
    SARAMIN_LOC = "saramin_loc"
    SARAMIN_KEYWORD_TREND = "saramin_keyword_trend"
    SARAMIN_ACTIVE_LOC = "saramin_active_loc"
    SARAMIN_ACTIVE_JOB_CD = "saramin_active_job_cd"
    SARAMIN_EXPIRED_MONTHLY = "saramin_expired_monthly"
    SARAMIN_EXPIRED_JOB_CD = "saramin_expired_job_cd"
    SARAMIN_POSTING_DURATION = "saramin_posting_duration"
