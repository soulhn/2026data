"""프로젝트 전역 설정 상수"""
import datetime as dt

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
CACHE_TTL_MARKET = 86400         # 시장 동향 (24시간 — ETL 주기와 동기화)

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
