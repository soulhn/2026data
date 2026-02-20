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
CACHE_TTL_MARKET = 3600          # 시장 동향 (1시간)

# ── ETL 파라미터 ──
ETL_ARCHIVE_START = dt.date(2023, 1, 1)
ETL_REFRESH_MONTHS = 12
ETL_PAGE_SIZE = 100
ETL_MAX_WORKERS = 4
ETL_BATCH_SIZE = 1000
ETL_UPDATE_CUTOFF_DAYS = 7
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
