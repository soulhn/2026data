"""프로젝트 전역 설정 상수"""
import datetime as dt
import os

# ── 출결 기준 ──
LATE_CUTOFF_HHMM = 910          # 9시 10분 이후 입실 → 지각
CLASS_END_HHMM = 1750            # 17시 50분 이전 퇴실 → 조퇴
ATTENDANCE_TARGET = 90           # 목표 출석률 (%)

# ── 누적 위험군 임계값 ──
RISK_ABSENT = 3                  # 결석 N회 이상
RISK_LATE = 5                    # 지각 N회 이상
RISK_EARLY_LEAVE = 5             # 조퇴 N회 이상

# ── 캐시 TTL (초) ──
CACHE_TTL_DEFAULT = 1800         # 일반 페이지 (30분 — 내부 데이터는 ETL이 평일 매시간 갱신하므로 10분은 과도하게 짧아 콜드 로드 빈발)
CACHE_TTL_REALTIME = 300         # 실시간 출결 (5분)
CACHE_TTL_API = 300              # 실시간 API 캐시 (운영 현황)
CACHE_TTL_MARKET = 86400         # 시장 동향 (24시간 — ETL 주기와 동기화)
CACHE_TTL_SARAMIN = 86400        # 채용 동향 (24시간 — ETL 주기와 동기화)

# ── 실시간 API 파라미터 ──
API_MAX_WORKERS = 8              # ThreadPoolExecutor 워커 수
API_TIMEOUT = (15, 30)           # (connect, read) 초 — Streamlit Cloud→한국 정부 서버 핸드셰이크 여유 확보
API_TOTAL_DEADLINE = 120         # 실시간 조회 전체 상한(초). 초과 시 미완료 기관을 버리고 DB 폴백
                                 # 한 기관은 과정목록 → (명부·출결) 2단계 순차이고 각 단계가
                                 # API_TIMEOUT 상 최대 45초라, 상한이 90초보다 짧으면 느리지만
                                 # 정상인 경로까지 잘린다. 기관 간에는 병렬이라 기관 수와 무관.

# ── ETL 파라미터 ──
ETL_ARCHIVE_START = dt.date(2023, 1, 1)
ETL_REFRESH_MONTHS = 12
ETL_PAGE_SIZE = 100
ETL_MAX_WORKERS = 4
ETL_BATCH_SIZE = 1000
ETL_UPDATE_CUTOFF_DAYS = 7      # 종료 후 N일 초과 → 출결 수집 스킵
ETL_FULL_SKIP_MONTHS = 7        # 종료 후 N개월 초과 → API 호출 완전 스킵 (취업률 6개월 확정 이후)
ETL_BATCH_PAGE_SIZE = 100
ETL_FAILED_ROW_SAMPLE = 3       # batch_execute 폴백 실패 행 중 로그에 남길 최대 건수
ETL_FUTURE_DAYS = 90            # market_etl 미래 수집 일수 — 모집 중(개강 예정) 과정 포함
ETL_FULL_REFRESH = os.environ.get("ETL_FULL_REFRESH", "") == "1"  # =1이면 market_etl 증분 무시, ARCHIVE_START부터 전체 재수집

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
SARAMIN_PUBLISHED_DAYS = int(os.environ.get("SARAMIN_PUBLISHED_DAYS", "3"))

# ── 사람인 보존 정책 (2026-08 도입: Supabase 500MB 한도 대응) ──
# 원본은 "새 집계 계산에 필요한 최소한"만 보존. 화면 추이는 TB_MARKET_CACHE 누적 집계가 담당.
SARAMIN_RETENTION_EXPIRED_DAYS = 30      # 마감 후 이 일수가 지난 공고 삭제
SARAMIN_EVERGREEN_MIN_AHEAD_DAYS = 365   # 마감일이 이보다 먼 미래면 상시채용으로 간주
SARAMIN_RETENTION_EVERGREEN_DAYS = 90    # 상시채용은 게시 후 이 일수가 지나면 삭제
                                         # (마감일이 2032년 등 먼 미래라 마감 기준으로는 영원히 안 지워짐)
# ── 사람인 수집 쿼리 (2026-09 과정별 취업 방향 개편, 설계: docs/track_job_mapping.md) ──
# 축 A `job_cd`: 사람인 직무 키워드 코드(docs/api/saramin.md 코드표). 하루 110건 이내로
#   떨어지는 좁은 코드만 수집한다. 백엔드(84)·웹개발(87)·Python(272) 같은 넓은 코드는
#   1일 창에서도 절단되므로 수집이 아니라 분류(SARAMIN_TRACK_RULES)에만 쓴다.
# 축 B `keywords`: 코드표에 없는 과정 특화 용어(LLM·RAG·MLOps·Airflow 등).
# `label`은 수집 출처로 TB_JOB_POSTING_KEYWORD.SEARCH_KEYWORD 에 저장된다.
# 어느 과정 공고인지는 여기서 정하지 않는다 — 수집 후 SARAMIN_TRACK_RULES 가 단독 결정.
SARAMIN_QUERIES = [
    # 축 A — 직무 코드
    {'label': 'NLP(160)',          'job_cd': '160'},
    {'label': '머신러닝(109)',       'job_cd': '109'},
    {'label': '딥러닝(108)',        'job_cd': '108'},
    {'label': 'AI(181)',           'job_cd': '181'},
    {'label': '챗봇(131)',          'job_cd': '131'},
    {'label': '데이터엔지니어(83)',    'job_cd': '83'},
    {'label': '데이터사이언티스트(2248)', 'job_cd': '2248'},
    {'label': 'DevOps(146)',       'job_cd': '146'},
    {'label': 'Kafka(241)',        'job_cd': '241'},
    {'label': 'Spark(289)',        'job_cd': '289'},
    {'label': 'Kubernetes(244)',   'job_cd': '244'},
    {'label': 'ETL(150)',          'job_cd': '150'},
    {'label': '빅데이터(116)',       'job_cd': '116'},
    # 축 B — MLE (LLM·RAG)
    {'label': 'LLM',          'keywords': 'LLM'},
    {'label': 'RAG',          'keywords': 'RAG'},
    {'label': 'LangChain',    'keywords': 'LangChain'},
    {'label': '생성형 AI',     'keywords': '생성형 AI'},
    {'label': '파인튜닝',       'keywords': '파인튜닝'},
    {'label': '벡터DB',        'keywords': '벡터DB'},
    {'label': 'GraphRAG',     'keywords': 'GraphRAG'},
    {'label': 'LLM 엔지니어',   'keywords': 'LLM 엔지니어'},
    # 축 B — AIO (에이전트·AI 앱)
    {'label': 'AI 에이전트',    'keywords': 'AI 에이전트'},
    {'label': 'LangGraph',    'keywords': 'LangGraph'},
    {'label': 'AI 서비스 개발', 'keywords': 'AI 서비스 개발'},
    {'label': 'FastAPI',      'keywords': 'FastAPI'},
    {'label': 'AI 애플리케이션', 'keywords': 'AI 애플리케이션'},
    {'label': 'n8n',          'keywords': 'n8n'},
    {'label': 'MCP',          'keywords': 'MCP'},
    {'label': '챗봇 개발',      'keywords': '챗봇 개발'},
    # 축 B — MLO (데이터 엔지니어·MLOps)
    {'label': 'MLOps',        'keywords': 'MLOps'},
    {'label': 'Airflow',      'keywords': 'Airflow'},
    {'label': '데이터 파이프라인', 'keywords': '데이터 파이프라인'},
    {'label': '데이터 플랫폼',   'keywords': '데이터 플랫폼'},
    {'label': 'MLflow',       'keywords': 'MLflow'},
    {'label': 'Terraform',    'keywords': 'Terraform'},
    {'label': '데이터 표준화',   'keywords': '데이터 표준화'},
    {'label': '데이터 모델러',   'keywords': '데이터 모델러'},
    # 축 B — 공통 (신입 진입 직무)
    {'label': 'AI 개발자 신입',   'keywords': 'AI 개발자 신입'},
    {'label': 'Python 개발자 신입', 'keywords': 'Python 개발자 신입'},
    {'label': '데이터 엔지니어',   'keywords': '데이터 엔지니어'},
    {'label': 'AI 엔지니어',     'keywords': 'AI 엔지니어'},
    {'label': '머신러닝 엔지니어',  'keywords': '머신러닝 엔지니어'},
]

# ── 과정(트랙) 정의 — 화면 표시용 ──
SARAMIN_TRACK_ORDER = ['MLE', 'AIO', 'MLO', 'COMMON']
SARAMIN_TRACKS = {
    'MLE': {
        'name': '머신러닝캠프',
        'course': 'LLM 지식 그래프 기반 신뢰형 GraphRAG 구축을 위한 머신러닝 엔지니어 양성 과정',
        'direction': 'LLM·RAG 엔지니어',
        'jobs': [
            'LLM 엔지니어 · RAG 엔지니어 (1순위)',
            'AI 엔지니어 — LLM 애플리케이션 (1순위)',
            'NLP 엔지니어 (2순위)',
            '머신러닝 엔지니어 — 파인튜닝·서빙 (2순위)',
        ],
        'stacks': 'Python · LangChain · LangGraph · Neo4j · 벡터DB · vLLM · PEFT · RAGAS · FastAPI · Docker',
    },
    'AIO': {
        'name': '멀티에이전트',
        'course': '멀티 에이전트 워크플로우 기반 AI 오케스트레이션 애플리케이션 개발자 양성 과정',
        'direction': 'AI 애플리케이션·에이전트 개발자',
        'jobs': [
            'AI 애플리케이션 개발자 · AI 에이전트 개발자 (1순위)',
            'Python 백엔드 개발자 — FastAPI (1순위)',
            '챗봇 · 대화형 AI 서비스 개발자 (2순위)',
            'AI 워크플로우 자동화 — n8n·로코드 (2순위)',
        ],
        'stacks': 'Python · FastAPI · PostgreSQL · Redis · LangGraph · LangSmith · MCP · Streamlit · n8n · Docker · AWS',
    },
    'MLO': {
        'name': 'AI Ready 데이터',
        'course': 'AI Ready Data 기반 Cloud-Native 자동화를 위한 MLOps 엔지니어 양성 과정',
        'direction': '데이터 엔지니어·MLOps',
        'jobs': [
            '데이터 엔지니어 · 데이터 파이프라인 엔지니어 (1순위)',
            'MLOps 엔지니어 (1순위)',
            'DevOps · 클라우드 플랫폼 엔지니어 (2순위)',
            '데이터 모델러 · 데이터 표준화 (2순위)',
        ],
        'stacks': 'Python · Linux · MySQL · MongoDB · Spark · Kafka · AWS · Airflow · MLflow · Docker · Kubernetes · Terraform',
    },
    'COMMON': {
        'name': '공통',
        'course': '3개 과정 공통 진입 직무',
        'direction': 'Python·AI 개발 신입',
        'jobs': [
            'Python 개발자 · 소프트웨어 개발 신입',
            'AI(인공지능) 신입 · 생성형 AI 활용 개발',
            '데이터 처리·분석 기초 직무',
        ],
        'stacks': 'Python · Git · pandas · SQL · LLM API · Streamlit · FastAPI · Docker',
    },
}

# ── 트랙 분류 규칙 (수집된 모든 공고에 적용, 기존 공고 소급 태깅) ──
# 점수 = 강한 신호 3점 + 보조 신호 1점, threshold 이상이면 태깅. 한 공고가 여러 트랙에 붙을 수 있다.
# 코드는 JOB_CD(쉼표 목록)의 숫자 코드, 정규식은 제목+공고 키워드에 적용(대소문자 구분 —
# RAG·NLP 같은 약어를 storage·average 등에서 오탐하지 않기 위함).
SARAMIN_ENTRY_LEVEL_CODES = {'0', '1', '3'}      # 경력무관 · 신입 · 신입/경력
SARAMIN_EXCLUDE_JOB_TYPES = {'5', '8', '11'}     # 아르바이트 · 파견직 · 교육생
# 강사·헤드헌팅·마케팅·상담·라벨링·보조 인력은 IT 태그가 붙어 있어도 과정 취업 방향이 아님
SARAMIN_EXCLUDE_TITLE_RE = (r'강사|헤드헌팅|과외|학원|마케팅|영업|고객상담|텔레마케팅|어시스턴트|보조 ?스탭|라벨링|라벨러|검수'
                            r'|기획자|Product ?Owner|Product ?Manager|품질관리|(?<![A-Za-z])QA(?![A-Za-z])'
                            r'|교육생|양성과정|훈련생|세일즈|Sales|매니저|Manager')
# 코드 단독 매치일 때 제목이 갖춰야 할 최소 근거 — "정기 채용 사원 모집"처럼 직무 태그만 잔뜩 붙은
# 일괄 채용 공고를 거른다. 텍스트 근거가 있는 매치에는 적용하지 않는다.
SARAMIN_CODE_ONLY_TITLE_RE = (r'개발|엔지니어|Engineer|Developer|프로그래머|데이터|Data|DevOps|MLOps|플랫폼'
                              r'|인프라|클라우드|Cloud|백엔드|서버|소프트웨어|(?<![A-Za-z])(AI|ML|SW|S/W)(?![A-Za-z])'
                              r'|인공지능|머신러닝|딥러닝|연구')
# 코드 단독 매치 최소 점수 — 사람인 직무 태그는 기업이 넓게 붙이는 경향이 있어(로봇 제어·CAE 해석 공고에
# 데이터엔지니어 태그 등) 제목·키워드 근거 없이 코드만으로 태깅하려면 강한 코드가 최소 하나 있고
# 합계가 이 점수 이상(강한 코드 + 다른 코드 1개 이상)이어야 한다. 텍스트 근거가 있으면 threshold 만 본다.
SARAMIN_CODE_ONLY_MIN_SCORE = 4
_ACR = r'(?<![A-Za-z])'   # 약어 앞뒤에 영문자가 붙지 않을 때만 (한글 인접은 허용)
_END = r'(?![A-Za-z])'
SARAMIN_TRACK_RULES = {
    'MLE': {
        'strong_codes': {'160'},
        'strong_re': (rf'{_ACR}(LLM|RAG|NLP|sLLM){_END}|LangChain|GraphRAG|파인튜닝|[Ff]ine[- ]?[Tt]un|자연어|생성형'
                      rf'|머신러닝|딥러닝|Machine ?Learning|Deep ?Learning|인공지능'
                      rf'|{_ACR}(ML|AI)\s?(엔지니어|Engineer|개발|모델|연구|리서치)'),
        'weak_codes': {'109', '108', '181', '131'},
        'weak_re': r'벡터|임베딩|Embedding|PyTorch|Pytorch|Hugging|프롬프트',
        'threshold': 3,
    },
    'AIO': {
        'strong_codes': {'131'},
        'strong_re': rf'에이전트|{_ACR}(Agent(ic)?|MCP|n8n){_END}|LangGraph|FastAPI|AI ?서비스|AI ?애플리케이션|AI ?앱|챗봇',
        'weak_codes': {'272', '142', '280', '270', '181', '84'},
        'weak_re': rf'Streamlit|Redis|Supabase|생성형|{_ACR}LLM{_END}|Python',
        'threshold': 3,
    },
    'MLO': {
        'strong_codes': {'83', '146', '241', '289', '244', '150'},
        'strong_re': rf'{_ACR}(MLOps|ETL){_END}|Airflow|데이터 ?파이프라인|데이터 ?플랫폼|MLflow|Terraform|데이터 ?엔지니어',
        'weak_codes': {'116', '136', '201', '214', '254', '270', '257', '280'},
        'weak_re': rf'Docker|Kubernetes|쿠버네티스|Spark|Kafka|데이터 ?표준|데이터 ?모델|Linux|리눅스|{_ACR}AWS{_END}',
        'threshold': 3,
    },
    # 공통: 신입 가능 + Python 태그(272)에 개발·AI·데이터 계열 근거가 하나 더 있어야 함
    # (Python 태그만으로는 로봇 제어·펌웨어 공고까지 들어와 threshold 4)
    'COMMON': {
        'strong_codes': {'272'},
        'strong_re': r'Python ?개발|파이썬 ?개발',
        'weak_codes': {'84', '181', '83', '82'},
        'weak_re': rf'Python|파이썬|{_ACR}AI{_END}|인공지능|데이터|백엔드|소프트웨어',
        'threshold': 4,
        'entry_only': True,
    },
}

# ── 매출 분석 상수 ──
DAILY_TRAINING_FEE = 145_200        # 일 훈련비 단가 (원)
REVENUE_FULL_THRESHOLD = 0.80       # 전액 청구 최소 출석률

# ── 과정 축약명 (운영 현황 사이드바 표시용, 내부 호칭) ──
COURSE_SHORT_NAMES = {
    "AIG20260000578382": "MLE",   # [엔코아] LLM 지식 그래프 기반 신뢰형 GraphRAG … 머신러닝 엔지니어 양성 과정
    "AIG20260000578396": "AIO",   # [엔코아] 멀티 에이전트 워크플로우 기반 AI 오케스트레이션 … 개발자 양성 과정
}

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
OPENAI_MODEL = "gpt-5.4-mini"
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
    # saramin_etl.py 에서 생성 (2026-09 트랙 개편). 진행중 분포·목록은 페이지가 직접 조회하고,
    # 보존 삭제 후에도 남아야 하는 시계열과 수집 현황만 캐시한다.
    SARAMIN_TRACK_MONTHLY = "saramin_track_monthly"   # TRACK('ALL' 포함), YEAR_MONTH, CNT, ENTRY_CNT — 누적 병합
    SARAMIN_QUERY_HITS = "saramin_query_hits"         # SEARCH_KEYWORD, CNT — 수집 쿼리별 보유 공고
