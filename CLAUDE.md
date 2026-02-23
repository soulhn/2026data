# CLAUDE.md - 프로젝트 컨텍스트

## ⚠️ 한글 코드포인트 규칙 (CRITICAL)

**"분석"의 "석"은 반드시 한글 U+C11D (HANGUL SYLLABLE SEOG) 을 사용한다.**
- ✅ 올바름: 분석 = U+BD84(분) + U+C11D(석)
- ❌ 금지: 분析 — 析은 U+6790 (CJK 한자)으로 전혀 다른 문자

이 규칙은 파일 경로, 코드 문자열, 응답 텍스트 모두에 적용된다.
Claude는 "분석"을 쓸 때 析(U+6790)을 사용하는 경향이 있으므로 반드시 주의한다.

## 프로젝트 개요

HRD-Net 공공데이터 기반 훈련 과정 성과 분석 대시보드 (Streamlit + PostgreSQL/SQLite)

## 아키텍처

```
[GitHub Actions]                [Supabase]              [Streamlit Cloud]
hrd_etl.py (평일 매시간)  →   PostgreSQL DB    ←    대시보드 (읽기 전용)
market_etl.py (매일 21시) →                    ←    https://playdata.streamlit.app
```

### DB 이중 지원 (SQLite / PostgreSQL)
- `DATABASE_URL` 환경변수 있으면 → PostgreSQL (Supabase), 없으면 → SQLite (로컬)
- `utils.py`의 `is_pg()`, `get_database_url()`, `adapt_query()`로 자동 전환
- `adapt_query()`: `?` → `%s`, `INSERT OR IGNORE` → `ON CONFLICT DO NOTHING` 자동 변환
- PostgreSQL은 컬럼명을 소문자로 반환하므로 `load_data()`에서 대문자 변환 처리
- PG 읽기: `@st.cache_resource` 커넥션 풀링 (`_get_pg_pool()`)으로 TCP 재연결 방지

### 성능 최적화 전략
- **커넥션 풀링**: `load_data()`가 PG 읽기 시 `@st.cache_resource` 캐싱 커넥션 사용
- **파생 컬럼 사전 계산**: `YEAR_MONTH`, `REGION` 컬럼을 ETL 시 DB에 저장 (Python 파싱 제거)
- **SQL-Side 집계**: 시장 페이지가 30만건 전체를 Python으로 로드하지 않고, 탭별 `GROUP BY` SQL로 수백건만 조회
- **`build_where_clause()`**: 사이드바 필터를 SQL WHERE로 변환하여 DB에서 필터링
- **산점도/키워드**: `ORDER BY RANDOM() LIMIT N`으로 샘플링
- **ETL 집계 캐시** (`TB_MARKET_CACHE`): market_etl.py 완료 후 `compute_and_cache_aggregations()`가 10개 집계를 JSON으로 저장 → 시장 페이지 `where == ""` (필터 없음)일 때 `get_market_cache(key)`로 즉시 반환, 필터 적용 시 live SQL 폴백
- **시장 페이지 `@st.cache_data`**: 22개 `load_*` 함수 전체에 `ttl=CACHE_TTL_MARKET` 적용 (캐시 히트 시 DB 쿼리 완전 생략)

### ETL 자동화 (GitHub Actions)
- `.github/workflows/hrd_etl.yml` - 평일 KST 09:00~18:00 매시간
- `.github/workflows/market_etl.yml` - 매일 KST 21:00
- Secrets: `HRD_API_KEY`, `HANWHA_COURSE_ID`, `DATABASE_URL`

### 홈 페이지 (home.py)
- **인증 전 프로젝트 소개 블록** (`_render_project_intro()`): 문제/해결책 2컬럼 + 메트릭 5개 (총 운영 기수, 전국 순위 2023/2024/2025, 누적 훈련비 매출)
- **인증 후 섹션 순서**:
  1. KPI 5개 (총 운영 과정, 누적 수강생, 평균 수료율, 평균 취업률 3개월/6개월)
  2. **운영 평균 성과** (`get_attendance_stats()` 활용): 평균 수료율·출석률·취업률(3개월/6개월) 4칸 — 출석률은 TB_ATTENDANCE_LOG 종료 기수 집계
  3. **기수 기록**: 최고 수료율·취업률(6개월)·출석률·단일기수 최고매출 각 달성 회차 표시 — 매출은 `출석+지각 일수 × DAILY_TRAINING_FEE` 근사치
  4. **시장 포지셔닝** (`📍 전국 KDT 시장 포지셔닝`): 연도별(2023/2024/2025) 전국 순위 메트릭 + Altair 바차트
  5. **오늘의 출결 현황**: 입실중 재분류(IN_TIME 있으면 결석→입실중), 출석률/입실중/결석/지각/조퇴 KPI 6칸 + 기수별 출석률 바차트
  6. 연도별 운영 규모, 우수 성과 Top 5, 현재 운영 중 과정 테이블 (수강신청/개강인원/제적/중도탈락/현재인원/잔여율)
- **`st.navigation()` 사용**: 사이드바 레이블을 파일명과 무관하게 커스텀 지정. 새 페이지 추가 시 `pg = st.navigation([...])` 목록에 수동 등록 필요
- 사이드바 순서: 성과 대시보드 → 시장 분석 & 기회 발굴 → 종료 과정 성과 분석 → 현재 운영 현황 → 매출 분석 → DB 명세

### 시장 분석 페이지 (pages/1_*.py) 구조
- 사이드바 레이블: **시장 분석 & 기회 발굴**
- **월별 추이 차트**: 이번 달(진행 중)은 집계 미완료이므로 `datetime.now().strftime('%Y-%m')`으로 현재 월을 제외하고 전 달까지만 표시
- **9개 탭** (2025-02 13탭→9탭 재편):

| 탭 | 내용 | KDT(취업률 미제공) 시 |
|---|---|---|
| 📊 시장 개요 | KPI 3개(총과정수·평균모집률·평균훈련비), 월별추이, 지역별 | 정상 작동 |
| 🏆 순위 & 모집 | 기관/과정 순위, 유형별·NCS별 모집 현황 | 정상 작동 |
| 🎨 유형 & 일정 | 훈련유형 분포, 주말/주중, 유형별 모집률 | 정상 작동 |
| 📈 시계열 & 경쟁 | 월별 훈련비·개설수·지역별 추이 + NCS 경쟁심화도 + 기관경쟁력 매트릭스 | 정상 작동 |
| 📊 취업률 분석 | 월별취업률·유형별·비용산점도·비용대비성과·자격증별·키워드별 취업률 | 상단 배너만 표시 |
| 💎 우리 과정 vs 시장 | 핵심지표 비교, 백분위, 레이더차트, 회차별 테이블 | 취업률 항목 "미제공", 레이더 3축 |
| ☁️ 키워드 & 자격증 | 키워드 빈도, 자격증 과정수(color=훈련비), 전체 테이블 | 정상 작동 |
| 🔭 사업기회 발굴 | 지역별 수요-공급 갭, NCS 성장, NCS 기회매트릭스, 기회지수 | §1·2만 표시, §3·4 안내 |

- **공통 사전 로드**: `type_perf_data`, `kwd_shared`/`top_words_shared`, `cert_stats_shared`를 탭 생성 전에 한 번만 계산 → 취업률 탭·자격증 탭 공유
- **헬퍼 함수 2개**: `render_ranking_table()`, `render_scatter_with_overlay()` — 탭 간 중복 코드 통합
- `load_internal_courses()`: TB_COURSE_MASTER 캐시 로드 (HANWHA_COURSE_ID 기반)
- 내부 과정 NCS 코드는 TB_MARKET_TREND와 TRPR_ID merge로 매칭
- `scikit-learn` LinearRegression: 비용→취업률 시뮬레이터
- **자격증 분석**: CERTIFICATE 컬럼 파싱 → 자격증별 과정수/취업률 Top 20
- **회차별 상세 비교**: 정원/수강신청인원/수료인원/수료율/취업률(NaN→None, column_config 방식)
- **사업기회 발굴**: 지역별 수요-공급 갭 / 성장 NCS 분야(최근 6개월 vs 이전 6개월) / 고성과·저경쟁 NCS 매트릭스 / 종합 기회지수 Top 15 (`load_region_opp()`, `load_ncs_growth()`, `load_ncs_opp_matrix()`)
- HANWHA_COURSE_ID 미설정 시 시장 전체 분석만 표시 (st.info 안내)
- **ETL 캐시 폴백**: `get_market_cache(key)` — `where == ""` 조건 시 TB_MARKET_CACHE에서 즉시 반환 (10개 함수 적용: kpi/monthly_counts/region_counts/inst_stats/ncs_agg/monthly_empl/monthly_recruit/region_opp/ncs_growth/ncs_opp_matrix)

### 종료 과정 성과 분석 페이지 (pages/2_*.py) 구조
- 사이드바 레이블: **종료 과정 성과 분석**
- **2가지 탭**: 🌐 전체 기수 비교 / 📌 개별 기수 분석 (st.tabs, 전체 기수 먼저)
- 개별 기수 분석 **6개 탭**: 인구통계(+유형별 성과) / 요일별 출결 패턴(히트맵) / 시간대별 지각 분포 / **체류시간 분석**(IN_TIME~OUT_TIME) / 출결·이탈 / 학생별 출결 현황(출석률 프로그레스바)
- 전체 기수 비교: 수료율/취업률 바차트, 결석 건수, 출석률 추이 라인차트, 종합 비교 테이블 (수강신청/수강인원/제적/중도탈락/잔여율/수료인원/수료율/취업률)
- **수료 판정**: `TRNEE_STATUS.str.contains('수료|조기취업')` — HRD-Net API 실제 값이 `'정상수료'`, `'80%이상수료'`, `'조기취업'` 등이므로 `== '수료'` 정확일치 사용 금지
- **TRNEE_TYPE 표시**: `TRNEE_TYPE_MAP` (config.py)으로 코드 → 한글 변환 (`C0031`→근로자원격, `C0055`→실업자원격 등)

### 현재 운영 현황 페이지 (pages/3_*.py) 구조
- 사이드바 레이블: **현재 운영 현황**
- 대시보드형 UI: 출석률 게이지 + KPI 8개
- 보고용 텍스트 (expander) — 지각: 입실시간, 조퇴: 퇴실시간 표시
- **최근 출결 추이** 미니차트 (최근 10일, 90% 기준선)
- **누적 출결 위험 지표**: 결석 3회+, 지각 5회+, 조퇴 5회+ 위험군 자동 감지
- 상세 탭: 미퇴실/특이사항, 결석자, 전체 출석부

### 매출 분석 페이지 (pages/4_*.py) 구조
- 사이드바 레이블: **매출 분석**
- 30일 단위 청구 기간(단위기간) × 수강생별 출석률로 훈련비 매출 산출
- **상단 핵심 지표 4개** (탭 위, 종강 기수 기준): 누적 총매출 / 기수당 평균 매출 / 평균 달성률 / 전액 청구 비율 — `build_all_terms_revenue()` 결과를 탭과 공유(`_top_rev`)하여 중복 호출 없음
- **2가지 탭**: 🌐 전체 기수 비교 / 📌 개별 기수 분석 (st.tabs, 전체 기수 먼저)
- 개별 기수 분석 **4개 탭**: 매출 개요(단위기간별 바차트+누적라인) / 수강생별 상세(매트릭스+CSV) / 단위기간 상세(히스토그램+파이차트) / **위험 현황**(진행중 기간 위험 수강생 + 추가 필요 출석/회복 가능 여부)
- 전체 기수 비교: 기수별 총 매출 바차트, 청구 유형 비율 스택바, 종합 비교 테이블
- **청구 기준**: 출석률 80%+ → 전액 / 0~80% → 비례 / 0% → 미청구 (지각·조퇴·외출 3개 누적 = 가상 결석 1일)
- **수강생 유형별 rate 분모 결정** (`build_revenue_df` 내):
  - 일반: `student_td`(개인 훈련일수) = `period_td` → 구분 불필요
  - **중도입과** (중도탈락미출석 없음, `student_td < period_td`): rate 분모 = `student_td`
  - **중도탈락** (중도탈락미출석 기록 있음, `has_dropout=True`): rate 분모 = `period_td`
- **단위기간 집계 버림**: 수강생 개별 fee 합산 후 10원 단위 버림 `(raw // 10) * 10` → 총 매출은 단위기간 버림액 합계
- **카드사 소계 버림 한계**: HRD-Net은 신한(SH)/농협(NH) 카드사별 소계에 각각 버림 적용 → 우리 DB에 카드사 구분 없어 단위기간 전체 단일 버림 → 최대 10원/기간 오차 허용
- `get_billing_periods()` (utils.py): 개강일 기준 월 단위 청구 기간 목록 (월말 클램프 처리)
- `calc_revenue(attend_days, training_days, period_training_days=None)` (utils.py):
  - `training_days`: 출석률 계산 분모 (중도입과면 개인 훈련일수, 탈락이면 기간 전체)
  - `period_training_days`: full_fee 기준 (None이면 training_days 사용)
  - 비례 계산: 부동소수점 오차 방지 위해 정수 산술 `full_fee * rate_per_mille // 1000`

### DB 명세 페이지 (pages/DB_명세.py) 구조
- 사이드바 레이블: **DB 명세**
- **상단 수집 현황 KPI**: 수집된 과정 / 훈련생 / 로그 / 시장 동향 / 최종 수집 시각(KST) 5개 메트릭
- **테이블 개요**: 5개 테이블 레코드 수 + 채움 양호 컬럼 비율 요약
- **테이블별 상세 탭**: 컬럼명·타입·설명·채움률(🟢≥95% 🟡50–94% 🔴1–49% ⚫0%) + 예시값 + 20행 미리보기(expander) + 데이터 분포
- 수동 ETL 버튼 없음 — GitHub Actions 자동 실행으로 대체 (필요 시 Actions 수동 트리거)

### 설정 상수 (config.py)
- 출결 기준, 위험군 임계값, 캐시 TTL, ETL 파라미터, 시장 분석 상수를 중앙 관리
- 모든 페이지에서 `from config import ...`으로 참조 (하드코딩 금지)
- 주요 상수 그룹:
  - **출결 기준**: `LATE_CUTOFF_HHMM = 910` (9시 10분 이후 → 지각), `ATTENDANCE_TARGET = 90` (목표 출석률 %)
  - **위험군 임계값**: `RISK_ABSENT = 3`, `RISK_LATE = 5`, `RISK_EARLY_LEAVE = 5`
  - **캐시 TTL**: `CACHE_TTL_DEFAULT = 600`, `CACHE_TTL_REALTIME = 300`, `CACHE_TTL_MARKET = 86400` (24시간 — 매일 21시 ETL 주기와 동기화)
  - **ETL 파라미터**: `ETL_ARCHIVE_START`, `ETL_REFRESH_MONTHS`, `ETL_PAGE_SIZE`, `ETL_MAX_WORKERS`, `ETL_UPDATE_CUTOFF_DAYS=7`(출결 스킵 기준일), `ETL_FULL_SKIP_MONTHS=7`(전체 API 스킵 기준월) 등
  - **훈련유형 코드 매핑**: `TRNEE_TYPE_MAP` — HRD-Net `trneeTracseSe` 코드를 한글 레이블로 변환 (C0031→근로자원격, C0055→실업자원격, C0104→K-디지털트레이닝 등)
  - **시장 분석**: `COST_BINS/LABELS` (비용 구간), `SCATTER_SAMPLE_LIMIT = 3000`, `REGRESSION_SAMPLE_LIMIT = 2000`, `RECENT_TREND_DAYS = 10`, `NCS_MIN_COURSES = 5`, `TOP_CERTS_LIMIT = 20`
  - **매출 분석**: `DAILY_TRAINING_FEE = 145_200` (일 훈련비 단가), `REVENUE_FULL_THRESHOLD = 0.80` (전액 청구 최소 출석률)

### 테스트 (tests/)
- `pytest` 기반, 89개 테스트
- `conftest.py`: `_NoCloseConnection` 프록시로 인메모리 SQLite fixture 제공
- `test_utils.py`, `test_config.py`, `test_init_db.py`, `test_hrd_etl.py`, `test_market_etl.py`
- `test_billing.py`: 매출 청구 로직 TDD — 실제 HRD-Net 청구 금액으로 검증 (33개)
  - 17기 2단위·6단위 / 18기 1단위(중도입과)·2단위(중도탈락) 수강생 개별 fee
  - 단위기간 raw 합계 → 10원 버림 → 총 매출 검증
  - 카드사별 소계 버림 한계(최대 10원/기간) 문서화
- 실행: `python -m pytest tests/ -v`

### 주의사항
- ETL 파일(hrd_etl.py, market_etl.py)에서 모듈 최상위 레벨에 `exit()` 사용 금지 (Streamlit import 시 앱 종료됨)
- 모든 SQL 쿼리는 `adapt_query()`를 거쳐야 PG 호환
- 페이지에서 직접 `pd.read_sql()` 대신 `load_data()` 사용 권장
- Plotly `add_vline`에 문자열 x값 + `annotation_text` 동시 사용 시 TypeError 발생 → `add_annotation` 별도 호출
- **Plotly 시계열 끊김**: `YEAR_MONTH` 문자열('2021-01')을 Plotly가 날짜로 자동 해석하면 데이터 없는 달에 시각적 공백 발생 → `go.Figure` 라인차트는 `date_range`로 빈 달 fill 후 datetime 축 사용, `px.line` 은 `.update_xaxes(type='category')` 적용
- UI에 노출되는 텍스트(subheader, metric, caption, 탭 레이블 등)는 **반드시 한글**만 사용 — 한자(분석·競·搜 등) 혼용 금지
- 취업률 미제공 유형(KDT 등) 처리: `no_empl_data` 플래그로 판별 → 관련 지표는 `"미제공"` 표시, 레이더 차트는 취업률 축 제거(3축), `st.stop()` 은 탭 내부에서 사용 금지(이후 탭 렌더링도 중단됨) → `if/else` 패턴 사용
- `TB_MARKET_TREND.TR_STA_DT`는 **`YYYY-MM-DD`** 형식으로 저장됨 → WHERE 절 날짜 파라미터는 반드시 `strftime('%Y-%m-%d')` 사용 (`YYYYMMDD` 형식 사용 시 문자열 비교 오류로 데이터 누락)
- **hrd_etl.py 3단계 스킵 로직**: ①종료 7개월 초과 + 미확정 훈련생 없음 → API 전체 스킵 ②종료 7일 초과 → 출결 스킵(명부는 업데이트) ③진행중/7일 이내 → 전체 수집. PostgreSQL `COUNT(*)` 쿼리는 반드시 `AS cnt` 별칭 사용 (`RealDictCursor`에서 `row[0]` 대신 `row['cnt']`)
- `home.py`에서 `st.navigation()` 사용 중 → pages/ 폴더 자동감지 비활성화. **새 페이지 추가 시 반드시 `pg = st.navigation([...])` 목록에도 수동 등록**

## 커밋 컨벤션

### 형식
```
Tag: English summary (한글 설명)
```

### 태그
| Tag | 용도 |
|---|---|
| `Feat` | 새로운 기능 추가 |
| `Fix` | 버그 수정 |
| `Docs` | 문서 변경 (README, CLAUDE.md 등) |
| `Style` | UI/CSS 변경, 코드 포맷팅 (기능 변경 없음) |
| `Refactor` | 코드 리팩토링 (기능 변경 없음) |
| `Chore` | 빌드, CI/CD, 설정 파일 변경 |

### 예시
```
Feat: Add attendance risk alert on dashboard (출결 위험군 알림 추가)
Fix: Correct completion rate calculation (수료율 계산 오류 수정)
Chore: Configure devcontainer with Node.js and pip cache (개발 컨테이너 환경 설정)
```

### 규칙
- 커밋 메시지에 `Co-Authored-By` 라인 포함하지 않음
- 영어 요약은 동사 원형으로 시작 (Add, Fix, Update, Remove 등)
- 한글 설명은 괄호 안에 간결하게

## 환경 변수 (.env)

- `HRD_API_KEY` - HRD-Net API 인증키
- `HANWHA_COURSE_ID` - 내부 관리 대상 과정 ID
- `DATABASE_URL` - PostgreSQL 연결 문자열 (없으면 SQLite 폴백)

## 파일 구조

| 파일 | 역할 |
|---|---|
| `utils.py` | DB 연결, adapt_query, load_data 등 공통 유틸 |
| `config.py` | 전역 설정 상수 (출결 기준, 캐시 TTL, ETL 파라미터 등) |
| `init_db.py` | 테이블 DDL (5개), 인덱스 10개, 마이그레이션 |
| `hrd_etl.py` | 내부 과정/훈련생/출결 데이터 수집 (배치 에러 폴백, ETL Summary) |
| `market_etl.py` | 시장 동향 데이터 수집 (30만건+, ThreadPool 에러 핸들링) + ETL 완료 후 `compute_and_cache_aggregations()` 자동 실행 |
| `home.py` | 메인 대시보드 (프로젝트 소개 블록, KPI, 운영 평균 성과, 기수 기록, 시장 포지셔닝, 오늘의 출결 현황) |
| `pages/1_*.py` | 시장 분석 & 기회 발굴 9탭 (헬퍼 함수 2개, 사업기회 발굴 탭 포함) |
| `pages/2_*.py` | 과정 성과 분석 6탭 + 전체 비교 모드 (인구통계, 요일별, 지각, 체류시간, 출결이탈, 학생별) |
| `pages/3_*.py` | 운영 현황 (출석률 게이지, 출결추이, 누적 위험지표) |
| `pages/4_*.py` | 매출 분석 (단위기간별 청구 4탭 + 전체 기수 비교) |
| `pages/DB_명세.py` | DB 명세 (수집현황 KPI, 테이블 상세, 20행 미리보기) |
| `tests/` | pytest 테스트 89개 (utils, config, init_db, hrd_etl, market_etl, billing) |

## DB 테이블

| 테이블 | 용도 | PK |
|---|---|---|
| `TB_COURSE_MASTER` | 과정 마스터 (회차별 운영 정보) | (TRPR_ID, TRPR_DEGR) |
| `TB_TRAINEE_INFO` | 훈련생 정보 | (TRPR_ID, TRPR_DEGR, TRNEE_ID) |
| `TB_ATTENDANCE_LOG` | 출결 로그 | UNIQUE(TRPR_ID, TRPR_DEGR, TRNEE_ID, ATEND_DT) |
| `TB_MARKET_TREND` | 시장 동향 (외부 과정 전체) | (TRPR_ID, TRPR_DEGR) |
| `TB_MARKET_CACHE` | 시장 집계 캐시 (ETL 후 pre-compute, JSON) | CACHE_KEY TEXT PK |

---

## HRD-Net API 명세

상세 파라미터/응답 필드는 [`API_SPEC.md`](./API_SPEC.md) 참조.

| API | 용도 | URL (끝부분) |
|---|---|---|
| API 1 | 훈련과정 목록 조회 | `HRDPOA60_1.jsp` |
| API 2 | 훈련일정 상세 조회 | `HRDPOA60_3.jsp` |
| API 3 | 훈련생 출결정보 조회 | `HRDPOA60_4.jsp` |
