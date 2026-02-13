# CLAUDE.md - 프로젝트 컨텍스트

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

### ETL 자동화 (GitHub Actions)
- `.github/workflows/hrd_etl.yml` - 평일 KST 09:00~18:00 매시간
- `.github/workflows/market_etl.yml` - 매일 KST 21:00
- Secrets: `HRD_API_KEY`, `HANWHA_COURSE_ID`, `DATABASE_URL`

### 홈 페이지 (home.py)
- KPI 요약 (총 과정수, 누적 수강생, **수료율**, 취업률)
- **오늘의 출결 현황** 섹션: 입실중 재분류(IN_TIME 있으면 결석→입실중), 출석률/입실중/결석/지각/조퇴 KPI 6칸 + 기수별 출석률 바차트
- 연도별 운영 규모, 우수 성과 Top 5, 현재 운영 중 과정 테이블 (수강신청/수강인원/수료인원/수료율)

### 기수별 분석 페이지 (pages/1_*.py) 구조
- **2가지 모드**: 개별 기수 분석 / 전체 기수 비교 (st.radio 전환)
- 개별 기수 분석 **6개 탭**: 인구통계(+유형별 성과) / 요일별 출결 패턴(히트맵) / 시간대별 지각 분포 / **체류시간 분석**(IN_TIME~OUT_TIME) / 출결·이탈 / 학생별 출결 현황(출석률 프로그레스바)
- 전체 기수 비교: 수료율/취업률 바차트, 결석 건수, 출석률 추이 라인차트, 종합 비교 테이블

### 진행과정 관리 페이지 (pages/2_*.py) 구조
- 대시보드형 UI: 출석률 게이지 + KPI 8개
- 보고용 텍스트 (expander)
- **최근 출결 추이** 미니차트 (최근 10일, 90% 기준선)
- **누적 출결 위험 지표**: 결석 3회+, 지각 5회+, 조퇴 5회+ 위험군 자동 감지
- 상세 탭: 미퇴실/특이사항, 결석자, 전체 출석부

### 데이터 감사 페이지 (pages/3_*.py) 구조
- **4개 탭**: 과정 운영 현황 / 훈련생 명부 / 출결 기록부 / **시장 동향 데이터** (TB_MARKET_TREND 500건 미리보기 + CSV 다운로드)
- 수동 ETL 실행 버튼

### 시장 동향 페이지 (pages/4_*.py) 구조
- **12개 탭**: 시장 개요 / 순위&모집 / **우리 과정 vs 시장** / 유형·일정 / 비용·성과 / **시계열 트렌드** / **경쟁 심화도** / **비용 대비 성과** / 경쟁 현황 / 키워드 / **자격증 분석** / 데이터 조회
- **헬퍼 함수 3개**: `render_ranking_table()`, `render_agg_bar_chart()`, `render_scatter_with_overlay()` — 탭 간 중복 코드 통합
- `load_internal_courses()`: TB_COURSE_MASTER 캐시 로드 (HANWHA_COURSE_ID 기반)
- 내부 과정 NCS 코드는 TB_MARKET_TREND와 TRPR_ID merge로 매칭
- `scikit-learn` LinearRegression: 비용→취업률 시뮬레이터
- **자격증 분석**: CERTIFICATE 컬럼 파싱 → 자격증별 과정수/취업률 Top 20
- **회차별 상세 비교**: 정원/수강신청인원/수료인원/수료율/취업률 (모집률 clip 제거)
- HANWHA_COURSE_ID 미설정 시 시장 전체 분석만 표시 (st.info 안내)

### 설정 상수 (config.py)
- 출결 기준, 위험군 임계값, 캐시 TTL, ETL 파라미터, 시장 분석 상수를 중앙 관리
- 모든 페이지에서 `from config import ...`으로 참조 (하드코딩 금지)

### 테스트 (tests/)
- `pytest` 기반, 56개 테스트
- `conftest.py`: `_NoCloseConnection` 프록시로 인메모리 SQLite fixture 제공
- `test_utils.py`, `test_config.py`, `test_init_db.py`, `test_hrd_etl.py`, `test_market_etl.py`
- 실행: `python -m pytest tests/ -v`

### 주의사항
- ETL 파일(hrd_etl.py, market_etl.py)에서 모듈 최상위 레벨에 `exit()` 사용 금지 (Streamlit import 시 앱 종료됨)
- 모든 SQL 쿼리는 `adapt_query()`를 거쳐야 PG 호환
- 페이지에서 직접 `pd.read_sql()` 대신 `load_data()` 사용 권장
- Plotly `add_vline`에 문자열 x값 + `annotation_text` 동시 사용 시 TypeError 발생 → `add_annotation` 별도 호출

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
| `init_db.py` | 테이블 DDL, 인덱스 6개, 마이그레이션 |
| `hrd_etl.py` | 내부 과정/훈련생/출결 데이터 수집 (배치 에러 폴백, ETL Summary) |
| `market_etl.py` | 시장 동향 데이터 수집 (30만건+, ThreadPool 에러 핸들링) |
| `home.py` | 메인 대시보드 (KPI 요약, 오늘의 출결 현황) |
| `pages/1_*.py` | 기수별 분석 6탭 + 전체 비교 모드 (인구통계, 요일별, 지각, 체류시간, 출결이탈, 학생별) |
| `pages/2_*.py` | 진행과정 관리 (출석률 게이지, 출결추이, 누적 위험지표) |
| `pages/3_*.py` | 데이터 감사 4탭 (과정/훈련생/출결/시장동향) |
| `pages/4_*.py` | 시장 동향 분석 12탭 (헬퍼 함수 3개로 중복 제거) |
| `tests/` | pytest 테스트 56개 (utils, config, init_db, hrd_etl, market_etl) |

## DB 테이블

| 테이블 | 용도 | PK |
|---|---|---|
| `TB_COURSE_MASTER` | 과정 마스터 (회차별 운영 정보) | (TRPR_ID, TRPR_DEGR) |
| `TB_TRAINEE_INFO` | 훈련생 정보 | (TRPR_ID, TRPR_DEGR, TRNEE_ID) |
| `TB_ATTENDANCE_LOG` | 출결 로그 | UNIQUE(TRPR_ID, TRPR_DEGR, TRNEE_ID, ATEND_DT) |
| `TB_MARKET_TREND` | 시장 동향 (외부 과정 전체) | (TRPR_ID, TRPR_DEGR) |

---

## HRD-Net API 명세

### 공통 사항

- **Base Domain:** `https://hrd.work24.go.kr`
- **인증:** 모든 요청에 `authKey` 파라미터 필수
- **인코딩:** UTF-8

---

### API 1: 훈련과정 목록 조회

1. 요청 URL
   1.1) URL
   https://hrd.work24.go.kr/jsp/HRDP/HRDPO00/HRDPOA60/HRDPOA60_1.jsp
   1.2) 사용예제
   예제1) 기본조건만을 이용하여 검색하는 경우

https://hrd.work24.go.kr/jsp/HRDP/HRDPO00/HRDPOA60/HRDPOA60_1.jsp?authKey=[인증키]&returnType=XML&outType=1&pageNum=1&pageSize=20&srchTraStDt=20141001&srchTraEndDt=20141231&sort=ASC&sortCol=2

예제2) 선택조건을 추가하는 경우

https://hrd.work24.go.kr/jsp/HRDP/HRDPO00/HRDPOA60/HRDPOA60_1.jsp?authKey=[인증키]&returnType=XML&outType=1&pageNum=1&pageSize=20&srchTraStDt=20141001&srchTraEndDt=20141231&srchTraArea1=[훈련지역 대분류]&sort=ASC&sortCol=2

2. 요청 Parameters
   요청 Parameters의 항목, 타입, 필수여부 및 설명 제공
   항목 타입 필수여부 설명
   파라미터명 string 선택,필수 해당필드에 대한 설명
   authKey string 필수 인증키(ex : authKey=[인증키]
   returnType string 필수 리턴타입:XML, JSON 중 하나로 지정합니다.(ex : returnType=XML)
   outType string 필수 구분자 : 출력형태('1':리스트 '2':상세)
   pageNum string 필수 시작페이지. 기본값 1, 최대 1000 검색 시작위치를 지정할 수 있습니다. 최대 1000 까지 가능.
   pageSize string 필수 페이지당 출력건수, 기본값 10, 최대 100까지 가능.
   wkendSe string 선택 주말/주중 구분 주말 : 1
   주말, 주중 혼합 : 2
   주중 : 3
   해당없음 : 9

- 전체일 경우에는 옵션 파라미터의 미등록처리
  srchTraArea1 string 선택 훈련지역 대분류 '11' : 서울, '26' : 부산, '27' : 대구, '28' : 인천 '29' : 광주, '30' : 대전, '31' : 울산, '36' : 세종, '41' : 경기, '43' : 충북, '44' : 충남, '45' : 전북, '46' : 전남, '47' : 경북, '48' : 경남, '50' : 제주, '51' : 강원
- 전체일 경우에는 옵션 파라미터의 미등록처리
  srchTraArea2 string 선택 훈련지역 중분류
  훈련지역 대분류에 따라 내용이 달라짐 \* 해당 코드관련 API 제공
- 훈련지역 대분류 제외시 이항목도 옵션 파라미터에 미등록처리
  srchNcs1 string 선택 NCS 직종 1차분류 코드 '01' : 사업관리
  '02' : 경영/회계/사무
  '03' : 금융/보험
  '04' : 교육/자연/사회과학
  '05' : 법률/경찰/소방/교도/국방
  '06' : 보건/의료
  '07' : 사회복지/종교
  '08' : 문화/예술/디자인/방송
  '09' : 운전/운송
  '10' : 영업판매
  '11' : 경비/청소
  '12' : 이용/숙박/여행/오락/스포츠 '13' : 음식서비스
  '14' : 건설
  '15' : 기계
  '16' : 재료
  '17' : 화학/바이오
  '18' : 섬유/의복
  '19' : 전기/전자
  '20' : 정보통신
  '21' : 식품가공
  '22' : 인쇄/목재/가구/공예
  '23' : 환경/에너지/안전
  '24' : 농림어업
- 전체일 경우에는 옵션 파라미터의 미등록처리
  srchNcs2 string 선택 NCS 직종 2차분류 코드
  상위분류에 따라 내용이 달라짐 \* 해당 코드관련 API 제공
- 전체일 경우에는 옵션 파라미터의 미등록처리
  srchNcs3 string 선택 NCS 직종 3차분류 코드
  상위분류에 따라 내용이 달라짐 \* 해당 코드관련 API 제공
- 전체일 경우에는 옵션 파라미터의 미등록처리
  srchNcs4 string 선택 NCS 직종 4차분류 코드
  상위분류에 따라 내용이 달라짐 \* 해당 코드관련 API 제공
- 전체일 경우에는 옵션 파라미터의 미등록처리
  crseTracseSe string 선택 훈련유형 'C0061' : 국민내일배움카드(일반)
  'C0061S' : 국민내일배움카드(주 훈련대상 : 구직자)
  'C0061I' : 국민내일배움카드(주 훈련대상 : 재직자)
  'C0054' : 국가기간전략산업직종
  'C0055C' : 과정평가형훈련
  'C0054G' : 기업맞춤형훈련
  'C0054Y' : 스마트혼합훈련
  'C0054S' : 일반고특화훈련
  'C0104' : K-디지털 트레이닝
  'C0105' : K-디지털 기초역량훈련
  'C0102' : 산업구조변화대응
  'C0055' : 실업자 원격훈련
  'C0031' : 근로자 원격훈련
  'C0031C' : 돌봄서비스훈련
  'C0031F' : 근로자 외국어훈련
- 전체일 경우에는 옵션 파라미터의 미등록처리
  srchTraGbn string 선택 훈련구분코드 'M1001' : 일반과정
  'M1005' : 인터넷과정
  'M1010' : 혼합과정(BL)
  'M1014' : 스마트혼합훈련
- 전체일 경우에는 옵션 파라미터의 미등록처리
  srchTraType string 선택 훈련종류(훈련구분에 따라 세부내용이 변경됨) '\* 해당 코드관련 API 제공
- 훈련지역 대분류 제외시 이항목도 옵션 파라미터에 미등록처리
- 인터넷과정과 혼합과정의 경우 세부항목이 없음
  srchTraStDt string 필수 훈련시작일 From
  srchTraEndDt string 필수 훈련시작일 To
  srchTraProcessNm string 선택 훈련과정명
  srchTraOrganNm string 선택 훈련기관명
  sort string 필수 정렬방법 "ASC",
  "DESC"
  sortCol string 필수 정렬컬럼 훈련기관명 : 1
  훈련시작일 : 2
  훈련기관 직종별 취업률 : 3
  만족도점수 : 5

3. 출력결과
   출력결과의 출력항목, 타입, 설명, 비고에 대한 정보 제공
   출력항목 타입 설명 비고
   <HRDNet> XML문서의 최상위 노드입니다.
   <scn_cnt> string 검색된 총 건수 </scn_cnt>
   <pageNum> string 현재페이지 </pageNum>
   <pageSize> string 페이지당 출력개수, 페이지당 표현될 자료의 개수 </pageSize>
   <srchList> string
   <scn_list>
   ADDRESS string 주소 <address></address>
   CERTIFICATE string 자격증 <certificate></certificate>
   CONTENTS string 컨텐츠 <contents></contents>
   COURSE_MAN string 수강비 <courseMan></courseMan>
   EI_EMPL_CNT3 string 고용보험3개월 취업인원 수 <eiEmplCnt3></eiEmplCnt3>
   EI_EMPL_CNT3_GT10 string 고용보험3개월 취업누적인원 10인이하 여부 (Y/N)
   17.11.07부터 제공되지 않는 항목이나 기존 API 사용자를 위해 Null값을 제공 <eiEmplCnt3Gt10></eiEmplCnt3Gt10>
   EI_EMPL_RATE3 string 고용보험3개월 취업률 <eiEmplRate3></eiEmplRate3>
   EI_EMPL_RATE6 string 고용보험6개월 취업률 <eiEmplRate6></eiEmplRate6>
   GRADE string 등급 <grade></grade>
   INST_CD string 훈련기관 코드 <instCd></instCd>
   NCS_CD string NCS 코드 <ncsCd></ncsCd>
   REAL_MAN string 실제 훈련비 <realMan></realMan>
   REG_COURSE_MAN string 수강신청 인원 <regCourseMan></regCourseMan>
   STDG_SCOR string 만족도 점수 <stdgScor></stdgScor>
   SUB_TITLE string 부 제목 <subTitle></subTitle>
   SUB_TITLE_LINK string 부 제목 링크 <subTitleLink></subTitleLink>
   TEL_NO string 전화번호 <telNo></telNo>
   TITLE string 제목 <title></title>
   TITLE_ICON string 제목 아이콘 <titleIcon></titleIcon>
   TITLE_LINK string 제목 링크 <titleLink></titleLink>
   TRA_END_DATE string 훈련종료일자 <traEndDate></traEndDate>
   TRA_START_DATE string 훈련시작일자 <traStartDate></traStartDate>
   TRAIN_TARGET string 훈련대상 <trainTarget></trainTarget>
   TRAIN_TARGET_CD string 훈련구분 <trainTargetCd></trainTargetCd>
   TRAINST_CST_ID string 훈련기관ID <trainstCstId></trainstCstId>
   TRNG_AREA_CD string 지역코드(중분류) <trngAreaCd></trngAreaCd>
   TRPR_DEGR string 훈련과정 순차 <trprDegr></trprDegr>
   TRPR_ID string 훈련과정ID <trprId></trprId>
   WKED_SE string 주말/주중 구분 <wkendSe></wkendSe>
   YARD_MAN string 정원 <yardMan></yardMan>
   </scn_list>
   </srchList>
   </HRDNet>

### API 2: 훈련일정 상세 조회

1. 요청 URL
   1.1) URL
   https://hrd.work24.go.kr/jsp/HRDP/HRDPO00/HRDPOA60/HRDPOA60_3.jsp
   1.2) 사용예제
   예제1) 기본조건만을 이용하여 검색하는 경우

https://hrd.work24.go.kr/jsp/HRDP/HRDPO00/HRDPOA60/HRDPOA60_3.jsp?authKey=[인증키]&returnType=XML&outType=2&srchTrprId=[훈련과정ID]&srchTrprDegr=[훈련회차]&srchTorgId=[훈련기관ID]

2. 요청 Parameters
   요청 Parameters의 항목, 타입, 필수여부 및 설명 제공
   항목 타입 필수여부 설명
   파라미터명 string 선택,필수 해당필드에 대한 설명
   authKey string 필수 인증키(ex : authKey=[인증키]
   returnType string 필수 리턴타입:XML, JSON 중 하나로 지정합니다. (ex : returnType=XML)
   outType string 필수 구분자 : 출력형태('1':리스트 '2':상세) '2'
   srchTrprId string 필수 훈련과정 ID
   srchTrprDegr string 선택 훈련과정 회차(입력하지 않으면 모든 회차가 조회됩니다.)
3. 출력결과
   출력결과의 출력항목, 타입, 설명, 비고에 대한 정보 제공
   출력항목 타입 설명 비고
   <HRDNet> XML문서의 최상위 노드입니다.
   <scn_list> string
   EI_EMPL_RATE_3 string 3개월 고용보험 취업률(%)
   'A‘ : 개설예정
   ‘B‘ : 진행중
   ‘C‘ : 미실시
   ‘D‘ : 수료자 없음
   숫자 : 취업률 <eiEmplRate3></eiEmplRate3>
   EI_EMPL_CNT_3 string 3개월 고용보험 취업인원 <eiEmplCnt3></eiEmplCnt3>
   EI_EMPL_RATE_6 string 6개월 고용보험 취업률(%)
   'A‘ : 개설예정
   ‘B‘ : 진행중
   ‘C‘ : 미실시
   ‘D‘ : 수료자 없음
   숫자 : 취업률 <eiEmplRate6></eiEmplRate6>
   EI_EMPL_CNT_6 string 6개월 고용보험 취업인원 <eiEmplCnt6></eiEmplCnt6>
   HRD_EMPL_RATE_6 string 6개월 고용보험 미가입 취업률(%)
   'A‘ : 개설예정
   ‘B‘ : 진행중
   ‘C‘ : 미실시
   ‘D‘ : 수료자 없음
   숫자 : 취업률 <hrdEmplRate6></hrdEmplRate6>
   HRD_EMPL_CNT_6 string 6개월 고용보험 미가입 취업인원 <hrdEmplCnt6></hrdEmplCnt6>
   INST_INO string 훈련기관관리번호 <instIno></instIno>
   2 string 모집인원(정원) <totFxnum></totFxnum>
   TOT_PAR_MKS string 수강인원 <totParMks></totParMks>
   FINI_CNT string 수료인원 <finiCnt></finiCnt>
   TOT_TRCO string 총 훈련비 <totTrco></totTrco>
   TOT_TRP_CNT string 수강(신청) 인원 <totTrpCnt></totTrpCnt>
   TR_END_DT string 훈련종료일 <trEndDt></trEndDt>
   TR_STA_DT string 훈련 시작일 <trStaDt></trStaDt>
   TRPR_DEGR string 훈련과정 회차 <trprDegr></trprDegr>
   TRPR_ID string 훈련과정ID <trprId></trprId>
   TRPR_NM string 훈련과정명 <trprNm></trprNm>
   </scn_list> string
   </HRDNet>

### API 3: 훈련생 출결정보 조회

1. 요청 URL
   1.1) URL
   https://hrd.work24.go.kr/jsp/HRDP/HRDPO00/HRDPOA60/HRDPOA60_4.jsp
   1.2) 사용예제
   예제1) 기본조건만을 이용하여 검색하는 경우

https://hrd.work24.go.kr/jsp/HRDP/HRDPO00/HRDPOA60/HRDPOA60_4.jsp?authKey=[인증키]&returnType=XML&outType=2&srchTrprId=[훈련과정ID]&srchTrprDegr=[훈련회차]&srchTorgId=[훈련기관ID]

2. 요청 Parameters
   요청 Parameters의 항목, 타입, 필수여부 및 설명 제공
   항목 타입 필수여부 설명
   파라미터명 string 선택,필수 해당필드에 대한 설명
   authKey string 필수 인증키(ex : authKey=[인증키]
   returnType string 필수 리턴타입:XML, JSON 중 하나로 지정합니다. (ex : returnType=XML)
   srchTrprId string 필수 훈련과정 ID
   srchTrprDegr string 필수 훈련과정 회차
   srchTorgId string 선택,필수 출결 상세정보
3. 출력결과
   출력결과의 출력항목, 타입, 설명, 비고에 대한 정보 제공
   출력항목 타입 설명 비고
   <HRDNet> XML문서의 최상위 노드입니다.
   <atab_cnt> 훈련생 출결리스트 수
   <totTrneeCo> 총훈련생수
   <trne_cnt> 훈련생 수
   <trneList> string
   <trne_list>
   ABSENT_CNT string 결석일수 <absentCnt></absentCnt>
   ATEND_CNT string 출석일수 <atendCnt></atendCnt>
   LIFYEAMD string 훈련생 생년월일 <lifyeaMd></lifyeaMd>
   OFLHD_CNT string 공가일수 <oflhdCnt></oflhdCnt>
   TRACSE_ID string 과정 ID <tracseId></tracseId>
   TRACSE_TME string 과정 회차 <tracseTme></tracseTme>
   TRANING_DE_CNT string 훈련일수 <traingDeCnt></traingDeCnt>
   CSTMR_ID string 훈련생 코드 <trneeCstmrId></trneeCstmrId>
   CSTMR_NM string 훈련생 이름 <trneeCstmrNm></trneeCstmrNm>
   STTUS_NM string 훈련생 상태 <trneeSttusNm></trneeSttusNm>
   TRANEE_TRACSE_SE string 훈련생 유형 <trneeTracseSe></trneeTracseSe>
   VCATN_CNT string 휴가일수 <vcatnCnt></vcatnCnt>
   </trne_list>
   </trneList>
   <atabList>
   <atab_list>
   ATEND_DE string 출석일 <atendDe></atendDe>
   ATEND_STTUS_NM string 출결여부 <atendSttusNm></atendSttusNm>
   CSTMR_NM string 훈련생 이름 <cstmrNm></cstmrNm>
   KOR_DAY_NM string 훈련요일 <korDayNm></korDayNm>
   LEVROM_TIME string 퇴실시간 <levromTime></levromTime>
   LPSIL_TIME string 입실시간 <lpsilTime></lpsilTime>
   CSTMR_ID string 훈련생 코드 <trneeCstmrId></trneeCstmrId>
   </atabList>
   <atabList>
   </HRDNet>
