# CLAUDE.md

## Commands

```bash
streamlit run home.py           # 앱 실행 (port 8501)
python -m pytest tests/ -v      # 전체 테스트
python init_db.py               # DB 스키마 초기화 (최초 1회)
python hrd_etl.py               # 내부 과정/출결 ETL (수동 실행)
python market_etl.py            # 시장 동향 ETL (40만+ records)
python saramin_etl.py           # 채용공고 ETL (사람인 API, 일일 500회 제한)
python build_home_snapshot.py   # 홈 확정 스냅샷 재생성 (취업률 확정 시 1회)
```

## ⚠️ 한글 코드포인트 규칙 (CRITICAL)

**"분석"의 "석"은 반드시 한글 U+C11D (HANGUL SYLLABLE SEOG) 을 사용한다.**
- ✅ 올바름: 분석 = U+BD84(분) + U+C11D(석)
- ❌ 금지: 분析 — 析은 U+6790 (CJK 한자)으로 전혀 다른 문자

파일 경로, 코드 문자열, 응답 텍스트 모두 적용. Claude는 析(U+6790)을 쓰는 경향이 있으므로 반드시 주의.

## 프로젝트 개요

HRD-Net 공공데이터 기반 훈련 과정 성과 분석 대시보드 (Streamlit + PostgreSQL)

## 아키텍처

```
[GitHub Actions]                [Supabase]              [Streamlit Cloud]
hrd_etl.py (평일 매시간)  →   메인 DB          ←    대시보드 (읽기 전용)
market_etl.py (매일 21시) →   시장 DB(TB_MARKET_TREND) ← https://playdata.streamlit.app
saramin_etl.py (매일 04:43)→                    ←    운영 현황: hrd_api.py로 API 직접 호출
                                                     (기관 병렬 조회, 전체 상한 120초, 실패 시 DB 폴백)
```

### 홈 화면 = 확정 스냅샷 (2026-07 전환)
- `home.py`는 DB를 조회하지 않음 — 커밋된 `data/home_snapshot.json`(확정 스냅샷)만 렌더
- 갱신: `python build_home_snapshot.py` → 테스트 → 커밋 (취업률 확정 시 1회 예정, 상세 배경은 docs/DEV_LOG.md 2026-07-14)
- 벤치마크(60.5·85.7·90.3)·누적 매출 헤드라인(104.1억)은 원장 확정값 고정(`LEDGER_*` 상수) — 시장 데이터 증가에 따른 재계산 드리프트 방지
- 상세 페이지(pages/)는 기존대로 DB 동적 조회

### DB 연결 (PostgreSQL, 2026-07 폴백 제거 · 2026-09 시장 DB 분리)
- 런타임은 PostgreSQL (Supabase) 전용. `DATABASE_URL`이 없으면 `utils.get_connection()`이
  `DatabaseNotConfiguredError`로 즉시 실패 — 빈 SQLite 파일을 만들던 조용한 폴백은 제거됨
- **DB는 둘**: 메인(`DATABASE_URL`)과 시장(`DATABASE_URL_MARKET`). `TB_MARKET_TREND`(46만 행, 무료 한도의 90%)만
  두 번째 Supabase 프로젝트에 산다. `TB_MARKET_CACHE`는 ETL 3종이 공유하므로 메인에 남김 (`utils.MARKET_TABLES`)
  - `get_connection(db=)`·`load_data(db=)`·`_get_pg_pool(db=)`. `load_data`는 db 생략 시 쿼리 본문에
    시장 테이블이 있으면 자동으로 시장 DB — **시장 테이블과 다른 테이블의 JOIN·서브쿼리는 불가** (build_home_snapshot 참고)
  - `DATABASE_URL_MARKET` 미설정이면 메인으로 폴백 → 로컬·CI·분리 전 환경은 예전 단일 DB로 동작.
    `is_market_db_separate()`로 실제 분리 여부 판단 (market_etl이 경고 로그)
  - `init_all_tables(include_market=True)`: hrd_etl·saramin_etl은 `include_market=False` — 시장 URL 없는 환경에서
    메인 DB에 빈 TB_MARKET_TREND를 되살리지 않기 위함. 시장 DDL은 `init_market_tables()`
  - 마이그레이션: `scripts/migrate_market_db.py copy → verify → drop` (docs/DEV_LOG.md 2026-09-15)
- **`is_pg()`·`adapt_query()`·ETL의 `execute_batch` vs `executemany` 분기는 유지 — 제거 금지.**
  pytest가 인메모리 SQLite로 돌기 때문에 `?` 플레이스홀더 패스스루가 필요함
- `adapt_query()`: `?` → `%s`, `INSERT OR IGNORE` → `ON CONFLICT DO NOTHING` 자동 변환
- PostgreSQL 컬럼명 소문자 반환 → `load_data()`에서 대문자 변환 처리

### 실시간 조회 (`hrd_api.py`, 운영 현황 전용)
- 기관 (인증키, 과정ID) 쌍을 **병렬** 조회. 순차로 두면 요청 최악 46초(재시도 2회 포함)가
  기관 수만큼 누적돼 화면이 수 분간 멈춤 — `config.API_TOTAL_DEADLINE`(120초) 전체 상한 필수
- **상한은 `sum(API_TIMEOUT) * 2` 이상이어야 함** — 한 기관이 과정목록 → (명부·출결) 2단계
  순차라 그보다 짧으면 느리지만 정상인 조회가 잘림 (`tests/test_hrd_api.py`가 강제)
- 폴백은 정상 동작이라 예외가 화면까지 안 감 → `get_last_realtime_error()`로 사유를 남기고
  진단 패널에 노출. **부분 실패도 기록** — 살아남은 기관이 빈 결과면 거짓 안내가 되기 때문
- 상한 초과 시 `executor.shutdown(wait=False, cancel_futures=True)`로 즉시 반환.
  **`with ThreadPoolExecutor` 사용 금지** — `shutdown(wait=True)`라 상한을 넘겨도 끝까지 기다림
- `get_active_data_with_fallback()` source 3종: `"API"` / `"DB"`(키 미설정) / `"DB_FALLBACK"`(API 실패)
- **운영 현황은 응답 속도를 위해 실시간 API를 주 경로로 삼는다 (의도된 설계).** DB 폴백은
  보조 수단이며, `hrd_etl.py`가 `config.ETL_COURSE_ID`(한화) 하나만 수집하므로 엔코아 등 타 기관 과정은
  DB에 없다 — **폴백이 비는 것은 정상**이고, ETL 확장으로 메울 대상이 아니다
- 따라서 폴백이 비었을 때 "운영 중인 과정 없음"으로 표시하면 거짓 안내가 된다.
  `DB_FALLBACK`·`realtime_error`를 구분해 "실시간 조회 실패"임을 명확히 알릴 것

### ETL 자동화
- `hrd_etl.yml` — cron은 평일 KST 09:00~18:00 매시간이지만 **GitHub 예약 실행이 지연·누락돼 실제로는 하루 2회(약 13:30·18:35 KST)만 돈다** (2026-09 실측, 2주 이상 일관). 정확한 주기가 필요하면 외부 트리거(workflow_dispatch API 호출)로 바꿀 것. 4단계: `hrd_etl.py`(한화 출결) → `kpi_etl.py`(회차 스냅샷 TB_COURSE_SNAPSHOT 변화 시만 기록 + 명부 사람 스냅샷 TB_ROSTER_MEMBER: 승인 감지·상태 변화·첫 참석일. 참석 판정 = 입실 시간 있거나 출석 계열 상태) → `notion_applicants_etl.py`(신청자 리스트 미러 TB_APPLICANT + 전이 로그) → `notion_registry_publish.py`(우리 소유 노션 「HRD 등록자 관리」 페이지: 「기수」 표(API 신청인원 vs 노션 수집 등록 인원·개강 참석률) + 「등록자」 DB(한 번이라도 HRD 등록한 사람 = 노션 HRD신청·HRD등록 이력 ∪ 명부, 기수 관계로 연결). 2026-09-21부터 SKN 포함, `config.NOTION_REGISTRY_SINCE` 이후 개강 회차만. 노션 쓰기 공용 헬퍼(요청·DB 생성/속성 관리·값 변환·해시 upsert)는 `notion_publish.py`. 2026-09-15~21의 「모집 KPI」 페이지·`notion_kpi_publish.py`는 삭제됨). 뒤 셋은 `if: always()`, NOTION_TOKEN 필요. **노션 쓰기는 이 페이지 하나뿐, 담당자 DB는 항상 읽기만**. 신청자 리스트는 `config.NOTION_APPLICANT_SOURCES`(AI·SKN)를 데이터 소스 API(2025-09-03)로 읽는다 — SKN DB는 데이터 소스가 둘이라 2022 API·관계 속성 모두 거부됨
- `kpi_poll.yml` — **예약 없음, 노션 웹훅 전용**: 노션 API 웹훅 → `supabase/functions/notion-relay`(서명 검증 → 최종결과가 HRD등록으로 바뀐 경우만, AI·SKN 두 DB) → workflow_dispatch. 3단계(`kpi_etl.py --kpi-only`(KPI 과정 중 종료되지 않은 회차 명부만) → notion_applicants_etl → notion_registry_publish, 약 3분), `concurrency: kpi-poll`로 겹침 방지. 설정 절차는 `recruit-kpi/docs/SETUP_REALTIME.md`. "한 번이라도 HRD 등록"은 명부 스냅샷(사라져도 행 유지) + 노션 전이 로그로 모은다
- 디스코드 알림(`notify.py`): `DISCORD_WEBHOOK_URL`이 있으면 kpi_etl(승인 감지·명부 이탈)과 notion_applicants_etl(HRD신청·HRD등록·합격취소 전이)이 실행당 한 메시지. 없으면 무동작. **가린 이름 + 기수만** 보낸다
- `market_etl.yml` — 매일 KST 21:00
- `saramin_etl.yml` — 매일 KST 04:43 (사람인 채용공고, 정각 회피로 지연 최소화)

### CI
- `tests.yml` — push(main)/PR 시 Python 3.12로 `pytest tests/` 실행
- **`DATABASE_URL`을 주입하지 않음** — CI엔 `.env`가 없어 `is_pg()`가 False가 되고 인메모리
  SQLite로 돌아야 정상. 주입하면 ETL 테스트가 psycopg2 경로로 새어 검증이 무력화됨

### 사람인 ETL 수집 전략 (`saramin_etl.py`, 2026-09 과정 트랙 개편)

채용 동향은 **AI캠퍼스 3개 과정(MLE 머신러닝캠프 · AIO 멀티에이전트 · MLO AI Ready 데이터 + COMMON 공통)의
취업 방향별** 공고를 보여준다. 수집(어떤 쿼리로 가져오나)과 분류(어느 과정 공고인가)는 분리돼 있다 —
설계 근거·직무 방향은 `docs/track_job_mapping.md`.

| 항목 | 값 | 설정 위치 |
|---|---|---|
| **수집 쿼리** | 직무 코드 13개(`job_cd`: NLP 160·머신러닝 109·데이터엔지니어 83·DevOps 146·Kafka 241 등) + 키워드 29개(LLM·RAG·LangGraph·MLOps·Airflow 등) = 42개. **넓은 코드(백엔드 84·웹개발 87·Python 272)는 하루 110건에 절단되므로 수집이 아니라 분류에만 사용** | `config.SARAMIN_QUERIES` |
| **트랙 분류** | `tag_tracks()`가 보유 공고 **전량을 삭제 후 재태깅** → `TB_JOB_POSTING_TRACK` (규칙 바꾸면 소급 반영). 강한 신호 3점 · 보조 1점 · 임계 3. **코드 단독 매치는 강한 코드 포함 4점 이상 + 제목에 직무 단어 필수** — 사람인 태그는 기업이 넓게 붙여 코드 하나만으로는 오탐. 제외: IT개발·데이터 외, 알바·파견·교육생, 강사·마케팅·기획자·라벨링·세일즈 제목 | `config.SARAMIN_TRACK_RULES`, `SARAMIN_EXCLUDE_*`, `SARAMIN_CODE_ONLY_*` |
| **약어 정규식** | `LLM·RAG·NLP·ETL·MCP·AWS`는 대소문자 구분 + 영문자 비인접 조건(`(?<![A-Za-z])`) — `STORAGE`·`SETTLE` 오탐 방지. 한글 인접은 허용 | `config._ACR` |
| **신입 가능** | `EXPERIENCE_CD ∈ {0 경력무관, 1 신입, 3 신입/경력}` → `ENTRY_LEVEL=1`. API에 경력 필터가 없어 후처리 | `config.SARAMIN_ENTRY_LEVEL_CODES` |
| **쿼리당 건수** | 최대 110건/호출 (API 페이징 미지원, 도달 시 WARNING 로그). 1일 창 분할 | `config.SARAMIN_PAGE_SIZE` |
| **일일 API 호출** | 42 쿼리 × (3일 + 오늘) = **168회** (한도 480). `tests/test_saramin_etl.py`가 예산 초과를 막음 | `config.SARAMIN_API_CALL_LIMIT`, `SARAMIN_PUBLISHED_DAYS` |
| **정렬** | `pd` (게시일 최신순) | `saramin_etl.py` 고정 |
| **중복 처리** | `ON CONFLICT(JOB_ID) DO UPDATE` — 쿼리 간 중복 공고 자동 병합, `TB_JOB_POSTING_KEYWORD`에 수집 쿼리 라벨 전부 보존 | `saramin_etl.py` |
| **보존 정책** | 마감 후 30일 / 상시채용(마감일 1년 이상 미래)은 게시 후 90일 지나면 삭제. junction 3종(KEYWORD·REGION·TRACK) 함께 삭제 | `config.SARAMIN_RETENTION_*` |
| **누적 추이 캐시** | `saramin_track_monthly`(TRACK × YEAR_MONTH, `ALL` 행 포함)는 **누적 병합** — 같은 키는 max 채택. 원본이 삭제돼도 과거 추이 유지. **전체 재계산으로 되돌리면 삭제 시점에 추이 소실** | `saramin_etl.merge_cumulative()` |
| **캐시 집계** | 2종만: `saramin_track_monthly` · `saramin_query_hits`. **진행중 분포·목록은 페이지가 PG 직접 조회**(`UNNEST`·`STRING_AGG` 등 PG 전용 SQL — 트랙·신입 필터 조합이 많아 캐시 부적합). 구 캐시 키 11종은 집계 시 자동 삭제 | `saramin_etl.py`, `pages/채용_동향.py` |
| **실행 모드** | 기본(수집→삭제→태깅→집계) / `--cleanup-only`(삭제→태깅→집계) / `--tag-only`(태깅→집계, 규칙 조정 후 소급용). 뒤 둘은 API 쿼터 소모 없음 | `saramin_etl.py` |
| **저장 테이블** | `TB_JOB_POSTING` (33 컬럼, PK: `JOB_ID`), `TB_JOB_POSTING_KEYWORD`, `TB_JOB_POSTING_REGION`, `TB_JOB_POSTING_TRACK` | `init_db.py` |

## 주의사항

- **홈 수치는 스냅샷 고정**: DB를 갱신해도 홈 화면에는 반영되지 않음 → `build_home_snapshot.py` 재실행 후 `data/home_snapshot.json` 커밋 필요
- **adapt_query() 필수**: 모든 SQL 쿼리는 `adapt_query()` 통과 → PG 호환. 직접 `pd.read_sql()` 대신 `load_data()` 사용 권장
- **날짜 형식**: `TB_MARKET_TREND.TR_STA_DT` = `YYYY-MM-DD`. WHERE 절에 `strftime('%Y-%m-%d')` 사용 (`YYYYMMDD` 사용 시 데이터 누락)
- **PG COUNT**: `COUNT(*) AS cnt` 별칭 필수 (`RealDictCursor`에서 `row[0]` 불가)
- **exit() 금지**: ETL 파일 최상위 레벨에서 `exit()` 사용 시 Streamlit import 시 앱 종료됨
- **st.navigation() 수동 등록**: `home.py`에서 `st.navigation()` 사용 중 → pages/ 자동감지 비활성화. 새 페이지 추가 시 `pg = st.navigation([...])` 목록에도 수동 등록 필요
- **Plotly 시계열 끊김**: `YEAR_MONTH` 문자열 → `px.line`은 `.update_xaxes(type='category')`, `go.Figure`는 `date_range`로 빈 달 fill 후 datetime 축
- **Plotly add_vline**: 문자열 x값 + `annotation_text` 동시 사용 시 TypeError → `add_annotation` 별도 호출
- **수료 판정**: `TRNEE_STATUS.str.contains('수료|조기취업')` — HRD-Net 실제 값이 `'정상수료'`, `'80%이상수료'` 등이므로 `== '수료'` 정확일치 금지
- **취업률 미제공 유형**: `no_empl_data` 플래그로 판별 → `"미제공"` 표시. `st.stop()`은 탭 내부 사용 금지(이후 탭 렌더링 중단) → `if/else` 패턴 사용
- **UI 텍스트**: `st.subheader`, `st.metric`, 탭 레이블 등 화면 노출 텍스트는 한글만 사용 — 한자 혼용 금지
- **UI 용어 표기**: 지표 표시명은 `docs/GLOSSARY.md` 기준 준수. 수식어+지표 사이 공백 필수

## 핵심 지표 정의 (비즈니스 기준)

지표 계산 기준이 여러 파일에서 달라지지 않도록 여기에 명문화. 새 페이지 추가 시 반드시 준수.

### 출석률

**기준 파일**: `매출_분석.py` → 공통 함수 `utils.calc_attendance_rate()` / `calc_attendance_rate_from_counts()`

| 구분 | 내용 |
|---|---|
| **분모** | 전체 출결 기록 수 — **`중도탈락미출석` 제외** |
| **분자 (기본)** | `NOT_ATTEND_STATUSES` 제외한 기록 수 = 출석 + 지각 + 조퇴 + 외출 + 공가 등 |
| **NOT_ATTEND_STATUSES** | `{'결석', '중도탈락미출석', '100분의50미만출석'}` |
| **패널티** | 지각 + 조퇴 + 외출 누적 3개 → 가상 결석 1일 차감 |
| **최종 분자** | 기본 출석일 − (패널티 합계 // 3) |

> **왜 제외 방식?** 공가 종류(경조사, 공식행사 등)가 다양해 포함 목록 나열보다 불인정 목록만 정의하는 것이 정확.

**유지 예외**:
- `현재_운영_현황.py` 실시간 출석률: `입실중` 상태 추가 포함 (퇴실 전 실시간 특성)
- `매출_분석.py` 청구용 출석률: 동일 공식이 원본, 건드리지 않음

---

### 현재 재원 (운영 현황 전용)

**기준 파일**: `현재_운영_현황.py` — "현재 인원/총 재원"은 **실제 훈련중인 인원만** 집계.

| 구분 | 내용 |
|---|---|
| **재원 정의** | `훈련중`만. 수료·조기취업(`is_completed`)·중도탈락·제적은 **모두 제외** |
| **제외 판정** | `is_completed(status) \| status.str.contains('중도탈락\|제적')` (정확일치 금지) |
| **Tab1 현재 인원** | `개강 인원 − 제적 − 중도탈락 − 수료·조기취업` |

> **왜?** `조기취업`은 수료 계열(좋은 결과)이지만 코스를 이미 떠나 더 이상 출석하지 않음. 운영 현황의 "현재 재원"은 출석 관리 대상(훈련중)만 의미해야 정확. 출석률 분모(`total_cnt`)도 이 기준을 따름.

---

### 취업률

**기준 컬럼**: `TB_COURSE_MASTER` — HRD-Net API 수집값 (수료자 기준 %)

> **⚠️ 시장 취업률 데이터 한계**: `TB_MARKET_TREND`에도 `EI_EMPL_RATE_3/6` 컬럼이 있으나 전체 중 ~7.5%만 유효값 보유 (2026.07 기준, 대부분 NULL). 수료 후 3~6개월 경과해야 집계되는 API 특성상 최근 과정은 거의 비어있음. **시장 벤치마크 비교 시 취업률은 활용 불가** — 모집률·만족도만 비교 가능. 내부 과정 취업률은 `TB_COURSE_MASTER` 컬럼 그대로 활용 (위 표 참조).

| 지표 | 컬럼 | 설명 |
|---|---|---|
| **3개월 취업률** | `EI_EMPL_RATE_3` | 수료 후 3개월 고용보험 가입률 |
| **6개월 취업률 (EI)** | `EI_EMPL_RATE_6` | 수료 후 6개월 고용보험 가입률 |
| **6개월 취업률 (HRD)** | `HRD_EMPL_RATE_6` | 고용보험 미가입자 중 취업 확인 비율 |
| **6개월 합산 취업률** | `TOTAL_RATE_6` (계산) | `EI_EMPL_RATE_6 + HRD_EMPL_RATE_6` |

> `REAL_EMPL_RATE` 컬럼 = `EI_EMPL_RATE_3`와 동일값 (ETL 수집 시 복사). 컬럼명만 다름.

**특수값 처리** (`EMPL_CODE_MAP` in `config.py`):

| 코드 | 의미 |
|---|---|
| A | 개설예정 |
| B | 진행중 (아직 집계 안 됨) |
| C | 미실시 |
| D | 수료자 없음 |

- 특수코드가 하나라도 있으면 `TOTAL_RATE_6 = pd.NA` (0과 구분 필수)
- 공통 함수: `utils.calc_employment_rate_6(ei6, hrd6)` / `utils.parse_empl_rate(val)`

---

### 수료율

| 구분 | 공식 | 사용 위치 |
|---|---|---|
| **KPI 요약** | `Σ FINI_CNT / Σ TOT_PAR_MKS × 100` (전 기수 합산) | `home.py` — HRD-Net 집계값 기준, help에 기수 단순평균 병기 |
| **개별 기수 상세** | `TRNEE_STATUS.str.contains('수료\|조기취업').sum() / 총원` | `종료과정_성과.py` — 훈련생 개별 상태 재집계 |

두 값은 HRD-Net 데이터 수집 시점 차이로 미세하게 다를 수 있음. 둘 다 올바른 값.

> **수료 판정 주의**: `== '수료'` 정확일치 금지. HRD-Net 실제값은 `'정상수료'`, `'80%이상수료'` 등 → 반드시 `.str.contains('수료|조기취업')` 사용.

---

### 모집률

| 구분 | 내용 |
|---|---|
| **공식** | 신청인원 / 정원 × 100 |
| **상한** | 100% (초과분 clip) |
| **정원 0** | NA 처리 |
| **월별 평균** | 신청인원 0명 과정 제외 |

**공통 함수**: `utils.calc_recruit_rate()`

---

### 등록 대비 승인률 (모집 퍼널)

**기준 파일**: `HRD등록_개강참석률.py` (파일명은 URL 유지, 화면 제목 "HRD 등록 대비 승인률") — HRD-Net 훈련일정 상세 API(`_3.jsp`) 집계값 + 명부 스냅샷.

| 단계 | API 필드 | DB 컬럼 | 의미 |
|---|---|---|---|
| 정원 | `totFxnum` | `TOT_FXNUM` | 승인 정원 |
| 수강신청 | `totTrpCnt` | `TOT_TRP_CNT` | HRD 등록(수강신청) 인원 (누적인지 현재값인지 미확정 — TB_COURSE_SNAPSHOT 이력으로 판별 예정) |
| **승인 인원** | `totParMks` | `TOT_PAR_MKS` | 기관 승인 = 확정 신고 인원 = 명부 건수. **개강 전에도 잡힌다** → "개강 인원"이라 부르지 않는다 (2026-09-16 명칭 변경) |
| 수료 | `finiCnt` | `FINI_CNT` | 수료 인원 (종료 회차만 값 있음). 조기취업 미포함 |
| 개강일 참석 | 명부 스냅샷 | `TB_ROSTER_MEMBER.FIRST_ATTEND_DT = 개강일` | 입실 시간이 있거나 출석 계열 상태. 출결을 읽지 않은 회차는 NA |

| 지표 | 공식 |
|---|---|
| **등록 대비 승인률** | 승인 인원 / 수강신청 × 100 (수강신청 0 → NA). 구 명칭 `개강 참석률` |
| **미승인** | 수강신청 − 승인 인원. 구 명칭 `신청 이탈` |
| **정원 충원율** | 승인 인원 / 정원 × 100 |
| **개강일 참석률** | 개강일 참석 / 승인 인원 × 100 — 진짜 개강 참석률 (Streamlit 페이지) |

**노션 「HRD 등록자 관리」 기수 표** (`notion_registry_publish.build_cohort_rows`, 2026-09-21) — 운영TF 구간 7 정의를 따른다. 분모는 **API 신청인원** = HRD-Net `totTrpCnt`(한 번이라도 수강신청한 사람의 누적, 취소자 포함):

| 열 | 원천 | 계산식 | 조건 |
|---|---|---|---|
| 노션 수집 등록 인원 · 일치 여부 | 신청자 리스트(AI·SKN) HRD신청·HRD등록 이력 ∪ 신청/등록 일자 | API 신청인원과 같으면 일치 | — |
| 개강일 출석 인원 · 개강 참석률(%) | 출결 API → `FIRST_ATTEND_DT` = 개강일 | 개강일 출석 ÷ API 신청인원 | 개강 다음 날부터. 출결을 못 읽은 회차는 비움 |
| 확정 신고(API) · 확정자 신고율(%) | `_3.jsp` `totParMks` (= 명부 건수 = 노션 운영표 확정자신고) | 확정 신고 ÷ API 신청인원 | 개강 + `config.NOTION_KPI_CONFIRM_DAYS`(7)일부터 |

운영TF 「일별 액션 측정」용 분모·분자(등록 = 노션 HRD등록·합격자등록 기준 참석률·기록 채움률)는 `python scripts/tf_section7.py`가 출력한다. 상세는 `docs/GLOSSARY.md` "HRD 등록자 관리".

> **API 한계**: 명부(`_4.jsp`)에는 승인자만 내려오므로 신청만 하고 미승인인 개인은 식별 불가 — 인원 차이로만 잡힌다.
> 시점 정보도 없어(스냅샷) 승인 반영일은 알 수 없다 → `kpi_etl.py`가 TB_COURSE_SNAPSHOT·TB_ROSTER_MEMBER에 변화 이력을 남긴다.
> 전 회차 조회는 `hrd_api.get_course_history_with_fallback(get_funnel_institutions())` — API 우선, 실패 시 `TB_COURSE_MASTER` 폴백(한화만).

---

### 매출

| 구분 | 내용 |
|---|---|
| **일 훈련비** | 145,200원 (`config.DAILY_TRAINING_FEE`) |
| **기준 매출** | 훈련일수 × 일훈련비 × 수강생수 |
| **전액 청구** | 출석률 ≥ 80% |
| **비례 청구** | 0% < 출석률 < 80% → 출석률 비례 |
| **미청구** | 출석률 = 0% |
| **달성률** | 실제 매출 / 기준 매출 × 100 |

**공통 함수**: `utils.calc_revenue()`, `utils.get_billing_periods()`

---

## 커밋 컨벤션

```
Tag: English summary (한글 설명)

- 변경 이유 / 증상 / 영향 범위를 bullet으로 기록
- 필요 시 추가 bullet
```

| Tag | 용도 |
|---|---|
| `Feat` | 새로운 기능 추가 |
| `Fix` | 버그 수정 |
| `Docs` | 문서 변경 |
| `Style` | UI/CSS 변경, 코드 포맷팅 |
| `Refactor` | 코드 리팩토링 |
| `Chore` | 빌드, CI/CD, 설정 파일 변경 |

- `Co-Authored-By` 라인 포함하지 않음
- 영어 요약은 동사 원형으로 시작 (Add, Fix, Update, Remove 등)
- 본문(body)은 제목 아래 빈 줄 후 작성, 변경 이유·증상·영향 범위를 bullet(`-`)으로 2~3줄 기록

예시:
```
Fix: Correct completion rate calculation (수료율 계산 오류 수정)

- 수료 판정이 정확일치(=='수료')로 되어 있어 80%이상수료 등이 누락됨
- str.contains('수료|조기취업')로 변경하여 모든 수료 유형 포함
- home.py, 종료과정_성과.py 두 파일에 영향
```

## 환경 변수

- `HRD_API_KEY` — 플레이데이터평생교육원 기관 인증키 (한화·SKN 과정). GitHub Actions + Streamlit secrets 양쪽 등록
- `ENCORE_API_KEY` — (주)엔코아 기관 인증키 (MLE·AIO·MLO 과정). Streamlit secrets 등록. **명부/출결 API는 인증키 소속 기관의 과정만 조회 가능**
- **과정 ID는 환경변수가 아니라 `config.py`에서 관리** (2026-09-08 통일): `INSTITUTIONS`(기관 → 키 환경변수 이름) · `COURSES`(과정 ID → 기관·약칭) ·
  용도별 범위 `ETL_COURSE_ID`(DB 수집, 한화 1개) / `OPS_COURSE_IDS`(운영 현황) / `FUNNEL_COURSE_IDS`(모집 퍼널·노션 대조, 전체).
  `hrd_api.get_institutions(course_ids)`가 과정마다 소속 기관 키를 붙여 (키, 과정ID) 쌍을 만든다. 과정 추가 = `COURSES`에 한 줄 + 범위 목록에 추가.
  구 변수 `HANWHA_COURSE_ID`·`ENCORE_COURSE_IDS`는 더 이상 읽지 않음 (시크릿에 남아 있어도 무해)
- `DATABASE_URL` — PostgreSQL 연결 문자열 (**필수**. 미설정 시 `get_connection()`이 `DatabaseNotConfiguredError`)
- `DATABASE_URL_MARKET` — 시장 DB(두 번째 Supabase 프로젝트) 연결 문자열. GitHub Actions 3개 워크플로 + Streamlit secrets 등록. 미설정 시 메인으로 폴백
- `NOTION_TOKEN` — 노션 **내부 통합** 토큰 (통합 이름 `sul`, 워크스페이스 교육 BU, 2026-09-20부터. 그전엔 개인 액세스 토큰). 콘텐츠 읽기·업데이트·삽입. **연결된 페이지만 보인다**: 신청자 리스트 DB·운영현황표 DB(읽기)·「모집 KPI」 페이지(쓰기) 세 곳에 연결돼 있음 — 새 페이지를 읽으려면 그 페이지에 통합을 연결해야 한다. GitHub Actions + Streamlit secrets. 없으면 폴링은 건너뛰고 대조 페이지는 CSV 업로드로 대체
- `DISCORD_WEBHOOK_URL` — (선택) 디스코드 채널 웹훅. GitHub Actions에만. 없으면 알림을 건너뛴다
- `SARAMIN_API_KEY` — 사람인 채용공고 API 키 (GitHub Actions + Streamlit secrets 등록)
- `ETL_FULL_REFRESH` — `=1`이면 market_etl이 증분(12개월) 대신 2023-01-01부터 전체 재수집. GitHub Actions 수동 실행의 `full_refresh` 입력으로 전달 (`gh workflow run market_etl.yml -f full_refresh=true`)

## Claude Code 구조 관리

새 규칙/워크플로우가 생기면 아래 기준으로 분류한다. **Claude는 항상 제안 → 승인 대기 → 작업 순서로 진행하며, 자율적으로 이 파일들을 수정하지 않는다.**

```
새 규칙/워크플로우
├─ 파일 저장 즉시 자동 실행해야 하나?          → Hook   (.claude/settings.json hooks)
├─ 여러 단계 반복 작업, 사람이 /명령 으로 호출?  → Skill  (.claude/skills/<name>/SKILL.md)
├─ 특정 파일 경로에만 적용되는 코딩 패턴?       → Rule   (.claude/rules/<domain>.md)
└─ 전역 + 코드만으로 추론 불가능한 핵심 규칙?   → CLAUDE.md
```

| 위치 | 넣는 것 | 넣지 않는 것 |
|---|---|---|
| `CLAUDE.md` | 전 파일 공통 규칙, 비즈니스 결정, 아키텍처 컨텍스트 | 구현 세부사항, 특정 파일 전용 패턴 |
| `rules/` | 경로별 코딩 패턴 (`paths:` 스코프 활용) | 전역 규칙, 실행 절차 |
| `skills/` | 순서 있는 반복 작업, 부작용 있는 명령 | 단순 단일 명령, 자동 실행 검사 |
| `hooks/` | 항상 자동 실행 검증, 빠른 체크 | 느린 작업 (→ Skill), 사람 판단 필요 작업 |

### 문서 관리 규칙

문서는 `docs/` 하위에 관리한다. 루트에는 `README.md`와 `CLAUDE.md`만 둔다.

| 경로 | 내용 |
|---|---|
| `docs/GLOSSARY.md` | UI 용어 사전 |
| `docs/DEV_LOG.md` | 개발 일지 (의사결정·삽질 기록) |
| `docs/api/` | 외부 API 명세 (hrd_net.md, saramin.md) |

**자동 갱신 규칙** — 아래 작업 시 관련 문서도 함께 갱신한다:

| 작업 | 갱신 대상 |
|---|---|
| 신규 ETL/API 추가 | `docs/api/`에 명세 파일 추가, `CLAUDE.md` 아키텍처·환경변수 업데이트 |
| 신규 페이지 추가 | `CLAUDE.md` navigation 주의사항 확인, `docs/GLOSSARY.md` 용어 점검 |
| 주요 기능 추가/아키텍처 변경 | `docs/DEV_LOG.md`에 결정 배경·대안·삽질 기록 추가 |
| 비즈니스 지표 정의 변경 | `CLAUDE.md` 핵심 지표 정의 섹션 업데이트 |

### 등록된 훅 목록

| 이벤트 | matcher | 역할 |
|---|---|---|
| `SessionStart` | `compact` | 컨텍스트 압축 시 7가지 핵심 비즈니스 규칙 리마인더 재주입 |
| `PreToolUse` | `Edit\|Write` | `.py` 파일 SQL 안티패턴 사전 차단 (`pd.read_sql` 직접 사용, `== '수료'`, `COUNT(*)` 별칭 누락) |
| `PreToolUse` | `Bash` | ETL 스크립트(`hrd_etl.py`, `market_etl.py`, `init_db.py`, `saramin_etl.py`) 실행 시 프로덕션 DB 쓰기 확인 프롬프트 |
| `PostToolUse` | `Edit\|Write` | CJK 한자 析(U+6790) 감지 + ruff 미사용 import/변수 검사 |
| `Stop` | *(전체)* | `.py` 파일 수정 턴 종료 시 `pytest -x -q` 자동 실행 |
