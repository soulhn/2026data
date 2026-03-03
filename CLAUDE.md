# CLAUDE.md

## Commands

```bash
streamlit run home.py           # 앱 실행 (port 8501)
python -m pytest tests/ -v      # 전체 테스트
python init_db.py               # DB 스키마 초기화 (최초 1회)
python hrd_etl.py               # 내부 과정/출결 ETL (수동 실행)
python market_etl.py            # 시장 동향 ETL (30만+ records)
```

## ⚠️ 한글 코드포인트 규칙 (CRITICAL)

**"분석"의 "석"은 반드시 한글 U+C11D (HANGUL SYLLABLE SEOG) 을 사용한다.**
- ✅ 올바름: 분석 = U+BD84(분) + U+C11D(석)
- ❌ 금지: 분析 — 析은 U+6790 (CJK 한자)으로 전혀 다른 문자

파일 경로, 코드 문자열, 응답 텍스트 모두 적용. Claude는 析(U+6790)을 쓰는 경향이 있으므로 반드시 주의.

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
- PostgreSQL 컬럼명 소문자 반환 → `load_data()`에서 대문자 변환 처리

### ETL 자동화
- `hrd_etl.yml` — 평일 KST 09:00~18:00 매시간
- `market_etl.yml` — 매일 KST 21:00

## 주의사항

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
- **UI 용어 표기**: 지표 표시명은 `GLOSSARY.md` 기준 준수. 수식어+지표 사이 공백 필수

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

### 취업률

**기준 컬럼**: `TB_COURSE_MASTER` — HRD-Net API 수집값 (수료자 기준 %)

> **⚠️ 시장 취업률 데이터 한계**: `TB_MARKET_TREND`에도 `EI_EMPL_RATE_3/6` 컬럼이 있으나 전체 32만건 중 ~7.5%만 유효값 보유 (91% NULL). 수료 후 3~6개월 경과해야 집계되는 API 특성상 최근 과정은 거의 비어있음. **시장 벤치마크 비교 시 취업률은 활용 불가** — 모집률·만족도만 비교 가능. 내부 과정 취업률은 `TB_COURSE_MASTER` 컬럼 그대로 활용 (위 표 참조).

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
| **KPI 요약** | `FINI_CNT / TOT_PAR_MKS × 100` | `home.py` — HRD-Net 집계값 기준 |
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

예시: `Fix: Correct completion rate calculation (수료율 계산 오류 수정)`

## 환경 변수

- `HRD_API_KEY` — HRD-Net API 인증키
- `HANWHA_COURSE_ID` — 내부 관리 대상 과정 ID
- `DATABASE_URL` — PostgreSQL 연결 문자열 (없으면 SQLite 폴백)

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
