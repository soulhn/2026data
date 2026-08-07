# 개발 일지

## 2026-08-07 — 채용공고 보존 정책 도입 + 채용 동향 개편 (Supabase 500MB 대응)

### 배경
- DB 468/500MB(93.6%), 여유 32MB. 실측 증가 2.75MB/일 → **8월 중 한도 도달** 예정이었음
- 증가의 3분의 2가 채용공고(월 26MB): `saramin_etl.py`에 DELETE가 한 줄도 없어 무기한 누적.
  `init_db.py`의 "2026-04 이전 삭제"는 1회성 마이그레이션(고정 문자열)이지 보존 정책이 아니었음
- 보유 공고의 71%가 이미 마감된 공고였고, 13.4%(1.07만 건)는 마감일이 2032~2033년인 **상시채용**

### 결정 사항
- **보존 정책: 마감 후 30일 + 상시채용(마감일 1년 이상 미래)은 게시 후 90일** (사용자 확정)
  - 상시채용은 마감 기준으로는 영원히 안 지워지므로 게시일 컷을 별도로 둠
  - 3개월 보존안은 회수가 12MB뿐이라 기각 — 1개월이 57MB 회수 + 6개월 여유 확보
  - 날짜가 'YYYY-MM-DD' 문자열이라 커트오프를 파이썬에서 계산해 문자열 비교 → PG/SQLite 엔진 분기 불필요
- **선행 조건 — 시계열 캐시 누적 병합**: ETL이 캐시를 매번 원본에서 전체 재계산하므로,
  원본을 지우면 추이도 사라짐. 월별 신규·월별 종료·키워드 추이 3종을 `merge_cumulative()`로 전환
  - 병합 규칙: 같은 월은 **max(기존, 신규)** — 월 카운트는 수집 직후 며칠만 늘고(게시 3일 창)
    이후엔 삭제로 줄기만 하므로 max가 곧 완전한 값. 날짜 비교 없이 월 경계·실행 순서에 안전
  - 주의: 오염 데이터를 의도적으로 지워도 캐시 값은 남음 → 그땐 캐시 수동 삭제 필요
- **실행 순서: 삭제 → 집계** (과거 월은 병합이 보호). `--cleanup-only` 모드로 API 쿼터 소모 없이 실행 가능
- **채용 동향 개편** (사용자 선택: 종료 탭 → 월별 추이 탭)
  - 탭2를 누적 캐시 기반 "월별 추이"(신규·종료)로 교체. 보존 후 표본이 30일치로 편향되는
    "직무별 종료 분포"와, 상시채용 오염으로 평균 2,821일이 나오던 "평균 게시 기간" 차트는 제거
  - KPI 재정의: 누적 수집(캐시 합 — 삭제분 포함) / 진행중 / 최근 마감(30일) / 기업 수(보유 기준)

### 실행 결과 (2026-08-07)
- 삭제: 공고 43,250건 + 키워드 88,342건 + 지역 45,690건 (예상 43,152건과 일치)
- 캐시 검증: 4~7월 월별 값이 삭제 전 측정치와 정확히 일치(19,800/18,191/19,880/18,306) — 추이 보존 확인
- VACUUM FULL(작은 테이블부터 — 새 사본만큼 여유 공간이 필요해 region→keyword→posting 순):
  region 8.4→3.4, keyword 20.0→8.4, posting 85.5→32.4MB
- **DB 468.0 → 398.8MB (79.8%), 여유 101.2MB.** 이후 증가는 시장 동향(월 ~15MB)만 남아
  약 6~7개월 여유. 다음 병목은 시장 테이블(325MB) — 그때 아카이빙 or Pro 결정 필요

### 검증
- 회귀 테스트 7개: 병합 규칙 4(과거 월 보존·max 채택·성장 월 갱신·복합 키),
  보존 규칙 2(5행 시나리오·junction 고아 방지), e2e 1(집계→삭제→재집계 후 추이 잔존).
  병합을 무력화하면 e2e가 실패하는 것 확인. 총 233개, 로컬·CI 재현 모두 통과

## 2026-07-29 — 실시간 조회 상한 60→120초 + 실패 사유 노출 (같은 날 후속)

배포 후 운영 현황이 `data_source: DB_FALLBACK`으로 떨어짐. **왜 실패했는지 화면에 아무 정보가 없어** 원인 추적이 막혔다.

### 상한 60초가 정상 경로를 자를 수 있었음 (내 직전 변경의 결함)
- 한 기관은 **과정목록 → (명부·출결)** 2단계 **순차**이고, 각 단계가 `API_TIMEOUT=(15,30)` 상 최대 45초 → 한 기관의 "느리지만 정상" 경로가 **최대 90초**
- 그런데 전체 상한을 60초로 걸어, 느린 응답을 실패로 잘라버릴 수 있었다. `config.py`에 "Streamlit Cloud→한국 정부 서버 핸드셰이크 여유 확보"라고 적힌 걸 보면 이 구간이 느린 건 이미 알려진 사실이었다
- **상한 120초로 상향** + `test_deadline_covers_two_sequential_stages`로 `API_TOTAL_DEADLINE >= sum(API_TIMEOUT) * 2`를 강제. 60초로 되돌리면 테스트가 실패하는 것 확인
- 참고: 기관 간에는 병렬이므로 상한은 기관 수와 무관하다. 로컬 실측 전체 0.4초

### 실패 사유가 화면에 안 보였음
- 폴백은 **정상 동작**이라 예외가 화면까지 전파되지 않는다 → "느려서 잘렸는지" "API가 거부했는지" 구분 불가
- `_last_realtime_error` + `get_last_realtime_error()`로 사유를 남기고 진단 패널에 표시. 타임아웃은 `"전체 상한 N초 초과 (응답 지연)"`, API 오류는 `"ConnectionError: ..."`처럼 예외 타입까지 보존
- 캐시 히트 시 모듈 변수가 이미 비어 있으므로 **`@st.cache_data` 반환값 안에 사유를 함께 담았다**
- `show_spinner="실시간 출결 조회 중… (응답이 느리면 최대 2분)"` — 원 제보였던 "아무 피드백 없는 멈춤"에 대한 직접 대응

### 부분 실패에서 거짓 안내가 재발할 수 있었음
- 일부 기관만 타임아웃되고 **살아남은 기관이 빈 결과**면 예외가 안 나므로 `data_source`는 `"API"`로 남는다 → 다시 "현재 진행 중인 과정이 없습니다 ☕"
- `fetch_all_institutions`가 **부분 실패도** `_last_realtime_error`에 기록하도록 바꾸고, 페이지는 `data_source == "DB_FALLBACK" or realtime_error`로 판정
- 회귀 테스트 `test_partial_failure_is_also_recorded` 추가

### 검증
- 3개 시나리오(정상 / 전부 실패 / 부분 타임아웃) AppTest 렌더 확인 — 부분 타임아웃에서 거짓 안내 사라짐
- 테스트 220 → **226개**, 로컬·CI 재현 모두 통과

---

## 2026-07-29 — 운영 현황 "Running get_active_data(" 멈춤 + 폴백 거짓 안내 수정

"AI캠퍼스 운영 현황이 제대로 동작 안 한다"는 제보에서 출발. 원인이 셋으로 갈렸다.

### 증상과 재현
- 화면에 `Running get_active_data(` 스피너만 계속 돎. **로컬은 1.1초라 재현 불가** — Streamlit Cloud→한국 정부 서버 구간에서만 발생
- 실측: 도달 불가 주소로 요청 1건이 실패하는 데 **46초** (재시도 2회 포함, connect 15초 × 3회 + backoff)

### ① 기관 순차 조회로 대기 시간 누적 (수정)
- `fetch_all_institutions`가 (인증키, 과정ID) 쌍 3개를 **순차** 순회 → 과정 목록 단계만 46×3 = 138초, 명부·출결 단계까지 **최대 276초(4.6분)**. 응답이 느리기만 한 경우(read 30초)는 약 7분
- **해결**: 기관을 병렬 조회 + `config.API_TOTAL_DEADLINE = 60`초 전체 상한. 초과 시 미완료 기관을 버리고 DB 폴백
- **구현 함정**: `with ThreadPoolExecutor(...)`는 `shutdown(wait=True)`라 상한을 넘겨도 네트워크 대기 중인 스레드를 끝까지 기다린다. `cancel()`로도 실행 중인 future는 못 멈춘다 → `with` 대신 `executor.shutdown(wait=False, cancel_futures=True)`로 즉시 반환
- **실측**: 276초 → **46초**(6배 단축), 상한 초과 시 정확히 상한에 반환. 잔존 스레드는 백그라운드에서 자동 정리(누수 없음) 확인
- **회귀**: `pairs=[]`일 때 `max_workers=0`으로 `ValueError` → 빈 목록 조기 반환 가드 추가 (기존 `test_empty_result_keeps_columns`가 잡아냄)

### ② 폴백이 "휴식 시간"이라고 거짓 안내 (수정)
- API 실패 후 DB 폴백이 빈 결과를 내면 페이지가 `"현재 진행 중인 과정이 없습니다. 꿀 같은 휴식 시간입니다! ☕"`를 띄웠다. **실제로는 5개 기수가 운영 중**
- **해결**: `get_active_data_with_fallback`의 source를 `"DB"`(키 미설정)와 `"DB_FALLBACK"`(API 실패)로 분리. 후자에서 빈 결과면 "실시간 조회 실패 · DB에 해당 과정 데이터 없음 — **운영 중인 과정이 없다는 뜻이 아닙니다**"로 표시하고, DB 기준 표시 시 경고 배너도 추가

### ③ 근본 원인 — ETL이 한화 과정만 수집 (이번엔 미수정, 결정 사항)
```
API 활성 과정 5개   → 전부 엔코아 (AIG...578382, AIG...578396)
DB TB_COURSE_MASTER → 한화(AIG...455635) 25행뿐, 최신 종료일 2026-07-03
                      엔코아 과정: 0행
```
- `hrd_etl.py`는 `HANWHA_COURSE_ID` **하나만** 수집한다. ETL 워크플로는 매시간 정상 실행 중(최근 5회 전부 success)이지만 수집 대상에 엔코아가 없다 → DB 폴백은 현재 운영 기수에 대해 비어 있다
- **결론(사용자 확인): 이건 결함이 아니라 의도된 설계다.** 운영 현황은 **응답 속도를 위해 실시간 API를 주 경로로 전환**한 페이지이고, DB 폴백은 보조 수단이다. 따라서 ETL을 `get_institutions()` 기반으로 확장해 폴백을 메울 대상이 아니다 (확장하면 매출 분석·종료과정 성과 등 한화 기준 페이지에 엔코아 데이터가 섞여 지표도 바뀐다)
- → 올바른 대응은 ②의 **정직한 안내**다. 폴백이 비면 "운영 중인 과정 없음"이 아니라 "실시간 조회 실패"로 표시한다

### QA 결과 (전체)
- 페이지 10개 전부 렌더 성공, 미처리 예외 0건. `DATABASE_URL` 없이도 10개 전부 미처리 예외 0건(원인 표시 또는 DB 미사용)
- 테스트 213 → **220개**, 로컬·CI 재현 환경 모두 통과. ruff(F401/F841/F821) clean
- 모듈 8종 import 시 최상위 `exit()` 없음 확인 (Streamlit import 시 앱 종료 방지)
- **QA에서 발견 → 같은 날 수정**: `pages/매출_분석.py`의 수강생별 상세 표가 `_출석` 컬럼에 출석 수(int)와 `"-"`(str)를 섞어 Arrow 직렬화 경고 발생. Streamlit이 자동 보정해 표는 렌더됐지만 컬럼이 object가 되어 정렬도 문자열 기준이었음
  - 해당 기간에 없던 훈련생 → `0` (구분은 `_상태`의 `"해당없음"`이 담당)
  - **합계 행은 `0`이 아니라 출석일수 합** — 합계 행에 0을 넣으면 "출석 0일"로 읽혀 거짓이 된다. 훈련비 합산과 동일한 처리
  - `_출석률`은 `"12.3%"` 형태의 문자열 컬럼이라 `"-"` 유지 (숫자를 섞으면 오히려 새 경고 발생)
  - 검증: `_출석` 컬럼 dtype `object` → `int64`, 페이지 10개 전체 Arrow 경고 0건

---

## 2026-07-29 — 폴백 제거 후속 정리 4건

폴백 제거 작업에서 "남긴 것"으로 분류했던 4건을 순차 처리. 하나는 실제 버그였고 나머지는 정리.

### ① `batch_execute` 폴백이 PG에서 무력했던 문제 (실제 버그)
- **증상**: PG는 문 하나가 실패하면 트랜잭션 전체가 aborted 되어 이후 `execute`가 전부 `InFailedSqlTransaction`으로 죽는다. row-by-row 폴백이 **한 행도 건지지 못하고 전량 실패**
- **영향**: 호출부 3곳(`hrd_etl.py` 명부·명부보정·출결)이 모두 회차 단위 `conn.commit()` 안에 있어, 배치 1건 실패 시 **그 회차 데이터가 통째로 소실**되고 로그에는 `[ETL Summary] 실패: N건`만 남는 조용한 손실. UPSERT라 PK 충돌은 무해하고, 실제 유발 원인은 NOT NULL 위반·컬럼 길이 초과·타입 캐스팅 실패
- **해결**: 배치를 `SAVEPOINT batch_sp`로 감싸 실패 시 `ROLLBACK TO`로 트랜잭션 복구, 폴백 중 나쁜 행도 `SAVEPOINT row_sp`로 격리. `get_connection()`이 ETL 경로에서 `autocommit=False`라 SAVEPOINT 전제는 이미 충족
- **검증**: PG 트랜잭션 의미론(문 실패 → aborted, `ROLLBACK TO`로만 복구)을 흉내내는 `FakePgCursor`로 회귀 테스트 3개 추가. 실제 PG 없이도 검증되며, **수정 전 코드에서 `(0, 3)` 전량 소실 → 수정 후 `(2, 1)` 복구**를 확인

### ①-2 실패 행 진단 로깅 (후속, 같은 날 처리)
- 폴백이 실제로 동작하게 됐으므로 실패 행의 원인 추적 수단이 필요해짐 — 기존에는 카운트만 남았음
- **개인정보 문제를 함께 발견**: 명부 행에는 `trnee_nm`(이름)·`lifyeaMd`(생년월일)가 들어간다. 그런데 PG는 NOT NULL 위반 시 `DETAIL: Failing row contains (...)`로 **행 전체를 오류 메시지에 덧붙인다.** 즉 기존 배치 실패 warning(`logger.warning(f"...: {e}")`)만으로도 이미 훈련생 이름·생년월일이 GitHub Actions 로그에 남을 수 있었다 (Actions 로그는 보존·열람 가능)
- **결정 — 값 대신 형태를 남긴다**:
  - `_brief_error()` — 오류의 **첫 줄만** 사용. 컬럼명·제약조건은 첫 줄에 있고, 값이 담긴 `DETAIL`/`HINT` 줄은 버린다. 기존 배치 warning에도 적용해 **이전부터 있던 유출 경로까지 차단**
  - `_row_shape()` — 값 대신 타입·문자열 길이·None 여부만 요약 (`[str(11), int, None, str(3)]`). 실제 실패 원인이 NOT NULL 위반·길이 초과·타입 불일치이므로, `_brief_error`가 알려주는 컬럼명과 대조하면 값 없이도 원인 특정이 가능
  - 행 인덱스를 함께 남겨 원본 API 응답과 대조 가능
  - `ETL_FAILED_ROW_SAMPLE = 3` — 앞 3건만 남기고 나머지는 건수만 보고 (한 배치가 통째로 깨질 때 로그 폭주 방지)
- **실제 출력**: `[batch_execute] 행 1 실패: null value in column "trnee_nm" violates not-null constraint | 형태 [str(11), int, None, str(3), str(8)]`
- **검증**: 진단 정보 존재·샘플 상한·**개인정보 미노출** 3개 테스트 추가. 유출 테스트는 `_brief_error`를 무력화하면 로그에 이름·생년월일이 그대로 찍히며 실패하는 것을 확인
- **트레이드오프**: 값을 못 보므로 "어느 훈련생인지"는 행 인덱스로 원본 응답을 되짚어야 한다. CI 로그에 개인정보를 남기지 않는 쪽을 우선했다

### ② `sqlite3.Row` 센티널 → `dict_rows` 플래그
- `get_connection(row_factory=sqlite3.Row)`가 "dict 형태 행을 달라"는 매직 토큰으로 쓰여, SQLite가 테스트 전용이 된 뒤에도 `utils.py`·`hrd_etl.py`가 `import sqlite3`를 유지하게 만들었음
- `dict_rows=True`로 교체 (호출부는 `hrd_etl.py:71` 1곳뿐). **프로덕션 코드에서 sqlite3 의존 완전 제거 — 이제 `tests/`에만 남음**
- 폴백 제거 당시엔 `run_etl` 커버리지 0을 이유로 보류했으나, 호출부가 1곳이고 conftest가 `lambda **kwargs`로 받아 테스트도 안 깨지므로 진행. 실제 PG 연결로 `dict_rows=True` → `RealDictRow`, 미지정 → `tuple` 확인

### ③ `pages/채용_동향.py` 도달 불가 분기 제거
- fail-fast 도입으로 `load_data()`가 성공하려면 `is_pg()`가 반드시 True → `SUBSTR`/`JULIANDAY` else 가지는 **논리적으로 도달 불가**임이 증명됨 (이전엔 "아마 안 쓰임" 수준)
- **`saramin_etl.py`의 같은 모양 분기는 유지** — `test_saramin_etl.py`가 `saramin_etl.is_pg`를 False로 패치해 그 else를 실제 실행한다. 테스트가 있는 쪽은 남기고 없는 쪽만 지우는 역설적 구조
- `AppTest`로 페이지 렌더 확인(예외 0건, 차트 9개) + PG 표현식 직접 실행 검증

### ④ `hrd_etl.py`의 미사용 `load_data` import
- 1줄짜리지만 **①②의 선결 조건**이었음 — `PostToolUse` 훅이 `.py` 편집 후 `ruff F401`을 돌려 `exit 2`로 차단하므로, 이게 남아 있으면 `hrd_etl.py`를 건드리는 모든 작업이 막힌다

### 영향 범위
- 수정: `hrd_etl.py`, `utils.py`, `config.py`, `pages/채용_동향.py`, `tests/test_hrd_etl.py`
- 테스트: 210 → **216개** (`TestBatchExecutePostgres` 3개 + `TestBatchExecuteFailureLogging` 3개)

---

## 2026-07-29 — SQLite 런타임 폴백 제거(fail-fast) + pytest CI 도입

### 배경
- "DB 이중화·도커가 여전히 필요한가" 점검에서 출발. **도커는 애초에 없었음** — `Dockerfile`/`docker-compose.yml`은 402개 커밋 전체 히스토리에 존재한 적이 없고, `.devcontainer/`만 2026-03-31 이후 4개월 방치 상태였음 (Python 3.11 고정으로 CI 3.12·로컬 3.14.2와 불일치, 존재한 적 없는 `packages.txt` 참조, `/root` 마운트가 non-root 이미지에서 무효). 커밋 타임존이 5월 말 `+0000`(컨테이너) → 6월 이후 `+0900`(로컬 Mac)으로 바뀐 뒤 돌아가지 않음
- **DB 이중화는 "런타임 폴백"으로는 위험, "테스트용"으로는 필요**한 상태였음
  - `utils.DB_FILE = "hrd_analysis.db"`는 디스크에 존재하지 않는 파일. `DATABASE_URL`이 누락되면 `get_connection()`이 빈 SQLite를 새로 만들고 이후 모든 쿼리가 `no such table`로 실패 → **화면만 비는 조용한 실패**. Streamlit secrets 오타·만료 시 "DB 설정 없음"이 아니라 "데이터 없는 대시보드"로 보임
  - 프로덕션 경로는 전부 PG (`.env`·Streamlit secrets·ETL 워크플로 3개 모두 `DATABASE_URL` 주입) → SQLite로 도는 실사용 경로 0개. 루트의 `hrd_data.db`/`hrd_training.db`/`market.db`는 0바이트·코드 참조 0건 유물
  - 반면 테스트는 SQLite에 실제로 의존 (`conftest.py` 인메모리 fixture, 210개가 인프라 0으로 1초에 실행). 전면 제거는 PG testcontainer(=도커)나 대량 mock을 요구해 비용 대비 이득 없음

### 결정 사항
- **`get_connection()`에서만 fail-fast** — `DatabaseNotConfiguredError`. `get_database_url()`이나 `is_pg()`에 넣으면 `adapt_query()`의 SQLite 패스스루가 죽어 `TestAdaptQuery` 4개 + 모든 fixture가 붕괴
- **`is_pg()`·`adapt_query()`·ETL의 `execute_batch` vs `executemany` 분기·init_db의 PG 전용 commit/rollback 33곳은 유지** — 테스트 호환 계층
- `page_error_boundary()`에 전용 분기 추가 — 설정 오류를 "잠시 후 다시 시도"로 오안내하지 않음. `st.error`를 데이터 계층에 넣지 않은 이유는 `utils.py`를 헤드리스 ETL 3종이 import하기 때문
- `DB_FILE` 상수 및 참조 3곳(`init_db`, `SQL_Playground`, `DB_명세`) 제거, 연결 라벨 PostgreSQL 고정
- **선결 조건으로 드러난 테스트 버그**: `tests/test_hrd_etl.py`가 `utils.is_pg`만 패치하는데 `hrd_etl.py:12`가 `from utils import is_pg`로 값 바인딩 → `hrd_etl.is_pg`가 True로 남아 `execute_batch`가 sqlite3 커서에 호출되고 `AttributeError` → 폴백으로 **우연히 통과**. `cursor.executemany` 경로는 한 번도 커버된 적 없었음. `DATABASE_URL`이 없는 CI에서는 진짜 `executemany`를 타면서 SQLite의 비원자적 부분 삽입 때문에 `test_fallback_on_error`가 `(0, 3)`을 반환해 실패 (실측: 로컬 210 passed / `DATABASE_URL=` 209 passed 1 failed). `hrd_etl` 모듈 패치 + 선점 행 방식으로 결정화
- **CI에 `DATABASE_URL` 미주입** — 주입하면 위 버그가 다시 가려진 채 초록불이 되고 프로덕션 접속 문자열이 러너에 노출됨. `.env`가 CI에 없어 자동으로 SQLite 모드가 되므로 주입할 이유도 없음
- `.devcontainer/` 및 유물 `.db` 3개 삭제

### 남긴 것 / 후속
후속 4건은 같은 날 순차 처리 완료 — 아래 "2026-07-29 — 폴백 제거 후속 정리 4건" 참조.
- `.claude/settings.json` PreToolUse(Bash) 훅은 **확인 프롬프트로 전환 완료** — 기존 "`DATABASE_URL` 있으면 ETL 차단(exit 2)"은 SQLite 폴백을 전제하던 로직이라, 폴백 제거 후엔 있으면 훅이 막고 없으면 코드가 죽어 ETL이 영구 실행 불가가 됨. `DATABASE_URL` 검사 조건을 빼고 `permissionDecision: "ask"`로 변경 (완전 삭제는 비권장 — `permissions.allow`에 `Bash(python:*)`가 있어 마찰 없이 프로덕션에 쓰게 됨)

### 영향 범위
- 수정: `utils.py`, `init_db.py`, `saramin_etl.py`, `pages/SQL_Playground.py`, `pages/DB_명세.py`, `tests/test_hrd_etl.py`
- 추가: `.github/workflows/tests.yml`
- 삭제: `.devcontainer/`, `hrd_data.db`, `hrd_training.db`, `market.db`
- 문서: `README.md`, `CLAUDE.md`, `.claude/rules/database.md`, `.claude/rules/testing.md`

---

## 2026-07-29 — market_etl upsert에서 개강일(TR_STA_DT) 갱신 누락 수정

### 배경
- 개강 현황 탭 사용 팀원 문의: "개강 일자가 바뀌는 과정이 많은데 알아서 업데이트되나요?"
- 확인 결과 `_UPSERT_QUERY_RAW`의 `ON CONFLICT DO UPDATE SET`에 `TR_STA_DT`만 빠져 있었음. `TR_END_DT`·정원·신청인원은 매일 갱신되는데 개강일만 최초 수집값 고정
- 파생 증상: 개강일에서 계산되는 `YEAR_MONTH`는 갱신 대상에 포함되어 있어, 개강이 다음 달로 밀리면 **월별 개강 예정 차트는 새 달로 이동하는데 표의 개강일·D-day·상태 구분(개강 예정/진행 중/종료)은 옛 날짜 기준**으로 남아 두 화면이 서로 다른 달을 가리킴

### 결정 사항
- `TR_STA_DT=excluded.TR_STA_DT` 추가 — PK가 `(TRPR_ID, TRPR_DEGR)`라 회차가 같으면 행이 늘지 않고 갱신됨
- 회귀 테스트 `TestSaveRowsUpsert` 신설: 같은 과정을 개강일만 바꿔 두 번 저장 → `TR_STA_DT`·`YEAR_MONTH`·신청인원이 모두 최신값인지 검증. 수정 전 코드에서 실패하는 것 확인 후 반영
- 기존에 어긋난 행은 매일 증분 수집(최근 12개월 + 미래 90일) 시 자동 정정됨. 즉시 정리하려면 `gh workflow run market_etl.yml -f full_refresh=true`
- **미수정(별건)**: 폐강·취소 과정을 삭제하는 로직이 없어 HRD-Net에서 내려간 과정도 개강 예정 목록에 남음 — 별도 판단 필요

### 영향 범위
- 수정: market_etl.py, tests/test_market_etl.py (테스트 209→210개)
- 문서: docs/DEV_LOG.md

## 2026-07-22 — 시장 분석 개강 현황 탭 + market_etl 미래 수집 확장

### 배경
- 요청: 타 기관 과정들의 개강·종강 일정, 정원, 신청인원, 모집률을 볼 수 있는 탭 필요
- 확인 결과 `TB_MARKET_TREND`에 필요한 컬럼(TR_STA_DT·TR_END_DT·TOT_FXNUM·REG_COURSE_MAN)은 이미 존재하나, `market_etl.get_collect_range()`가 수집 종료일을 **오늘까지로 제한** — HRD-Net API의 `srchTraStDt/EndDt`는 훈련시작일 범위라서 모집 중(개강 예정) 과정이 아예 수집되지 않고 있었음

### 결정 사항
- **ETL 수집 종료일을 `today + ETL_FUTURE_DAYS`(기본 90일)로 확장** (config.py) — 모집 중·개강 예정 과정 수집. 상수만 추가, 환경변수 아님
- 시장 분석 페이지에 **📅 개강 현황 탭** 신설: 상태별 KPI(개강 예정/이번 달 개강/진행 중/개강 예정 평균 모집률), 월별 개강 예정 수 차트, 상태 라디오(개강 예정·진행 중·종료)로 전환하는 과정 목록(개강까지 D-일, 개강일, 종강일, 정원, 신청인원, 모집률). 사이드바 필터 공유
- 상태 판정은 SQL에서 수행(개강 예정 = `TR_STA_DT > 오늘`) — 40만 행 테이블이라 전체 로드 대신 상태별 LIMIT 1,000 조회
- 미래 데이터 유입 영향: 기존 추이 차트는 `YEAR_MONTH < 이번 달` 필터로 왜곡 없음. 페이지 상단 KPI(검색된 과정 수·평균 모집률)에는 모집 중 과정이 포함됨 — 조회 기간 필터로 제외 가능. 홈 화면은 확정 스냅샷이라 무영향
- 파생 수정: `test_hrd_api.py` 픽스처의 종강일(2026-07-15)이 실제 시간 경과로 과거가 되어 실패 → `2099-12-31`로 고정 (동일 파일 기존 관례)

### 영향 범위
- 수정: pages/시장_분석.py, market_etl.py, config.py, tests/test_market_etl.py, tests/test_hrd_api.py
- 문서: docs/GLOSSARY.md(개강 현황 탭 용어), docs/DEV_LOG.md
- 주의: 개강 예정 데이터는 다음 market_etl 실행(매일 KST 21시)부터 반영 — 그 전까지 탭에 안내 문구 표시

## 2026-07-15 — 운영 현황 다중 기관 지원 (엔코아 AI캠퍼스 추가)

### 배경
- 한화 과정 전체 종료로 운영 현황이 "쉬는 시간" 상태 지속. 엔코아 자체 운영기관으로 신규 과정 2개 개설(GraphRAG·AI 오케스트레이션, 기수 5개) — 새 인증키 발급
- 삽질: 새 키로 과정 목록(_3)은 조회되는데 명부/출결(_4)만 전부 0건 → 원시 응답 대조로 `요청하신 훈련기관에서 운영하는 과정만 조회가 가능합니다` 확인. **명부/출결 API만 키-기관 소유권을 검사**하고 과정 목록은 검사하지 않음 (상세: docs/api/hrd_net.md). 1차 발급 키가 엔코아 소속이 아니었던 것이 원인 — 올바른 키 재발급으로 해소
- 파생 발견: `test_hrd_api.py`의 `@patch.dict`에 `clear=True` 누락 → 로컬 `.env`의 ENCORE_* 값이 테스트에 유입되어 결과가 환경 의존적이었음

### 결정 사항
- 키 교체가 아닌 **기관별 (인증키, 과정ID) 쌍 병렬 등록**: `hrd_api.get_institutions()` + `fetch_all_institutions()` 순회·병합. 한화 쌍 유지(과거 데이터 DB 폴백용), 일부 과정 실패 시 나머지 유지, 전체 실패 시에만 DB 폴백
- 과정 2개 × 기수 중복(각 1·2회차)으로 기수 번호 충돌 → **사이드바 과정 선택으로 선필터** 후 기존 로직 무수정 재사용 (Tab1 합산 개편 대신 최소 변경 선택)
- 기본 선택값 = 출결이 가장 최근에 찍힌 과정·기수 — 개강 예정 기수가 잡혀 첫 화면이 0명으로 보이는 문제 회피
- 페이지명 `현재 운영 현황` → `AI캠퍼스 운영 현황`
- **미확장(의도적)**: hrd_etl.py는 여전히 한화 단일 과정만 수집 — 엔코아 데이터는 DB 미적재 → 누적 위험 지표는 당월 한정, 매출 분석·종료과정 성과 미반영, API 장애 시 엔코아 폴백 없음. ETL 확장은 후속 작업

### 영향 범위
- 수정: hrd_api.py, pages/현재_운영_현황.py, home.py, tests/test_hrd_api.py (테스트 203→209개)
- 문서: CLAUDE.md(환경 변수), README.md, docs/api/hrd_net.md(소유권 제약)
- 배포: Streamlit Cloud secrets에 `ENCORE_API_KEY`·`ENCORE_COURSE_IDS` 등록 완료 (GitHub Actions는 ETL 미확장으로 불필요)

## 2026-07-14 — 앱 전체 Segfault 원인 규명: pyarrow 24.0.0 고정

### 배경
- 7/14 오전부터 데이터 페이지 브라우징 시 앱 프로세스가 Segmentation fault로 사망 — 클라우드 로그 `201 Segmentation fault ... streamlit`. 부팅·비밀번호 게이트까지는 정상이라 keepalive(게이트만 방문)로는 탐지되지 않았고, 사람이 데이터 페이지를 열 때만 죽어 "Error running app" 반복
- 원인: **pyarrow 25.0.0(7/10 릴리즈)의 SIGSEGV 버그**(bundled mimalloc, Streamlit 메인테이너 lukasmasuch 진단, 포럼 다발 보고). `st.dataframe` 직렬화 경로에서 발생. pyarrow는 requirements.txt에 없는 streamlit 간접 의존성이라 7/13 이후 환경 재빌드부터 유입 — 7/14 직접 의존성 전체 핀 고정에서도 누락됐던 지점
- 로컬 재현: pyarrow 25.0.0에서 매출 분석 로딩 중 서버 exit 139(SIGSEGV) 재현 → 24.0.0으로 다운그레이드 후 데이터 페이지 5종 + 동시 세션 3개 모두 정상

### 결정 사항
- requirements.txt에 `pyarrow==24.0.0` 명시 고정 (참고 주석 포함). streamlit 1.59.2+ 업그레이드 시 재검토
- 교훈: 간접 의존성도 네이티브 라이브러리(pyarrow 등)는 핀 대상 — "직접 의존성만 고정"의 사각지대

### 영향 범위
- requirements.txt, docs/DEV_LOG.md

## 2026-07-14 — 홈 화면 정적 스냅샷 전환 (DB 조회 제거)

### 배경
- 전 과정 종료로 홈 수치는 확정 상태인데 매 접속마다 DB 4종 조회 발생 — 벤치마크 쿼리는 40만 행 테이블 필터로 로컬 0.6초, 클라우드(미국↔서울)에선 1~3초, 30분 TTL마다 반복
- 시장 데이터가 매일 증가해 동적 재계산값이 확정값에서 드리프트하는 문제 실증: 전국 KDT 모집률 60.5(7/8 확정) → 60.44(7/14 재계산), 누적 매출 캐시 합계 104.0996억 — 버림 표시식으로 104.0억이 되어 원장 확정 104.1억과 불일치

### 결정 사항
- `build_home_snapshot.py` 신설: DB 집계 → `data/home_snapshot.json` 생성 후 커밋. home.py는 스냅샷만 읽어 렌더 — DB 접근 0회, 비밀번호 통과 후 KPI 표시 0.3초, DB 장애에도 홈 무영향
- 벤치마크 3종(mkt_recruit 60.5 · mkt_satis 85.7 · our_satis 90.3)과 누적 매출 헤드라인(104.1억)은 **원장 확정값 고정**(`LEDGER_*` 상수) — 순위 3종 하드코딩과 동일 정책. 재생성 시 재계산값과의 차이를 자동 출력해 드리프트 가시화
- 누적 총 매출 표기 `104.1억+` → `104.1억` — 매출이 확정되어 "+"의 증가 뉘앙스 제거
- 갱신 절차: 취업률 확정(HRD-Net 'B' 코드 해제, 2026년 말~2027년 초 예상) 후 스크립트 재실행 → 테스트 → 커밋. 시장 분석·채용 동향 등 나머지 페이지는 동적 유지

### 영향 범위
- 수정: home.py, CLAUDE.md / 신설: build_home_snapshot.py, data/home_snapshot.json, tests/test_home_snapshot.py (테스트 191→203개)

## 2026-07-08 — 수강생 실명 마스킹 (개인정보 보호)

### 배경
- 개인정보 처리 강화 차원에서 개인 단위 데이터(훈련생 성명 등)는 화면 표시 계층에서 마스킹하기로 결정
- 성과 지표(모집률·만족도 등)는 고용24 공공 데이터지만 개인 단위 데이터는 개인정보 — 캠프 종료로 실명 운영 필요성도 소멸

### 결정 사항
- `utils.mask_name()`(홍길동→홍*동) / `mask_name_columns()` 신설 — **데이터 로드 직후 표시 계층 마스킹, DB 원본 불변**
- 적용 5개 페이지: 현재_운영_현황(get_active_data 단일 관문) · 종료과정_성과 · 매출_분석 · AI_리포트(**외부 AI API로 전송되는 프롬프트에도 마스킹된 이름만 포함**) · SQL_Playground(결과 표시 전 마스킹 — 별칭(AS) 조회는 미적용 한계 주석 명시)
- 부수 정리: 매출_분석 미사용 import·변수 제거, utils 스트림릿 가용성 프로브 import에 noqa 명시

### 영향 범위
- 수정: utils.py, pages 5종, tests/test_utils.py (테스트 213→224개)

## 2026-07-08 — KDT 전국 순위 산출 기준 확정·문서화

### 배경
- 홈의 순위 3종(2023 10/300 · 2024 14/611 · 2025 22/561)이 하드코딩인데 산출 기준이 미기록, 일부 문서에 "성과평가 순위"로 오기되어 있었음
- 시장 데이터 재현 검증으로 기준 확정: **과정(TRPR_ID) 단위 모집 인원 합 랭킹** — 2023년은 개강 분기(10~12월 개강 과정) 대상, 2024·2025년은 연간 대상. 현 데이터로 10/301·14/612·22/563 재현(분모 +1~2는 이후 수집분)

### 결정 사항
- UI 표기를 "KDT 전국 순위 (모집 인원 기준)"으로 정정하고 산출 기준 캡션 추가 — 심사평가원 '성과평가'와 별개 지표
- **동적 계산으로 전환하지 않음** (사용자 결정): 분모가 수집분에 따라 ±1~2 흔들려 확정값(300·611·561)과 어긋나므로 확정 시점 스냅샷 유지

### 영향 범위
- 수정: home.py (라벨·캡션), docs/GLOSSARY.md (Section B 'KDT 전국 순위' 지표 정의 신설)

## 2026-07-08 — 홈 대시보드를 캠프 최종 성과 리포트로 개편

### 배경
- 캠프 종료(2026-07-03, 1~25기 전 과정 완주)로 홈 화면의 역할이 "운영 현황판" → "최종 성과 증거물"로 전환
- 대외 공유에 인용되는 확정 수치를 대시보드가 라이브로 뒷받침해야 함

### 결정 사항
- 타이틀·nav를 **한화시스템 BEYOND SW캠프 성과 대시보드**로 변경, 헤더에 "1기~25기 전 과정 종료" 명시
- KPI 재구성: `운영 기수`·`모집률`·`만족도`·`누적 총 매출` 신설, 모집률·만족도는 **전국 KDT 평균 대비 델타** 표시
- 수료율 KPI를 기수 단순평균 → **전 기수 합산(Σ수료/Σ수강)** 기준으로 전환 — 핵심수치 원장과 일치, help에 기수 평균 병기
- `get_market_benchmark()` 신설: TB_MARKET_TREND에서 K-디지털 트레이닝 필터로 전국 모집률(신청 0 포함·상한 100%)·만족도(STDG_SCOR>0, 100점 환산) 동적 집계 + 자사 만족도(기수 평균) — utils.calc_recruit_rate 재사용
- "전국 KDT 시장 비교" 표 추가 (본 과정 vs 전국 평균 vs 격차), "연도별 운영 규모" 차트 → **기수별 모집·수료 인원** 차트 + **1~25기 전체 성과 테이블**로 교체

### 영향 범위
- 수정: home.py, CLAUDE.md(수료율 KPI 정의), docs/GLOSSARY.md(만족도 표기·home.py 카탈로그 동기화, 낡은 `6개월 취업률` 표기 정정)

## 2026-07-08 — market_etl 전체 재수집 모드 (ETL_FULL_REFRESH)

### 배경
- `STDG_SCOR`(만족도)는 기수별 점수가 아니라 **수집 시점의 과정 누적 만족도 스냅샷** — 증분 윈도우(12개월, 훈련시작일 기준) 밖 행은 영구히 낡은 값으로 남음 (자사 1~15기: 2026-02-09 수집분으로 고정, 현행 API값과 불일치)
- 25기 만족도 조사 종료(2026-07-07) 후 전 기수 값 통일 필요 → 수동 전체 재수집 수단 부재

### 결정 사항
- `ETL_FULL_REFRESH=1` env로 `get_collect_range()`가 증분 대신 `ARCHIVE_START(2023-01-01)~오늘` 전체 수집
- workflow_dispatch에 `full_refresh` boolean 입력 추가, `timeout-minutes` 60→180 (전체 43개월 수집 대비)
- 스케줄 실행은 기존과 동일하게 증분 유지 (입력 미지정 시 빈 문자열)

### 영향 범위
- 수정: config.py, market_etl.py, .github/workflows/market_etl.yml, CLAUDE.md
- 테스트: TestGetCollectRange 1개 추가 (212→213개)

## 2026-06-30 — 페이지 로딩 성능 개선 (콜드 로드 절감)

### 결정 사항
- 느림의 원인은 "캐싱 부재"가 아니라 **콜드 로드**(첫 방문/TTL 만료/Streamlit Cloud 휴면 깨어남) 시 Supabase 쿼리 다중 왕복으로 진단. 거의 모든 로더는 이미 `@st.cache_data` 적용 상태.
- `CACHE_TTL_DEFAULT` 600초(10분) → 1800초(30분). 내부 데이터는 ETL이 평일 매시간 갱신하므로 10분 캐시는 과도하게 짧아 콜드 로드가 잦았음.
- `init_db.py`에 누락 인덱스 3종 추가: `TB_TRAINEE_INFO(TRPR_DEGR)`, `TB_ATTENDANCE_LOG(TRNEE_ID)`, `TB_COURSE_MASTER(TR_STA_DT)`. 운영 중인 Supabase에는 신규 init이 적용되지 않으므로 `sql/perf_indexes.sql`을 1회 수동 실행.
- `DB_명세.py` 테이블별 COUNT 루프(7회 왕복) → 단일 `UNION ALL` 쿼리(1회). 실패 시 기존 루프로 폴백.

### 보류 / 대안
- **apply/iterrows 벡터화**: 대부분 수백 행짜리 작은 DataFrame이라 실제 비용이 작고, 출석률·매출 등 검증된 비즈니스 로직이라 회귀 위험이 커 보류.
- **탭 lazy-load**: `st.tabs`는 모든 탭 본문을 매 rerun 실행 → 진짜 lazy-load하려면 radio 전환(UX 변경)이 필요. 회귀 위험 대비 이득이 TTL 상향으로 줄어 일괄 적용 보류, 필요 시 가장 무거운 페이지부터 개별 적용 검토.

### 영향 범위
- 수정: config.py, init_db.py, pages/DB_명세.py, docs/DEV_LOG.md
- 신규: sql/perf_indexes.sql

## 2026-03-11 — 사람인 채용공고 API 통합

### 결정 사항
- 사람인 API 선택 (Work24 HRDPOA60_2.jsp는 후순위)
- XML이 아닌 JSON 파싱 채택 (API가 기본 JSON 반환)
- 키워드 15개 × 최대 3페이지 수집 전략 (일일 500회 중 ~45회 사용)

### 삽질 기록
- API 엔드포인트 오류: `/guide/v1/job-search`(문서 페이지) → `/job-search`(실제 API)
- XML 파서로 구현했으나 실제 응답은 JSON → 전면 교체
- PG에서 KPI 값 None 반환 → f-string 포맷 에러 → int(... or 0) 처리

### 영향 범위
- 신규: saramin_etl.py, pages/채용_동향.py, tests/test_saramin_etl.py, saramin_etl.yml
- 수정: config.py, init_db.py, home.py, CLAUDE.md, API_SPEC.md, settings.json

---

## 2026-03-중순 — 사람인 ETL 수집 전략 안정화

### 결정 사항
- 페이징 로직 시행착오 끝에 제거 — 사람인 API가 offset 기반 페이징을 공식 지원하지 않음
- `published_min/max` unix timestamp 기반 절대 날짜 필터로 전환 (환경변수 `SARAMIN_PUBLISHED_DAYS`로 범위 설정, 기본 7일)
- 키워드당 최대 110건 수집 (`SARAMIN_PAGE_SIZE`), 일일 API 호출 한도 480회 안전마진 확보

### 삽질 기록
- 페이징 복원 → 오프셋 수정 → 재제거: 3회 반복 후 API 한계 확인하고 단일 요청 방식 확정
- `SARAMIN_PUBLISHED_DAYS` 도입으로 백필(과거 데이터 보강) 지원

### 영향 범위
- 수정: saramin_etl.py, config.py

---

## 2026-03-하순 — 채용공고-키워드 다대다 매핑 테이블 도입

### 결정 사항
- `TB_JOB_KEYWORD` 정션 테이블 추가 — 하나의 공고가 여러 키워드로 수집될 수 있어 다대다 관계 정규화
- 키워드별 추이 분석 시 정션 테이블 우선 조회, 미존재 시 `TB_JOB_POSTING` 폴백

### 삽질 기록
- 정션 테이블 미존재 환경에서 폴백 쿼리가 `YEAR_MONTH` 컬럼을 직접 참조하지 않아 오류 → 수정

### 영향 범위
- 신규: TB_JOB_KEYWORD 테이블 (init_db.py)
- 수정: saramin_etl.py, pages/채용_동향.py

---

## 2026-03-말 — 채용 동향 페이지 개선 및 조퇴 판정 변경

### 결정 사항
- 채용 동향 페이지: 캐시(TB_MARKET_CACHE) 의존 제거 → 직접 쿼리 전환으로 실시간성 확보
- 진행중/종료 공고 분리 분석 기능 추가
- 조퇴 판정: API 상태값 대신 `OUT_TIME` 기준으로 변경 — 실제 퇴실 시각이 더 정확
- 사이드바 네비게이션을 역할 기반 그룹(개요/성과 분석/외부 동향/채용/도구)으로 재편

### 영향 범위
- 수정: pages/채용_동향.py, pages/현재_운영_현황.py, home.py

---

## 2026-04-01 — DB 명세 페이지에 사람인 테이블 추가

### 결정 사항
- DB 명세 페이지에서 TB_JOB_POSTING, TB_JOB_KEYWORD 테이블의 컬럼 명세 및 채움률 표시
- 사람인 API 명세 문서(docs/api/saramin.md)에 DB 테이블 스키마 추가
- 키워드 추이 차트 월 정렬을 시간순(chronological)으로 수정

### 영향 범위
- 수정: pages/DB_명세.py, docs/api/saramin.md

---

## 2026-04-01 — 사람인 ETL 수집량·품질 대폭 개선

### 결정 사항
- 게시일 범위 7일→3일 축소 + **1일 단위 분할 호출** — 키워드당 4 API 호출로 110건 한계 극복
- 인기 키워드 세분화 (16→20개) — Python/Java/AI/데이터를 세분화하여 커버리지 확대
- `TB_JOB_POSTING_REGION` junction 테이블 추가 — 서울+경기 동시 모집 같은 다중 지역 정확 반영
- SEARCH_KEYWORD UPSERT 시 최초값 보존 (덮어쓰기 방지)
- YEAR_MONTH null 폴백 체인 (POSTING_DT → OPENING_DT → MODIFICATION_DT)
- 110건 도달 시 WARNING 로그 출력으로 누락 키워드 사전 식별

### 삽질 기록
- 3일 통합 호출(1,956건) → 1일 분할 호출(4,578건)로 수집량 2.3배 증가 확인
- API 호출 80회/480 한도(17%)로 여유 충분
- `데이터 분석`(일 690건+), `보안`(일 110건+)은 1일 단위로도 110건 초과 — API 페이징 미지원 한계로 추가 세분화 외 방법 없음. 현재 수준으로 운영 판단

### 영향 범위
- 수정: config.py, saramin_etl.py, init_db.py, pages/채용_동향.py, CLAUDE.md
- 신규: TB_JOB_POSTING_REGION 테이블
- 테스트: 17→30개 (전체 209개 통과)
