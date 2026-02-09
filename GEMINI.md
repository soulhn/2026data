# 🤖 AI 에이전트 협업 로그

이 문서는 AI 에이전트와의 협업을 통해 이루어진 주요 변경 사항을 기록하는 로그입니다. 각 세션별로 작업 목표, 변경 내용, 그리고 관련 커밋을 추적하여 프로젝트의 유지보수성과 투명성을 높입니다.

---

## 세션 1: 프로젝트 초기 설정 및 문서 정리 (Gemini)

- **날짜:** 2026년 2월 3일
- **목표:** 프로젝트 실행 환경을 바로잡고, 코드와 문서의 불일치 문제를 해결.

### 주요 변경 사항:

1.  **`requirements.txt` 의존성 수정:**
    - `streamlit`, `plotly`, `altair` 라이브러리를 추가하여, `README.md`의 실행 가이드에 따라 프로젝트가 정상적으로 동작하도록 수정함.
    - **관련 파일:** `requirements.txt`

2.  **`README.md` 내용 수정:**
    - 실제 코드에 구현되지 않은 '워드클라우드' 시각화 기능에 대한 언급을 `README.md`에서 삭제함.
    - **관련 파일:** `README.md`

### 관련 커밋:

- `Fix: requirements.txt에 누락된 의존성 추가` (커밋 ID: `46d865f`)
- `Docs: README.md에서 미구현된 워드클라우드 기능 언급 삭제` (커밋 ID: `ff8fa15`)

---

## 세션 2: ETL 스크립트 오류 해결 (Gemini)

- **날짜:** 2026년 2월 3일
- **목표:** `hrd_etl.py` 실행 시 발생하는 `database is locked` 오류 및 `UnicodeEncodeError` 해결.

### 주요 변경 사항:

1.  **`hrd_etl.py` DB 연결 수정:**
    - `sqlite3.connect` 함수에 `timeout=30` 파라미터를 추가하여 DB lock으로 인한 대기 시간을 30초로 설정함.
    - **관련 파일:** `hrd_etl.py`

2.  **`hrd_etl.py` 출력문 수정:**
    - Windows `cp949` 코덱과 호환되지 않는 이모지(e.g., `🚀`, `🎉`)를 모두 제거하여 `UnicodeEncodeError`를 해결함.
    - **관련 파일:** `hrd_etl.py`

---

## 세션 3: 코드 품질 개선 및 보안 강화 (Claude Code)

- **날짜:** 2026년 2월 9일
- **에이전트:** Claude Code (Opus 4.6)
- **목표:** 프로젝트 전반의 코드 품질, 보안성, 유지보수성 개선. 총 7개 파일, 6개 항목 일괄 리팩토링.

### 주요 변경 사항:

1.  **bare `except` 제거 및 에러 로그 추가:**
    - `hrd_etl.py`의 4개 bare `except`를 `except (ValueError, TypeError) as e` 또는 `except Exception as e`로 변경.
    - ETL 수집 실패 시 회차/월 정보와 함께 에러 메시지가 출력되도록 개선.
    - **관련 파일:** `hrd_etl.py`

2.  **SQL Injection 방지 (파라미터 바인딩):**
    - `utils.py`의 `load_data()`에 `params` 파라미터 지원 추가.
    - 페이지 1(기수별 분석), 페이지 2(진행과정 관리)의 f-string SQL을 `?` 플레이스홀더 방식으로 전환.
    - **관련 파일:** `utils.py`, `pages/1_기수별_분석.py`, `pages/2_진행과정_관리.py`

3.  **`@st.cache_data` TTL 설정:**
    - TTL이 없던 `home.py`(10분), 페이지 1(10분), 페이지 2(5분, 실시간 모니터링), 페이지 3(10분)에 적절한 TTL 추가.
    - ETL 실행 후 캐시 만료 시 자동으로 최신 데이터 반영.
    - **관련 파일:** `home.py`, `pages/1_기수별_분석.py`, `pages/2_진행과정_관리.py`, `pages/3_데이터_감사.py`

4.  **DB 연결 함수 통합:**
    - `utils.py`의 `get_connection()`에 `timeout`, `row_factory` 파라미터 추가.
    - `hrd_etl.py`의 자체 `get_db_connection()` 함수 제거 후 `utils.get_connection()` 사용으로 통합.
    - `pages/3_데이터_감사.py`의 자체 `get_connection()` 정의도 utils로 위임.
    - **관련 파일:** `utils.py`, `hrd_etl.py`, `pages/3_데이터_감사.py`

5.  **API 키 노출 방지:**
    - `hrd_etl.py`의 3개 API 호출(과정 목록, 명부, 출결)을 URL 직접 삽입에서 `requests.get(url, params={})` 방식으로 전환.
    - 로그/디버그 출력 시 API 키가 노출되지 않도록 개선.
    - **관련 파일:** `hrd_etl.py`

6.  **유틸 함수 중복 제거:**
    - `safe_float()`, `safe_int()`를 `utils.py`에 통합 정의 (default 파라미터 지원).
    - `market_etl.py`, `pages/1_기수별_분석.py`, `pages/3_데이터_감사.py`의 로컬 중복 정의 제거 후 utils import로 전환.
    - `utils.py`의 `calculate_age_at_training()` 내 bare `except`도 `except (ValueError, TypeError)`로 수정.
    - **관련 파일:** `utils.py`, `market_etl.py`, `pages/1_기수별_분석.py`, `pages/3_데이터_감사.py`

### 변경 규모:

- **변경 파일:** 7개 (`utils.py`, `home.py`, `hrd_etl.py`, `market_etl.py`, 페이지 1~3)
- **변경량:** +82줄 / -70줄

---