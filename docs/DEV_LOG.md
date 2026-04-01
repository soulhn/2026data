# 개발 일지

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
