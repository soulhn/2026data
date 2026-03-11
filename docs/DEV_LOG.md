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
