---
paths:
  - "**/*.py"
---

## 쿼리 호환성

모든 SQL 쿼리는 `adapt_query()` 통과 필수 (utils.py).
- `?` → `%s` 자동 변환 (PostgreSQL)
- `INSERT OR IGNORE` → `ON CONFLICT DO NOTHING` 자동 변환
- `pd.read_sql()` 직접 호출 대신 `load_data()` 사용 권장

## 날짜 형식

`TB_MARKET_TREND.TR_STA_DT` = `'YYYY-MM-DD'` 형식으로 저장.
WHERE 절 날짜 파라미터는 반드시 `strftime('%Y-%m-%d')` 사용.
`YYYYMMDD` 형식 사용 시 문자열 비교 오류로 데이터 누락.

## PostgreSQL 주의사항

- `COUNT(*) AS cnt` 별칭 필수 — `RealDictCursor`에서 `row[0]` 불가, `row['cnt']` 사용
- 컬럼명 소문자 반환 → `load_data()`에서 대문자 변환 처리됨 (직접 처리 불필요)
- PG 읽기: `@st.cache_resource` 커넥션 풀링 (`_get_pg_pool()`)으로 TCP 재연결 방지

## DB 이중 지원

`DATABASE_URL` 환경변수 있으면 PostgreSQL, 없으면 SQLite.
`is_pg()`, `get_database_url()`, `adapt_query()` 모두 `utils.py` 소재.
