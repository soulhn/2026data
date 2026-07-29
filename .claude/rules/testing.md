---
paths:
  - "tests/**"
---

## 테스트 실행

```bash
python -m pytest tests/ -v          # 전체
```

`conftest.py`: `_NoCloseConnection` 프록시로 인메모리 SQLite fixture 제공.

`from utils import is_pg`로 값 바인딩한 모듈을 테스트할 땐 **그 모듈 속성을 패치**해야 한다.
`monkeypatch.setattr(utils, "is_pg", ...)`는 `hrd_etl.is_pg`를 바꾸지 못한다.

```python
monkeypatch.setattr(hrd_etl, "is_pg", lambda: False)   # ✅
monkeypatch.setattr(utils, "is_pg", lambda: False)     # ❌ 대상 모듈에 안 먹음
```

## CI

`.github/workflows/tests.yml` — push(main)/PR마다 Python 3.12로 `python -m pytest tests/ -q`.

CI에는 `.env`가 없어 `DATABASE_URL`이 비고 `is_pg()`가 False가 된다 →
**워크플로에 `DATABASE_URL`을 주입하지 말 것.** 주입하면 ETL 테스트가 psycopg2 경로로
새어 나가 `executemany` 검증이 무력화되고, 프로덕션 접속 문자열이 러너에 노출된다.

