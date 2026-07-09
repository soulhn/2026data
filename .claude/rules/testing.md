---
paths:
  - "tests/**"
---

## 테스트 실행

```bash
python -m pytest tests/ -v          # 전체
```

`conftest.py`: `_NoCloseConnection` 프록시로 인메모리 SQLite fixture 제공.

