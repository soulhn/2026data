---
paths:
  - "tests/**"
---

## 테스트 실행

```bash
python -m pytest tests/ -v          # 전체 (168개)
python -m pytest tests/test_billing.py -v  # 매출 청구 로직만
```

`conftest.py`: `_NoCloseConnection` 프록시로 인메모리 SQLite fixture 제공.

## test_billing.py 주의

실제 HRD-Net 청구 금액으로 검증된 33개 테스트 — 비즈니스 로직 변경 없이는 수정 금지.

핵심 로직:
- 버림 단위: `(raw // 10) * 10` (10원 단위, 단위기간별 적용)
- 카드사 소계 버림 한계: 최대 10원/기간 오차 허용
- 중도입과: rate 분모 = `student_td` (개인 훈련일수)
- 중도탈락: rate 분모 = `period_td` (기간 전체)
