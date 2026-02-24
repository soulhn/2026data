---
paths:
  - "pages/**/*.py"
  - "home.py"
---

## Y축 라벨 세로 정렬

차트 y축 제목은 한 글자씩 세로로 배치한다. Plotly는 축 제목 회전 제어가 안 되므로
**y축 제목이 필요한 차트는 Altair를 사용**한다.

```python
# ✅ Altair — 글자를 리스트로 분리 + titleAngle=0
y=alt.Y('비율:Q', axis=alt.Axis(title=['비', '율', '(%)'], titleAngle=0))

# ❌ Plotly — 제목이 90도 회전되어 읽기 어려움
fig.update_layout(yaxis_title='비율 (%)')
```

Plotly는 x축 카테고리(`type='category'`, `update_xaxes`) 또는 시계열 등
y축 제목이 필요 없는 경우에만 사용한다.
