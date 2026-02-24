---
name: test
description: Run the full pytest test suite for this project
allowed-tools: Bash(python -m pytest:*)
---

Run: `python -m pytest tests/ -v`

결과를 요약해서 보고:
- 전체 통과/실패 수
- 실패한 테스트가 있으면 에러 메시지와 원인 요약
