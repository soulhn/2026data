---
name: commit
description: Run tests then create a git commit following project convention
allowed-tools: Bash(python -m pytest:*), Bash(git status:*), Bash(git add:*), Bash(git commit:*)
---

프로젝트 커밋 컨벤션 준수:

**형식**: `Tag: English summary (한글 설명)`

**태그**: Feat / Fix / Docs / Style / Refactor / Chore

**절차**:
1. `python -m pytest tests/ -v` 실행
2. 실패 시 → 중단하고 에러 보고 (커밋 하지 않음)
3. 전체 통과 시 → `git status` 확인 후 관련 파일 `git add`
4. 커밋 메시지 작성: `$ARGUMENTS`를 한글 설명으로 활용
5. `Co-Authored-By` 라인 포함하지 않음

**예시**: `/commit "출석률 계산 오류 수정"`
→ `Fix: Fix attendance rate calculation bug (출석률 계산 오류 수정)`
