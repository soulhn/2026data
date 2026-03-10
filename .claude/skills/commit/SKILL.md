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
4. 커밋 메시지 제목 작성: `$ARGUMENTS`를 한글 설명으로 활용
5. 커밋 메시지 본문 작성: `git diff --cached`를 보고 변경 이유·증상·영향 범위를 bullet(`-`) 2~3줄로 작성
6. HEREDOC으로 제목+빈 줄+본문을 `git commit`에 전달
7. `Co-Authored-By` 라인 포함하지 않음

**커밋 메시지 HEREDOC 예시**:
```bash
git commit -m "$(cat <<'EOF'
Fix: Fix attendance rate calculation bug (출석률 계산 오류 수정)

- 수료 판정이 정확일치로 되어 있어 일부 유형 누락
- str.contains 패턴으로 변경하여 모든 수료 유형 포함
- home.py, 종료과정_성과.py에 영향
EOF
)"
```

**예시**: `/commit "출석률 계산 오류 수정"`
→ 제목: `Fix: Fix attendance rate calculation bug (출석률 계산 오류 수정)`
→ 본문: diff 기반 bullet 2~3줄 자동 생성
