# 노션 웹훅으로 HRD등록 즉시 반영 — 설정 절차

**2026-09-20 19:17 가동 확인**: 최종결과 → HRD등록 변경 후 노션 알림 몇 초 → 함수 `workflow_dispatch 204` → GitHub `KPI poll` 실행 → 신청자 폴링이 전이 1건 감지. 함수 URL `https://tyoxlhernofdrgqccvww.supabase.co/functions/v1/notion-relay`, 구독 API 버전 2026-03-11, 이벤트 page.properties_updated.
실측 지연: 알림까지 수 초, 파이프라인 완료까지 처음 약 6분 → `kpi_etl.py --kpi-only`(AI캠퍼스 9회차만)로 바꿔 **약 3분** (2026-09-20 저녁).

**2026-09-21 06:02 SKN도 가동 확인** (SKN 신청자 리스트 HRD등록 → 함수 → GitHub 실행 2분 58초). 발행 대상은 「HRD 등록자 관리」 페이지.

코드는 끝났다(`supabase/functions/notion-relay/index.ts`, `kpi_poll.yml`, `notify.py`). 아래는 계정이 필요한 사용자 쪽 작업, 약 30분.

```
담당자: 최종결과 → HRD등록
  → 노션 웹훅 (몇 초) → notion-relay: 서명 검증 → 그 페이지 최종결과만 읽어 HRD등록인지 확인
  → GitHub workflow_dispatch → kpi_poll.yml (약 2분): 명부 스냅샷 → 전이 로그 → KPI 페이지 갱신 → 디스코드
```
결과는 하루 2회 폴링과 같고 반영 시점만 반나절 → 약 3분. 다른 전이(합격취소 등)는 여전히 하루 2회에 잡힌다 — 즉시 잡고 싶으면 4단계 `TRIGGER_STATUSES`에 추가.

## 1. GitHub 토큰 (워크플로 실행 권한만)
**저장소 설정이 아니라 계정 설정**에 있다: 우측 상단 프로필 사진 › Settings › 왼쪽 맨 아래 Developer settings › Personal access tokens ›
**Fine-grained tokens** › Generate new token. 바로 가기: https://github.com/settings/personal-access-tokens/new
- Repository access: **Only select repositories → soulhn/2026data**
- Permissions › Repository › **Actions: Read and write** (그 외 없음. Metadata Read-only는 자동으로 붙음). Expiration 1년
- 발급 2026-09-20, 토큰 이름 `notion-relay`, **만료 2027-09-20** → 그때 같은 화면에서 Regenerate 후 Supabase Secrets `GITHUB_TOKEN` 교체
- "No expiration"을 고르면 숨은 날짜 칸 때문에 발급이 막힐 수 있다 → 기본 선택지(1 year)로
- `github_pat_…` 복사 (한 번만 보임)

## 2. 중계 함수 배포 (Supabase 대시보드)
1. Supabase › 메인 프로젝트(2026data) › **Edge Functions** › Deploy a new function › Via Editor
2. 이름 `notion-relay`, 코드 = `2026data/supabase/functions/notion-relay/index.ts` 전체 붙여넣기 › Deploy
3. 함수 › Details(또는 Settings) › **Verify JWT with legacy secret: 끔** (노션은 JWT를 못 보낸다). 확인: 헤더 없이 `curl -X POST <함수 URL>` → 401이면 아직 켜짐, `POST only`/`bad json`이면 꺼짐
   ⚠️ **재배포할 때마다 다시 켜지는 버그**(supabase/supabase#43608) — 코드 고쳐 올린 뒤엔 반드시 다시 끈다
4. Edge Functions › **Secrets**:
   | 이름 | 값 |
   |---|---|
   | `NOTION_TOKEN` | 통합 sul 토큰 (`.env`와 같은 값) |
   | `GITHUB_TOKEN` | 1번 토큰 |
   | `NOTION_VERIFICATION_TOKEN` | 아직 비움 — 3단계에서 채운다 |
5. 함수 URL 복사: `https://<project-ref>.supabase.co/functions/v1/notion-relay`

## 3. 노션 웹훅 구독 (통합 설정 화면)
1. notion.so/profile/integrations › `sul` › **Webhooks** 탭 › **+ Create a subscription**
2. Webhook URL = 2-5 주소 › 다음. 노션이 그 주소로 `{"verification_token": "secret_…"}`를 한 번 보낸다
3. Supabase › Edge Functions › notion-relay › **Logs**에서 `verification_token = secret_…` 줄을 찾아 값 복사
4. 노션 화면의 "Verification token" 칸에 붙여넣기 › Verify
5. **같은 값을 Supabase Secrets `NOTION_VERIFICATION_TOKEN`에 저장** (이후 모든 이벤트의 서명을 이 값으로 검사한다)
6. Event types: **page.properties_updated** 하나만 체크 › Save. 상태가 Active면 끝

> 통합이 연결된 페이지의 이벤트만 온다. 신청자 리스트는 이미 연결돼 있다. 노션은 같은 페이지의 연속 편집을 약 1분 단위로 묶어 보낸다.

## 4. 확인 (2026-09-20 통과)
삽질 기록: ① `NOTION_VERIFICATION_TOKEN` 시크릿을 빠뜨리면 모든 이벤트가 401 `bad signature` ② 재배포 후 Verify JWT가 다시 켜짐 ③ 첫 코드의 이벤트 필터가 2026-03-11 형식에서 `ignored` → 필터를 느슨하게 하고 구조 로그 추가

- 신청자 리스트에서 테스트 페이지 하나의 최종결과를 다른 값 → **HRD등록**으로 바꾼다 (끝나면 원래대로)
- Supabase 함수 로그: `최종결과=HRD등록 → workflow_dispatch 204`
- `gh run list --workflow kpi_poll.yml --limit 1` 에 새 실행. 2분 뒤 「모집 KPI」 사람별 표 갱신
- 다른 값으로 바꾸면 로그에 `→ 트리거 아님` (정상 — 하루 2회 폴링이 잡는다). 합격취소도 즉시 원하면 Secrets에
  `TRIGGER_STATUSES` = `HRD등록,합격취소` 추가 (앞부분 일치라 사유 무관)

## 5. 디스코드 알림 (선택)
디스코드 채널 › 연동 › 웹훅 › URL 복사 → `cd ~/Desktop/New_work/2026data && gh secret set DISCORD_WEBHOOK_URL`.
변화가 있을 때만 메시지 (가린 이름 + 기수만). 없으면 조용.

## 6. 비밀 관리
GitHub 토큰 · NOTION_VERIFICATION_TOKEN · 디스코드 URL은 코드·노션·문서에 적지 않는다. 만료: GitHub 토큰 1년.
