# Notion API 명세 — 운영현황표 읽기 (대조 전용)

**용도:** `pages/노션_HRD_대조.py` — 회사 노션 "운영현황표 (매일 10시 기준)"을 읽어 HRD-Net 회차 집계와 같은 기수끼리 대조.
**원칙:** **읽기 전용.** 조회(`POST …/query`)만 호출하고 페이지·속성을 만들거나 고치는 엔드포인트는 쓰지 않는다.
**모듈:** `notion_ops.py`

---

## 인증·연결

| 항목 | 값 |
|---|---|
| Base | `https://api.notion.com/v1` (`config.NOTION_API_BASE`) |
| 버전 헤더 | `Notion-Version: 2022-06-28` (`config.NOTION_API_VERSION`) |
| 인증 | `Authorization: Bearer <NOTION_TOKEN>` — 내부 통합(Internal integration) 시크릿 |
| 대상 DB | `3e899143fd714dc28eb4af8c58e50e48` (`config.NOTION_OPS_DB_ID`) — 사업관리 › 운영현황 › 운영현황표 |
| 시간 제한 | 요청당 20초 (`config.NOTION_TIMEOUT`), 페이지 100행 (`config.NOTION_PAGE_SIZE`) |

**설정 절차**
1. notion.so/profile/integrations → 내부 통합 생성, 권한은 *콘텐츠 읽기*만.
2. 노션 운영현황표 페이지 `…` → 연결(Connections) → 통합 추가. (여기까지 안 하면 404)
3. 시크릿을 Streamlit secrets / `.env`에 `NOTION_TOKEN`으로 등록.

토큰 없이 쓰려면 노션에서 운영현황표를 CSV로 내보내 페이지에 업로드한다 (`notion_ops.load_ops_csv`). 날짜 `2026년 7월 9일`·ISO·영문 형식 모두 파싱.

---

## 호출: 데이터베이스 조회

`POST /databases/{db_id}/query`

```json
{
  "page_size": 100,
  "filter": {"property": "개강일", "date": {"on_or_after": "2023-01-01"}},
  "sorts": [{"property": "개강일", "direction": "descending"}],
  "start_cursor": "<이전 응답의 next_cursor>"
}
```

`has_more`가 false가 될 때까지 `next_cursor`로 반복. 응답의 `results[].properties`를 아래 표대로 정규화한다.

| 노션 속성 | 타입 | 정규화 컬럼 | 비고 |
|---|---|---|---|
| 과정명 | title | `과정명` | "… N기" 형식 |
| 구분 | multi_select | `구분` | KDT디선 / KDT디신 / AI캠퍼스 |
| 팀 · 강의실 | select | `팀` · `강의실` | |
| 개강일 · 종강일 | date | `개강일` · `종강일` | `start`만 사용 |
| 교육일수 | rich_text | `교육일수` | |
| 개강인원 · 초기이탈 · 중도이탈 · 지각/외출 · 결석 | number | 동명 (`지각외출`) | |
| 추가인원 (개강~확정자신고) | number | `추가인원` | |
| 확정자신고 · 현재인원 · 이탈합계 | formula(number) | 동명 | 비어 오면 원본 숫자로 재계산 |
| PM · 강사 · 지각 / 결석 메모 | people / text | **가져오지 않음** | 사람 이름 포함 |
| 과정 페이지 relation 181개 | relation | 무시 | |

**오류 매핑** (`NotionFetchError`, 화면에 그대로 표시)

| HTTP | 의미 |
|---|---|
| 401 | 토큰 오류 |
| 403 / 404 | DB가 통합에 공유되지 않았거나 ID 오류 |
| 기타 | 상태 코드 + 본문 앞 200자 |

---

## 노션 운영현황표 인원 흐름 (2026-09-08 확인)

```
개강인원 ─(−초기이탈 +추가인원)→ 확정자신고 ─(−중도이탈)→ 현재인원
```

- HRD-Net `totParMks`(이 앱의 "개강 인원") **= 노션 확정자신고**. AIO 1·2기, MLE 1·2기 모두 정확히 일치.
- 노션 `개강인원`(첫날 출석)은 HRD-Net에 없다. 따라서 진짜 "개강 참석률"(개강인원 ÷ HRD 등록)은 이 대조 화면에서만 계산된다.
- 노션에는 정원·수강신청(HRD 등록)·수료 집계가 없다.

## 대조 키

`config.COURSE_GROUP_KEYWORDS`(AIO·MLE·한화)로 과정명을 그룹으로 묶고, **그룹 + 개강일** 일치를 같은 기수로 본다.
HRD 회차 번호와 노션 기수 번호는 체계가 달라 키로 쓰지 않는다. 키워드에 안 걸리는 노션 과정(SK네트웍스, AI Ready Data 등)은 대조 대상에서 뺀다.
