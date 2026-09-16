"""노션 '모집 KPI' 페이지 발행 — 기수별·사람별 정합성 데이터베이스를 매시간 갱신.

쓰는 곳은 **우리 소유 페이지**(config.NOTION_KPI_PARENT_PAGE_ID) 아래 두 DB뿐이다. 담당자의 신청자 리스트·운영현황표는
계속 읽기만 한다. 사람별 표는 신청자 페이지에 관계(단방향)로 연결해 이름을 복제하지 않는다.

  기수별 정합성 : 기수 한 줄. 노션 신청자 퍼널(신청→합격→HRD신청→HRD등록) vs HRD-Net(수강신청·승인·명부·개강일 참석)
  사람별 정합성 : 합격 단계 이상 신청자 한 줄. 노션 상태 vs HRD 승인(명부 등장)·첫 참석. (이름 해시, 기수)로 대조

원천은 Supabase 테이블(TB_COURSE_SNAPSHOT·TB_ROSTER_MEMBER·TB_APPLICANT)이고, 행 내용 해시가 같으면 노션 호출을 건너뛴다
(TB_NOTION_PUBLISH). 두 DB가 없으면 첫 실행 때 만들고 ID를 TB_SYNC_STATE에 둔다.

실행: python notion_kpi_publish.py   (hrd_etl.yml 4번째 단계)
"""
import hashlib
import json
import logging
import os
import time
from datetime import datetime, timezone, timedelta

import requests
from dotenv import load_dotenv

import config
from init_db import init_all_tables
from notion_applicants_etl import get_sync_state, set_sync_state
from notion_ops import NotionFetchError
from utils import _clean_secret, adapt_query, get_connection, load_data

load_dotenv()

logger = logging.getLogger(__name__)
logging.basicConfig(format="%(asctime)s %(levelname)s %(name)s %(message)s", level=logging.INFO)

COHORT_DB_KEY = "notion_kpi_cohort_db"
PERSON_DB_KEY = "notion_kpi_person_db"
_HASH_EXCLUDE = {"변경 시각"}

# ── 노션 API (쓰기 포함, 우리 페이지 전용) ──────────────────────────


def _headers(token):
    return {"Authorization": f"Bearer {token}", "Notion-Version": config.NOTION_API_VERSION,
            "Content-Type": "application/json"}


def _request(token, method, path, payload=None, session=None):
    """429는 Retry-After 만큼 쉬고 재시도. 그 외 4xx/5xx는 NotionFetchError."""
    http = session or requests
    url = f"{config.NOTION_API_BASE}{path}"
    for attempt in range(4):
        try:
            resp = http.request(method, url, headers=_headers(token), json=payload, timeout=config.NOTION_TIMEOUT)
        except requests.RequestException as e:
            raise NotionFetchError(f"노션 API 연결 실패: {type(e).__name__}") from e
        if resp.status_code == 429:
            time.sleep(float(resp.headers.get("Retry-After", 1)) + 0.5)
            continue
        if resp.status_code >= 400:
            raise NotionFetchError(f"노션 API 오류 {resp.status_code} ({method} {path}): {resp.text[:200]}")
        time.sleep(config.NOTION_WRITE_INTERVAL)
        return resp.json()
    raise NotionFetchError("노션 API 429 재시도 초과")


def create_database(token, parent_page_id, title, properties, session=None):
    body = {"parent": {"type": "page_id", "page_id": parent_page_id},
            "title": [{"type": "text", "text": {"content": title}}], "properties": properties}
    return _request(token, "POST", "/databases", body, session)["id"]


def create_page(token, db_id, properties, session=None):
    return _request(token, "POST", "/pages", {"parent": {"database_id": db_id}, "properties": properties}, session)["id"]


def update_page(token, page_id, properties, session=None):
    return _request(token, "PATCH", f"/pages/{page_id}", {"properties": properties}, session)["id"]


def get_database(token, db_id, session=None):
    """DB 메타(is_inline·description 포함). 없거나 접근 불가(404)면 None."""
    try:
        return _request(token, "GET", f"/databases/{db_id}", None, session)
    except NotionFetchError as e:
        if "404" in str(e):
            return None
        raise


def database_exists(token, db_id, session=None):
    return get_database(token, db_id, session) is not None


def ensure_database_layout(token, db_id, description, meta=None, session=None):
    """DB를 페이지 안에 표로 보이게(is_inline) 하고 제목 아래 한 줄 설명을 맞춘다. 이미 맞으면 요청 없음."""
    if meta is None:
        meta = get_database(token, db_id, session) or {}
    current = "".join(t.get("plain_text", "") for t in meta.get("description", []))
    if meta.get("is_inline") and current == description:
        return False
    _request(token, "PATCH", f"/databases/{db_id}",
             {"is_inline": True, "description": [{"type": "text", "text": {"content": description}}]}, session)
    return True


# ── 스키마 ──────────────────────────────────────────────────────────


def _num():
    return {"number": {"format": "number"}}


COHORT_SCHEMA = {
    "기수": {"title": {}}, "KEY": {"rich_text": {}},
    "과정": {"select": {"options": [{"name": c} for c in config.NOTION_KPI_COURSES]}},
    "회차": _num(), "상태": {"select": {"options": [{"name": "개설예정"}, {"name": "진행중"}, {"name": "종료"}]}},
    "개강일": {"date": {}}, "종강일": {"date": {}},
    "정원": _num(), "수강신청(API)": _num(), "승인(API)": _num(),
    "노션 신청자": _num(), "합격 이상(노션)": _num(), "HRD신청(노션)": _num(), "HRD등록(노션)": _num(),
    "놓침": _num(),
    "정합성": {"select": {"options": [{"name": "일치"}, {"name": "불일치"}, {"name": "미확인"}]}},
    "명부 인원": _num(), "훈련중": _num(), "중도탈락": _num(), "조기취업": _num(), "수료(API)": _num(),
    "개강일 참석": _num(), "개강일 참석률(%)": _num(),
    "변경 시각": {"date": {}},
}

PERSON_SCHEMA = {
    "이름": {"title": {}}, "KEY": {"rich_text": {}},
    "신청자": {"relation": {"database_id": config.NOTION_APPLICANTS_DB_ID, "single_property": {}}},
    "기수": {"select": {"options": []}},
    "노션 상태": {"rich_text": {}},
    "HRD 신청 일시": {"date": {}},
    "HRD 승인": {"checkbox": {}}, "승인 감지": {"date": {}}, "명부 상태": {"rich_text": {}},
    "첫 참석일": {"date": {}}, "첫 입실": {"rich_text": {}}, "등록 지연(일)": _num(),
    "정합성": {"select": {"options": [{"name": n} for n in ("일치", "노션만 등록", "HRD만 승인", "동명이인 확인", "대기", "취소")]}},
    "변경 시각": {"date": {}},
    "메모": {"rich_text": {}},   # 담당자 입력 열 — 파이프라인은 절대 쓰지 않는다
}


def ensure_databases(token, conn, session=None):
    """두 DB가 없으면 만들고 ID를 TB_SYNC_STATE에 저장. 있으면 인라인·설명만 맞춘다. 반환: (cohort_db_id, person_db_id)."""
    ids = []
    for key, title, schema, desc in ((COHORT_DB_KEY, "기수별 정합성", COHORT_SCHEMA, COHORT_DB_DESC),
                                     (PERSON_DB_KEY, "사람별 정합성", PERSON_SCHEMA, PERSON_DB_DESC)):
        db_id = get_sync_state(conn, key)
        meta = get_database(token, db_id, session) if db_id else None
        if meta is not None:
            ensure_database_layout(token, db_id, desc, meta, session)
            ids.append(db_id)
            continue
        db_id = create_database(token, config.NOTION_KPI_PARENT_PAGE_ID, title, schema, session)
        ensure_database_layout(token, db_id, desc, {}, session)
        set_sync_state(conn, db_id, key)
        cur = conn.cursor()
        cur.execute(adapt_query("DELETE FROM TB_NOTION_PUBLISH WHERE DB_KEY = ?"), [key.split("_")[2]])
        conn.commit()
        logger.info(f"[KPI 발행] 노션 DB 생성: {title} ({db_id})")
        ids.append(db_id)
    return tuple(ids)



# ── 페이지 안내문 (읽는 법) ─────────────────────────────────────────
# 두 DB 위에 "무엇을 먼저 볼지 · 각 열이 무슨 뜻인지"를 적는다. 내용이 바뀌면(해시) 예전 블록을 보관 처리하고 다시 쓴다.
# 이 파이프라인이 만든 블록만 건드린다 — 페이지에 사람이 직접 쓴 블록은 그대로 둔다.

GUIDE_DB_KEY = "guide"

COHORT_DB_DESC = "기수 하나가 한 줄. HRD-Net 승인 인원(API)과 노션 HRD등록 수가 같은지, 개강일에 실제로 몇 명 왔는지. 하루 2회 갱신"
PERSON_DB_DESC = "합격 이상 신청자 한 명이 한 줄. 노션 최종결과와 HRD 명부를 이름·기수로 맞춘 결과. '메모'는 담당자 열(자동 갱신 안 함)"

COHORT_GUIDE = {
    "title": "1️⃣ 기수별 정합성 — 읽는 법",
    "first": [
        "정합성이 '불일치'인 기수부터 본다 → 승인(API)과 HRD등록(노션)이 다른 기수. 누가 다른지는 아래 사람별 표에서 그 기수로 필터",
        "놓침 > 0 이면 HRD-Net에는 수강신청했는데 노션에 HRD신청·HRD등록으로 안 적힌 사람이 있다 → 담당자에게 노션 갱신 요청. "
        "음수면 반대로 노션에 더 많다 (HRD 신청 취소가 노션에 반영 안 됐을 가능성)",
        "개강일 참석률(%)은 승인 인원 중 개강 당일 입실한 비율. 출결이 아직 수집되지 않은 회차는 비어 있다",
        "상태가 '개설예정'인데 승인(API)이 있는 것은 정상 — 기관 승인은 개강 전에 이뤄진다",
    ],
    "columns": [
        ("기수 / 과정 / 회차", "AIO3 = 과정 약칭 + 노션 기수 번호. 회차는 HRD-Net 번호(현재 둘이 같음)", "행 식별"),
        ("상태", "개설예정 · 진행중 · 종료 (개강일·종강일과 오늘 비교)", "진행중 기수만 보고 싶을 때 필터"),
        ("개강일 / 종강일", "HRD-Net 훈련 기간", "—"),
        ("정원", "HRD-Net 승인 정원 (totFxnum)", "충원율 = 승인 ÷ 정원"),
        ("수강신청(API)", "HRD-Net에 수강신청한 인원 (totTrpCnt). 신청만 하고 미승인된 사람 포함", "놓침 계산의 기준"),
        ("승인(API)", "기관이 승인한 인원 (totParMks) = 확정 신고 인원 = 명부 건수", "노션 HRD등록과 같아야 한다"),
        ("노션 신청자", "그 기수로 배정된 노션 신청자 전체 (취소 포함)", "모집 규모 참고"),
        ("합격 이상(노션)", "인터뷰합격 · 추가선발대기 · 합격안내 · 합격자등록 · HRD신청 · HRD등록", "사람별 표의 대상 인원"),
        ("HRD신청(노션)", "노션 최종결과가 'HRD신청' — 신청은 했고 아직 승인 전", "개강 임박 시 독촉 대상"),
        ("HRD등록(노션)", "노션 최종결과가 'HRD등록' — 기관 승인까지 끝남", "승인(API)과 비교"),
        ("놓침", "수강신청(API) − HRD등록(노션) − HRD신청(노션)", "> 0 이면 노션 누락 의심"),
        ("정합성", "일치 = 승인(API) = HRD등록(노션) · 불일치 · 미확인(한쪽 값 없음)", "매일 '불일치'만 확인"),
        ("명부 인원 / 훈련중", "명부에 한 번이라도 잡힌 사람 수 / 지금 훈련중 상태인 사람 수", "명부 인원 − 훈련중 = 이탈·수료"),
        ("중도탈락 / 조기취업 / 수료(API)", "명부 상태 집계. 수료(API)는 종료 회차만 값이 있고 조기취업은 뺀 수", "종료 기수 성과"),
        ("개강일 참석 / 개강일 참석률(%)", "개강 당일 입실 기록이 있는 사람 수 / 승인 인원 대비 비율", "개강 다음 날 확인"),
        ("변경 시각", "이 줄이 마지막으로 바뀐 시각(UTC). 값이 같으면 갱신하지 않는다", "오래됐으면 파이프라인 점검"),
    ],
}

PERSON_GUIDE = {
    "title": "2️⃣ 사람별 정합성 — 읽는 법",
    "first": [
        "정합성 열만 보면 된다. '일치'·'취소'는 정상, 나머지 넷이 확인 대상",
        "노션만 등록 → 노션은 HRD등록인데 HRD 명부에 없다: 승인이 아직 안 됐거나 이름 표기가 다르다 (띄어쓰기·개명). 담당자 확인",
        "HRD만 승인 → 명부에는 있는데 노션이 아직 HRD신청·합격 단계다: 노션 최종결과를 HRD등록으로 올려 달라고 요청",
        "대기 → 합격~HRD신청 단계에서 승인 전. 개강이 가까우면 HRD 신청·승인 독촉 대상",
        "동명이인 확인 → 같은 기수에 같은 이름이 둘 이상. 사람이 직접 확인",
        "이름은 가운데를 가린 표기다. 원래 이름·연락처는 '신청자' 열을 눌러 신청자 리스트에서 본다",
    ],
    "columns": [
        ("이름", "가린 이름 · 기수. 이 표에는 실명·연락처를 저장하지 않는다", "행 식별"),
        ("신청자", "노션 신청자 리스트의 원본 페이지 링크", "클릭해서 상세 확인"),
        ("기수", "노션 최종기수 (AIO3 등)", "기수별 표와 같은 값으로 필터"),
        ("노션 상태", "노션 최종결과 값 그대로", "HRD등록이어야 명부와 일치"),
        ("HRD 신청 일시", "노션 'HRD 신청 일시' 속성 (담당자 입력)", "비어 있으면 입력 요청"),
        ("HRD 승인", "HRD 명부에 이 사람이 있으면 체크", "체크 = 기관 승인 완료"),
        ("승인 감지", "파이프라인이 명부에서 처음 본 시각. 실제 승인 시각과 최대 반나절 차이", "등록 지연 계산 기준"),
        ("명부 상태", "훈련중 · 중도탈락 · 정상수료 · 80%이상수료 · 조기취업 · 제적", "이탈자 확인"),
        ("첫 참석일 / 첫 입실", "HRD 출결에서 처음 입실한 날과 시각", "개강일과 다르면 지각 개강"),
        ("등록 지연(일)", "승인 감지일 − 개강일. 개강 전에 승인됐거나 추적 시작 전이면 비어 있음", "> 0 이면 개강 후 뒤늦게 승인"),
        ("정합성", "일치 · 노션만 등록 · HRD만 승인 · 동명이인 확인 · 대기 · 취소", "위 '읽는 법' 참고"),
        ("변경 시각", "이 줄이 마지막으로 바뀐 시각(UTC)", "—"),
        ("메모", "담당자 자유 입력. 파이프라인이 절대 덮어쓰지 않는다", "확인 결과·조치 기록"),
    ],
}


def _rt(text):
    return [{"type": "text", "text": {"content": text}}]


def guide_blocks(guide):
    """안내 한 묶음 → 노션 블록 목록: 제목 · 먼저 볼 것(콜아웃 + 글머리) · 열 설명 표."""
    callout = {"type": "callout", "callout": {"rich_text": _rt("먼저 볼 것"), "icon": {"type": "emoji", "emoji": "👀"},
                                              "children": [{"type": "bulleted_list_item",
                                                            "bulleted_list_item": {"rich_text": _rt(t)}} for t in guide["first"]]}}
    rows = [("열", "뜻", "언제 보나")] + list(guide["columns"])
    table = {"type": "table", "table": {"table_width": 3, "has_column_header": True, "has_row_header": False,
                                        "children": [{"type": "table_row", "table_row": {"cells": [_rt(c) for c in r]}} for r in rows]}}
    return [{"type": "heading_2", "heading_2": {"rich_text": _rt(guide["title"])}}, callout, table]


def guide_hash():
    return hashlib.sha256(json.dumps([COHORT_GUIDE, PERSON_GUIDE], ensure_ascii=False, sort_keys=True).encode()).hexdigest()[:16]


def _page_children(token, page_id, session=None):
    blocks, cursor = [], None
    while True:
        path = f"/blocks/{page_id}/children?page_size=100" + (f"&start_cursor={cursor}" if cursor else "")
        res = _request(token, "GET", path, None, session)
        blocks += res.get("results", [])
        if not res.get("has_more"):
            return blocks
        cursor = res.get("next_cursor")


GUIDE_BLOCK_TYPES = ("heading_2", "callout", "table")   # 안내문이 만드는 블록 종류 — 이것 외에는 절대 보관 처리하지 않는다


def _append_blocks(token, page_id, blocks, after=None, session=None):
    """블록을 붙이고 **우리가 만든 블록의 ID만** 돌려준다.

    `after`를 쓰면 노션이 새 블록 뒤에 오는 기존 형제 블록(child_database 포함)까지 함께 돌려준다 (2026-09-16 실측).
    그대로 저장하면 다음 갱신 때 DB 블록을 보관 처리해 버리므로, 보낸 순서·종류대로 앞에서부터만 집는다.
    """
    body = {"children": blocks}
    if after:
        body["after"] = after
    results = _request(token, "PATCH", f"/blocks/{page_id}/children", body, session).get("results", [])
    ids, want = [], [b["type"] for b in blocks]
    for r in results:
        if len(ids) == len(want):
            break
        if r.get("type") == want[len(ids)]:
            ids.append(r["id"])
    return ids


def _archive_guide_block(token, block_id, session=None):
    """안내문 블록만 보관 처리. 종류가 다르면(child_database 등) 건드리지 않고 경고만 남긴다."""
    try:
        block = _request(token, "GET", f"/blocks/{block_id}", None, session)
    except NotionFetchError as e:
        if "404" in str(e):
            return
        raise
    if block.get("type") not in GUIDE_BLOCK_TYPES:
        logger.warning(f"[KPI 발행] 안내문 기록에 {block.get('type')} 블록이 있어 건너뜀: {block_id}")
        return
    if not block.get("archived"):
        _request(token, "PATCH", f"/blocks/{block_id}", {"archived": True}, session)


def ensure_page_guide(token, conn, cohort_db, person_db, session=None):
    """「모집 KPI」 페이지에 두 DB의 '읽는 법'을 쓴다. 기수별 안내는 기수별 DB 바로 위, 사람별 안내는 두 DB 사이.

    내용 해시가 그대로면 아무것도 하지 않는다. 바뀌었으면 예전에 만든 블록만 보관 처리하고 다시 쓴다.
    반환: True면 이번에 썼음.
    """
    h = guide_hash()
    cur = conn.cursor()
    cur.execute(adapt_query("SELECT ROW_KEY, NOTION_PAGE_ID, CONTENT_HASH FROM TB_NOTION_PUBLISH WHERE DB_KEY = ?"), [GUIDE_DB_KEY])
    old = cur.fetchall()
    if old and all(r[2] == h for r in old):
        return False
    for _, block_id, _ in old:
        _archive_guide_block(token, block_id, session)
    cur.execute(adapt_query("DELETE FROM TB_NOTION_PUBLISH WHERE DB_KEY = ?"), [GUIDE_DB_KEY])
    conn.commit()

    page = config.NOTION_KPI_PARENT_PAGE_ID
    children = [b for b in _page_children(token, page, session) if not b.get("archived")]
    ids = [b["id"] for b in children]
    norm = lambda x: str(x).replace("-", "")   # noqa: E731 — 블록 ID는 하이픈 유무가 섞여 온다
    before_cohort = None
    for prev, cur_id in zip([None] + ids, ids):
        if norm(cur_id) == norm(cohort_db):
            before_cohort = prev
            break
    made = []
    # 두 번째(사람별) 안내를 먼저 넣어야 첫 번째 안내가 기수별 DB 앞에 들어가도 위치가 흔들리지 않는다
    made += [("person", i) for i in _append_blocks(token, page, guide_blocks(PERSON_GUIDE), after=cohort_db, session=session)]
    made += [("cohort", i) for i in _append_blocks(token, page, guide_blocks(COHORT_GUIDE), after=before_cohort, session=session)]
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    for n, (kind, block_id) in enumerate(made):
        cur.execute(adapt_query("INSERT INTO TB_NOTION_PUBLISH (DB_KEY, ROW_KEY, NOTION_PAGE_ID, CONTENT_HASH, UPDATED_AT) VALUES (?, ?, ?, ?, ?)"),
                    [GUIDE_DB_KEY, f"{kind}-{n}", block_id, h, now])
    conn.commit()
    logger.info(f"[KPI 발행] 페이지 안내문 갱신: 블록 {len(made)}개")
    return True


# ── 값 → 노션 속성 ────────────────────────────────────────────────────


def _text(v):
    return {"rich_text": [{"type": "text", "text": {"content": str(v)[:2000]}}] if v not in (None, "") else []}


def _date(v):
    if v in (None, ""):
        return {"date": None}
    s = str(v)
    if len(s) == 8 and s.isdigit():
        s = f"{s[:4]}-{s[4:6]}-{s[6:]}"
    return {"date": {"start": s[:19] if "T" in s or " " in s else s[:10]}}


def to_properties(schema, row):
    """스키마 타입에 맞춰 딕셔너리 값을 노션 속성 페이로드로. 없는 키·'메모'는 건드리지 않는다."""
    props = {}
    for name, spec in schema.items():
        if name == "메모" or name not in row:
            continue
        v = row[name]
        t = next(iter(spec))
        if t == "title":
            props[name] = {"title": [{"type": "text", "text": {"content": str(v or "")[:200]}}]}
        elif t == "rich_text":
            props[name] = _text(v)
        elif t == "number":
            props[name] = {"number": None if v is None else float(v)}
        elif t == "select":
            props[name] = {"select": {"name": str(v)} if v not in (None, "") else None}
        elif t == "date":
            props[name] = _date(v)
        elif t == "checkbox":
            props[name] = {"checkbox": bool(v)}
        elif t == "relation":
            props[name] = {"relation": [{"id": v}] if v else []}
    return props


def content_hash(row):
    return hashlib.sha256(json.dumps({k: v for k, v in row.items() if k not in _HASH_EXCLUDE},
                                     ensure_ascii=False, sort_keys=True, default=str).encode()).hexdigest()


# ── 행 만들기 (Supabase → 딕셔너리) ──────────────────────────────────


def _cohort_of(trpr_id, degr):
    short = config.COURSE_SHORT_NAMES.get(trpr_id)
    return f"{short}{int(degr)}" if short else None


def _status(sta, end, today):
    if sta and sta > today:
        return "개설예정"
    if end and end < today:
        return "종료"
    return "진행중"


def build_cohort_rows(today=None):
    """기수별 정합성 행. AI캠퍼스 과정(config.NOTION_KPI_COURSES) 회차만."""
    today = today or datetime.now(timezone(timedelta(hours=9))).strftime("%Y-%m-%d")
    snap = load_data("""
        SELECT s.* FROM TB_COURSE_SNAPSHOT s
        JOIN (SELECT TRPR_ID, TRPR_DEGR, MAX(SNAP_AT) AS MAX_AT FROM TB_COURSE_SNAPSHOT GROUP BY TRPR_ID, TRPR_DEGR) m
          ON m.TRPR_ID = s.TRPR_ID AND m.TRPR_DEGR = s.TRPR_DEGR AND m.MAX_AT = s.SNAP_AT""")
    roster = load_data("""
        SELECT TRPR_ID, TRPR_DEGR, COUNT(*) AS ROSTER_CNT,
               SUM(CASE WHEN GONE_AT IS NULL AND STATUS LIKE '%훈련중%' THEN 1 ELSE 0 END) AS ACTIVE_CNT,
               SUM(CASE WHEN FIRST_ATTEND_DT IS NOT NULL AND FIRST_ATTEND_DT = REPLACE(TR_STA_DT, '-', '') THEN 1 ELSE 0 END) AS DAY1_CNT
        FROM TB_ROSTER_MEMBER GROUP BY TRPR_ID, TRPR_DEGR""")
    apps = load_data("""
        SELECT COHORT, COUNT(*) AS N,
               SUM(CASE WHEN STATUS IN ('인터뷰합격','추가선발대기','합격안내','합격자등록','HRD신청','HRD등록') THEN 1 ELSE 0 END) AS PASS_CNT,
               SUM(CASE WHEN STATUS = 'HRD신청' THEN 1 ELSE 0 END) AS APPLY_CNT,
               SUM(CASE WHEN STATUS = 'HRD등록' THEN 1 ELSE 0 END) AS REG_CNT
        FROM TB_APPLICANT WHERE COHORT IS NOT NULL GROUP BY COHORT""")
    roster_map = {(r.TRPR_ID, int(r.TRPR_DEGR)): r for r in roster.itertuples(index=False)}
    apps_map = {r.COHORT: r for r in apps.itertuples(index=False)}

    rows = []
    for s in snap.itertuples(index=False):
        cohort = _cohort_of(s.TRPR_ID, s.TRPR_DEGR)
        if not cohort or config.COURSE_SHORT_NAMES[s.TRPR_ID] not in config.NOTION_KPI_COURSES:
            continue
        r = roster_map.get((s.TRPR_ID, int(s.TRPR_DEGR)))
        a = apps_map.get(cohort)
        approved = _i(s.TOT_PAR_MKS)
        reg = _i(a.REG_CNT) if a is not None else None
        apply_cnt = _i(a.APPLY_CNT) if a is not None else None
        trp = _i(s.TOT_TRP_CNT)
        day1 = _i(r.DAY1_CNT) if r is not None else None
        rows.append({
            "KEY": cohort, "기수": cohort, "과정": config.COURSE_SHORT_NAMES[s.TRPR_ID], "회차": int(s.TRPR_DEGR),
            "상태": _status(s.TR_STA_DT, s.TR_END_DT, today), "개강일": s.TR_STA_DT, "종강일": s.TR_END_DT,
            "정원": _i(s.TOT_FXNUM), "수강신청(API)": trp, "승인(API)": approved,
            "노션 신청자": _i(a.N) if a is not None else None, "합격 이상(노션)": _i(a.PASS_CNT) if a is not None else None,
            "HRD신청(노션)": apply_cnt, "HRD등록(노션)": reg,
            "놓침": (trp - (reg or 0) - (apply_cnt or 0)) if trp is not None and a is not None else None,
            "정합성": ("미확인" if approved is None or reg is None else ("일치" if approved == reg else "불일치")),
            "명부 인원": _i(r.ROSTER_CNT) if r is not None else None, "훈련중": _i(r.ACTIVE_CNT) if r is not None else None,
            "중도탈락": _i(s.DROPOUT_CNT), "조기취업": _i(s.EARLY_EMPL_CNT), "수료(API)": _i(s.FINI_CNT),
            "개강일 참석": day1,
            "개강일 참석률(%)": round(day1 / approved * 100, 1) if day1 is not None and approved else None,
            "변경 시각": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        })
    return rows


def build_person_rows(today=None):
    """사람별 정합성 행. 합격 단계 이상 신청자 ↔ 명부를 (이름 해시, 기수)로 대조."""
    today = today or datetime.now(timezone(timedelta(hours=9))).strftime("%Y-%m-%d")
    statuses = ",".join("?" * len(config.NOTION_KPI_PASS_STATUSES))
    apps = load_data(
        f"SELECT NOTION_PAGE_ID, NAME_HASH, NAME_MASKED, COHORT, STATUS, HRD_APPLY_AT FROM TB_APPLICANT "
        f"WHERE COHORT IS NOT NULL AND STATUS IN ({statuses})", params=list(config.NOTION_KPI_PASS_STATUSES))
    members = load_data("SELECT TRPR_ID, TRPR_DEGR, NAME_HASH, STATUS, FIRST_SEEN_AT, GONE_AT, FIRST_ATTEND_DT, FIRST_IN_TIME, TR_STA_DT FROM TB_ROSTER_MEMBER")
    tracking_start = None
    if not members.empty:
        tracking_start = str(members["FIRST_SEEN_AT"].min())[:10]   # 첫 스냅샷 날 = 그 전 승인은 시점을 모른다
    by_key = {}
    for m in members.itertuples(index=False):
        by_key.setdefault((_cohort_of(m.TRPR_ID, m.TRPR_DEGR), m.NAME_HASH), []).append(m)

    rows = []
    for a in apps.itertuples(index=False):
        matches = by_key.get((a.COHORT, a.NAME_HASH), [])
        live = [m for m in matches if m.GONE_AT is None or str(m.GONE_AT) in ("", "None", "nan", "NaT")]
        row = {"KEY": a.NOTION_PAGE_ID, "이름": f"{a.NAME_MASKED or '?'} · {a.COHORT}", "신청자": a.NOTION_PAGE_ID,
               "기수": a.COHORT, "노션 상태": a.STATUS, "HRD 신청 일시": a.HRD_APPLY_AT,
               "HRD 승인": False, "승인 감지": None, "명부 상태": None, "첫 참석일": None, "첫 입실": None,
               "등록 지연(일)": None, "정합성": "대기",
               "변경 시각": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")}
        if len(matches) > 1:
            row["정합성"] = "동명이인 확인"
        elif live:
            m = live[0]
            seen = str(m.FIRST_SEEN_AT)[:19].replace(" ", "T") if m.FIRST_SEEN_AT is not None else None
            row.update({"HRD 승인": True, "승인 감지": seen, "명부 상태": m.STATUS,
                        "첫 참석일": m.FIRST_ATTEND_DT, "첫 입실": m.FIRST_IN_TIME})
            if seen and m.TR_STA_DT and tracking_start and seen[:10] > tracking_start:
                delay = (datetime.fromisoformat(seen[:10]) - datetime.fromisoformat(str(m.TR_STA_DT)[:10])).days
                row["등록 지연(일)"] = max(delay, 0)
            row["정합성"] = "일치" if a.STATUS == "HRD등록" else "HRD만 승인"
        elif a.STATUS == "HRD등록":
            row["정합성"] = "노션만 등록"
        elif str(a.STATUS).startswith("합격취소"):
            row["정합성"] = "취소"          # 합격 후 취소, 명부에도 없음 — 정상 종료
        else:
            row["정합성"] = "대기"          # 합격~HRD신청 단계, 승인 전
        rows.append(row)
    return rows


def _i(v):
    try:
        if v is None or (isinstance(v, float) and v != v):
            return None
        return int(float(v))
    except (TypeError, ValueError):
        return None


# ── 발행 ─────────────────────────────────────────────────────────────


def publish(token, conn, db_key, db_id, schema, rows, session=None, now=None):
    """행 목록을 노션 DB에 upsert. 내용 해시가 같으면 건너뜀. 반환: (생성, 갱신, 건너뜀)."""
    now = now or datetime.now(timezone.utc).replace(tzinfo=None)
    cur = conn.cursor()
    cur.execute(adapt_query("SELECT ROW_KEY, NOTION_PAGE_ID, CONTENT_HASH FROM TB_NOTION_PUBLISH WHERE DB_KEY = ?"), [db_key])
    known = {r[0]: (r[1], r[2]) for r in cur.fetchall()}
    upsert = adapt_query(
        "INSERT INTO TB_NOTION_PUBLISH (DB_KEY, ROW_KEY, NOTION_PAGE_ID, CONTENT_HASH, UPDATED_AT) VALUES (?, ?, ?, ?, ?) "
        "ON CONFLICT(DB_KEY, ROW_KEY) DO UPDATE SET NOTION_PAGE_ID = excluded.NOTION_PAGE_ID, "
        "CONTENT_HASH = excluded.CONTENT_HASH, UPDATED_AT = excluded.UPDATED_AT")
    created = updated = skipped = 0
    for row in rows:
        h = content_hash(row)
        page_id, prev_hash = known.get(row["KEY"], (None, None))
        if page_id and prev_hash == h:
            skipped += 1
            continue
        props = to_properties(schema, row)
        if page_id:
            update_page(token, page_id, props, session)
            updated += 1
        else:
            page_id = create_page(token, db_id, props, session)
            created += 1
        cur.execute(upsert, [db_key, row["KEY"], page_id, h, now])
        conn.commit()
    return created, updated, skipped


def main():
    token = _clean_secret(os.getenv("NOTION_TOKEN"))
    if not token:
        logger.warning("NOTION_TOKEN 없음 — 노션 KPI 발행을 건너뜁니다")
        return
    t0 = time.monotonic()
    init_all_tables(include_market=False)
    conn = get_connection(timeout=30)
    try:
        cohort_db, person_db = ensure_databases(token, conn)
        ensure_page_guide(token, conn, cohort_db, person_db)
        c = publish(token, conn, "cohort", cohort_db, COHORT_SCHEMA, build_cohort_rows())
        p = publish(token, conn, "person", person_db, PERSON_SCHEMA, build_person_rows())
    except NotionFetchError as e:
        logger.error(f"[KPI 발행] {e}")
        return
    finally:
        conn.close()
    logger.info(f"[KPI 발행] 기수별 생성 {c[0]} · 갱신 {c[1]} · 건너뜀 {c[2]} | "
                f"사람별 생성 {p[0]} · 갱신 {p[1]} · 건너뜀 {p[2]} ({time.monotonic() - t0:.1f}s)")


if __name__ == "__main__":
    main()
