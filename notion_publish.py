"""노션 쓰기 헬퍼 — 우리 소유 페이지에만 쓴다 (담당자 DB는 절대 쓰지 않는다).

`notion_registry_publish.py`가 쓰는 공용 조각: 요청(429 재시도) · DB 생성/속성 추가·삭제/인라인·설명 · 값 → 노션 속성 변환 ·
내용 해시 기반 upsert(`publish`). 2026-09-15 「모집 KPI」 발행기(notion_kpi_publish.py)에서 뽑아냈고, 그 페이지는 2026-09-21 삭제됐다.
"""
import hashlib
import json
import logging
import time
from datetime import datetime, timezone

import requests

import config
from notion_ops import NotionFetchError
from utils import adapt_query

logger = logging.getLogger(__name__)

_HASH_EXCLUDE = {"변경 시각", "갱신 시각"}   # 매 실행 바뀌는 시각은 해시에서 뺀다 (안 그러면 전 행이 매번 갱신)


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


def _num():
    return {"number": {"format": "number"}}


def ensure_properties(token, db_id, schema, meta, session=None):
    """스키마에 새로 생긴 속성을 기존 DB에 추가한다 (있는 속성은 건드리지 않는다 — 담당자가 바꾼 옵션·이름 보존).

    select 옵션은 페이지를 쓸 때 노션이 자동 생성하므로 속성 자체가 없을 때만 PATCH.
    """
    have = meta.get("properties") or {}
    missing = {k: v for k, v in schema.items() if k not in have}
    if not missing:
        return []
    _request(token, "PATCH", f"/databases/{db_id}", {"properties": missing}, session)
    logger.info(f"[KPI 발행] 노션 DB 속성 추가 {db_id}: {', '.join(missing)}")
    return list(missing)


def remove_properties(token, db_id, names, meta, session=None):
    """기존 DB에서 더 이상 쓰지 않는 속성을 지운다 (노션은 값을 null로 보내면 삭제). 없으면 요청 없음."""
    have = meta.get("properties") or {}
    gone = [n for n in names if n in have]
    if not gone:
        return []
    _request(token, "PATCH", f"/databases/{db_id}", {"properties": {n: None for n in gone}}, session)
    logger.info(f"[KPI 발행] 노션 DB 속성 삭제 {db_id}: {', '.join(gone)}")
    return gone


def _rt(text):
    return [{"type": "text", "text": {"content": text}}]


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
        elif t == "url":
            props[name] = {"url": v or None}
    return props


def content_hash(row):
    return hashlib.sha256(json.dumps({k: v for k, v in row.items() if k not in _HASH_EXCLUDE},
                                     ensure_ascii=False, sort_keys=True, default=str).encode()).hexdigest()


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
