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


def database_exists(token, db_id, session=None):
    try:
        _request(token, "GET", f"/databases/{db_id}", None, session)
        return True
    except NotionFetchError as e:
        if "404" in str(e):
            return False
        raise


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
    "정합성": {"select": {"options": [{"name": n} for n in ("일치", "노션만 등록", "HRD만 승인", "동명이인 확인", "대기")]}},
    "변경 시각": {"date": {}},
    "메모": {"rich_text": {}},   # 담당자 입력 열 — 파이프라인은 절대 쓰지 않는다
}


def ensure_databases(token, conn, session=None):
    """두 DB가 없으면 만들고 ID를 TB_SYNC_STATE에 저장. 반환: (cohort_db_id, person_db_id)."""
    ids = []
    for key, title, schema in ((COHORT_DB_KEY, "기수별 정합성", COHORT_SCHEMA),
                               (PERSON_DB_KEY, "사람별 정합성", PERSON_SCHEMA)):
        db_id = get_sync_state(conn, key)
        if db_id and database_exists(token, db_id, session):
            ids.append(db_id)
            continue
        db_id = create_database(token, config.NOTION_KPI_PARENT_PAGE_ID, title, schema, session)
        set_sync_state(conn, db_id, key)
        cur = conn.cursor()
        cur.execute(adapt_query("DELETE FROM TB_NOTION_PUBLISH WHERE DB_KEY = ?"), [key.split("_")[2]])
        conn.commit()
        logger.info(f"[KPI 발행] 노션 DB 생성: {title} ({db_id})")
        ids.append(db_id)
    return tuple(ids)


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
        else:
            row["정합성"] = "노션만 등록" if a.STATUS == "HRD등록" else "대기"
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
