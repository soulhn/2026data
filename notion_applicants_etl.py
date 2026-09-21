"""노션 신청자 리스트(AI캠퍼스·SKN, `config.NOTION_APPLICANT_SOURCES`) 증분 폴링 ETL — 모집 KPI 1단계.

담당자가 노션에서 관리하는 개인 단위 퍼널(신청 → 연락 → 인터뷰 → 합격 → HRD신청 → HRD등록 → 개강 참석)을
매시간 읽어 TB_APPLICANT(현재 상태)에 미러하고, 추적 필드가 바뀐 사람은 TB_APPLICANT_STATUS_LOG에
전이(이전값 → 새값, 감지 시각)를 남긴다. 노션 API에는 변경 이력이 없어 이 차분 로그가 유일한 이력이다.

원칙
- 노션은 **읽기만** 한다 (query 엔드포인트만).
- 이름은 sha256 해시와 마스킹만 저장하고, 연락처·PM 등 개인 식별 속성은 가져오지 않는다.
  HRD 명부와의 대조는 명부 이름을 즉석에서 해시해 (NAME_HASH, COHORT)로 맞춘다.
- 증분: 마지막 동기화 시각에서 NOTION_SYNC_OVERLAP_MIN 만큼 되감은 시점 이후 last_edited_time 행만 조회.
  첫 실행은 전량. 같은 행을 다시 읽어도 값이 같으면 로그가 생기지 않는다.

실행: python notion_applicants_etl.py   (hrd_etl.yml 매시간, NOTION_TOKEN 없으면 건너뜀)
"""
import hashlib
import logging
import os
import re
import time
from datetime import datetime, timedelta, timezone

from dotenv import load_dotenv

import config
from init_db import init_all_tables
from notion_ops import NotionFetchError, prop_value, query_data_source
from notify import discord_post
from utils import _clean_secret, adapt_query, get_connection, mask_name

load_dotenv()

logger = logging.getLogger(__name__)
logging.basicConfig(format="%(asctime)s %(levelname)s %(name)s %(message)s", level=logging.INFO)

SYNC_KEY = "notion_applicants_last_sync"

# 노션 속성 → 컬럼. 속성이 없으면(신규 속성 미추가 등) None.
_PROP_MAP = {
    "최종기수": "COHORT", "기수": "COHORT_TEXT", "최종결과": "STATUS", "처리결과": "PROCESS_RESULT",
    "신청일시": "APPLIED_AT", "유입경로": "SOURCE",
    "인터뷰일시": "INTERVIEW_AT", "인터뷰평가": "INTERVIEW_GRADE",
    "합격 안내": "PASS_NOTICE_AT", "합격자 등록": "PASS_REG_AT", "합격자등록": "PASS_REG",
    "HRD 등록 일시": "HRD_REG_AT", "HRD 신청(현장)": "HRD_ONSITE", "OT참석": "OT_ATTEND",
    "취소 상세 사유": "CANCEL_REASON", "이관 대상 과정/기수": "TRANSFER_TO",
}
APPLICANT_COLUMNS = [
    "NOTION_PAGE_ID", "NOTION_URL", "NAME_HASH", "NAME_MASKED", "SOURCE_KEY",
    "COHORT", "COHORT_TEXT", "STATUS", "PROCESS_RESULT", "APPLIED_AT", "SOURCE",
    "INTERVIEW_AT", "INTERVIEW_GRADE", "PASS_NOTICE_AT", "PASS_REG_AT", "PASS_REG",
    "HRD_APPLY_AT", "HRD_REG_AT", "HRD_ONSITE", "OT_ATTEND", "ATTEND_DT",
    "CANCEL_REASON", "TRANSFER_TO", "NOTION_CREATED_AT", "NOTION_EDITED_AT",
]
# 바뀌면 전이 로그를 남기는 필드
TRACKED_FIELDS = ["STATUS", "COHORT", "PROCESS_RESULT", "HRD_APPLY_AT", "HRD_REG_AT", "ATTEND_DT", "OT_ATTEND"]


def name_hash(name):
    """이름 → sha256 hex. 공백 제거. HRD 명부 이름도 같은 함수로 해시해 대조한다."""
    if not name:
        return None
    return hashlib.sha256(str(name).replace(" ", "").strip().encode("utf-8")).hexdigest()


def _norm(v):
    if v is None:
        return None
    if isinstance(v, bool):
        return 1 if v else 0
    s = str(v).strip()
    return s or None


_COHORT_NUM = re.compile(r"^\s*(\d+)\s*기?\s*$")


def normalize_cohort(value, prefix=""):
    """노션 '최종기수' → 우리 기수 키. "35기" + prefix "SKN" → "SKN35". "AIO3"처럼 이미 완성형이면 그대로."""
    if value is None or str(value).strip() == "":
        return None
    v = str(value).strip()
    m = _COHORT_NUM.match(v)
    return f"{prefix}{int(m.group(1))}" if m and prefix else v


def parse_applicant_page(page, source=None):
    """노션 페이지 객체 → APPLICANT_COLUMNS 딕셔너리 (이름은 해시·마스킹만).

    source: config.NOTION_APPLICANT_SOURCES 항목(+ "key"). None이면 AI캠퍼스.
    """
    source = source or dict(config.NOTION_APPLICANT_SOURCES["AI"], key="AI")
    props = page.get("properties") or {}
    name = prop_value(props["이름"]) if "이름" in props else None
    row = {
        "NOTION_PAGE_ID": page.get("id"),
        "NOTION_URL": page.get("url"),
        "NAME_HASH": name_hash(name),
        "NAME_MASKED": mask_name(name) if name else None,
        "SOURCE_KEY": source["key"],
        "NOTION_CREATED_AT": page.get("created_time"),
        "NOTION_EDITED_AT": page.get("last_edited_time"),
    }
    prop_map = dict(_PROP_MAP, **source.get("prop_map", {}))
    for prop_name, col in prop_map.items():
        row[col] = _norm(prop_value(props[prop_name])) if prop_name in props else None
    row["COHORT"] = normalize_cohort(row.get("COHORT"), source.get("cohort_prefix", ""))
    # 담당자에게 추가 요청한 신규 속성 — 생기면 자동으로 읽힌다
    row["HRD_APPLY_AT"] = _norm(prop_value(props[config.NOTION_HRD_APPLY_PROP])) if config.NOTION_HRD_APPLY_PROP in props else None
    row["ATTEND_DT"] = _norm(prop_value(props[config.NOTION_ATTEND_PROP])) if config.NOTION_ATTEND_PROP in props else None
    return {c: row.get(c) for c in APPLICANT_COLUMNS}


# ── 동기화 상태 ───────────────────────────────────────────────────────


def get_sync_state(conn, key=SYNC_KEY):
    cur = conn.cursor()
    cur.execute(adapt_query("SELECT SYNC_VALUE FROM TB_SYNC_STATE WHERE SYNC_KEY = ?"), [key])
    row = cur.fetchone()
    return row[0] if row else None


def set_sync_state(conn, value, key=SYNC_KEY):
    cur = conn.cursor()
    cur.execute(adapt_query(
        "INSERT INTO TB_SYNC_STATE (SYNC_KEY, SYNC_VALUE, UPDATED_AT) VALUES (?, ?, CURRENT_TIMESTAMP) "
        "ON CONFLICT(SYNC_KEY) DO UPDATE SET SYNC_VALUE = excluded.SYNC_VALUE, UPDATED_AT = excluded.UPDATED_AT"
    ), [key, value])
    conn.commit()


def since_from_state(last_sync_iso, overlap_min=None):
    """마지막 동기화 시각을 overlap 만큼 되감은 ISO 문자열. 없으면 None(전량)."""
    if not last_sync_iso:
        return None
    overlap = timedelta(minutes=config.NOTION_SYNC_OVERLAP_MIN if overlap_min is None else overlap_min)
    t = datetime.fromisoformat(str(last_sync_iso).replace("Z", "+00:00"))
    if t.tzinfo is None:
        t = t.replace(tzinfo=timezone.utc)
    return (t - overlap).strftime("%Y-%m-%dT%H:%M:%S.000Z")


def sources():
    """폴링할 신청자 리스트 목록 — config 항목에 "key"를 붙여 돌려준다."""
    return [dict(v, key=k) for k, v in config.NOTION_APPLICANT_SOURCES.items()]


def sync_key(source_key):
    """원본별 마지막 동기화 시각 키. AI캠퍼스는 예전 키를 그대로 써서 전량 재조회를 피한다."""
    return SYNC_KEY if source_key == "AI" else f"{SYNC_KEY}:{source_key}"


def fetch_changed_pages(token, since_iso=None, session=None, source=None):
    """since 이후 수정된 신청자 페이지(없으면 전량), 수정 시각 오름차순. 데이터 소스 API(2025-09-03)."""
    source = source or dict(config.NOTION_APPLICANT_SOURCES["AI"], key="AI")
    flt = {"timestamp": "last_edited_time", "last_edited_time": {"on_or_after": since_iso}} if since_iso else None
    return query_data_source(
        token, source["data_source_id"], filter=flt,
        sorts=[{"timestamp": "last_edited_time", "direction": "ascending"}],
        session=session, what=source.get("name", "신청자 리스트"),
    )


# ── 저장 ─────────────────────────────────────────────────────────────


HRD_STATUSES = ("HRD신청", "HRD등록")


def _notable(old, new):
    """알림 대상 전이: HRD신청·HRD등록으로 들어오거나 거기서 나가거나, 합격취소로 가는 것."""
    return (new in HRD_STATUSES) or (old in HRD_STATUSES) or str(new or "").startswith("합격취소")


def upsert_applicants(conn, rows, now=None, events=None):
    """현재 상태 upsert + 추적 필드 전이 로그. 반환: (신규, 갱신, 전이 로그 수).

    events: 리스트를 주면 알림용 최종결과 전이를 {"name","cohort","old","new"}로 덧붙인다 (기수 있는 사람만).
    """
    now = now or datetime.now(timezone.utc).replace(tzinfo=None)
    cur = conn.cursor()
    tracked_sql = ", ".join(TRACKED_FIELDS)
    select = adapt_query(f"SELECT {tracked_sql}, NOTION_EDITED_AT FROM TB_APPLICANT WHERE NOTION_PAGE_ID = ?")
    insert = adapt_query(
        f"INSERT INTO TB_APPLICANT ({', '.join(APPLICANT_COLUMNS)}, SYNCED_AT) "
        f"VALUES ({', '.join('?' * len(APPLICANT_COLUMNS))}, ?)"
    )
    update_cols = [c for c in APPLICANT_COLUMNS if c != "NOTION_PAGE_ID"]
    update = adapt_query(
        "UPDATE TB_APPLICANT SET " + ", ".join(f"{c} = ?" for c in update_cols) + ", SYNCED_AT = ? "
        "WHERE NOTION_PAGE_ID = ?"
    )
    log = adapt_query(
        "INSERT INTO TB_APPLICANT_STATUS_LOG (NOTION_PAGE_ID, DETECTED_AT, FIELD, OLD_VALUE, NEW_VALUE, NOTION_EDITED_AT) "
        "VALUES (?, ?, ?, ?, ?, ?)"
    )

    inserted = updated = transitions = 0
    for r in rows:
        pid = r["NOTION_PAGE_ID"]
        cur.execute(select, [pid])
        prev = cur.fetchone()
        if prev is None:
            cur.execute(insert, [r.get(c) for c in APPLICANT_COLUMNS] + [now])
            inserted += 1
            if r.get("STATUS") is not None:
                cur.execute(log, [pid, now, "STATUS", None, r["STATUS"], r.get("NOTION_EDITED_AT")])
                transitions += 1
                if events is not None and r.get("COHORT") and _notable(None, r["STATUS"]):
                    events.append({"name": r.get("NAME_MASKED"), "cohort": r["COHORT"], "old": None, "new": r["STATUS"]})
            continue
        prev_vals = dict(zip(TRACKED_FIELDS, prev[:len(TRACKED_FIELDS)]))
        changed = [f for f in TRACKED_FIELDS if _norm(prev_vals.get(f)) != _norm(r.get(f))]
        if prev[len(TRACKED_FIELDS)] == r.get("NOTION_EDITED_AT") and not changed:
            continue                                  # 겹침 구간에서 다시 읽은 같은 행
        for f in changed:
            cur.execute(log, [pid, now, f, _norm(prev_vals.get(f)), _norm(r.get(f)), r.get("NOTION_EDITED_AT")])
            transitions += 1
            if f == "STATUS" and events is not None and r.get("COHORT") and _notable(_norm(prev_vals.get(f)), _norm(r.get(f))):
                events.append({"name": r.get("NAME_MASKED"), "cohort": r["COHORT"],
                               "old": _norm(prev_vals.get(f)), "new": _norm(r.get(f))})
        cur.execute(update, [r.get(c) for c in update_cols] + [now, pid])
        updated += 1
    conn.commit()
    return inserted, updated, transitions


def run(token, conn, now=None, session=None, events=None, source_keys=None):
    """원본별로 한 번씩 폴링. 반환: (조회 페이지 수, 신규, 갱신, 전이) 합계. events: 알림용 전이 수집 리스트(선택)."""
    now = now or datetime.now(timezone.utc).replace(tzinfo=None)
    total = [0, 0, 0, 0]
    for src in sources():
        if source_keys and src["key"] not in source_keys:
            continue
        since = since_from_state(get_sync_state(conn, sync_key(src["key"])))
        pages = fetch_changed_pages(token, since, session=session, source=src)
        rows = [parse_applicant_page(p, src) for p in pages]
        inserted, updated, transitions = upsert_applicants(conn, rows, now, events=events)
        set_sync_state(conn, now.strftime("%Y-%m-%dT%H:%M:%SZ"), sync_key(src["key"]))
        logger.info(f"[신청자 폴링] {src['key']}: 조회 {len(pages)} · 신규 {inserted} · 갱신 {updated} · 전이 {transitions}")
        for i, v in enumerate((len(pages), inserted, updated, transitions)):
            total[i] += v
    return tuple(total)


def main():
    token = _clean_secret(os.getenv("NOTION_TOKEN"))
    if not token:
        logger.warning("NOTION_TOKEN 없음 — 신청자 리스트 폴링을 건너뜁니다 (읽기 통합 발급 후 시크릿 등록)")
        return
    t0 = time.monotonic()
    init_all_tables(include_market=False)
    conn = get_connection(timeout=30)
    events = []
    try:
        n, ins, upd, tr = run(token, conn, events=events)
        set_sync_state(conn, "", "notion_applicants_last_error")
    except NotionFetchError as e:
        logger.error(f"[신청자 폴링] {e}")
        set_sync_state(conn, str(e)[:300], "notion_applicants_last_error")   # 점검이 읽는다
        return
    finally:
        conn.close()
    logger.info(f"[신청자 폴링] 조회 {n} · 신규 {ins} · 갱신 {upd} · 전이 {tr} ({time.monotonic() - t0:.1f}s)")
    sent = discord_post([("📋 노션 최종결과 변경", [f"{e['name'] or '?'} · {e['cohort']}: {e['old'] or '(신규)'} → {e['new']}" for e in events])])
    if sent:
        logger.info(f"[신청자 폴링] 디스코드 알림 {sent}건 (전이 {len(events)}건)")


if __name__ == "__main__":
    main()
