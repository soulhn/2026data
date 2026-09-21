"""파이프라인 자기 점검 — 이상할 때만 디스코드로 알린다 (hrd_etl.yml 마지막 단계, 하루 2회).

멈춘 것은 GitHub 실패 메일이 잡지만, "조용히 틀리는 것"은 아무도 모른다: 노션 토큰 만료로 건너뜀, HRD API 한 기관만 실패,
기수명 오타로 매칭 누락, 담당자 오기입. 그래서 실행마다 아래를 검사하고 걸리는 것만 보낸다. 정상이면 조용하다.

  1 신선도   회차 스냅샷·명부·신청자 폴링(AI/SKN)·페이지 발행이 36시간 안에 돌았나, 직전 실행의 원천 오류가 남았나
  2 값       노션 수집 > API 신청인원, 확정 신고 > API 신청인원, 개강 지났는데 출결 기록 없음
  3 매칭     스냅샷에 없는 기수명(오타), 같은 기수 동명이인, 최근 24시간에 새로 생긴 '노션에 없음'
  4 담당자   운영현황표 확정자신고 vs API 확정 신고 (확정 신고 끝난 기수)

같은 내용이면 다시 보내지 않고(해시), 그래도 7일마다 한 번은 다시 알린다. 읽기만 한다 — 노션엔 쓰지 않는다.
실행: python health_check.py
"""
import hashlib
import logging
import os
import time
from datetime import datetime, timedelta, timezone

from dotenv import load_dotenv

import config
from notify import discord_post
from notion_applicants_etl import get_sync_state, set_sync_state, sync_key
from notion_ops import NotionFetchError, cohort_number, course_group, fetch_ops_table
from notion_registry_publish import build_cohort_rows, cohort_key
from utils import _clean_secret, get_connection, load_data

load_dotenv()
logger = logging.getLogger(__name__)
logging.basicConfig(format="%(asctime)s %(levelname)s %(name)s %(message)s", level=logging.INFO)

STALE_HOURS = 36            # 하루 2회 실행이라 36시간이면 한 번은 걸러진 것
NEW_WINDOW_HOURS = 24       # '새로 생긴 노션에 없음'을 세는 창
RESEND_DAYS = 7             # 같은 내용도 이 주기로는 다시 알린다
KST = timezone(timedelta(hours=9))


def _parse(ts):
    """DB·동기화 상태의 시각(문자열·datetime·None) → UTC aware datetime 또는 None."""
    if ts is None or str(ts) in ("", "None", "NaT", "nan"):
        return None
    if isinstance(ts, datetime):
        return ts if ts.tzinfo else ts.replace(tzinfo=timezone.utc)
    s = str(ts).replace("Z", "+00:00")
    try:
        t = datetime.fromisoformat(s)
    except ValueError:
        t = datetime.fromisoformat(s[:19].replace(" ", "T"))
    return t if t.tzinfo else t.replace(tzinfo=timezone.utc)


def _hours_since(ts, now):
    t = _parse(ts)
    return None if t is None else (now - t).total_seconds() / 3600


def _scalar(sql):
    df = load_data(sql)
    return None if df.empty else df.iloc[0, 0]


# ── 1 신선도 ──────────────────────────────────────────────────────────


def check_freshness(conn, now):
    out = []
    for label, ts in (("회차 스냅샷", _scalar("SELECT MAX(SNAP_AT) AS v FROM TB_COURSE_SNAPSHOT")),
                      ("HRD 명부", _scalar("SELECT MAX(LAST_SEEN_AT) AS v FROM TB_ROSTER_MEMBER")),
                      ("신청자 폴링 AI", get_sync_state(conn, sync_key("AI"))),
                      ("신청자 폴링 SKN", get_sync_state(conn, sync_key("SKN"))),
                      ("등록자 페이지 발행", get_sync_state(conn, "notion_registry_last_publish"))):
        h = _hours_since(ts, now)
        if h is None:
            out.append(f"{label}: 기록 없음 (한 번도 안 돌았거나 상태 키가 없음)")
        elif h > STALE_HOURS:
            out.append(f"{label}: {h:.0f}시간째 갱신 없음 (마지막 {str(ts)[:16]})")
    for label, key in (("HRD API", "kpi_last_errors"), ("노션 신청자 폴링", "notion_applicants_last_error")):
        err = get_sync_state(conn, key)
        if err:
            out.append(f"{label} 직전 실행 오류: {str(err)[:160]}")
    return out


# ── 2 값 ─────────────────────────────────────────────────────────────


def check_values(cohort_rows, today):
    out = []
    for r in cohort_rows:
        k, applied = r["기수"], r["API 신청인원"]
        if applied is not None and r["노션 수집 등록 인원"] is not None and r["노션 수집 등록 인원"] > applied:
            out.append(f"{k}: 노션 수집 등록 {r['노션 수집 등록 인원']} > API 신청인원 {applied} — 기수 매핑이나 중복 의심")
        if applied is not None and r["확정 신고(API)"] is not None and r["확정 신고(API)"] > applied:
            out.append(f"{k}: 확정 신고 {r['확정 신고(API)']} > API 신청인원 {applied}")
        start = str(r["개강일"] or "")[:10]
        day_after = start and (datetime.fromisoformat(start) + timedelta(days=1)).strftime("%Y-%m-%d") <= today
        if day_after and (applied or 0) > 0 and r["개강일 출석 인원"] is None and str(r["개강일"]) >= config.NOTION_REGISTRY_SINCE:
            out.append(f"{k}: 개강({start}) 지났는데 출결 기록 없음 — 출결 API 실패 또는 명부 없음")
    return out


# ── 3 매칭 ───────────────────────────────────────────────────────────


def check_matching(now):
    out = []
    snap = load_data("SELECT DISTINCT TRPR_ID, TRPR_DEGR FROM TB_COURSE_SNAPSHOT")
    known = {cohort_key(t, d) for t, d in zip(snap.TRPR_ID, snap.TRPR_DEGR)} - {None}
    apps = load_data("SELECT NOTION_PAGE_ID, SOURCE_KEY, COHORT, NAME_HASH, STATUS, HRD_APPLY_AT, HRD_REG_AT FROM TB_APPLICANT WHERE COHORT IS NOT NULL")
    unknown = apps[~apps.COHORT.isin(known)].groupby(["SOURCE_KEY", "COHORT"]).size()
    for (src, c), n in unknown.items():
        out.append(f"{src} 신청자 리스트 최종기수 '{c}' {n}명 — HRD 회차와 안 맞음 (오타·미개설)")
    reg = apps[apps.STATUS.isin(["HRD신청", "HRD등록"]) | apps.HRD_APPLY_AT.notna() | apps.HRD_REG_AT.notna()]
    dup = reg.groupby(["COHORT", "NAME_HASH"]).size()
    dup = dup[dup > 1]
    if len(dup):
        out.append("같은 기수에 같은 이름 등록자: " + ", ".join(f"{c} {n}명" for (c, _), n in dup.items()))
    mem = load_data("SELECT TRPR_ID, TRPR_DEGR, NAME_HASH, NAME_MASKED, FIRST_SEEN_AT, TR_STA_DT, GONE_AT FROM TB_ROSTER_MEMBER")
    if not mem.empty:
        mem["COHORT"] = [cohort_key(t, d) for t, d in zip(mem.TRPR_ID, mem.TRPR_DEGR)]
        mem = mem[mem.COHORT.map(lambda c: c and c.rstrip("0123456789") in config.NOTION_KPI_COURSES)
                  & (mem.TR_STA_DT.astype(str).str[:10] >= config.NOTION_REGISTRY_SINCE) & mem.GONE_AT.isna()]
        have = set(zip(apps.COHORT, apps.NAME_HASH))
        fresh = [m for m in mem.itertuples(index=False)
                 if (m.COHORT, m.NAME_HASH) not in have and (_hours_since(m.FIRST_SEEN_AT, now) or 1e9) <= NEW_WINDOW_HOURS]
        if fresh:
            out.append(f"명부에 새로 잡혔는데 노션 신청자 리스트에 없음 {len(fresh)}명: "
                       + ", ".join(f"{m.NAME_MASKED or '?'} · {m.COHORT}" for m in fresh[:10]))
        rdup = mem.groupby(["COHORT", "NAME_HASH"]).size()
        rdup = rdup[rdup > 1]
        if len(rdup):
            out.append("HRD 명부 안 같은 기수 동명이인: " + ", ".join(f"{c} {n}명" for (c, _), n in rdup.items()))
    return out


# ── 4 담당자 기록 대조 ────────────────────────────────────────────────


def check_ops_table(token, cohort_rows, session=None):
    """운영현황표(담당자 수기) 확정자신고 vs API 확정 신고 — 확정 신고가 끝난 기수만."""
    if not token:
        return ["운영현황표 대조 건너뜀: NOTION_TOKEN 없음"]
    try:
        ops = fetch_ops_table(token, session=session)
    except NotionFetchError as e:
        return [f"운영현황표 조회 실패: {e}"]
    by_key = {}
    for r in ops.itertuples(index=False):
        g, n = course_group(r.과정명), cohort_number(r.과정명)
        if g and n:
            by_key[f"{g}{n}"] = r.확정자신고
    out = []
    for r in cohort_rows:
        api = r["확정 신고(API)"]
        if api is None or r["기수"] not in by_key:
            continue
        notion = by_key[r["기수"]]
        if notion is None or str(notion) in ("", "nan"):
            continue
        if int(float(notion)) != int(api):
            out.append(f"{r['기수']}: 운영현황표 확정자신고 {int(float(notion))} vs API 확정 {int(api)} — 담당자 확인")
    return out


# ── 실행 ─────────────────────────────────────────────────────────────


def run_checks(conn, token=None, now=None, session=None):
    """반환: [(섹션 제목, [발견 …]), …] — 발견이 있는 섹션만."""
    now = now or datetime.now(timezone.utc)
    today = now.astimezone(KST).strftime("%Y-%m-%d")
    rows = build_cohort_rows(today=today)
    sections = [("1 신선도·원천 오류", check_freshness(conn, now)),
                ("2 값이 이상함", check_values(rows, today)),
                ("3 매칭 구멍", check_matching(now)),
                ("4 담당자 기록 대조", check_ops_table(token, rows, session))]
    return [(t, f) for t, f in sections if f]


def findings_hash(sections):
    body = "\n".join(f"{t}|{f}" for t, fs in sections for f in fs)
    return hashlib.sha256(body.encode()).hexdigest()[:16]


def should_send(conn, h, now):
    """내용이 바뀌었거나 마지막 알림이 RESEND_DAYS 전이면 보낸다."""
    last_hash = get_sync_state(conn, "health_last_hash")
    last_sent = _parse(get_sync_state(conn, "health_last_sent"))
    if h != last_hash:
        return True
    return last_sent is None or (now - last_sent) > timedelta(days=RESEND_DAYS)


def main(now=None):
    t0 = time.monotonic()
    token = _clean_secret(os.getenv("NOTION_TOKEN"))
    now = now or datetime.now(timezone.utc)
    conn = get_connection(timeout=30)
    try:
        sections = run_checks(conn, token, now)
        for title, items in sections:
            for f in items:
                logger.warning(f"[점검] {title} — {f}")
        h = findings_hash(sections)
        if not sections:
            logger.info(f"[점검] 이상 없음 ({time.monotonic() - t0:.1f}s)")
            set_sync_state(conn, h, "health_last_hash")
            return
        if should_send(conn, h, now):
            n = discord_post([("⚠️ 파이프라인 점검 · " + t, f) for t, f in sections])
            if n:
                set_sync_state(conn, now.strftime("%Y-%m-%dT%H:%M:%SZ"), "health_last_sent")
            logger.info(f"[점검] 발견 {sum(len(f) for _, f in sections)}건 · 디스코드 {n}건 전송 ({time.monotonic() - t0:.1f}s)")
        else:
            logger.info(f"[점검] 발견 {sum(len(f) for _, f in sections)}건 — 직전과 같아 알림 생략 ({time.monotonic() - t0:.1f}s)")
        set_sync_state(conn, h, "health_last_hash")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
