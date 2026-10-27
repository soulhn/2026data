"""파이프라인 자기 점검 — 이상할 때만 디스코드로 알린다 (hrd_etl.yml 마지막 단계, 하루 2회).

멈춘 것은 GitHub 실패 메일이 잡지만, "조용히 틀리는 것"은 아무도 모른다: 노션 토큰 만료로 건너뜀, HRD API 한 기관만 실패,
기수명 오타로 매칭 누락, 담당자 오기입. 그래서 실행마다 아래를 검사하고 걸리는 것만 보낸다. 정상이면 조용하다.

  1 신선도   회차 스냅샷·명부·신청자 폴링(AI/SKN)·페이지 발행이 36시간 안에 돌았나, 직전 실행의 원천 오류가 남았나
  2 값       노션 수집 > API 신청인원, 확정 신고 > API 신청인원, 개강 지났는데 출결 기록 없음, 등록한 일치 예외 기수의 차이가 바뀜,
            완전 기록 관리 기수(config.NOTION_FULL_RECORD_SINCE 이후 개강)에서 API 신청 > 노션 수집 (HRD-Net엔 신청했는데 노션에 HRD신청으로 없는 사람)
  3 매칭     스냅샷에 없는 기수명(오타), 같은 기수 동명이인, 최근 24시간에 새로 생긴 '노션에 없음'
  4 담당자   운영현황표 확정자신고 vs API 확정 신고 (확정 신고 끝난 기수),
            HRD 이력 뒤 취소됐는데 「HRD 취소 처리」가 빈 사람 (config.NOTION_HRD_CANCEL_SINCE 이후 취소, 24시간 지남)

같은 내용이면 다시 보내지 않고(해시), 그래도 7일마다 한 번은 다시 알린다. 읽기만 한다 — 노션엔 쓰지 않는다.
사람 이름은 디스코드에만 실명으로 보내고 로그·해시에는 가린 이름을 쓴다 (public 레포라 Actions 로그가 공개, 2026-10-26).
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
from notion_registry_publish import DROP_COL, build_cohort_rows, cohort_key, ever_registered_pages
from utils import _clean_secret, get_connection, load_data

load_dotenv()
logger = logging.getLogger(__name__)
logging.basicConfig(format="%(asctime)s %(levelname)s %(name)s %(message)s", level=logging.INFO)

STALE_HOURS = 36            # 하루 2회 실행이라 36시간이면 한 번은 걸러진 것
NEW_WINDOW_HOURS = 24       # '새로 생긴 노션에 없음'을 세는 창
RESEND_DAYS = 7             # 같은 내용도 이 주기로는 다시 알린다
CANCEL_FIELD_GRACE_HOURS = 24   # 취소 처리 후 「HRD 취소 처리」를 고를 여유. 최종결과만 먼저 바꾸는 경우가 있어 바로 알리지 않는다
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
            # 노션 수집은 누적, API totTrpCnt는 지금 남은 신청 건수(고용24 본인 취소는 빠짐) → HRD 이력 뒤 취소된 사람 수까지는 정상
            gap, cancelled = r["노션 수집 등록 인원"] - applied, r.get("_HRD 이력 취소") or 0
            if gap > cancelled:
                out.append(f"{k}: 노션 수집 등록 {r['노션 수집 등록 인원']} > API 신청인원 {applied} — 차이 {gap}명 중 HRD 이력 뒤 취소·전환 {cancelled}명으로 "
                           f"설명되지 않음. 기수 매핑이나 중복 의심")
        if applied is not None and r["확정 신고(API)"] is not None and r["확정 신고(API)"] > applied:
            out.append(f"{k}: 확정 신고 {r['확정 신고(API)']} > API 신청인원 {applied}")
        exc = config.NOTION_MATCH_EXCEPTIONS.get(k)
        gap = r["노션 수집 등록 인원"] - applied if applied is not None and r["노션 수집 등록 인원"] is not None else None
        if exc and gap is not None and gap != exc[0] and gap != (r.get(DROP_COL) or 0):
            # 등록한 예외 기수는 차이가 정확히 같아야 일치(예외). 달라지면 노션에서만 불일치로 바뀌어 조용히 묻히므로 알린다 (2026-10-27).
            # 「HRD 취소 처리」 ①·③ 인원으로 차이가 설명되면(일치(취소 반영)) 조용
            out.append(f"{k}: 노션 {r['노션 수집 등록 인원']} − API 신청인원 {applied} = {r['노션 수집 등록 인원'] - applied}명인데 등록한 예외는 {exc[0]}명({exc[1]}). "
                       f"새 본인 취소·노션 기록 변경을 확인하고 config.NOTION_MATCH_EXCEPTIONS 갱신")
        start = str(r["개강일"] or "")[:10]
        notion = r["노션 수집 등록 인원"]
        if applied is not None and notion is not None and applied > notion and start >= config.NOTION_FULL_RECORD_SINCE:
            # 수집 전 기수는 취소자에게 HRD 일자가 없어 항상 걸리므로 완전 기록 관리 기수만
            out.append(f"{k}: HRD-Net 신청 {applied} vs 노션 {notion} — {applied - notion}명이 노션에 HRD신청으로 없음. "
                       f"최종결과를 HRD신청으로 바꾸고 HRD 신청 일시 기록 요청 (취소자도 일시는 남겨야 함)")
        day_after = start and (datetime.fromisoformat(start) + timedelta(days=1)).strftime("%Y-%m-%d") <= today
        if day_after and (applied or 0) > 0 and r["개강일 출석 인원(API)"] is None and str(r["개강일"]) >= config.NOTION_REGISTRY_SINCE:
            out.append(f"{k}: 개강({start}) 지났는데 출결 기록 없음 — 출결 API 실패 또는 명부 없음")
    return out


# ── 3 매칭 ───────────────────────────────────────────────────────────


class Named(str):
    """점검 한 줄. 문자열 값(가린 이름)은 로그·해시에, .discord(실명)는 디스코드에만 쓴다 (public 레포라 Actions 로그가 공개된다)."""

    def __new__(cls, masked, discord):
        s = super().__new__(cls, masked)
        s.discord = discord
        return s


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
    mem = load_data("SELECT TRPR_ID, TRPR_DEGR, NAME_HASH, NAME_MASKED, NAME, FIRST_SEEN_AT, TR_STA_DT, GONE_AT FROM TB_ROSTER_MEMBER")
    if not mem.empty:
        mem["COHORT"] = [cohort_key(t, d) for t, d in zip(mem.TRPR_ID, mem.TRPR_DEGR)]
        mem = mem[mem.COHORT.map(lambda c: c and c.rstrip("0123456789") in config.NOTION_KPI_COURSES)
                  & (mem.TR_STA_DT.astype(str).str[:10] >= config.NOTION_REGISTRY_SINCE) & mem.GONE_AT.isna()]
        have = set(zip(apps.COHORT, apps.NAME_HASH))
        fresh = [m for m in mem.itertuples(index=False)
                 if (m.COHORT, m.NAME_HASH) not in have and (_hours_since(m.FIRST_SEEN_AT, now) or 1e9) <= NEW_WINDOW_HOURS]
        if fresh:
            shown = fresh[:10]
            head = f"명부에 새로 잡혔는데 노션 신청자 리스트에 없음 {len(fresh)}명: "
            out.append(Named(head + ", ".join(f"{m.NAME_MASKED or '?'} · {m.COHORT}" for m in shown),
                             head + ", ".join(f"{m.NAME or m.NAME_MASKED or '?'} · {m.COHORT}" for m in shown)))
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


def check_cancel_field(now):
    """HRD 이력 뒤 취소됐는데 「HRD 취소 처리」가 빈 사람. config.NOTION_HRD_CANCEL_SINCE 이후 취소, CANCEL_FIELD_GRACE_HOURS 지난 경우만 (2026-10-27).
    로그·해시엔 가린 이름, 디스코드엔 실명(Named)."""
    apps = load_data("SELECT NOTION_PAGE_ID, COHORT, NAME, NAME_MASKED, STATUS FROM TB_APPLICANT WHERE HRD_CANCEL_BY IS NULL AND STATUS IS NOT NULL")
    apps = apps[apps.STATUS.str.contains("취소") & apps.NOTION_PAGE_ID.isin(ever_registered_pages())]
    if apps.empty:
        return []
    log = load_data("SELECT NOTION_PAGE_ID, NEW_VALUE, MAX(DETECTED_AT) AS LAST_AT FROM TB_APPLICANT_STATUS_LOG WHERE FIELD = 'STATUS' GROUP BY NOTION_PAGE_ID, NEW_VALUE")
    when = {(r.NOTION_PAGE_ID, r.NEW_VALUE): r.LAST_AT for r in log.itertuples(index=False)}
    late = []
    for a in apps.itertuples(index=False):
        t = _parse(when.get((a.NOTION_PAGE_ID, a.STATUS)))   # 지금 최종결과(취소)로 바뀐 것을 감지한 시각
        if t and t.astimezone(KST).strftime("%Y-%m-%d") >= config.NOTION_HRD_CANCEL_SINCE and (now - t) >= timedelta(hours=CANCEL_FIELD_GRACE_HOURS):
            late.append(a)
    if not late:
        return []
    shown = late[:10]
    head = f"HRD 이력 뒤 취소됐는데 「HRD 취소 처리」가 빈 사람 {len(late)}명 (취소 {CANCEL_FIELD_GRACE_HOURS}시간 지남, ①본인·②기관·③고용노동부 중 선택 요청): "
    return [Named(head + ", ".join(f"{a.NAME_MASKED or '?'} · {a.COHORT or '-'}" for a in shown),
                  head + ", ".join(f"{a.NAME or a.NAME_MASKED or '?'} · {a.COHORT or '-'}" for a in shown))]


# ── 실행 ─────────────────────────────────────────────────────────────


def run_checks(conn, token=None, now=None, session=None):
    """반환: [(섹션 제목, [발견 …]), …] — 발견이 있는 섹션만."""
    now = now or datetime.now(timezone.utc)
    today = now.astimezone(KST).strftime("%Y-%m-%d")
    rows = build_cohort_rows(today=today)
    sections = [("1 신선도·원천 오류", check_freshness(conn, now)),
                ("2 값이 이상함", check_values(rows, today)),
                ("3 매칭 구멍", check_matching(now)),
                ("4 담당자 기록 대조", check_ops_table(token, rows, session) + check_cancel_field(now))]
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
            n = discord_post([("⚠️ 파이프라인 점검 · " + t, [getattr(x, "discord", x) for x in f]) for t, f in sections])
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
