"""회차·명부 스냅샷 ETL — 모집 KPI 1단계.

HRD-Net은 현재 값만 준다(시점 없음). 그래서 매시간 읽어서 **바뀐 것만** 남긴다.

1) 회차 스냅샷 (TB_COURSE_SNAPSHOT)
   회차별 수강신청·승인·수료와 명부 상태 집계를 직전 스냅샷과 비교해 다를 때만 한 줄 (하루 1회는 생존 신호).
   → "신청이 언제 늘고 언제 승인됐는지", "수강신청이 누적값인지 현재값인지"
2) 명부 사람 스냅샷 (TB_ROSTER_MEMBER + _LOG)
   사람마다 명부에 처음 나타난 시각(= HRD 승인 감지), 상태 변화, 첫 참석일·입실 시각.
   참석 판정 = 입실 시간이 있거나 출석 계열 상태 (퇴실 전에는 상태가 '결석'으로 오므로 입실 시간을 본다).
   → "HRD 등록자 개강 참석률"이 개강 당일 실시간으로, "승인이 개강 며칠 뒤였는지"가 사람 단위로 나온다.
   이름은 해시·마스킹만 저장한다. 노션 신청자(TB_APPLICANT.NAME_HASH)와 (해시, 기수)로 대조한다.

대상: config.FUNNEL_COURSE_IDS. 명부·출결은 기관 키가 필요하므로 키가 없는 기관의 회차는 비어 저장된다.
출결은 진행중 회차의 이번 달(+개강이 지난달이면 지난달)만 읽는다 — 첫 참석일은 한 번 정해지면 바뀌지 않는다.

실행: python kpi_etl.py   (GitHub Actions hrd_etl.yml 에서 hrd_etl 다음 단계로 매시간)
"""
import argparse
import logging
import os
import time
from datetime import datetime, timedelta, timezone

import pandas as pd
from dotenv import load_dotenv

import config
from hrd_api import (
    ROSTER_COUNT_COLUMNS, fetch_all_attendance, fetch_all_course_history, fetch_all_rosters,
    get_funnel_institutions, get_institutions, summarize_roster_status,
)
from init_db import init_all_tables
from notify import discord_post
from notion_applicants_etl import name_hash
from utils import adapt_query, get_connection, mask_name

load_dotenv()

logger = logging.getLogger(__name__)
logging.basicConfig(format="%(asctime)s %(levelname)s %(name)s %(message)s", level=logging.INFO)

VALUE_COLUMNS = ["TOT_FXNUM", "TOT_TRP_CNT", "TOT_PAR_MKS", "FINI_CNT",
                 "ROSTER_CNT", "ACTIVE_CNT", "DROPOUT_CNT", "PARTIAL_FINI_CNT", "EARLY_EMPL_CNT"]
SNAPSHOT_COLUMNS = ["TRPR_ID", "TRPR_DEGR", "TR_STA_DT", "TR_END_DT"] + VALUE_COLUMNS

# 출결 상태가 이 중 하나거나 입실 시간이 있으면 '참석'. 휴가·질병·공가는 참석이 아니다.
ATTENDED_STATUSES = frozenset({"출석", "지각", "조퇴", "외출", "100분의50미만출석"})


def _to_int(v):
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    try:
        return int(float(v))
    except (TypeError, ValueError):
        return None


def _s(v):
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    s = str(v).strip()
    return s or None


# ── 1) 회차 스냅샷 ────────────────────────────────────────────────────


def fetch_round_snapshots(pairs=None):
    """회차 집계 + 명부 상태 집계 → (SNAPSHOT_COLUMNS DataFrame, roster_df, error_detail).

    명부 원본(roster_df)은 2)에서 다시 쓰므로 함께 돌려준다 (호출 1회로 두 스냅샷을 만든다).
    """
    if pairs is None:
        pairs = get_funnel_institutions()
    history, hist_err = fetch_all_course_history(pairs)
    df = history.copy()
    df["TRPR_DEGR"] = pd.to_numeric(df["TRPR_DEGR"], errors="coerce").fillna(0).astype(int)
    df = df[df["TRPR_DEGR"] > 0].copy()
    for c in ("TOT_FXNUM", "TOT_TRP_CNT", "TOT_PAR_MKS", "FINI_CNT"):
        df[c] = pd.to_numeric(df[c], errors="coerce")

    # 명부는 승인 인원이 있는 회차만 (개설예정·미승인 회차는 명부가 비어 호출 낭비)
    rounds = [(r.TRPR_ID, int(r.TRPR_DEGR))
              for r in df[df["TOT_PAR_MKS"].fillna(0) > 0][["TRPR_ID", "TRPR_DEGR"]].itertuples(index=False)]
    roster, roster_err, done = fetch_all_rosters(pairs, rounds)
    counts = summarize_roster_status(roster)
    if counts.empty:
        counts = pd.DataFrame(columns=ROSTER_COUNT_COLUMNS)
    df = df.merge(counts, on=["TRPR_ID", "TRPR_DEGR"], how="left")

    errors = " / ".join(e for e in (hist_err, roster_err) if e) or None
    roster = roster.copy()
    roster.attrs["done_rounds"] = done
    return df[SNAPSHOT_COLUMNS].reset_index(drop=True), roster, errors


def load_last_snapshots(conn):
    """(TRPR_ID, TRPR_DEGR) → 직전 스냅샷 {컬럼: 값, 'SNAP_AT': datetime}."""
    cur = conn.cursor()
    cols = ", ".join(VALUE_COLUMNS)
    # 회차별 최신 행 하나 — 창 함수 없이도 SQLite·PG 양쪽에서 도는 형태
    cur.execute(adapt_query(f"""
        SELECT s.TRPR_ID, s.TRPR_DEGR, s.SNAP_AT, {cols}
        FROM TB_COURSE_SNAPSHOT s
        JOIN (SELECT TRPR_ID, TRPR_DEGR, MAX(SNAP_AT) AS MAX_AT
              FROM TB_COURSE_SNAPSHOT GROUP BY TRPR_ID, TRPR_DEGR) m
          ON m.TRPR_ID = s.TRPR_ID AND m.TRPR_DEGR = s.TRPR_DEGR AND m.MAX_AT = s.SNAP_AT
    """))
    out = {}
    for row in cur.fetchall():
        key = (row[0], int(row[1]))
        snap_at = row[2]
        if isinstance(snap_at, str):
            snap_at = datetime.fromisoformat(snap_at.replace("Z", ""))
        out[key] = {"SNAP_AT": snap_at, **{c: _to_int(v) for c, v in zip(VALUE_COLUMNS, row[3:])}}
    return out


def diff_snapshot(current, last, now):
    """저장할지 판단. 반환: None(건너뜀) 또는 CHANGED 문자열."""
    if last is None:
        return "first"
    changed = [c for c in VALUE_COLUMNS if _to_int(current.get(c)) != last.get(c)]
    if changed:
        return ",".join(changed)
    last_day = last["SNAP_AT"].date() if hasattr(last["SNAP_AT"], "date") else None
    if last_day != now.date():
        return "daily"
    return None


def save_snapshots(df, conn, now=None):
    """변화가 있거나 오늘 첫 수집인 회차만 저장. 반환: (저장 건수, 건너뛴 건수)."""
    now = now or datetime.now(timezone.utc).replace(tzinfo=None)
    last = load_last_snapshots(conn)
    cur = conn.cursor()
    insert = adapt_query(
        "INSERT INTO TB_COURSE_SNAPSHOT (TRPR_ID, TRPR_DEGR, SNAP_AT, TR_STA_DT, TR_END_DT, "
        + ", ".join(VALUE_COLUMNS) + ", CHANGED) VALUES (" + ", ".join("?" * (6 + len(VALUE_COLUMNS))) + ")"
    )
    saved = skipped = 0
    for r in df.to_dict("records"):
        key = (r["TRPR_ID"], int(r["TRPR_DEGR"]))
        changed = diff_snapshot(r, last.get(key), now)
        if changed is None:
            skipped += 1
            continue
        cur.execute(insert, [r["TRPR_ID"], int(r["TRPR_DEGR"]), now, r.get("TR_STA_DT"), r.get("TR_END_DT")]
                    + [_to_int(r.get(c)) for c in VALUE_COLUMNS] + [changed])
        saved += 1
    conn.commit()
    return saved, skipped


# ── 2) 명부 사람 스냅샷 ────────────────────────────────────────────────


def is_attended(status, in_time):
    """참석 판정: 입실 시간이 있거나 출석 계열 상태. 퇴실 전엔 상태가 '결석'으로 와서 입실 시간을 본다."""
    return bool(_s(in_time)) or _s(status) in ATTENDED_STATUSES


def attendance_months(tr_sta_dt, today):
    """진행중 회차에 대해 읽을 출결 월: 개강 달 + 이번 달.

    개강 달이 있어야 원래 멤버의 첫 참석일이 맞고, 이번 달이 있어야 늦게 승인된 사람의 첫 참석이 잡힌다.
    (그 사이 달에 합류한 사람은 첫 참석일이 비는데, 승인은 대개 개강 전후 며칠이라 드물다.)
    """
    sta = _s(tr_sta_dt)
    months = {today.strftime("%Y%m")}
    if sta and len(sta) >= 7:
        months.add(sta[:7].replace("-", ""))
    return sorted(months)


def first_attendance(attend_df, round_start=None):
    """출결 → (TRPR_ID, TRPR_DEGR, TRNEE_ID)별 첫 참석일·그날 입실 시각 + 개강일 출결 상태.

    DAY1_STATUS = 개강일(round_start)에 출결 행이 있으면 그 상태(출석·결석·지각 …), 없으면 None.
    "개강일에 왔는지"와 별개로 "개강일 기록이 있는지"(참석 기록 채움률)를 세기 위한 값이라 결석도 남긴다.
    round_start: (과정ID, 회차) → 'YYYY-MM-DD'. 없으면 DAY1_STATUS는 전부 None.
    """
    cols = ["TRPR_ID", "TRPR_DEGR", "TRNEE_ID", "FIRST_ATTEND_DT", "FIRST_IN_TIME", "DAY1_STATUS"]
    if attend_df is None or attend_df.empty:
        return pd.DataFrame(columns=cols)
    keys = ["TRPR_ID", "TRPR_DEGR", "TRNEE_ID"]
    a = attend_df.copy()
    a["TRPR_DEGR"] = pd.to_numeric(a["TRPR_DEGR"], errors="coerce").fillna(0).astype(int)
    a["TRNEE_ID"] = a["TRNEE_ID"].astype(str)
    a["ATEND_DT"] = a["ATEND_DT"].astype(str)
    a["_att"] = [is_attended(s, t) for s, t in zip(a["ATEND_STATUS"], a["IN_TIME"])]
    first = (a[a["_att"]].sort_values("ATEND_DT").groupby(keys, as_index=False).first()
             .rename(columns={"ATEND_DT": "FIRST_ATTEND_DT", "IN_TIME": "FIRST_IN_TIME"})[keys + ["FIRST_ATTEND_DT", "FIRST_IN_TIME"]])
    starts = {k: str(v).replace("-", "")[:8] for k, v in (round_start or {}).items()}
    a["_start"] = [starts.get((t, d)) for t, d in zip(a["TRPR_ID"], a["TRPR_DEGR"])]
    day1 = (a[a["ATEND_DT"] == a["_start"]].groupby(keys, as_index=False).first()
            .rename(columns={"ATEND_STATUS": "DAY1_STATUS"})[keys + ["DAY1_STATUS"]])
    out = first.merge(day1, on=keys, how="outer")
    return out.reindex(columns=cols).astype(object).where(out.reindex(columns=cols).notna(), None)


def fetch_first_attendance(pairs, snapshots_df, today=None):
    """진행중 회차의 출결을 읽어 첫 참석일 표 반환. 반환: (df, error_detail)."""
    today = today or datetime.now(timezone(timedelta(hours=9))).date()
    today_str = today.strftime("%Y-%m-%d")
    active = snapshots_df[
        (snapshots_df["TOT_PAR_MKS"].fillna(0) > 0)
        & (snapshots_df["TR_STA_DT"].astype(str) <= today_str)
        & (snapshots_df["TR_END_DT"].astype(str) >= today_str)
    ]
    targets = [(r.TRPR_ID, int(r.TRPR_DEGR), ym)
               for r in active.itertuples(index=False) for ym in attendance_months(r.TR_STA_DT, today)]
    attend, err, _ = fetch_all_attendance(pairs, targets)
    round_start = {(r.TRPR_ID, int(r.TRPR_DEGR)): str(r.TR_STA_DT) for r in active.itertuples(index=False)}
    return first_attendance(attend, round_start), err


def _label(trpr_id, degr):
    return f"{config.COURSE_SHORT_NAMES.get(trpr_id, trpr_id)}{int(degr)}"


def upsert_roster_members(conn, roster_df, first_att_df, done_rounds, round_start, now=None, events=None):
    """명부 사람 단위 upsert + 이벤트 로그. 반환: {'joined','status','first_attend','left'} 건수.

    done_rounds: 이번에 명부를 성공적으로 읽은 (과정ID, 회차) 집합 — 그 회차에서만 '사라짐(LEFT)'을 판정한다.
    round_start: (과정ID, 회차) → 개강일.
    events: 리스트를 주면 알림용 승인(JOINED)·이탈(LEFT)을 {"kind","name","cohort"}로 덧붙인다 (AI캠퍼스 과정만).
    """
    now = now or datetime.now(timezone.utc).replace(tzinfo=None)
    cur = conn.cursor()
    counts = {"joined": 0, "status": 0, "first_attend": 0, "left": 0}
    log = adapt_query(
        "INSERT INTO TB_ROSTER_MEMBER_LOG (TRPR_ID, TRPR_DEGR, TRNEE_ID, DETECTED_AT, EVENT, OLD_VALUE, NEW_VALUE) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)"
    )
    first_map = {}
    if first_att_df is not None and not first_att_df.empty:
        for r in first_att_df.itertuples(index=False):
            first_map[(r.TRPR_ID, int(r.TRPR_DEGR), str(r.TRNEE_ID))] = (
                _s(r.FIRST_ATTEND_DT), _s(r.FIRST_IN_TIME), _s(getattr(r, "DAY1_STATUS", None)))

    # 현재 DB에 있는 회원 (성공 회차만)
    existing = {}
    if done_rounds:
        cur.execute(adapt_query(
            "SELECT TRPR_ID, TRPR_DEGR, TRNEE_ID, STATUS, GONE_AT, FIRST_ATTEND_DT, DAY1_STATUS, NAME_MASKED FROM TB_ROSTER_MEMBER"))
        for row in cur.fetchall():
            if (row[0], int(row[1])) in done_rounds:
                existing[(row[0], int(row[1]), str(row[2]))] = {"STATUS": row[3], "GONE_AT": row[4],
                                                                 "FIRST_ATTEND_DT": row[5], "DAY1_STATUS": row[6], "NAME_MASKED": row[7]}

    seen = set()
    rows = roster_df.to_dict("records") if roster_df is not None and not roster_df.empty else []
    for r in rows:
        key = (r["TRPR_ID"], int(r["TRPR_DEGR"]), str(r["TRNEE_ID"]))
        seen.add(key)
        status = _s(r.get("TRNEE_STATUS"))
        fa_dt, fa_time, day1 = first_map.get(key, (None, None, None))
        prev = existing.get(key)
        if prev is None:
            cur.execute(adapt_query(
                "INSERT INTO TB_ROSTER_MEMBER (TRPR_ID, TRPR_DEGR, TRNEE_ID, NAME_HASH, NAME_MASKED, TR_STA_DT, STATUS, "
                "FIRST_SEEN_AT, LAST_SEEN_AT, STATUS_CHANGED_AT, FIRST_ATTEND_DT, FIRST_IN_TIME, DAY1_STATUS) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"),
                [key[0], key[1], key[2], name_hash(r.get("TRNEE_NM")), mask_name(r.get("TRNEE_NM")),
                 round_start.get((key[0], key[1])), status, now, now, now, fa_dt, fa_time, day1])
            cur.execute(log, [key[0], key[1], key[2], now, "JOINED", None, status])
            counts["joined"] += 1
            if events is not None and config.COURSE_SHORT_NAMES.get(key[0]) in config.NOTION_KPI_COURSES:
                events.append({"kind": "JOINED", "name": mask_name(r.get("TRNEE_NM")), "cohort": _label(key[0], key[1])})
            if fa_dt:
                cur.execute(log, [key[0], key[1], key[2], now, "FIRST_ATTEND", None, fa_dt])
                counts["first_attend"] += 1
            continue
        sets, params = ["LAST_SEEN_AT = ?", "GONE_AT = NULL"], [now]
        if _s(prev["STATUS"]) != status:
            cur.execute(log, [key[0], key[1], key[2], now, "STATUS", _s(prev["STATUS"]), status])
            sets += ["STATUS = ?", "STATUS_CHANGED_AT = ?"]
            params += [status, now]
            counts["status"] += 1
        if fa_dt and not _s(prev["FIRST_ATTEND_DT"]):
            cur.execute(log, [key[0], key[1], key[2], now, "FIRST_ATTEND", None, fa_dt])
            sets += ["FIRST_ATTEND_DT = ?", "FIRST_IN_TIME = ?"]
            params += [fa_dt, fa_time]
            counts["first_attend"] += 1
        if day1 and day1 != _s(prev.get("DAY1_STATUS")):     # 개강일 출결은 사후 정정될 수 있어 값이 있으면 최신으로
            sets += ["DAY1_STATUS = ?"]
            params += [day1]
        cur.execute(adapt_query(
            f"UPDATE TB_ROSTER_MEMBER SET {', '.join(sets)} WHERE TRPR_ID = ? AND TRPR_DEGR = ? AND TRNEE_ID = ?"),
            params + list(key))

    # 성공 회차 명부에서 사라진 사람
    for key, prev in existing.items():
        if key in seen or prev["GONE_AT"]:
            continue
        cur.execute(adapt_query(
            "UPDATE TB_ROSTER_MEMBER SET GONE_AT = ? WHERE TRPR_ID = ? AND TRPR_DEGR = ? AND TRNEE_ID = ?"),
            [now, *key])
        cur.execute(log, [key[0], key[1], key[2], now, "LEFT", _s(prev["STATUS"]), None])
        counts["left"] += 1
        if events is not None and config.COURSE_SHORT_NAMES.get(key[0]) in config.NOTION_KPI_COURSES:
            events.append({"kind": "LEFT", "name": prev.get("NAME_MASKED"), "cohort": _label(key[0], key[1])})
    conn.commit()
    return counts


# ── 실행 ─────────────────────────────────────────────────────────────


def kpi_course_ids():
    """노션 「모집 KPI」 대상 과정(AI캠퍼스)만 — 웹훅 경로에서 SKN·한화 72회차를 다 읽으면 5분이 걸려 이걸로 좁힌다."""
    return [cid for cid in config.FUNNEL_COURSE_IDS if config.COURSE_SHORT_NAMES.get(cid) in config.NOTION_KPI_COURSES]


def main(argv=None):
    parser = argparse.ArgumentParser(description="HRD 회차·명부 스냅샷")
    parser.add_argument("--kpi-only", action="store_true",
                        help="AI캠퍼스(config.NOTION_KPI_COURSES) 과정만 읽는다. 노션 웹훅 트리거(kpi_poll.yml)용 — 다른 과정 회차는 건드리지 않는다")
    args = parser.parse_args(argv)
    if not (os.getenv("HRD_API_KEY") or os.getenv("ENCORE_API_KEY")):
        logger.error("HRD_API_KEY / ENCORE_API_KEY 가 없습니다.")
        return
    t0 = time.monotonic()
    init_all_tables(include_market=False)
    pairs = get_institutions(kpi_course_ids()) if args.kpi_only else get_funnel_institutions()
    logger.info(f"[KPI 스냅샷] 과정 {len(pairs)}개 조회{' (AI캠퍼스만)' if args.kpi_only else ''}")
    df, roster, errors = fetch_round_snapshots(pairs)
    if errors:
        logger.warning(f"[KPI 스냅샷] 일부 조회 실패: {errors}")
    first_att, att_err = fetch_first_attendance(pairs, df)
    if att_err:
        logger.warning(f"[KPI 명부] 출결 일부 조회 실패: {att_err}")

    round_start = {(r.TRPR_ID, int(r.TRPR_DEGR)): _s(r.TR_STA_DT) for r in df.itertuples(index=False)}
    conn = get_connection(timeout=30)
    try:
        saved, skipped = save_snapshots(df, conn)
        events = []
        member = upsert_roster_members(conn, roster, first_att, roster.attrs.get("done_rounds", set()), round_start, events=events)
    finally:
        conn.close()
    logger.info(f"[KPI 스냅샷] 회차 {len(df)}개 중 저장 {saved} · 변화 없음 {skipped}")
    logger.info(f"[KPI 명부] 사람 {len(roster)}명 · 신규 {member['joined']} · 상태 변화 {member['status']} · "
                f"첫 참석 {member['first_attend']} · 이탈(명부 제외) {member['left']} ({time.monotonic() - t0:.1f}s)")
    sent = discord_post([("✅ HRD 승인 감지 (명부 등장)", [f"{e['name'] or '?'} · {e['cohort']}" for e in events if e["kind"] == "JOINED"]),
                         ("⚠️ HRD 명부에서 사라짐 (승인 취소·이탈)", [f"{e['name'] or '?'} · {e['cohort']}" for e in events if e["kind"] == "LEFT"])])
    if sent:
        logger.info(f"[KPI 명부] 디스코드 알림 {sent}건")


if __name__ == "__main__":
    main()
