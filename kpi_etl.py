"""회차별 HRD-Net 집계 스냅샷 ETL — 모집 KPI 1단계.

HRD-Net은 현재 값만 준다(시점 없음). 그래서 매시간 회차별 수강신청·승인·수료와 명부 상태를 읽어
**직전 스냅샷과 다를 때만** TB_COURSE_SNAPSHOT에 한 줄 남긴다 (하루에 한 번은 변화가 없어도 남겨 생존 신호).
이 이력이 있어야 "신청이 언제 늘고 언제 승인됐는지", "수강신청이 누적값인지 현재값인지(줄어드는 날이 있는지)",
"승인 → 확정 신고까지 며칠 걸리는지"를 답할 수 있다.

대상: config.FUNNEL_COURSE_IDS (등록된 과정 전부). 회차 집계는 키 소속과 무관하게 조회되지만
명부는 기관 키가 필요하므로, 키가 없는 기관의 회차는 명부 컬럼이 비어 저장된다.

실행: python kpi_etl.py   (GitHub Actions hrd_etl.yml 에서 hrd_etl 다음 단계로 매시간)
"""
import logging
import os
import time
from datetime import datetime, timezone

import pandas as pd
from dotenv import load_dotenv

from hrd_api import (
    ROSTER_COUNT_COLUMNS, fetch_all_course_history, fetch_all_roster_counts, get_funnel_institutions,
)
from init_db import init_all_tables
from utils import adapt_query, get_connection

load_dotenv()

logger = logging.getLogger(__name__)
logging.basicConfig(format="%(asctime)s %(levelname)s %(name)s %(message)s", level=logging.INFO)

VALUE_COLUMNS = ["TOT_FXNUM", "TOT_TRP_CNT", "TOT_PAR_MKS", "FINI_CNT",
                 "ROSTER_CNT", "ACTIVE_CNT", "DROPOUT_CNT", "PARTIAL_FINI_CNT", "EARLY_EMPL_CNT"]
SNAPSHOT_COLUMNS = ["TRPR_ID", "TRPR_DEGR", "TR_STA_DT", "TR_END_DT"] + VALUE_COLUMNS


def _to_int(v):
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    try:
        return int(float(v))
    except (TypeError, ValueError):
        return None


def fetch_round_snapshots(pairs=None):
    """회차 집계 + 명부 상태 집계를 합쳐 SNAPSHOT_COLUMNS 구조의 DataFrame으로.

    Returns:
        (df, error_detail) — 부분 실패한 과정·회차는 빠지고 사유를 문자열로 돌려준다.
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
    counts, roster_err = fetch_all_roster_counts(pairs, rounds)
    if counts.empty:
        counts = pd.DataFrame(columns=ROSTER_COUNT_COLUMNS)
    df = df.merge(counts, on=["TRPR_ID", "TRPR_DEGR"], how="left")

    errors = " / ".join(e for e in (hist_err, roster_err) if e) or None
    return df[SNAPSHOT_COLUMNS].reset_index(drop=True), errors


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


def main():
    if not (os.getenv("HRD_API_KEY") or os.getenv("ENCORE_API_KEY")):
        logger.error("HRD_API_KEY / ENCORE_API_KEY 가 없습니다.")
        return
    t0 = time.monotonic()
    init_all_tables(include_market=False)
    pairs = get_funnel_institutions()
    logger.info(f"[KPI 스냅샷] 과정 {len(pairs)}개 조회")
    df, errors = fetch_round_snapshots(pairs)
    if errors:
        logger.warning(f"[KPI 스냅샷] 일부 조회 실패: {errors}")
    conn = get_connection(timeout=30)
    try:
        saved, skipped = save_snapshots(df, conn)
    finally:
        conn.close()
    logger.info(f"[KPI 스냅샷] 회차 {len(df)}개 중 저장 {saved} · 변화 없음 {skipped} ({time.monotonic() - t0:.1f}s)")


if __name__ == "__main__":
    main()
