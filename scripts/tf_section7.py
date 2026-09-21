"""운영TF 구간 7 측정값 출력 — 「일별 액션 측정」에 붙여넣을 분모·분자.

TF 정의(2026-09-21): 등록 = 노션 최종결과 HRD등록·합격자등록. 등록자를 (기수, 이름 해시)로 HRD 명부와 맞춘다.
  · HRD 등록 대비 개강 참석률   분모 등록자 / 분자 그중 HRD-Net 출결 개강일 입실
  · 개강 참석 기록 채움률       분모 등록자 / 분자 그중 개강일 출결 행이 있는 사람(출석·결석 무관)
  · 확정자 신고율               분모 HRD-Net 수강신청 인원(totTrpCnt) / 분자 HRD-Net 확정 신고 인원(totParMks) — 개강 + 7일부터
실행: python scripts/tf_section7.py [--since 2026-07-01]   (읽기만, 노션에 쓰지 않는다)
"""
import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from datetime import datetime, timedelta, timezone  # noqa: E402

import config  # noqa: E402
from utils import load_data  # noqa: E402


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--since", default="2026-07-01", help="이 날 이후 개강 기수만 (YYYY-MM-DD)")
    args = ap.parse_args(argv)
    today = datetime.now(timezone(timedelta(hours=9))).strftime("%Y-%m-%d")

    apps = load_data("SELECT COHORT, NAME_HASH FROM TB_APPLICANT WHERE COHORT IS NOT NULL AND STATUS IN ('HRD등록', '합격자등록')")
    mem = load_data("SELECT TRPR_ID, TRPR_DEGR, NAME_HASH, FIRST_ATTEND_DT, DAY1_STATUS, TR_STA_DT FROM TB_ROSTER_MEMBER")
    mem["COHORT"] = [f"{config.COURSE_SHORT_NAMES.get(t)}{int(d)}" for t, d in zip(mem.TRPR_ID, mem.TRPR_DEGR)]
    snap = load_data("""SELECT TRPR_ID, TRPR_DEGR, TR_STA_DT, TOT_TRP_CNT, TOT_PAR_MKS FROM TB_COURSE_SNAPSHOT s
        WHERE SNAP_AT = (SELECT MAX(SNAP_AT) FROM TB_COURSE_SNAPSHOT s2 WHERE s2.TRPR_ID = s.TRPR_ID AND s2.TRPR_DEGR = s.TRPR_DEGR)""")
    snap["COHORT"] = [f"{config.COURSE_SHORT_NAMES.get(t)}{int(d)}" for t, d in zip(snap.TRPR_ID, snap.TRPR_DEGR)]
    snap = snap[snap["TR_STA_DT"].astype(str) >= args.since].set_index("COHORT").sort_values("TR_STA_DT")
    m = apps.merge(mem.drop_duplicates(["COHORT", "NAME_HASH"]), on=["COHORT", "NAME_HASH"], how="left")

    tot = {"reg": 0, "day1": 0, "rec": 0, "trp": 0, "par": 0}
    print(f'{"기수":6} {"개강":10} {"등록자":>4} {"개강일출석":>6} {"출결기록":>5} {"API신청":>6} {"확정":>4}  {"참석률":>6} {"채움률":>6} {"신고율":>6}')
    for c, s in snap.iterrows():
        start = str(s.TR_STA_DT)[:10]
        g = m[m.COHORT == c]
        reg = len(g)
        day1 = int((g.FIRST_ATTEND_DT.astype(str).str[:8] == start.replace("-", "")).sum())
        rec = int(g.DAY1_STATUS.notna().sum())
        trp = int(s.TOT_TRP_CNT) if s.TOT_TRP_CNT == s.TOT_TRP_CNT else 0
        par = int(s.TOT_PAR_MKS) if s.TOT_PAR_MKS == s.TOT_PAR_MKS else None
        started = start < today
        due = (datetime.fromisoformat(start) + timedelta(days=config.NOTION_KPI_CONFIRM_DAYS)).strftime("%Y-%m-%d") <= today
        pct = lambda a, b: f"{a / b * 100:5.1f}" if b else "    -"   # noqa: E731
        print(f"{c:6} {start:10} {reg:>5} {day1 if started else '-':>7} {rec if started else '-':>7} {trp:>7} {par if due else '-':>5}  "
              f"{pct(day1, reg) if started else '     -':>6} {pct(rec, reg) if started else '     -':>6} {pct(par or 0, trp) if due else '     -':>6}")
        if started:
            tot["reg"] += reg; tot["day1"] += day1; tot["rec"] += rec   # noqa: E702
        if due and par is not None:
            tot["trp"] += trp; tot["par"] += par   # noqa: E702
    print("\n「일별 액션 측정」 입력값 (개강한 기수 합계, 측정일", today + ")")
    print(f"  HRD 등록 대비 개강 참석률   분모 {tot['reg']} / 분자 {tot['day1']}  → {tot['day1'] / tot['reg'] * 100:.1f}%" if tot["reg"] else "  등록자 없음")
    print(f"  개강 참석 기록 채움률       분모 {tot['reg']} / 분자 {tot['rec']}  → {tot['rec'] / tot['reg'] * 100:.1f}%" if tot["reg"] else "")
    print(f"  확정자 신고율               분모 {tot['trp']} / 분자 {tot['par']}  → {tot['par'] / tot['trp'] * 100:.1f}%" if tot["trp"] else "")


if __name__ == "__main__":
    main()
