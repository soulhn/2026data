"""운영TF 구간 7 측정값 출력 — 「일별 액션 측정」에 붙여넣을 분모·분자.

TF 정의(2026-09-21): 등록 = 노션 최종결과 HRD등록·합격자등록. 등록자를 (기수, 이름 해시)로 HRD 명부와 맞춘다.
  · HRD 등록 대비 개강 참석률   분모 등록자 / 분자 그중 HRD-Net 출결 개강일 입실
  · 개강 참석 기록 채움률       분모 등록자 / 분자 그중 개강일 출결 행이 있는 사람(출석·결석 무관)
  · 개강 대비 확정 비율(책임 수치, 2026-09-29 팀 확정)  분모 운영현황표 개강인원(오프라인) / 분자 운영현황표 확정자신고(= 개강인원 − 초기이탈 + 추가인원). 100% 초과 가능
      하위: 초기이탈률 = 초기이탈 ÷ 개강인원(낮을수록 좋음) · 추가인원율 = 추가인원 ÷ 개강인원. 운영현황표는 notion_registry_publish.load_ops_counts로 읽기만
  · 확정자 신고율(API, 참고)     분모 HRD-Net 수강신청(totTrpCnt) / 분자 확정 신고(totParMks, 개강 + 7일 이후 첫 스냅샷 값으로 고정)
실행: python scripts/tf_section7.py [--since 2026-07-01]   (읽기만, 노션에 쓰지 않는다)
"""
import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from datetime import datetime, timedelta, timezone  # noqa: E402

import config  # noqa: E402
from notion_registry_publish import load_confirmed_at_due, load_ops_counts  # noqa: E402
from utils import _clean_secret, load_data  # noqa: E402


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
    frozen = load_confirmed_at_due()   # 확정 신고 고정값 (개강 + 7일 이후 첫 스냅샷)
    token = _clean_secret(os.getenv("NOTION_TOKEN"))
    ops = load_ops_counts(token) if token else {}   # 운영현황표 수기(개강인원·초기이탈·추가인원·확정자신고), 읽기만

    tot = {"reg": 0, "day1": 0, "rec": 0, "trp": 0, "par": 0, "open": 0, "early": 0, "add": 0, "conf": 0}
    print(f'{"기수":6} {"개강":10} {"등록자":>4} {"개강일출석":>6} {"출결기록":>5} {"API신청":>6} {"확정":>4}  {"참석률":>6} {"채움률":>6} {"API신고율":>6} | {"개강인원":>4} {"이탈":>3} {"추가":>3} {"대비확정":>6} {"이탈률":>6}')
    for c, s in snap.iterrows():
        start = str(s.TR_STA_DT)[:10]
        g = m[m.COHORT == c]
        reg = len(g)
        day1 = int((g.FIRST_ATTEND_DT.astype(str).str[:8] == start.replace("-", "")).sum())
        rec = int(g.DAY1_STATUS.notna().sum())
        trp = int(s.TOT_TRP_CNT) if s.TOT_TRP_CNT == s.TOT_TRP_CNT else 0
        par = frozen.get((s.TRPR_ID, int(s.TRPR_DEGR)), (None, None))[0]
        started = start < today
        due = (datetime.fromisoformat(start) + timedelta(days=config.NOTION_KPI_CONFIRM_DAYS)).strftime("%Y-%m-%d") <= today
        pct = lambda a, b: f"{a / b * 100:5.1f}" if b else "    -"   # noqa: E731
        o = ops.get(c) or {}
        opened, early, added, conf = o.get("개강인원"), o.get("초기이탈"), o.get("추가인원"), o.get("확정자신고")
        has_ops = due and opened
        print(f"{c:6} {start:10} {reg:>5} {day1 if started else '-':>7} {rec if started else '-':>7} {trp:>7} {par if due else '-':>5}  "
              f"{pct(day1, reg) if started else '     -':>6} {pct(rec, reg) if started else '     -':>6} {pct(par or 0, trp) if due else '     -':>6} | "
              f"{opened if has_ops else '-':>6} {early if has_ops else '-':>4} {added if has_ops else '-':>4} {pct(conf or 0, opened) if has_ops else '     -':>7} {pct(early or 0, opened) if has_ops else '     -':>6}")
        if has_ops and conf is not None:
            tot["open"] += opened; tot["early"] += early or 0; tot["add"] += added or 0; tot["conf"] += conf   # noqa: E702
        if started:
            tot["reg"] += reg; tot["day1"] += day1; tot["rec"] += rec   # noqa: E702
        if due and par is not None:
            tot["trp"] += trp; tot["par"] += par   # noqa: E702
    print("\n「일별 액션 측정」 입력값 (개강한 기수 합계, 측정일", today + ")")
    print(f"  HRD 등록 대비 개강 참석률   분모 {tot['reg']} / 분자 {tot['day1']}  → {tot['day1'] / tot['reg'] * 100:.1f}%" if tot["reg"] else "  등록자 없음")
    print(f"  개강 참석 기록 채움률       분모 {tot['reg']} / 분자 {tot['rec']}  → {tot['rec'] / tot['reg'] * 100:.1f}%" if tot["reg"] else "")
    if tot["open"]:
        print(f"  개강 대비 확정 비율          분모 {tot['open']} / 분자 {tot['conf']}  → {tot['conf'] / tot['open'] * 100:.1f}%   (책임 수치 · 운영현황표 수기 · 100% 초과 가능)")
        print(f"    ㄴ 초기이탈률              분모 {tot['open']} / 분자 {tot['early']}  → {tot['early'] / tot['open'] * 100:.1f}%   (낮을수록 좋음 · 액션②)")
        print(f"    ㄴ 추가인원율              분모 {tot['open']} / 분자 {tot['add']}  → {tot['add'] / tot['open'] * 100:.1f}%")
    else:
        print("  개강 대비 확정 비율          운영현황표를 읽지 못함 (NOTION_TOKEN 확인)")
    print(f"  확정자 신고율(API, 참고)    분모 {tot['trp']} / 분자 {tot['par']}  → {tot['par'] / tot['trp'] * 100:.1f}%" if tot["trp"] else "")


if __name__ == "__main__":
    main()
