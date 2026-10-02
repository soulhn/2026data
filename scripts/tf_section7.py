"""운영TF 구간 7 측정값 출력 — 구글시트 「7_통합」 황설현 행(25~29)과 노션 「일별 액션 측정」에 넣을 분모·분자.

2026-10-02 시트 확정 정의. 기준 = `config.NOTION_KPI_BASE_COHORTS`(운영TF 20개 기수), 현재 = 그 뒤에 개강한 기수.
  · 기수당 확정자 신고 인원      운영현황표 확정자신고 합 ÷ 기수 수
  · HRD 등록 대비 개강 참석률   분모 HRD-Net 수강신청(totTrpCnt, 취소자 포함) / 분자 HRD-Net 출결 개강일 입실 (기수 표 「개강 참석률(API, %)」과 같은 값)
  · 개강 대비 확정 비율(책임 수치)  분모 운영현황표 개강인원(오프라인) / 분자 운영현황표 확정자신고(= 개강인원 − 초기이탈 + 추가인원). 100% 초과 가능
      액션 지표: 기수당 초기이탈 인원(낮을수록 좋음) · 기수당 추가인원. 운영현황표는 notion_registry_publish.load_ops_counts로 읽기만
  · 개강 참석 기록 채움률(참고)   분모 노션 최종결과 HRD등록·합격자등록 / 분자 그중 개강일 출결 행이 있는 사람. 자동 수집 이후 측정 종료(프로세스 도입)
  · 확정자 신고율(API, 참고)     분모 HRD-Net 수강신청 / 분자 확정 신고(totParMks, 개강 + 7일 이후 첫 스냅샷 값으로 고정)
실행: python scripts/tf_section7.py   (읽기만, 노션에 쓰지 않는다)
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from datetime import datetime, timedelta, timezone  # noqa: E402
from decimal import ROUND_HALF_UP, Decimal  # noqa: E402

import config  # noqa: E402
from notion_registry_publish import (  # noqa: E402
    OPS_ADDED_COL, OPS_CONFIRM_COL, OPS_EARLY_COL, OPS_OPEN_COL, build_cohort_rows, load_ops_counts,
)
from utils import _clean_secret, load_data  # noqa: E402

DAY1_COL = "개강일 출석 인원(API)"


def group_of(row, today):
    """기준(운영TF 기준 기수) / 현재(기준 이후 개강) / 예정(아직 개강 전)."""
    if row["기수"] in config.NOTION_KPI_BASE_COHORTS:
        return "기준"
    return "현재" if str(row["개강일"])[:10] < today else "예정"


def summarize(rows):
    """기수 행 묶음 → 합계. 값이 없는 기수는 그 지표의 분모·분자 양쪽에서 빠진다."""
    t = {"trp": 0, "day1": 0, "open": 0, "early": 0, "add": 0, "conf": 0, "n_ops": 0, "api_trp": 0, "api_conf": 0}
    for r in rows:
        applied, day1 = r.get("API 신청인원"), r.get(DAY1_COL)
        if applied and day1 is not None:
            t["trp"] += applied; t["day1"] += day1   # noqa: E702
        opened, conf = r.get(OPS_OPEN_COL), r.get(OPS_CONFIRM_COL)
        if opened and conf is not None:
            t["open"] += opened; t["conf"] += conf; t["n_ops"] += 1   # noqa: E702
            t["early"] += r.get(OPS_EARLY_COL) or 0; t["add"] += r.get(OPS_ADDED_COL) or 0   # noqa: E702
        if applied and r.get("확정 신고(API)") is not None:
            t["api_trp"] += applied; t["api_conf"] += r["확정 신고(API)"]   # noqa: E702
    return t


def fill_counts(cohorts):
    """채움률(참고) — 노션 등록 상태(HRD등록·합격자등록)인 사람 수와 그중 개강일 출결 행이 있는 사람 수."""
    apps = load_data("SELECT COHORT, NAME_HASH FROM TB_APPLICANT WHERE COHORT IS NOT NULL AND STATUS IN ('HRD등록', '합격자등록')")
    mem = load_data("SELECT TRPR_ID, TRPR_DEGR, NAME_HASH, DAY1_STATUS FROM TB_ROSTER_MEMBER")
    mem["COHORT"] = [f"{config.COURSE_SHORT_NAMES.get(t)}{int(d)}" for t, d in zip(mem.TRPR_ID, mem.TRPR_DEGR)]
    m = apps.merge(mem.drop_duplicates(["COHORT", "NAME_HASH"]), on=["COHORT", "NAME_HASH"], how="left")
    m = m[m.COHORT.isin(cohorts)]
    return len(m), int(m.DAY1_STATUS.notna().sum())


def _r1(a, b):
    """a ÷ b를 소수 첫째 자리로 — 사사오입(21.65 → 21.7). 부동소수 반올림은 21.6이 되어 시트 값과 어긋난다."""
    return (Decimal(a) / Decimal(b)).quantize(Decimal("0.1"), rounding=ROUND_HALF_UP)


def _pct(a, b):
    return f"{_r1(a * 100, b)}%" if b else "-"


def _print_summary(title, t, fill):
    print(f"\n[{title}]")
    if t["n_ops"]:
        n = t["n_ops"]
        print(f"  기수당 확정자 신고 인원      {_r1(t['conf'], n)}명 ({t['conf']}/{n}기수)")
    print(f"  HRD 등록 대비 개강 참석률   분모 {t['trp']} / 분자 {t['day1']}  → {_pct(t['day1'], t['trp'])}   (API)")
    if t["n_ops"]:
        print(f"  개강 대비 확정 비율          분모 {t['open']} / 분자 {t['conf']}  → {_pct(t['conf'], t['open'])}   (책임 수치 · 운영현황표 수기 · 100% 초과 가능)")
        print(f"    ㄴ 기수당 초기이탈 인원    {_r1(t['early'], n)}명 ({t['early']}명/{n}기수)   비율 {_pct(t['early'], t['open'])}   (낮을수록 좋음)")
        print(f"    ㄴ 기수당 추가인원         {_r1(t['add'], n)}명 ({t['add']}명/{n}기수)   비율 {_pct(t['add'], t['open'])}")
        print(f"    ㄴ 기수당 개강인원         {_r1(t['open'], n)}명 ({t['open']}/{n}기수)")
    else:
        print("  개강 대비 확정 비율          운영현황표 값 없음 (확정자 신고 전이거나 NOTION_TOKEN 확인)")
    print(f"  개강 참석 기록 채움률(참고)  분모 {fill[0]} / 분자 {fill[1]}  → {_pct(fill[1], fill[0])}")
    print(f"  확정자 신고율(API, 참고)    분모 {t['api_trp']} / 분자 {t['api_conf']}  → {_pct(t['api_conf'], t['api_trp'])}")


def main():
    today = datetime.now(timezone(timedelta(hours=9))).strftime("%Y-%m-%d")
    token = _clean_secret(os.getenv("NOTION_TOKEN"))
    ops = load_ops_counts(token) if token else {}   # 운영현황표 수기(개강인원·초기이탈·추가인원·확정자신고), 읽기만
    rows = sorted(build_cohort_rows(today=today, ops=ops), key=lambda r: str(r["개강일"]))

    print(f'{"구분":4} {"기수":6} {"개강":10} {"API신청":>6} {"개강일출석":>6} {"참석률":>6} | {"개강인원":>4} {"이탈":>3} {"추가":>3} {"확정":>4} {"대비확정":>6}')
    groups = {"기준": [], "현재": [], "예정": []}
    for r in rows:
        g = group_of(r, today)
        groups[g].append(r)
        show = lambda v: "-" if v is None else v   # noqa: E731
        applied, day1, opened, conf = r.get("API 신청인원"), r.get(DAY1_COL), r.get(OPS_OPEN_COL), r.get(OPS_CONFIRM_COL)
        print(f"{g:4} {r['기수']:6} {str(r['개강일'])[:10]:10} {show(applied):>7} {show(day1):>7} {_pct(day1, applied) if day1 is not None else '-':>7} | "
              f"{show(opened):>6} {show(r.get(OPS_EARLY_COL)):>4} {show(r.get(OPS_ADDED_COL)):>4} {show(conf):>5} {_pct(conf, opened) if conf is not None else '-':>7}")

    print(f"\n측정일 {today}")
    for name in ("기준", "현재"):
        g = groups[name]
        title = f"{name} · {len(g)}개 기수" + (" (운영TF 기준 기수)" if name == "기준" else " (기준 이후 개강: " + ", ".join(r["기수"] for r in g) + ")")
        _print_summary(title, summarize(g), fill_counts([r["기수"] for r in g]))


if __name__ == "__main__":
    main()
