"""scripts/tf_section7.py — 운영TF 구간 7 합계: 기준·현재 구분과 분모·분자."""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "scripts"))

import config
import tf_section7 as tf
from notion_registry_publish import OPS_ADDED_COL, OPS_CONFIRM_COL, OPS_EARLY_COL, OPS_OPEN_COL


def _row(cohort, start, applied, day1, opened=None, early=None, added=None, conf=None, api_conf=None):
    return {"기수": cohort, "개강일": start, "API 신청인원": applied, tf.DAY1_COL: day1, "확정 신고(API)": api_conf,
            OPS_OPEN_COL: opened, OPS_EARLY_COL: early, OPS_ADDED_COL: added, OPS_CONFIRM_COL: conf}


def test_base_cohorts_are_the_tf_twenty():
    assert len(config.NOTION_KPI_BASE_COHORTS) == len(set(config.NOTION_KPI_BASE_COHORTS)) == 20
    assert "AIO3" not in config.NOTION_KPI_BASE_COHORTS and {"SKN22", "SKN25"} <= set(config.NOTION_KPI_BASE_COHORTS)


def test_group_of():
    assert tf.group_of(_row("SKN37", "2026-09-04", 23, 18), "2026-10-02") == "기준"
    assert tf.group_of(_row("AIO3", "2026-09-15", 17, 13), "2026-10-02") == "현재"      # 기준 기수 밖, 개강함
    assert tf.group_of(_row("MLO3", "2026-10-07", 20, None), "2026-10-02") == "예정"


def test_summarize_skips_missing_values_per_metric():
    rows = [_row("SKN37", "2026-09-04", 23, 18, opened=21, early=1, added=2, conf=22, api_conf=22),
            _row("AIO3", "2026-09-15", 17, 13, opened=14, early=1, added=1, conf=14, api_conf=14),
            _row("MLO3", "2026-10-07", 20, None)]                                        # 개강 전 — 어느 지표에도 안 들어간다
    t = tf.summarize(rows)
    assert (t["trp"], t["day1"]) == (40, 31)                                             # 참석률 분모는 수강신청(취소자 포함)
    assert (t["open"], t["early"], t["add"], t["conf"], t["n_ops"]) == (35, 2, 3, 36, 2)  # 개강 − 이탈 + 추가 = 확정
    assert t["open"] - t["early"] + t["add"] == t["conf"]
    assert (t["api_trp"], t["api_conf"]) == (40, 36)


def test_round_half_up_matches_sheet_values():
    assert str(tf._r1(433, 20)) == "21.7" and str(tf._r1(61, 20)) == "3.1" and str(tf._r1(17, 20)) == "0.9"   # 21.65 · 3.05 · 0.85
    assert tf._pct(433, 477) == "90.8%" and tf._pct(393, 534) == "73.6%" and tf._pct(1, 0) == "-"
