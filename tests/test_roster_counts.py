"""hrd_api 회차별 명부 상태 집계(이탈 인원·80%이상수료) 테스트"""
from unittest.mock import patch

import pandas as pd
import pytest

from hrd_api import (
    ROSTER_COUNT_COLUMNS,
    fetch_all_roster_counts,
    summarize_roster_status,
)


def _raise(exc):
    raise exc


def _roster(cid, degr, statuses):
    return pd.DataFrame({
        "TRPR_ID": [cid] * len(statuses),
        "TRPR_DEGR": [degr] * len(statuses),
        "TRNEE_ID": [f"T{i}" for i in range(len(statuses))],
        "TRNEE_NM": ["홍*동"] * len(statuses),
        "TRNEE_STATUS": statuses,
    })


class TestSummarizeRosterStatus:
    def test_counts_dropout_and_partial_completion(self):
        # 실측 상태값: 훈련중 / 중도탈락 / 정상수료 / 80%이상수료 (+ 제적·조기취업 가능)
        roster = pd.concat([
            _roster("H", 25, ["정상수료"] * 23 + ["중도탈락"] * 6 + ["80%이상수료"]),
            _roster("A", "1", ["훈련중"] * 23 + ["중도탈락", "제적", "조기취업"]),
        ])
        out = summarize_roster_status(roster).set_index(["TRPR_ID", "TRPR_DEGR"])
        assert list(out.reset_index().columns) == ROSTER_COUNT_COLUMNS
        assert out.loc[("H", 25), "DROPOUT_CNT"] == 6
        assert out.loc[("H", 25), "PARTIAL_FINI_CNT"] == 1
        assert out.loc[("A", 1), "DROPOUT_CNT"] == 2          # 중도탈락 + 제적, 조기취업은 이탈 아님
        assert out.loc[("A", 1), "PARTIAL_FINI_CNT"] == 0
        assert out["DROPOUT_CNT"].dtype.kind == "i"

    def test_partial_completion_is_not_counted_as_dropout(self):
        out = summarize_roster_status(_roster("H", 1, ["80%이상수료"] * 3))
        assert out.iloc[0]["DROPOUT_CNT"] == 0 and out.iloc[0]["PARTIAL_FINI_CNT"] == 3

    def test_empty(self):
        assert list(summarize_roster_status(None).columns) == ROSTER_COUNT_COLUMNS
        assert summarize_roster_status(pd.DataFrame()).empty


class TestFetchAllRosterCounts:
    @patch("hrd_api.get_retry_session")
    @patch("hrd_api.fetch_trainee_roster")
    def test_uses_matching_key_per_course_and_skips_unknown(self, mock_fetch, _sess):
        mock_fetch.side_effect = lambda s, key, cid, degr: _roster(cid, degr, ["훈련중", "중도탈락"])
        pairs = [("hanwha-key", "H"), ("encore-key", "A")]
        counts, error = fetch_all_roster_counts(pairs, [("H", 25), ("A", "1"), ("UNKNOWN", 1)])

        assert error is None
        assert len(counts) == 2                              # UNKNOWN(키 없음)은 호출조차 안 함
        called = {(c.args[1], c.args[2]) for c in mock_fetch.call_args_list}
        assert called == {("hanwha-key", "H"), ("encore-key", "A")}
        assert counts.set_index(["TRPR_ID", "TRPR_DEGR"]).loc[("A", 1), "DROPOUT_CNT"] == 1

    @patch("hrd_api.get_retry_session")
    @patch("hrd_api.fetch_trainee_roster")
    def test_partial_failure_keeps_survivors_and_reports(self, mock_fetch, _sess):
        mock_fetch.side_effect = lambda s, key, cid, degr: (
            _raise(Exception("명부 오류")) if degr == 2 else _roster(cid, degr, ["중도탈락"])
        )
        counts, error = fetch_all_roster_counts([("k", "A")], [("A", 1), ("A", 2)])
        assert counts["TRPR_DEGR"].tolist() == [1]
        assert error and "A 2회차" in error and "명부 오류" in error

    @patch("hrd_api.get_retry_session")
    @patch("hrd_api.fetch_trainee_roster")
    def test_all_failed_does_not_raise(self, mock_fetch, _sess):
        """보조 컬럼이라 전부 실패해도 퍼널 페이지는 떠야 한다."""
        mock_fetch.side_effect = Exception("down")
        counts, error = fetch_all_roster_counts([("k", "A")], [("A", 1)])
        assert counts.empty and list(counts.columns) == ROSTER_COUNT_COLUMNS
        assert error

    def test_empty_inputs(self):
        counts, error = fetch_all_roster_counts([], [("A", 1)])
        assert counts.empty and error is None
        counts, error = fetch_all_roster_counts([("k", "A")], [])
        assert counts.empty and error is None

    @patch("hrd_api.get_retry_session")
    @patch("hrd_api.fetch_trainee_roster")
    def test_deadline_exceeded_reports_pending(self, mock_fetch, _sess):
        import time

        def slow(s, key, cid, degr):
            time.sleep(0.5)
            return _roster(cid, degr, ["훈련중"])

        mock_fetch.side_effect = slow
        counts, error = fetch_all_roster_counts([("k", "A")], [("A", 1)], deadline=0.05)
        assert counts.empty
        assert error and "초과" in error


@pytest.mark.parametrize("statuses,expected", [
    (["정상수료", "80%이상수료", "중도탈락"], (1, 1)),
    (["훈련중"] * 4, (0, 0)),
])
def test_dropout_partial_pairs(statuses, expected):
    out = summarize_roster_status(_roster("X", 1, statuses)).iloc[0]
    assert (out["DROPOUT_CNT"], out["PARTIAL_FINI_CNT"]) == expected
