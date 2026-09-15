"""kpi_etl.py — 회차 스냅샷 변화 감지·저장 테스트 (인메모리 SQLite)"""
from datetime import datetime
from unittest.mock import patch

import pandas as pd
import pytest

import init_db
import kpi_etl
import utils
from kpi_etl import SNAPSHOT_COLUMNS, VALUE_COLUMNS, diff_snapshot, fetch_round_snapshots, save_snapshots


def _row(cid="A", degr=1, **kw):
    base = {"TRPR_ID": cid, "TRPR_DEGR": degr, "TR_STA_DT": "2026-09-15", "TR_END_DT": "2027-03-12",
            "TOT_FXNUM": 30, "TOT_TRP_CNT": 13, "TOT_PAR_MKS": 9, "FINI_CNT": 0,
            "ROSTER_CNT": 9, "ACTIVE_CNT": 9, "DROPOUT_CNT": 0, "PARTIAL_FINI_CNT": 0, "EARLY_EMPL_CNT": 0}
    base.update(kw)
    return base


@pytest.fixture
def snap_db(monkeypatch, mock_db_connection):
    monkeypatch.setattr(kpi_etl, "adapt_query", utils.adapt_query)
    init_db.init_all_tables(include_market=False)
    return mock_db_connection


def _count(conn):
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) AS cnt FROM TB_COURSE_SNAPSHOT")
    return cur.fetchone()[0]


class TestDiff:
    def test_first_snapshot(self):
        assert diff_snapshot(_row(), None, datetime(2026, 9, 15, 1)) == "first"

    def test_unchanged_same_day_is_skipped(self):
        last = {"SNAP_AT": datetime(2026, 9, 15, 0), **{c: _row()[c] for c in VALUE_COLUMNS}}
        assert diff_snapshot(_row(), last, datetime(2026, 9, 15, 3)) is None

    def test_unchanged_next_day_is_daily(self):
        last = {"SNAP_AT": datetime(2026, 9, 14, 8), **{c: _row()[c] for c in VALUE_COLUMNS}}
        assert diff_snapshot(_row(), last, datetime(2026, 9, 15, 0)) == "daily"

    def test_changed_fields_listed(self):
        last = {"SNAP_AT": datetime(2026, 9, 15, 0), **{c: _row()[c] for c in VALUE_COLUMNS}}
        assert diff_snapshot(_row(TOT_TRP_CNT=15, TOT_PAR_MKS=11), last, datetime(2026, 9, 15, 2)) == "TOT_TRP_CNT,TOT_PAR_MKS"

    def test_nan_roster_equals_none(self):
        """명부를 못 읽은 회차(NaN)는 직전 None과 같은 값으로 봐서 매시간 저장되지 않아야 한다."""
        last = {"SNAP_AT": datetime(2026, 9, 15, 0), **{c: _row()[c] for c in VALUE_COLUMNS}}
        last.update({"ROSTER_CNT": None, "ACTIVE_CNT": None, "DROPOUT_CNT": None, "PARTIAL_FINI_CNT": None, "EARLY_EMPL_CNT": None})
        cur = _row(ROSTER_CNT=float("nan"), ACTIVE_CNT=float("nan"), DROPOUT_CNT=float("nan"),
                   PARTIAL_FINI_CNT=float("nan"), EARLY_EMPL_CNT=float("nan"))
        assert diff_snapshot(cur, last, datetime(2026, 9, 15, 2)) is None


class TestSave:
    def test_first_then_skip_then_change(self, snap_db):
        df = pd.DataFrame([_row(), _row(cid="B", degr=2, TOT_TRP_CNT=20, TOT_PAR_MKS=14)])
        assert save_snapshots(df, snap_db, now=datetime(2026, 9, 15, 1)) == (2, 0)
        assert save_snapshots(df, snap_db, now=datetime(2026, 9, 15, 2)) == (0, 2)      # 변화 없음
        df2 = pd.DataFrame([_row(TOT_TRP_CNT=14), _row(cid="B", degr=2, TOT_TRP_CNT=20, TOT_PAR_MKS=14)])
        assert save_snapshots(df2, snap_db, now=datetime(2026, 9, 15, 3)) == (1, 1)     # A만 변화
        assert save_snapshots(df2, snap_db, now=datetime(2026, 9, 16, 0)) == (2, 0)     # 다음 날 생존 신호
        assert _count(snap_db) == 5
        cur = snap_db.cursor()
        cur.execute("SELECT CHANGED FROM TB_COURSE_SNAPSHOT WHERE TRPR_ID='A' ORDER BY SNAP_AT")
        assert [r[0] for r in cur.fetchall()] == ["first", "TOT_TRP_CNT", "daily"]

    def test_last_snapshot_is_latest_row(self, snap_db):
        df = pd.DataFrame([_row()])
        save_snapshots(df, snap_db, now=datetime(2026, 9, 15, 1))
        save_snapshots(pd.DataFrame([_row(TOT_TRP_CNT=14)]), snap_db, now=datetime(2026, 9, 15, 2))
        last = kpi_etl.load_last_snapshots(snap_db)[("A", 1)]
        assert last["TOT_TRP_CNT"] == 14 and last["SNAP_AT"] == datetime(2026, 9, 15, 2)


class TestFetch:
    @patch("kpi_etl.fetch_all_roster_counts")
    @patch("kpi_etl.fetch_all_course_history")
    def test_merges_history_and_roster(self, mock_hist, mock_roster):
        mock_hist.return_value = (pd.DataFrame([
            {"TRPR_ID": "A", "TRPR_DEGR": "0", "TRPR_NM": "hdr", "TR_STA_DT": "", "TR_END_DT": "",
             "TOT_FXNUM": "30", "TOT_TRP_CNT": "0", "TOT_PAR_MKS": None, "FINI_CNT": "0", "INST_INO": "1"},
            {"TRPR_ID": "A", "TRPR_DEGR": "3", "TRPR_NM": "x", "TR_STA_DT": "2026-09-15", "TR_END_DT": "2027-03-12",
             "TOT_FXNUM": "30", "TOT_TRP_CNT": "13", "TOT_PAR_MKS": "9", "FINI_CNT": "0", "INST_INO": "1"},
            {"TRPR_ID": "A", "TRPR_DEGR": "4", "TRPR_NM": "x", "TR_STA_DT": "2026-10-14", "TR_END_DT": "2027-04-06",
             "TOT_FXNUM": "30", "TOT_TRP_CNT": "0", "TOT_PAR_MKS": None, "FINI_CNT": "0", "INST_INO": "1"},
        ]), None)
        mock_roster.return_value = (pd.DataFrame([{
            "TRPR_ID": "A", "TRPR_DEGR": 3, "DROPOUT_CNT": 0, "PARTIAL_FINI_CNT": 0, "EARLY_EMPL_CNT": 0,
            "ROSTER_CNT": 9, "ACTIVE_CNT": 9}]), None)

        df, err = fetch_round_snapshots([("k", "A")])

        assert err is None and list(df.columns) == SNAPSHOT_COLUMNS
        assert df["TRPR_DEGR"].tolist() == [3, 4]                       # 회차 0 제외
        mock_roster.assert_called_once_with([("k", "A")], [("A", 3)])   # 승인 인원 0인 4회차는 명부 호출 안 함
        r3 = df[df["TRPR_DEGR"] == 3].iloc[0]
        assert r3["ROSTER_CNT"] == 9 and r3["TOT_PAR_MKS"] == 9
        assert pd.isna(df[df["TRPR_DEGR"] == 4].iloc[0]["ROSTER_CNT"])

    @patch("kpi_etl.fetch_all_roster_counts")
    @patch("kpi_etl.fetch_all_course_history")
    def test_errors_are_joined(self, mock_hist, mock_roster):
        mock_hist.return_value = (pd.DataFrame(columns=["TRPR_ID", "TRPR_DEGR", "TRPR_NM", "TR_STA_DT", "TR_END_DT",
                                                        "TOT_FXNUM", "TOT_TRP_CNT", "TOT_PAR_MKS", "FINI_CNT", "INST_INO"]), "B → timeout")
        mock_roster.return_value = (pd.DataFrame(), None)
        df, err = fetch_round_snapshots([("k", "A")])
        assert df.empty and err == "B → timeout"
