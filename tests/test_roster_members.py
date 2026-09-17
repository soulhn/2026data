"""kpi_etl 명부 사람 스냅샷 — 참석 판정·첫 참석일·승인 감지·상태 변화·명부 이탈 (인메모리 SQLite)"""
from datetime import date, datetime

import pandas as pd
import pytest

import init_db
import kpi_etl
import utils
from kpi_etl import (
    attendance_months, first_attendance, is_attended, upsert_roster_members,
)


@pytest.fixture
def db(monkeypatch, mock_db_connection):
    monkeypatch.setattr(kpi_etl, "adapt_query", utils.adapt_query)
    init_db.init_all_tables(include_market=False)
    return mock_db_connection


def _roster(*people, cid="A", degr=3):
    return pd.DataFrame([{"TRPR_ID": cid, "TRPR_DEGR": degr, "TRNEE_ID": tid, "TRNEE_NM": nm, "TRNEE_STATUS": st}
                         for tid, nm, st in people])


def _att(rows, cid="A", degr=3):
    return pd.DataFrame([{"TRPR_ID": cid, "TRPR_DEGR": degr, "TRNEE_ID": tid, "ATEND_DT": d, "IN_TIME": it,
                          "OUT_TIME": None, "ATEND_STATUS": st, "COLLECTED_AT": None} for tid, d, it, st in rows])


class TestAttendanceRule:
    @pytest.mark.parametrize("status,in_time,expected", [
        ("결석", "08:43", True),      # 퇴실 전: 상태는 결석이지만 입실 시간이 있으면 참석 (실측 2026-09-15)
        ("결석", None, False),
        ("결석", "", False),
        ("출석", None, True),
        ("지각", None, True),
        ("휴가", None, False),
        ("질병/입원", None, False),
        ("100분의50미만출석", None, True),
    ])
    def test_is_attended(self, status, in_time, expected):
        assert is_attended(status, in_time) is expected

    def test_first_attendance_picks_earliest_attended_day(self):
        att = _att([
            ("t1", "20260915", "09:02", "결석"),      # 개강일 입실중
            ("t1", "20260916", "08:50", "출석"),
            ("t2", "20260915", None, "결석"),        # 개강일 안 옴
            ("t2", "20260916", "13:10", "지각"),
            ("t3", "20260915", None, "휴가"),        # 참석 아님
        ])
        out = first_attendance(att).set_index("TRNEE_ID")
        assert out.loc["t1", "FIRST_ATTEND_DT"] == "20260915" and out.loc["t1", "FIRST_IN_TIME"] == "09:02"
        assert out.loc["t2", "FIRST_ATTEND_DT"] == "20260916"
        assert "t3" not in out.index

    def test_first_attendance_records_day1_status(self):
        """개강일 출결 행이 있으면 상태(결석 포함)를 남긴다 — 참석 기록 채움률의 분자. 개강일 행이 없으면 None."""
        att = _att([("t1", "20260915", "09:02", "결석"),      # 입실 있음 → 참석, 개강일 기록 있음
                    ("t2", "20260915", None, "결석"),         # 결석 → 참석 아님, 기록은 있음
                    ("t3", "20260916", "08:50", "출석")])     # 개강 다음 날부터 → 기록 없음
        out = first_attendance(att, {("A", 3): "2026-09-15"}).set_index("TRNEE_ID")
        assert out.loc["t1", "DAY1_STATUS"] == "결석" and out.loc["t1", "FIRST_ATTEND_DT"] == "20260915"
        assert out.loc["t2", "DAY1_STATUS"] == "결석" and out.loc["t2", "FIRST_ATTEND_DT"] is None
        assert out.loc["t3", "DAY1_STATUS"] is None and out.loc["t3", "FIRST_ATTEND_DT"] == "20260916"
        assert set(out.index) == {"t1", "t2", "t3"}
        assert first_attendance(att)["DAY1_STATUS"].isna().all()           # 개강일 정보 없으면 전부 None

    def test_attendance_months(self):
        assert attendance_months("2026-09-15", date(2026, 9, 15)) == ["202609"]
        assert attendance_months("2026-08-28", date(2026, 9, 15)) == ["202608", "202609"]   # 개강 달 + 이번 달
        assert attendance_months("2026-07-09", date(2026, 9, 15)) == ["202607", "202609"]   # 개강 달은 항상 포함 (첫 참석일)
        assert attendance_months(None, date(2026, 9, 15)) == ["202609"]


class TestUpsert:
    def _rows(self, conn, sql):
        cur = conn.cursor(); cur.execute(sql); return cur.fetchall()

    def test_join_then_status_change_then_leave(self, db):
        rs = {("A", 3): "2026-09-15"}
        t1 = datetime(2026, 9, 15, 0)
        c = upsert_roster_members(db, _roster(("t1", "홍길동", "훈련중"), ("t2", "김철수", "훈련중")),
                                  first_attendance(_att([("t1", "20260915", "09:02", "결석")])), {("A", 3)}, rs, now=t1)
        assert c == {"joined": 2, "status": 0, "first_attend": 1, "left": 0}
        row = self._rows(db, "SELECT NAME_HASH, NAME_MASKED, FIRST_SEEN_AT, FIRST_ATTEND_DT, FIRST_IN_TIME FROM TB_ROSTER_MEMBER WHERE TRNEE_ID='t1'")[0]
        assert row[0] == kpi_etl.name_hash("홍길동") and row[1] == "홍*동"
        assert row[3] == "20260915" and row[4] == "09:02"
        assert "홍길동" not in str(self._rows(db, "SELECT * FROM TB_ROSTER_MEMBER"))

        # 다음 시간: t2가 첫 참석, t1 상태 변화 없음
        t2 = datetime(2026, 9, 15, 1)
        c = upsert_roster_members(db, _roster(("t1", "홍길동", "훈련중"), ("t2", "김철수", "훈련중")),
                                  first_attendance(_att([("t1", "20260915", "09:02", "결석"), ("t2", "20260915", "10:30", "결석")])),
                                  {("A", 3)}, rs, now=t2)
        assert c == {"joined": 0, "status": 0, "first_attend": 1, "left": 0}

        # 다음 날: t1 중도탈락, t2는 명부에서 사라짐, t3 신규 승인(지연 등록)
        t3 = datetime(2026, 9, 16, 0)
        c = upsert_roster_members(db, _roster(("t1", "홍길동", "중도탈락"), ("t3", "이영희", "훈련중")),
                                  first_attendance(pd.DataFrame()), {("A", 3)}, rs, now=t3)
        assert c == {"joined": 1, "status": 1, "first_attend": 0, "left": 1}
        assert self._rows(db, "SELECT STATUS, STATUS_CHANGED_AT FROM TB_ROSTER_MEMBER WHERE TRNEE_ID='t1'")[0][0] == "중도탈락"
        assert self._rows(db, "SELECT GONE_AT FROM TB_ROSTER_MEMBER WHERE TRNEE_ID='t2'")[0][0] is not None
        assert str(self._rows(db, "SELECT FIRST_SEEN_AT FROM TB_ROSTER_MEMBER WHERE TRNEE_ID='t3'")[0][0])[:19] == "2026-09-16 00:00:00"
        events = self._rows(db, "SELECT TRNEE_ID, EVENT, OLD_VALUE, NEW_VALUE FROM TB_ROSTER_MEMBER_LOG ORDER BY DETECTED_AT, TRNEE_ID, EVENT")
        assert ("t1", "STATUS", "훈련중", "중도탈락") in events
        assert ("t2", "LEFT", "훈련중", None) in events
        assert ("t3", "JOINED", None, "훈련중") in events

    def test_left_only_judged_for_fetched_rounds(self, db):
        rs = {("A", 3): "2026-09-15", ("B", 1): "2026-07-09"}
        upsert_roster_members(db, pd.concat([_roster(("t1", "홍길동", "훈련중")), _roster(("u1", "박민수", "훈련중"), cid="B", degr=1)]),
                              pd.DataFrame(), {("A", 3), ("B", 1)}, rs, now=datetime(2026, 9, 15, 0))
        # B 회차 명부 조회가 실패한 시간: B의 사람은 LEFT로 찍히면 안 된다
        c = upsert_roster_members(db, _roster(("t1", "홍길동", "훈련중")), pd.DataFrame(), {("A", 3)}, rs, now=datetime(2026, 9, 15, 1))
        assert c["left"] == 0
        assert self._rows(db, "SELECT GONE_AT FROM TB_ROSTER_MEMBER WHERE TRNEE_ID='u1'")[0][0] is None

    def test_returning_member_clears_gone(self, db):
        rs = {("A", 3): "2026-09-15"}
        upsert_roster_members(db, _roster(("t1", "홍길동", "훈련중")), pd.DataFrame(), {("A", 3)}, rs, now=datetime(2026, 9, 15, 0))
        upsert_roster_members(db, _roster(), pd.DataFrame(), {("A", 3)}, rs, now=datetime(2026, 9, 15, 1))
        assert self._rows(db, "SELECT GONE_AT FROM TB_ROSTER_MEMBER WHERE TRNEE_ID='t1'")[0][0] is not None
        upsert_roster_members(db, _roster(("t1", "홍길동", "훈련중")), pd.DataFrame(), {("A", 3)}, rs, now=datetime(2026, 9, 15, 2))
        assert self._rows(db, "SELECT GONE_AT FROM TB_ROSTER_MEMBER WHERE TRNEE_ID='t1'")[0][0] is None

    def test_first_attend_never_overwritten(self, db):
        rs = {("A", 3): "2026-09-15"}
        upsert_roster_members(db, _roster(("t1", "홍길동", "훈련중")),
                              first_attendance(_att([("t1", "20260915", "09:02", "결석")])), {("A", 3)}, rs, now=datetime(2026, 9, 15, 0))
        upsert_roster_members(db, _roster(("t1", "홍길동", "훈련중")),
                              first_attendance(_att([("t1", "20260916", "08:00", "출석")])), {("A", 3)}, rs, now=datetime(2026, 9, 16, 0))
        assert self._rows(db, "SELECT FIRST_ATTEND_DT FROM TB_ROSTER_MEMBER")[0][0] == "20260915"
