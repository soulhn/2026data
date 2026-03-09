"""hrd_api.py 단위 테스트 — API 응답 파싱 및 폴백 검증"""
import json
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from hrd_api import (
    fetch_attendance_month,
    fetch_course_list,
    fetch_trainee_roster,
    fetch_active_data_realtime,
    get_active_data_with_fallback,
)


# ── 테스트용 API 응답 fixtures ──────────────────────────────────────────


@pytest.fixture
def mock_session():
    return MagicMock()


def _make_response(data):
    """mock session.get().json() 형태 응답 생성"""
    resp = MagicMock()
    resp.json.return_value = {"returnJSON": json.dumps(data)}
    return resp


# ── fetch_course_list ──────────────────────────────────────────────────


class TestFetchCourseList:
    def test_parses_response(self, mock_session):
        courses = [
            {
                "trprId": "AIG00001",
                "trprDegr": "5",
                "trprNm": "데이터 분석 5기",
                "trStaDt": "2026-01-15",
                "trEndDt": "2026-07-15",
                "totFxnum": "30",
                "totParMks": "28",
                "totTrpCnt": "35",
            },
            {
                "trprId": "AIG00001",
                "trprDegr": "1",
                "trprNm": "데이터 분석 1기",
                "trStaDt": "2024-01-15",
                "trEndDt": "2024-07-15",
                "totFxnum": "30",
                "totParMks": "25",
                "totTrpCnt": "32",
            },
        ]
        mock_session.get.return_value = _make_response(courses)

        df = fetch_course_list(mock_session, "KEY", "AIG00001")

        assert set(df.columns) == {
            "TRPR_ID", "TRPR_DEGR", "TRPR_NM", "TR_STA_DT", "TR_END_DT",
            "TOT_FXNUM", "TOT_PAR_MKS", "TOT_TRP_CNT",
        }
        # 종료된 1기는 필터링됨 (today > 2024-07-15)
        assert len(df) == 1
        assert df.iloc[0]["TRPR_DEGR"] == 5

    def test_skips_invalid_degr(self, mock_session):
        courses = [{"trprDegr": "abc", "trEndDt": "2099-12-31"}]
        mock_session.get.return_value = _make_response(courses)
        df = fetch_course_list(mock_session, "KEY", "AIG00001")
        assert df.empty

    def test_skips_zero_degr(self, mock_session):
        courses = [{"trprDegr": "0", "trEndDt": "2099-12-31"}]
        mock_session.get.return_value = _make_response(courses)
        df = fetch_course_list(mock_session, "KEY", "AIG00001")
        assert df.empty


# ── fetch_trainee_roster ───────────────────────────────────────────────


class TestFetchTraineeRoster:
    def test_parses_response(self, mock_session):
        roster = {
            "trneList": [
                {
                    "trneeCstmrId": "T001",
                    "trneeCstmrNm": "홍길동",
                    "trneeSttusNm": "수강중",
                },
                {
                    "trneeCstmrId": "T002",
                    "trneeCstmrNm": "김철수",
                    "trneeSttusNm": "중도탈락",
                },
            ]
        }
        mock_session.get.return_value = _make_response(roster)

        df = fetch_trainee_roster(mock_session, "KEY", "AIG00001", 5)

        assert set(df.columns) == {"TRPR_ID", "TRPR_DEGR", "TRNEE_ID", "TRNEE_NM", "TRNEE_STATUS"}
        assert len(df) == 2
        assert df.iloc[0]["TRNEE_NM"] == "홍길동"

    def test_empty_response(self, mock_session):
        resp = MagicMock()
        resp.json.return_value = {"returnJSON": None}
        mock_session.get.return_value = resp

        df = fetch_trainee_roster(mock_session, "KEY", "AIG00001", 5)
        assert df.empty
        assert "TRNEE_ID" in df.columns


# ── fetch_attendance_month ─────────────────────────────────────────────


class TestFetchAttendanceMonth:
    def test_parses_response_with_clean_time(self, mock_session):
        atab = {
            "atabList": [
                {
                    "trneeCstmrId": "T001",
                    "atendDe": "2026-03-09",
                    "lpsilTime": "0905",
                    "levromTime": "1800",
                    "atendSttusNm": "출석",
                },
                {
                    "trneeCstmrId": "T002",
                    "atendDe": "2026-03-09",
                    "lpsilTime": "0000",
                    "levromTime": None,
                    "atendSttusNm": "결석",
                },
            ]
        }
        mock_session.get.return_value = _make_response(atab)

        df = fetch_attendance_month(mock_session, "KEY", "AIG00001", 5, "202603")

        assert set(df.columns) == {
            "TRPR_ID", "TRPR_DEGR", "TRNEE_ID", "ATEND_DT",
            "IN_TIME", "OUT_TIME", "ATEND_STATUS", "COLLECTED_AT",
        }
        assert len(df) == 2
        # clean_time 적용 확인
        assert df.iloc[0]["IN_TIME"] == "09:05"
        assert df.iloc[0]["OUT_TIME"] == "18:00"
        # 0000 → None
        assert df.iloc[1]["IN_TIME"] is None
        assert df.iloc[1]["OUT_TIME"] is None

    def test_collected_at_populated(self, mock_session):
        atab = {"atabList": [{"trneeCstmrId": "T001", "atendDe": "2026-03-09",
                              "lpsilTime": "0900", "levromTime": "1800", "atendSttusNm": "출석"}]}
        mock_session.get.return_value = _make_response(atab)
        df = fetch_attendance_month(mock_session, "KEY", "AIG00001", 5, "202603")
        assert df.iloc[0]["COLLECTED_AT"] is not None


# ── 병렬 실행 ──────────────────────────────────────────────────────────


class TestParallelExecution:
    @patch("hrd_api.get_retry_session")
    def test_parallel_calls(self, mock_get_session, mock_session):
        """활성 기수 2개 → 명부 2 + 출결 2 = 최소 5 API 호출"""
        courses = [
            {"trprId": "AIG", "trprDegr": "5", "trprNm": "5기",
             "trStaDt": "2026-01-01", "trEndDt": "2026-12-31",
             "totFxnum": "30", "totParMks": "28", "totTrpCnt": "35"},
            {"trprId": "AIG", "trprDegr": "6", "trprNm": "6기",
             "trStaDt": "2026-03-01", "trEndDt": "2026-12-31",
             "totFxnum": "30", "totParMks": "25", "totTrpCnt": "30"},
        ]
        roster = {"trneList": [{"trneeCstmrId": "T1", "trneeCstmrNm": "A", "trneeSttusNm": "수강중"}]}
        atab = {"atabList": []}

        call_count = [0]
        def mock_get(*args, **kwargs):
            call_count[0] += 1
            params = kwargs.get("params", {})
            if "atendMo" in params:
                return _make_response(atab)
            if "srchTrprDegr" in params:
                return _make_response(roster)
            return _make_response(courses)

        mock_session.get.side_effect = mock_get
        mock_get_session.return_value = mock_session

        c, t, l = fetch_active_data_realtime("KEY", "AIG")

        # 1 (course list) + 2 (roster) + 2 (attendance) = 5
        assert call_count[0] == 5
        assert len(c) == 2
        assert len(t) == 2  # 2 rosters


# ── 폴백 ──────────────────────────────────────────────────────────────


class TestFallback:
    @patch.dict("os.environ", {}, clear=True)
    @patch("hrd_api._get_active_data_from_db")
    def test_fallback_without_api_key(self, mock_db):
        mock_db.return_value = (pd.DataFrame({"A": [1]}), pd.DataFrame(), pd.DataFrame())
        c, t, l, source = get_active_data_with_fallback()
        assert source == "DB"
        mock_db.assert_called_once()

    @patch.dict("os.environ", {"HRD_API_KEY": "key", "HANWHA_COURSE_ID": "cid"})
    @patch("hrd_api.fetch_active_data_realtime")
    @patch("hrd_api._get_active_data_from_db")
    def test_fallback_on_api_failure(self, mock_db, mock_api):
        mock_api.side_effect = Exception("API timeout")
        mock_db.return_value = (pd.DataFrame({"A": [1]}), pd.DataFrame(), pd.DataFrame())

        c, t, l, source = get_active_data_with_fallback()

        assert source == "DB"
        mock_db.assert_called_once()

    @patch.dict("os.environ", {"HRD_API_KEY": "key", "HANWHA_COURSE_ID": "cid"})
    @patch("hrd_api.fetch_active_data_realtime")
    def test_api_success(self, mock_api):
        mock_api.return_value = (
            pd.DataFrame({"TRPR_ID": ["A"], "TRPR_DEGR": [1]}),
            pd.DataFrame({"TRNEE_ID": ["T1"]}),
            pd.DataFrame({"ATEND_DT": ["2026-03-09"]}),
        )

        c, t, l, source = get_active_data_with_fallback()

        assert source == "API"
        assert len(c) == 1


# ── 컬럼 호환성 ───────────────────────────────────────────────────────


class TestColumnCompatibility:
    """API 반환 컬럼 == DB 반환 컬럼 이름 일치 확인"""

    def test_course_columns(self, mock_session):
        courses = [{"trprId": "A", "trprDegr": "1", "trprNm": "X",
                     "trStaDt": "2026-01-01", "trEndDt": "2099-12-31",
                     "totFxnum": "30", "totParMks": "28", "totTrpCnt": "35"}]
        mock_session.get.return_value = _make_response(courses)
        df = fetch_course_list(mock_session, "KEY", "A")
        expected = {"TRPR_ID", "TRPR_DEGR", "TRPR_NM", "TR_STA_DT", "TR_END_DT",
                    "TOT_FXNUM", "TOT_PAR_MKS", "TOT_TRP_CNT"}
        assert set(df.columns) == expected

    def test_trainee_columns(self, mock_session):
        roster = {"trneList": [{"trneeCstmrId": "T1", "trneeCstmrNm": "A", "trneeSttusNm": "수강중"}]}
        mock_session.get.return_value = _make_response(roster)
        df = fetch_trainee_roster(mock_session, "KEY", "A", 1)
        expected = {"TRPR_ID", "TRPR_DEGR", "TRNEE_ID", "TRNEE_NM", "TRNEE_STATUS"}
        assert set(df.columns) == expected

    def test_attendance_columns(self, mock_session):
        atab = {"atabList": [{"trneeCstmrId": "T1", "atendDe": "2026-03-09",
                              "lpsilTime": "0900", "levromTime": "1800", "atendSttusNm": "출석"}]}
        mock_session.get.return_value = _make_response(atab)
        df = fetch_attendance_month(mock_session, "KEY", "A", 1, "202603")
        expected = {"TRPR_ID", "TRPR_DEGR", "TRNEE_ID", "ATEND_DT",
                    "IN_TIME", "OUT_TIME", "ATEND_STATUS", "COLLECTED_AT"}
        assert set(df.columns) == expected
