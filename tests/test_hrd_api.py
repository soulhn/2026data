"""hrd_api.py 단위 테스트 — API 응답 파싱 및 폴백 검증"""
import json
import time
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from hrd_api import (
    fetch_all_institutions,
    fetch_attendance_month,
    fetch_course_list,
    fetch_trainee_roster,
    fetch_active_data_realtime,
    get_active_data_with_fallback,
    get_institutions,
)


def _raise(exc):
    """람다 안에서 예외를 던지기 위한 헬퍼."""
    raise exc


def _course_frames(course_id):
    """과정 ID를 식별할 수 있는 (courses, trainees, logs) 3종 DataFrame."""
    return (
        pd.DataFrame({"TRPR_ID": [course_id], "TRPR_DEGR": [1]}),
        pd.DataFrame({"TRPR_ID": [course_id], "TRNEE_ID": ["T1"]}),
        pd.DataFrame({"TRPR_ID": [course_id], "ATEND_DT": ["20260715"]}),
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
                "trEndDt": "2099-12-31",
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

    # clear=True 필수: 실제 .env의 ENCORE_* 가 새어들어오면 기관 쌍이 늘어 결과가 달라짐
    @patch.dict("os.environ", {"HRD_API_KEY": "key", "HANWHA_COURSE_ID": "cid"}, clear=True)
    @patch("hrd_api.fetch_active_data_realtime")
    @patch("hrd_api._get_active_data_from_db")
    def test_fallback_on_api_failure(self, mock_db, mock_api):
        mock_api.side_effect = Exception("API timeout")
        mock_db.return_value = (pd.DataFrame({"A": [1]}), pd.DataFrame(), pd.DataFrame())

        c, t, l, source = get_active_data_with_fallback()

        # "DB"(키 미설정)와 구분해야 페이지가 "운영 중인 과정 없음"으로 오안내하지 않는다
        assert source == "DB_FALLBACK"
        mock_db.assert_called_once()

    @patch.dict("os.environ", {"HRD_API_KEY": "key", "HANWHA_COURSE_ID": "cid"}, clear=True)
    @patch("hrd_api.fetch_active_data_realtime")
    @patch("hrd_api._get_active_data_from_db")
    def test_fallback_with_empty_db_is_distinguishable(self, mock_db, mock_api):
        """ETL이 한화 과정만 수집하므로 다른 기관 과정은 DB에 없다.
        이때 빈 결과를 '운영 중인 과정 없음'과 섞으면 거짓 안내가 된다."""
        mock_api.side_effect = Exception("API timeout")
        mock_db.return_value = (None, None, None)

        c, t, l, source = get_active_data_with_fallback()

        assert c is None and source == "DB_FALLBACK"

    @patch.dict("os.environ", {"HRD_API_KEY": "key", "HANWHA_COURSE_ID": "cid"}, clear=True)
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


# ── 운영기관 다중 지원 ────────────────────────────────────────────────


class TestInstitutions:
    """명부/출결 API는 인증키 소속 기관의 과정만 허용 → (키, 과정ID) 쌍으로 관리."""

    @patch.dict("os.environ", {
        "HRD_API_KEY": "hkey", "HANWHA_COURSE_ID": "hcid",
        "ENCORE_API_KEY": "ekey", "ENCORE_COURSE_IDS": "e1, e2",
    }, clear=True)
    def test_pairs_key_bound_to_own_courses(self):
        assert get_institutions() == [("hkey", "hcid"), ("ekey", "e1"), ("ekey", "e2")]

    @patch.dict("os.environ", {"ENCORE_API_KEY": "ekey", "ENCORE_COURSE_IDS": ""}, clear=True)
    def test_key_without_course_ids_yields_no_pairs(self):
        assert get_institutions() == []

    @patch.dict("os.environ", {"ENCORE_COURSE_IDS": "e1"}, clear=True)
    def test_course_ids_without_key_yields_no_pairs(self):
        assert get_institutions() == []

    @patch("hrd_api.fetch_active_data_realtime")
    def test_partial_failure_keeps_surviving_course(self, mock_api):
        """한 과정이 죽어도 나머지는 살아야 한다."""
        # 병렬 조회라 호출 순서가 고정되지 않으므로 과정 ID로 분기 (side_effect 리스트 금지)
        mock_api.side_effect = lambda key, cid: (
            _raise(Exception("과정 A 조회 실패")) if cid == "A" else _course_frames(cid)
        )

        courses, trainees, logs = fetch_all_institutions([("k", "A"), ("k", "B")])

        assert courses["TRPR_ID"].tolist() == ["B"]
        assert len(logs) == 1

    @patch("hrd_api.fetch_active_data_realtime")
    def test_all_failed_raises_for_db_fallback(self, mock_api):
        mock_api.side_effect = Exception("API timeout")
        with pytest.raises(RuntimeError):
            fetch_all_institutions([("k", "A"), ("k", "B")])

    def test_empty_result_keeps_columns(self):
        """과정이 하나도 없어도 컬럼은 유지 — 페이지가 컬럼명으로 필터하므로."""
        courses, trainees, logs = fetch_all_institutions([])
        assert "TRPR_ID" in courses.columns
        assert "TRNEE_STATUS" in trainees.columns
        assert "ATEND_DT" in logs.columns


class TestRealtimeDeadline:
    """기관을 순차 조회하면 요청 최악 46초가 기관 수만큼 누적돼 화면이 수 분간 멈춘다.
    병렬 + 전체 상한으로 대기 시간이 고정되는지 검증."""

    @patch("hrd_api.fetch_active_data_realtime")
    def test_deadline_aborts_instead_of_waiting(self, mock_api):
        def _hang(key, cid):
            time.sleep(5)
            raise AssertionError("상한을 넘겼는데도 끝까지 기다림")

        mock_api.side_effect = _hang
        t0 = time.monotonic()
        with pytest.raises(RuntimeError):
            fetch_all_institutions([("k", "A"), ("k", "B")], deadline=0.3)
        elapsed = time.monotonic() - t0
        assert elapsed < 2, f"상한 0.3초인데 {elapsed:.1f}초 대기 (스레드 종료를 기다림)"

    @patch("hrd_api.fetch_active_data_realtime")
    def test_deadline_keeps_completed_institutions(self, mock_api):
        """상한을 넘긴 기관만 버리고, 제때 끝난 기관 데이터는 살린다."""
        def _by_course(key, cid):
            if cid == "SLOW":
                time.sleep(5)
            return _course_frames(cid)

        mock_api.side_effect = _by_course
        courses, trainees, logs = fetch_all_institutions(
            [("k", "SLOW"), ("k", "FAST")], deadline=0.5
        )
        assert courses["TRPR_ID"].tolist() == ["FAST"]

    @patch("hrd_api.fetch_active_data_realtime")
    def test_institutions_run_in_parallel(self, mock_api):
        """3기관 × 각 0.4초가 순차면 1.2초, 병렬이면 ~0.4초."""
        def _slow(key, cid):
            time.sleep(0.4)
            return _course_frames(cid)

        mock_api.side_effect = _slow
        t0 = time.monotonic()
        fetch_all_institutions([("k", "A"), ("k", "B"), ("k", "C")], deadline=10)
        elapsed = time.monotonic() - t0
        assert elapsed < 0.9, f"순차 실행으로 보임 ({elapsed:.2f}초)"


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
