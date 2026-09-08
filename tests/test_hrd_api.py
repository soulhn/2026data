"""hrd_api.py 단위 테스트 — API 응답 파싱 및 폴백 검증"""
import json
import time
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from hrd_api import (
    COURSE_HISTORY_COLUMNS,
    fetch_all_course_history,
    fetch_all_institutions,
    fetch_course_history,
    fetch_attendance_month,
    fetch_course_list,
    fetch_trainee_roster,
    fetch_active_data_realtime,
    get_active_data_with_fallback,
    get_course_history_with_fallback,
    get_institutions,
    get_last_realtime_error,
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
    @patch.dict("os.environ", {"HRD_API_KEY": "key"}, clear=True)
    @patch("hrd_api.fetch_active_data_realtime")
    @patch("hrd_api._get_active_data_from_db")
    def test_fallback_on_api_failure(self, mock_db, mock_api):
        mock_api.side_effect = Exception("API timeout")
        mock_db.return_value = (pd.DataFrame({"A": [1]}), pd.DataFrame(), pd.DataFrame())

        c, t, l, source = get_active_data_with_fallback()

        # "DB"(키 미설정)와 구분해야 페이지가 "운영 중인 과정 없음"으로 오안내하지 않는다
        assert source == "DB_FALLBACK"
        mock_db.assert_called_once()

    @patch.dict("os.environ", {"HRD_API_KEY": "key"}, clear=True)
    @patch("hrd_api.fetch_active_data_realtime")
    @patch("hrd_api._get_active_data_from_db")
    def test_fallback_with_empty_db_is_distinguishable(self, mock_db, mock_api):
        """ETL이 한화 과정만 수집하므로 다른 기관 과정은 DB에 없다.
        이때 빈 결과를 '운영 중인 과정 없음'과 섞으면 거짓 안내가 된다."""
        mock_api.side_effect = Exception("API timeout")
        mock_db.return_value = (None, None, None)

        c, t, l, source = get_active_data_with_fallback()

        assert c is None and source == "DB_FALLBACK"

    @patch.dict("os.environ", {"HRD_API_KEY": "key"}, clear=True)
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
    """명부/출결 API는 인증키 소속 기관의 과정만 허용 → (키, 과정ID) 쌍. 과정 ID는 config, 키는 환경변수."""

    _COURSES = {
        "hcid": ("PLAYDATA", "한화", ""), "e1": ("ENCORE", "MLE", ""), "e2": ("ENCORE", "AIO", ""),
    }

    @patch.dict("os.environ", {"HRD_API_KEY": "hkey", "ENCORE_API_KEY": "ekey"}, clear=True)
    @patch("hrd_api.config.COURSES", _COURSES)
    @patch("hrd_api.config.OPS_COURSE_IDS", ["hcid", "e1", "e2"])
    def test_pairs_key_bound_to_own_courses(self):
        assert get_institutions() == [("hkey", "hcid"), ("ekey", "e1"), ("ekey", "e2")]

    @patch.dict("os.environ", {"ENCORE_API_KEY": "ekey"}, clear=True)
    @patch("hrd_api.config.COURSES", _COURSES)
    @patch("hrd_api.config.OPS_COURSE_IDS", ["hcid", "e1"])
    def test_course_without_its_institution_key_is_skipped(self):
        assert get_institutions() == [("ekey", "e1")]

    @patch.dict("os.environ", {}, clear=True)
    @patch("hrd_api.config.COURSES", _COURSES)
    @patch("hrd_api.config.OPS_COURSE_IDS", ["hcid", "e1"])
    def test_no_keys_yields_no_pairs(self):
        assert get_institutions() == []

    @patch.dict("os.environ", {"HRD_API_KEY": "hkey"}, clear=True)
    @patch("hrd_api.config.COURSES", _COURSES)
    def test_explicit_course_list_dedupes_and_ignores_unregistered(self):
        assert get_institutions(["hcid", "hcid", "unknown"]) == [("hkey", "hcid")]

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

    def test_deadline_covers_two_sequential_stages(self):
        """한 기관은 과정목록 → (명부·출결) 2단계 순차. 각 단계가 API_TIMEOUT 상 최대
        connect+read 초까지 걸리므로, 상한이 그보다 짧으면 정상 경로가 잘린다."""
        import config
        one_request = sum(config.API_TIMEOUT)      # connect + read
        assert config.API_TOTAL_DEADLINE >= one_request * 2, (
            f"상한 {config.API_TOTAL_DEADLINE}초 < 2단계 {one_request * 2}초 — "
            "느리지만 정상인 조회가 폴백으로 잘림"
        )


class TestRealtimeFailureReason:
    """폴백은 정상 동작이라 예외가 화면까지 안 간다. 이유를 남기지 않으면
    '느려서 잘림'과 'API 거부'를 구분할 수 없어 원인 추적이 막힌다."""

    @patch("hrd_api.fetch_active_data_realtime")
    def test_timeout_reason_is_distinguishable(self, mock_api):
        def _hang(key, cid):
            time.sleep(5)

        mock_api.side_effect = _hang
        with pytest.raises(RuntimeError) as ei:
            fetch_all_institutions([("k", "A")], deadline=0.3)
        assert "상한" in str(ei.value) and "초과" in str(ei.value), str(ei.value)

    @patch("hrd_api.fetch_active_data_realtime")
    def test_api_error_reason_keeps_exception_type(self, mock_api):
        mock_api.side_effect = ConnectionError("Max retries exceeded")
        with pytest.raises(RuntimeError) as ei:
            fetch_all_institutions([("k", "A")], deadline=5)
        assert "ConnectionError" in str(ei.value)
        assert "Max retries exceeded" in str(ei.value)

    @patch.dict("os.environ", {"HRD_API_KEY": "key"}, clear=True)
    @patch("hrd_api.fetch_active_data_realtime")
    @patch("hrd_api._get_active_data_from_db")
    def test_reason_exposed_after_fallback(self, mock_db, mock_api):
        mock_api.side_effect = ConnectionError("서버 응답 없음")
        mock_db.return_value = (None, None, None)

        get_active_data_with_fallback()

        reason = get_last_realtime_error()
        assert reason and "ConnectionError" in reason

    @patch.dict("os.environ", {"HRD_API_KEY": "key"}, clear=True)
    @patch("hrd_api.fetch_active_data_realtime")
    def test_reason_cleared_on_success(self, mock_api):
        mock_api.return_value = _course_frames("A")
        get_active_data_with_fallback()
        assert get_last_realtime_error() is None

    @patch("hrd_api.fetch_active_data_realtime")
    def test_partial_failure_is_also_recorded(self, mock_api):
        """일부 기관만 실패하면 예외가 안 나므로, 살아남은 기관이 빈 결과일 때
        '운영 중인 과정 없음'과 구분하려면 부분 실패도 기록돼야 한다."""
        mock_api.side_effect = lambda key, cid: (
            _raise(ConnectionError("서버 응답 없음")) if cid == "DEAD" else _course_frames(cid)
        )

        courses, _, _ = fetch_all_institutions([("k", "DEAD"), ("k", "OK")], deadline=5)

        assert courses["TRPR_ID"].tolist() == ["OK"]     # 예외는 나지 않음
        reason = get_last_realtime_error()
        assert reason and "DEAD" in reason and "ConnectionError" in reason


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


# ── 모집 퍼널 (전 회차 이력) ───────────────────────────────────────────


def _history_records():
    return [
        {"trprId": "AIG00001", "trprDegr": "2", "trprNm": "과정 2기",
         "trStaDt": "2026-08-28", "trEndDt": "2027-02-23",
         "totFxnum": "30", "totTrpCnt": "16", "totParMks": "8", "finiCnt": "0", "instIno": "I1"},
        {"trprId": "AIG00001", "trprDegr": "1", "trprNm": "과정 1기",
         "trStaDt": "2024-01-15", "trEndDt": "2024-07-15",
         "totFxnum": "30", "totTrpCnt": "52", "totParMks": "30", "finiCnt": "24", "instIno": "I1"},
        # 개설예정 회차: totParMks가 None으로 내려온다
        {"trprId": "AIG00001", "trprDegr": "3", "trprNm": "과정 3기",
         "trStaDt": "2099-09-21", "trEndDt": "2099-12-31",
         "totFxnum": "30", "totTrpCnt": "0", "totParMks": None, "finiCnt": "0", "instIno": "I1"},
        {"trprId": "AIG00001", "trprDegr": "0", "trprNm": "회차 0 (스킵)"},
    ]


class TestFetchCourseHistory:
    def test_keeps_ended_courses(self, mock_session):
        """운영 현황용 fetch_course_list와 달리 종료 회차도 남겨야 퍼널이 완성된다."""
        mock_session.get.return_value = _make_response(_history_records())
        df = fetch_course_history(mock_session, "KEY", "AIG00001")
        assert df["TRPR_DEGR"].tolist() == [1, 2, 3]          # 회차 오름차순
        assert df.loc[df["TRPR_DEGR"] == 1, "FINI_CNT"].item() == "24"

    def test_open_scheduled_keeps_none_par_mks(self, mock_session):
        mock_session.get.return_value = _make_response(_history_records())
        df = fetch_course_history(mock_session, "KEY", "AIG00001")
        assert pd.isna(df.loc[df["TRPR_DEGR"] == 3, "TOT_PAR_MKS"].item())

    def test_columns_match_db(self, mock_session):
        """DB 폴백(TB_COURSE_MASTER)과 컬럼이 같아야 페이지가 소스를 구분하지 않아도 된다."""
        mock_session.get.return_value = _make_response(_history_records())
        df = fetch_course_history(mock_session, "KEY", "AIG00001")
        assert list(df.columns) == COURSE_HISTORY_COLUMNS

    def test_active_list_unchanged_by_refactor(self, mock_session):
        """공통 파서로 바꿨어도 운영 현황용 목록은 여전히 활성 회차만 남긴다."""
        mock_session.get.return_value = _make_response(_history_records())
        df = fetch_course_list(mock_session, "KEY", "AIG00001")
        assert sorted(df["TRPR_DEGR"].tolist()) == [2, 3]
        assert "FINI_CNT" not in df.columns


class TestCourseHistoryFallback:
    @patch("hrd_api.get_retry_session")
    @patch("hrd_api.fetch_course_history")
    def test_partial_failure_keeps_surviving_and_reports(self, mock_fetch, _sess):
        mock_fetch.side_effect = lambda s, key, cid: (
            _raise(Exception("과정 A 조회 실패")) if cid == "A"
            else pd.DataFrame({"TRPR_ID": [cid], "TRPR_DEGR": [1]})
        )
        df, error = fetch_all_course_history([("k", "A"), ("k", "B")])
        assert df["TRPR_ID"].tolist() == ["B"]
        assert error and "A" in error and "과정 A 조회 실패" in error

    @patch("hrd_api.get_retry_session")
    @patch("hrd_api.fetch_course_history")
    def test_all_failed_raises(self, mock_fetch, _sess):
        mock_fetch.side_effect = Exception("API timeout")
        with pytest.raises(RuntimeError):
            fetch_all_course_history([("k", "A"), ("k", "B")])

    def test_empty_pairs_keeps_columns(self):
        df, error = fetch_all_course_history([])
        assert list(df.columns) == COURSE_HISTORY_COLUMNS and error is None

    @patch.dict("os.environ", {}, clear=True)
    @patch("hrd_api._get_course_history_from_db")
    def test_no_keys_uses_db(self, mock_db):
        mock_db.return_value = pd.DataFrame({"TRPR_ID": ["H"]})
        df, source, error = get_course_history_with_fallback()
        assert source == "DB" and error is None
        mock_db.assert_called_once()

    @patch.dict("os.environ", {"HRD_API_KEY": "key"}, clear=True)
    @patch("hrd_api.fetch_all_course_history")
    @patch("hrd_api._get_course_history_from_db")
    def test_api_failure_falls_back_with_reason(self, mock_db, mock_api):
        """폴백은 한화 과정만 있으므로 페이지가 '엔코아 빠짐'을 안내하려면 소스 구분이 필요하다."""
        mock_api.side_effect = RuntimeError("모든 과정 이력 조회 실패")
        mock_db.return_value = pd.DataFrame({"TRPR_ID": ["H"]})
        df, source, error = get_course_history_with_fallback()
        assert source == "DB_FALLBACK" and "RuntimeError" in error

    @patch.dict("os.environ", {"HRD_API_KEY": "key"}, clear=True)
    @patch("hrd_api.fetch_all_course_history")
    def test_api_success_passes_partial_error_through(self, mock_api):
        mock_api.return_value = (pd.DataFrame({"TRPR_ID": ["H"]}), "E1 → Timeout")
        df, source, error = get_course_history_with_fallback()
        assert source == "API" and error == "E1 → Timeout"


# ── get_funnel_institutions ────────────────────────────────────────────


class TestFunnelInstitutions:
    """퍼널·노션 대조는 등록된 과정 전부(config.FUNNEL_COURSE_IDS), 운영 현황은 OPS_COURSE_IDS."""

    _COURSES = {
        "hcid": ("PLAYDATA", "한화", ""), "skn": ("PLAYDATA", "SKN", ""),
        "e1": ("ENCORE", "MLE", ""), "mlo": ("ENCORE", "MLO", ""),
    }

    @patch.dict("os.environ", {"HRD_API_KEY": "hkey", "ENCORE_API_KEY": "ekey"}, clear=True)
    @patch("hrd_api.config.COURSES", _COURSES)
    @patch("hrd_api.config.OPS_COURSE_IDS", ["hcid", "e1"])
    @patch("hrd_api.config.FUNNEL_COURSE_IDS", ["hcid", "skn", "e1", "mlo"])
    def test_funnel_is_superset_of_ops(self):
        from hrd_api import get_funnel_institutions
        assert get_institutions() == [("hkey", "hcid"), ("ekey", "e1")]
        assert get_funnel_institutions() == [("hkey", "hcid"), ("hkey", "skn"), ("ekey", "e1"), ("ekey", "mlo")]

    @patch("hrd_api.fetch_all_course_history")
    @patch("hrd_api.get_institutions")
    def test_history_uses_given_pairs(self, mock_inst, mock_hist):
        mock_hist.return_value = (pd.DataFrame({"TRPR_ID": ["x"]}), None)
        df, source, err = get_course_history_with_fallback([("k", "x")])
        assert source == "API" and df["TRPR_ID"].tolist() == ["x"]
        mock_inst.assert_not_called()
        mock_hist.assert_called_once_with([("k", "x")])

    def test_real_config_is_consistent(self):
        """실제 config: 모든 범위가 등록된 과정 안에 있고, 기관·약칭·그룹 키가 서로 맞아야 한다."""
        import config
        assert config.ETL_COURSE_ID in config.COURSES
        assert set(config.OPS_COURSE_IDS) <= set(config.COURSES)
        assert set(config.FUNNEL_COURSE_IDS) == set(config.COURSES)
        for cid, (inst, short, _) in config.COURSES.items():
            assert inst in config.INSTITUTIONS
            assert short in config.COURSE_GROUP_KEYWORDS
        assert config.COURSE_SHORT_NAMES == {cid: v[1] for cid, v in config.COURSES.items()}
