"""utils.py 유틸리티 함수 테스트"""
import datetime as dt

import pandas as pd
import pytest

from utils import (
    adapt_query,
    safe_float,
    safe_int,
    calculate_age_at_training,
    _attendance_penalty,
    calc_attendance_rate,
    calc_attendance_rate_from_counts,
    calc_employment_rate_6,
    parse_empl_rate,
    is_completed,
    calc_recruit_rate,
    get_billing_periods,
)


class TestAdaptQuery:
    def test_sqlite_passthrough(self, monkeypatch):
        import utils
        monkeypatch.setattr(utils, "is_pg", lambda: False)
        sql = "SELECT * FROM t WHERE id = ?"
        assert adapt_query(sql) == sql

    def test_pg_placeholder_conversion(self, monkeypatch):
        import utils
        monkeypatch.setattr(utils, "is_pg", lambda: True)
        monkeypatch.setattr(utils, "get_database_url", lambda: "postgres://dummy")
        result = adapt_query("SELECT * FROM t WHERE id = ? AND name = ?")
        assert "?" not in result
        assert "%s" in result

    def test_pg_insert_or_ignore(self, monkeypatch):
        import utils
        monkeypatch.setattr(utils, "is_pg", lambda: True)
        monkeypatch.setattr(utils, "get_database_url", lambda: "postgres://dummy")
        sql = "INSERT OR IGNORE INTO t (a) VALUES (?)"
        result = adapt_query(sql)
        assert "INSERT INTO" in result
        assert "OR IGNORE" not in result
        assert "ON CONFLICT DO NOTHING" in result

    def test_pg_preserves_existing_on_conflict(self, monkeypatch):
        import utils
        monkeypatch.setattr(utils, "is_pg", lambda: True)
        monkeypatch.setattr(utils, "get_database_url", lambda: "postgres://dummy")
        sql = "INSERT INTO t (a) VALUES (?) ON CONFLICT(a) DO UPDATE SET b=excluded.b"
        result = adapt_query(sql)
        assert result.count("ON CONFLICT") == 1


class TestSafeFloat:
    def test_valid_number(self):
        assert safe_float("3.14") == 3.14

    def test_empty_string(self):
        assert safe_float("") == 0.0

    def test_none(self):
        assert safe_float(None) == 0.0

    def test_non_numeric(self):
        assert safe_float("A") == 0.0

    def test_custom_default(self):
        assert safe_float("bad", default=None) is None

    def test_integer_string(self):
        assert safe_float("42") == 42.0


class TestSafeInt:
    def test_valid_number(self):
        assert safe_int("42") == 42

    def test_empty_string(self):
        assert safe_int("") is None

    def test_none(self):
        assert safe_int(None) is None

    def test_non_numeric(self):
        assert safe_int("abc") is None

    def test_custom_default(self):
        assert safe_int("bad", default=0) == 0


class TestCalculateAgeAtTraining:
    def test_normal_case(self):
        assert calculate_age_at_training("19900101", "2024-01-15") == 35

    def test_missing_birth(self):
        assert calculate_age_at_training(None, "2024-01-15") is None

    def test_short_birth(self):
        assert calculate_age_at_training("99", "2024-01-15") is None

    def test_missing_training_date(self):
        """훈련 시작일이 없으면 현재 연도 기준"""
        result = calculate_age_at_training("19900101", None)
        assert result is not None
        assert result > 0

    def test_invalid_values(self):
        assert calculate_age_at_training("abcdef", "2024-01-15") is None


# ── 출석률 패널티 ──

class TestAttendancePenalty:
    @pytest.mark.parametrize("status,expected", [
        ("출석", 0),
        ("지각", 1),
        ("조퇴", 1),
        ("외출", 1),
        ("지각조퇴", 2),
        ("지각조퇴외출", 3),
        ("결석", 0),
        ("중도탈락미출석", 0),
        (None, 0),
        (123, 0),
        ("", 0),
        ("지각(10분)", 1),
    ])
    def test_penalty(self, status, expected):
        assert _attendance_penalty(status) == expected


# ── 출석률 (DataFrame 기반) ──

def _make_att_df(statuses):
    """테스트용 출결 DataFrame: 상태별로 고유 날짜 부여."""
    dates = [f"2024-01-{i + 1:02d}" for i in range(len(statuses))]
    return pd.DataFrame({"ATEND_DT": dates, "ATEND_STATUS": statuses})


class TestCalcAttendanceRate:
    def test_empty_dataframe(self):
        df = pd.DataFrame(columns=["ATEND_DT", "ATEND_STATUS"])
        assert calc_attendance_rate(df) == 0.0

    def test_all_present(self):
        df = _make_att_df(["출석"] * 10)
        assert calc_attendance_rate(df) == 100.0

    def test_with_absences(self):
        df = _make_att_df(["출석"] * 8 + ["결석"] * 2)
        assert calc_attendance_rate(df) == 80.0

    def test_dropout_absent_excluded_from_denominator(self):
        """중도탈락미출석은 분모·분자 모두 제외"""
        df = _make_att_df(["출석"] * 7 + ["결석"] + ["중도탈락미출석"] * 2)
        # training_days = 8 (10 - 2 탈락), base_attend = 7, rate = 7/8*100 = 87.5
        assert calc_attendance_rate(df) == 87.5

    def test_only_dropout_absent(self):
        df = _make_att_df(["중도탈락미출석"] * 5)
        assert calc_attendance_rate(df) == 0.0

    def test_lt50_excluded_from_numerator(self):
        """100분의50미만출석은 분자에서만 제외 (분모에는 포함)"""
        df = _make_att_df(["출석"] * 8 + ["100분의50미만출석"] * 2)
        # training_days = 10, base_attend = 8, rate = 80.0
        assert calc_attendance_rate(df) == 80.0

    def test_penalty_3late_minus_1day(self):
        """지각 3회 → 출석일 1일 차감"""
        df = _make_att_df(["출석"] * 7 + ["지각"] * 3)
        # training_days = 10, base_attend = 10, penalty = 3, attend = 10 - 1 = 9
        assert calc_attendance_rate(df) == 90.0

    def test_penalty_mixed(self):
        """지각+조퇴+외출 각 1회 = 3포인트 → 1일 차감"""
        df = _make_att_df(["출석"] * 7 + ["지각", "조퇴", "외출"])
        assert calc_attendance_rate(df) == 90.0

    def test_penalty_2points_no_deduction(self):
        """2포인트 → 차감 없음 (2//3=0)"""
        df = _make_att_df(["출석"] * 8 + ["지각"] * 2)
        assert calc_attendance_rate(df) == 100.0

    def test_penalty_floor_to_zero(self):
        """과다 패널티 → max(0, ...) 적용"""
        # 2일 모두 지각조퇴외출(3점씩) → penalty=6, attend = max(0, 2-2) = 0
        df = _make_att_df(["지각조퇴외출"] * 2)
        assert calc_attendance_rate(df) == 0.0

    def test_raw_flag(self):
        """raw=True → 반올림 없는 값"""
        df = _make_att_df(["출석"] * 2 + ["결석"])
        raw = calc_attendance_rate(df, raw=True)
        rounded = calc_attendance_rate(df, raw=False)
        assert raw == pytest.approx(200 / 3, rel=1e-9)
        assert rounded == 66.7

    def test_gongga_counted_as_present(self):
        """공가(경조사)는 출석으로 인정 (제외 목록에 없음)"""
        df = _make_att_df(["출석"] * 8 + ["공가(경조사)", "결석"])
        # training_days = 10, base_attend = 9 (공가 포함), rate = 90.0
        assert calc_attendance_rate(df) == 90.0


# ── 출석률 (집계 카운트 기반) ──

class TestCalcAttendanceRateFromCounts:
    @pytest.mark.parametrize(
        "args,expected",
        [
            ((20, 0, 0, 0, 0, 0, 0), 100.0),
            ((20, 2, 0, 0, 0, 0, 0), 90.0),
            ((20, 0, 5, 0, 0, 0, 0), 100.0),   # 탈락미출석 분모 제외
            ((20, 0, 0, 2, 0, 0, 0), 90.0),     # 50미만 2일
            ((20, 0, 0, 0, 3, 0, 0), 95.0),     # 지각 3회=1일 차감
            ((20, 0, 0, 0, 1, 1, 1), 95.0),     # 혼합 패널티 3=1일
            ((20, 0, 0, 0, 2, 0, 0), 100.0),    # 2포인트=0 차감
            ((10, 0, 10, 0, 0, 0, 0), 0.0),     # 전원 탈락
            ((0, 0, 0, 0, 0, 0, 0), 0.0),       # 기록 없음
            ((5, 0, 0, 0, 10, 10, 10), 0.0),    # 과다 패널티 → max(0)
        ],
    )
    def test_from_counts(self, args, expected):
        assert calc_attendance_rate_from_counts(*args) == expected


# ── 취업률 ──

class TestCalcEmploymentRate6:
    def test_both_numeric(self):
        assert calc_employment_rate_6('75.5', '10.0') == 85.5

    def test_zero_is_valid(self):
        """0은 pd.NA와 다름 — 유효한 숫자값"""
        assert calc_employment_rate_6('0', '0') == 0.0

    def test_code_A(self):
        assert pd.isna(calc_employment_rate_6('A', '10.0'))

    def test_code_B(self):
        assert pd.isna(calc_employment_rate_6('B', '5.0'))

    def test_code_C(self):
        assert pd.isna(calc_employment_rate_6('10.0', 'C'))

    def test_code_D(self):
        assert pd.isna(calc_employment_rate_6('D', 'D'))

    def test_both_none(self):
        assert pd.isna(calc_employment_rate_6(None, None))

    def test_both_nan_str(self):
        assert pd.isna(calc_employment_rate_6('nan', 'nan'))

    def test_both_empty(self):
        assert pd.isna(calc_employment_rate_6('', ''))

    def test_one_empty_one_numeric(self):
        assert calc_employment_rate_6('', '10.0') == 10.0

    def test_float_inputs(self):
        assert calc_employment_rate_6(75.5, 10.0) == 85.5


# ── 취업률 파싱 ──

class TestParseEmplRate:
    @pytest.mark.parametrize("val,expected", [
        ('75.5', (75.5, None)),
        ('0', (0.0, None)),
        ('A', (None, '개설예정')),
        ('B', (None, '진행중')),
        ('C', (None, '미실시')),
        ('D', (None, '수료자없음')),
        (None, (None, None)),
        ('', (None, None)),
    ])
    def test_parse(self, val, expected):
        assert parse_empl_rate(val) == expected

    def test_nan_string(self):
        """'nan'은 float('nan')으로 파싱됨 (safe_float 통과)"""
        import math
        num, label = parse_empl_rate('nan')
        assert math.isnan(num)
        assert label is None


# ── 수료 판정 ──

class TestIsCompleted:
    def test_various_statuses(self):
        s = pd.Series(['정상수료', '80%이상수료', '조기취업', '수료', '중도탈락', '훈련중', None])
        result = is_completed(s)
        expected = [True, True, True, True, False, False, False]
        assert list(result) == expected


# ── 모집률 ──

class TestCalcRecruitRate:
    def test_normal(self):
        result = calc_recruit_rate(pd.Series([25]), pd.Series([30]))
        assert result.iloc[0] == pytest.approx(83.333, rel=1e-2)

    def test_full(self):
        result = calc_recruit_rate(pd.Series([30]), pd.Series([30]))
        assert result.iloc[0] == 100.0

    def test_over_capacity_clipped(self):
        result = calc_recruit_rate(pd.Series([40]), pd.Series([30]))
        assert result.iloc[0] == 100.0

    def test_zero_capacity(self):
        result = calc_recruit_rate(pd.Series([10]), pd.Series([0]))
        assert result.iloc[0] == 0.0

    def test_zero_reg(self):
        result = calc_recruit_rate(pd.Series([0]), pd.Series([30]))
        assert result.iloc[0] == 0.0


# ── 청구 기간 ──

class TestGetBillingPeriods:
    def test_single_period(self):
        periods = get_billing_periods("2024-01-01", "2024-01-15")
        assert len(periods) == 1
        assert periods[0]["start"] == dt.date(2024, 1, 1)
        assert periods[0]["end"] == dt.date(2024, 1, 15)

    def test_multi_periods(self):
        periods = get_billing_periods("2024-01-01", "2024-03-31")
        assert len(periods) == 3
        assert periods[0]["end"] == dt.date(2024, 1, 31)
        assert periods[1]["start"] == dt.date(2024, 2, 1)
        assert periods[2]["end"] == dt.date(2024, 3, 31)

    def test_boundaries_match_start_day(self):
        """5/19 시작 → 1단위 5/19~6/18"""
        periods = get_billing_periods("2024-05-19", "2024-08-19")
        assert periods[0]["start"] == dt.date(2024, 5, 19)
        assert periods[0]["end"] == dt.date(2024, 6, 18)
        assert periods[1]["start"] == dt.date(2024, 6, 19)

    def test_last_period_clamped(self):
        """마지막 기간 end == end_date"""
        periods = get_billing_periods("2024-05-19", "2024-07-05")
        assert periods[-1]["end"] == dt.date(2024, 7, 5)

    def test_month_end_clamping(self):
        """1/31 시작 → 다음 기간 시작이 2/28로 클램프 (평년)"""
        periods = get_billing_periods("2025-01-31", "2025-03-31")
        # _add_one_month(1/31) = 2/28, period_end = 2/27
        assert periods[0]["end"] == dt.date(2025, 2, 27)
        assert periods[1]["start"] == dt.date(2025, 2, 28)

    def test_leap_year(self):
        """1/31 시작 2024년(윤년) → 다음 기간 시작이 2/29로 클램프"""
        periods = get_billing_periods("2024-01-31", "2024-03-31")
        # _add_one_month(1/31) = 2/29 (leap), period_end = 2/28
        assert periods[0]["end"] == dt.date(2024, 2, 28)
        assert periods[1]["start"] == dt.date(2024, 2, 29)

    def test_string_input(self):
        periods = get_billing_periods("2024-06-01", "2024-06-30")
        assert len(periods) == 1
        assert periods[0]["start"] == dt.date(2024, 6, 1)

    def test_date_object_input(self):
        periods = get_billing_periods(dt.date(2024, 6, 1), dt.date(2024, 6, 30))
        assert len(periods) == 1
        assert periods[0]["start"] == dt.date(2024, 6, 1)

    def test_period_num_sequential(self):
        periods = get_billing_periods("2024-01-01", "2024-04-30")
        nums = [p["period_num"] for p in periods]
        assert nums == [1, 2, 3, 4]

    def test_label_format(self):
        periods = get_billing_periods("2024-05-19", "2024-06-20")
        assert "1단위" in periods[0]["label"]
        assert "05/19" in periods[0]["label"]

    def test_status_past_completed(self):
        """과거 기간 → '완료'"""
        periods = get_billing_periods("2020-01-01", "2020-01-31")
        assert all(p["status"] == "완료" for p in periods)

    def test_status_future_expected(self):
        """미래 기간 → '예정'"""
        periods = get_billing_periods("2099-01-01", "2099-03-31")
        assert all(p["status"] == "예정" for p in periods)
