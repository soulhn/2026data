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
    mask_name,
    mask_name_columns,
    build_market_export,
)
from config import MARKET_EXPORT_COLUMNS


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


class TestMaskName:
    def test_three_chars(self):
        assert mask_name('홍길동') == '홍*동'

    def test_two_chars(self):
        assert mask_name('김수') == '김*'

    def test_four_chars(self):
        assert mask_name('남궁민수') == '남**수'

    def test_none_passthrough(self):
        assert mask_name(None) is None

    def test_nan_passthrough(self):
        result = mask_name(float('nan'))
        assert isinstance(result, float) and pd.isna(result)

    def test_empty_string(self):
        assert mask_name('') == ''

    def test_single_char(self):
        assert mask_name('김') == '김'


class TestMaskNameColumns:
    def test_masks_trnee_nm(self):
        df = pd.DataFrame({'TRNEE_NM': ['홍길동', '김수'], 'CNT': [1, 2]})
        out = mask_name_columns(df)
        assert out['TRNEE_NM'].tolist() == ['홍*동', '김*']

    def test_original_untouched(self):
        df = pd.DataFrame({'TRNEE_NM': ['홍길동']})
        mask_name_columns(df)
        assert df['TRNEE_NM'].tolist() == ['홍길동']

    def test_no_name_column_returns_original(self):
        df = pd.DataFrame({'CNT': [1]})
        assert mask_name_columns(df) is df

    def test_none_and_empty_df(self):
        assert mask_name_columns(None) is None
        empty = pd.DataFrame()
        assert mask_name_columns(empty) is empty


class TestBuildMarketExport:
    """시장 분석 「필터 결과 내려받기」 CSV 표"""
    TODAY = '2026-11-11'

    @staticmethod
    def _rows(*overrides):
        base = {
            'TRPR_NM': '과정', 'TRPR_DEGR': 1, 'TRAINST_NM': '기관', 'REGION': '서울',
            'TRAIN_TARGET': 'K-디지털 트레이닝', 'TR_STA_DT': '2026-10-01', 'TR_END_DT': '2027-03-01',
            'TOT_FXNUM': 20, 'REG_COURSE_MAN': 15, 'TOT_TRCO': 19166400.0, 'COURSE_MAN': 19166400.0,
            'STDG_SCOR': 8250.0, 'WKEND_SE': '3', 'NCS_CD': '20010202', 'CERTIFICATE': '',
            'EI_EMPL_RATE_3': None, 'EI_EMPL_RATE_6': None, 'EI_EMPL_CNT_3': None,
            'ADDRESS': '서울 강남구', 'TEL_NO': '02-000-0000', 'YEAR_MONTH': '2026-10',
            'TRPR_ID': 'AIG000', 'INST_INO': '500', 'TRAINST_CST_ID': '600',
            'TRNG_AREA_CD': '11', 'TRAIN_TARGET_CD': 'C0104',
        }
        return pd.DataFrame([{**base, **o} for o in overrides])

    def test_status_matches_schedule_tab_boundaries(self):
        df = self._rows(
            {'TR_STA_DT': '2026-11-12', 'TR_END_DT': '2027-04-01'},   # 내일 개강
            {'TR_STA_DT': '2026-11-11', 'TR_END_DT': '2027-04-01'},   # 오늘 개강 → 진행 중
            {'TR_STA_DT': '2026-06-01', 'TR_END_DT': '2026-11-11'},   # 오늘 종강 → 진행 중
            {'TR_STA_DT': '2026-06-01', 'TR_END_DT': '2026-11-10'},   # 어제 종강
            {'TR_STA_DT': '2026-06-01', 'TR_END_DT': None},           # 종강일 없음 → 어느 탭에도 없음
        )
        out = build_market_export(df, self.TODAY)
        assert out['상태'].tolist() == ['개강 예정', '진행 중', '진행 중', '종료', '']

    def test_columns_order_and_excluded(self):
        out = build_market_export(self._rows({}), self.TODAY)
        cols = out.columns.tolist()
        assert cols[0] == '상태'
        assert cols[cols.index('신청인원') + 1] == '모집률(%)'
        assert len(cols) == len(MARKET_EXPORT_COLUMNS) + 2
        assert not {'CONTENTS', 'GRADE', 'EI_EMPL_CNT_3_GT10', 'COLLECTED_AT'} & set(MARKET_EXPORT_COLUMNS)

    def test_value_conversions(self):
        df = self._rows(
            {'WKEND_SE': '3', 'STDG_SCOR': 8250.0, 'REG_COURSE_MAN': 15, 'TOT_FXNUM': 20,
             'EI_EMPL_RATE_3': '20.000', 'EI_EMPL_CNT_3': 4.0},
            {'WKEND_SE': '1', 'STDG_SCOR': 0.0, 'REG_COURSE_MAN': 40, 'TOT_FXNUM': 30,
             'EI_EMPL_RATE_3': 'A'},
            {'WKEND_SE': '9', 'REG_COURSE_MAN': 5, 'TOT_FXNUM': 0},
            {'WKEND_SE': '7'},
        )
        out = build_market_export(df, self.TODAY)
        assert out['주말/주중'].tolist() == ['주중', '주말', '해당없음', '7']   # 모르는 코드는 그대로
        assert out.loc[0, '만족도(점)'] == 82.5
        assert pd.isna(out.loc[1, '만족도(점)'])                              # 0 = 미집계
        assert out['모집률(%)'].tolist()[:3] == [75.0, 100.0, 0.0]           # 상한 100, 정원 0
        assert out.loc[0, '훈련비(원)'] == 19166400 and str(out['훈련비(원)'].dtype) == 'Int64'
        assert out.loc[0, '고용보험 취업률 (3개월)'] == 20.0
        assert pd.isna(out.loc[1, '고용보험 취업률 (3개월)'])                 # 특수코드는 빈칸
        assert out.loc[0, '고용보험 취업 인원 (3개월)'] == 4

    def test_csv_round_trip_and_empty(self):
        out = build_market_export(self._rows({}), self.TODAY)
        data = out.to_csv(index=False).encode('utf-8-sig')
        assert data.startswith(b'\xef\xbb\xbf')                             # 엑셀 한글 깨짐 방지 BOM
        empty = build_market_export(self._rows({}).iloc[0:0], self.TODAY)
        assert empty.empty and empty.columns.tolist() == out.columns.tolist()

    def test_export_columns_exist_in_schema(self, mock_db_connection):
        import init_db
        init_db.init_market_tables()
        cur = mock_db_connection.cursor()
        cur.execute(f"SELECT {', '.join(MARKET_EXPORT_COLUMNS)} FROM TB_MARKET_TREND")   # 없는 열이면 OperationalError
        assert [d[0] for d in cur.description] == list(MARKET_EXPORT_COLUMNS)
