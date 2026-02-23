"""매출 청구 로직 테스트 — 실제 HRD-Net 청구 금액 검증"""
from utils import calc_revenue
from config import DAILY_TRAINING_FEE, REVENUE_FULL_THRESHOLD


# ── 상수 ───────────────────────────────────────────────────────────────────────
PERIOD2_17 = [
    # (ID, training_days, attend_days, expected_fee)
    ("S01", 21, 21, 3_049_200),
    ("S02", 21, 17, 3_049_200),  # 81% >= 80% → 전액
    ("S03", 21, 15, 2_177_128),  # 71.4% → 비례
    ("S04", 21, 20, 3_049_200),
    ("S05", 21, 21, 3_049_200),
    ("S06", 21, 21, 3_049_200),
    ("S07", 21, 21, 3_049_200),
    ("S08", 21, 21, 3_049_200),
    ("S09", 21, 21, 3_049_200),
    ("S10", 21, 21, 3_049_200),
    ("S11", 21, 21, 3_049_200),
    ("S12", 21, 19, 3_049_200),  # 90.5% → 전액
    ("S13", 21, 21, 3_049_200),
    ("S14", 21, 17, 3_049_200),  # 81% → 전액
    ("S15", 21, 21, 3_049_200),
    ("S16", 21, 20, 3_049_200),
    ("S17", 21, 20, 3_049_200),
    ("S18", 21,  0,         0),  # 미청구
    ("S19", 21, 19, 3_049_200),
    ("S20", 21, 20, 3_049_200),
    ("S21", 21, 15, 2_177_128),  # 71.4% → 비례
    ("S22", 21, 20, 3_049_200),
    ("S23", 21, 21, 3_049_200),
    ("S24", 21, 17, 3_049_200),  # 81% → 전액
]

PERIOD2_17_ACTUAL = 68_387_450

# 17기 6단위 (training_days=19) — SH/NH 카드사 2그룹
# Group1 SH(신한카드) 16명
PERIOD6_17_GROUP1 = [
    # (ID, attend_days, expected_fee)
    ("G1_S01", 18, 2_758_800),  # 18/19=94.7% → 전액
    ("G1_S02",  0,         0),  # 미청구
    ("G1_S03",  0,         0),  # 미청구
    ("G1_S04", 16, 2_758_800),  # 16/19=84.2% → 전액
    ("G1_S05", 19, 2_758_800),  # 100% → 전액
    ("G1_S06", 17, 2_758_800),  # 17/19=89.5% → 전액
    ("G1_S07", 14, 2_033_235),  # 14/19=73.7% → 비례
    ("G1_S08", 17, 2_758_800),  # 17/19=89.5% → 전액
    ("G1_S09", 15, 2_176_693),  # 15/19=78.9% → 비례
    ("G1_S10", 18, 2_758_800),  # 18/19=94.7% → 전액
    ("G1_S11", 16, 2_758_800),  # 16/19=84.2% → 전액
    ("G1_S12",  0,         0),  # 미청구
    ("G1_S13", 19, 2_758_800),  # 100% → 전액
    ("G1_S14",  0,         0),  # 미청구
    ("G1_S15", 17, 2_758_800),  # 17/19=89.5% → 전액
    ("G1_S16", 16, 2_758_800),  # 16/19=84.2% → 전액
]

# Group2 NH(농협카드) 8명
PERIOD6_17_GROUP2 = [
    ("G2_S01", 17, 2_758_800),  # 17/19=89.5% → 전액
    ("G2_S02",  0,         0),  # 미청구
    ("G2_S03", 13, 1_887_019),  # 13/19=68.4% → 비례
    ("G2_S04", 16, 2_758_800),  # 16/19=84.2% → 전액
    ("G2_S05", 16, 2_758_800),  # 16/19=84.2% → 전액
    ("G2_S06", 16, 2_758_800),  # 16/19=84.2% → 전액
    ("G2_S07", 17, 2_758_800),  # 17/19=89.5% → 전액
    ("G2_S08",  0,         0),  # 미청구
]

PERIOD6_17_RAW_GROUP1 = 31_797_928   # SH 16명 개별합
PERIOD6_17_RAW_GROUP2 = 15_681_019   # NH 8명 개별합
PERIOD6_17_RAW = 47_478_947          # 전체 raw (Group1 + Group2)
PERIOD6_17_ACTUAL = 47_478_930       # HRD-Net 실제 (카드사별 소계 버림)

PERIOD_TOTALS_17 = [73_180_800, 68_387_450, 64_031_740, 55_031_960, 41_236_800, 47_478_930]
GRAND_TOTAL_17 = 349_347_680


# ── 18기 1단위 (중도입과 케이스) ───────────────────────────────────────────────

PERIOD1_18 = [
    # (ID, period_td, student_td, attend_days, expected_fee)
    # period_td=22 (1단위 전체 훈련일수), student_td=개인 훈련일수
    ("S01", 22, 22, 22, 3_194_400),
    ("S02", 22, 22, 21, 3_194_400),
    ("S03", 22, 22, 21, 3_194_400),
    ("S04", 22, 20, 20, 3_194_400),  # 중도입과: student_td=20, 20/20=100%
    ("S05", 22, 22, 21, 3_194_400),
    ("S06", 22, 22, 21, 3_194_400),
    ("S07", 22, 20, 20, 3_194_400),  # 중도입과: 20/20=100%
    ("S08", 22, 20, 17, 3_194_400),  # 중도입과: 17/20=85% → 전액 (기존 코드에서 비례로 오계산)
    ("S09", 22, 22, 20, 3_194_400),
    ("S10", 22, 22, 21, 3_194_400),
    ("S11", 22, 20, 20, 3_194_400),  # 중도입과: 20/20=100%
    ("S12", 22, 22, 19, 3_194_400),
    ("S13", 22, 22, 22, 3_194_400),
    ("S14", 22, 22, 21, 3_194_400),
    ("S15", 22, 22, 20, 3_194_400),
    ("S16", 22, 19, 19, 3_194_400),  # 중도입과: 19/19=100%
    ("S17", 22, 22, 22, 3_194_400),
    ("S18", 22, 22, 20, 3_194_400),
    ("S19", 22, 21, 20, 3_194_400),  # 중도입과: 20/21=95.2%
    ("S20", 22, 22, 22, 3_194_400),
    ("S21", 22, 22, 22, 3_194_400),
    ("S22", 22, 22, 22, 3_194_400),
    ("S23", 22, 22, 21, 3_194_400),
    ("S24", 22, 22, 22, 3_194_400),
    ("S25", 22, 22, 22, 3_194_400),
    ("S26", 22, 22, 22, 3_194_400),
    ("S27", 22, 22, 21, 3_194_400),
]

PERIOD1_18_ACTUAL = 86_248_800  # 27 × 3,194,400


# ── 18기 2단위 ─────────────────────────────────────────────────────────────────
# 전원 training_days=20 (period_td=20 동일)
# - 일반 수강생: rate_td=student_td=20 (중도탈락미출석 없음)
# - 중도탈락: rate_td=period_td=20 (중도탈락미출석 기록 있음)

PERIOD2_18_FULL = [
    # (ID, attend_days, expected_fee) — training_days=20 for all
    # 24명 전액
    ("S01", 20, 2_904_000),
    ("S02", 20, 2_904_000),
    ("S03", 20, 2_904_000),
    ("S04", 19, 2_904_000),  # 19/20=95% → 전액
    ("S05", 20, 2_904_000),
    ("S06", 20, 2_904_000),
    ("S07", 20, 2_904_000),
    ("S08", 19, 2_904_000),
    ("S09", 20, 2_904_000),
    ("S10", 19, 2_904_000),
    ("S11", 19, 2_904_000),
    ("S12", 20, 2_904_000),
    ("S13", 20, 2_904_000),
    ("S14", 20, 2_904_000),
    ("S15", 20, 2_904_000),
    ("S16", 16, 2_904_000),  # 16/20=80% → 전액 (전액 경계값)
    ("S17", 19, 2_904_000),
    ("S18", 20, 2_904_000),
    ("S19", 20, 2_904_000),
    ("S20", 20, 2_904_000),
    ("S21", 19, 2_904_000),
    ("S22", 20, 2_904_000),
    ("S23", 20, 2_904_000),
    ("S24", 19, 2_904_000),
    # 3명 중도탈락 비례 (rate_td=period_td=20)
    ("D01", 13, 1_887_600),  # 13/20=65% → 비례
    ("D02",  7, 1_016_400),  # 7/20=35% → 비례
    ("D03",  6,   871_200),  # 6/20=30% → 비례
]

PERIOD2_18_DROPOUTS = [
    # (ID, period_td, attend_days, expected_fee)
    ("D01", 20, 13, 1_887_600),  # 13/20=65% → 비례
    ("D02", 20,  7, 1_016_400),  # 7/20=35%  → 비례
    ("D03", 20,  6,   871_200),  # 6/20=30%  → 비례
]

PERIOD2_18_ACTUAL = 73_471_200  # 24 × 2,904,000 + 1,887,600 + 1,016,400 + 871,200


# ── 개별 청구액 ────────────────────────────────────────────────────────────────

class TestCalcRevenue:
    def test_full_payment_100pct(self):
        """100% 출석 → 전액"""
        fee, rate, status = calc_revenue(21, 21)
        assert fee == 21 * DAILY_TRAINING_FEE == 3_049_200
        assert status == "전액"

    def test_full_payment_threshold_81pct(self):
        """81% (>= 80%) → 전액"""
        fee, rate, status = calc_revenue(17, 21)
        assert fee == 21 * DAILY_TRAINING_FEE == 3_049_200
        assert status == "전액"

    def test_full_payment_threshold_80pct_exact(self):
        """80% 정확히 (경계값) → 전액"""
        fee, rate, status = calc_revenue(16, 20)
        assert fee == 20 * DAILY_TRAINING_FEE == 2_904_000
        assert status == "전액"

    def test_proportional_714pct(self):
        """71.4% → 비례: full_fee × rate_per_mille // 1000"""
        fee, rate, status = calc_revenue(15, 21)
        # rate_per_mille = round(15*1000/21) = 714
        # fee = 3,049,200 × 714 // 1000 = 2,177,128
        assert fee == 2_177_128
        assert status == "비례"

    def test_zero_attendance(self):
        """0% → 미청구"""
        fee, rate, status = calc_revenue(0, 21)
        assert fee == 0
        assert status == "미청구"

    def test_period2_17_all_students(self):
        """17기 2단위 수강생 24명 개별 청구액 검증"""
        for sid, training, attend, expected_fee in PERIOD2_17:
            fee, _, _ = calc_revenue(attend, training)
            assert fee == expected_fee, (
                f"{sid}: calc_revenue({attend}, {training}) = {fee}, expected {expected_fee}"
            )


# ── 17기 6단위 개별 청구액 ────────────────────────────────────────────────────

class TestPeriod6_17Students:
    """17기 6단위 (training_days=19) 24명 개별 청구액 검증"""

    def test_group1_individual_fees(self):
        """Group1(SH) 16명 개별 청구액"""
        for sid, attend, expected in PERIOD6_17_GROUP1:
            fee, _, _ = calc_revenue(attend, 19)
            assert fee == expected, (
                f"{sid}: calc_revenue({attend}, 19) = {fee:,}, expected {expected:,}"
            )

    def test_group2_individual_fees(self):
        """Group2(NH) 8명 개별 청구액"""
        for sid, attend, expected in PERIOD6_17_GROUP2:
            fee, _, _ = calc_revenue(attend, 19)
            assert fee == expected, (
                f"{sid}: calc_revenue({attend}, 19) = {fee:,}, expected {expected:,}"
            )

    def test_group1_raw_sum(self):
        """Group1(SH) raw 합계 = 31,797,928"""
        raw = sum(calc_revenue(a, 19)[0] for _, a, _ in PERIOD6_17_GROUP1)
        assert raw == PERIOD6_17_RAW_GROUP1 == 31_797_928

    def test_group2_raw_sum(self):
        """Group2(NH) raw 합계 = 15,681,019"""
        raw = sum(calc_revenue(a, 19)[0] for _, a, _ in PERIOD6_17_GROUP2)
        assert raw == PERIOD6_17_RAW_GROUP2 == 15_681_019

    def test_period6_combined_raw(self):
        """6단위 전체 raw 합계 = 47,478,947"""
        g1 = sum(calc_revenue(a, 19)[0] for _, a, _ in PERIOD6_17_GROUP1)
        g2 = sum(calc_revenue(a, 19)[0] for _, a, _ in PERIOD6_17_GROUP2)
        assert g1 + g2 == PERIOD6_17_RAW == 47_478_947

    def test_737pct_proportional(self):
        """14/19=73.7% → 비례 → 2,033,235"""
        fee, rate, status = calc_revenue(14, 19)
        assert rate == 0.737
        assert status == "비례"
        assert fee == 2_033_235

    def test_789pct_proportional(self):
        """15/19=78.9% (< 80%) → 비례 → 2,176,693"""
        fee, rate, status = calc_revenue(15, 19)
        assert rate == 0.789
        assert status == "비례"
        assert fee == 2_176_693

    def test_684pct_proportional(self):
        """13/19=68.4% → 비례 → 1,887,019"""
        fee, rate, status = calc_revenue(13, 19)
        assert rate == 0.684
        assert status == "비례"
        assert fee == 1_887_019


# ── 단위기간 집계 ──────────────────────────────────────────────────────────────

class TestPeriodTruncation:
    def test_period2_17_raw_sum(self):
        """17기 2단위 raw 합계 = 68,387,456"""
        raw = sum(calc_revenue(a, t)[0] for _, t, a, _ in PERIOD2_17)
        assert raw == 68_387_456

    def test_period2_17_truncated(self):
        """17기 2단위: raw 합계를 10원 단위 버림 → 실제 청구액"""
        raw = sum(calc_revenue(a, t)[0] for _, t, a, _ in PERIOD2_17)
        truncated = (raw // 10) * 10
        assert truncated == PERIOD2_17_ACTUAL == 68_387_450

    def test_truncation_formula(self):
        """(raw // 10) * 10 = 10원 단위 버림"""
        assert (68_387_456 // 10) * 10 == 68_387_450
        assert (64_031_746 // 10) * 10 == 64_031_740
        assert (55_031_961 // 10) * 10 == 55_031_960


# ── 총 매출 ────────────────────────────────────────────────────────────────────

class TestGrandTotal:
    def test_grand_total_17(self):
        """17기 총 매출 = 각 단위기간 버림액 합계"""
        assert sum(PERIOD_TOTALS_17) == GRAND_TOTAL_17 == 349_347_680


# ── 18기 1단위 (중도입과 케이스) ───────────────────────────────────────────────

class TestMidTermJoiner:
    """중도입과(단위기간 도중 입과) 수강생 청구액 검증"""

    def test_mid_joiner_85pct_full(self):
        """중도입과: student_td=20, attend=17 → 17/20=85% → 전액 (period_td=22 기준 full_fee)"""
        fee, rate, status = calc_revenue(17, 20, period_training_days=22)
        assert rate == 0.85
        assert status == "전액"
        assert fee == 22 * DAILY_TRAINING_FEE == 3_194_400

    def test_mid_joiner_proportional(self):
        """중도입과 + 비례: student_td=20, attend=14 → 14/20=70% → 비례 (full_fee는 period_td 기준)"""
        fee, rate, status = calc_revenue(14, 20, period_training_days=22)
        full_fee = 22 * DAILY_TRAINING_FEE
        assert status == "비례"
        # rate_per_mille = round(14*1000/20) = 700; fee = full_fee * 700 // 1000
        assert fee == full_fee * 700 // 1000

    def test_period1_18_all_students(self):
        """18기 1단위 수강생 27명 개별 청구액 검증"""
        for sid, period_td, student_td, attend, expected in PERIOD1_18:
            fee, _, _ = calc_revenue(attend, student_td, period_training_days=period_td)
            assert fee == expected, (
                f"{sid}: calc_revenue({attend}, {student_td}, period={period_td}) = {fee:,}, expected {expected:,}"
            )

    def test_period1_18_total(self):
        """18기 1단위 합계: 27 × 3,194,400 = 86,248,800"""
        raw = sum(
            calc_revenue(attend, student_td, period_training_days=period_td)[0]
            for _, period_td, student_td, attend, _ in PERIOD1_18
        )
        assert raw == PERIOD1_18_ACTUAL == 86_248_800


# ── 18기 2단위 (중도탈락 케이스) ───────────────────────────────────────────────
# 중도탈락미출석 있는 수강생은 rate 분모 = 기간 전체 훈련일수(20)

class TestDropout:
    """중도탈락 수강생: rate 분모 = 기간 전체 훈련일수"""

    def test_dropout_rate_uses_period_td(self):
        """중도탈락 시 분모는 기간 전체(period_td), 개인 훈련일수 아님"""
        for sid, period_td, attend, expected in PERIOD2_18_DROPOUTS:
            fee, rate, status = calc_revenue(attend, period_td, period_training_days=period_td)
            assert fee == expected, (
                f"{sid}: fee={fee:,}, expected={expected:,}"
            )
            assert status == "비례"

    def test_dropout_65pct(self):
        """13/20=65% → 비례 → 1,887,600"""
        fee, rate, status = calc_revenue(13, 20, period_training_days=20)
        assert rate == 0.65
        assert status == "비례"
        assert fee == 1_887_600

    def test_dropout_35pct(self):
        """7/20=35% → 비례 → 1,016,400"""
        fee, rate, status = calc_revenue(7, 20, period_training_days=20)
        assert rate == 0.35
        assert status == "비례"
        assert fee == 1_016_400

    def test_dropout_30pct(self):
        """6/20=30% → 비례 → 871,200"""
        fee, rate, status = calc_revenue(6, 20, period_training_days=20)
        assert rate == 0.30
        assert status == "비례"
        assert fee == 871_200

    def test_period2_18_total(self):
        """18기 2단위 합계: 24 × 2,904,000 + 3개 비례 = 73,471,200"""
        full_fee = 20 * DAILY_TRAINING_FEE  # 2,904,000
        normal_count = 27 - len(PERIOD2_18_DROPOUTS)
        raw = normal_count * full_fee + sum(
            calc_revenue(attend, period_td, period_training_days=period_td)[0]
            for _, period_td, attend, _ in PERIOD2_18_DROPOUTS
        )
        assert raw == PERIOD2_18_ACTUAL == 73_471_200


# ── 18기 2단위 전체 27명 검증 ──────────────────────────────────────────────────

class TestPeriod2_18Full:
    """18기 2단위 전체 27명 개별 청구액 + 합계 검증"""

    def test_all_students_individual_fees(self):
        """27명 전원 개별 청구액 검증 (training_days=period_td=20)"""
        for sid, attend, expected in PERIOD2_18_FULL:
            fee, _, _ = calc_revenue(attend, 20, period_training_days=20)
            assert fee == expected, (
                f"{sid}: calc_revenue({attend}, 20) = {fee:,}, expected {expected:,}"
            )

    def test_80pct_boundary_full(self):
        """16/20=80% 정확히 → 전액 (REVENUE_FULL_THRESHOLD 경계)"""
        fee, rate, status = calc_revenue(16, 20, period_training_days=20)
        assert rate == 0.80
        assert status == "전액"
        assert fee == 20 * DAILY_TRAINING_FEE == 2_904_000

    def test_period2_18_full_total(self):
        """18기 2단위 전 27명 합계 = 73,471,200"""
        raw = sum(
            calc_revenue(attend, 20, period_training_days=20)[0]
            for _, attend, _ in PERIOD2_18_FULL
        )
        assert raw == PERIOD2_18_ACTUAL == 73_471_200


# ── 구현 한계: 카드사별 소계 버림 재현 불가 ────────────────────────────────────

class TestKnownLimitation:
    """
    [구현 한계] 카드사별 소계 내 버림 재현 불가

    HRD-Net 청구서는 신한카드(SH) / 농협카드(NH) 등 제휴 금융기관별로
    학생을 그룹화한 뒤 각 소계에 10원 단위 버림을 적용한다.

    예) 17기 6단위 기간:
        SH 16명 raw=31,797,928 → 버림 31,797,920
        NH  8명 raw=15,681,019 → 버림 15,681,010
        실제 합계: 47,478,930

    우리 DB(TB_TRAINEE_INFO, TB_ATTENDANCE_LOG)에는 카드사 구분 정보가
    없으므로, 단위기간 전체 합에 버림을 1회 적용한다.
        전체 raw=47,478,947 → 버림 47,478,940 (실제 대비 +10원)

    이로 인해 카드사 그룹이 N개인 단위기간은 최대 (N-1)×9원의 오차 발생.
    실제 관측된 최대 오차: 10원/기간.
    """

    def test_period6_single_truncation(self):
        """단위기간 단일 버림: 실제 대비 최대 10원 오차 허용"""
        our_result = (PERIOD6_17_RAW // 10) * 10   # 47,478,940
        assert our_result == 47_478_940
        assert abs(our_result - PERIOD6_17_ACTUAL) <= 10, (
            f"오차 {our_result - PERIOD6_17_ACTUAL}원: 카드사별 소계 버림 미반영으로 허용 범위(10원) 내"
        )

    def test_card_group_truncation_detail(self):
        """카드사 그룹별 버림 후 합산 = HRD-Net 실제값"""
        sh_truncated = (PERIOD6_17_RAW_GROUP1 // 10) * 10  # 31,797,920
        nh_truncated = (PERIOD6_17_RAW_GROUP2 // 10) * 10  # 15,681,010
        assert sh_truncated == 31_797_920
        assert nh_truncated == 15_681_010
        assert sh_truncated + nh_truncated == PERIOD6_17_ACTUAL == 47_478_930

    def test_our_single_truncation_vs_card_split(self):
        """우리 방식(단일 버림) vs HRD-Net(카드사별 버림): 10원 차이"""
        our_result = (PERIOD6_17_RAW // 10) * 10   # 47,478,940
        assert our_result - PERIOD6_17_ACTUAL == 10  # 정확히 10원 차이
