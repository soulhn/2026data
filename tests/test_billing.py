"""매출 청구 로직 테스트 — 실제 HRD-Net 청구 금액 검증"""
from utils import calc_revenue
from config import DAILY_TRAINING_FEE, REVENUE_FULL_THRESHOLD


# ── 상수 ───────────────────────────────────────────────────────────────────────
PERIOD2_17 = [
    # (이름, training_days, attend_days, expected_fee)
    ("강병욱", 21, 21, 3_049_200),
    ("구창모",  21, 17, 3_049_200),  # 81% >= 80% → 전액
    ("권재찬",  21, 15, 2_177_128),  # 71.4% → 비례
    ("김성인",  21, 20, 3_049_200),
    ("양승우",  21, 21, 3_049_200),
    ("양형모",  21, 21, 3_049_200),
    ("염준선",  21, 21, 3_049_200),
    ("유현경",  21, 21, 3_049_200),
    ("이상우",  21, 21, 3_049_200),
    ("이시욱",  21, 21, 3_049_200),
    ("이현식",  21, 21, 3_049_200),
    ("임주식",  21, 19, 3_049_200),  # 90.5% → 전액
    ("최민성",  21, 21, 3_049_200),
    ("허정빈",  21, 17, 3_049_200),  # 81% → 전액
    ("허정우",  21, 21, 3_049_200),
    ("홍서연",  21, 20, 3_049_200),
    ("강설",    21, 20, 3_049_200),
    ("김광호",  21,  0, 0),           # 미청구
    ("김륜환",  21, 19, 3_049_200),
    ("김아영",  21, 20, 3_049_200),
    ("김영재",  21, 15, 2_177_128),  # 71.4% → 비례
    ("김원중",  21, 20, 3_049_200),
    ("윤소민",  21, 21, 3_049_200),
    ("최경민",  21, 17, 3_049_200),  # 81% → 전액
]

PERIOD2_ACTUAL = 68_387_450

PERIOD_TOTALS_17 = [73_180_800, 68_387_450, 64_031_740, 55_031_960, 41_236_800, 47_478_930]
GRAND_TOTAL_17 = 349_347_680


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

    def test_proportional_714pct(self):
        """71.4% → 비례: int(full_fee × round(attend/training, 3))"""
        fee, rate, status = calc_revenue(15, 21)
        full_fee = 21 * DAILY_TRAINING_FEE  # 3,049,200
        expected = int(full_fee * round(15 / 21, 3))  # int(3,049,200 × 0.714) = 2,177,128
        assert fee == expected == 2_177_128
        assert status == "비례"

    def test_zero_attendance(self):
        """0% → 미청구"""
        fee, rate, status = calc_revenue(0, 21)
        assert fee == 0
        assert status == "미청구"

    def test_period2_all_students(self):
        """2단위 기간 수강생 24명 개별 청구액 검증"""
        for name, training, attend, expected_fee in PERIOD2_17:
            fee, _, _ = calc_revenue(attend, training)
            assert fee == expected_fee, (
                f"{name}: calc_revenue({attend}, {training}) = {fee}, expected {expected_fee}"
            )


# ── 단위기간 집계 ──────────────────────────────────────────────────────────────

class TestPeriodTruncation:
    def test_period2_raw_sum(self):
        """2단위 기간 raw 합계 검증"""
        raw = sum(calc_revenue(a, t)[0] for _, t, a, _ in PERIOD2_17)
        assert raw == 68_387_456

    def test_period2_truncated(self):
        """2단위 기간: raw 합계를 10원 단위 버림 → 실제 청구액"""
        raw = sum(calc_revenue(a, t)[0] for _, t, a, _ in PERIOD2_17)
        truncated = (raw // 10) * 10
        assert truncated == PERIOD2_ACTUAL == 68_387_450

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

PERIOD1_18 = [
    # (이름, period_td, student_td, attend_days, expected_fee)
    # period_td=22 (1단위 전체 훈련일수), student_td=개인 훈련일수
    ("김대의",  22, 22, 22, 3_194_400),
    ("김민수",  22, 22, 21, 3_194_400),
    ("김재상",  22, 22, 21, 3_194_400),
    ("김택곤",  22, 20, 20, 3_194_400),  # 중도입과: student_td=20, 20/20=100%
    ("박종원",  22, 22, 21, 3_194_400),
    ("박진우",  22, 22, 21, 3_194_400),
    ("안진기",  22, 20, 20, 3_194_400),  # 중도입과: 20/20=100%
    ("육세윤",  22, 20, 17, 3_194_400),  # 중도입과: 17/20=85% → 전액 (기존 코드에서 비례로 오계산)
    ("윤동기",  22, 22, 20, 3_194_400),
    ("윤석현",  22, 22, 21, 3_194_400),
    ("이승진",  22, 20, 20, 3_194_400),  # 중도입과: 20/20=100%
    ("이인화",  22, 22, 19, 3_194_400),
    ("이진구",  22, 22, 22, 3_194_400),
    ("임성민",  22, 22, 21, 3_194_400),
    ("전하윤",  22, 22, 20, 3_194_400),
    ("조상원",  22, 19, 19, 3_194_400),  # 중도입과: 19/19=100%
    ("조용주",  22, 22, 22, 3_194_400),
    ("조원석",  22, 22, 20, 3_194_400),
    ("최유경",  22, 21, 20, 3_194_400),  # 중도입과: 20/21=95.2%
    ("최정우",  22, 22, 22, 3_194_400),
    ("최정필",  22, 22, 22, 3_194_400),
    ("김민준",  22, 22, 22, 3_194_400),
    ("박채연",  22, 22, 21, 3_194_400),
    ("서현원",  22, 22, 22, 3_194_400),
    ("손혜원",  22, 22, 22, 3_194_400),
    ("이원진",  22, 22, 22, 3_194_400),
    ("임승택",  22, 22, 21, 3_194_400),
]

PERIOD1_18_ACTUAL = 86_248_800  # 27 × 3,194,400


class TestMidTermJoiner:
    """중도입과(단위기간 도중 입과) 수강생 청구액 검증"""

    def test_yukseyun_mid_joiner(self):
        """육세윤: student_td=20, attend=17 → 17/20=85% → 전액 (period_td=22 기준 full_fee)"""
        fee, rate, status = calc_revenue(17, 20, period_training_days=22)
        assert rate == 0.85
        assert status == "전액"
        assert fee == 22 * DAILY_TRAINING_FEE == 3_194_400

    def test_mid_joiner_proportional(self):
        """중도입과 + 비례: student_td=20, attend=14 → 14/20=70% → 비례 (full_fee는 period_td 기준)"""
        fee, rate, status = calc_revenue(14, 20, period_training_days=22)
        full_fee = 22 * DAILY_TRAINING_FEE
        assert status == "비례"
        assert fee == int(full_fee * round(14 / 20, 3))

    def test_period1_18_all_students(self):
        """18기 1단위 수강생 27명 개별 청구액 검증"""
        for name, period_td, student_td, attend, expected in PERIOD1_18:
            fee, _, _ = calc_revenue(attend, student_td, period_training_days=period_td)
            assert fee == expected, (
                f"{name}: calc_revenue({attend}, {student_td}, period={period_td}) = {fee:,}, expected {expected:,}"
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

PERIOD2_18_DROPOUTS = [
    # (이름, period_td, attend_days, expected_fee)
    ("안진기", 20, 13, 1_887_600),  # 13/20=65% → 비례
    ("전하윤", 20,  7, 1_016_400),  # 7/20=35%  → 비례
    ("임승택", 20,  6,   871_200),  # 6/20=30%  → 비례
]

PERIOD2_18_ACTUAL = 73_471_200  # 24 × 2,904,000 + 1,887,600 + 1,016,400 + 871,200


class TestDropout:
    """중도탈락 수강생: rate 분모 = 기간 전체 훈련일수"""

    def test_dropout_rate_uses_period_td(self):
        """중도탈락 시 분모는 기간 전체(period_td), 개인 훈련일수 아님"""
        for name, period_td, attend, expected in PERIOD2_18_DROPOUTS:
            # 중도탈락: training_days = period_td (full period)
            fee, rate, status = calc_revenue(attend, period_td, period_training_days=period_td)
            assert fee == expected, (
                f"{name}: fee={fee:,}, expected={expected:,}"
            )
            assert status == "비례"

    def test_anjingi_65pct(self):
        """안진기: 13/20=65% → 비례 → 1,887,600"""
        fee, rate, status = calc_revenue(13, 20, period_training_days=20)
        assert rate == 0.65
        assert status == "비례"
        assert fee == 1_887_600

    def test_jeonhayun_35pct(self):
        """전하윤: 7/20=35% → 비례 → 1,016,400"""
        fee, rate, status = calc_revenue(7, 20, period_training_days=20)
        assert rate == 0.35
        assert status == "비례"
        assert fee == 1_016_400

    def test_imsungtaek_30pct(self):
        """임승택: 6/20=30% → 비례 → 871,200"""
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
        # Group1(SH) raw + Group2(NH) raw
        raw_total = 31_797_928 + 15_681_019
        our_result = (raw_total // 10) * 10      # 47,478,940
        actual     = 47_478_930                   # HRD-Net 실제
        assert abs(our_result - actual) <= 10, (
            f"오차 {our_result - actual}원: 카드사별 소계 버림 미반영으로 허용 범위(10원) 내"
        )
