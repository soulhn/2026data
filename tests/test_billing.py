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
