"""data/home_snapshot.json 확정 스냅샷 무결성 테스트.

홈 화면은 DB 대신 커밋된 스냅샷을 렌더링한다 (전 과정 종료로 수치 확정).
build_home_snapshot.py로 재생성할 때 확정 수치가 깨지면 여기서 잡는다.
"""
import json
from pathlib import Path

import pytest

SNAPSHOT_PATH = Path(__file__).parent.parent / "data" / "home_snapshot.json"

COURSE_COLUMNS = {
    'TRPR_DEGR', 'TRPR_NM', 'TR_STA_DT', 'TR_END_DT', 'TOT_FXNUM',
    'TOT_PAR_MKS', 'FINI_CNT', 'EI_EMPL_RATE_3', 'TOTAL_RATE_6', '수료율', '상태',
}


@pytest.fixture(scope="module")
def snap():
    with open(SNAPSHOT_PATH, encoding="utf-8") as f:
        return json.load(f)


class TestSnapshotStructure:
    def test_file_exists(self):
        assert SNAPSHOT_PATH.exists(), "build_home_snapshot.py 실행 필요"

    def test_top_level_keys(self, snap):
        assert set(snap.keys()) >= {'generated_at', 'courses', 'attendance', 'benchmark', 'revenue'}

    def test_course_count_and_columns(self, snap):
        assert len(snap['courses']) == 25
        for row in snap['courses']:
            assert COURSE_COLUMNS <= set(row.keys())

    def test_attendance_rows(self, snap):
        assert len(snap['attendance']) == 25
        for row in snap['attendance']:
            assert {'TRPR_DEGR', 'ATT_RATE'} <= set(row.keys())

    def test_employment_rate_null_or_range(self, snap):
        # 취업률: HRD-Net 집계중 기수는 null 유지 (0과 구분), 값이 있으면 0~100
        for row in snap['courses']:
            rate = row['TOTAL_RATE_6']
            assert rate is None or 0 <= rate <= 100

    def test_revenue_rows(self, snap):
        assert len(snap['revenue']) == 25
        for row in snap['revenue']:
            assert {'TRPR_DEGR', 'actual_fee'} <= set(row.keys())


class TestSnapshotLedgerValues:
    """원장(핵심수치) 확정값 가드 — 재생성 시 이 값이 바뀌면 원장과 대조 필요."""

    def test_recruit_totals(self, snap):
        assert sum(r['TOT_PAR_MKS'] for r in snap['courses']) == 654
        assert sum(r['TOT_FXNUM'] for r in snap['courses']) == 750

    def test_completion_total(self, snap):
        assert sum(r['FINI_CNT'] for r in snap['courses']) == 566

    def test_benchmark_pinned_to_ledger(self, snap):
        bench = snap['benchmark']
        assert bench['mkt_recruit'] == 60.5
        assert bench['mkt_satis'] == 85.7
        assert bench['our_satis'] == 90.3

    def test_attendance_average(self, snap):
        rates = [r['ATT_RATE'] for r in snap['attendance']]
        assert round(sum(rates) / len(rates), 1) == 93.6

    def test_revenue_headline_pinned(self, snap):
        # 헤드라인은 원장 확정값 고정 (캐시 재계산 드리프트 무관)
        assert snap['kpi_revenue_eok'] == 104.1

    def test_revenue_rows_near_ledger(self, snap):
        # 개별 기수 합계는 실측 캐시 — 확정값과 크게 벌어지면 원장 대조 필요
        total_eok = sum(r['actual_fee'] for r in snap['revenue']) / 1e8
        assert abs(total_eok - 104.1) < 0.05
