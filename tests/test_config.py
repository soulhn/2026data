"""config.py 상수 타입/범위 검증 테스트"""
import datetime as dt
from config import (
    LATE_CUTOFF_HHMM, ATTENDANCE_TARGET,
    RISK_ABSENT, RISK_LATE, RISK_EARLY_LEAVE,
    CACHE_TTL_DEFAULT, CACHE_TTL_REALTIME, CACHE_TTL_MARKET,
    ETL_ARCHIVE_START, ETL_REFRESH_MONTHS, ETL_PAGE_SIZE,
    ETL_MAX_WORKERS, ETL_BATCH_SIZE, ETL_BATCH_PAGE_SIZE,
    COST_BINS, COST_BIN_LABELS,
    SCATTER_SAMPLE_LIMIT, REGRESSION_SAMPLE_LIMIT,
)


def test_attendance_constants_positive():
    assert LATE_CUTOFF_HHMM > 0
    assert 0 < ATTENDANCE_TARGET <= 100


def test_risk_thresholds_positive():
    assert RISK_ABSENT > 0
    assert RISK_LATE > 0
    assert RISK_EARLY_LEAVE > 0


def test_cache_ttl_ordering():
    assert CACHE_TTL_REALTIME <= CACHE_TTL_DEFAULT <= CACHE_TTL_MARKET


def test_etl_archive_start_is_date():
    assert isinstance(ETL_ARCHIVE_START, dt.date)
    assert ETL_ARCHIVE_START.year >= 2020


def test_etl_parameters_positive():
    assert ETL_REFRESH_MONTHS > 0
    assert ETL_PAGE_SIZE > 0
    assert ETL_MAX_WORKERS >= 1
    assert ETL_BATCH_SIZE > 0
    assert ETL_BATCH_PAGE_SIZE > 0


def test_cost_bins_and_labels_match():
    assert len(COST_BIN_LABELS) == len(COST_BINS) - 1


def test_sample_limits_positive():
    assert SCATTER_SAMPLE_LIMIT > 0
    assert REGRESSION_SAMPLE_LIMIT > 0
