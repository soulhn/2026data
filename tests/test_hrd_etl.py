"""hrd_etl.py 유틸리티 함수 + batch_execute 테스트"""
import sqlite3
from hrd_etl import clean_time, get_month_list, batch_execute


class TestCleanTime:
    def test_valid_time(self):
        assert clean_time("0930") == "09:30"

    def test_midnight(self):
        assert clean_time("0000") is None

    def test_none(self):
        assert clean_time(None) is None

    def test_empty_string(self):
        assert clean_time("") is None

    def test_short_string(self):
        assert clean_time("09") is None

    def test_long_string(self):
        assert clean_time("09300") is None

    def test_afternoon(self):
        assert clean_time("1430") == "14:30"


class TestGetMonthList:
    def test_same_month(self):
        result = get_month_list("2024-01-15", "2024-01-20")
        assert result == ["202401"]

    def test_three_months(self):
        result = get_month_list("2024-01-01", "2024-03-31")
        assert result == ["202401", "202402", "202403"]

    def test_cross_year(self):
        result = get_month_list("2023-11-01", "2024-02-28")
        assert result == ["202311", "202312", "202401", "202402"]

    def test_empty_on_none(self):
        assert get_month_list(None, "2024-01-01") == []
        assert get_month_list("2024-01-01", None) == []

    def test_single_month(self):
        result = get_month_list("2024-06-01", "2024-06-30")
        assert len(result) == 1


class TestBatchExecute:
    def test_empty_list(self, monkeypatch):
        import utils
        monkeypatch.setattr(utils, "is_pg", lambda: False)
        s, e = batch_execute(None, "SELECT 1", [])
        assert s == 0 and e == 0

    def test_sqlite_insert(self, monkeypatch):
        import utils
        monkeypatch.setattr(utils, "is_pg", lambda: False)
        conn = sqlite3.connect(":memory:")
        conn.execute("CREATE TABLE t (a TEXT, b INTEGER)")
        cursor = conn.cursor()
        data = [("x", 1), ("y", 2), ("z", 3)]
        s, e = batch_execute(cursor, "INSERT INTO t VALUES (?, ?)", data)
        conn.commit()
        assert s == 3
        assert e == 0
        cursor.execute("SELECT COUNT(*) FROM t")
        assert cursor.fetchone()[0] == 3
        conn.close()

    def test_fallback_on_error(self, monkeypatch):
        """배치 실패 시 row-by-row 폴백 동작 확인"""
        import utils
        monkeypatch.setattr(utils, "is_pg", lambda: False)
        conn = sqlite3.connect(":memory:")
        conn.execute("CREATE TABLE t (a TEXT PRIMARY KEY)")
        cursor = conn.cursor()
        # 중복 키가 포함된 데이터: executemany가 실패하면 row-by-row 폴백
        data = [("a",), ("b",), ("a",)]  # 'a' 중복
        s, e = batch_execute(cursor, "INSERT INTO t VALUES (?)", data)
        conn.commit()
        # executemany가 성공하면 3, 0 (SQLite는 중복 시 에러)
        # 폴백이면 2 성공, 1 실패
        assert s + e == 3
        conn.close()
