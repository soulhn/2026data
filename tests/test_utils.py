"""utils.py 유틸리티 함수 테스트"""
from utils import adapt_query, safe_float, safe_int, calculate_age_at_training


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
