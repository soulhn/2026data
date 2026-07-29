"""hrd_etl.py 유틸리티 함수 + batch_execute 테스트"""
import logging
import sqlite3
import hrd_etl
from hrd_etl import get_month_list, batch_execute
from config import ETL_FAILED_ROW_SAMPLE
from utils import clean_time


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
        monkeypatch.setattr(hrd_etl, "is_pg", lambda: False)
        s, e = batch_execute(None, "SELECT 1", [])
        assert s == 0 and e == 0

    def test_sqlite_insert(self, monkeypatch):
        monkeypatch.setattr(hrd_etl, "is_pg", lambda: False)
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
        """배치 실패 시 row-by-row 폴백: 중복 'a' 실패, 'b'·'c' 성공"""
        monkeypatch.setattr(hrd_etl, "is_pg", lambda: False)
        conn = sqlite3.connect(":memory:")
        conn.execute("CREATE TABLE t (a TEXT PRIMARY KEY)")
        conn.execute("INSERT INTO t VALUES ('a')")  # 선점 행
        cursor = conn.cursor()
        # 첫 행부터 중복 → executemany가 아무것도 삽입하지 못한 채 실패 → row-by-row 폴백
        # (SQLite executemany는 원자적이지 않아, 중복을 뒤에 두면 앞 행이 남아 폴백 결과가 달라짐)
        data = [("a",), ("b",), ("c",)]
        s, e = batch_execute(cursor, "INSERT INTO t VALUES (?)", data)
        conn.commit()
        assert s == 2  # 'b','c' 성공
        assert e == 1  # 중복 'a' 실패
        # DB 상태 검증: 선점 'a' + 신규 'b','c'
        rows = conn.execute("SELECT a FROM t ORDER BY a").fetchall()
        assert [r[0] for r in rows] == ['a', 'b', 'c']
        conn.close()


class FakePgCursor:
    """PG 트랜잭션 의미론을 흉내내는 커서.

    - 문이 하나라도 실패하면 aborted 상태가 되어 이후 모든 문이 실패한다
    - `ROLLBACK TO SAVEPOINT` 만 aborted를 해제하고, 해당 시점 이후 삽입을 되돌린다
    """

    def __init__(self, bad_rows=(), detail_row=None):
        self.bad = {repr(r) for r in bad_rows}
        self.detail_row = detail_row  # PG가 DETAIL 줄에 덧붙이는 원본 행 (개인정보 유출 재현용)
        self.aborted = False
        self.rows = []          # 커밋 가능한 상태로 남은 행
        self.stmts = []         # 발행된 제어문 기록
        self._marks = {}        # savepoint 이름 → 당시 rows 길이
        self._pending = []      # mogrify로 예약된 배치 행

    def _fail(self, suffix=""):
        """PG 스타일 다중 줄 오류 — 첫 줄은 원인, DETAIL 줄은 행 전체를 노출한다."""
        msg = f'null value in column "trnee_nm" violates not-null constraint{suffix}'
        if self.detail_row is not None:
            values = ", ".join("null" if v is None else str(v) for v in self.detail_row)
            msg += f"\nDETAIL:  Failing row contains ({values})."
        return RuntimeError(msg)

    def mogrify(self, sql, args=None):
        self._pending.append(args)
        return repr(args).encode()

    def execute(self, sql, params=None):
        if isinstance(sql, bytes):
            sql = sql.decode()
        text = sql.strip()
        upper = text.upper()

        if upper.startswith("ROLLBACK TO SAVEPOINT"):
            name = text.split()[-1]
            self.rows = self.rows[: self._marks[name]]
            self.aborted = False
            self.stmts.append(f"ROLLBACK TO {name}")
            return
        if self.aborted:
            raise RuntimeError("current transaction is aborted (InFailedSqlTransaction)")
        if upper.startswith("RELEASE SAVEPOINT"):
            self.stmts.append(f"RELEASE {text.split()[-1]}")
            return
        if upper.startswith("SAVEPOINT"):
            name = text.split()[-1]
            self._marks[name] = len(self.rows)
            self.stmts.append(f"SAVEPOINT {name}")
            return

        if params is not None:                      # row-by-row 폴백
            if repr(params) in self.bad:
                self.aborted = True
                raise self._fail()
            self.rows.append(params)
            return

        batch, self._pending = self._pending, []    # execute_batch가 이어붙인 문
        if any(repr(r) in self.bad for r in batch):
            self.aborted = True
            raise self._fail(" (batch)")
        self.rows.extend(batch)


class TestBatchExecutePostgres:
    """PG 경로: 배치 실패 후에도 정상 행이 살아남는지 (SAVEPOINT 격리) 검증."""

    def test_batch_failure_recovers_good_rows(self, monkeypatch):
        monkeypatch.setattr(hrd_etl, "is_pg", lambda: True)
        cursor = FakePgCursor(bad_rows=[("bad",)])
        data = [("a",), ("bad",), ("c",)]

        s, e = batch_execute(cursor, "INSERT INTO t VALUES (?)", data)

        # SAVEPOINT 없이는 배치 실패 후 폴백이 전량 실패해 (0, 3)이 된다
        assert (s, e) == (2, 1)
        assert cursor.rows == [("a",), ("c",)]
        assert not cursor.aborted
        assert "ROLLBACK TO batch_sp" in cursor.stmts       # 배치 실패 복구
        assert "ROLLBACK TO row_sp" in cursor.stmts         # 나쁜 행만 격리

    def test_batch_success_releases_savepoint(self, monkeypatch):
        monkeypatch.setattr(hrd_etl, "is_pg", lambda: True)
        cursor = FakePgCursor()
        data = [("a",), ("b",)]

        s, e = batch_execute(cursor, "INSERT INTO t VALUES (?)", data)

        assert (s, e) == (2, 0)
        assert cursor.rows == data
        assert cursor.stmts == ["SAVEPOINT batch_sp", "RELEASE batch_sp"]

    def test_empty_list_issues_no_savepoint(self, monkeypatch):
        monkeypatch.setattr(hrd_etl, "is_pg", lambda: True)
        cursor = FakePgCursor()
        assert batch_execute(cursor, "INSERT INTO t VALUES (?)", []) == (0, 0)
        assert cursor.stmts == []


class TestBatchExecuteFailureLogging:
    """폴백 실패 행의 진단 정보는 남기되, 개인정보는 로그에 남기지 않는지 검증."""

    def test_logs_row_index_and_shape(self, monkeypatch, caplog):
        monkeypatch.setattr(hrd_etl, "is_pg", lambda: True)
        bad = ("AIG123", 2, None, "홍길동")
        cursor = FakePgCursor(bad_rows=[bad])

        with caplog.at_level(logging.WARNING, logger="hrd_etl"):
            batch_execute(cursor, "INSERT INTO t VALUES (?, ?, ?, ?)",
                          [("AIG123", 1, "x", "김철수"), bad])

        row_log = [r for r in caplog.messages if "행 1 실패" in r]
        assert len(row_log) == 1, caplog.messages
        assert "not-null" in row_log[0]                       # 원인
        assert "[str(6), int, None, str(3)]" in row_log[0]    # 형태 요약

    def test_does_not_leak_personal_data(self, monkeypatch, caplog):
        """실패 행의 이름·생년월일이 로그에 노출되면 안 된다 (GitHub Actions 로그 유출 방지)."""
        monkeypatch.setattr(hrd_etl, "is_pg", lambda: True)
        bad = ("AIG123", 1, "홍길동", "19900101")
        cursor = FakePgCursor(bad_rows=[bad], detail_row=bad)

        with caplog.at_level(logging.WARNING, logger="hrd_etl"):
            batch_execute(cursor, "INSERT INTO t VALUES (?, ?, ?, ?)", [bad])

        joined = "\n".join(caplog.messages)
        assert "홍길동" not in joined      # 이름
        assert "19900101" not in joined   # 생년월일
        assert "Failing row contains" not in joined  # PG DETAIL 줄 자체가 제거돼야 함
        assert "not-null" in joined       # 진단 정보는 남아야 함

    def test_caps_sample_and_reports_remainder(self, monkeypatch, caplog):
        monkeypatch.setattr(hrd_etl, "is_pg", lambda: True)
        rows = [(f"bad{i}",) for i in range(6)]
        cursor = FakePgCursor(bad_rows=rows)

        with caplog.at_level(logging.WARNING, logger="hrd_etl"):
            s, e = batch_execute(cursor, "INSERT INTO t VALUES (?)", rows)

        assert (s, e) == (0, 6)
        sampled = [r for r in caplog.messages if "실패:" in r]
        assert len(sampled) == ETL_FAILED_ROW_SAMPLE
        assert any(f"나머지 {6 - ETL_FAILED_ROW_SAMPLE}건 생략" in r for r in caplog.messages)
