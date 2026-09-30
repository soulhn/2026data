"""init_db.py 테이블 생성 및 멱등성 테스트"""
from init_db import init_all_tables


EXPECTED_TABLES = [
    "TB_COURSE_MASTER",
    "TB_TRAINEE_INFO",
    "TB_ATTENDANCE_LOG",
    "TB_MARKET_TREND",
    "TB_JOB_POSTING_KEYWORD",
    "TB_JOB_POSTING_TRACK",
]


def test_tables_created(mock_db_connection):
    """init_all_tables가 4개 테이블을 생성하는지 확인"""
    init_all_tables()
    cursor = mock_db_connection.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = {row[0] for row in cursor.fetchall()}
    for t in EXPECTED_TABLES:
        assert t in tables, f"테이블 {t}가 생성되지 않음"


def test_idempotent(mock_db_connection):
    """init_all_tables를 두 번 호출해도 에러 없음 (멱등성)"""
    init_all_tables()
    init_all_tables()  # 두 번째 호출에서 에러 없어야 함
    cursor = mock_db_connection.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = {row[0] for row in cursor.fetchall()}
    assert len(tables & set(EXPECTED_TABLES)) == len(EXPECTED_TABLES)


def test_indexes_created(mock_db_connection):
    """인덱스가 생성되었는지 확인"""
    init_all_tables()
    cursor = mock_db_connection.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='index'")
    indexes = {row[0] for row in cursor.fetchall()}
    expected_indexes = [
        "IDX_MARKET_NCS", "IDX_MARKET_DATE",
        "IDX_ATTEND_DEGR", "IDX_ATTEND_DATE", "IDX_COURSE_END_DT",
    ]
    for idx in expected_indexes:
        assert idx in indexes, f"인덱스 {idx}가 생성되지 않음"
    # 2026-08 제거된 미사용 인덱스가 재생성되지 않는지 (init_db 목록에서 빠졌는지)
    for idx in ("IDX_MARKET_AREA", "IDX_MARKET_TRAINST"):
        assert idx not in indexes, f"제거된 인덱스 {idx}가 다시 생성됨"


# ---------------------------------------------------------------------------
# RLS (Supabase 보안 경고 2026-09-30) — public 테이블에 Row-Level Security 활성화
# ---------------------------------------------------------------------------
def test_enable_rls_noop_on_sqlite(mock_db_connection):
    """SQLite(테스트·CI) 경로에서는 pg_tables 조회 없이 아무것도 하지 않는다."""
    import init_db
    cursor = mock_db_connection.cursor()
    assert init_db.enable_rls(mock_db_connection, cursor) == []


class _FakeCursor:
    def __init__(self, rls_off):
        self.rls_off = rls_off
        self.executed = []

    def execute(self, sql):
        self.executed.append(sql)

    def fetchall(self):
        return [(t,) for t in self.rls_off]


class _FakeConn:
    def commit(self):
        pass

    def rollback(self):
        pass


def test_enable_rls_targets_only_tables_without_rls(monkeypatch):
    """PG 경로: pg_tables에서 rowsecurity=false인 테이블만 골라 ENABLE ROW LEVEL SECURITY."""
    import init_db
    monkeypatch.setattr(init_db, "is_pg", lambda: True)
    cur = _FakeCursor(["tb_roster_member", "tb_sync_state"])
    enabled = init_db.enable_rls(_FakeConn(), cur)
    assert enabled == ["tb_roster_member", "tb_sync_state"]
    alters = [q for q in cur.executed if q.startswith("ALTER TABLE")]
    assert alters == [
        'ALTER TABLE public."tb_roster_member" ENABLE ROW LEVEL SECURITY',
        'ALTER TABLE public."tb_sync_state" ENABLE ROW LEVEL SECURITY',
    ]
    assert "NOT rowsecurity" in cur.executed[0]


def test_enable_rls_nothing_to_do(monkeypatch):
    """모든 테이블이 이미 켜져 있으면 ALTER를 한 번도 실행하지 않는다 (멱등)."""
    import init_db
    monkeypatch.setattr(init_db, "is_pg", lambda: True)
    cur = _FakeCursor([])
    assert init_db.enable_rls(_FakeConn(), cur) == []
    assert not [q for q in cur.executed if q.startswith("ALTER TABLE")]
