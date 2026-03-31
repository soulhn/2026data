"""init_db.py 테이블 생성 및 멱등성 테스트"""
from init_db import init_all_tables


EXPECTED_TABLES = [
    "TB_COURSE_MASTER",
    "TB_TRAINEE_INFO",
    "TB_ATTENDANCE_LOG",
    "TB_MARKET_TREND",
    "TB_JOB_POSTING_KEYWORD",
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
        "IDX_MARKET_NCS", "IDX_MARKET_DATE", "IDX_MARKET_AREA",
        "IDX_ATTEND_DEGR", "IDX_ATTEND_DATE", "IDX_COURSE_END_DT",
    ]
    for idx in expected_indexes:
        assert idx in indexes, f"인덱스 {idx}가 생성되지 않음"
