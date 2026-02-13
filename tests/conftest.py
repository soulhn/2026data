import sqlite3
import pytest
import sys
import os

# 프로젝트 루트를 import 경로에 추가
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import utils
import init_db


class _NoCloseConnection:
    """sqlite3.Connection을 래핑하여 close()를 무효화하는 프록시.
    init_all_tables 등 내부에서 conn.close()를 호출해도 실제로 닫히지 않음."""
    def __init__(self, conn):
        self._conn = conn

    def close(self):
        pass  # 무시

    def __getattr__(self, name):
        return getattr(self._conn, name)


@pytest.fixture
def in_memory_db():
    """인메모리 SQLite DB 연결"""
    conn = sqlite3.connect(":memory:")
    yield conn
    conn.close()


@pytest.fixture
def mock_db_connection(monkeypatch, in_memory_db):
    """utils.get_connection을 인메모리 DB로 교체하는 fixture.
    init_db 모듈에서도 직접 import하므로 양쪽 모두 패치합니다."""
    wrapper = _NoCloseConnection(in_memory_db)
    _get_conn = lambda **kwargs: wrapper
    _is_pg = lambda: False
    # utils 모듈 패치
    monkeypatch.setattr(utils, "get_connection", _get_conn)
    monkeypatch.setattr(utils, "is_pg", _is_pg)
    # init_db 모듈 패치 (from utils import ... 로 가져온 참조)
    monkeypatch.setattr(init_db, "get_connection", _get_conn)
    monkeypatch.setattr(init_db, "is_pg", _is_pg)
    yield in_memory_db
