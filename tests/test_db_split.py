"""시장 DB 분리 — 접속 라우팅(utils)·시장 테이블 초기화(init_db)·집계 캐시 2-커넥션(market_etl) 검증"""
import pytest

import utils
import init_db
import market_etl
from utils import MAIN_DB, MARKET_DB, db_for_sql, db_for_table, get_database_url, is_market_db_separate


class TestRouting:
    @pytest.mark.parametrize("table,expected", [
        ("TB_MARKET_TREND", MARKET_DB),
        ("tb_market_trend", MARKET_DB),
        ("TB_MARKET_CACHE", MAIN_DB),     # 캐시는 ETL 3종이 공유 → 메인
        ("TB_COURSE_MASTER", MAIN_DB),
    ])
    def test_db_for_table(self, table, expected):
        assert db_for_table(table) == expected

    @pytest.mark.parametrize("sql,expected", [
        ("SELECT * FROM TB_MARKET_TREND WHERE REGION = ?", MARKET_DB),
        ("select count(*) as cnt from tb_market_trend", MARKET_DB),
        ("SELECT CACHE_DATA FROM TB_MARKET_CACHE WHERE CACHE_KEY = ?", MAIN_DB),
        ("SELECT * FROM TB_MARKET_TREND_BACKUP", MAIN_DB),   # 단어 경계 — 유사 이름은 매칭 안 됨
        ("", MAIN_DB),
    ])
    def test_db_for_sql(self, sql, expected):
        assert db_for_sql(sql) == expected


class TestDatabaseUrl:
    def test_market_falls_back_to_main_when_unset(self, monkeypatch):
        monkeypatch.setenv("DATABASE_URL", "postgres://main")
        monkeypatch.delenv("DATABASE_URL_MARKET", raising=False)
        assert get_database_url() == "postgres://main"
        assert get_database_url(MARKET_DB) == "postgres://main"
        assert is_market_db_separate() is False

    def test_market_uses_own_url_when_set(self, monkeypatch):
        monkeypatch.setenv("DATABASE_URL", "postgres://main")
        monkeypatch.setenv("DATABASE_URL_MARKET", "postgres://market")
        assert get_database_url(MARKET_DB) == "postgres://market"
        assert get_database_url(MAIN_DB) == "postgres://main"
        assert is_market_db_separate() is True

    def test_same_url_is_not_separate(self, monkeypatch):
        monkeypatch.setenv("DATABASE_URL", "postgres://x")
        monkeypatch.setenv("DATABASE_URL_MARKET", "postgres://x")
        assert is_market_db_separate() is False


class TestInitSplit:
    def _tables(self, conn):
        cur = conn.cursor()
        cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
        return {r[0] for r in cur.fetchall()}

    def test_main_only_skips_market_table(self, mock_db_connection):
        init_db.init_all_tables(include_market=False)
        tables = self._tables(mock_db_connection)
        assert "TB_MARKET_CACHE" in tables and "TB_COURSE_MASTER" in tables
        assert "TB_MARKET_TREND" not in tables

    def test_market_tables_created_with_indexes(self, mock_db_connection):
        init_db.init_market_tables()
        init_db.init_market_tables()   # 멱등
        assert "TB_MARKET_TREND" in self._tables(mock_db_connection)
        cur = mock_db_connection.cursor()
        cur.execute("SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='TB_MARKET_TREND'")
        idx = {r[0] for r in cur.fetchall()}
        assert {"IDX_MARKET_NCS", "IDX_MARKET_DATE", "IDX_MARKET_YEAR_MONTH", "IDX_MARKET_REGION", "IDX_MARKET_TARGET"} <= idx

    def test_market_init_uses_market_db(self, monkeypatch, mock_db_connection):
        """init_market_tables는 db="market"으로 접속해야 한다 (미설정 환경에선 메인으로 폴백)."""
        seen = []
        real = utils.get_connection
        monkeypatch.setattr(init_db, "get_connection", lambda **kw: (seen.append(kw.get("db")), real())[1])
        init_db.init_market_tables()
        assert seen == [MARKET_DB]


class TestCacheAggregationTwoConnections:
    def test_reads_market_writes_main(self, monkeypatch, mock_db_connection):
        """집계는 시장 DB에서 읽고 TB_MARKET_CACHE는 메인 DB에 쓴다 — 커넥션 요청 db를 검증."""
        seen = []
        real = utils.get_connection
        monkeypatch.setattr(market_etl, "get_connection", lambda **kw: (seen.append(kw.get("db", MAIN_DB)), real())[1])
        monkeypatch.setattr(market_etl, "is_pg", lambda: False)
        monkeypatch.setattr(market_etl, "adapt_query", utils.adapt_query)
        init_db.init_all_tables()
        cur = mock_db_connection.cursor()
        cur.execute("INSERT INTO TB_MARKET_TREND (TRPR_ID, TRPR_DEGR, TR_STA_DT, YEAR_MONTH, REGION, TOT_FXNUM, REG_COURSE_MAN, TRAINST_NM, NCS_CD) "
                    "VALUES ('A', 1, '2026-01-10', '2026-01', '서울', 30, 20, '기관', '20')")
        mock_db_connection.commit()

        market_etl.compute_and_cache_aggregations()

        assert seen[:2] == [MARKET_DB, MAIN_DB]
        cur.execute("SELECT COUNT(*) AS cnt FROM TB_MARKET_CACHE")
        assert cur.fetchone()[0] >= 5


class TestSecretCleaning:
    """시크릿에 따옴표·접두어가 섞여 들어와도 접속 문자열만 남겨야 한다 (GitHub Actions 실패 재발 방지)."""

    @pytest.mark.parametrize("raw,expected", [
        ("postgresql://u:p@h:5432/db", "postgresql://u:p@h:5432/db"),
        ('"postgresql://u:p@h:5432/db"', "postgresql://u:p@h:5432/db"),
        ("'postgresql://u:p@h:5432/db'", "postgresql://u:p@h:5432/db"),
        ("  postgresql://u:p@h:5432/db\n", "postgresql://u:p@h:5432/db"),
        ('DATABASE_URL_MARKET="postgresql://u:p@h:5432/db"', "postgresql://u:p@h:5432/db"),
        ("postgresql://u:p%3D@h:5432/db?sslmode=require", "postgresql://u:p%3D@h:5432/db?sslmode=require"),  # 쿼리스트링의 = 는 보존
        ("", None),
        (None, None),
    ])
    def test_clean(self, raw, expected):
        assert utils._clean_secret(raw) == expected

    def test_env_value_is_cleaned(self, monkeypatch):
        monkeypatch.setenv("DATABASE_URL_MARKET", '"postgres://market"')
        monkeypatch.setenv("DATABASE_URL", "postgres://main")
        assert get_database_url(MARKET_DB) == "postgres://market"
