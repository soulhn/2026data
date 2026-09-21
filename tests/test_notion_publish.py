"""notion_publish.py — 노션 쓰기 헬퍼: 속성 변환·해시·upsert·DB 레이아웃/속성 관리·429 재시도 (노션 API 모킹)."""
from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

import config
import init_db
import notion_publish as pub
import utils
from notion_publish import (
    content_hash, ensure_database_layout, ensure_properties, publish, remove_properties, to_properties,
)


@pytest.fixture
def db(monkeypatch, mock_db_connection):
    monkeypatch.setattr(pub, "adapt_query", utils.adapt_query)
    init_db.init_all_tables(include_market=False)
    return mock_db_connection


def _fake_session():
    session = MagicMock()
    counter = {"n": 0}

    def request(method, url, headers=None, json=None, timeout=None):
        resp = MagicMock()
        resp.status_code = 200
        resp.headers = {}
        counter["n"] += 1
        resp.json.return_value = {"id": f"page-{counter['n']}"}
        return resp
    session.request.side_effect = request
    return session


SCHEMA = {"이름": {"title": {}}, "메모": {"rich_text": {}}, "기수": {"select": {"options": []}}, "값": {"number": {}},
          "날짜": {"date": {}}, "체크": {"checkbox": {}}, "관계": {"relation": {"database_id": "x", "single_property": {}}},
          "링크": {"url": {}}, "글": {"rich_text": {}}}


class TestProperties:
    def test_types(self):
        props = to_properties(SCHEMA, {"이름": "홍*동 · AIO3", "메모": "지워지면 안 됨", "기수": "AIO3", "값": 3, "날짜": "20260915",
                                      "체크": True, "관계": "p1", "링크": "https://notion.so/x", "글": "t"})
        assert props["이름"]["title"][0]["text"]["content"] == "홍*동 · AIO3"
        assert props["기수"] == {"select": {"name": "AIO3"}} and props["값"] == {"number": 3.0}
        assert props["날짜"] == {"date": {"start": "2026-09-15"}}                  # YYYYMMDD → ISO
        assert props["체크"] == {"checkbox": True} and props["관계"] == {"relation": [{"id": "p1"}]}
        assert props["링크"] == {"url": "https://notion.so/x"}
        assert "메모" not in props                                                 # 담당자 열은 절대 안 쓴다

    def test_null_and_missing(self):
        props = to_properties(SCHEMA, {"이름": "x", "기수": None, "날짜": None, "값": None, "관계": None, "링크": None})
        assert props["기수"] == {"select": None} and props["날짜"] == {"date": None} and props["값"] == {"number": None}
        assert props["관계"] == {"relation": []} and props["링크"] == {"url": None}
        assert "체크" not in props and "글" not in props                            # 행에 없는 키는 건드리지 않는다

    def test_hash_ignores_timestamps_only(self):
        a = {"KEY": "x", "값": 1, "변경 시각": "t1", "갱신 시각": "u1"}
        b = {"KEY": "x", "값": 1, "변경 시각": "t2", "갱신 시각": "u2"}
        assert content_hash(a) == content_hash(b) and content_hash(a) != content_hash({**a, "값": 2})


class TestPublish:
    def test_create_then_skip_then_update(self, db, monkeypatch):
        monkeypatch.setattr(config, "NOTION_WRITE_INTERVAL", 0)
        session = _fake_session()
        rows = [{"KEY": "AIO3", "이름": "AIO3", "값": 2, "갱신 시각": "t1"}]
        assert publish("tok", db, "k", "db1", SCHEMA, rows, session=session, now=datetime(2026, 9, 15, 9)) == (1, 0, 0)
        assert session.request.call_args.args[:2] == ("POST", f"{config.NOTION_API_BASE}/pages")
        rows[0]["갱신 시각"] = "t2"
        assert publish("tok", db, "k", "db1", SCHEMA, rows, session=session) == (0, 0, 1)      # 내용 같음 → 건너뜀
        rows[0]["값"] = 3
        assert publish("tok", db, "k", "db1", SCHEMA, rows, session=session) == (0, 1, 0)
        assert session.request.call_args.args[:2] == ("PATCH", f"{config.NOTION_API_BASE}/pages/page-1")


class TestDatabaseAdmin:
    def test_layout_skips_when_inline_with_same_description(self, monkeypatch):
        monkeypatch.setattr(config, "NOTION_WRITE_INTERVAL", 0)
        session = _fake_session()
        meta = {"is_inline": True, "description": [{"plain_text": "설명"}]}
        assert ensure_database_layout("tok", "db1", "설명", meta, session=session) is False
        session.request.assert_not_called()
        assert ensure_database_layout("tok", "db1", "다른 설명", meta, session=session) is True
        assert session.request.call_args.args[:2] == ("PATCH", f"{config.NOTION_API_BASE}/databases/db1")

    def test_ensure_properties_adds_only_missing(self, monkeypatch):
        monkeypatch.setattr(config, "NOTION_WRITE_INTERVAL", 0)
        session = _fake_session()
        meta = {"properties": {k: {} for k in SCHEMA if k not in ("링크", "체크")}}
        assert ensure_properties("tok", "db1", SCHEMA, meta, session=session) == ["체크", "링크"]
        assert set(session.request.call_args.kwargs["json"]["properties"]) == {"체크", "링크"}     # 있는 속성은 다시 보내지 않는다
        assert ensure_properties("tok", "db1", SCHEMA, {"properties": dict.fromkeys(SCHEMA, {})}, session=session) == []

    def test_remove_properties_only_when_present(self, monkeypatch):
        monkeypatch.setattr(config, "NOTION_WRITE_INTERVAL", 0)
        session = _fake_session()
        assert remove_properties("tok", "db1", ("놓침", "KEY"), {"properties": {"기수": {}, "놓침": {}}}, session=session) == ["놓침"]
        assert session.request.call_args.kwargs["json"] == {"properties": {"놓침": None}}         # null = 삭제
        assert remove_properties("tok", "db1", ("놓침",), {"properties": {"기수": {}}}, session=session) == []

    @patch("notion_publish.time.sleep")
    def test_429_retries(self, _sleep, monkeypatch):
        monkeypatch.setattr(config, "NOTION_WRITE_INTERVAL", 0)
        session = MagicMock()
        r429 = MagicMock()
        r429.status_code = 429
        r429.headers = {"Retry-After": "1"}
        r200 = MagicMock()
        r200.status_code = 200
        r200.headers = {}
        r200.json.return_value = {"id": "ok"}
        session.request.side_effect = [r429, r200]
        assert pub._request("tok", "GET", "/databases/x", session=session)["id"] == "ok"
