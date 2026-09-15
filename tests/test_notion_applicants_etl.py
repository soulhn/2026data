"""notion_applicants_etl.py — 신청자 페이지 파싱·증분 조회·upsert·전이 로그 테스트"""
from datetime import datetime
from unittest.mock import MagicMock

import pytest

import config
import init_db
import notion_applicants_etl as etl
import utils
from notion_applicants_etl import (
    APPLICANT_COLUMNS, TRACKED_FIELDS, fetch_changed_pages, name_hash, parse_applicant_page,
    since_from_state, upsert_applicants,
)


def _page(pid, name, status, cohort="AIO3", edited="2026-09-15T01:00:00.000Z", extra=None):
    props = {
        "이름": {"type": "title", "title": [{"plain_text": name}]},
        "최종기수": {"type": "select", "select": {"name": cohort}},
        "기수": {"type": "rich_text", "rich_text": [{"plain_text": "3기"}]},
        "최종결과": {"type": "select", "select": {"name": status} if status else None},
        "처리결과": {"type": "select", "select": {"name": "이관완료"}},
        "신청일시": {"type": "rich_text", "rich_text": [{"plain_text": "2026-08-01 10:00"}]},
        "유입경로": {"type": "rich_text", "rich_text": [{"plain_text": "홈페이지"}]},
        "HRD 등록 일시": {"type": "date", "date": {"start": "2026-09-10"}},
        "HRD 신청(현장)": {"type": "checkbox", "checkbox": False},
        "합격자등록": {"type": "checkbox", "checkbox": True},
        "OT참석": {"type": "select", "select": {"name": "O"}},
        "연락처": {"type": "phone_number", "phone_number": "010-0000-0000"},
        "연락담당자": {"type": "people", "people": [{"name": "담당자"}]},
    }
    if extra:
        props.update(extra)
    return {"id": pid, "url": f"https://n/{pid}", "created_time": "2026-08-01T01:00:00.000Z",
            "last_edited_time": edited, "properties": props}


@pytest.fixture
def db(monkeypatch, mock_db_connection):
    monkeypatch.setattr(etl, "adapt_query", utils.adapt_query)
    init_db.init_all_tables(include_market=False)
    return mock_db_connection


class TestParse:
    def test_columns_and_privacy(self):
        row = parse_applicant_page(_page("p1", "홍길동", "HRD등록"))
        assert list(row.keys()) == APPLICANT_COLUMNS
        assert row["NAME_HASH"] == name_hash("홍길동") and row["NAME_MASKED"] == "홍*동"
        assert "홍길동" not in str(row.values())
        assert "010" not in str(row.values()) and "담당자" not in str(row.values())
        assert row["STATUS"] == "HRD등록" and row["COHORT"] == "AIO3" and row["HRD_REG_AT"] == "2026-09-10"
        assert row["PASS_REG"] == 1 and row["HRD_ONSITE"] == 0 and row["OT_ATTEND"] == "O"
        assert row["ATTEND_DT"] is None and row["HRD_APPLY_AT"] is None   # 신규 속성 아직 없음

    def test_reads_new_properties_when_present(self):
        extra = {config.NOTION_ATTEND_PROP: {"type": "date", "date": {"start": "2026-09-15"}},
                 config.NOTION_HRD_APPLY_PROP: {"type": "date", "date": {"start": "2026-09-05"}}}
        row = parse_applicant_page(_page("p1", "홍길동", "HRD등록", extra=extra))
        assert row["ATTEND_DT"] == "2026-09-15" and row["HRD_APPLY_AT"] == "2026-09-05"

    def test_name_hash_ignores_spaces(self):
        assert name_hash("홍 길동") == name_hash("홍길동")
        assert name_hash(None) is None


class TestSince:
    def test_none_when_first_run(self):
        assert since_from_state(None) is None

    def test_rewinds_by_overlap(self):
        assert since_from_state("2026-09-15T08:00:00Z", overlap_min=10) == "2026-09-15T07:50:00.000Z"


class TestFetch:
    def test_incremental_filter_payload(self):
        session = MagicMock()
        resp = MagicMock(); resp.status_code = 200; resp.json.return_value = {"results": [], "has_more": False}
        session.post.return_value = resp
        fetch_changed_pages("tok", "2026-09-15T07:50:00.000Z", session=session)
        body = session.post.call_args.kwargs["json"]
        assert body["filter"] == {"timestamp": "last_edited_time", "last_edited_time": {"on_or_after": "2026-09-15T07:50:00.000Z"}}
        assert body["sorts"][0]["timestamp"] == "last_edited_time"
        assert session.post.call_args.args[0].endswith(f"/databases/{config.NOTION_APPLICANTS_DB_ID}/query")

    def test_full_scan_has_no_filter(self):
        session = MagicMock()
        resp = MagicMock(); resp.status_code = 200; resp.json.return_value = {"results": [], "has_more": False}
        session.post.return_value = resp
        fetch_changed_pages("tok", None, session=session)
        assert "filter" not in session.post.call_args.kwargs["json"]


class TestUpsert:
    def _rows(self, *pages):
        return [parse_applicant_page(p) for p in pages]

    def test_insert_logs_initial_status(self, db):
        ins, upd, tr = upsert_applicants(db, self._rows(_page("p1", "홍길동", "HRD신청")), now=datetime(2026, 9, 15, 1))
        assert (ins, upd, tr) == (1, 0, 1)
        cur = db.cursor()
        cur.execute("SELECT FIELD, OLD_VALUE, NEW_VALUE FROM TB_APPLICANT_STATUS_LOG")
        assert cur.fetchall() == [("STATUS", None, "HRD신청")]

    def test_status_change_is_logged_and_row_updated(self, db):
        upsert_applicants(db, self._rows(_page("p1", "홍길동", "HRD신청")), now=datetime(2026, 9, 15, 1))
        ins, upd, tr = upsert_applicants(
            db, self._rows(_page("p1", "홍길동", "HRD등록", edited="2026-09-15T02:00:00.000Z")), now=datetime(2026, 9, 15, 3))
        assert (ins, upd, tr) == (0, 1, 1)
        cur = db.cursor()
        cur.execute("SELECT OLD_VALUE, NEW_VALUE, NOTION_EDITED_AT FROM TB_APPLICANT_STATUS_LOG WHERE FIELD='STATUS' ORDER BY DETECTED_AT")
        assert cur.fetchall()[-1] == ("HRD신청", "HRD등록", "2026-09-15T02:00:00.000Z")
        cur.execute("SELECT STATUS FROM TB_APPLICANT WHERE NOTION_PAGE_ID='p1'")
        assert cur.fetchone()[0] == "HRD등록"

    def test_same_row_reread_in_overlap_is_noop(self, db):
        page = _page("p1", "홍길동", "HRD등록")
        upsert_applicants(db, self._rows(page), now=datetime(2026, 9, 15, 1))
        assert upsert_applicants(db, self._rows(page), now=datetime(2026, 9, 15, 2)) == (0, 0, 0)

    def test_untracked_edit_updates_without_log(self, db):
        upsert_applicants(db, self._rows(_page("p1", "홍길동", "HRD등록")), now=datetime(2026, 9, 15, 1))
        p2 = _page("p1", "홍길동", "HRD등록", edited="2026-09-15T02:00:00.000Z")
        p2["properties"]["유입경로"] = {"type": "rich_text", "rich_text": [{"plain_text": "지인추천"}]}
        assert upsert_applicants(db, self._rows(p2), now=datetime(2026, 9, 15, 3)) == (0, 1, 0)

    def test_regression_from_hrd_reg_to_cancel_is_logged(self, db):
        upsert_applicants(db, self._rows(_page("p1", "홍길동", "HRD등록")), now=datetime(2026, 9, 15, 1))
        upsert_applicants(db, self._rows(_page("p1", "홍길동", "합격취소(신청자 요청)", edited="2026-09-16T00:00:00.000Z")),
                          now=datetime(2026, 9, 16, 1))
        cur = db.cursor()
        cur.execute("SELECT NEW_VALUE FROM TB_APPLICANT_STATUS_LOG WHERE FIELD='STATUS' ORDER BY DETECTED_AT")
        assert [r[0] for r in cur.fetchall()] == ["HRD등록", "합격취소(신청자 요청)"]

    def test_tracked_fields_include_attendance(self):
        assert {"STATUS", "COHORT", "HRD_REG_AT", "ATTEND_DT"} <= set(TRACKED_FIELDS)


class TestRun:
    def test_run_sets_sync_state_and_uses_overlap(self, db, monkeypatch):
        calls = []

        def fake_fetch(token, since, session=None):
            calls.append(since)
            return [_page("p1", "홍길동", "HRD신청")]

        monkeypatch.setattr(etl, "fetch_changed_pages", fake_fetch)
        assert etl.run("tok", db, now=datetime(2026, 9, 15, 8)) == (1, 1, 0, 1)
        assert etl.get_sync_state(db) == "2026-09-15T08:00:00Z"
        etl.run("tok", db, now=datetime(2026, 9, 15, 9))
        assert calls == [None, "2026-09-15T07:50:00.000Z"]
