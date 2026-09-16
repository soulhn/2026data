"""notion_kpi_publish.py — 속성 변환·행 생성(정합성 판정)·해시 기반 upsert 테스트 (인메모리 SQLite, 노션 API 모킹)"""
from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

import config
import init_db
import notion_kpi_publish as pub
import utils
from notion_kpi_publish import (
    COHORT_GUIDE, COHORT_SCHEMA, PERSON_GUIDE, PERSON_SCHEMA, attend_verdict, build_cohort_rows, build_person_rows,
    content_hash, ensure_database_layout, ensure_databases, ensure_page_guide, ensure_properties, guide_blocks, publish,
    to_properties,
)


@pytest.fixture
def db(monkeypatch, mock_db_connection):
    monkeypatch.setattr(pub, "adapt_query", utils.adapt_query)
    import notion_applicants_etl
    monkeypatch.setattr(notion_applicants_etl, "adapt_query", utils.adapt_query)
    init_db.init_all_tables(include_market=False)
    return mock_db_connection


def _seed(conn):
    """AIO 3회차: 승인 2명(명부), 신청자 4명 — 일치 / 노션만 등록 / HRD만 승인 / 대기 + 동명이인 케이스."""
    cur = conn.cursor()
    cur.execute("INSERT INTO TB_COURSE_SNAPSHOT (TRPR_ID, TRPR_DEGR, SNAP_AT, TR_STA_DT, TR_END_DT, TOT_FXNUM, TOT_TRP_CNT, TOT_PAR_MKS, FINI_CNT, "
                "ROSTER_CNT, ACTIVE_CNT, DROPOUT_CNT, PARTIAL_FINI_CNT, EARLY_EMPL_CNT, CHANGED) "
                "VALUES ('AIG20260000578396', 3, '2026-09-15 08:00:00', '2026-09-15', '2027-03-12', 30, 5, 2, 0, 2, 2, 0, 0, 0, 'first')")
    cur.execute("INSERT INTO TB_COURSE_SNAPSHOT (TRPR_ID, TRPR_DEGR, SNAP_AT, TR_STA_DT, TR_END_DT, TOT_FXNUM, TOT_TRP_CNT, TOT_PAR_MKS, FINI_CNT, "
                "ROSTER_CNT, ACTIVE_CNT, DROPOUT_CNT, PARTIAL_FINI_CNT, EARLY_EMPL_CNT, CHANGED) "
                "VALUES ('AIG20240000459068', 37, '2026-09-15 08:00:00', '2026-09-04', '2027-03-03', 30, 22, 21, 0, 21, 21, 0, 0, 0, 'first')")  # SKN → 제외
    for tid, h, status, seen, fa, gone in [("t1", "h_kim", "훈련중", "2026-09-15 08:00:00", "20260915", None),
                                           ("t2", "h_lee", "훈련중", "2026-09-16 09:00:00", None, None),
                                           ("t3", "h_dup", "훈련중", "2026-09-15 08:00:00", "20260915", None),
                                           ("t4", "h_dup", "훈련중", "2026-09-15 08:00:00", None, None),
                                           ("t5", "h_gone", "훈련중", "2026-09-15 08:00:00", None, "2026-09-16 13:00:00"),   # 승인됐다가 사라짐
                                           ("t6", "h_stay", "훈련중", "2026-09-15 08:00:00", "20260916", None),           # 노션은 취소인데 명부 잔류
                                           ("t7", "h_noshow", "훈련중", "2026-09-15 08:00:00", None, None)]:            # 개강 지났는데 출석 없음
        cur.execute("INSERT INTO TB_ROSTER_MEMBER (TRPR_ID, TRPR_DEGR, TRNEE_ID, NAME_HASH, NAME_MASKED, TR_STA_DT, STATUS, FIRST_SEEN_AT, LAST_SEEN_AT, FIRST_ATTEND_DT, FIRST_IN_TIME, GONE_AT) "
                    "VALUES ('AIG20260000578396', 3, ?, ?, 'x', '2026-09-15', ?, ?, ?, ?, '09:00', ?)", [tid, h, status, seen, seen, fa, gone])
    for pid, h, status, apply_at in [("p1", "h_kim", "HRD등록", "2026-09-01"),      # 일치
                                     ("p2", "h_park", "HRD등록", "2026-09-02"),     # 노션만 등록
                                     ("p3", "h_lee", "HRD신청", "2026-09-10"),      # HRD만 승인 (지연 승인 9/16)
                                     ("p4", "h_choi", "합격안내", None),            # 대기
                                     ("p5", "h_dup", "HRD등록", None),              # 동명이인 확인
                                     ("p7", "h_can", "합격취소(신청자 요청)", None),  # 취소
                                     ("p8", "h_gone", "HRD등록", None),             # 등록 후 이탈
                                     ("p9", "h_stay", "합격취소(연락두절)", None),   # 취소인데 명부 잔류
                                     ("p10", "h_noshow", "HRD등록", None),          # 일치 + 미참석
                                     ("p6", "h_no", "신청취소(본인요청)", None)]:   # 대상 아님
        cur.execute("INSERT INTO TB_APPLICANT (NOTION_PAGE_ID, NAME_HASH, NAME_MASKED, COHORT, STATUS, HRD_APPLY_AT, CANCEL_REASON) VALUES (?, ?, '홍*동', 'AIO3', ?, ?, ?)",
                    [pid, h, status, apply_at, "C2 취업 확정" if status.startswith("합격취소") else None])
    cur.execute("INSERT INTO TB_APPLICANT_STATUS_LOG (NOTION_PAGE_ID, DETECTED_AT, FIELD, OLD_VALUE, NEW_VALUE) VALUES ('p9', '2026-09-16 09:00:00', 'STATUS', 'HRD등록', '합격취소(연락두절)')")
    conn.commit()


class TestProperties:
    def test_to_properties_types(self):
        props = to_properties(PERSON_SCHEMA, {"이름": "홍*동 · AIO3", "KEY": "p1", "신청자": "p1", "기수": "AIO3", "노션 상태": "HRD등록",
                                              "HRD 신청 일시": "2026-09-01", "HRD 승인": True, "승인 감지": "2026-09-15T08:00:00",
                                              "첫 참석일": "20260915", "첫 입실": "09:02", "등록 지연(일)": 0, "정합성": "일치",
                                              "변경 시각": "2026-09-15T09:00:00Z", "메모": "지워지면 안 됨"})
        assert props["이름"]["title"][0]["text"]["content"] == "홍*동 · AIO3"
        assert props["신청자"] == {"relation": [{"id": "p1"}]}
        assert props["첫 참석일"] == {"date": {"start": "2026-09-15"}}        # YYYYMMDD → ISO
        assert props["승인 감지"] == {"date": {"start": "2026-09-15T08:00:00"}}
        assert props["HRD 승인"] == {"checkbox": True} and props["등록 지연(일)"] == {"number": 0.0}
        assert props["정합성"] == {"select": {"name": "일치"}}
        assert "메모" not in props                                             # 담당자 열은 절대 안 씀

    def test_missing_and_null_values(self):
        props = to_properties(COHORT_SCHEMA, {"기수": "AIO3", "KEY": "AIO3", "정합성": None, "개강일": None, "놓침": None})
        assert props["정합성"] == {"select": None} and props["개강일"] == {"date": None} and props["놓침"] == {"number": None}
        assert "회차" not in props

    def test_hash_ignores_change_time(self):
        a = {"KEY": "x", "값": 1, "변경 시각": "t1"}
        b = {"KEY": "x", "값": 1, "변경 시각": "t2"}
        assert content_hash(a) == content_hash(b) and content_hash(a) != content_hash({**a, "값": 2})


class TestBuildRows:
    def test_cohort_rows(self, db, monkeypatch):
        monkeypatch.setattr(pub, "load_data", lambda q, params=None, db=None: utils.load_data(q, params=params))
        _seed(db)
        rows = build_cohort_rows(today="2026-09-15")
        assert [r["기수"] for r in rows] == ["AIO3"]                    # SKN 회차는 제외
        r = rows[0]
        assert r["승인(API)"] == 2 and r["HRD등록(노션)"] == 5 and r["정합성"] == "불일치"
        assert r["HRD신청(노션)"] == 1 and r["놓침"] == 5 - 5 - 1
        assert r["합격 이상(노션)"] == 7 and r["노션 신청자"] == 10
        assert r["명부 인원"] == 7 and r["개강일 참석"] == 2 and r["개강일 참석률(%)"] == 100.0
        assert r["상태"] == "진행중"
        assert r["합격 후 취소(노션)"] == 2 and r["취소율(%)"] == round(2 / 9 * 100, 1)
        assert r["등록 후 이탈(API)"] == 1                                   # t5: 출석 없이 사라짐
        assert r["미참석(등록)"] is None                                     # today = 개강일 → 아직 세지 않는다
        r2 = build_cohort_rows(today="2026-09-16")[0]
        assert r2["미참석(등록)"] == 3                                       # t2·t4·t7: 훈련중인데 출석 없음

    def test_person_rows_consistency(self, db, monkeypatch):
        monkeypatch.setattr(pub, "load_data", lambda q, params=None, db=None: utils.load_data(q, params=params))
        _seed(db)
        rows = {r["KEY"]: r for r in build_person_rows(today="2026-09-16")}
        assert set(rows) == {"p1", "p2", "p3", "p4", "p5", "p7", "p8", "p9", "p10"}
        assert rows["p7"]["정합성"] == "취소" and rows["p7"]["취소 사유"] == "C2 취업 확정" and rows["p7"]["취소 전 상태"] is None
        assert rows["p8"]["정합성"] == "등록 후 이탈" and rows["p8"]["HRD 승인"] is False and rows["p8"]["개강 참석"] == "취소"
        assert rows["p9"]["정합성"] == "취소인데 명부 잔류" and rows["p9"]["취소 전 상태"] == "HRD등록" and rows["p9"]["개강 참석"] == "늦게 합류"
        assert rows["p10"]["정합성"] == "일치" and rows["p10"]["개강 참석"] == "미참석"
        assert rows["p1"]["개강 참석"] == "개강일 참석" and rows["p3"]["개강 참석"] == "미참석" and rows["p2"]["개강 참석"] is None
        assert rows["p1"]["취소 사유"] is None                                          # 취소 아닌 사람은 비움
        assert rows["p1"]["정합성"] == "일치" and rows["p1"]["HRD 승인"] is True and rows["p1"]["첫 참석일"] == "20260915"
        assert rows["p2"]["정합성"] == "노션만 등록" and rows["p2"]["HRD 승인"] is False
        assert rows["p3"]["정합성"] == "HRD만 승인" and rows["p3"]["등록 지연(일)"] == 1     # 9/16 승인, 개강 9/15
        assert rows["p1"]["등록 지연(일)"] is None                                          # 첫 스냅샷 날 승인 → 시점 불명
        assert rows["p4"]["정합성"] == "대기"
        assert rows["p5"]["정합성"] == "동명이인 확인"
        assert rows["p1"]["신청자"] == "p1" and rows["p1"]["이름"] == "홍*동 · AIO3"


class TestPublish:
    def _fake_session(self):
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

    def test_create_then_skip_then_update(self, db, monkeypatch):
        monkeypatch.setattr(config, "NOTION_WRITE_INTERVAL", 0)
        session = self._fake_session()
        rows = [{"KEY": "AIO3", "기수": "AIO3", "승인(API)": 2, "변경 시각": "t1"}]
        assert publish("tok", db, "cohort", "db1", COHORT_SCHEMA, rows, session=session, now=datetime(2026, 9, 15, 9)) == (1, 0, 0)
        assert session.request.call_args.args[:2] == ("POST", f"{config.NOTION_API_BASE}/pages")
        rows[0]["변경 시각"] = "t2"
        assert publish("tok", db, "cohort", "db1", COHORT_SCHEMA, rows, session=session) == (0, 0, 1)   # 내용 같음 → 건너뜀
        rows[0]["승인(API)"] = 3
        assert publish("tok", db, "cohort", "db1", COHORT_SCHEMA, rows, session=session) == (0, 1, 0)
        assert session.request.call_args.args[:2] == ("PATCH", f"{config.NOTION_API_BASE}/pages/page-1")

    def test_ensure_databases_creates_once(self, db, monkeypatch):
        monkeypatch.setattr(config, "NOTION_WRITE_INTERVAL", 0)
        session = self._fake_session()
        ids = ensure_databases("tok", db, session=session)
        assert ids == ("page-1", "page-3")          # 생성(1) → 레이아웃 PATCH(2) → 생성(3)
        calls = [c.args[:2] for c in session.request.call_args_list]
        assert calls.count(("POST", f"{config.NOTION_API_BASE}/databases")) == 2
        body = session.request.call_args_list[0].kwargs["json"]
        assert body["parent"] == {"type": "page_id", "page_id": config.NOTION_KPI_PARENT_PAGE_ID}
        # 만든 직후 인라인 표 + 설명 한 줄로 맞춘다
        layout = [c.kwargs["json"] for c in session.request.call_args_list if c.args[:2] == ("PATCH", f"{config.NOTION_API_BASE}/databases/page-1")]
        assert layout and layout[0]["is_inline"] is True and layout[0]["description"][0]["text"]["content"]
        # 두 번째 호출: 존재 확인(GET)만 하고 다시 만들지 않는다
        n_before = len(session.request.call_args_list)
        assert ensure_databases("tok", db, session=session) == ids
        later = [c.args[:2] for c in session.request.call_args_list[n_before:]]
        assert ("POST", f"{config.NOTION_API_BASE}/databases") not in later
        assert ("GET", f"{config.NOTION_API_BASE}/databases/page-1") in later and ("GET", f"{config.NOTION_API_BASE}/databases/page-3") in later

    def test_ensure_properties_adds_only_missing(self):
        session = self._fake_session()
        meta = {"properties": {k: {} for k in PERSON_SCHEMA if k not in ("개강 참석", "취소 사유")}}
        assert ensure_properties("tok", "db1", PERSON_SCHEMA, meta, session=session) == ["개강 참석", "취소 사유"]
        body = session.request.call_args.kwargs["json"]
        assert set(body["properties"]) == {"개강 참석", "취소 사유"}              # 있는 속성은 다시 보내지 않는다
        assert ensure_properties("tok", "db1", PERSON_SCHEMA, {"properties": dict.fromkeys(PERSON_SCHEMA, {})}, session=session) == []

    def test_layout_skips_when_already_inline_with_same_description(self):
        session = self._fake_session()
        meta = {"is_inline": True, "description": [{"plain_text": "설명"}]}
        assert ensure_database_layout("tok", "db1", "설명", meta, session=session) is False
        session.request.assert_not_called()
        assert ensure_database_layout("tok", "db1", "다른 설명", meta, session=session) is True
        assert session.request.call_args.args[:2] == ("PATCH", f"{config.NOTION_API_BASE}/databases/db1")

    def test_person_relation_is_one_way(self):
        rel = PERSON_SCHEMA["신청자"]["relation"]
        assert rel["database_id"] == config.NOTION_APPLICANTS_DB_ID and "single_property" in rel   # 담당자 DB에 열을 추가하지 않는다

    @patch("notion_kpi_publish.time.sleep")
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


class TestPageGuide:
    def _page_session(self, children):
        """페이지 자식 GET → children, 블록 추가 PATCH → 넣은 개수만큼 새 ID, 보관 PATCH → OK."""
        session = MagicMock()
        counter, types = {"n": 0}, {}

        def request(method, url, headers=None, json=None, timeout=None):
            resp = MagicMock()
            resp.status_code = 200
            resp.headers = {}
            if method == "GET" and "/children" in url:
                resp.json.return_value = {"results": children, "has_more": False}
            elif method == "PATCH" and url.endswith("/children"):
                ids = []
                for b in json["children"]:
                    counter["n"] += 1
                    ids.append({"id": f"blk-{counter['n']}", "type": b["type"]})
                    types[f"blk-{counter['n']}"] = b["type"]
                # 노션은 `after` 뒤에 오던 기존 형제 블록(DB 포함)까지 같이 돌려준다 — 실측 재현
                resp.json.return_value = {"results": ids + [{"id": "pdb", "type": "child_database"}, {"id": "tail", "type": "paragraph"}]}
            elif method == "GET" and "/blocks/" in url:
                bid = url.rsplit("/", 1)[1]
                resp.json.return_value = {"id": bid, "type": types.get(bid, "child_database"), "archived": False}
            else:
                resp.json.return_value = {"id": "ok"}
            return resp
        session.request.side_effect = request
        return session

    def test_guide_blocks_shape(self):
        blocks = guide_blocks(COHORT_GUIDE)
        assert [b["type"] for b in blocks] == ["heading_2", "callout", "table"]
        assert len(blocks[1]["callout"]["children"]) == len(COHORT_GUIDE["first"])
        rows = blocks[2]["table"]["children"]
        assert len(rows) == len(COHORT_GUIDE["columns"]) + 1                    # 헤더 행 포함
        assert rows[0]["table_row"]["cells"][0][0]["text"]["content"] == "열"
        assert all(len(r["table_row"]["cells"]) == 3 for r in rows)
        # 표에 적힌 열 이름은 실제 스키마 속성과 이어져야 한다 (오타·이름 변경 방지)
        for schema, guide in ((COHORT_SCHEMA, COHORT_GUIDE), (PERSON_SCHEMA, PERSON_GUIDE)):
            described = " ".join(c[0] for c in guide["columns"])
            missing = [p for p in schema if p != "KEY" and p not in described]
            assert not missing, missing

    def test_writes_before_cohort_db_and_between_dbs_then_skips(self, db, monkeypatch):
        monkeypatch.setattr(config, "NOTION_WRITE_INTERVAL", 0)
        children = [{"id": "intro", "type": "paragraph"}, {"id": "cdb", "type": "child_database"}, {"id": "pdb", "type": "child_database"}]
        session = self._page_session(children)
        assert ensure_page_guide("tok", db, "cdb", "pdb", session=session) is True
        appends = [c.kwargs["json"] for c in session.request.call_args_list if c.args[0] == "PATCH" and c.args[1].endswith("/children")]
        assert [a.get("after") for a in appends] == ["cdb", "intro"]           # 사람별 안내 = 기수별 DB 뒤, 기수별 안내 = DB 앞
        assert appends[0]["children"][0]["heading_2"]["rich_text"][0]["text"]["content"].startswith("2️⃣")
        cur = db.cursor()
        cur.execute("SELECT NOTION_PAGE_ID FROM TB_NOTION_PUBLISH WHERE DB_KEY = 'guide'")
        stored = {r[0] for r in cur.fetchall()}
        assert len(stored) == 6 and "pdb" not in stored and "tail" not in stored    # 뒤따라온 형제 블록은 기록하지 않는다
        # 같은 내용이면 요청 없음
        n = len(session.request.call_args_list)
        assert ensure_page_guide("tok", db, "cdb", "pdb", session=session) is False
        assert len(session.request.call_args_list) == n

    def test_rewrites_and_archives_old_blocks_when_content_changes(self, db, monkeypatch):
        monkeypatch.setattr(config, "NOTION_WRITE_INTERVAL", 0)
        children = [{"id": "cdb", "type": "child_database"}, {"id": "pdb", "type": "child_database"}]
        session = self._page_session(children)
        ensure_page_guide("tok", db, "cdb", "pdb", session=session)
        monkeypatch.setattr(pub, "guide_hash", lambda: "changed")
        n = len(session.request.call_args_list)
        assert ensure_page_guide("tok", db, "cdb", "pdb", session=session) is True
        archived = [c for c in session.request.call_args_list[n:] if c.args[0] == "PATCH" and c.kwargs["json"] == {"archived": True}]
        assert len(archived) == 6 and all("/blocks/blk-" in c.args[1] for c in archived)
        appends = [c.kwargs["json"] for c in session.request.call_args_list[n:] if c.args[1].endswith("/children") and c.args[0] == "PATCH"]
        assert [a.get("after") for a in appends] == ["cdb", None]              # DB가 맨 앞이면 기수별 안내는 끝에 붙는다

    def test_never_archives_database_blocks(self, db, monkeypatch):
        """기록이 잘못돼 DB 블록 ID가 들어 있어도 보관 처리하지 않는다 (DB가 통째로 사라지는 사고 방지)."""
        monkeypatch.setattr(config, "NOTION_WRITE_INTERVAL", 0)
        session = self._page_session([{"id": "cdb", "type": "child_database"}, {"id": "pdb", "type": "child_database"}])
        cur = db.cursor()
        cur.execute("INSERT INTO TB_NOTION_PUBLISH (DB_KEY, ROW_KEY, NOTION_PAGE_ID, CONTENT_HASH) VALUES ('guide', 'x', 'cdb', 'stale')")
        db.commit()
        ensure_page_guide("tok", db, "cdb", "pdb", session=session)
        archived = [c for c in session.request.call_args_list if c.args[0] == "PATCH" and c.kwargs.get("json") == {"archived": True}]
        assert archived == []


class TestAttendVerdict:
    @pytest.mark.parametrize("first, start, today, gone, expected", [
        ("20260915", "2026-09-15", "2026-09-16", False, "개강일 참석"),
        ("20260917", "2026-09-15", "2026-09-18", False, "늦게 합류"),
        (None, "2026-09-15", "2026-09-15", False, "개강 전"),        # 개강 당일 아침엔 아직 미참석이라 하지 않는다
        (None, "2026-09-15", "2026-09-16", False, "미참석"),
        (None, "2026-09-15", "2026-09-16", True, "취소"),
        ("20260915", "2026-09-15", "2026-09-20", True, "개강일 참석"),   # 나갔어도 참석 사실은 남긴다
    ])
    def test_verdicts(self, first, start, today, gone, expected):
        assert attend_verdict(first, start, today, gone=gone) == expected
