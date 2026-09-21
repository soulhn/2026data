"""notion_registry_publish.py — 「HRD 등록자 관리」 행 생성: 기수 표(API 신청 vs 노션 수집)·등록자(노션 ∪ 명부)."""
from datetime import datetime

import pytest

import init_db
import notion_applicants_etl
import notion_kpi_publish
import notion_registry_publish as reg
import utils
from notion_registry_publish import attend_verdict, body_blocks, body_rows, build_cohort_rows, build_person_rows, publish_cohort_bodies


@pytest.fixture
def db(monkeypatch, mock_db_connection):
    for mod in (reg, notion_kpi_publish, notion_applicants_etl):
        monkeypatch.setattr(mod, "adapt_query", utils.adapt_query)
    monkeypatch.setattr(reg, "load_data", lambda q, params=None, db=None: utils.load_data(q, params=params))
    reg._START_CACHE.clear()
    init_db.init_all_tables(include_market=False)
    return mock_db_connection


def _seed(conn):
    """AIO3(개강 9/15): API 수강신청 5 · 명부 3(t1 노션 일치, t2 노션 없음, t3 사라짐) · 노션 등록 이력 4(p1 일치, p2 노션만, p3 취소됐지만 로그, p4 신청일자만).
    SKN37: API 수강신청 2 · 노션 HRD등록 2 → 일치."""
    cur = conn.cursor()
    cur.execute("INSERT INTO TB_COURSE_SNAPSHOT (TRPR_ID, TRPR_DEGR, SNAP_AT, TR_STA_DT, TR_END_DT, TOT_FXNUM, TOT_TRP_CNT, TOT_PAR_MKS, FINI_CNT, CHANGED) "
                "VALUES ('AIG20260000578396', 3, '2026-09-15 08:00:00', '2026-09-15', '2027-03-12', 30, 5, 2, 0, 'first')")
    cur.execute("INSERT INTO TB_COURSE_SNAPSHOT (TRPR_ID, TRPR_DEGR, SNAP_AT, TR_STA_DT, TR_END_DT, TOT_FXNUM, TOT_TRP_CNT, TOT_PAR_MKS, FINI_CNT, CHANGED) "
                "VALUES ('AIG20240000459068', 37, '2026-09-15 08:00:00', '2026-09-04', '2027-03-03', 30, 2, 2, 0, 'first')")
    for tid, h, fa, gone, seen in [("t1", "h_kim", "20260915", None, "2026-09-15 08:00:00"),
                                   ("t2", "h_only", None, None, "2026-09-17 08:00:00"),
                                   ("t3", "h_gone", None, "2026-09-18 08:00:00", "2026-09-15 08:00:00")]:
        cur.execute("INSERT INTO TB_ROSTER_MEMBER (TRPR_ID, TRPR_DEGR, TRNEE_ID, NAME_HASH, NAME_MASKED, TR_STA_DT, STATUS, FIRST_SEEN_AT, LAST_SEEN_AT, FIRST_ATTEND_DT, GONE_AT) "
                    "VALUES ('AIG20260000578396', 3, ?, ?, '홍*동', '2026-09-15', '훈련중', ?, ?, ?, ?)", [tid, h, seen, seen, fa, gone])
    for pid, src, h, cohort, status, apply_at, reg_at in [
            ("p1", "AI", "h_kim", "AIO3", "HRD등록", "2026-09-01", None),
            ("p2", "AI", "h_park", "AIO3", "HRD등록", None, None),
            ("p3", "AI", "h_can", "AIO3", "합격취소(신청자 요청)", None, None),   # 로그로 등록 이력
            ("p4", "AI", "h_dt", "AIO3", "합격취소(연락두절)", "2026-09-02", None),  # 신청 일자로 등록 이력
            ("p5", "AI", "h_no", "AIO3", "합격안내", None, None),                 # 등록 이력 없음 → 제외
            ("s1", "SKN", "h_s1", "SKN37", "HRD등록", None, "2026-08-30"),
            ("s2", "SKN", "h_s2", "SKN37", "HRD등록", None, None)]:
        cur.execute("INSERT INTO TB_APPLICANT (NOTION_PAGE_ID, NOTION_URL, SOURCE_KEY, NAME_HASH, NAME_MASKED, COHORT, STATUS, HRD_APPLY_AT, HRD_REG_AT) "
                    "VALUES (?, ?, ?, ?, '홍*동', ?, ?, ?, ?)", [pid, f"https://notion.so/{pid}", src, h, cohort, status, apply_at, reg_at])
    cur.execute("INSERT INTO TB_APPLICANT_STATUS_LOG (NOTION_PAGE_ID, DETECTED_AT, FIELD, OLD_VALUE, NEW_VALUE) VALUES ('p3', '2026-09-16 09:00:00', 'STATUS', 'HRD등록', '합격취소(신청자 요청)')")
    conn.commit()


class TestCohorts:
    def test_api_vs_notion_and_attendance(self, db):
        _seed(db)
        rows = {r["기수"]: r for r in build_cohort_rows(today="2026-09-16")}
        assert set(rows) == {"AIO3", "SKN37"}
        a = rows["AIO3"]
        assert a["API 신청인원"] == 5 and a["노션 수집 등록 인원"] == 4 and a["일치 여부"] == "불일치"   # p1·p2·p3·p4
        assert a["개강일 출석 인원"] == 1 and a["개강 참석률(%)"] == 20.0                          # t1 ÷ API 5
        assert a["상태"] == "진행중" and a["과정"] == "AIO"
        assert a["확정 신고(API)"] is None and a["확정자 신고율(%)"] is None                      # 개강 + 7일 전
        later = {r["기수"]: r for r in build_cohort_rows(today="2026-09-22")}["AIO3"]
        assert later["확정 신고(API)"] == 2 and later["확정자 신고율(%)"] == 40.0                  # totParMks 2 ÷ 수강신청 5
        s = rows["SKN37"]
        assert s["API 신청인원"] == 2 and s["노션 수집 등록 인원"] == 2 and s["일치 여부"] == "일치"
        assert s["개강일 출석 인원"] is None and s["개강 참석률(%)"] is None                     # 명부를 읽은 적 없으면 모름

    def test_unknown_attendance_when_never_read(self, db):
        """명부는 있는데 출결을 한 번도 못 읽은 회차(추적 전 종료)는 0이 아니라 비운다."""
        _seed(db)
        cur = db.cursor()
        cur.execute("UPDATE TB_ROSTER_MEMBER SET FIRST_ATTEND_DT = NULL, DAY1_STATUS = NULL")
        db.commit()
        a = {r["기수"]: r for r in build_cohort_rows(today="2026-09-16")}["AIO3"]
        assert a["개강일 출석 인원"] is None and a["개강 참석률(%)"] is None
        rows = {r["KEY"]: r for r in build_person_rows({"AIO3": "c"}, today="2026-09-16")}
        assert rows["p1"]["개강날 출석"] == "기록 없음" and rows["roster:AIG20260000578396:3:t2"]["개강날 출석"] == "기록 없음"
        assert rows["p2"]["개강날 출석"] == "미출석"      # 명부에 없는 사람은 회차 기록과 무관하게 미출석

    def test_before_start_no_attendance(self, db):
        _seed(db)
        a = {r["기수"]: r for r in build_cohort_rows(today="2026-09-15")}["AIO3"]
        assert a["개강일 출석 인원"] is None and a["개강 참석률(%)"] is None and a["상태"] == "진행중"


class TestPersons:
    def test_union_of_notion_and_roster(self, db):
        _seed(db)
        pages = {"AIO3": "cohort-aio3", "SKN37": "cohort-skn37"}
        rows = {r["KEY"]: r for r in build_person_rows(pages, today="2026-09-16")}
        assert set(rows) == {"p1", "p2", "p3", "p4", "s1", "s2",
                             "roster:AIG20260000578396:3:t2", "roster:AIG20260000578396:3:t3"}   # p5 제외, t2·t3는 노션에 없음
        assert rows["p1"]["기수"] == "cohort-aio3" and rows["p1"]["개강날 출석"] == "출석" and rows["p1"]["HRD 승인"] is True
        assert rows["p1"]["등록일"] == "2026-09-01" and rows["p1"]["원본 링크"] == "https://notion.so/p1"
        assert rows["p2"]["개강날 출석"] == "미출석" and rows["p2"]["HRD 승인"] is False and rows["p2"]["등록일"] is None
        assert rows["p3"]["노션 상태"].startswith("합격취소") and rows["p4"]["등록일"] == "2026-09-02"
        r = rows["roster:AIG20260000578396:3:t2"]
        assert r["노션 상태"] == "노션에 없음" and r["등록일"] == "2026-09-17" and r["기수"] == "cohort-aio3"   # 추적 시작(9/15) 이후 등장
        assert rows["s1"]["등록일"] == "2026-08-30" and rows["s1"]["원본 링크"] == "https://notion.so/s1" and rows["s1"]["기수"] == "cohort-skn37"
        assert rows["s2"]["개강날 출석"] == "미출석"                                                    # 명부 없음, 개강 지남

    def test_gone_member_keeps_row_via_notion(self, db):
        """명부에서 사라진 t3는 노션에 없어 명부 전용 행으로 남고 HRD 승인은 해제."""
        _seed(db)
        rows = {r["KEY"]: r for r in build_person_rows({"AIO3": "c"}, today="2026-09-20")}
        assert rows["roster:AIG20260000578396:3:t3"]["HRD 승인"] is False

    def test_skips_cohorts_without_page(self, db):
        _seed(db)
        assert all(r["기수"] == "c" for r in build_person_rows({"AIO3": "c"}, today="2026-09-16"))
        assert not [r for r in build_person_rows({}, today="2026-09-16")]


class TestVerdict:
    @pytest.mark.parametrize("first, start, today, expected", [
        ("20260915", "2026-09-15", "2026-09-16", "출석"),
        (None, "2026-09-15", "2026-09-16", "미출석"),
        ("20260917", "2026-09-15", "2026-09-18", "미출석"),      # 늦게 합류는 개강날 기준으론 미출석
        (None, "2026-09-15", "2026-09-15", "개강 전"),
        (None, None, "2026-09-16", "개강 전"),
    ])
    def test_attend(self, first, start, today, expected):
        assert attend_verdict(first, start, today) == expected

    def test_unknown_round(self):
        assert attend_verdict(None, "2026-03-12", "2026-09-21", known=False) == "기록 없음"
        assert attend_verdict("20260312", "2026-03-12", "2026-09-21", known=False) == "출석"


def test_hash_ignores_updated_at():
    from notion_kpi_publish import content_hash
    assert content_hash({"KEY": "x", "갱신 시각": "a"}) == content_hash({"KEY": "x", "갱신 시각": "b"})
    assert datetime  # 사용 표시


def test_url_property_conversion():
    from notion_kpi_publish import to_properties
    props = to_properties({"원본 링크": {"url": {}}}, {"원본 링크": "https://notion.so/x"})
    assert props["원본 링크"] == {"url": "https://notion.so/x"}
    assert to_properties({"원본 링크": {"url": {}}}, {"원본 링크": None})["원본 링크"] == {"url": None}


class TestCohortBody:
    def _persons(self):
        return [{"기수": "c1", "이름": "홍*동 · AIO3", "등록일": "2026-09-02", "개강날 출석": "미출석", "노션 상태": "HRD등록", "HRD 승인": True},
                {"기수": "c1", "이름": "김*수 · AIO3", "등록일": "2026-09-01", "개강날 출석": "출석", "노션 상태": "HRD등록", "HRD 승인": True},
                {"기수": "c1", "이름": "박*희 · AIO3", "등록일": None, "개강날 출석": "출석", "노션 상태": "노션에 없음", "HRD 승인": False},
                {"기수": "c2", "이름": "이*정 · SKN37", "등록일": "2026-08-30", "개강날 출석": "출석", "노션 상태": "HRD등록", "HRD 승인": True}]

    def test_rows_filtered_sorted_and_masked(self):
        rows = body_rows(self._persons(), "c1")
        assert [r[0] for r in rows] == ["김*수", "박*희", "홍*동"]           # 출석 먼저, 그 안에서 등록일 순, 없는 날짜는 뒤로
        assert rows[0] == ["김*수", "2026-09-01", "출석", "HRD등록", "✓"] and rows[1][4] == ""

    def test_blocks_split_over_90_rows(self):
        rows = [[f"n{i}", "", "출석", "", ""] for i in range(120)]
        blocks, rest = body_blocks(rows, "db")
        assert [b["type"] for b in blocks] == ["table", "paragraph"]
        assert len(blocks[0]["table"]["children"]) == 90 and len(rest) == 31        # 헤더 1 + 89행, 나머지 31행
        assert blocks[0]["table"]["children"][0]["table_row"]["cells"][0][0]["text"]["content"] == "이름"
        assert blocks[1]["paragraph"]["rich_text"][1]["mention"]["database"]["id"] == "db"

    def test_publish_writes_once_then_skips_then_rewrites(self, db, monkeypatch):
        import config
        monkeypatch.setattr(config, "NOTION_WRITE_INTERVAL", 0)
        from unittest.mock import MagicMock
        session = MagicMock()
        calls = []

        def request(method, url, headers=None, json=None, timeout=None):
            calls.append((method, url.rsplit("/v1", 1)[-1], json))
            resp = MagicMock()
            resp.status_code = 200
            resp.headers = {}
            if method == "PATCH" and url.endswith("/children"):
                resp.json.return_value = {"results": [{"id": f"blk-{len(calls)}-{i}", "type": b["type"]} for i, b in enumerate(json["children"])]
                                          + [{"id": "db-block", "type": "child_database"}]}
            elif method == "GET":
                resp.json.return_value = {"id": url.rsplit("/", 1)[-1], "type": "table", "archived": False}
            else:
                resp.json.return_value = {"id": "ok"}
            return resp
        session.request.side_effect = request
        pages = {"AIO3": "c1", "SKN37": "c2"}
        assert publish_cohort_bodies("tok", db, pages, self._persons(), "pdb", session=session) == (2, 0)
        cur = db.cursor()
        cur.execute("SELECT ROW_KEY, NOTION_PAGE_ID FROM TB_NOTION_PUBLISH WHERE DB_KEY = 'reg_body' ORDER BY ROW_KEY")
        stored = dict(cur.fetchall())
        assert set(stored) == {"AIO3", "SKN37"} and "db-block" not in stored["AIO3"]      # 뒤따라온 DB 블록은 기록하지 않는다
        n = len(calls)
        assert publish_cohort_bodies("tok", db, pages, self._persons(), "pdb", session=session) == (0, 2) and len(calls) == n
        persons = self._persons()
        persons[0]["개강날 출석"] = "출석"
        assert publish_cohort_bodies("tok", db, pages, persons, "pdb", session=session) == (1, 1)
        archived = [c for c in calls[n:] if c[0] == "PATCH" and c[2] == {"archived": True}]
        assert len(archived) == 2 and all("/blocks/blk-" in c[1] for c in archived)     # AIO3의 옛 표·문단만 보관
