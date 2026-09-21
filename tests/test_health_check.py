"""health_check.py — 신선도·값·매칭·담당자 대조 점검과 알림 중복 방지 (인메모리 SQLite, 노션 모킹)."""
from datetime import datetime, timedelta, timezone
import pandas as pd
import pytest

import health_check as hc
import init_db
import notion_applicants_etl
import notion_publish
import notion_registry_publish as reg
import utils
from health_check import check_freshness, check_matching, check_ops_table, check_values, findings_hash, run_checks, should_send

NOW = datetime(2026, 9, 21, 6, 0, tzinfo=timezone.utc)   # KST 15:00


@pytest.fixture
def db(monkeypatch, mock_db_connection):
    for mod in (reg, notion_publish, notion_applicants_etl):
        monkeypatch.setattr(mod, "adapt_query", utils.adapt_query)
    for mod in (hc, reg):
        monkeypatch.setattr(mod, "load_data", lambda q, params=None, db=None: utils.load_data(q, params=params))
    reg._START_CACHE.clear()
    init_db.init_all_tables(include_market=False)
    return mock_db_connection


def _seed(conn, fresh=True):
    """AIO3(개강 9/15) 정상 기수 하나 + 상태 키. fresh=False면 모두 3일 전."""
    t = "2026-09-21 04:00:00" if fresh else "2026-09-18 04:00:00"
    cur = conn.cursor()
    cur.execute("INSERT INTO TB_COURSE_SNAPSHOT (TRPR_ID, TRPR_DEGR, SNAP_AT, TR_STA_DT, TR_END_DT, TOT_FXNUM, TOT_TRP_CNT, TOT_PAR_MKS, FINI_CNT, CHANGED) "
                "VALUES ('AIG20260000578396', 3, ?, '2026-09-15', '2027-03-12', 30, 5, 3, 0, 'first')", [t])
    for tid, h, fa, seen in [("t1", "h_kim", "20260915", "2026-09-15 08:00:00"), ("t2", "h_lee", None, "2026-09-15 08:00:00")]:
        cur.execute("INSERT INTO TB_ROSTER_MEMBER (TRPR_ID, TRPR_DEGR, TRNEE_ID, NAME_HASH, NAME_MASKED, TR_STA_DT, STATUS, FIRST_SEEN_AT, LAST_SEEN_AT, FIRST_ATTEND_DT, DAY1_STATUS) "
                    "VALUES ('AIG20260000578396', 3, ?, ?, '홍*동', '2026-09-15', '훈련중', ?, ?, ?, ?)", [tid, h, seen, t, fa, "출석" if fa else "결석"])
    for pid, h, status in [("p1", "h_kim", "HRD등록"), ("p2", "h_lee", "HRD등록"), ("p3", "h_park", "HRD등록")]:
        cur.execute("INSERT INTO TB_APPLICANT (NOTION_PAGE_ID, SOURCE_KEY, NAME_HASH, NAME_MASKED, COHORT, STATUS) VALUES (?, 'AI', ?, '홍*동', 'AIO3', ?)", [pid, h, status])
    conn.commit()
    iso = t.replace(" ", "T") + "Z"
    for key in (notion_applicants_etl.sync_key("AI"), notion_applicants_etl.sync_key("SKN"), "notion_registry_last_publish"):
        notion_applicants_etl.set_sync_state(conn, iso, key)
    notion_applicants_etl.set_sync_state(conn, "", "kpi_last_errors")
    notion_applicants_etl.set_sync_state(conn, "", "notion_applicants_last_error")


class TestFreshness:
    def test_quiet_when_everything_recent(self, db):
        _seed(db)
        assert check_freshness(db, NOW) == []

    def test_reports_stale_and_last_errors(self, db):
        _seed(db, fresh=False)
        notion_applicants_etl.set_sync_state(db, "출결 조회 실패: ENCORE 타임아웃", "kpi_last_errors")
        out = check_freshness(db, NOW)
        assert any("회차 스냅샷" in f and "시간째" in f for f in out)
        assert any("신청자 폴링 SKN" in f for f in out) and any("등록자 페이지 발행" in f for f in out)
        assert any("HRD API 직전 실행 오류" in f and "ENCORE" in f for f in out)

    def test_reports_missing_state_keys(self, db):
        out = check_freshness(db, NOW)
        assert any("신청자 폴링 AI: 기록 없음" in f for f in out)


class TestValues:
    def test_impossible_and_missing_attendance(self, db):
        _seed(db)
        rows = reg.build_cohort_rows(today="2026-09-21")
        assert check_values(rows, "2026-09-21") == []                                   # 노션 3 ≤ API 5, 출결 있음
        rows[0]["노션 수집 등록 인원"] = 9
        rows[0]["개강일 출석 인원"] = None
        out = check_values(rows, "2026-09-21")
        assert any("노션 수집 등록 9 > API 신청인원 5" in f for f in out) and any("출결 기록 없음" in f for f in out)

    def test_no_attendance_warning_before_day_after_start(self, db):
        _seed(db)
        rows = reg.build_cohort_rows(today="2026-09-15")
        assert check_values(rows, "2026-09-15") == []


class TestMatching:
    def test_unknown_cohort_duplicates_and_new_roster_only(self, db):
        _seed(db)
        cur = db.cursor()
        cur.execute("INSERT INTO TB_APPLICANT (NOTION_PAGE_ID, SOURCE_KEY, NAME_HASH, NAME_MASKED, COHORT, STATUS) VALUES ('p9', 'SKN', 'h_x', '김*수', 'SKN99', 'HRD등록')")
        cur.execute("INSERT INTO TB_APPLICANT (NOTION_PAGE_ID, SOURCE_KEY, NAME_HASH, NAME_MASKED, COHORT, STATUS) VALUES ('p4', 'AI', 'h_kim', '홍*동', 'AIO3', 'HRD등록')")
        cur.execute("INSERT INTO TB_ROSTER_MEMBER (TRPR_ID, TRPR_DEGR, TRNEE_ID, NAME_HASH, NAME_MASKED, TR_STA_DT, STATUS, FIRST_SEEN_AT, LAST_SEEN_AT) "
                    "VALUES ('AIG20260000578396', 3, 't9', 'h_new', '박*희', '2026-09-15', '훈련중', '2026-09-21 04:00:00', '2026-09-21 04:00:00')")
        db.commit()
        out = check_matching(NOW)
        assert any("SKN 신청자 리스트 최종기수 'SKN99' 1명" in f for f in out)
        assert any("같은 기수에 같은 이름 등록자: AIO3 2명" in f for f in out)
        assert any("노션 신청자 리스트에 없음 1명" in f and "박*희 · AIO3" in f for f in out)

    def test_old_roster_only_member_is_not_new(self, db):
        _seed(db)
        cur = db.cursor()
        cur.execute("INSERT INTO TB_ROSTER_MEMBER (TRPR_ID, TRPR_DEGR, TRNEE_ID, NAME_HASH, NAME_MASKED, TR_STA_DT, STATUS, FIRST_SEEN_AT, LAST_SEEN_AT) "
                    "VALUES ('AIG20260000578396', 3, 't8', 'h_old', '박*희', '2026-09-15', '훈련중', '2026-09-15 08:00:00', '2026-09-21 04:00:00')")
        db.commit()
        assert not [f for f in check_matching(NOW) if "노션 신청자 리스트에 없음" in f]


class TestOpsTable:
    def test_mismatch_only_for_confirmed_cohorts(self, monkeypatch):
        ops = pd.DataFrame([{"과정명": "멀티에이전트 AI 오케스트레이션 캠프 3기", "확정자신고": 15},
                            {"과정명": "SK네트웍스 Family AI 캠프 37기", "확정자신고": 22}])
        monkeypatch.setattr(hc, "fetch_ops_table", lambda token, session=None: ops)
        rows = [{"기수": "AIO3", "확정 신고(API)": 14}, {"기수": "SKN37", "확정 신고(API)": 22}, {"기수": "AIO4", "확정 신고(API)": None}]
        out = check_ops_table("tok", rows)
        assert out == ["AIO3: 운영현황표 확정자신고 15 vs API 확정 14 — 담당자 확인"]

    def test_no_token(self):
        assert check_ops_table(None, []) == ["운영현황표 대조 건너뜀: NOTION_TOKEN 없음"]


class TestSend:
    def test_run_checks_returns_only_sections_with_findings(self, db, monkeypatch):
        _seed(db)
        monkeypatch.setattr(hc, "fetch_ops_table", lambda token, session=None: pd.DataFrame(columns=["과정명", "확정자신고"]))
        assert run_checks(db, "tok", NOW) == []

    def test_dedupe_then_weekly_resend(self, db):
        h = findings_hash([("1", ["a"])])
        assert should_send(db, h, NOW) is True                                          # 처음
        notion_applicants_etl.set_sync_state(db, h, "health_last_hash")
        notion_applicants_etl.set_sync_state(db, NOW.strftime("%Y-%m-%dT%H:%M:%SZ"), "health_last_sent")
        assert should_send(db, h, NOW + timedelta(days=1)) is False                     # 같은 내용, 하루 뒤
        assert should_send(db, h, NOW + timedelta(days=8)) is True                      # 7일 지나면 다시
        assert should_send(db, findings_hash([("1", ["b"])]), NOW + timedelta(days=1)) is True   # 내용 바뀜

    def test_main_sends_and_records(self, db, monkeypatch):
        _seed(db, fresh=False)
        class _NoClose:                                   # main()이 닫아도 테스트 DB는 살아 있어야 한다
            def __getattr__(self, n):
                return getattr(db, n)

            def close(self):
                pass
        monkeypatch.setattr(hc, "get_connection", lambda timeout=30: _NoClose())
        monkeypatch.setattr(hc, "fetch_ops_table", lambda token, session=None: pd.DataFrame(columns=["과정명", "확정자신고"]))
        sent = []
        monkeypatch.setattr(hc, "discord_post", lambda sections: sent.append(sections) or 1)
        monkeypatch.setenv("NOTION_TOKEN", "tok")
        hc.main(now=NOW)
        assert sent and sent[0][0][0].startswith("⚠️ 파이프라인 점검 · 1 신선도")
        assert notion_applicants_etl.get_sync_state(db, "health_last_sent") == "2026-09-21T06:00:00Z"
        hc.main(now=NOW)
        assert len(sent) == 1                                                           # 같은 내용 → 두 번째는 조용
