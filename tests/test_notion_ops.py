"""notion_ops.py 단위 테스트 — 노션 응답 파싱, CSV 로딩, HRD 회차 대조"""
import io
from unittest.mock import MagicMock

import pandas as pd
import pytest

import config
from notion_ops import (
    NOTION_OPS_COLUMNS,
    NotionFetchError,
    compare_ops,
    course_group,
    fetch_ops_table,
    load_ops_csv,
    parse_notion_date,
    parse_ops_pages,
    roster_current_counts,
)


# ── fixtures ──────────────────────────────────────────────────────────


def _title(text):
    return {"type": "title", "title": [{"plain_text": text}]}


def _num(v):
    return {"type": "number", "number": v}


def _formula_num(v):
    return {"type": "formula", "formula": {"type": "number", "number": v}}


def _date(s):
    return {"type": "date", "date": {"start": s} if s else None}


def _page(name, sta, end, open_cnt, early, extra, mid, confirmed=None, current=None, url="https://n/x"):
    props = {
        "과정명": _title(name),
        "구분": {"type": "multi_select", "multi_select": [{"name": "AI캠퍼스"}]},
        "팀": {"type": "select", "select": {"name": "사업1팀"}},
        "강의실": {"type": "select", "select": {"name": "동캠 4"}},
        "개강일": _date(sta),
        "종강일": _date(end),
        "교육일수": {"type": "rich_text", "rich_text": [{"plain_text": "120일"}]},
        "개강인원": _num(open_cnt),
        "초기이탈": _num(early),
        "추가인원 (개강~확정자신고)": _num(extra),
        "중도이탈": _num(mid),
        "확정자신고": _formula_num(confirmed),
        "현재인원": _formula_num(current),
        "이탈합계": _formula_num(None),
        "지각/외출": _num(1),
        "결석": _num(0),
        # 사람 이름이 들어가는 속성은 가져오지 않아야 한다
        "PM": {"type": "people", "people": [{"name": "홍길동"}]},
        "지각 / 결석": {"type": "rich_text", "rich_text": [{"plain_text": "지각 : 김아무개"}]},
    }
    return {"url": url, "properties": props}


@pytest.fixture
def notion_pages():
    return [
        _page("멀티에이전트 AI 오케스트레이션 캠프 1기", "2026-07-09", "2027-01-04", 26, 2, 0, 1, confirmed=24, current=23),
        _page("데이터 분석 & AI 머신러닝 1기", "2026-07-16", "2027-01-11", 26, 6, 1, 4),  # 수식 비어 옴 → 보정
        _page("SK네트웍스 Family AI 캠프 35기", "2026-07-07", "2026-12-28", 30, 6, 0, 1, confirmed=24),
    ]


@pytest.fixture
def hrd_history():
    return pd.DataFrame([
        # 회차 0 = 과정 헤더 행 → 제외돼야 함
        {"TRPR_ID": "AIG396", "TRPR_DEGR": 0, "TRPR_NM": "[엔코아] 오케스트레이션 개발자", "TR_STA_DT": "",
         "TR_END_DT": "", "TOT_FXNUM": 30, "TOT_TRP_CNT": 0, "TOT_PAR_MKS": None, "FINI_CNT": 0},
        {"TRPR_ID": "AIG396", "TRPR_DEGR": 1, "TRPR_NM": "[엔코아] 오케스트레이션 개발자", "TR_STA_DT": "2026-07-09",
         "TR_END_DT": "2027-01-04", "TOT_FXNUM": 30, "TOT_TRP_CNT": 31, "TOT_PAR_MKS": 24, "FINI_CNT": 0},
        {"TRPR_ID": "AIG382", "TRPR_DEGR": 1, "TRPR_NM": "[엔코아] GraphRAG 머신러닝 엔지니어", "TR_STA_DT": "2026-07-16",
         "TR_END_DT": "2027-01-11", "TOT_FXNUM": 30, "TOT_TRP_CNT": 25, "TOT_PAR_MKS": 20, "FINI_CNT": 0},
        {"TRPR_ID": "AIG396", "TRPR_DEGR": 3, "TRPR_NM": "[엔코아] 오케스트레이션 개발자", "TR_STA_DT": "2026-09-15",
         "TR_END_DT": "2027-03-12", "TOT_FXNUM": 30, "TOT_TRP_CNT": 13, "TOT_PAR_MKS": 9, "FINI_CNT": 0},
    ])


# ── course_group ──────────────────────────────────────────────────────


class TestCourseGroup:
    @pytest.mark.parametrize("name,expected", [
        ("멀티에이전트 AI 오케스트레이션 캠프 2기", "AIO"),
        ("[엔코아] 멀티 에이전트 워크플로우 기반 AI 오케스트레이션 … 개발자 양성 과정", "AIO"),
        ("데이터 분석 & AI 머신러닝 1기", "MLE"),
        ("[엔코아] LLM 지식 그래프 기반 신뢰형 GraphRAG … 머신러닝 엔지니어 양성 과정", "MLE"),
        ("한화시스템 BEYOND SW 캠프 - “어쩌구”", "한화"),
        ("SK네트웍스 Family AI 캠프 35기", "SKN"),
        ("sk네트웍스 Family AI 캠프 37기", "SKN"),        # 노션 실제 표기 (소문자)
        ("AI Ready Data 데이터 엔지니어링 캠프 1기", "MLO"),
        ("[엔코아] AI Ready Data 기반 Cloud·Native 자동화를 위한 MLOps 엔지니어 양성 과정", "MLO"),
        ("업무 성과 향상을 위한 현업 데이터 기반 데이터 분석", None),
        (None, None),
    ])
    def test_maps_keywords(self, name, expected):
        assert course_group(name) == expected

    def test_every_short_name_is_a_group(self):
        # HRD 과정 약칭(COURSE_SHORT_NAMES)이 그룹 키와 어긋나면 대조가 통째로 빠진다
        assert set(config.COURSE_SHORT_NAMES.values()) <= set(config.COURSE_GROUP_KEYWORDS)


# ── parse_ops_pages ───────────────────────────────────────────────────


class TestParseOpsPages:
    def test_columns_and_values(self, notion_pages):
        df = parse_ops_pages(notion_pages)
        assert list(df.columns) == NOTION_OPS_COLUMNS + ["그룹"]
        row = df.iloc[0]
        assert row["과정명"].startswith("멀티에이전트")
        assert row["구분"] == "AI캠퍼스" and row["팀"] == "사업1팀" and row["강의실"] == "동캠 4"
        assert row["개강일"] == "2026-07-09" and row["종강일"] == "2027-01-04"
        assert row["추가인원"] == 0 and row["확정자신고"] == 24 and row["현재인원"] == 23
        assert row["그룹"] == "AIO"

    def test_never_carries_personal_name_properties(self, notion_pages):
        df = parse_ops_pages(notion_pages)
        assert not any(c in df.columns for c in ("PM", "강사", "지각 / 결석"))
        assert not df.astype(str).apply(lambda s: s.str.contains("홍길동|김아무개")).any().any()

    def test_fills_missing_formulas_from_base_numbers(self, notion_pages):
        df = parse_ops_pages(notion_pages)
        mle = df[df["그룹"] == "MLE"].iloc[0]
        assert mle["확정자신고"] == 26 - 6 + 1          # 21
        assert mle["현재인원"] == 21 - 4                # 17
        assert mle["이탈합계"] == 6 + 4

    def test_untracked_course_has_no_group(self):
        df = parse_ops_pages([_page("업무 성과 향상을 위한 현업 데이터 분석 1기", "2025-10-21", "2025-11-08", 15, 0, 0, 0)])
        assert df["그룹"].isna().all()

    def test_empty(self):
        df = parse_ops_pages([])
        assert df.empty and "그룹" in df.columns


# ── fetch_ops_table ───────────────────────────────────────────────────


def _resp(status, payload=None, text=""):
    r = MagicMock()
    r.status_code = status
    r.json.return_value = payload or {}
    r.text = text
    return r


class TestFetchOpsTable:
    def test_paginates_until_has_more_false(self, notion_pages):
        session = MagicMock()
        session.post.side_effect = [
            _resp(200, {"results": notion_pages[:2], "has_more": True, "next_cursor": "c2"}),
            _resp(200, {"results": notion_pages[2:], "has_more": False, "next_cursor": None}),
        ]
        df = fetch_ops_table("tok", session=session)
        assert len(df) == 3
        assert session.post.call_count == 2
        first, second = session.post.call_args_list
        assert "start_cursor" not in first.kwargs["json"]
        assert second.kwargs["json"]["start_cursor"] == "c2"
        # 읽기 전용: query 엔드포인트만, 토큰·버전 헤더 포함
        assert first.args[0].endswith(f"/databases/{config.NOTION_OPS_DB_ID}/query")
        assert first.kwargs["headers"]["Authorization"] == "Bearer tok"
        assert first.kwargs["headers"]["Notion-Version"] == config.NOTION_API_VERSION
        assert first.kwargs["json"]["filter"]["date"]["on_or_after"] == config.NOTION_OPS_SINCE

    @pytest.mark.parametrize("status,needle", [
        (401, "토큰"),
        (404, "공유"),
        (403, "공유"),
        (500, "500"),
    ])
    def test_http_errors_become_korean_messages(self, status, needle):
        session = MagicMock()
        session.post.return_value = _resp(status, text="boom")
        with pytest.raises(NotionFetchError) as ei:
            fetch_ops_table("tok", session=session)
        assert needle in str(ei.value)

    def test_network_error(self):
        import requests
        session = MagicMock()
        session.post.side_effect = requests.ConnectionError("down")
        with pytest.raises(NotionFetchError):
            fetch_ops_table("tok", session=session)


# ── CSV ───────────────────────────────────────────────────────────────


class TestCsv:
    @pytest.mark.parametrize("raw,expected", [
        ("2026년 7월 9일", "2026-07-09"),
        ("2026년 12월 28일", "2026-12-28"),
        ("2026-07-09", "2026-07-09"),
        ("2026/07/09", "2026-07-09"),
        ("July 9, 2026", "2026-07-09"),
        ("2026년 7월 9일 → 2027년 1월 4일", "2026-07-09"),
        ("", None),
        (None, None),
        ("abc", None),
    ])
    def test_parse_notion_date(self, raw, expected):
        assert parse_notion_date(raw) == expected

    def test_load_ops_csv_normalizes(self):
        csv = (
            "﻿과정명,구분,팀,강의실,개강일,종강일,교육일수,개강인원,초기이탈,추가인원 (개강~확정자신고),"
            "확정자신고,중도이탈,현재인원,이탈합계,지각/외출,결석,PM,강사\n"
            "멀티에이전트 AI 오케스트레이션 캠프 2기,AI캠퍼스,사업1팀,동캠 5,2026년 8월 6일,2027년 1월 29일,120일,"
            "20,1,0,19,1,18,2,2,0,홍길동,김강사\n"
        )
        df = load_ops_csv(io.StringIO(csv))
        assert list(df.columns) == NOTION_OPS_COLUMNS + ["그룹"]
        row = df.iloc[0]
        assert row["개강일"] == "2026-08-06" and row["종강일"] == "2027-01-29"
        assert row["추가인원"] == 0 and row["확정자신고"] == 19 and row["지각외출"] == 2
        assert row["그룹"] == "AIO"
        assert "PM" not in df.columns and "강사" not in df.columns

    def test_load_ops_csv_missing_columns_tolerated(self):
        df = load_ops_csv(io.StringIO("과정명,개강일,개강인원,초기이탈\n데이터 분석 & AI 머신러닝 3기,2026-09-21,10,1\n"))
        assert df.iloc[0]["확정자신고"] == 9      # 추가인원 없음 → 0으로 보정
        assert pd.isna(df.iloc[0]["종강일"])


# ── compare_ops ───────────────────────────────────────────────────────


class TestCompareOps:
    def test_outer_join_on_group_and_start_date(self, hrd_history, notion_pages):
        notion = parse_ops_pages(notion_pages)
        cmp = compare_ops(hrd_history, notion, today="2026-09-08")

        # 회차 0 제외 → AIO1(양쪽), MLE1(양쪽), AIO3(HRD만), SKN35(노션만 — HRD 이력에 없음)
        assert len(cmp) == 4
        by = cmp.set_index(["그룹", "개강일"])
        assert by.loc[("AIO", "2026-07-09"), "매칭"] == "양쪽"
        assert by.loc[("MLE", "2026-07-16"), "매칭"] == "양쪽"
        assert by.loc[("AIO", "2026-09-15"), "매칭"] == "HRD만"
        assert by.loc[("SKN", "2026-07-07"), "매칭"] == "노션만"

    def test_confirmed_match_and_mismatch(self, hrd_history, notion_pages):
        cmp = compare_ops(hrd_history, parse_ops_pages(notion_pages), today="2026-09-08")
        by = cmp.set_index(["그룹", "개강일"])
        aio = by.loc[("AIO", "2026-07-09")]
        assert aio["확정_차이"] == 0 and aio["판정"] == "✅ 일치"
        mle = by.loc[("MLE", "2026-07-16")]
        assert mle["HRD_확정"] == 20 and mle["노션_확정자신고"] == 21
        assert mle["확정_차이"] == -1 and mle["판정"] == "⚠️ 불일치"
        assert by.loc[("AIO", "2026-09-15"), "판정"] == "— 한쪽만"

    def test_rates(self, hrd_history, notion_pages):
        cmp = compare_ops(hrd_history, parse_ops_pages(notion_pages), today="2026-09-08")
        by = cmp.set_index(["그룹", "개강일"])
        aio = by.loc[("AIO", "2026-07-09")]
        assert aio["등록확정전환율"] == pytest.approx(24 / 31 * 100, abs=0.05)
        assert aio["개강참석률_노션"] == pytest.approx(26 / 31 * 100, abs=0.05)
        assert aio["등록대비개강차이"] == 26 - 31
        mle = by.loc[("MLE", "2026-07-16")]
        assert mle["개강참석률_노션"] > 100          # 개강 26 > 등록 25 — 실측에서 실제로 나온 케이스

    def test_status_uses_notion_end_when_hrd_missing(self, hrd_history, notion_pages):
        cmp = compare_ops(hrd_history, parse_ops_pages(notion_pages), today="2026-09-08")
        by = cmp.set_index(["그룹", "개강일"])
        assert by.loc[("AIO", "2026-09-15"), "상태"] == "개설예정"
        assert by.loc[("AIO", "2026-07-09"), "상태"] == "진행중"
        # 노션만 있는 행은 종강일로 판정
        only_notion = parse_ops_pages([_page("데이터 분석 & AI 머신러닝 9기", "2025-01-01", "2025-06-30", 10, 0, 0, 0)])
        cmp2 = compare_ops(hrd_history.iloc[:0], only_notion, today="2026-09-08")
        assert cmp2.iloc[0]["매칭"] == "노션만" and cmp2.iloc[0]["상태"] == "종료"

    def test_handles_none_inputs(self):
        cmp = compare_ops(None, None)
        assert cmp.empty and "매칭" in cmp.columns

    def test_sorted_newest_first(self, hrd_history, notion_pages):
        cmp = compare_ops(hrd_history, parse_ops_pages(notion_pages), today="2026-09-08")
        assert cmp["개강일"].tolist() == sorted(cmp["개강일"].tolist(), reverse=True)


# ── roster_current_counts ─────────────────────────────────────────────


class TestRosterCurrentCounts:
    def test_counts_only_active_trainees(self):
        trainees = pd.DataFrame({
            "TRPR_ID": ["A"] * 5 + ["B"],
            "TRPR_DEGR": ["1"] * 5 + ["2"],
            "TRNEE_ID": list("abcdef"),
            "TRNEE_STATUS": ["훈련중", "훈련중", "정상수료", "중도탈락", "제적", "훈련중"],
        })
        out = roster_current_counts(trainees).set_index(["TRPR_ID", "TRPR_DEGR"])["HRD_훈련중"]
        assert out.loc[("A", 1)] == 2
        assert out.loc[("B", 2)] == 1

    def test_empty(self):
        assert roster_current_counts(None).empty
        assert roster_current_counts(pd.DataFrame()).empty
