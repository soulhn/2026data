"""notion_jobs_publish.py — 채용공고 행 변환(마감 판정·쉼표 목록·옵션명)과 multi_select 속성 변환. SQL은 타지 않는다."""
import pandas as pd
import pytest

import notion_jobs_publish as jobs
from notion_publish import to_properties


def _df(**over):
    base = dict(JOB_ID=123, POSITION_TITLE="데이터 엔지니어", COMPANY_NM="회사", REGION="서울", LOC_NM="서울 &gt; 강남구",
                EXPERIENCE_NM="신입", EDU_LV_NM="대학졸업(2,3년)이상", JOB_TYPE_NM="정규직, 계약직, 정규직", IND_NM="IT",
                JOB_NM="데이터, 분석", SALARY_NM="회사내규", CLOSE_TYPE_NM="접수마감일", POSTING_DT="2026-09-01",
                EXPIRATION_DT="2026-09-30", POSITION_URL="https://www.saramin.co.kr/x", ACTIVE=1, TRACKS="AIO,MLE", ENTRY_LEVEL=1)
    base.update(over)
    return pd.DataFrame([base], dtype=object)   # None을 NaN으로 바꾸지 않도록


@pytest.fixture
def rows_from(monkeypatch):
    def run(df, today="2026-09-21"):
        monkeypatch.setattr(jobs, "load_data", lambda q, params=None, db=None: df)
        return jobs.build_rows(today=today)
    return run


class TestBuildRows:
    def test_row_mapping(self, rows_from):
        r = rows_from(_df())[0]
        assert r["KEY"] == "123" and r["공고 ID"] == "123"
        assert r["해당 과정"] == ["AIO", "MLE"] and r["신입 가능"] is True
        assert r["학력"] == "대학졸업(2·3년)이상"            # 노션 select 옵션명에 쉼표 불가
        assert r["고용형태"] == ["정규직", "계약직"]           # 쉼표 분할 + 중복 제거 + 순서 유지
        assert r["근무지"] == "서울 > 강남구"
        assert r["상태"] == "진행중" and r["마감일"] == "2026-09-30"

    @pytest.mark.parametrize("active, exp, today, expected", [
        (1, "2026-09-30", "2026-09-21", "진행중"),
        (1, "2026-09-21", "2026-09-21", "진행중"),    # 마감 당일까지 진행중
        (1, "2026-09-20", "2026-09-21", "마감"),      # 마감일 지남
        (0, "2026-09-30", "2026-09-21", "마감"),      # 사람인에서 내려감
        (1, None, "2026-09-21", "진행중"),            # 마감일 없음(상시)
    ])
    def test_status(self, rows_from, active, exp, today, expected):
        assert rows_from(_df(ACTIVE=active, EXPIRATION_DT=exp), today=today)[0]["상태"] == expected

    def test_empty_fields(self, rows_from):
        r = rows_from(_df(POSITION_TITLE=None, EDU_LV_NM=None, JOB_TYPE_NM=None, TRACKS=None, ENTRY_LEVEL=None, LOC_NM=None))[0]
        assert r["공고 제목"] == "(제목 없음)" and r["학력"] is None and r["근무지"] is None
        assert r["고용형태"] == [] and r["해당 과정"] == [] and r["신입 가능"] is False


    def test_nan_int_columns(self, rows_from):
        """PG NULL이 pandas에서 NaN(float)으로 오는 경우 — int(NaN)으로 터지지 않고 마감·신입 아님으로."""
        df = pd.DataFrame([{**_df().iloc[0].to_dict(), "ACTIVE": float("nan"), "ENTRY_LEVEL": float("nan")}])
        r = rows_from(df)[0]
        assert r["상태"] == "마감" and r["신입 가능"] is False


def test_int_helper():
    assert jobs._i(None) == 0 and jobs._i(float("nan")) == 0 and jobs._i(1.0) == 1 and jobs._i("2") == 2


def test_split_dedup_and_strip():
    assert jobs._split(" a, b ,a,,c") == ["a", "b", "c"]
    assert jobs._split(None) == [] and jobs._split(float("nan")) == []


def test_multi_select_properties(rows_from):
    props = to_properties(jobs.SCHEMA, rows_from(_df())[0])
    assert props["해당 과정"] == {"multi_select": [{"name": "AIO"}, {"name": "MLE"}]}
    assert props["고용형태"]["multi_select"] == [{"name": "정규직"}, {"name": "계약직"}]
    assert props["신입 가능"] == {"checkbox": True}
