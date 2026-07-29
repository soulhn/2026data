"""market_etl.py 유틸리티 함수 테스트"""
import datetime as dt
import pytest
from bs4 import BeautifulSoup

import init_db
import market_etl
from market_etl import parse_rows_xml, ymd, month_shards, week_shards, _normalize_stdg_scor


class TestYmd:
    def test_basic(self):
        assert ymd(dt.date(2024, 1, 5)) == "20240105"

    def test_end_of_year(self):
        assert ymd(dt.date(2024, 12, 31)) == "20241231"


class TestMonthShards:
    def test_single_month(self):
        shards = list(month_shards(dt.date(2024, 3, 1), dt.date(2024, 3, 31)))
        assert len(shards) == 1
        assert shards[0] == (dt.date(2024, 3, 1), dt.date(2024, 3, 31))

    def test_two_months(self):
        shards = list(month_shards(dt.date(2024, 1, 15), dt.date(2024, 2, 20)))
        assert len(shards) == 2
        assert shards[0][0] == dt.date(2024, 1, 15)
        assert shards[1][1] == dt.date(2024, 2, 20)

    def test_cross_year(self):
        shards = list(month_shards(dt.date(2023, 11, 1), dt.date(2024, 2, 28)))
        assert len(shards) == 4


class TestWeekShards:
    def test_single_week(self):
        shards = list(week_shards(dt.date(2024, 1, 1), dt.date(2024, 1, 7)))
        assert len(shards) == 1

    def test_two_weeks(self):
        shards = list(week_shards(dt.date(2024, 1, 1), dt.date(2024, 1, 14)))
        assert len(shards) == 2

    def test_partial_week(self):
        shards = list(week_shards(dt.date(2024, 1, 1), dt.date(2024, 1, 3)))
        assert len(shards) == 1
        assert shards[0] == (dt.date(2024, 1, 1), dt.date(2024, 1, 3))


class TestParseRowsXml:
    def test_empty_xml(self):
        soup = BeautifulSoup("<HRDNet></HRDNet>", "lxml-xml")
        assert parse_rows_xml(soup) == []

    def test_single_row(self):
        xml = """<HRDNet>
        <srchList>
            <scn_list>
                <trprId>T001</trprId>
                <trprDegr>1</trprDegr>
                <title>테스트 과정</title>
                <subTitle>부제목</subTitle>
                <traStartDate>20240101</traStartDate>
                <traEndDate>20240630</traEndDate>
                <ncsCd>20</ncsCd>
                <trngAreaCd>11</trngAreaCd>
                <yardMan>30</yardMan>
                <realMan>5000000</realMan>
                <courseMan>100000</courseMan>
                <regCourseMan>25</regCourseMan>
                <eiEmplRate3>75.5</eiEmplRate3>
                <eiEmplRate6>80.0</eiEmplRate6>
                <eiEmplCnt3>15</eiEmplCnt3>
                <eiEmplCnt3Gt10>N</eiEmplCnt3Gt10>
                <stdgScor>8500</stdgScor>
                <grade>A</grade>
                <certificate>정보처리기사</certificate>
                <contents>내용</contents>
                <address>서울 강남구</address>
                <telNo>02-1234</telNo>
                <instCd>I001</instCd>
                <trainstCstId>C001</trainstCstId>
                <trainTarget>K-디지털</trainTarget>
                <trainTargetCd>C0104</trainTargetCd>
                <wkendSe>1</wkendSe>
                <titleIcon></titleIcon>
                <titleLink></titleLink>
                <subTitleLink></subTitleLink>
            </scn_list>
        </srchList>
        </HRDNet>"""
        soup = BeautifulSoup(xml, "lxml-xml")
        rows = parse_rows_xml(soup)
        assert len(rows) == 1
        assert rows[0][0] == "T001"  # TRPR_ID
        assert rows[0][1] == 1       # TRPR_DEGR (int)
        assert rows[0][2] == "테스트 과정"  # TRPR_NM

    def test_stdg_scor_normalized(self):
        """10000 스케일 값은 그대로, 100 스케일 값은 ×100 정규화."""
        xml = """<HRDNet><srchList>
            <scn_list>
                <trprId>T1</trprId><trprDegr>1</trprDegr>
                <title>A</title><subTitle>B</subTitle>
                <traStartDate>20240101</traStartDate><traEndDate>20240630</traEndDate>
                <ncsCd>20</ncsCd><trngAreaCd>11</trngAreaCd>
                <yardMan>30</yardMan><realMan>0</realMan><courseMan>0</courseMan><regCourseMan>0</regCourseMan>
                <eiEmplRate3>0</eiEmplRate3><eiEmplRate6>0</eiEmplRate6><eiEmplCnt3>0</eiEmplCnt3><eiEmplCnt3Gt10></eiEmplCnt3Gt10>
                <stdgScor>94.3</stdgScor><grade></grade>
                <certificate></certificate><contents></contents><address>서울</address><telNo></telNo>
                <instCd></instCd><trainstCstId></trainstCstId><trainTarget></trainTarget><trainTargetCd></trainTargetCd>
                <wkendSe></wkendSe><titleIcon></titleIcon><titleLink></titleLink><subTitleLink></subTitleLink>
            </scn_list>
        </srchList></HRDNet>"""
        soup = BeautifulSoup(xml, "lxml-xml")
        rows = parse_rows_xml(soup)
        assert rows[0][16] == 9430.0  # 94.3 * 100

    def test_no_srchlist(self):
        soup = BeautifulSoup("<HRDNet><other>data</other></HRDNet>", "lxml-xml")
        assert parse_rows_xml(soup) == []


class TestNormalizeStdgScor:
    def test_10000_scale_unchanged(self):
        assert _normalize_stdg_scor(9430) == 9430

    def test_100_scale_multiplied(self):
        assert _normalize_stdg_scor(94.3) == 9430.0

    def test_none_passthrough(self):
        assert _normalize_stdg_scor(None) is None

    def test_zero_passthrough(self):
        assert _normalize_stdg_scor(0) == 0

    def test_boundary_100(self):
        assert _normalize_stdg_scor(100) == 10000

    def test_boundary_above_100(self):
        assert _normalize_stdg_scor(100.1) == 100.1


def _course_xml(sta_dt, end_dt, reg_man=25):
    """개강·종강일과 신청인원만 바꾼 최소 과정 XML."""
    return f"""<HRDNet><srchList><scn_list>
        <trprId>T900</trprId><trprDegr>1</trprDegr>
        <title>일정 변경 과정</title>
        <traStartDate>{sta_dt}</traStartDate>
        <traEndDate>{end_dt}</traEndDate>
        <yardMan>30</yardMan><regCourseMan>{reg_man}</regCourseMan>
        <address>서울 강남구</address>
    </scn_list></srchList></HRDNet>"""


@pytest.fixture
def mock_market_db(monkeypatch, mock_db_connection):
    """mock_db_connection 위에 market_etl 모듈도 패치."""
    import utils
    monkeypatch.setattr(market_etl, "get_connection", lambda **kwargs: utils.get_connection())
    monkeypatch.setattr(market_etl, "is_pg", lambda: False)
    monkeypatch.setattr(market_etl, "adapt_query", utils.adapt_query)
    init_db.init_all_tables()
    return mock_db_connection


class TestSaveRowsUpsert:
    def test_start_date_change_is_reflected(self, mock_market_db):
        """개강 연기 시 TR_STA_DT도 갱신되어야 함.

        누락 시 YEAR_MONTH만 새 달로 바뀌어 개강 현황 표(개강일·D-day·상태)와
        월별 개강 예정 차트가 서로 다른 달을 가리킴.
        """
        first = parse_rows_xml(BeautifulSoup(_course_xml("2026-08-05", "2026-12-20"), "lxml-xml"))
        assert market_etl.save_rows(first) == 1

        delayed = parse_rows_xml(
            BeautifulSoup(_course_xml("2026-09-02", "2027-01-15", reg_man=28), "lxml-xml"))
        assert market_etl.save_rows(delayed) == 1

        cursor = mock_market_db.cursor()
        cursor.execute("""
            SELECT TR_STA_DT, TR_END_DT, YEAR_MONTH, REG_COURSE_MAN
            FROM TB_MARKET_TREND WHERE TRPR_ID = 'T900'
        """)
        # 회차(TRPR_DEGR)가 같으므로 행이 늘지 않고 최신값으로 갱신
        assert cursor.fetchall() == [("2026-09-02", "2027-01-15", "2026-09", 28)]


class TestGetCollectRange:
    def test_full_refresh_env_collects_from_archive_start(self, monkeypatch):
        import market_etl
        monkeypatch.setattr(market_etl, "FULL_REFRESH", True)
        start, end = market_etl.get_collect_range()
        assert start == market_etl.ARCHIVE_START
        # 종료일 = 오늘 + FUTURE_DAYS (모집 중·개강 예정 과정 포함)
        assert end == dt.date.today() + dt.timedelta(days=market_etl.FUTURE_DAYS)
