"""market_etl.py 유틸리티 함수 테스트"""
import datetime as dt
from bs4 import BeautifulSoup
from market_etl import parse_rows_xml, ymd, month_shards, week_shards


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

    def test_no_srchlist(self):
        soup = BeautifulSoup("<HRDNet><other>data</other></HRDNet>", "lxml-xml")
        assert parse_rows_xml(soup) == []
