"""사람인 채용공고 ETL 테스트"""
import json
import pytest
import saramin_etl
import init_db


@pytest.fixture
def mock_saramin_db(monkeypatch, mock_db_connection):
    """mock_db_connection 위에 saramin_etl 모듈도 패치."""
    import utils
    _get_conn = lambda **kwargs: utils.get_connection()
    monkeypatch.setattr(saramin_etl, "get_connection", _get_conn)
    monkeypatch.setattr(saramin_etl, "is_pg", lambda: False)
    monkeypatch.setattr(saramin_etl, "adapt_query", utils.adapt_query)
    init_db.init_all_tables()
    return mock_db_connection


SAMPLE_JSON = {
    "jobs": {
        "count": 2,
        "start": 0,
        "total": "2",
        "job": [
            {
                "id": "12345",
                "active": 1,
                "url": "https://saramin.co.kr/12345",
                "company": {"detail": {"name": "TestCorp"}},
                "position": {
                    "title": "Python Backend Developer",
                    "industry": {"code": "301", "name": "IT"},
                    "location": {"code": "101000", "name": "Seoul"},
                    "job-type": {"code": "1", "name": "Full-time"},
                    "job-mid-code": {"code": "2", "name": "IT Dev"},
                    "job-code": {"code": "84", "name": "Backend"},
                    "experience-level": {"code": "1", "min": 0, "max": 0, "name": "Entry"},
                    "required-education-level": {"code": "5", "name": "Bachelor"},
                },
                "keyword": "Python, Django",
                "salary": {"code": "99", "name": "TBD"},
                "close-type": {"code": "1", "name": "Deadline"},
                "posting-timestamp": "1710000000",
                "expiration-timestamp": "1712592000",
                "opening-timestamp": "1710000000",
            },
            {
                "id": "67890",
                "active": 0,
                "url": "https://saramin.co.kr/67890",
                "company": {"detail": {"name": "AnotherCorp"}},
                "position": {
                    "title": "Java Developer",
                    "industry": {"code": "302", "name": "SW"},
                    "location": {"code": "102000", "name": "Gyeonggi"},
                    "job-type": {"code": "2", "name": "Contract"},
                    "job-mid-code": {"code": "2", "name": "IT Dev"},
                    "job-code": {"code": "92", "name": "Server"},
                    "experience-level": {"code": "2", "min": 3, "max": 5, "name": "Experienced"},
                    "required-education-level": {"code": "0", "name": "Any"},
                },
                "keyword": "Java, Spring",
                "salary": {"code": "22", "name": "3500"},
                "close-type": {"code": "2", "name": "When filled"},
                "posting-timestamp": "1709900000",
                "expiration-timestamp": "1712500000",
                "opening-timestamp": "1709900000",
            },
        ],
    }
}

EMPTY_JSON = {"jobs": {"count": 0, "start": 0, "total": "0", "job": []}}


class TestParseJobsJson:
    def test_parse_two_jobs(self):
        rows, total = saramin_etl.parse_jobs_json(SAMPLE_JSON)
        assert total == 2
        assert len(rows) == 2

    def test_first_job_fields(self):
        rows, _ = saramin_etl.parse_jobs_json(SAMPLE_JSON)
        r = rows[0]
        assert r[0] == '12345'        # JOB_ID
        assert r[1] == 1              # ACTIVE
        assert r[2] == 'TestCorp'     # COMPANY_NM
        assert r[3] == 'Python Backend Developer'  # POSITION_TITLE
        assert r[10] == '101000'      # LOC_CD

    def test_experience_parsing(self):
        rows, _ = saramin_etl.parse_jobs_json(SAMPLE_JSON)
        r1 = rows[0]
        assert r1[16] == '1'    # EXPERIENCE_CD
        assert r1[17] == 0      # EXPERIENCE_MIN
        assert r1[18] == 0      # EXPERIENCE_MAX

        r2 = rows[1]
        assert r2[16] == '2'
        assert r2[17] == 3
        assert r2[18] == 5

    def test_empty_json(self):
        rows, total = saramin_etl.parse_jobs_json(EMPTY_JSON)
        assert total == 0
        assert rows == []

    def test_posting_dt_converted(self):
        rows, _ = saramin_etl.parse_jobs_json(SAMPLE_JSON)
        dt_str = rows[0][24]
        assert dt_str is not None
        assert len(dt_str) == 10
        assert dt_str[4] == '-'

    def test_parse_from_bytes(self):
        """bytes (resp.content) 입력도 처리 가능."""
        raw = json.dumps(SAMPLE_JSON).encode('utf-8')
        rows, total = saramin_etl.parse_jobs_json(raw)
        assert total == 2
        assert len(rows) == 2


class TestHelpers:
    def test_ts_to_date(self):
        assert saramin_etl._ts_to_date('1710000000') is not None
        assert saramin_etl._ts_to_date('') is None
        assert saramin_etl._ts_to_date(None) is None
        assert saramin_etl._ts_to_date('invalid') is None

    def test_ts_to_date_int(self):
        assert saramin_etl._ts_to_date(1710000000) is not None

    def test_extract_region(self):
        assert saramin_etl._extract_region('101000') == '서울'
        assert saramin_etl._extract_region('102000') == '경기'
        assert saramin_etl._extract_region('') is None
        assert saramin_etl._extract_region(None) is None
        assert saramin_etl._extract_region('999') is None

    def test_extract_region_multi_code(self):
        assert saramin_etl._extract_region('101000,102000') == '서울'


class TestSaveRows:
    def test_save_and_upsert(self, mock_saramin_db):
        rows, _ = saramin_etl.parse_jobs_json(SAMPLE_JSON)
        extended = [r + ('Python', r[24][:7] if r[24] else None, saramin_etl._extract_region(r[10])) for r in rows]
        saved = saramin_etl.save_rows(extended)
        assert saved == 2

        cursor = mock_saramin_db.cursor()
        cursor.execute("SELECT COUNT(*) AS cnt FROM TB_JOB_POSTING")
        assert cursor.fetchone()[0] == 2

        # Upsert same data — count stays same
        saved2 = saramin_etl.save_rows(extended)
        assert saved2 == 2
        cursor.execute("SELECT COUNT(*) AS cnt FROM TB_JOB_POSTING")
        assert cursor.fetchone()[0] == 2

    def test_save_empty(self, mock_saramin_db):
        assert saramin_etl.save_rows([]) == 0


class TestCacheAggregations:
    def test_aggregations_run(self, mock_saramin_db):
        rows, _ = saramin_etl.parse_jobs_json(SAMPLE_JSON)
        extended = [r + ('Python', r[24][:7] if r[24] else None, saramin_etl._extract_region(r[10])) for r in rows]
        saramin_etl.save_rows(extended)
        saramin_etl.compute_and_cache_aggregations()

        cursor = mock_saramin_db.cursor()
        cursor.execute("SELECT COUNT(*) AS cnt FROM TB_MARKET_CACHE WHERE CACHE_KEY LIKE 'saramin_%'")
        count = cursor.fetchone()[0]
        assert count == 5
