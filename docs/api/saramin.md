# 사람인 채용공고 API 명세

**Base URL:** `https://oapi.saramin.co.kr/`
**인증:** `access-key` 쿼리 파라미터 (일일 500회 제한)
**응답 형식:** XML(기본) 또는 JSON (`Accept: application/json` 헤더)

**제한사항:**
- 진행 중인 공고만 조회 가능 (마감된 공고 조회 불가)
- 1회 호출당 최대 110건 반환, 페이징 미지원

---

## 엔드포인트: 채용공고 검색

**URL:** `https://oapi.saramin.co.kr/job-search`

### 요청 파라미터

| 파라미터 | 필수 | 설명 |
|---|---|---|
| `access-key` | 필수 | 발급받은 API 키 |
| `keywords` | 선택 | 검색 키워드 |
| `id` | 선택 | 공고번호 (쉼표구분, 최대 200개) |
| `count` | 선택 | 페이지당 건수 (기본 10, 최대 110) |
| `start` | 선택 | 시작 위치 (기본 0) |
| `sort` | 선택 | 정렬: `pd` 게시일, `rc` 마감일, `ac` 정확도, `ud` 수정일 |
| `published` | 선택 | 게시일 기간: `0` 전체, `1` 1일 이내, `7` 7일 이내, `30` 30일 이내 |
| `published_min` | 선택 | 게시일 From (unix timestamp) |
| `published_max` | 선택 | 게시일 To (unix timestamp) |
| `loc_cd` | 선택 | 지역코드 (쉼표구분) |
| `ind_cd` | 선택 | 업종코드 (쉼표구분) |
| `job_cd` | 선택 | 직무코드 (쉼표구분) |
| `job_type` | 선택 | 근무형태코드 (쉼표구분) |
| `edu_lv` | 선택 | 학력코드 |
| `fields` | 선택 | 추가 응답 필드 (쉼표구분): `posting-date`, `expiration-date`, `count`, `keyword-code` 등 |
| `sr` | 선택 | 채용형태 필터: `directhire` (직접채용만, 헤드헌팅·파견 제외) |
| `job_mid_cd` | 선택 | 상위 직무코드 (하단 코드표 참조) |

### 응답 구조

```xml
<jobs count="100" start="0" total="1234">
  <job id="12345" active="1" url="https://...">
    <company>
      <name><![CDATA[기업명]]></name>
    </company>
    <position>
      <title><![CDATA[공고 제목]]></title>
      <industry>
        <code>301</code>
        <name><![CDATA[솔루션·SI·ERP·CRM]]></name>
      </industry>
      <location>
        <code>101000</code>
        <name><![CDATA[서울 &gt; 서울전체]]></name>
      </location>
      <job-type>
        <code>1</code>
        <name><![CDATA[정규직]]></name>
      </job-type>
      <job-mid-code>
        <code>2</code>
        <name><![CDATA[IT개발·데이터]]></name>
      </job-mid-code>
      <job-code>
        <code>84,92</code>
        <name><![CDATA[서버개발,백엔드/서버개발]]></name>
      </job-code>
      <experience-level>
        <code>1</code>
        <min>0</min>
        <max>0</max>
        <name><![CDATA[신입]]></name>
      </experience-level>
      <required-education-level>
        <code>7</code>
        <name><![CDATA[대학교졸업(4년)이상]]></name>
      </required-education-level>
    </position>
    <keyword><![CDATA[Python, Java]]></keyword>
    <salary>
      <code>99</code>
      <name><![CDATA[면접후 결정]]></name>
    </salary>
    <close-type>
      <code>1</code>
      <name><![CDATA[접수마감일]]></name>
    </close-type>
    <posting-timestamp>1710000000</posting-timestamp>
    <modification-timestamp>1710100000</modification-timestamp>
    <opening-timestamp>1710000000</opening-timestamp>
    <expiration-timestamp>1712592000</expiration-timestamp>
  </job>
</jobs>
```

---

## 에러 코드

| 코드 | 설명 |
|---|---|
| 1 | access-key 미입력 |
| 2 | 유효하지 않은 access-key |
| 3 | 일일 요청 한도 초과 (500회) |
| 4 | 요청 파라미터 오류 |
| 99 | 시스템 에러 |

---

## 코드표

### 근무형태 (job_type)

| 코드 | 명칭 |
|---|---|
| 1 | 정규직 |
| 2 | 계약직 |
| 3 | 병역특례 |
| 4 | 인턴직 |
| 5 | 아르바이트 |
| 6 | 프리랜서 |
| 7 | 전문연구요원 |
| 8 | 파견직 |
| 9 | 해외취업 |
| 10 | 위촉직 |
| 11 | 교육생 |
| 16 | 청년인턴 |
| 22 | 기타 |

### 학력 (edu_lv)

| 코드 | 명칭 |
|---|---|
| 0 | 학력무관 |
| 1 | 초등학교졸업 |
| 2 | 중학교졸업 |
| 3 | 고등학교졸업 |
| 4 | 대학졸업(2,3년) |
| 5 | 대학교졸업(4년) |
| 6 | 석사졸업 |
| 7 | 박사졸업 |
| 8 | 초등학교졸업이상 |
| 9 | 중학교졸업이상 |

### 경력 (experience-level code)

| 코드 | 명칭 |
|---|---|
| 0 | 경력무관 |
| 1 | 신입 |
| 2 | 경력 |
| 3 | 신입/경력 |

### 마감유형 (close-type code)

| 코드 | 명칭 |
|---|---|
| 1 | 접수마감일 |
| 2 | 채용시 |
| 3 | 상시 |
| 4 | 수시 |

### 1차 지역코드 (loc_cd 앞 3자리)

| 코드 | 지역 |
|---|---|
| 101 | 서울 |
| 102 | 경기 |
| 103 | 광주 |
| 104 | 대구 |
| 105 | 대전 |
| 106 | 부산 |
| 107 | 울산 |
| 108 | 인천 |
| 109 | 강원 |
| 110 | 경남 |
| 111 | 경북 |
| 112 | 전남 |
| 113 | 전북 |
| 114 | 충북 |
| 115 | 충남 |
| 116 | 제주 |
| 117 | 세종 |
| 118 | 전국 |

### 상위 직무코드 (job-mid-code)

| 코드 | 직무 |
|---|---|
| 2 | IT개발·데이터 |
| 3 | 마케팅·광고·홍보 |
| 4 | 디자인 |
| 5 | 영업·고객상담 |
| 7 | 인사·총무 |
| 8 | 재무·회계 |
| 10 | 생산·제조 |
| 13 | 서비스 |
| 14 | 건축·토목 |
| 16 | 의료 |

### 상위 산업/업종코드 (ind_cd 앞 2자리)

| 코드 | 산업 |
|---|---|
| 1 | 서비스업 |
| 2 | 제조·화학 |
| 3 | IT·웹·통신 |
| 4 | 은행·금융업 |
| 5 | 미디어·디자인 |
| 6 | 교육업 |
| 7 | 의료·제약·복지 |
| 8 | 유통·무역·운송 |
| 9 | 건설업 |
| 10 | 기관·협회 |

---

## DB 테이블 명세

### TB_JOB_POSTING

채용공고 원본 데이터. PK: `JOB_ID`

| 컬럼 | 타입 | 설명 |
|---|---|---|
| `JOB_ID` | TEXT PK | 사람인 공고 ID |
| `ACTIVE` | INTEGER | 공고 활성 상태 (1: 진행중, 0: 마감) |
| `COMPANY_NM` | TEXT | 기업명 |
| `POSITION_TITLE` | TEXT | 공고 제목 |
| `IND_CD` | TEXT | 업종코드 |
| `IND_NM` | TEXT | 업종명 |
| `JOB_MID_CD` | TEXT | 상위 직무코드 |
| `JOB_MID_NM` | TEXT | 상위 직무명 (예: IT개발·데이터) |
| `JOB_CD` | TEXT | 직무코드 (쉼표구분) |
| `JOB_NM` | TEXT | 직무명 (쉼표구분) |
| `LOC_CD` | TEXT | 지역코드 |
| `LOC_NM` | TEXT | 지역명 (예: 서울 > 강남구) |
| `JOB_TYPE_CD` | TEXT | 근무형태코드 |
| `JOB_TYPE_NM` | TEXT | 근무형태명 (예: 정규직) |
| `EDU_LV_CD` | TEXT | 학력코드 |
| `EDU_LV_NM` | TEXT | 학력 요건명 |
| `EXPERIENCE_CD` | TEXT | 경력코드 |
| `EXPERIENCE_MIN` | INTEGER | 최소 경력 (년) |
| `EXPERIENCE_MAX` | INTEGER | 최대 경력 (년) |
| `EXPERIENCE_NM` | TEXT | 경력 요건명 (예: 신입, 경력) |
| `SALARY_CD` | TEXT | 급여코드 |
| `SALARY_NM` | TEXT | 급여 조건명 |
| `CLOSE_TYPE_CD` | TEXT | 마감유형코드 |
| `CLOSE_TYPE_NM` | TEXT | 마감유형명 |
| `POSTING_DT` | TEXT | 게시일 (YYYY-MM-DD HH:MM:SS) |
| `EXPIRATION_DT` | TEXT | 마감일 |
| `OPENING_DT` | TEXT | 접수 시작일 |
| `MODIFICATION_DT` | TEXT | 수정일 |
| `KEYWORD` | TEXT | 공고 키워드 (API 원본, 쉼표구분) |
| `POSITION_URL` | TEXT | 공고 URL |
| `SEARCH_KEYWORD` | TEXT | 수집 시 사용된 검색 키워드 (마지막 매칭) |
| `YEAR_MONTH` | TEXT | 게시 연월 (YYYY-MM, `POSTING_DT` 기반) |
| `REGION` | TEXT | 1차 지역명 (`LOC_NM`에서 추출) |
| `COLLECTED_AT` | TIMESTAMP | 수집 시각 (기본값 CURRENT_TIMESTAMP) |

**인덱스:** `JOB_CD`, `JOB_MID_CD`, `LOC_CD`, `IND_CD`, `YEAR_MONTH`, `POSTING_DT`, `SEARCH_KEYWORD`

**중복 처리:** `ON CONFLICT(JOB_ID) DO UPDATE` — 동일 공고가 다른 키워드로 재수집 시 업데이트

### TB_JOB_POSTING_KEYWORD

채용공고 ↔ 검색 키워드 다대다 매핑. 하나의 공고가 여러 키워드 검색에 매칭될 수 있음.

| 컬럼 | 타입 | 설명 |
|---|---|---|
| `JOB_ID` | TEXT | 사람인 공고 ID (FK → TB_JOB_POSTING) |
| `SEARCH_KEYWORD` | TEXT | 수집 시 사용된 검색 키워드 |
| `COLLECTED_AT` | TIMESTAMP | 수집 시각 |

**PK:** (`JOB_ID`, `SEARCH_KEYWORD`) 복합키

### TB_MARKET_CACHE

ETL 후 사전 집계된 캐시 데이터. 대시보드에서 빠른 조회용.

| 컬럼 | 타입 | 설명 |
|---|---|---|
| `CACHE_KEY` | TEXT PK | 캐시 식별자 (예: `saramin_keyword_trend`) |
| `CACHE_DATA` | TEXT | JSON 직렬화된 집계 결과 |
| `COMPUTED_AT` | TIMESTAMP | 집계 시각 |
