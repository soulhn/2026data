# HRD-Net API 명세

**Base Domain:** `https://hrd.work24.go.kr`
**인증:** 모든 요청에 `authKey` 파라미터 필수 / **인코딩:** UTF-8

---

## API 1: 훈련과정 목록 조회

**URL:** `https://hrd.work24.go.kr/jsp/HRDP/HRDPO00/HRDPOA60/HRDPOA60_1.jsp`

### 필수 파라미터
| 파라미터 | 설명 |
|---|---|
| `authKey` | 인증키 |
| `returnType` | `XML` 또는 `JSON` |
| `outType` | `1` (리스트) |
| `pageNum` | 시작페이지 (기본 1, 최대 1000) |
| `pageSize` | 페이지당 건수 (기본 10, 최대 100) |
| `srchTraStDt` | 훈련시작일 From (YYYYMMDD) |
| `srchTraEndDt` | 훈련시작일 To (YYYYMMDD) |
| `sort` | `ASC` / `DESC` |
| `sortCol` | `1`:기관명 `2`:시작일 `3`:취업률 `5`:만족도 |

### 선택 파라미터
| 파라미터 | 설명 |
|---|---|
| `wkendSe` | `1`:주말 `2`:혼합 `3`:주중 `9`:해당없음 |
| `srchTraArea1` | 지역 대분류: `11`:서울 `26`:부산 `27`:대구 `28`:인천 `29`:광주 `30`:대전 `31`:울산 `36`:세종 `41`:경기 `43`:충북 `44`:충남 `45`:전북 `46`:전남 `47`:경북 `48`:경남 `50`:제주 `51`:강원 |
| `srchNcs1` | NCS 1차: `01`:사업관리 `02`:경영/회계/사무 `03`:금융/보험 `04`:교육/자연/사회과학 `05`:법률/경찰/소방 `06`:보건/의료 `07`:사회복지/종교 `08`:문화/예술/디자인 `09`:운전/운송 `10`:영업판매 `11`:경비/청소 `12`:이용/숙박/여행 `13`:음식서비스 `14`:건설 `15`:기계 `16`:재료 `17`:화학/바이오 `18`:섬유/의복 `19`:전기/전자 `20`:정보통신 `21`:식품가공 `22`:인쇄/목재 `23`:환경/에너지 `24`:농림어업 |
| `crseTracseSe` | 훈련유형: `C0061`:내일배움카드(일반) `C0061S`:내일배움(구직자) `C0061I`:내일배움(재직자) `C0054`:국가기간전략 `C0055C`:과정평가형 `C0054G`:기업맞춤형 `C0054Y`:스마트혼합 `C0054S`:일반고특화 `C0104`:K-디지털트레이닝 `C0105`:K-디지털기초 `C0102`:산업구조변화 `C0055`:실업자원격 `C0031`:근로자원격 `C0031C`:돌봄서비스 `C0031F`:근로자외국어 |
| `srchTraGbn` | 훈련구분: `M1001`:일반 `M1005`:인터넷 `M1010`:혼합(BL) `M1014`:스마트혼합 |
| `srchTraProcessNm` | 훈련과정명 |
| `srchTraOrganNm` | 훈련기관명 |

### 주요 응답 필드
`TRPR_ID`, `TRPR_DEGR`, `EI_EMPL_RATE3`, `EI_EMPL_RATE6`, `YARD_MAN`, `REG_COURSE_MAN`, `REAL_MAN`, `NCS_CD`, `CERTIFICATE`, `TRNG_AREA_CD`, `TRA_START_DATE`, `TRA_END_DATE`, `WKED_SE`, `TRAIN_TARGET_CD`

---

## API 2: 훈련일정 상세 조회

**URL:** `https://hrd.work24.go.kr/jsp/HRDP/HRDPO00/HRDPOA60/HRDPOA60_3.jsp`

### 파라미터
| 파라미터 | 필수 | 설명 |
|---|---|---|
| `authKey` | 필수 | 인증키 |
| `returnType` | 필수 | `XML` 또는 `JSON` |
| `outType` | 필수 | `2` (상세) |
| `srchTrprId` | 필수 | 훈련과정 ID |
| `srchTrprDegr` | 선택 | 훈련과정 회차 (미입력 시 전체) |

### 주요 응답 필드
`TRPR_ID`, `TRPR_DEGR`, `TRPR_NM`, `TR_STA_DT`, `TR_END_DT`, `TOT_FXNUM`(정원), `TOT_PAR_MKS`(수강인원), `TOT_TRP_CNT`(수강신청), `FINI_CNT`(수료인원), `TOT_TRCO`(총훈련비), `EI_EMPL_RATE_3`, `EI_EMPL_RATE_6`, `EI_EMPL_CNT_3`, `EI_EMPL_CNT_6`

취업률 특수값: `A`:개설예정 `B`:진행중 `C`:미실시 `D`:수료자없음

---

## API 3: 훈련생 출결정보 조회

**URL:** `https://hrd.work24.go.kr/jsp/HRDP/HRDPO00/HRDPOA60/HRDPOA60_4.jsp`

### 파라미터
| 파라미터 | 필수 | 설명 |
|---|---|---|
| `authKey` | 필수 | 인증키 |
| `returnType` | 필수 | `XML` 또는 `JSON` |
| `srchTrprId` | 필수 | 훈련과정 ID |
| `srchTrprDegr` | 필수 | 훈련과정 회차 |
| `srchTorgId` | 선택 | 훈련기관 ID |

### 주요 응답 필드 (훈련생 목록 `trneList`)
`CSTMR_ID`, `CSTMR_NM`, `STTUS_NM`(상태), `TRANEE_TRACSE_SE`(유형), `ATEND_CNT`(출석), `ABSENT_CNT`(결석), `VCATN_CNT`(휴가), `OFLHD_CNT`(공가), `TRANING_DE_CNT`(훈련일수)

### 주요 응답 필드 (출결 상세 `atabList`)
`CSTMR_ID`, `CSTMR_NM`, `ATEND_DE`(출석일), `ATEND_STTUS_NM`(출결여부), `LPSIL_TIME`(입실시간), `LEVROM_TIME`(퇴실시간), `KOR_DAY_NM`(요일)

---

## 사람인 채용공고 API

별도 명세 파일: [`saramin.md`](saramin.md)

- **엔드포인트:** `https://oapi.saramin.co.kr/guide/v1/job-search`
- **인증:** `access-key` 쿼리 파라미터, 일일 500회 제한
- **용도:** IT 채용공고 수집 → `TB_JOB_POSTING` 테이블 → 채용 동향 대시보드
