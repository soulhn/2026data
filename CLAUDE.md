# CLAUDE.md - 프로젝트 컨텍스트

## 프로젝트 개요

HRD-Net 공공데이터 기반 훈련 과정 성과 분석 대시보드 (Streamlit + SQLite)

## 커밋 컨벤션

`태그: 작업 내용 요약` (Feat / Fix / Docs / Style / Refactor / Chore)

## 환경 변수 (.env)

- `HRD_API_KEY` - HRD-Net API 인증키
- `HANWHA_COURSE_ID` - 내부 관리 대상 과정 ID

---

## HRD-Net API 명세

### 공통 사항

- **Base Domain:** `https://hrd.work24.go.kr`
- **인증:** 모든 요청에 `authKey` 파라미터 필수
- **인코딩:** UTF-8

---

### API 1: (이름)

- **URL:** ``
- **Method:** GET
- **응답 형식:** JSON / XML
- **용도:**

#### 요청 파라미터

| 파라미터  | 필수 | 설명       | 예시 |
| --------- | ---- | ---------- | ---- |
| `authKey` | Y    | API 인증키 |      |
|           |      |            |      |

#### 응답 예시

```json

```

#### 비고

- ***

### API 2: (이름)

- **URL:** ``
- **Method:** GET
- **응답 형식:** JSON / XML
- **용도:**

#### 요청 파라미터

| 파라미터  | 필수 | 설명       | 예시 |
| --------- | ---- | ---------- | ---- |
| `authKey` | Y    | API 인증키 |      |
|           |      |            |      |

#### 응답 예시

```json

```

#### 비고

- ***

<!-- 필요한 만큼 API 섹션을 추가하세요 -->

국민내일배움카드 훈련과정API 목록

1. 요청 URL
   1.1) URL
   https://hrd.work24.go.kr/jsp/HRDP/HRDPO00/HRDPOA60/HRDPOA60_1.jsp
   1.2) 사용예제
   예제1) 기본조건만을 이용하여 검색하는 경우

https://hrd.work24.go.kr/jsp/HRDP/HRDPO00/HRDPOA60/HRDPOA60_1.jsp?authKey=[인증키]&returnType=XML&outType=1&pageNum=1&pageSize=20&srchTraStDt=20141001&srchTraEndDt=20141231&sort=ASC&sortCol=2

예제2) 선택조건을 추가하는 경우

https://hrd.work24.go.kr/jsp/HRDP/HRDPO00/HRDPOA60/HRDPOA60_1.jsp?authKey=[인증키]&returnType=XML&outType=1&pageNum=1&pageSize=20&srchTraStDt=20141001&srchTraEndDt=20141231&srchTraArea1=[훈련지역 대분류]&sort=ASC&sortCol=2

2. 요청 Parameters
   요청 Parameters의 항목, 타입, 필수여부 및 설명 제공
   항목 타입 필수여부 설명
   파라미터명 string 선택,필수 해당필드에 대한 설명
   authKey string 필수 인증키(ex : authKey=[인증키]
   returnType string 필수 리턴타입:XML, JSON 중 하나로 지정합니다.(ex : returnType=XML)
   outType string 필수 구분자 : 출력형태('1':리스트 '2':상세)
   pageNum string 필수 시작페이지. 기본값 1, 최대 1000 검색 시작위치를 지정할 수 있습니다. 최대 1000 까지 가능.
   pageSize string 필수 페이지당 출력건수, 기본값 10, 최대 100까지 가능.
   wkendSe string 선택 주말/주중 구분 주말 : 1
   주말, 주중 혼합 : 2
   주중 : 3
   해당없음 : 9

- 전체일 경우에는 옵션 파라미터의 미등록처리
  srchTraArea1 string 선택 훈련지역 대분류 '11' : 서울, '26' : 부산, '27' : 대구, '28' : 인천 '29' : 광주, '30' : 대전, '31' : 울산, '36' : 세종, '41' : 경기, '43' : 충북, '44' : 충남, '45' : 전북, '46' : 전남, '47' : 경북, '48' : 경남, '50' : 제주, '51' : 강원
- 전체일 경우에는 옵션 파라미터의 미등록처리
  srchTraArea2 string 선택 훈련지역 중분류
  훈련지역 대분류에 따라 내용이 달라짐 \* 해당 코드관련 API 제공
- 훈련지역 대분류 제외시 이항목도 옵션 파라미터에 미등록처리
  srchNcs1 string 선택 NCS 직종 1차분류 코드 '01' : 사업관리
  '02' : 경영/회계/사무
  '03' : 금융/보험
  '04' : 교육/자연/사회과학
  '05' : 법률/경찰/소방/교도/국방
  '06' : 보건/의료
  '07' : 사회복지/종교
  '08' : 문화/예술/디자인/방송
  '09' : 운전/운송
  '10' : 영업판매
  '11' : 경비/청소
  '12' : 이용/숙박/여행/오락/스포츠 '13' : 음식서비스
  '14' : 건설
  '15' : 기계
  '16' : 재료
  '17' : 화학/바이오
  '18' : 섬유/의복
  '19' : 전기/전자
  '20' : 정보통신
  '21' : 식품가공
  '22' : 인쇄/목재/가구/공예
  '23' : 환경/에너지/안전
  '24' : 농림어업
- 전체일 경우에는 옵션 파라미터의 미등록처리
  srchNcs2 string 선택 NCS 직종 2차분류 코드
  상위분류에 따라 내용이 달라짐 \* 해당 코드관련 API 제공
- 전체일 경우에는 옵션 파라미터의 미등록처리
  srchNcs3 string 선택 NCS 직종 3차분류 코드
  상위분류에 따라 내용이 달라짐 \* 해당 코드관련 API 제공
- 전체일 경우에는 옵션 파라미터의 미등록처리
  srchNcs4 string 선택 NCS 직종 4차분류 코드
  상위분류에 따라 내용이 달라짐 \* 해당 코드관련 API 제공
- 전체일 경우에는 옵션 파라미터의 미등록처리
  crseTracseSe string 선택 훈련유형 'C0061' : 국민내일배움카드(일반)
  'C0061S' : 국민내일배움카드(주 훈련대상 : 구직자)
  'C0061I' : 국민내일배움카드(주 훈련대상 : 재직자)
  'C0054' : 국가기간전략산업직종
  'C0055C' : 과정평가형훈련
  'C0054G' : 기업맞춤형훈련
  'C0054Y' : 스마트혼합훈련
  'C0054S' : 일반고특화훈련
  'C0104' : K-디지털 트레이닝
  'C0105' : K-디지털 기초역량훈련
  'C0102' : 산업구조변화대응
  'C0055' : 실업자 원격훈련
  'C0031' : 근로자 원격훈련
  'C0031C' : 돌봄서비스훈련
  'C0031F' : 근로자 외국어훈련
- 전체일 경우에는 옵션 파라미터의 미등록처리
  srchTraGbn string 선택 훈련구분코드 'M1001' : 일반과정
  'M1005' : 인터넷과정
  'M1010' : 혼합과정(BL)
  'M1014' : 스마트혼합훈련
- 전체일 경우에는 옵션 파라미터의 미등록처리
  srchTraType string 선택 훈련종류(훈련구분에 따라 세부내용이 변경됨) '\* 해당 코드관련 API 제공
- 훈련지역 대분류 제외시 이항목도 옵션 파라미터에 미등록처리
- 인터넷과정과 혼합과정의 경우 세부항목이 없음
  srchTraStDt string 필수 훈련시작일 From
  srchTraEndDt string 필수 훈련시작일 To
  srchTraProcessNm string 선택 훈련과정명
  srchTraOrganNm string 선택 훈련기관명
  sort string 필수 정렬방법 "ASC",
  "DESC"
  sortCol string 필수 정렬컬럼 훈련기관명 : 1
  훈련시작일 : 2
  훈련기관 직종별 취업률 : 3
  만족도점수 : 5

3. 출력결과
   출력결과의 출력항목, 타입, 설명, 비고에 대한 정보 제공
   출력항목 타입 설명 비고
   <HRDNet> XML문서의 최상위 노드입니다.
   <scn_cnt> string 검색된 총 건수 </scn_cnt>
   <pageNum> string 현재페이지 </pageNum>
   <pageSize> string 페이지당 출력개수, 페이지당 표현될 자료의 개수 </pageSize>
   <srchList> string
   <scn_list>
   ADDRESS string 주소 <address></address>
   CERTIFICATE string 자격증 <certificate></certificate>
   CONTENTS string 컨텐츠 <contents></contents>
   COURSE_MAN string 수강비 <courseMan></courseMan>
   EI_EMPL_CNT3 string 고용보험3개월 취업인원 수 <eiEmplCnt3></eiEmplCnt3>
   EI_EMPL_CNT3_GT10 string 고용보험3개월 취업누적인원 10인이하 여부 (Y/N)
   17.11.07부터 제공되지 않는 항목이나 기존 API 사용자를 위해 Null값을 제공 <eiEmplCnt3Gt10></eiEmplCnt3Gt10>
   EI_EMPL_RATE3 string 고용보험3개월 취업률 <eiEmplRate3></eiEmplRate3>
   EI_EMPL_RATE6 string 고용보험6개월 취업률 <eiEmplRate6></eiEmplRate6>
   GRADE string 등급 <grade></grade>
   INST_CD string 훈련기관 코드 <instCd></instCd>
   NCS_CD string NCS 코드 <ncsCd></ncsCd>
   REAL_MAN string 실제 훈련비 <realMan></realMan>
   REG_COURSE_MAN string 수강신청 인원 <regCourseMan></regCourseMan>
   STDG_SCOR string 만족도 점수 <stdgScor></stdgScor>
   SUB_TITLE string 부 제목 <subTitle></subTitle>
   SUB_TITLE_LINK string 부 제목 링크 <subTitleLink></subTitleLink>
   TEL_NO string 전화번호 <telNo></telNo>
   TITLE string 제목 <title></title>
   TITLE_ICON string 제목 아이콘 <titleIcon></titleIcon>
   TITLE_LINK string 제목 링크 <titleLink></titleLink>
   TRA_END_DATE string 훈련종료일자 <traEndDate></traEndDate>
   TRA_START_DATE string 훈련시작일자 <traStartDate></traStartDate>
   TRAIN_TARGET string 훈련대상 <trainTarget></trainTarget>
   TRAIN_TARGET_CD string 훈련구분 <trainTargetCd></trainTargetCd>
   TRAINST_CST_ID string 훈련기관ID <trainstCstId></trainstCstId>
   TRNG_AREA_CD string 지역코드(중분류) <trngAreaCd></trngAreaCd>
   TRPR_DEGR string 훈련과정 순차 <trprDegr></trprDegr>
   TRPR_ID string 훈련과정ID <trprId></trprId>
   WKED_SE string 주말/주중 구분 <wkendSe></wkendSe>
   YARD_MAN string 정원 <yardMan></yardMan>
   </scn_list>
   </srchList>
   </HRDNet>

국민내일배움카드 훈련과정API 훈련일정

1. 요청 URL
   1.1) URL
   https://hrd.work24.go.kr/jsp/HRDP/HRDPO00/HRDPOA60/HRDPOA60_3.jsp
   1.2) 사용예제
   예제1) 기본조건만을 이용하여 검색하는 경우

https://hrd.work24.go.kr/jsp/HRDP/HRDPO00/HRDPOA60/HRDPOA60_3.jsp?authKey=[인증키]&returnType=XML&outType=2&srchTrprId=[훈련과정ID]&srchTrprDegr=[훈련회차]&srchTorgId=[훈련기관ID]

2. 요청 Parameters
   요청 Parameters의 항목, 타입, 필수여부 및 설명 제공
   항목 타입 필수여부 설명
   파라미터명 string 선택,필수 해당필드에 대한 설명
   authKey string 필수 인증키(ex : authKey=[인증키]
   returnType string 필수 리턴타입:XML, JSON 중 하나로 지정합니다. (ex : returnType=XML)
   outType string 필수 구분자 : 출력형태('1':리스트 '2':상세) '2'
   srchTrprId string 필수 훈련과정 ID
   srchTrprDegr string 선택 훈련과정 회차(입력하지 않으면 모든 회차가 조회됩니다.)
3. 출력결과
   출력결과의 출력항목, 타입, 설명, 비고에 대한 정보 제공
   출력항목 타입 설명 비고
   <HRDNet> XML문서의 최상위 노드입니다.
   <scn_list> string
   EI_EMPL_RATE_3 string 3개월 고용보험 취업률(%)
   'A‘ : 개설예정
   ‘B‘ : 진행중
   ‘C‘ : 미실시
   ‘D‘ : 수료자 없음
   숫자 : 취업률 <eiEmplRate3></eiEmplRate3>
   EI_EMPL_CNT_3 string 3개월 고용보험 취업인원 <eiEmplCnt3></eiEmplCnt3>
   EI_EMPL_RATE_6 string 6개월 고용보험 취업률(%)
   'A‘ : 개설예정
   ‘B‘ : 진행중
   ‘C‘ : 미실시
   ‘D‘ : 수료자 없음
   숫자 : 취업률 <eiEmplRate6></eiEmplRate6>
   EI_EMPL_CNT_6 string 6개월 고용보험 취업인원 <eiEmplCnt6></eiEmplCnt6>
   HRD_EMPL_RATE_6 string 6개월 고용보험 미가입 취업률(%)
   'A‘ : 개설예정
   ‘B‘ : 진행중
   ‘C‘ : 미실시
   ‘D‘ : 수료자 없음
   숫자 : 취업률 <hrdEmplRate6></hrdEmplRate6>
   HRD_EMPL_CNT_6 string 6개월 고용보험 미가입 취업인원 <hrdEmplCnt6></hrdEmplCnt6>
   INST_INO string 훈련기관관리번호 <instIno></instIno>
   2 string 모집인원(정원) <totFxnum></totFxnum>
   TOT_PAR_MKS string 수강인원 <totParMks></totParMks>
   FINI_CNT string 수료인원 <finiCnt></finiCnt>
   TOT_TRCO string 총 훈련비 <totTrco></totTrco>
   TOT_TRP_CNT string 수강(신청) 인원 <totTrpCnt></totTrpCnt>
   TR_END_DT string 훈련종료일 <trEndDt></trEndDt>
   TR_STA_DT string 훈련 시작일 <trStaDt></trStaDt>
   TRPR_DEGR string 훈련과정 회차 <trprDegr></trprDegr>
   TRPR_ID string 훈련과정ID <trprId></trprId>
   TRPR_NM string 훈련과정명 <trprNm></trprNm>
   </scn_list> string
   </HRDNet>

국민내일배움카드 훈련과정API 훈련생 출결정보

1. 요청 URL
   1.1) URL
   https://hrd.work24.go.kr/jsp/HRDP/HRDPO00/HRDPOA60/HRDPOA60_4.jsp
   1.2) 사용예제
   예제1) 기본조건만을 이용하여 검색하는 경우

https://hrd.work24.go.kr/jsp/HRDP/HRDPO00/HRDPOA60/HRDPOA60_4.jsp?authKey=[인증키]&returnType=XML&outType=2&srchTrprId=[훈련과정ID]&srchTrprDegr=[훈련회차]&srchTorgId=[훈련기관ID]

2. 요청 Parameters
   요청 Parameters의 항목, 타입, 필수여부 및 설명 제공
   항목 타입 필수여부 설명
   파라미터명 string 선택,필수 해당필드에 대한 설명
   authKey string 필수 인증키(ex : authKey=[인증키]
   returnType string 필수 리턴타입:XML, JSON 중 하나로 지정합니다. (ex : returnType=XML)
   srchTrprId string 필수 훈련과정 ID
   srchTrprDegr string 필수 훈련과정 회차
   srchTorgId string 선택,필수 출결 상세정보
3. 출력결과
   출력결과의 출력항목, 타입, 설명, 비고에 대한 정보 제공
   출력항목 타입 설명 비고
   <HRDNet> XML문서의 최상위 노드입니다.
   <atab_cnt> 훈련생 출결리스트 수
   <totTrneeCo> 총훈련생수
   <trne_cnt> 훈련생 수
   <trneList> string
   <trne_list>
   ABSENT_CNT string 결석일수 <absentCnt></absentCnt>
   ATEND_CNT string 출석일수 <atendCnt></atendCnt>
   LIFYEAMD string 훈련생 생년월일 <lifyeaMd></lifyeaMd>
   OFLHD_CNT string 공가일수 <oflhdCnt></oflhdCnt>
   TRACSE_ID string 과정 ID <tracseId></tracseId>
   TRACSE_TME string 과정 회차 <tracseTme></tracseTme>
   TRANING_DE_CNT string 훈련일수 <traingDeCnt></traingDeCnt>
   CSTMR_ID string 훈련생 코드 <trneeCstmrId></trneeCstmrId>
   CSTMR_NM string 훈련생 이름 <trneeCstmrNm></trneeCstmrNm>
   STTUS_NM string 훈련생 상태 <trneeSttusNm></trneeSttusNm>
   TRANEE_TRACSE_SE string 훈련생 유형 <trneeTracseSe></trneeTracseSe>
   VCATN_CNT string 휴가일수 <vcatnCnt></vcatnCnt>
   </trne_list>
   </trneList>
   <atabList>
   <atab_list>
   ATEND_DE string 출석일 <atendDe></atendDe>
   ATEND_STTUS_NM string 출결여부 <atendSttusNm></atendSttusNm>
   CSTMR_NM string 훈련생 이름 <cstmrNm></cstmrNm>
   KOR_DAY_NM string 훈련요일 <korDayNm></korDayNm>
   LEVROM_TIME string 퇴실시간 <levromTime></levromTime>
   LPSIL_TIME string 입실시간 <lpsilTime></lpsilTime>
   CSTMR_ID string 훈련생 코드 <trneeCstmrId></trneeCstmrId>
   </atabList>
   <atabList>
   </HRDNet>
