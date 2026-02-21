# DB 스키마 명세서

> **DB 엔진:** Supabase PostgreSQL (운영) / SQLite (로컬 개발)
> **스키마 정의:** `init_db.py`
> **최종 갱신:** 2026-02-21

---

## 테이블 요약

| 테이블 | 용도 | 데이터 출처 | PK | 대략 건수 |
|---|---|---|---|---|
| `TB_COURSE_MASTER` | 과정 마스터 (회차별 운영 정보) | HRD-Net API 2 (상세조회) | `(TRPR_ID, TRPR_DEGR)` | ~10 |
| `TB_TRAINEE_INFO` | 훈련생 정보 (인적사항, 출결 요약) | HRD-Net API 3 (출결정보) | `(TRPR_ID, TRPR_DEGR, TRNEE_ID)` | ~200 |
| `TB_ATTENDANCE_LOG` | 출결 로그 (일별 입퇴실 기록) | HRD-Net API 3 (출결정보) | `UNIQUE(TRPR_ID, TRPR_DEGR, TRNEE_ID, ATEND_DT)` | ~20,000 |
| `TB_MARKET_TREND` | 시장 동향 (전국 IT 훈련과정) | HRD-Net API 1 (목록조회) | `(TRPR_ID, TRPR_DEGR)` | ~320,000 |

---

## TB_COURSE_MASTER

내부 관리 대상 과정의 회차별 운영 정보. `hrd_etl.py`가 HRD-Net API 2(훈련일정 상세조회)로 수집.

| 컬럼 | 타입 | 설명 | API 필드 | 비고 |
|---|---|---|---|---|
| `TRPR_ID` | TEXT | 훈련과정 ID | `trprId` | **PK** |
| `TRPR_DEGR` | INTEGER | 훈련과정 회차 | `trprDegr` | **PK** |
| `TRPR_NM` | TEXT | 훈련과정명 | `trprNm` | |
| `TR_STA_DT` | TEXT | 훈련 시작일 | `trStaDt` | YYYYMMDD |
| `TR_END_DT` | TEXT | 훈련 종료일 | `trEndDt` | YYYYMMDD |
| `TOT_TRCO` | INTEGER | 총 훈련비 (원) | `totTrco` | |
| `FINI_CNT` | INTEGER | 수료 인원 | `finiCnt` | |
| `TOT_FXNUM` | INTEGER | 정원 | `totFxnum` | |
| `TOT_PAR_MKS` | INTEGER | 수강 인원 (개강 인원) | `totParMks` | |
| `TOT_TRP_CNT` | INTEGER | 수강 신청 인원 | `totTrpCnt` | |
| `INST_INO` | TEXT | 훈련기관 관리번호 | `instIno` | |
| `EI_EMPL_RATE_3` | TEXT | 3개월 고용보험 취업률 (%) | `eiEmplRate3` | A/B/C/D 또는 숫자 |
| `EI_EMPL_CNT_3` | INTEGER | 3개월 고용보험 취업 인원 | `eiEmplCnt3` | |
| `EI_EMPL_RATE_6` | TEXT | 6개월 고용보험 취업률 (%) | `eiEmplRate6` | A/B/C/D 또는 숫자 |
| `EI_EMPL_CNT_6` | INTEGER | 6개월 고용보험 취업 인원 | `eiEmplCnt6` | |
| `HRD_EMPL_RATE_6` | TEXT | 6개월 HRD 자체 취업률 (%) | `hrdEmplRate6` | 고용보험 미가입 취업 |
| `HRD_EMPL_CNT_6` | INTEGER | 6개월 HRD 자체 취업 인원 | `hrdEmplCnt6` | |
| `REAL_EMPL_RATE` | REAL | 실질 취업률 (%) | 파생 | EI + HRD 합산 |
| `COLLECTED_AT` | TIMESTAMP | 수집 시각 | - | DEFAULT CURRENT_TIMESTAMP |

**취업률 특수값:** `A`=개설예정, `B`=진행중, `C`=미실시, `D`=수료자 없음

---

## TB_TRAINEE_INFO

훈련생 개인 정보 및 출결 요약. `hrd_etl.py`가 HRD-Net API 3(출결정보)의 `trneList`로 수집.

| 컬럼 | 타입 | 설명 | API 필드 | 비고 |
|---|---|---|---|---|
| `TRPR_ID` | TEXT | 훈련과정 ID | `tracseId` | **PK** |
| `TRPR_DEGR` | INTEGER | 훈련과정 회차 | `tracseTme` | **PK** |
| `TRNEE_ID` | TEXT | 훈련생 코드 | `trneeCstmrId` | **PK** |
| `TRNEE_NM` | TEXT | 훈련생 이름 | `trneeCstmrNm` | |
| `TRNEE_STATUS` | TEXT | 훈련생 상태 | `trneeSttusNm` | 훈련중/수료/중도탈락/제적 |
| `TRNEE_TYPE` | TEXT | 훈련생 유형 | `trneeTracseSe` | |
| `BIRTH_DATE` | TEXT | 생년월일 | `lifyeaMd` | YYYYMMDD |
| `TOTAL_DAYS` | INTEGER | 훈련일수 | `traingDeCnt` | |
| `OFLHD_CNT` | INTEGER | 공가 일수 | `oflhdCnt` | |
| `VCATN_CNT` | INTEGER | 휴가 일수 | `vcatnCnt` | |
| `ABSENT_CNT` | INTEGER | 결석 일수 | `absentCnt` | 마이그레이션 추가 |
| `ATEND_CNT` | INTEGER | 출석 일수 | `atendCnt` | 마이그레이션 추가 |
| `COLLECTED_AT` | TIMESTAMP | 수집 시각 | - | DEFAULT CURRENT_TIMESTAMP |

**TRNEE_STATUS 값:** `훈련중`, `수료`, `중도탈락`, `제적`

---

## TB_ATTENDANCE_LOG

일별 출결 상세 기록. `hrd_etl.py`가 HRD-Net API 3(출결정보)의 `atabList`로 수집.

| 컬럼 | 타입 | 설명 | API 필드 | 비고 |
|---|---|---|---|---|
| `TRPR_ID` | TEXT | 훈련과정 ID | `tracseId` | **UK** |
| `TRPR_DEGR` | INTEGER | 훈련과정 회차 | `tracseTme` | **UK** |
| `TRNEE_ID` | TEXT | 훈련생 코드 | `trneeCstmrId` | **UK** |
| `ATEND_DT` | TEXT | 출석일 | `atendDe` | **UK**, YYYY-MM-DD |
| `DAY_NM` | TEXT | 요일명 | `korDayNm` | 월/화/수/목/금 |
| `IN_TIME` | TEXT | 입실 시간 | `lpsilTime` | HH:MM |
| `OUT_TIME` | TEXT | 퇴실 시간 | `levromTime` | HH:MM |
| `ATEND_STATUS` | TEXT | 출결 상태 | `atendSttusNm` | 출석/결석/지각/조퇴/외출 |
| `ATEND_STATUS_CD` | TEXT | 출결 상태 코드 | - | |
| `COLLECTED_AT` | TIMESTAMP | 수집 시각 | - | DEFAULT CURRENT_TIMESTAMP |

**UK** = UNIQUE 제약조건 구성 컬럼

**출결 판정 로직 (대시보드):**
- `IN_TIME` 있고 `ATEND_STATUS`='결석' → **입실중** (수업 중, 퇴실 전)
- `IN_TIME` 없음 → **결석**
- `IN_TIME` 있고 `OUT_TIME` 없음 → **미퇴실**

---

## TB_MARKET_TREND

전국 IT(NCS 코드 20) 훈련과정 시장 데이터. `market_etl.py`가 HRD-Net API 1(목록조회)로 수집.

| 컬럼 | 타입 | 설명 | API 필드 | 비고 |
|---|---|---|---|---|
| `TRPR_ID` | TEXT | 훈련과정 ID | `trprId` | **PK** |
| `TRPR_DEGR` | INTEGER | 훈련과정 순차 | `trprDegr` | **PK** |
| `TRPR_NM` | TEXT | 과정명 | `title` | |
| `TRAINST_NM` | TEXT | 훈련기관명 | `subTitle` | |
| `TR_STA_DT` | TEXT | 훈련 시작일 | `traStartDate` | **YYYY-MM-DD** (WHERE절 파라미터도 반드시 이 형식 사용) |
| `TR_END_DT` | TEXT | 훈련 종료일 | `traEndDate` | YYYYMMDD |
| `NCS_CD` | TEXT | NCS 코드 | `ncsCd` | 20=정보통신 |
| `TRNG_AREA_CD` | TEXT | 지역코드 (중분류) | `trngAreaCd` | |
| `TOT_FXNUM` | INTEGER | 정원 | `yardMan` | |
| `TOT_TRCO` | REAL | 훈련비 (원) | `realMan` | |
| `COURSE_MAN` | REAL | 수강비 (원) | `courseMan` | |
| `REG_COURSE_MAN` | INTEGER | 수강 신청 인원 | `regCourseMan` | |
| `EI_EMPL_RATE_3` | REAL | 3개월 취업률 (%) | `eiEmplRate3` | |
| `EI_EMPL_RATE_6` | REAL | 6개월 취업률 (%) | `eiEmplRate6` | |
| `EI_EMPL_CNT_3` | INTEGER | 3개월 취업 인원 | `eiEmplCnt3` | |
| `EI_EMPL_CNT_3_GT10` | TEXT | 10인 미만 여부 | `eiEmplCnt3Gt10` | Y/N (현재 미제공) |
| `STDG_SCOR` | REAL | 만족도 점수 | `stdgScor` | |
| `GRADE` | TEXT | 등급 | `grade` | |
| `CERTIFICATE` | TEXT | 관련 자격증 | `certificate` | 구분자: `,` `/` `\|` `\n` |
| `CONTENTS` | TEXT | 과정 내용 | `contents` | |
| `ADDRESS` | TEXT | 훈련기관 주소 | `address` | |
| `TEL_NO` | TEXT | 전화번호 | `telNo` | |
| `INST_INO` | TEXT | 훈련기관 코드 | `instCd` | |
| `TRAINST_CST_ID` | TEXT | 훈련기관 ID | `trainstCstId` | |
| `TRAIN_TARGET` | TEXT | 훈련 유형명 | `trainTarget` | 국민내일배움카드 등 |
| `TRAIN_TARGET_CD` | TEXT | 훈련 유형 코드 | `trainTargetCd` | C0061, C0054 등 |
| `WKEND_SE` | TEXT | 주말/주중 구분 | `wkendSe` | 1=주중, 2=주말, 3=혼합 |
| `TITLE_ICON` | TEXT | 제목 아이콘 | `titleIcon` | |
| `TITLE_LINK` | TEXT | 제목 링크 | `titleLink` | |
| `SUB_TITLE_LINK` | TEXT | 부제목 링크 | `subTitleLink` | |
| `YEAR_MONTH` | TEXT | 개설 연월 | 파생 | ETL에서 계산, YYYY-MM |
| `REGION` | TEXT | 지역 (시/도) | 파생 | ADDRESS 첫 단어 |
| `COLLECTED_AT` | TIMESTAMP | 수집 시각 | - | DEFAULT CURRENT_TIMESTAMP |

---

## 인덱스

| 인덱스명 | 테이블 | 컬럼 | 용도 |
|---|---|---|---|
| `IDX_MARKET_NCS` | TB_MARKET_TREND | NCS_CD | NCS 분류별 필터링 |
| `IDX_MARKET_DATE` | TB_MARKET_TREND | TR_STA_DT | 기간별 조회 |
| `IDX_MARKET_AREA` | TB_MARKET_TREND | TRNG_AREA_CD | 지역별 필터링 |
| `IDX_MARKET_TRAINST` | TB_MARKET_TREND | TRAINST_NM | 기관별 집계 |
| `IDX_MARKET_YEAR_MONTH` | TB_MARKET_TREND | YEAR_MONTH | 월별 시계열 집계 |
| `IDX_MARKET_REGION` | TB_MARKET_TREND | REGION | 지역별 집계 |
| `IDX_MARKET_TARGET` | TB_MARKET_TREND | TRAIN_TARGET | 유형별 집계 |
| `IDX_ATTEND_DEGR` | TB_ATTENDANCE_LOG | TRPR_DEGR | 기수별 출결 조회 |
| `IDX_ATTEND_DATE` | TB_ATTENDANCE_LOG | ATEND_DT | 날짜별 출결 조회 |
| `IDX_COURSE_END_DT` | TB_COURSE_MASTER | TR_END_DT | 진행중/종료 과정 분류 |

---

## ERD (관계)

```text
TB_COURSE_MASTER (TRPR_ID, TRPR_DEGR)
    │
    ├──< TB_TRAINEE_INFO (TRPR_ID, TRPR_DEGR, TRNEE_ID)
    │        │
    │        └──< TB_ATTENDANCE_LOG (TRPR_ID, TRPR_DEGR, TRNEE_ID, ATEND_DT)
    │
    └── TB_MARKET_TREND (TRPR_ID, TRPR_DEGR)  ← 동일 PK 구조, 별도 데이터 소스
```

- **TB_COURSE_MASTER → TB_TRAINEE_INFO:** 1:N (과정 1개에 훈련생 다수)
- **TB_TRAINEE_INFO → TB_ATTENDANCE_LOG:** 1:N (훈련생 1명에 출결 기록 다수)
- **TB_MARKET_TREND:** 독립 테이블 (외부 시장 데이터, TRPR_ID로 내부 과정과 매칭 가능)
