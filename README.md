# 📊 HRD-Net 훈련 과정 성과 분석 시스템

한국산업인력공단(HRD-Net) 공공데이터를 활용하여 직업훈련 과정의 성과를 분석하고, 훈련생의 중도 탈락 위험을 관리하는 **대시보드**입니다.

## 🎯 프로젝트 개요

훈련 기관에서 운영 중인 과정들의 수료율, 취업률 데이터를 수집/분석하고, 실시간 출결 모니터링을 통해 **데이터 기반의 운영 관리**를 돕기 위해 개발되었습니다.

### 🔑 주요 기능

1.  **자동화된 데이터 수집 (ETL)**: HRD-Net Open API를 통해 과정, 훈련생, 출결 데이터를 DB로 자동 적재
2.  **종합 현황 대시보드**: 누적 수료율, 취업률(고용보험+HRD), 연도별 추이 시각화
3.  **기수별 심층 분석**: 종료된 과정의 연령별/유형별 성과 및 출석 패턴 분석
4.  **위험군 조기 탐지**: 현재 진행 중인 과정의 결석/휴가 패턴을 분석하여 중도 탈락 위험군 자동 추출
5.  **데이터 감사 (Audit)**: 수집된 원본 데이터의 정합성 검증

## 🛠️ 기술 스택 (Tech Stack)

- **Language**: Python 3.12
- **Web Framework**: Streamlit
- **Database**: SQLite
- **Data Processing**: Pandas
- **Visualization**: Altair, Plotly
- **API**: HRD-Net Open API

## 📂 프로젝트 구조

```text
📦 hrd-dashboard
 ┣ 📂 pages
 ┃ ┣ 📜 1_📊_기수별_분석.py    # 종료 과정 성과 분석
 ┃ ┣ 📜 2_🚨_진행과정_관리.py  # 실시간 출결/위험군 관리
 ┃ ┗ 📜 3_🔎_데이터_감사.py    # DB 원본 데이터 조회
 ┣ 📜 home.py                 # 메인 대시보드 (KPI)
 ┣ 📜 hrd_etl.py              # 데이터 수집 스크립트 (ETL)
 ┣ 📜 utils.py                # 공통 함수 모음
 ┗ 📜 hrd_analysis.db         # SQLite 데이터베이스 (Local)
```

### 2️⃣ 커밋 컨벤션 (Commit Convention)

**[Conventional Commits]** 방식

#### 📌 기본 규칙

> **`태그: 작업 내용 요약`**

#### 📋 태그 종류 (Cheat Sheet)

| 태그         | 설명                                        | 사용 예시                                  |
| :----------- | :------------------------------------------ | :----------------------------------------- |
| **Feat**     | 새로운 기능 추가                            | `Feat: 중도탈락 위험군 탐지 로직 추가`     |
| **Fix**      | 버그 수정                                   | `Fix: 'B'등급 데이터 float 변환 에러 수정` |
| **Docs**     | 문서 수정 (README 등)                       | `Docs: README.md 프로젝트 개요 작성`       |
| **Style**    | 코드 포맷팅, 세미콜론 누락 등 (로직 변경 X) | `Style: hrd_etl.py 불필요한 공백 제거`     |
| **Refactor** | 코드 리팩토링 (기능 변경 없이 구조 개선)    | `Refactor: DB 연결 함수 utils.py로 분리`   |
| **Chore**    | 빌드, 패키지 매니저 설정 등                 | `Chore: .gitignore에 .env 추가`            |

<details> <summary><strong>🚀 설치 및 실행 방법 (클릭하여 펼치기)</strong></summary>

1. 환경 설정

```
# 가상환경 생성
python -m venv .venv

# 가상환경 활성화
# Windows
.venv\Scripts\activate
# Mac/Linux
source .venv/bin/activate

# 패키지 설치
pip install -r requirements.txt
```

2. 환경 변수 설정

```
HRD_API_KEY="발급받은_API_키"
HANWHA_COURSE_ID="분석할_과정_ID"
```

3. 데이터 수집(ETL)

```
python hrd_etl.py
```

4.대시보드 실행

```
streamlit run home.py
```

</details>

## ⏰ 자동화 설정 (Automation)

이 시스템은 **Windows 작업 스케줄러**를 통해 매일 데이터를 자동 업데이트하도록 설계되었습니다.

1. **실행 주기**: 매일 09:10 ~ 18:10 (1시간 간격 권장)
2. **실행 파일**: `.venv/Scripts/python.exe`
3. **인수(Arguments)**: `hrd_etl.py`
4. **시작 위치(Start in)**: 프로젝트 폴더 경로 (필수)

## 📈 시장 동향 분석 (Market Trend Analysis)

HRD-Net 공공데이터포털 API를 활용하여 대한민국 전체 정보통신(IT) 훈련 시장 데이터를 수집하고 분석합니다.
(2023.01 ~ 2026.01 기간의 약 317,000건 데이터 전수 분석)

### 📂 주요 파일 구성

- **`market_etl.py`**: 대용량 XML 데이터 수집기 (ETL)
  - 자동 재시도(Retry), 병렬 처리(Threading), 세션 관리 탑재
  - JSON API 오류를 우회하기 위한 XML 파싱 및 지역코드 파라미터 최적화 적용
- **`init_market_db.py`**: 데이터베이스 스키마 초기화 스크립트
- **`TB_MARKET_TREND` (DB Table)**: 시장 분석 전용 테이블 (컬럼 30개+)

### ⚙️ 설치 및 실행 방법 (Setup & Usage)

**1. 필수 라이브러리 설치**

```bash
pip install -r requirements.txt
```

**2. DB 테이블 생성 (최초 1회) 시장 분석을 위한 TB_MARKET_TREND 테이블을 생성**

```bash
python init_market_db.py
```

**3. 데이터 수집 시작 (ETL) 2023년부터 현재까지의 시장 데이터를 수집하여 DB에 적재합니다. (약 30만 건, 소요시간 5~10분)**

```bash
python market_etl.py
```
