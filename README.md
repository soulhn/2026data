# 📊 HRD-Net 훈련 과정 성과 분석 & 시장 동향 시스템

한국산업인력공단(HRD-Net) 공공데이터를 활용하여 **내부 훈련 과정의 성과 관리**와 **외부 IT 훈련 시장의 트렌드**를 통합 분석하는 대시보드입니다.

## 🎯 프로젝트 개요

### 1. 내부 성과 관리 (Internal Management)

운영 중인 과정의 **수료율, 취업률, 실시간 출결**을 모니터링하여 중도 탈락 위험을 방지하고 성과를 관리합니다.

### 2. 시장 동향 분석 (Market Intelligence)

2023~2026년 대한민국 전체 정보통신(IT) 훈련 과정(약 31만 건)을 분석하여 **경쟁사 현황, 인기 키워드, 적정 훈련비** 등 전략적 인사이트를 제공합니다.

---

## 📂 프로젝트 구조 (Project Structure)

```text
📦 hrd-dashboard
 ┣ 📂 pages
 ┃ ┣ 📜 1_📊_기수별_분석.py      # [내부] 종료 과정 성과/인구통계 분석
 ┃ ┣ 📜 2_🚨_진행과정_관리.py    # [내부] 실시간 출결/위험군 탐지/보고서 생성
 ┃ ┣ 📜 3_🔎_데이터_감사.py      # [공통] DB 원본 데이터 조회 (Audit)
 ┃ ┗ 📜 4_📈_시장_동향_분석.py    # [외부] 시장 규모, 경쟁사, 트렌드 분석
 ┣ 📜 home.py                   # 메인 대시보드 (KPI 요약)
 ┣ 📜 hrd_etl.py                # [수집] 내부 과정/훈련생/출결 데이터 (JSON)
 ┣ 📜 market_etl.py             # [수집] 외부 시장 전체 데이터 (XML, 대용량)
 ┣ 📜 init_market_db.py         # [DB] 시장 분석용 테이블 초기화
 ┣ 📜 utils.py                  # [공통] DB 연결, 날짜 계산 등 유틸리티
 ┣ 📜 hrd_analysis.db           # SQLite 데이터베이스 (Local)
 ┗ 📜 requirements.txt          # 의존성 라이브러리 목록
```

---

## 📝 커밋 컨벤션 (Commit Convention)

**[Conventional Commits]** 방식을 따릅니다.

### 📌 기본 규칙

> **`태그: 작업 내용 요약`**

### 📋 태그 종류 (Cheat Sheet)

| 태그         | 설명                      | 사용 예시                                  |
| :----------- | :------------------------ | :----------------------------------------- |
| **Feat**     | 새로운 기능 추가          | `Feat: 중도탈락 위험군 탐지 로직 추가`     |
| **Fix**      | 버그 수정                 | `Fix: 'B'등급 데이터 float 변환 에러 수정` |
| **Docs**     | 문서 수정 (README 등)     | `Docs: README.md 프로젝트 개요 작성`       |
| **Style**    | 코드 포맷팅 (로직 변경 X) | `Style: hrd_etl.py 불필요한 공백 제거`     |
| **Refactor** | 코드 리팩토링             | `Refactor: DB 연결 함수 utils.py로 분리`   |
| **Chore**    | 설정 변경, 패키지 관리 등 | `Chore: .gitignore에 .env 추가`            |

---

## 🚀 설치 및 실행 방법 (Setup & Usage)

### 1. 환경 설정

```bash
# 가상환경 생성 및 활성화
python -m venv .venv
# Windows: .venv\Scripts\activate
# Mac/Linux: source .venv/bin/activate

# 필수 라이브러리 설치
pip install -r requirements.txt
```

### 2. 환경 변수 설정 (`.env` 파일 생성)

프로젝트 루트에 `.env` 파일을 만들고 아래 내용을 입력하세요.

```ini
HRD_API_KEY="발급받은_인증키_입력"
HANWHA_COURSE_ID="관리할_내부_과정_ID"
```

### 3. 데이터베이스 구축 (ETL)

**① 시장 데이터 준비 (최초 1회 필수)**
시장 분석을 위한 대용량 데이터를 수집합니다. (2023.01 ~ 2026.01, 약 5~10분 소요)

```bash
# 테이블 생성
python init_market_db.py

# 데이터 수집 실행
python market_etl.py
```

**② 내부 과정 데이터 준비 (수시 실행)**
내부 운영 과정의 최신 현황을 업데이트합니다.

```bash
# 과정/훈련생/출결 데이터 수집
python hrd_etl.py
```

### 4. 대시보드 실행

```bash
streamlit run home.py
```

---

## 🛠️ 기술적 특징 (Technical Highlights)

- **ETL (Extract, Transform, Load)**
  - **Robustness:** `market_etl.py`에 자동 재시도(Retry) 및 세션 관리를 적용하여 대용량(30만 건) 수집 안정성 확보
  - **Smart Update:** `hrd_etl.py`는 종료된 과정의 중복 수집을 방지하는 스마트 스킵 로직 적용
- **Performance**
  - Streamlit의 `@st.cache_data`를 적극 활용하여 대시보드 로딩 속도 최적화
  - 대용량 데이터 조회 시 `Pagination` 및 `Sampling` 기법 적용 (브라우저 메모리 보호)
- **Visualization**
  - Plotly & Altair를 활용한 인터랙티브 차트 구현
  - 워드클라우드, 산점도(Scatter), 파이 차트 등 다각적 시각화
