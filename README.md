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
