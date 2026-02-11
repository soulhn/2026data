# 📊 HRD-Net 훈련 과정 성과 분석 & 시장 동향 시스템

한국산업인력공단(HRD-Net) 공공데이터를 활용하여 **내부 훈련 과정의 성과 관리**와 **외부 IT 훈련 시장의 트렌드**를 통합 분석하는 대시보드입니다.

## 🎯 프로젝트 개요

### 1. 내부 성과 관리 (Internal Management)

운영 중인 과정의 **수료율, 취업률, 실시간 출결**을 모니터링하여 중도 탈락 위험을 방지하고 성과를 관리합니다.

### 2. 시장 동향 분석 (Market Intelligence)

2023~2026년 대한민국 전체 정보통신(IT) 훈련 과정(약 32만 건)을 분석하여 **경쟁사 현황, 인기 키워드, 적정 훈련비** 등 전략적 인사이트를 제공합니다.

---

## 🏗️ 아키텍처

```text
[GitHub Actions]                  [Supabase]              [Streamlit Cloud]
hrd_etl.py (평일 매시간 09~18시) → PostgreSQL DB ← 대시보드 (읽기 전용)
market_etl.py (매일 21시)        →               ←
```

- **DB:** Supabase (PostgreSQL) / 로컬 개발 시 SQLite 자동 폴백
- **ETL 자동화:** GitHub Actions (cron 스케줄)
- **대시보드:** Streamlit Cloud 배포

---

## 📂 프로젝트 구조

```text
📦 2026data
 ┣ 📂 .github/workflows
 ┃ ┣ 📜 hrd_etl.yml              # GitHub Actions: 내부 ETL (평일 매시간)
 ┃ ┗ 📜 market_etl.yml           # GitHub Actions: 시장 ETL (매일 저녁)
 ┣ 📂 pages
 ┃ ┣ 📜 1_📊_기수별_분석.py      # [내부] 기수별 심층 분석 6탭 + 전체 기수 비교 모드
 ┃ ┣ 📜 2_🚨_진행과정_관리.py    # [내부] 출석률 게이지, 출결추이, 누적 위험지표
 ┃ ┣ 📜 3_🔎_데이터_감사.py      # [공통] DB 원본 데이터 조회 4탭 (과정/훈련생/출결/시장동향)
 ┃ ┗ 📜 4_📈_시장_동향_분석.py    # [외부] 시장 분석 12개 탭 (교차분석, 시계열, 경쟁, 비용-성과, 자격증 등)
 ┣ 📜 home.py                   # 메인 대시보드 (KPI 요약 + 수료율 + 오늘의 출결 현황(입실중 재분류))
 ┣ 📜 hrd_etl.py                # [수집] 내부 과정/훈련생/출결 데이터
 ┣ 📜 market_etl.py             # [수집] 외부 시장 전체 데이터 (32만건)
 ┣ 📜 init_db.py                # [DB] 전체 테이블 초기화 및 마이그레이션
 ┣ 📜 utils.py                  # [공통] DB 연결, adapt_query, 유틸리티
 ┣ 📜 requirements.txt          # 의존성 라이브러리 목록
 ┗ 📜 CLAUDE.md                 # Claude Code 프로젝트 컨텍스트
```

---

## 🚀 설치 및 실행 방법

### 1. 환경 설정

```bash
git clone https://github.com/soulhn/2026data.git
cd 2026data

# 가상환경 생성 및 활성화
python -m venv .venv
# Windows: .venv\Scripts\activate
# Mac/Linux: source .venv/bin/activate

# 필수 라이브러리 설치
pip install -r requirements.txt
```

### 2. 환경 변수 설정 (`.env` 파일 생성)

```ini
HRD_API_KEY="발급받은_인증키"
HANWHA_COURSE_ID="관리할_내부_과정_ID"
DATABASE_URL="postgresql://..."   # Supabase 연결 (없으면 SQLite 사용)
```

### 3. 데이터베이스 구축

```bash
# 테이블 생성
python init_db.py

# 내부 과정 데이터 수집
python hrd_etl.py

# 시장 동향 데이터 수집 (20~30분 소요)
python market_etl.py
```

### 4. 대시보드 실행

```bash
streamlit run home.py
```

---

## ☁️ 배포 환경

### Streamlit Cloud

Secrets 설정:
```toml
DATABASE_URL = "postgresql://..."

[passwords]
admin = "비밀번호"
```

### GitHub Actions (ETL 자동화)

Repository Secrets에 등록:
- `HRD_API_KEY`
- `HANWHA_COURSE_ID`
- `DATABASE_URL`

| 워크플로우 | 스케줄 | 소요 시간 |
|---|---|---|
| HRD ETL | 평일 09:00~18:00 매시간 (KST) | ~10분 |
| Market ETL | 매일 21:00 (KST) | ~30분 |

---

## 📝 커밋 컨벤션

> **`태그: 작업 내용 요약`**

| 태그         | 설명                      | 사용 예시                                  |
| :----------- | :------------------------ | :----------------------------------------- |
| **Feat**     | 새로운 기능 추가          | `Feat: 중도탈락 위험군 탐지 로직 추가`     |
| **Fix**      | 버그 수정                 | `Fix: PG 플레이스홀더 변환 누락 수정`      |
| **Docs**     | 문서 수정                 | `Docs: README.md 배포 구조 업데이트`       |
| **Style**    | 코드 포맷팅 (로직 변경 X) | `Style: 불필요한 공백 제거`                |
| **Refactor** | 코드 리팩토링             | `Refactor: DB 연결 함수 utils.py로 분리`   |
| **Chore**    | 설정 변경, 패키지 관리 등 | `Chore: GitHub Actions 타임아웃 조정`      |

---

## 🛠️ 기술적 특징

- **DB 이중 지원:** `DATABASE_URL` 환경변수 유무로 PostgreSQL/SQLite 자동 전환
- **ETL 자동화:** GitHub Actions cron으로 무인 데이터 갱신
- **Smart Update:** 종료 과정 중복 수집 방지, 증분 수집 지원
- **Robustness:** 자동 재시도(Retry), 세션 관리로 대용량 수집 안정성 확보
- **Performance:** `@st.cache_data`, Pagination, Sampling으로 대시보드 최적화
- **Visualization:** Plotly & Altair 인터랙티브 차트 (히트맵, 히스토그램, 게이지 등)
- **시장 분석:** 내부 과정 vs 시장 교차분석, 시계열 트렌드, 경쟁 심화도, 비용-성과 시뮬레이터, 자격증 분석 (scikit-learn)
- **위험 관리:** 누적 출결 위험 지표 (결석 3회+, 지각 5회+, 조퇴 5회+), 출결 추이 모니터링

---

## 🔗 배포 URL

https://playdata.streamlit.app
