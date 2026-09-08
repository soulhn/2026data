# 과정별 취업 방향 및 사람인 수집 설계

작성일: 2026-09-08 · 상태: **구현 완료 (2026-09-08)** — 구현 세부는 `CLAUDE.md` 사람인 ETL 절, 튜닝 경과는 `docs/DEV_LOG.md`
원본: `data/curriculum/` 커리큘럼 3종 (git 제외 자료) · 근거 데이터: 운영 DB `TB_JOB_POSTING` 34,901건 (2026-09-08 조회)

> 구현 시 설계에서 달라진 점: ① 캐시 4종 계획 → 월별 추이·수집 현황 2종만 캐시하고 분포·목록은 페이지가 PG 직접 조회
> ② 코드 단독 매치에 "강한 코드 포함 4점 이상 + 제목 직무 단어" 조건 추가 (사람인 태그 오탐 대응)
> ③ `job_cd` 2개 AND 조합은 사용하지 않음(API 의미 미확인) → 단일 코드만 ④ 제외 규칙에 기획자·QA·라벨링·세일즈·교육생 추가

## 0. 과정 코드 정의

| 코드 | 과정 | 커리큘럼 직군/직무 (원문) | 총 시간 |
|---|---|---|---|
| **MLE** | 머신러닝캠프 — LLM 지식 그래프 기반 신뢰형 GraphRAG 구축 | AI 엔지니어 / 머신러닝 엔지니어 | 960h |
| **AIO** | 멀티에이전트 과정 — 멀티 에이전트 워크플로우 기반 AI 오케스트레이션 애플리케이션 개발자 | AI 애플리케이션 개발자 | 960h |
| **MLO** | AI Ready 데이터 — AI Ready Data 기반 Cloud-Native 자동화를 위한 MLOps 엔지니어 | AI 엔지니어 / MLOps 엔지니어 | 960h |

`config.COURSE_SHORT_NAMES`에 MLE·AIO는 이미 등록. MLO는 과정 ID 확정 시 추가.

---

## 1. 과정별 취업 방향

커리큘럼의 교과목 비중(시간)과 최종 프로젝트 주제를 기준으로 **주 방향(1순위)·보조 방향(2순위)**을 나눴다.
"신입 가능"은 사람인 `EXPERIENCE_CD ∈ {0 경력무관, 1 신입, 3 신입/경력}` 기준.

### MLE — LLM·RAG 엔지니어 방향

| 순위 | 직무 | 근거 (커리큘럼) |
|---|---|---|
| 1 | **LLM 엔지니어 / RAG 엔지니어** | 3교과목 296h가 GraphRAG·하이브리드 검색·Agentic RAG·RAGAS 평가. 최종 프로젝트 5팀 전부 GraphRAG 에이전트 |
| 1 | **AI 엔지니어 (LLM 애플리케이션)** | LangChain·LangGraph·Tool Use·MCP·Streamlit 배포 (1교과목 208h) |
| 2 | **NLP 엔지니어** | 임베딩·BERTopic·개체/관계 추출(NER·RE)·지식 그래프 |
| 2 | **머신러닝 엔지니어 (파인튜닝·서빙)** | SFT·LoRA·QLoRA·DPO·vLLM·양자화 (120h) — 단, 전통 ML(분류·회귀 모델링)은 거의 없음 |
| 3 | 데이터 분석가 | 통계·가설검정·회귀 80h가 있으나 부차적 |

핵심 스택: Python · LangChain · LangGraph · Neo4j/Cypher · 벡터DB(Chroma·Qdrant·pgvector) · OpenAI API · vLLM · PEFT · RAGAS · NeMo Guardrails · FastAPI · Docker · Langfuse

### AIO — AI 애플리케이션·에이전트 개발자 방향

| 순위 | 직무 | 근거 (커리큘럼) |
|---|---|---|
| 1 | **AI 애플리케이션 개발자 / AI 에이전트 개발자** | 단일·멀티 에이전트 오케스트레이션 312h, LangGraph·Function Calling·MCP |
| 1 | **Python 백엔드 개발자 (FastAPI)** | 1교과목 192h: FastAPI·Pydantic·비동기·Supabase·Redis·Auth |
| 2 | **챗봇 / 대화형 AI 서비스 개발자** | Streamlit 챗봇 UI·세션·대화 이력·스트리밍 |
| 2 | **AI 워크플로우 자동화 (n8n·로코드) / AI 솔루션 엔지니어** | 4교과목 152h 노코드 워크플로우, 기업형 Tech Support 자동화 |
| 3 | DevOps 초급 (배포·운영) | Docker·AWS·GitHub Actions·Auto Healing·CloudWatch (48h) |

핵심 스택: Python · FastAPI · Supabase/PostgreSQL · Redis · LangChain · LangGraph · LangSmith · MCP · Streamlit(React 선택) · n8n · Docker · AWS · GitHub Actions · Guardrails AI

### MLO — 데이터 엔지니어·MLOps 방향

| 순위 | 직무 | 근거 (커리큘럼) |
|---|---|---|
| 1 | **데이터 엔지니어 / 데이터 파이프라인 엔지니어** | 3교과목 248h: Spark·Kafka·S3 데이터 레이크·Delta Lake·CDC·MongoDB |
| 1 | **MLOps 엔지니어** | 4교과목 144h: MLflow·Airflow·FastAPI 서빙·Evidently 드리프트·CT 파이프라인 |
| 2 | **DevOps / 클라우드(플랫폼) 엔지니어** | Docker·Kubernetes(EKS)·Helm·Terraform·ArgoCD·Prometheus/Grafana |
| 2 | **데이터 모델러 / DA (데이터 표준화)** | 2교과목 80h: 표준 단어·용어·도메인, AI Ready Data, 엔코아 Meta# — 국내 SI·공공 특화 |
| 3 | DBA · SQL 개발 | SQLD 연계 이론·성능 튜닝 (자격증 방향) |

핵심 스택: Python · Linux · MySQL · MongoDB · Redis · Spark · Kafka · AWS(S3·EC2·EKS·ECR) · Delta Lake · Airflow · MLflow · FastAPI · Docker · Kubernetes · Helm · Terraform · scikit-learn

### 공통 방향 (3과정 모두 해당)

| 직무 | 근거 |
|---|---|
| **Python 개발자 / 소프트웨어 개발 (신입)** | 3과정 모두 Python·Git·Vibe Coding 기초 → FastAPI/Docker 배포까지 공통 |
| **AI(인공지능) 신입 · 생성형 AI 활용 개발** | 3과정 모두 LLM API·RAG 기초·프롬프트·Streamlit 서비스 구현 포함 (MLO는 최종 프로젝트 1팀 RAG) |
| **데이터 처리·분석 기초 직무** | pandas·SQL·EDA 공통 |

교집합 정리:
- MLE ∩ AIO: LangChain·LangGraph·RAG·에이전트·가드레일 → **AI 에이전트/LLM 앱 개발** 공고는 두 과정 모두 대상
- AIO ∩ MLO: FastAPI·Docker·AWS·GitHub Actions·Redis·PostgreSQL → **Python 백엔드·DevOps 초급**
- MLE ∩ MLO: MLflow·FastAPI 서빙·Docker → **ML 서빙** (약한 교집합)

---

## 2. 현재 수집 데이터로 판단 가능한가 — 아니오

`saramin_etl.py`는 키워드 20개(`config.SARAMIN_KEYWORDS`)로만 수집한다. 과정 특화 용어는 **검색 자체를 한 적이 없어** 코퍼스에 거의 없다.

| 제목·키워드 포함 건수 (전체 34,901건) | 건수 |
|---|---|
| LLM | 185 |
| 데이터 엔지니어 | 158 |
| 에이전트 | 78 |
| RAG | 47 |
| MLOps | 38 |
| Kafka | 6 |
| Airflow | 4 |
| FastAPI | 3 |
| LangChain | 2 |
| Spark · Neo4j · n8n · MLflow | 0 |

반면 사람인 **직무 키워드 코드(`JOB_CD`)** 는 이미 저장돼 있어 코드 기준 분류는 즉시 가능하다.

| `JOB_NM` 포함 건수 | 건수 |
|---|---|
| AI(인공지능) | 3,684 |
| 데이터엔지니어 | 1,733 |
| 클라우드 | 1,686 |
| 데이터분석가 | 1,264 |
| 머신러닝 | 1,187 |
| 딥러닝 | 1,114 |
| 빅데이터 | 1,024 |
| 데이터 사이언티스트 | 689 |
| DevOps | 575 |
| NLP(자연어처리) | 275 |
| 챗봇 | 238 |
| MLOps | 9 (코드표에 없음 — 키워드로만 잡힘) |

IT개발·데이터 직군(`JOB_MID_CD='2'`) 경력 구분: 경력 11,083 · 경력무관 3,077 · 신입/경력 1,455 · 신입 663 → **신입 지원 가능은 약 32%**.

결론: **① 기존 데이터에 코드 기반 분류를 먼저 적용해 규모를 본 뒤, ② 과정 특화 키워드 수집을 추가**하는 2단계가 맞다.

---

## 3. 사람인 API 검색 설계

### 3.1 API 제약 (docs/api/saramin.md)

- 파라미터: `keywords`(자유 텍스트), `job_mid_cd`(상위 직무), `job_cd`(직무·스킬 키워드 코드, 쉼표 다중), `job_type`, `edu_lv`, `loc_cd`, `published_min/max`
- **경력 필터 파라미터 없음** → 신입 가능 여부는 수집 후 `EXPERIENCE_CD`로 후처리
- 1회 최대 110건, 페이징 없음 → 기존처럼 `published_min/max` 1일 창으로 분할
- 일일 500회 (안전마진 480). 현재 사용 약 60~80회/일 → **여유 약 400회**
- `job_cd` 응답값은 직무명과 스킬명이 한 리스트(예: `84,86,87,92,181` = 백엔드,앱,웹,프론트,AI). `JOB_NM` 토큰 수가 `JOB_CD`보다 많은 건 스킬명이 이름에만 붙기 때문 — 분류는 **숫자 코드**로 한다

### 3.2 사람인 직무 코드표 (IT개발·데이터, `mcode=2`, 261행 중 발췌)

출처: https://oapi.saramin.co.kr/guide/code-table5?mcode=2 (2026-09-08 확인)

| 구분 | 코드: 이름 |
|---|---|
| 직무 | 83 데이터엔지니어 · 84 백엔드/서버개발 · 87 웹개발 · 92 프론트엔드 · 2232 풀스택 · 82 데이터분석가 · 2248 데이터 사이언티스트 · 2246 BI 엔지니어 · 107 데이터시각화 · 105 데이터라벨링 · 95 DBA · 127 인프라 · 146 DevOps · 136 클라우드 · 100 SE(시스템엔지니어) · 150 ETL · 122 알고리즘 |
| AI | 181 AI(인공지능) · 109 머신러닝 · 108 딥러닝 · 160 NLP(자연어처리) · 131 챗봇 · 133 컴퓨터비전 · 123 영상처리 · 116 빅데이터 |
| 스킬 | 272 Python · 235 Java · 291 Spring · 292 SpringBoot · 214 Docker · 244 Kubernetes · 201 AWS · 246 Linux · 241 Kafka · 289 Spark · 227 Hadoop · 217 ElasticStack · 254 MongoDB · 257 MySQL · 270 PostgreSQL · 280 Redis · 259 NoSQL · 293 SQL · 269 PL/SQL · 273 Pytorch · 300 Tensorflow · 142 API · 282 RestAPI |
| **코드 없음** | MLOps · LLM · RAG · LangChain · Airflow · MLflow · FastAPI · Neo4j · n8n · 에이전트 · 파인튜닝 · Terraform → **keywords 축으로만 수집** |

### 3.3 수집 축 2개 + 분류 1개

```
[축 A] job_cd 검색   — 좁은 직무·스킬 코드 (하루 110건 이내로 떨어지는 것만)
[축 B] keywords 검색 — 코드표에 없는 과정 특화 용어
[분류] 수집된 모든 공고(기존 3.5만 포함)에 규칙 기반 트랙 태깅 → TB_JOB_POSTING_TRACK
```

축 A·B는 "코퍼스를 넓히는" 역할이고, 어떤 공고가 어느 과정 것인지는 **분류 단계가 단독으로 결정**한다. 검색 키워드와 트랙을 1:1로 묶지 않는 이유는, 하나의 공고가 여러 과정에 해당하고(예: "LLM 백엔드 개발자" = MLE+AIO), 이미 수집된 3.5만 건도 같은 규칙으로 소급 태깅해야 하기 때문.

넓은 코드(84 백엔드, 87 웹개발, 181 AI, 272 Python)는 하루 110건을 넘겨 잘리므로 **단독 수집하지 않고 분류에만 사용**한다. 기존 20개 키워드 수집이 이 영역을 이미 덮고 있다.

### 3.4 트랙별 검색 목록 (제안)

| 트랙 | 축 A `job_cd` (단독 또는 2개 AND) | 축 B `keywords` |
|---|---|---|
| **MLE** | `160`(NLP) · `109,181`(머신러닝+AI) · `108`(딥러닝) · `131`(챗봇) | `LLM` · `RAG` · `LangChain` · `LLM 엔지니어` · `생성형 AI 개발` · `파인튜닝` · `벡터DB` · `GraphRAG` |
| **AIO** | `131`(챗봇) · `272,142`(Python+API) | `AI 에이전트` · `LangGraph` · `AI 서비스 개발` · `FastAPI` · `AI 애플리케이션` · `n8n` · `MCP` · `챗봇 개발` |
| **MLO** | `83`(데이터엔지니어) · `146`(DevOps) · `241`(Kafka) · `289`(Spark) · `244`(Kubernetes) · `150`(ETL) · `116`(빅데이터) | `MLOps` · `Airflow` · `데이터 파이프라인` · `데이터 플랫폼` · `MLflow` · `Terraform` · `데이터 표준화` · `데이터 모델러` |
| **공통** | (기존 20개 키워드 유지) | `AI 개발자 신입` · `Python 개발자 신입` |

호출 예산: 축 A 15개 + 축 B 26개 = 41개 검색 × 3일 창 = **123회/일**, 기존 60~80회와 합쳐 **약 200/480**. 6개월 뒤 히트율을 보고 0건 키워드는 정리.

### 3.5 분류 규칙 (TB_JOB_POSTING_TRACK)

점수 합이 임계값 이상이면 태깅. 한 공고가 여러 트랙에 붙을 수 있다.

| 트랙 | 강한 신호 (+3) | 보조 신호 (+1) | 임계 |
|---|---|---|---|
| MLE | 제목·키워드에 `LLM\|RAG\|LangChain\|GraphRAG\|파인튜닝\|NLP\|자연어` · 코드 `160` | 코드 `109·108·181·131` · 키워드 `벡터\|임베딩\|Pytorch\|Hugging` | ≥3 |
| AIO | 제목·키워드에 `에이전트\|Agent\|LangGraph\|FastAPI\|AI 서비스\|AI 애플리케이션\|챗봇\|n8n\|MCP` · 코드 `131` | 코드 `272+142` · `84+181` · 키워드 `Streamlit\|Redis\|Supabase\|생성형` | ≥3 |
| MLO | 코드 `83·146·241·289·244·150` · 제목·키워드에 `MLOps\|Airflow\|데이터 파이프라인\|데이터 플랫폼\|MLflow\|Terraform` | 코드 `116·136·201·214·254·270` · 키워드 `Docker\|Kubernetes\|Spark\|Kafka\|ETL\|데이터 표준` | ≥3 |
| 공통 | 코드 `272`(Python) 또는 `181`(AI) 이면서 `EXPERIENCE_CD ∈ {0,1,3}` | — | 조건 충족 |

제외 규칙: `JOB_MID_CD`에 `2`가 없는 공고, `JOB_TYPE_CD`가 5(아르바이트)·8(파견)·11(교육생)인 공고, 제목에 `강사\|교육\|헤드헌팅`.

```sql
CREATE TABLE IF NOT EXISTS TB_JOB_POSTING_TRACK (
    JOB_ID       TEXT NOT NULL,
    TRACK        TEXT NOT NULL,        -- 'MLE' | 'AIO' | 'MLO' | 'COMMON'
    SCORE        INTEGER,
    MATCH_SOURCE TEXT,                 -- 'code' | 'keyword' | 'both'
    ENTRY_LEVEL  INTEGER,              -- EXPERIENCE_CD ∈ {0,1,3}
    TAGGED_AT    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (JOB_ID, TRACK)
);
```

`TB_JOB_POSTING_KEYWORD`·`TB_JOB_POSTING_REGION`과 같은 junction 패턴. 보존 정책 삭제 시 함께 삭제 (고아 방지 테스트 추가).

### 3.6 캐시·화면

- 캐시 키 추가: `saramin_track_kpi` · `saramin_track_monthly` · `saramin_track_job_cd` · `saramin_track_company`
- `saramin_track_monthly`는 원본 30일 보존 뒤에도 추이가 남도록 **`merge_cumulative()` 누적 병합** (기존 3종과 동일 규칙)
- `채용_동향` 페이지에 "과정별 취업 방향" 탭: 트랙 선택 → 신입 가능 공고 수 / 전체 대비 비율 / 상위 직무 코드 / 상위 기업 / 지역 / 월별 추이. 전체·신입가능 토글

---

## 4. 구현 결과 (2026-09-08)

| 단계 | 작업 | 상태 |
|---|---|---|
| 1 | `config.SARAMIN_TRACK_RULES` + `tag_tracks()` + `TB_JOB_POSTING_TRACK` DDL + 기존 3.5만 건 소급 태깅 | 완료 — 오프라인 검증 4회 반복으로 규칙 튜닝 (DEV_LOG 표) |
| 2 | 수집 쿼리 42개(`config.SARAMIN_QUERIES`) — `collect_query()`가 `keywords`/`job_cd` 모두 지원 | 완료 — 168회/일 |
| 3 | 캐시 2종(`saramin_track_monthly` 누적 병합 · `saramin_query_hits`) + 보존 정책 TRACK 연동 | 완료 — 회귀 테스트 통과 |
| 4 | `pages/채용_동향.py` 전면 교체 (과정 선택 · 신입 가능 토글 · 5개 탭) | 완료 |

최종 분류 분포(진행중 기준, 2026-09-08): MLE 643 · AIO 359 · MLO 763 · COMMON 409. 1주 뒤 `수집 현황` 탭에서 0건 쿼리를 정리한다.

---

## 5. 열어 둔 결정

- **MLO 과정 ID**: `COURSE_SHORT_NAMES`에 추가 필요 (HRD-Net 과정 ID 확인)
- **트랙 이름 노출**: 화면에는 내부 코드(MLE·AIO·MLO) 대신 과정명 축약을 쓸지 — `GLOSSARY.md` 등록 필요
- **"신입 가능" 정의**: `경력무관`을 포함할지. 포함하면 32%, 신입·신입/경력만이면 13%. 초안은 포함
- **지역 제한**: 과정 소재지(서울) 기준으로 서울·경기 우선 정렬만 할지, 전국 유지할지. 초안은 전국 수집, 화면 필터
