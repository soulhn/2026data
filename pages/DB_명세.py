import json
import streamlit as st
import pandas as pd
import sys, os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils import check_password, load_data, is_pg, DB_FILE, get_connection, load_cache_json, page_error_boundary
from config import CACHE_TTL_DEFAULT, CacheKey


def _load_cached_dist(cache_key, columns):
    """TB_MARKET_CACHE에서 캐시된 분포 데이터를 조회. 없으면 None 반환."""
    try:
        df = load_data(
            "SELECT CACHE_DATA FROM TB_MARKET_CACHE WHERE CACHE_KEY = ?",
            params=[cache_key],
        )
        if not df.empty and df['CACHE_DATA'].iloc[0]:
            rows = json.loads(df['CACHE_DATA'].iloc[0])
            if rows:
                result = pd.DataFrame(rows)
                result.columns = [c.upper() for c in result.columns]
                rename_map = {c.upper(): c for c in columns}
                result = result.rename(columns=rename_map)
                return result
    except Exception:
        pass
    return None

st.set_page_config(page_title="DB 명세", page_icon="🗄️", layout="wide")
check_password()

with page_error_boundary():
    st.title("🗄️ DB 명세 & 데이터 현황")
    st.caption("테이블 구조, 컬럼별 채움률, 실제 데이터 분포를 확인합니다.")

    db_label = "PostgreSQL (Supabase)" if is_pg() else f"SQLite ({DB_FILE})"
    st.info(f"현재 연결: **{db_label}**")
    st.divider()


    # ==========================================
    # 스키마 정의 (컬럼명, 타입, 설명)
    # ==========================================
    SCHEMAS = {
        "TB_COURSE_MASTER": {
            "설명": "내부 훈련 과정 마스터. HRD-Net API에서 수집한 과정별 운영 정보.",
            "PK": "(TRPR_ID, TRPR_DEGR)",
            "columns": [
                ("TRPR_ID",         "TEXT",      "훈련과정 ID"),
                ("TRPR_DEGR",       "INTEGER",   "훈련 회차"),
                ("TRPR_NM",         "TEXT",      "과정명"),
                ("TR_STA_DT",       "TEXT",      "훈련 시작일"),
                ("TR_END_DT",       "TEXT",      "훈련 종료일"),
                ("TOT_FXNUM",       "INTEGER",   "정원"),
                ("TOT_PAR_MKS",     "INTEGER",   "수강인원"),
                ("TOT_TRP_CNT",     "INTEGER",   "수강신청인원"),
                ("FINI_CNT",        "INTEGER",   "수료인원"),
                ("TOT_TRCO",        "INTEGER",   "총 훈련비"),
                ("INST_INO",        "TEXT",      "훈련기관 관리번호"),
                ("EI_EMPL_RATE_3",  "TEXT",      "3개월 고용보험 취업률 (%) — 특수값: A=개설예정 B=진행중 C=미실시 D=수료자없음"),
                ("EI_EMPL_CNT_3",   "INTEGER",   "3개월 취업인원"),
                ("EI_EMPL_RATE_6",  "TEXT",      "6개월 고용보험 취업률 (%) — 특수값: A=개설예정 B=진행중 C=미실시 D=수료자없음"),
                ("EI_EMPL_CNT_6",   "INTEGER",   "6개월 취업인원"),
                ("HRD_EMPL_RATE_6", "TEXT",      "6개월 고용보험 미가입 취업률 (%) — 특수값: A=개설예정 B=진행중 C=미실시 D=수료자없음"),
                ("HRD_EMPL_CNT_6",  "INTEGER",   "6개월 미가입 취업인원"),
                ("COLLECTED_AT",    "TIMESTAMP", "수집 시각"),
            ],
        },
        "TB_TRAINEE_INFO": {
            "설명": "훈련생 정보. 과정별 수강생 명부 및 상태.",
            "PK": "(TRPR_ID, TRPR_DEGR, TRNEE_ID)",
            "columns": [
                ("TRPR_ID",      "TEXT",      "훈련과정 ID"),
                ("TRPR_DEGR",    "INTEGER",   "훈련 회차"),
                ("TRNEE_ID",     "TEXT",      "훈련생 코드"),
                ("TRNEE_NM",     "TEXT",      "훈련생 이름"),
                ("TRNEE_STATUS", "TEXT",      "훈련생 상태 (수강중/수료/제적 등)"),
                ("TRNEE_TYPE",   "TEXT",      "훈련생 유형 코드 — C0031=근로자원격 / C0055=실업자원격 등"),
                ("BIRTH_DATE",   "TEXT",      "생년월일 (YYYYMMDD)"),
                ("TOTAL_DAYS",   "INTEGER",   "총 훈련일수"),
                ("OFLHD_CNT",    "INTEGER",   "공가일수"),
                ("VCATN_CNT",    "INTEGER",   "휴가일수"),
                ("ABSENT_CNT",   "INTEGER",   "결석일수"),
                ("ATEND_CNT",    "INTEGER",   "출석일수"),
                ("COLLECTED_AT", "TIMESTAMP", "수집 시각"),
            ],
        },
        "TB_ATTENDANCE_LOG": {
            "설명": "출결 로그. 훈련생별 날짜별 출결 상세.",
            "PK": "UNIQUE(TRPR_ID, TRPR_DEGR, TRNEE_ID, ATEND_DT)",
            "columns": [
                ("TRPR_ID",         "TEXT",      "훈련과정 ID"),
                ("TRPR_DEGR",       "INTEGER",   "훈련 회차"),
                ("TRNEE_ID",        "TEXT",      "훈련생 코드"),
                ("ATEND_DT",        "TEXT",      "출결 날짜"),
                ("DAY_NM",          "TEXT",      "요일"),
                ("IN_TIME",         "TEXT",      "입실 시각"),
                ("OUT_TIME",        "TEXT",      "퇴실 시각"),
                ("ATEND_STATUS",    "TEXT",      "출결 상태 (출석/지각/결석/조퇴 등)"),
                ("ATEND_STATUS_CD", "TEXT",      "출결 상태 코드"),
                ("COLLECTED_AT",    "TIMESTAMP", "수집 시각"),
            ],
        },
        "TB_MARKET_TREND": {
            "설명": "시장 동향. HRD-Net API에서 수집한 전국 KDT 과정 정보 (30만건+).",
            "PK": "(TRPR_ID, TRPR_DEGR)",
            "columns": [
                ("TRPR_ID",          "TEXT",      "훈련과정 ID"),
                ("TRPR_DEGR",        "INTEGER",   "훈련 회차"),
                ("TRPR_NM",          "TEXT",      "과정명"),
                ("TRAINST_NM",       "TEXT",      "훈련기관명"),
                ("TR_STA_DT",        "TEXT",      "훈련 시작일 (YYYY-MM-DD)"),
                ("TR_END_DT",        "TEXT",      "훈련 종료일"),
                ("NCS_CD",           "TEXT",      "NCS 직종 코드"),
                ("TRNG_AREA_CD",     "TEXT",      "지역 코드 (중분류)"),
                ("TOT_FXNUM",        "INTEGER",   "정원"),
                ("TOT_TRCO",         "REAL",      "실제 훈련비 (원)"),
                ("COURSE_MAN",       "REAL",      "수강비 (원)"),
                ("REG_COURSE_MAN",   "INTEGER",   "수강신청인원"),
                ("EI_EMPL_RATE_3",   "REAL",      "3개월 고용보험 취업률 (%)"),
                ("EI_EMPL_RATE_6",   "REAL",      "6개월 고용보험 취업률 (%)"),
                ("EI_EMPL_CNT_3",    "INTEGER",   "3개월 취업인원"),
                ("EI_EMPL_CNT_3_GT10", "TEXT",    "3개월 취업 10인 미만 여부 (Y/N)"),
                ("STDG_SCOR",        "REAL",      "만족도 점수"),
                ("GRADE",            "TEXT",      "기관 등급"),
                ("CERTIFICATE",      "TEXT",      "연계 자격증 목록"),
                ("CONTENTS",         "TEXT",      "과정 내용"),
                ("ADDRESS",          "TEXT",      "훈련기관 주소"),
                ("TEL_NO",           "TEXT",      "전화번호"),
                ("INST_INO",         "TEXT",      "기관 코드"),
                ("TRAINST_CST_ID",   "TEXT",      "기관 ID"),
                ("TRAIN_TARGET",     "TEXT",      "훈련 유형 (K-디지털 트레이닝 등)"),
                ("TRAIN_TARGET_CD",  "TEXT",      "훈련 유형 코드 — C0031=근로자원격 / C0054=국가기간전략 / C0054G=기업맞춤형 / C0054S=일반고특화 / C0054Y=스마트혼합 / C0055=실업자원격 / C0055C=과정평가형 / C0061=내일배움카드 / C0104=K-디지털트레이닝 / C0105=K-디지털기초"),
                ("WKEND_SE",         "TEXT",      "주말/주중 구분"),
                ("TITLE_ICON",       "TEXT",      "아이콘"),
                ("TITLE_LINK",       "TEXT",      "과정 링크"),
                ("SUB_TITLE_LINK",   "TEXT",      "부제목 링크"),
                ("YEAR_MONTH",       "TEXT",      "개설 연월 파생 (YYYY-MM)"),
                ("REGION",           "TEXT",      "지역 파생 (ADDRESS 첫 단어)"),
                ("COLLECTED_AT",     "TIMESTAMP", "수집 시각"),
            ],
        },
        "TB_JOB_POSTING": {
            "설명": "채용공고. 사람인 API에서 수집한 채용공고 원본 데이터.",
            "PK": "JOB_ID (TEXT)",
            "columns": [
                ("JOB_ID",          "TEXT",      "사람인 공고 ID"),
                ("ACTIVE",          "INTEGER",   "공고 활성 상태 (1: 진행중, 0: 마감)"),
                ("COMPANY_NM",      "TEXT",      "기업명"),
                ("POSITION_TITLE",  "TEXT",      "공고 제목"),
                ("IND_CD",          "TEXT",      "업종코드"),
                ("IND_NM",          "TEXT",      "업종명"),
                ("JOB_MID_CD",      "TEXT",      "상위 직무코드"),
                ("JOB_MID_NM",      "TEXT",      "상위 직무명"),
                ("JOB_CD",          "TEXT",      "직무코드 (쉼표구분)"),
                ("JOB_NM",          "TEXT",      "직무명 (쉼표구분)"),
                ("LOC_CD",          "TEXT",      "지역코드"),
                ("LOC_NM",          "TEXT",      "지역명"),
                ("JOB_TYPE_CD",     "TEXT",      "근무형태코드"),
                ("JOB_TYPE_NM",     "TEXT",      "근무형태명"),
                ("EDU_LV_CD",       "TEXT",      "학력코드"),
                ("EDU_LV_NM",       "TEXT",      "학력 요건명"),
                ("EXPERIENCE_CD",   "TEXT",      "경력코드"),
                ("EXPERIENCE_MIN",  "INTEGER",   "최소 경력 (년)"),
                ("EXPERIENCE_MAX",  "INTEGER",   "최대 경력 (년)"),
                ("EXPERIENCE_NM",   "TEXT",      "경력 요건명"),
                ("SALARY_CD",       "TEXT",      "급여코드"),
                ("SALARY_NM",       "TEXT",      "급여 조건명"),
                ("CLOSE_TYPE_CD",   "TEXT",      "마감유형코드"),
                ("CLOSE_TYPE_NM",   "TEXT",      "마감유형명"),
                ("POSTING_DT",      "TEXT",      "게시일 (YYYY-MM-DD HH:MM:SS)"),
                ("EXPIRATION_DT",   "TEXT",      "마감일"),
                ("OPENING_DT",      "TEXT",      "접수 시작일"),
                ("MODIFICATION_DT", "TEXT",      "수정일"),
                ("KEYWORD",         "TEXT",      "공고 키워드 (API 원본, 쉼표구분)"),
                ("POSITION_URL",    "TEXT",      "공고 URL"),
                ("SEARCH_KEYWORD",  "TEXT",      "수집 시 사용된 검색 키워드 (마지막 매칭)"),
                ("YEAR_MONTH",      "TEXT",      "게시 연월 (YYYY-MM)"),
                ("REGION",          "TEXT",      "1차 지역명"),
                ("COLLECTED_AT",    "TIMESTAMP", "수집 시각"),
            ],
        },
        "TB_JOB_POSTING_KEYWORD": {
            "설명": "채용공고-키워드 매핑. 공고와 검색 키워드 간 다대다 관계.",
            "PK": "(JOB_ID, SEARCH_KEYWORD)",
            "columns": [
                ("JOB_ID",          "TEXT",      "사람인 공고 ID (FK → TB_JOB_POSTING)"),
                ("SEARCH_KEYWORD",  "TEXT",      "수집 시 사용된 검색 키워드"),
                ("COLLECTED_AT",    "TIMESTAMP", "수집 시각"),
            ],
        },
        "TB_MARKET_CACHE": {
            "설명": "집계 캐시. ETL 완료 후 시장/출결/훈련생 집계를 JSON으로 저장.",
            "PK": "CACHE_KEY (TEXT)",
            "columns": [
                ("CACHE_KEY",   "TEXT",      "집계 식별자"),
                ("CACHE_DATA",  "TEXT",      "집계 결과 JSON"),
                ("COMPUTED_AT", "TIMESTAMP", "마지막 계산 시각"),
            ],
        },
    }

    # 예시값 조회 대상 컬럼 (자유값/장문 제외)
    SAMPLE_COLS = {
        "TB_MARKET_TREND":   ["TRAIN_TARGET", "WKEND_SE", "GRADE", "NCS_CD", "REGION", "TRAIN_TARGET_CD"],
        "TB_TRAINEE_INFO":   ["TRNEE_STATUS", "TRNEE_TYPE"],
        "TB_ATTENDANCE_LOG": ["ATEND_STATUS", "ATEND_STATUS_CD", "DAY_NM"],
        "TB_COURSE_MASTER":  ["EI_EMPL_RATE_3", "EI_EMPL_RATE_6", "HRD_EMPL_RATE_6"],
        "TB_JOB_POSTING":    ["JOB_TYPE_NM", "EDU_LV_NM", "EXPERIENCE_NM", "SALARY_NM", "CLOSE_TYPE_NM", "SEARCH_KEYWORD", "REGION"],
        "TB_JOB_POSTING_KEYWORD": ["SEARCH_KEYWORD"],
        "TB_MARKET_CACHE":   ["CACHE_KEY"],
    }


    # ==========================================
    # 데이터 품질 로드 (테이블당 단일 쿼리)
    # ==========================================
    @st.cache_data(ttl=CACHE_TTL_DEFAULT)
    def load_fill_rates():
        """각 테이블의 컬럼별 채움률(%) 계산. 테이블당 쿼리 1개."""
        out = {}
        cached = load_cache_json(CacheKey.DB_FILL_RATES)
        if cached:
            out.update(cached)
        # 캐시에 없는 테이블은 직접 쿼리
        missing = [t for t in SCHEMAS if t not in out]
        if not missing:
            return out
        for tbl, info in ((t, SCHEMAS[t]) for t in missing):
            exprs = []
            for col, dtype, _ in info["columns"]:
                if dtype == "TEXT":
                    exprs.append(
                        f"ROUND(SUM(CASE WHEN {col} IS NOT NULL AND {col} != '' THEN 1.0 ELSE 0 END) * 100.0 / COUNT(*), 1) AS \"{col}\""
                    )
                else:
                    exprs.append(
                        f"ROUND(SUM(CASE WHEN {col} IS NOT NULL THEN 1.0 ELSE 0 END) * 100.0 / COUNT(*), 1) AS \"{col}\""
                    )
            sql = f"SELECT COUNT(*) AS _TOTAL, {', '.join(exprs)} FROM {tbl}"
            try:
                conn = get_connection()
                cur = conn.cursor()
                cur.execute(sql)
                row = cur.fetchone()
                desc = [d[0].upper() for d in cur.description]
                conn.close()
                if row:
                    total = int(row[0]) if row[0] else 0
                    rates = {desc[i]: (float(row[i]) if row[i] is not None else 0.0)
                             for i in range(1, len(desc))}
                    out[tbl] = {"_total": total, **rates}
            except Exception as e:
                out[tbl] = {"_error": str(e)}
        return out


    @st.cache_data(ttl=CACHE_TTL_DEFAULT)
    def load_sample_values():
        """지정된 카테고리 컬럼의 실제 고유값 목록 조회."""
        out = {}
        cached = load_cache_json(CacheKey.DB_SAMPLE_VALUES)
        if cached:
            out.update(cached)
        missing = {t: c for t, c in SAMPLE_COLS.items() if t not in out}
        if not missing:
            return out
        conn = get_connection()
        cur = conn.cursor()
        for tbl, cols in missing.items():
            out[tbl] = {}
            for col in cols:
                try:
                    cur.execute(
                        f"SELECT DISTINCT {col} FROM {tbl} "
                        f"WHERE {col} IS NOT NULL AND {col} != '' "
                        f"ORDER BY {col} LIMIT 10"
                    )
                    out[tbl][col] = [str(r[0]) for r in cur.fetchall()]
                except Exception:
                    out[tbl][col] = []
        conn.close()
        return out


    @st.cache_data(ttl=CACHE_TTL_DEFAULT)
    def load_db_counts():
        s = {}
        for tbl in SCHEMAS:
            try:
                df = load_data(f"SELECT COUNT(*) as CNT FROM {tbl}")
                s[tbl] = int(df["CNT"].iloc[0]) if not df.empty else 0
            except Exception:
                s[tbl] = None

        s["market_type"] = _load_cached_dist(CacheKey.DB_MARKET_TYPE, ['훈련유형', '건수'])
        if s["market_type"] is None:
            s["market_type"] = load_data("SELECT TRAIN_TARGET as 훈련유형, COUNT(*) as 건수 FROM TB_MARKET_TREND WHERE TRAIN_TARGET IS NOT NULL AND TRAIN_TARGET != '' GROUP BY TRAIN_TARGET ORDER BY 건수 DESC")

        s["market_region"] = _load_cached_dist(CacheKey.DB_MARKET_REGION, ['지역', '건수'])
        if s["market_region"] is None:
            s["market_region"] = load_data("SELECT REGION as 지역, COUNT(*) as 건수 FROM TB_MARKET_TREND WHERE REGION IS NOT NULL AND REGION != '' GROUP BY REGION ORDER BY 건수 DESC")

        s["market_year"] = _load_cached_dist(CacheKey.DB_MARKET_YEAR, ['연도', '건수'])
        if s["market_year"] is None:
            s["market_year"] = load_data("SELECT SUBSTR(YEAR_MONTH,1,4) as 연도, COUNT(*) as 건수 FROM TB_MARKET_TREND WHERE YEAR_MONTH IS NOT NULL GROUP BY SUBSTR(YEAR_MONTH,1,4) ORDER BY 연도")

        s["course_year"]   = load_data("SELECT SUBSTR(TR_STA_DT,1,4) as 연도, COUNT(*) as 기수 FROM TB_COURSE_MASTER GROUP BY SUBSTR(TR_STA_DT,1,4) ORDER BY 연도")

        s["attend_status"] = _load_cached_dist(CacheKey.DB_ATTEND_DIST, ['출결상태', '건수'])
        if s["attend_status"] is None:
            s["attend_status"] = load_data("SELECT ATEND_STATUS as 출결상태, COUNT(*) as 건수 FROM TB_ATTENDANCE_LOG GROUP BY ATEND_STATUS ORDER BY 건수 DESC")

        s["trainee_status"] = _load_cached_dist(CacheKey.DB_TRAINEE_DIST, ['훈련생상태', '건수'])
        if s["trainee_status"] is None:
            s["trainee_status"] = load_data("SELECT TRNEE_STATUS as 훈련생상태, COUNT(*) as 건수 FROM TB_TRAINEE_INFO GROUP BY TRNEE_STATUS ORDER BY 건수 DESC")
        s["job_region"] = load_data("SELECT REGION as 지역, COUNT(*) as 건수 FROM TB_JOB_POSTING WHERE REGION IS NOT NULL AND REGION != '' GROUP BY REGION ORDER BY 건수 DESC")
        s["job_keyword"] = load_data("SELECT SEARCH_KEYWORD as 키워드, COUNT(*) as 건수 FROM TB_JOB_POSTING_KEYWORD GROUP BY SEARCH_KEYWORD ORDER BY 건수 DESC")
        s["job_year_month"] = load_data("SELECT YEAR_MONTH as 연월, COUNT(*) as 건수 FROM TB_JOB_POSTING WHERE YEAR_MONTH IS NOT NULL GROUP BY YEAR_MONTH ORDER BY 연월")
        s["cache_items"]   = load_data("SELECT CACHE_KEY as 캐시키, COMPUTED_AT as 계산시각 FROM TB_MARKET_CACHE ORDER BY CACHE_KEY")
        df_last = load_data("SELECT MAX(COLLECTED_AT) AS LAST_AT FROM TB_COURSE_MASTER")
        if not df_last.empty and df_last["LAST_AT"].iloc[0]:
            from datetime import timedelta
            last_kst = pd.to_datetime(df_last["LAST_AT"].iloc[0]) + timedelta(hours=9)
            s["last_at"] = last_kst.strftime("%Y-%m-%d %H:%M") + " (KST)"
        else:
            s["last_at"] = "-"
        return s


    @st.cache_data(ttl=CACHE_TTL_DEFAULT)
    def load_table_preview(tbl_name):
        return load_data(f"SELECT * FROM {tbl_name} LIMIT 20")


    with st.spinner("DB 통계 로드 중..."):
        fill_rates  = load_fill_rates()
        sample_vals = load_sample_values()
        counts      = load_db_counts()

    # ── 수집 현황 KPI ──
    st.subheader("📡 수집 현황")
    k1, k2, k3, k4, k5, k6 = st.columns(6)
    k1.metric("수집된 과정", f"{counts.get('TB_COURSE_MASTER', 0):,}건")
    k2.metric("수집된 훈련생", f"{counts.get('TB_TRAINEE_INFO', 0):,}명")
    k3.metric("수집된 로그", f"{counts.get('TB_ATTENDANCE_LOG', 0):,}행")
    k4.metric("시장 동향", f"{counts.get('TB_MARKET_TREND', 0):,}건")
    k5.metric("채용공고", f"{counts.get('TB_JOB_POSTING', 0):,}건")
    k6.metric("최종 수집 시각", counts.get("last_at", "-"))
    st.divider()

    # ==========================================
    # 테이블 개요
    # ==========================================
    st.subheader("📊 테이블 개요")
    overview = []
    for tbl, info in SCHEMAS.items():
        cnt = counts.get(tbl)
        tbl_fill = fill_rates.get(tbl, {})
        col_total = len(info["columns"])
        # 채움률 80% 이상 컬럼 수
        good_cols = sum(1 for c, _, _ in info["columns"] if tbl_fill.get(c, 0) >= 80)
        overview.append({
            "테이블": tbl,
            "용도": info["설명"].split(".")[0],
            "PK": info["PK"],
            "레코드 수": f"{cnt:,}건" if cnt is not None else "오류",
            "컬럼 수": col_total,
            "채움 양호(≥80%)": f"{good_cols}/{col_total}",
        })
    st.dataframe(pd.DataFrame(overview), hide_index=True, use_container_width=True)
    st.divider()


    # ==========================================
    # 테이블별 상세
    # ==========================================
    def fill_indicator(pct):
        if pct >= 95:   return "🟢"
        elif pct >= 50: return "🟡"
        elif pct > 0:   return "🔴"
        else:           return "⚫"   # 완전 없음


    st.subheader("📋 테이블 상세")
    tabs = st.tabs(list(SCHEMAS.keys()))

    for tab, (tbl_name, info) in zip(tabs, SCHEMAS.items()):
        with tab:
            cnt = counts.get(tbl_name)
            cnt_str = f"{cnt:,}건" if cnt is not None else "오류"
            tbl_fill  = fill_rates.get(tbl_name, {})
            tbl_samp  = sample_vals.get(tbl_name, {})

            st.markdown(f"**{info['설명']}**")
            st.markdown(f"PK: `{info['PK']}` · 총 레코드: **{cnt_str}**")
            st.caption("🟢 ≥95%  🟡 50–94%  🔴 1–49%  ⚫ 0% (값 없음)")

            # 컬럼 상세 테이블
            rows = []
            for col, dtype, desc in info["columns"]:
                pct = tbl_fill.get(col)
                if pct is None:
                    indicator, pct_disp = "❓", None
                else:
                    indicator = fill_indicator(pct)
                    pct_disp  = pct

                # 예시값
                samp = tbl_samp.get(col, [])
                if samp:
                    samp_str = " / ".join(samp[:6])
                elif pct_disp == 0.0:
                    samp_str = "— API 미제공"
                else:
                    samp_str = ""

                rows.append({
                    " ":         indicator,
                    "컬럼명":    col,
                    "타입":      dtype,
                    "설명":      desc,
                    "채움률(%)": pct_disp,
                    "예시값":    samp_str,
                })

            schema_df = pd.DataFrame(rows)
            st.dataframe(
                schema_df,
                column_config={
                    " ":          st.column_config.TextColumn(" ", width="small"),
                    "채움률(%)":  st.column_config.ProgressColumn(
                        "채움률(%)", format="%.1f%%", min_value=0, max_value=100
                    ),
                    "예시값":     st.column_config.TextColumn("예시값", width="large"),
                },
                hide_index=True,
                use_container_width=True,
                height=min(80 + len(rows) * 35, 550),
            )

            # 데이터 미리보기
            st.divider()
            with st.expander("📋 데이터 미리보기 (최근 20건)"):
                preview_df = load_table_preview(tbl_name)
                if not preview_df.empty:
                    st.dataframe(preview_df, use_container_width=True, hide_index=True)
                else:
                    st.caption("데이터 없음")

            # 데이터 분포
            st.divider()
            st.markdown("**데이터 분포**")

            if tbl_name == "TB_MARKET_TREND":
                c1, c2, c3 = st.columns(3)
                with c1:
                    st.markdown("*훈련 유형별*")
                    df = counts.get("market_type", pd.DataFrame())
                    if not df.empty: st.dataframe(df, hide_index=True, use_container_width=True)
                with c2:
                    st.markdown("*연도별 개설 수*")
                    df = counts.get("market_year", pd.DataFrame())
                    if not df.empty: st.dataframe(df, hide_index=True, use_container_width=True)
                with c3:
                    st.markdown("*지역별*")
                    df = counts.get("market_region", pd.DataFrame())
                    if not df.empty: st.dataframe(df, hide_index=True, use_container_width=True)

            elif tbl_name == "TB_COURSE_MASTER":
                df = counts.get("course_year", pd.DataFrame())
                st.markdown("*연도별 기수*")
                if not df.empty: st.dataframe(df, hide_index=True, use_container_width=True)
                else: st.caption("데이터 없음")

            elif tbl_name == "TB_ATTENDANCE_LOG":
                df = counts.get("attend_status", pd.DataFrame())
                st.markdown("*출결 상태별*")
                if not df.empty: st.dataframe(df, hide_index=True, use_container_width=True)
                else: st.caption("데이터 없음")

            elif tbl_name == "TB_TRAINEE_INFO":
                df = counts.get("trainee_status", pd.DataFrame())
                st.markdown("*훈련생 상태별*")
                if not df.empty: st.dataframe(df, hide_index=True, use_container_width=True)
                else: st.caption("데이터 없음")

            elif tbl_name == "TB_JOB_POSTING":
                c1, c2 = st.columns(2)
                with c1:
                    st.markdown("*지역별*")
                    df = counts.get("job_region", pd.DataFrame())
                    if not df.empty: st.dataframe(df, hide_index=True, use_container_width=True)
                with c2:
                    st.markdown("*월별 수집 건수*")
                    df = counts.get("job_year_month", pd.DataFrame())
                    if not df.empty: st.dataframe(df, hide_index=True, use_container_width=True)

            elif tbl_name == "TB_JOB_POSTING_KEYWORD":
                st.markdown("*키워드별 매핑 건수*")
                df = counts.get("job_keyword", pd.DataFrame())
                if not df.empty: st.dataframe(df, hide_index=True, use_container_width=True)
                else: st.caption("데이터 없음")

            elif tbl_name == "TB_MARKET_CACHE":
                df = counts.get("cache_items", pd.DataFrame())
                st.markdown("*캐시 항목 목록*")
                if not df.empty: st.dataframe(df, hide_index=True, use_container_width=True)
                else: st.warning("캐시가 비어 있습니다. market_etl.py를 실행하세요.")

