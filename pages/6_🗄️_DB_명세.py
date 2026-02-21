import streamlit as st
import pandas as pd
import sys, os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils import check_password, load_data, is_pg, DB_FILE
from config import CACHE_TTL_DEFAULT

st.set_page_config(page_title="DB 명세", page_icon="🗄️", layout="wide")
check_password()

st.title("🗄️ DB 명세 & 데이터 현황")
st.caption("테이블 구조 및 실제 데이터 분포를 확인합니다.")

db_label = "PostgreSQL (Supabase)" if is_pg() else f"SQLite ({DB_FILE})"
st.info(f"현재 연결: **{db_label}**")
st.divider()


# ==========================================
# 스키마 정의 (하드코딩)
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
            ("EI_EMPL_RATE_3",  "TEXT",      "3개월 고용보험 취업률 (%)"),
            ("EI_EMPL_CNT_3",   "INTEGER",   "3개월 취업인원"),
            ("EI_EMPL_RATE_6",  "TEXT",      "6개월 고용보험 취업률 (%)"),
            ("EI_EMPL_CNT_6",   "INTEGER",   "6개월 취업인원"),
            ("HRD_EMPL_RATE_6", "TEXT",      "6개월 고용보험 미가입 취업률 (%)"),
            ("HRD_EMPL_CNT_6",  "INTEGER",   "6개월 미가입 취업인원"),
            ("REAL_EMPL_RATE",  "REAL",      "실질 취업률 (계산값)"),
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
            ("TRNEE_TYPE",   "TEXT",      "훈련생 유형"),
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
        "설명": "출결 로그. 훈련생별 날짜별 출결 상세. UNIQUE(과정ID, 회차, 훈련생ID, 날짜).",
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
            ("TRPR_ID",          "TEXT",    "훈련과정 ID"),
            ("TRPR_DEGR",        "INTEGER", "훈련 회차"),
            ("TRPR_NM",          "TEXT",    "과정명"),
            ("TRAINST_NM",       "TEXT",    "훈련기관명"),
            ("TR_STA_DT",        "TEXT",    "훈련 시작일 (YYYY-MM-DD)"),
            ("TR_END_DT",        "TEXT",    "훈련 종료일"),
            ("NCS_CD",           "TEXT",    "NCS 직종 코드"),
            ("TRNG_AREA_CD",     "TEXT",    "지역 코드 (중분류)"),
            ("TOT_FXNUM",        "INTEGER", "정원"),
            ("TOT_TRCO",         "REAL",    "실제 훈련비 (원)"),
            ("COURSE_MAN",       "REAL",    "수강비 (원)"),
            ("REG_COURSE_MAN",   "INTEGER", "수강신청인원"),
            ("EI_EMPL_RATE_3",   "REAL",    "3개월 고용보험 취업률 (%)"),
            ("EI_EMPL_RATE_6",   "REAL",    "6개월 고용보험 취업률 (%)"),
            ("EI_EMPL_CNT_3",    "INTEGER", "3개월 취업인원"),
            ("STDG_SCOR",        "REAL",    "만족도 점수"),
            ("GRADE",            "TEXT",    "기관 등급 (현재 API 미제공)"),
            ("CERTIFICATE",      "TEXT",    "연계 자격증 목록"),
            ("ADDRESS",          "TEXT",    "훈련기관 주소"),
            ("TRAIN_TARGET",     "TEXT",    "훈련 유형 (K-디지털 트레이닝 등)"),
            ("WKEND_SE",         "TEXT",    "주말/주중 구분"),
            ("YEAR_MONTH",       "TEXT",    "개설 연월 파생 (YYYY-MM)"),
            ("REGION",           "TEXT",    "지역 파생 (ADDRESS 첫 단어)"),
            ("COLLECTED_AT",     "TIMESTAMP", "수집 시각"),
        ],
    },
    "TB_MARKET_CACHE": {
        "설명": "시장 집계 캐시. market_etl.py 완료 후 10개 집계를 JSON으로 저장. 시장 분析 페이지 무필터 조회 시 즉시 반환.",
        "PK": "CACHE_KEY (TEXT)",
        "columns": [
            ("CACHE_KEY",    "TEXT",      "집계 식별자 (kpi / monthly_counts / region_counts 등)"),
            ("CACHE_DATA",   "TEXT",      "집계 결과 JSON (DataFrame rows)"),
            ("COMPUTED_AT",  "TIMESTAMP", "마지막 계산 시각"),
        ],
    },
}


# ==========================================
# 데이터 통계 로드
# ==========================================
@st.cache_data(ttl=CACHE_TTL_DEFAULT)
def load_db_stats():
    s = {}

    # 테이블별 건수
    for tbl in SCHEMAS:
        try:
            df = load_data(f"SELECT COUNT(*) as CNT FROM {tbl}")
            s[f"cnt_{tbl}"] = int(df["CNT"].iloc[0]) if not df.empty else 0
        except Exception:
            s[f"cnt_{tbl}"] = None

    # TB_MARKET_TREND 분포
    s["market_type"] = load_data("""
        SELECT TRAIN_TARGET as 훈련유형, COUNT(*) as 건수
        FROM TB_MARKET_TREND
        WHERE TRAIN_TARGET IS NOT NULL AND TRAIN_TARGET != ''
        GROUP BY TRAIN_TARGET ORDER BY 건수 DESC
    """)
    s["market_region"] = load_data("""
        SELECT REGION as 지역, COUNT(*) as 건수
        FROM TB_MARKET_TREND
        WHERE REGION IS NOT NULL AND REGION != ''
        GROUP BY REGION ORDER BY 건수 DESC LIMIT 15
    """)
    s["market_year"] = load_data("""
        SELECT SUBSTR(YEAR_MONTH, 1, 4) as 연도, COUNT(*) as 건수
        FROM TB_MARKET_TREND
        WHERE YEAR_MONTH IS NOT NULL
        GROUP BY SUBSTR(YEAR_MONTH, 1, 4) ORDER BY 연도
    """)

    # TB_COURSE_MASTER 분포
    s["course_year"] = load_data("""
        SELECT SUBSTR(TR_STA_DT, 1, 4) as 연도, COUNT(*) as 기수
        FROM TB_COURSE_MASTER
        GROUP BY SUBSTR(TR_STA_DT, 1, 4) ORDER BY 연도
    """)

    # TB_ATTENDANCE_LOG 출결 상태별
    s["attend_status"] = load_data("""
        SELECT ATEND_STATUS as 출결상태, COUNT(*) as 건수
        FROM TB_ATTENDANCE_LOG
        GROUP BY ATEND_STATUS ORDER BY 건수 DESC
    """)

    # TB_TRAINEE_INFO 훈련생 상태별
    s["trainee_status"] = load_data("""
        SELECT TRNEE_STATUS as 훈련생상태, COUNT(*) as 건수
        FROM TB_TRAINEE_INFO
        GROUP BY TRNEE_STATUS ORDER BY 건수 DESC
    """)

    # TB_MARKET_CACHE 항목
    s["cache_items"] = load_data("""
        SELECT CACHE_KEY as 캐시키, COMPUTED_AT as 계산시각
        FROM TB_MARKET_CACHE ORDER BY CACHE_KEY
    """)

    return s


with st.spinner("DB 통계 로드 중..."):
    stats = load_db_stats()


# ==========================================
# 테이블 개요 요약
# ==========================================
st.subheader("📊 테이블 개요")
overview_rows = []
for tbl, info in SCHEMAS.items():
    cnt = stats.get(f"cnt_{tbl}")
    cnt_str = f"{cnt:,}건" if cnt is not None else "오류"
    overview_rows.append({"테이블": tbl, "용도": info["설명"].split(".")[0], "PK": info["PK"], "데이터 건수": cnt_str})

st.dataframe(pd.DataFrame(overview_rows), hide_index=True, use_container_width=True)
st.divider()


# ==========================================
# 테이블별 상세
# ==========================================
st.subheader("📋 테이블 상세")
tabs = st.tabs(list(SCHEMAS.keys()))

for tab, (tbl_name, info) in zip(tabs, SCHEMAS.items()):
    with tab:
        cnt = stats.get(f"cnt_{tbl_name}")
        cnt_str = f"{cnt:,}건" if cnt is not None else "조회 오류"

        col_info, col_stat = st.columns([1, 1])

        with col_info:
            st.markdown(f"**설명**: {info['설명']}")
            st.markdown(f"**PK**: `{info['PK']}`")
            st.metric("총 레코드", cnt_str)
            st.markdown("**컬럼 목록**")
            schema_df = pd.DataFrame(info["columns"], columns=["컬럼명", "타입", "설명"])
            st.dataframe(schema_df, hide_index=True, use_container_width=True, height=350)

        with col_stat:
            st.markdown("**데이터 분포**")

            if tbl_name == "TB_MARKET_TREND":
                st.markdown("*훈련 유형별*")
                df_type = stats.get("market_type", pd.DataFrame())
                if not df_type.empty:
                    st.dataframe(df_type, hide_index=True, use_container_width=True)

                st.markdown("*연도별 개설 수*")
                df_year = stats.get("market_year", pd.DataFrame())
                if not df_year.empty:
                    st.dataframe(df_year, hide_index=True, use_container_width=True)

                st.markdown("*지역별 (상위 15)*")
                df_reg = stats.get("market_region", pd.DataFrame())
                if not df_reg.empty:
                    st.dataframe(df_reg, hide_index=True, use_container_width=True)

            elif tbl_name == "TB_COURSE_MASTER":
                st.markdown("*연도별 기수*")
                df_cy = stats.get("course_year", pd.DataFrame())
                if not df_cy.empty:
                    st.dataframe(df_cy, hide_index=True, use_container_width=True)
                else:
                    st.caption("데이터 없음")

            elif tbl_name == "TB_ATTENDANCE_LOG":
                st.markdown("*출결 상태별*")
                df_as = stats.get("attend_status", pd.DataFrame())
                if not df_as.empty:
                    st.dataframe(df_as, hide_index=True, use_container_width=True)
                else:
                    st.caption("데이터 없음")

            elif tbl_name == "TB_TRAINEE_INFO":
                st.markdown("*훈련생 상태별*")
                df_ts = stats.get("trainee_status", pd.DataFrame())
                if not df_ts.empty:
                    st.dataframe(df_ts, hide_index=True, use_container_width=True)
                else:
                    st.caption("데이터 없음")

            elif tbl_name == "TB_MARKET_CACHE":
                st.markdown("*캐시 항목 목록*")
                df_ci = stats.get("cache_items", pd.DataFrame())
                if not df_ci.empty:
                    st.dataframe(df_ci, hide_index=True, use_container_width=True)
                else:
                    st.warning("캐시가 비어 있습니다. market_etl.py를 실행하세요.")
