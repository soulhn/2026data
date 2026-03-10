import re
import time
import streamlit as st
import pandas as pd
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils import check_password, load_data, is_pg, DB_FILE, page_error_boundary

st.set_page_config(page_title="SQL Playground", page_icon="🔍", layout="wide")
check_password()

# ── 테이블 스키마 참조 (DB_명세.py SCHEMAS 기반) ──
SCHEMAS = {
    "TB_COURSE_MASTER": [
        ("TRPR_ID", "TEXT", "훈련과정 ID"),
        ("TRPR_DEGR", "INTEGER", "훈련 회차"),
        ("TRPR_NM", "TEXT", "과정명"),
        ("TR_STA_DT", "TEXT", "훈련 시작일"),
        ("TR_END_DT", "TEXT", "훈련 종료일"),
        ("TOT_FXNUM", "INTEGER", "정원"),
        ("TOT_PAR_MKS", "INTEGER", "수강인원"),
        ("TOT_TRP_CNT", "INTEGER", "수강신청인원"),
        ("FINI_CNT", "INTEGER", "수료인원"),
        ("EI_EMPL_RATE_3", "TEXT", "3개월 취업률"),
        ("EI_EMPL_RATE_6", "TEXT", "6개월 취업률"),
        ("HRD_EMPL_RATE_6", "TEXT", "6개월 HRD 취업률"),
    ],
    "TB_TRAINEE_INFO": [
        ("TRPR_ID", "TEXT", "훈련과정 ID"),
        ("TRPR_DEGR", "INTEGER", "훈련 회차"),
        ("TRNEE_ID", "TEXT", "훈련생 코드"),
        ("TRNEE_NM", "TEXT", "훈련생 이름"),
        ("TRNEE_STATUS", "TEXT", "상태 (수강중/수료/제적 등)"),
        ("TOTAL_DAYS", "INTEGER", "총 훈련일수"),
        ("ATEND_CNT", "INTEGER", "출석일수"),
        ("ABSENT_CNT", "INTEGER", "결석일수"),
    ],
    "TB_ATTENDANCE_LOG": [
        ("TRPR_ID", "TEXT", "훈련과정 ID"),
        ("TRPR_DEGR", "INTEGER", "훈련 회차"),
        ("TRNEE_ID", "TEXT", "훈련생 코드"),
        ("ATEND_DT", "TEXT", "출결 날짜"),
        ("DAY_NM", "TEXT", "요일"),
        ("IN_TIME", "TEXT", "입실 시각"),
        ("OUT_TIME", "TEXT", "퇴실 시각"),
        ("ATEND_STATUS", "TEXT", "출결 상태"),
    ],
    "TB_MARKET_TREND": [
        ("TRPR_ID", "TEXT", "훈련과정 ID"),
        ("TRPR_DEGR", "INTEGER", "훈련 회차"),
        ("TRPR_NM", "TEXT", "과정명"),
        ("TRAINST_NM", "TEXT", "훈련기관명"),
        ("TR_STA_DT", "TEXT", "시작일 (YYYY-MM-DD)"),
        ("REGION", "TEXT", "지역"),
        ("TOT_FXNUM", "INTEGER", "정원"),
        ("REG_COURSE_MAN", "INTEGER", "수강신청인원"),
        ("STDG_SCOR", "REAL", "만족도 점수"),
        ("TRAIN_TARGET", "TEXT", "훈련 유형"),
        ("YEAR_MONTH", "TEXT", "개설 연월"),
    ],
    "TB_MARKET_CACHE": [
        ("CACHE_KEY", "TEXT", "집계 식별자"),
        ("CACHE_DATA", "TEXT", "집계 결과 JSON"),
        ("COMPUTED_AT", "TIMESTAMP", "계산 시각"),
    ],
}

# ── 안전장치: SELECT만 허용 ──
_FORBIDDEN = re.compile(
    r"\b(INSERT|UPDATE|DELETE|DROP|ALTER|CREATE|TRUNCATE|REPLACE|MERGE|GRANT|REVOKE)\b",
    re.IGNORECASE,
)


def _validate_query(sql: str) -> str | None:
    """위험 키워드가 있으면 에러 메시지 반환, 없으면 None."""
    stripped = sql.strip().rstrip(";")
    if not stripped:
        return "쿼리를 입력해주세요."
    m = _FORBIDDEN.search(stripped)
    if m:
        return f"**{m.group().upper()}** 문은 허용되지 않습니다. SELECT만 사용할 수 있습니다."
    return None


def _ensure_limit(sql: str, max_rows: int = 100) -> str:
    """LIMIT 절이 없으면 자동 추가."""
    stripped = sql.strip().rstrip(";")
    if not re.search(r"\bLIMIT\b", stripped, re.IGNORECASE):
        return f"{stripped} LIMIT {max_rows}"
    return stripped


# ── 예제 쿼리 ──
EXAMPLES = {
    "초급": [
        ("전체 과정 목록 조회",
         "SELECT TRPR_ID, TRPR_DEGR, TRPR_NM, TR_STA_DT, TR_END_DT\n"
         "FROM TB_COURSE_MASTER\n"
         "ORDER BY TR_STA_DT DESC"),
        ("과정별 수강인원 카운트",
         "SELECT TRPR_DEGR, TRPR_NM, TOT_PAR_MKS\n"
         "FROM TB_COURSE_MASTER\n"
         "ORDER BY TOT_PAR_MKS DESC"),
        ("출석 상태별 건수",
         "SELECT ATEND_STATUS, COUNT(*) AS cnt\n"
         "FROM TB_ATTENDANCE_LOG\n"
         "GROUP BY ATEND_STATUS\n"
         "ORDER BY cnt DESC"),
    ],
    "중급": [
        ("과정별 수료율 (JOIN + GROUP BY)",
         "SELECT c.TRPR_DEGR, c.TRPR_NM,\n"
         "       COUNT(*) AS total_cnt,\n"
         "       SUM(CASE WHEN t.TRNEE_STATUS LIKE '%수료%'\n"
         "                  OR t.TRNEE_STATUS LIKE '%조기취업%'\n"
         "            THEN 1 ELSE 0 END) AS fini_cnt,\n"
         "       ROUND(CAST(SUM(CASE WHEN t.TRNEE_STATUS LIKE '%수료%'\n"
         "                       OR t.TRNEE_STATUS LIKE '%조기취업%'\n"
         "                  THEN 1.0 ELSE 0 END) * 100.0\n"
         "             / COUNT(1) AS numeric), 1) AS completion_rate\n"
         "FROM TB_COURSE_MASTER c\n"
         "JOIN TB_TRAINEE_INFO t\n"
         "  ON c.TRPR_ID = t.TRPR_ID AND c.TRPR_DEGR = t.TRPR_DEGR\n"
         "GROUP BY c.TRPR_DEGR, c.TRPR_NM\n"
         "ORDER BY c.TRPR_DEGR"),
        ("월별 시장 과정 개설 수",
         "SELECT YEAR_MONTH, COUNT(*) AS cnt\n"
         "FROM TB_MARKET_TREND\n"
         "WHERE YEAR_MONTH IS NOT NULL\n"
         "GROUP BY YEAR_MONTH\n"
         "ORDER BY YEAR_MONTH"),
        ("지역별 평균 모집률",
         "SELECT REGION,\n"
         "       COUNT(1) AS course_cnt,\n"
         "       ROUND(CAST(AVG(\n"
         "         CASE WHEN TOT_FXNUM > 0\n"
         "              THEN MIN(REG_COURSE_MAN * 100.0 / TOT_FXNUM, 100)\n"
         "              ELSE NULL END) AS numeric), 1) AS avg_recruit_rate\n"
         "FROM TB_MARKET_TREND\n"
         "WHERE REGION IS NOT NULL AND REGION != ''\n"
         "GROUP BY REGION\n"
         "HAVING COUNT(1) >= 10\n"
         "ORDER BY avg_recruit_rate DESC"),
    ],
    "고급": [
        ("과정별 출석률 (NOT_ATTEND 제외)",
         "SELECT a.TRPR_DEGR,\n"
         "       COUNT(1) AS total_records,\n"
         "       SUM(CASE WHEN a.ATEND_STATUS NOT IN\n"
         "           ('결석','중도탈락미출석','100분의50미만출석')\n"
         "           THEN 1 ELSE 0 END) AS attend_cnt,\n"
         "       ROUND(CAST(\n"
         "         SUM(CASE WHEN a.ATEND_STATUS NOT IN\n"
         "             ('결석','중도탈락미출석','100분의50미만출석')\n"
         "             THEN 1.0 ELSE 0 END) * 100.0\n"
         "         / NULLIF(SUM(CASE WHEN a.ATEND_STATUS != '중도탈락미출석'\n"
         "                      THEN 1 ELSE 0 END), 0) AS numeric), 1) AS att_rate\n"
         "FROM TB_ATTENDANCE_LOG a\n"
         "GROUP BY a.TRPR_DEGR\n"
         "ORDER BY a.TRPR_DEGR"),
        ("윈도우 함수: 과정별 수강인원 순위",
         "SELECT TRPR_DEGR, TRPR_NM, TOT_PAR_MKS,\n"
         "       RANK() OVER (ORDER BY TOT_PAR_MKS DESC) AS rank_by_par\n"
         "FROM TB_COURSE_MASTER\n"
         "ORDER BY rank_by_par"),
        ("CTE: 기관별 과정 수 & 평균 만족도",
         "WITH inst_stats AS (\n"
         "  SELECT TRAINST_NM,\n"
         "         COUNT(1) AS course_cnt,\n"
         "         ROUND(CAST(AVG(STDG_SCOR) AS numeric), 2) AS avg_score\n"
         "  FROM TB_MARKET_TREND\n"
         "  WHERE TRAINST_NM IS NOT NULL AND STDG_SCOR IS NOT NULL\n"
         "  GROUP BY TRAINST_NM\n"
         "  HAVING COUNT(1) >= 5\n"
         ")\n"
         "SELECT * FROM inst_stats\n"
         "ORDER BY avg_score DESC"),
    ],
}


with page_error_boundary():
    st.title("🔍 SQL Playground")
    st.caption("실제 HRD-Net 데이터로 SQL 쿼리를 직접 실행해 볼 수 있습니다.")

    db_label = "PostgreSQL (Supabase)" if is_pg() else f"SQLite ({DB_FILE})"
    st.info(f"현재 연결: **{db_label}** · SELECT 전용 (읽기만 가능) · LIMIT 미지정 시 최대 100행")
    st.divider()

    # ── 테이블 구조 참조 ──
    st.subheader("📋 테이블 구조 참조")
    for tbl_name, cols in SCHEMAS.items():
        with st.expander(tbl_name):
            df_schema = pd.DataFrame(cols, columns=["컬럼명", "타입", "설명"])
            st.dataframe(df_schema, hide_index=True, use_container_width=True)
    st.divider()

    # ── 예제 쿼리 버튼 ──
    st.subheader("📝 예제 쿼리")
    for level, queries in EXAMPLES.items():
        st.markdown(f"**{level}**")
        cols = st.columns(len(queries))
        for col, (label, sql) in zip(cols, queries):
            with col:
                if st.button(label, use_container_width=True, key=f"ex_{level}_{label}"):
                    st.session_state["sql_input"] = sql
    st.divider()

    # ── SQL 입력 ──
    st.subheader("🖊️ SQL 쿼리 입력")
    sql_input = st.text_area(
        "SQL 쿼리",
        value=st.session_state.get("sql_input", ""),
        height=150,
        placeholder="SELECT * FROM TB_COURSE_MASTER LIMIT 10",
        label_visibility="collapsed",
    )

    run_btn = st.button("▶ 실행", type="primary")

    # ── 실행 ──
    if run_btn and sql_input:
        err = _validate_query(sql_input)
        if err:
            st.error(err)
        else:
            final_sql = _ensure_limit(sql_input)
            if final_sql != sql_input.strip().rstrip(";"):
                st.caption("ℹ️ LIMIT 100이 자동으로 추가되었습니다.")
            try:
                t0 = time.time()
                result_df = load_data(final_sql)
                elapsed = time.time() - t0
                st.success(f"결과: **{len(result_df):,}행** · 실행 시간: **{elapsed:.2f}초**")
                st.dataframe(result_df, use_container_width=True, hide_index=True)
            except Exception as e:
                st.error(f"쿼리 실행 오류: {e}")
    elif run_btn:
        st.warning("쿼리를 입력해주세요.")
