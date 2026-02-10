import streamlit as st
import pandas as pd
import sys
import os
from datetime import datetime, timedelta

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
try:
    from hrd_etl import run_etl
    from utils import DB_FILE, get_connection as _utils_get_connection, load_data as _load_data, safe_float, check_password, is_pg
except ImportError:
    def run_etl(): st.error("❌ 'hrd_etl.py'를 찾을 수 없습니다.")
    DB_FILE = "hrd_analysis.db"

st.set_page_config(page_title="HRD 데이터 감사 시스템", page_icon="🔎", layout="wide")
check_password()

COLUMN_MAP = {
    "TRPR_ID": "TRPR_ID (과정ID)", "TRPR_DEGR": "TRPR_DEGR (회차)", "TRPR_NM": "TRPR_NM (과정명)",
    "TR_STA_DT": "TR_STA_DT (시작일)", "TR_END_DT": "TR_END_DT (종료일)", "TOT_TRCO": "TOT_TRCO (총훈련비)",
    "FINI_CNT": "FINI_CNT (수료인원)", "TOT_FXNUM": "TOT_FXNUM (정원)", "TOT_PAR_MKS": "TOT_PAR_MKS (현원)",
    "TOT_TRP_CNT": "TOT_TRP_CNT (신청인원)", "INST_INO": "INST_INO (기관ID)",
    "EI_EMPL_RATE_3": "EI_EMPL_RATE_3 (3개월 취업률)", "EI_EMPL_CNT_3": "EI_EMPL_CNT_3 (3개월 취업자수)",
    "EI_EMPL_RATE_6": "EI_EMPL_RATE_6 (6개월 취업률)", "EI_EMPL_CNT_6": "EI_EMPL_CNT_6 (6개월 취업자수)",
    "HRD_EMPL_RATE_6": "HRD_EMPL_RATE_6 (6개월 HRD취업률)", "HRD_EMPL_CNT_6": "HRD_EMPL_CNT_6 (6개월 HRD취업자수)",
    "REAL_EMPL_RATE": "REAL_EMPL_RATE (3개월 취업률_숫자)", "COLLECTED_AT": "COLLECTED_AT (수집일시)",
    "TRNEE_ID": "TRNEE_ID (훈련생ID)", "TRNEE_NM": "TRNEE_NM (이름)", "TRNEE_STATUS": "TRNEE_STATUS (상태)",
    "TRNEE_TYPE": "TRNEE_TYPE (유형코드)", "BIRTH_DATE": "BIRTH_DATE (생년월일)",
    "TOTAL_DAYS": "TOTAL_DAYS (총훈련일수)", "OFLHD_CNT": "OFLHD_CNT (공가일수)", "VCATN_CNT": "VCATN_CNT (휴가일수)",
    "ABSENT_CNT": "ABSENT_CNT (결석일수)", "ATEND_CNT": "ATEND_CNT (출석일수)",
    "ATEND_DT": "ATEND_DT (출석일자)", "DAY_NM": "DAY_NM (요일)", "IN_TIME": "IN_TIME (입실시간)",
    "OUT_TIME": "OUT_TIME (퇴실시간)", "ATEND_STATUS": "ATEND_STATUS (상태_한글)", "ATEND_STATUS_CD": "ATEND_STATUS_CD (상태코드)",
    "TOTAL_RATE_6": "TOTAL_RATE_6 (총 취업률 합산)", "수료율(%)": "수료율 (%)", "나이": "나이 (훈련 당시 기준)",
    "TRAINST_NM": "TRAINST_NM (훈련기관명)", "NCS_CD": "NCS_CD (NCS코드)", "TRNG_AREA_CD": "TRNG_AREA_CD (지역코드)",
    "COURSE_MAN": "COURSE_MAN (수강비)", "REG_COURSE_MAN": "REG_COURSE_MAN (등록인원)",
    "EI_EMPL_RATE_3": "EI_EMPL_RATE_3 (3개월 취업률)", "STDG_SCOR": "STDG_SCOR (만족도)",
    "GRADE": "GRADE (등급)", "ADDRESS": "ADDRESS (주소)", "CERTIFICATE": "CERTIFICATE (자격증)",
    "TRAIN_TARGET": "TRAIN_TARGET (훈련유형)", "WKEND_SE": "WKEND_SE (주말구분)",
}


def get_connection():
    return _utils_get_connection()


def safe_sum_rate(row):
    ei = safe_float(row.get('EI_EMPL_RATE_6', 0))
    hrd = safe_float(row.get('HRD_EMPL_RATE_6', 0))
    return round(ei + hrd, 1)


@st.cache_data(ttl=600)
def load_all_data():
    if not is_pg() and not os.path.exists(DB_FILE):
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    df_course = _load_data("SELECT * FROM TB_COURSE_MASTER ORDER BY TRPR_DEGR ASC")
    if not df_course.empty:
        df_course['TOT_PAR_MKS'] = pd.to_numeric(df_course['TOT_PAR_MKS'], errors='coerce').fillna(0)
        df_course['TOT_FXNUM'] = pd.to_numeric(df_course['TOT_FXNUM'], errors='coerce').fillna(0)
        df_course['FINI_CNT'] = pd.to_numeric(df_course['FINI_CNT'], errors='coerce').fillna(0)
        df_course['수료율(%)'] = df_course.apply(
            lambda x: round((x['FINI_CNT'] / x['TOT_PAR_MKS'] * 100), 1) if x['TOT_PAR_MKS'] > 0 else 0, axis=1
        )
        df_course['TOTAL_RATE_6'] = df_course.apply(safe_sum_rate, axis=1)

    df_trainee = _load_data("SELECT * FROM TB_TRAINEE_INFO")
    if not df_trainee.empty and not df_course.empty:
        year_map = {}
        for _, row in df_course.iterrows():
            if row['TR_STA_DT']:
                try:
                    year_map[row['TRPR_DEGR']] = int(str(row['TR_STA_DT'])[:4])
                except:
                    continue

        def calc_training_age(row):
            birth = row.get('BIRTH_DATE')
            if not birth or len(str(birth)) != 8:
                return None
            try:
                birth_year = int(str(birth)[:4])
                target_year = year_map.get(row['TRPR_DEGR'], datetime.now().year)
                return target_year - birth_year + 1
            except:
                return None

        df_trainee['나이'] = df_trainee.apply(calc_training_age, axis=1)

    df_log = _load_data("SELECT * FROM TB_ATTENDANCE_LOG ORDER BY ATEND_DT DESC, IN_TIME ASC")

    # TB_MARKET_TREND 추가
    df_market = _load_data("SELECT COUNT(*) as CNT FROM TB_MARKET_TREND")
    market_cnt = int(df_market.iloc[0]['CNT']) if not df_market.empty else 0

    def apply_kst(df):
        if not df.empty and 'COLLECTED_AT' in df.columns:
            df['COLLECTED_AT'] = pd.to_datetime(df['COLLECTED_AT']) + timedelta(hours=9)
        return df

    return apply_kst(df_course), apply_kst(df_trainee), apply_kst(df_log), market_cnt


st.title("🔎 HRD 데이터 전체 감사 (Full Audit)")
st.markdown("DB에 저장된 **모든 컬럼**을 숨김없이 표시합니다.")

try:
    df_course, df_trainee, df_log, market_cnt = load_all_data()
except Exception as e:
    st.error(f"데이터 로드 실패: {e}")
    st.stop()

if not df_course.empty:
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("수집된 과정", f"{len(df_course)}건")
    c2.metric("수집된 훈련생", f"{len(df_trainee)}명")
    c3.metric("수집된 로그", f"{len(df_log):,}행")
    c4.metric("시장 동향", f"{market_cnt:,}건")
    c5.metric("최종 수집 시각", str(df_course['COLLECTED_AT'].max())[:16])
else:
    st.warning("데이터가 없습니다. 데이터 가져오기를 실행하세요.")

st.divider()
tab1, tab2, tab3, tab4 = st.tabs(["📘 과정 운영 현황", "👥 훈련생 명부", "📝 출결 기록부", "📈 시장 동향 데이터"])

with tab1:
    st.subheader("1. TB_COURSE_MASTER")
    if not df_course.empty:
        st.dataframe(df_course.rename(columns=COLUMN_MAP), use_container_width=True)

with tab2:
    st.subheader("2. TB_TRAINEE_INFO")
    if not df_trainee.empty:
        st.dataframe(df_trainee.rename(columns=COLUMN_MAP), use_container_width=True)

with tab3:
    st.subheader("3. TB_ATTENDANCE_LOG")
    if not df_log.empty:
        st.dataframe(df_log.rename(columns=COLUMN_MAP), use_container_width=True)

with tab4:
    st.subheader("4. TB_MARKET_TREND")
    st.caption(f"총 {market_cnt:,}건 중 최근 500건을 표시합니다.")
    df_market_sample = _load_data(
        "SELECT * FROM TB_MARKET_TREND ORDER BY TR_STA_DT DESC LIMIT 500"
    )
    if not df_market_sample.empty:
        st.dataframe(df_market_sample.rename(columns=COLUMN_MAP), use_container_width=True, height=600)
        csv = df_market_sample.to_csv(index=False).encode('utf-8-sig')
        st.download_button("📥 표시 데이터 CSV 다운로드", csv, "market_trend_sample.csv", "text/csv")
    else:
        st.info("시장 동향 데이터가 없습니다. Market ETL을 실행하세요.")

with st.sidebar:
    st.header("관리자 메뉴")
    if st.button("🔄 화면 새로고침"):
        st.rerun()
    if st.button("🚀 HRD-Net 데이터 가져오기"):
        with st.spinner("데이터 업데이트 중..."):
            run_etl()
        st.success("완료!")
        st.rerun()
