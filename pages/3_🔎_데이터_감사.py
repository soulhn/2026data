import streamlit as st
import pandas as pd
import sys
import os
from datetime import datetime, timedelta

# 🚀 리팩토링: 상위 폴더 경로 추가 및 utils 가져오기
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
try:
    from hrd_etl import run_etl
    from utils import DB_FILE, get_connection as _utils_get_connection, safe_float, check_password, is_pg
except ImportError:
    def run_etl(): st.error("❌ 'hrd_etl.py'를 찾을 수 없습니다.")
    DB_FILE = "hrd_analysis.db" # 비상용 기본값

# ==========================================
# 1. 설정 및 컬럼 매핑
# ==========================================
st.set_page_config(page_title="HRD 데이터 감사 시스템", page_icon="🔎", layout="wide")

check_password()

# DB_FILE 정의 제거됨 (utils 사용)

# ✨ 컬럼 매핑 (그대로 유지)
COLUMN_MAP = {
    "TRPR_ID": "TRPR_ID (과정ID)", "TRPR_DEGR": "TRPR_DEGR (회차)", "TRPR_NM": "TRPR_NM (과정명)",
    "TR_STA_DT": "TR_STA_DT (시작일)", "TR_END_DT": "TR_END_DT (종료일)", "TOT_TRCO": "TOT_TRCO (총훈련비)",
    "FINI_CNT": "FINI_CNT (수료인원)", "TOT_FXNUM": "TOT_FXNUM (정원)", "TOT_PAR_MKS": "TOT_PAR_MKS (현원)",
    "TOT_TRP_CNT": "TOT_TRP_CNT (신청인원)", "INST_INO": "INST_INO (기관ID)",
    "EI_EMPL_RATE_3": "EI_EMPL_RATE_3 (3개월 고용보험 취업률)", "EI_EMPL_CNT_3": "EI_EMPL_CNT_3 (3개월 고용보험 취업자수)",
    "EI_EMPL_RATE_6": "EI_EMPL_RATE_6 (6개월 고용보험 취업률)", "EI_EMPL_CNT_6": "EI_EMPL_CNT_6 (6개월 고용보험 취업자수)",
    "HRD_EMPL_RATE_6": "HRD_EMPL_RATE_6 (6개월 HRD 취업률)", "HRD_EMPL_CNT_6": "HRD_EMPL_CNT_6 (6개월 HRD 취업자수)",
    "REAL_EMPL_RATE": "REAL_EMPL_RATE (3개월 취업률_숫자)", "COLLECTED_AT": "COLLECTED_AT (데이터수집일시)",
    "TRNEE_ID": "TRNEE_ID (훈련생ID)", "TRNEE_NM": "TRNEE_NM (이름)", "TRNEE_STATUS": "TRNEE_STATUS (상태)",
    "TRNEE_TYPE": "TRNEE_TYPE (유형코드)", "BIRTH_DATE": "BIRTH_DATE (생년월일)",
    "TOTAL_DAYS": "TOTAL_DAYS (총훈련일수)", "OFLHD_CNT": "OFLHD_CNT (공가일수)", "VCATN_CNT": "VCATN_CNT (휴가일수)",
    "ABSENT_CNT": "ABSENT_CNT (결석일수)", "ATEND_CNT": "ATEND_CNT (출석일수)",
    "ATEND_DT": "ATEND_DT (출석일자)", "DAY_NM": "DAY_NM (요일)", "IN_TIME": "IN_TIME (입실시간)",
    "OUT_TIME": "OUT_TIME (퇴실시간)", "ATEND_STATUS": "ATEND_STATUS (상태_한글)", "ATEND_STATUS_CD": "ATEND_STATUS_CD (상태코드)",
    "TOTAL_RATE_6": "TOTAL_RATE_6 (총 취업률 합산)", "모집률(%)": "모집률 (%)", "나이": "나이 (훈련 당시 기준)", "이름_가공": "이름 (ID매핑)"
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
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    conn = get_connection()
    df_course = pd.read_sql("SELECT * FROM TB_COURSE_MASTER ORDER BY TRPR_DEGR ASC", conn)
    if is_pg():
        df_course.columns = [c.upper() for c in df_course.columns]
    
    if not df_course.empty:
        df_course['TOT_PAR_MKS'] = pd.to_numeric(df_course['TOT_PAR_MKS'], errors='coerce').fillna(0)
        df_course['TOT_FXNUM'] = pd.to_numeric(df_course['TOT_FXNUM'], errors='coerce').fillna(0)
        df_course['모집률(%)'] = df_course.apply(lambda x: round((x['TOT_PAR_MKS']/x['TOT_FXNUM']*100),1) if x['TOT_FXNUM']>0 else 0, axis=1)
        df_course['TOTAL_RATE_6'] = df_course.apply(safe_sum_rate, axis=1)

    df_trainee = pd.read_sql("SELECT * FROM TB_TRAINEE_INFO", conn)
    if is_pg():
        df_trainee.columns = [c.upper() for c in df_trainee.columns]
    
    if not df_trainee.empty and not df_course.empty:
        year_map = {}
        for _, row in df_course.iterrows():
            if row['TR_STA_DT']:
                try: year_map[row['TRPR_DEGR']] = int(str(row['TR_STA_DT'])[:4])
                except: continue
        
        def calc_training_age(row):
            birth = row.get('BIRTH_DATE')
            if not birth or len(str(birth)) != 8: return None
            try:
                birth_year = int(str(birth)[:4])
                target_year = year_map.get(row['TRPR_DEGR'], datetime.now().year)
                return target_year - birth_year + 1 
            except: return None
                
        df_trainee['나이'] = df_trainee.apply(calc_training_age, axis=1)

    df_log = pd.read_sql("SELECT * FROM TB_ATTENDANCE_LOG ORDER BY ATEND_DT DESC, IN_TIME ASC", conn)
    if is_pg():
        df_log.columns = [c.upper() for c in df_log.columns]
    conn.close()

    def apply_kst(df):
        if not df.empty and 'COLLECTED_AT' in df.columns:
            df['COLLECTED_AT'] = pd.to_datetime(df['COLLECTED_AT']) + timedelta(hours=9)
        return df

    return apply_kst(df_course), apply_kst(df_trainee), apply_kst(df_log)

st.title("🔎 HRD 데이터 전체 감사 (Full Audit)")
st.markdown("DB에 저장된 **모든 컬럼**을 숨김없이 표시합니다.")

try:
    df_course, df_trainee, df_log = load_all_data()
except Exception as e:
    st.error(f"데이터 로드 실패: {e}")
    st.stop()

if not df_course.empty:
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("수집된 과정", f"{len(df_course)}건")
    c2.metric("수집된 훈련생", f"{len(df_trainee)}명")
    c3.metric("수집된 로그", f"{len(df_log):,}행")
    c4.metric("최종 수집 시각", str(df_course['COLLECTED_AT'].max())[:16])
else:
    st.warning("데이터가 없습니다. 데이터 가져오기를 실행하세요.")

st.divider()
tab1, tab2, tab3 = st.tabs(["📘 과정 운영 현황", "👥 훈련생 명부", "📝 출결 기록부"])

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

with st.sidebar:
    st.header("관리자 메뉴")
    if st.button("🔄 화면 새로고침"): st.rerun()
    if st.button("🚀 HRD-Net 데이터 가져오기"):
        with st.spinner("데이터 업데이트 중..."):
            run_etl()
        st.success("완료!")
        st.rerun()