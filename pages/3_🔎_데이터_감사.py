import streamlit as st
import pandas as pd
import sqlite3
import sys
import os
from datetime import datetime, timedelta  # 👈 timedelta 추가됨

# ==========================================
# 0. 경로 설정 및 모듈 불러오기 (ETL 함수용)
# ==========================================
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
try:
    from hrd_etl import run_etl
except ImportError:
    def run_etl():
        st.error("❌ 'hrd_etl.py' 파일을 찾을 수 없습니다.")

# ==========================================
# 1. 설정 및 컬럼 매핑 (Full Audit View)
# ==========================================
st.set_page_config(
    page_title="HRD 데이터 감사 시스템 (Full View)",
    page_icon="🔎",
    layout="wide"
)

DB_FILE = "hrd_analysis.db"

# ✨ 모든 컬럼에 대한 한글 설명 매핑
COLUMN_MAP = {
    # [1] 과정 마스터 (TB_COURSE_MASTER)
    "TRPR_ID": "TRPR_ID (과정ID)",
    "TRPR_DEGR": "TRPR_DEGR (회차)",
    "TRPR_NM": "TRPR_NM (과정명)",
    "TR_STA_DT": "TR_STA_DT (시작일)",
    "TR_END_DT": "TR_END_DT (종료일)",
    "TOT_TRCO": "TOT_TRCO (총훈련비)",
    "FINI_CNT": "FINI_CNT (수료인원)",
    "TOT_FXNUM": "TOT_FXNUM (정원)",
    "TOT_PAR_MKS": "TOT_PAR_MKS (현원)",
    "TOT_TRP_CNT": "TOT_TRP_CNT (신청인원)",
    "INST_INO": "INST_INO (기관ID)",
    "EI_EMPL_RATE_3": "EI_EMPL_RATE_3 (3개월 고용보험 취업률)",
    "EI_EMPL_CNT_3": "EI_EMPL_CNT_3 (3개월 고용보험 취업자수)",
    "EI_EMPL_RATE_6": "EI_EMPL_RATE_6 (6개월 고용보험 취업률)",
    "EI_EMPL_CNT_6": "EI_EMPL_CNT_6 (6개월 고용보험 취업자수)",
    "HRD_EMPL_RATE_6": "HRD_EMPL_RATE_6 (6개월 HRD 취업률)",
    "HRD_EMPL_CNT_6": "HRD_EMPL_CNT_6 (6개월 HRD 취업자수)",
    "REAL_EMPL_RATE": "REAL_EMPL_RATE (3개월 취업률_숫자)",
    "COLLECTED_AT": "COLLECTED_AT (데이터수집일시)",
    
    # [2] 훈련생 정보 (TB_TRAINEE_INFO)
    "TRNEE_ID": "TRNEE_ID (훈련생ID)",
    "TRNEE_NM": "TRNEE_NM (이름)",
    "TRNEE_STATUS": "TRNEE_STATUS (상태)",
    "TRNEE_TYPE": "TRNEE_TYPE (유형코드)",
    "BIRTH_DATE": "BIRTH_DATE (생년월일)",
    "TOTAL_DAYS": "TOTAL_DAYS (총훈련일수)",
    "OFLHD_CNT": "OFLHD_CNT (공가일수)",
    "VCATN_CNT": "VCATN_CNT (휴가일수)",
    
    # [3] 출결 로그 (TB_ATTENDANCE_LOG)
    "ATEND_DT": "ATEND_DT (출석일자)",
    "DAY_NM": "DAY_NM (요일)",
    "IN_TIME": "IN_TIME (입실시간)",
    "OUT_TIME": "OUT_TIME (퇴실시간)",
    "ATEND_STATUS": "ATEND_STATUS (상태_한글)",
    "ATEND_STATUS_CD": "ATEND_STATUS_CD (상태코드)",
    
    # [4] 가공 컬럼 (파생 변수)
    "TOTAL_RATE_6": "TOTAL_RATE_6 (총 취업률 합산)",
    "모집률(%)": "모집률 (%)",
    "나이": "나이 (훈련 당시 기준)", 
    "이름_가공": "이름 (ID매핑)"
}

# ==========================================
# 2. 데이터 로드 및 가공
# ==========================================
def get_connection():
    return sqlite3.connect(DB_FILE)

# 총 취업률 계산
def safe_sum_rate(row):
    try: ei = float(row.get('EI_EMPL_RATE_6', 0))
    except: ei = 0.0
    try: hrd = float(row.get('HRD_EMPL_RATE_6', 0))
    except: hrd = 0.0
    return round(ei + hrd, 1)

@st.cache_data
def load_all_data():
    if not os.path.exists(DB_FILE):
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame() # DB 없으면 빈 프레임

    conn = get_connection()
    
    # -----------------------------------------------------------------
    # 1. 과정 정보 (Master) - DB에서 정렬해서 가져옴
    # -----------------------------------------------------------------
    df_course = pd.read_sql("SELECT * FROM TB_COURSE_MASTER ORDER BY TRPR_DEGR ASC", conn)
    
    # 가공: 모집률 & 총취업률
    if not df_course.empty:
        df_course['TOT_PAR_MKS'] = pd.to_numeric(df_course['TOT_PAR_MKS'], errors='coerce').fillna(0)
        df_course['TOT_FXNUM'] = pd.to_numeric(df_course['TOT_FXNUM'], errors='coerce').fillna(0)
        
        df_course['모집률(%)'] = df_course.apply(lambda x: round((x['TOT_PAR_MKS']/x['TOT_FXNUM']*100),1) if x['TOT_FXNUM']>0 else 0, axis=1)
        df_course['TOTAL_RATE_6'] = df_course.apply(safe_sum_rate, axis=1)

    # -----------------------------------------------------------------
    # 2. 훈련생 정보 (Trainee)
    # -----------------------------------------------------------------
    df_trainee = pd.read_sql("SELECT * FROM TB_TRAINEE_INFO", conn)
    
    # 나이 계산 (훈련 시작일 기준)
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
            except:
                return None
                
        df_trainee['나이'] = df_trainee.apply(calc_training_age, axis=1)

    # -----------------------------------------------------------------
    # 3. 출결 로그 (Log)
    # -----------------------------------------------------------------
    df_log = pd.read_sql("SELECT * FROM TB_ATTENDANCE_LOG ORDER BY ATEND_DT DESC, IN_TIME ASC", conn)
    
    conn.close()

    # 🚀 [NEW] 모든 데이터프레임의 수집일시를 한국 시간(+9h)으로 변환
    def apply_kst(df):
        if not df.empty and 'COLLECTED_AT' in df.columns:
            # 문자열 -> 날짜 변환 -> 9시간 더하기
            df['COLLECTED_AT'] = pd.to_datetime(df['COLLECTED_AT']) + timedelta(hours=9)
        return df

    df_course = apply_kst(df_course)
    df_trainee = apply_kst(df_trainee)
    df_log = apply_kst(df_log)

    return df_course, df_trainee, df_log

# ==========================================
# 3. 메인 화면 구성
# ==========================================
st.title("🔎 HRD 데이터 전체 감사 (Full Audit)")
st.markdown("""
DB에 저장된 **모든 컬럼**을 숨김없이 표시합니다.  
헤더 형식: **`DB컬럼명 (한글설명)`**
""")

try:
    df_course, df_trainee, df_log = load_all_data()
except Exception as e:
    st.error(f"데이터 로드 실패: {e}")
    st.stop()

# 요약 지표
if not df_course.empty:
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("수집된 과정", f"{len(df_course)}건")
    c2.metric("수집된 훈련생", f"{len(df_trainee)}명")
    c3.metric("수집된 로그", f"{len(df_log):,}행")
    
    # UTC -> KST로 변환된 시간을 표시
    last_collected = df_course['COLLECTED_AT'].max()
    c4.metric("최종 수집 시각", str(last_collected)[:16]) # 초 단위 절삭
else:
    st.warning("데이터가 아직 없습니다. 사이드바의 [🚀 데이터 가져오기] 버튼을 눌러주세요!")

st.divider()

tab1, tab2, tab3 = st.tabs(["📘 과정 운영 현황 (Master)", "👥 훈련생 명부 (Trainee)", "📝 출결 기록부 (Log)"])

# ----------------------------------------------------
# TAB 1: 과정 운영 현황
# ----------------------------------------------------
with tab1:
    st.subheader("1. TB_COURSE_MASTER (전체 컬럼)")
    if not df_course.empty:
        cols_order = [
            'TRPR_DEGR', 'TRPR_NM', 'TR_STA_DT', 'TR_END_DT', 
            'TOT_FXNUM', 'TOT_PAR_MKS', '모집률(%)', 'FINI_CNT', 
            'TOTAL_RATE_6', 'EI_EMPL_RATE_6', 'HRD_EMPL_RATE_6',
            'TRPR_ID', 'INST_INO', 'COLLECTED_AT' 
        ]
        remaining = [c for c in df_course.columns if c not in cols_order]
        final_cols = cols_order + remaining
        
        st.dataframe(
            df_course[final_cols].rename(columns=COLUMN_MAP),
            use_container_width=True,
            hide_index=True
        )

# ----------------------------------------------------
# TAB 2: 훈련생 명부
# ----------------------------------------------------
with tab2:
    st.subheader("2. TB_TRAINEE_INFO (전체 컬럼)")
    if not df_trainee.empty and not df_course.empty:
        degr_opts = sorted(df_course['TRPR_DEGR'].unique())
        
        col_f1, col_f2 = st.columns(2)
        with col_f1:
            sel_degr = st.selectbox("회차 필터 (Trainee)", ["전체"] + list(degr_opts))
        with col_f2:
            sel_status = st.multiselect("상태 필터", df_trainee['TRNEE_STATUS'].unique(), default=[])

        df_show = df_trainee.copy()
        if sel_degr != "전체":
            df_show = df_show[df_show['TRPR_DEGR'] == sel_degr]
        if sel_status:
            df_show = df_show[df_show['TRNEE_STATUS'].isin(sel_status)]

        t_cols = [
            'TRPR_DEGR', 'TRNEE_NM', '나이', 'BIRTH_DATE', 'TRNEE_STATUS', 
            'TOTAL_DAYS', 'OFLHD_CNT', 'VCATN_CNT', 'TRNEE_TYPE', 
            'TRNEE_ID', 'TRPR_ID', 'COLLECTED_AT'
        ]
        t_cols = [c for c in t_cols if c in df_show.columns]

        st.dataframe(
            df_show[t_cols].rename(columns=COLUMN_MAP),
            use_container_width=True,
            hide_index=True
        )

# ----------------------------------------------------
# TAB 3: 출결 기록부
# ----------------------------------------------------
with tab3:
    st.subheader("3. TB_ATTENDANCE_LOG (전체 컬럼)")
    if not df_log.empty and not df_course.empty:
        log_degr_opts = sorted(df_course['TRPR_DEGR'].unique())
        log_degr = st.selectbox("회차 선택 (Log 조회용)", log_degr_opts, index=0)
        
        df_log_show = df_log[df_log['TRPR_DEGR'] == log_degr].copy()
        
        name_map = df_trainee[df_trainee['TRPR_DEGR'] == log_degr].set_index('TRNEE_ID')['TRNEE_NM'].to_dict()
        df_log_show.insert(3, '이름_가공', df_log_show['TRNEE_ID'].map(name_map)) 
        
        l_cols = [
            'ATEND_DT', 'DAY_NM', '이름_가공', 'ATEND_STATUS', 'IN_TIME', 'OUT_TIME',
            'ATEND_STATUS_CD', 'TRNEE_ID', 'TRPR_DEGR', 'TRPR_ID', 'COLLECTED_AT'
        ]

        st.dataframe(
            df_log_show[l_cols].rename(columns=COLUMN_MAP),
            use_container_width=True,
            hide_index=True
        )

# ==========================================
# 4. 사이드바 (관리자 메뉴)
# ==========================================
with st.sidebar:
    st.header("관리자 메뉴")
    
    st.markdown("### 1️⃣ 화면 갱신")
    if st.button("🔄 화면(DB) 새로고침"):
        st.cache_data.clear()
        st.rerun()
        
    st.divider()
    
    st.markdown("### 2️⃣ 데이터 강제 수집")
    st.caption("HRD-Net에서 최신 데이터를 가져옵니다.")
    
    if st.button("🚀 HRD-Net 데이터 가져오기"):
        try:
            with st.spinner("HRD-Net 접속 중... (진행 중인 과정만 업데이트)"):
                run_etl()
                
            st.cache_data.clear()
            st.success("✅ 업데이트 완료! 화면을 갱신합니다.")
            st.rerun()
            
        except Exception as e:
            st.error(f"수집 중 오류 발생: {e}")
            
    st.info("💡 'TR_STA_DT(시작일)'을 기준으로 나이를 계산하여 보여줍니다.")