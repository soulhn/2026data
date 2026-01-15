import streamlit as st
import pandas as pd
import altair as alt
from datetime import datetime, timedelta
import sys
import os

# utils.py 경로 설정
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils import load_data, calculate_age_at_training

# ==========================================
# 1. 페이지 설정
# ==========================================
st.set_page_config(
    page_title="진행 과정 관리",
    page_icon="🚨",
    layout="wide"
)

st.title("🚨 진행 과정 실시간 관리")
st.markdown("현재 운영 중인 과정의 **금일 출결 현황**과 **중도탈락 위험군**을 집중 관리합니다.")

# ==========================================
# 2. 데이터 로드 (진행 중인 과정만)
# ==========================================
@st.cache_data
def get_active_data():
    today_str = datetime.now().strftime('%Y-%m-%d')
    
    # 1. 진행 중인 과정 목록
    query_course = f"""
        SELECT * FROM TB_COURSE_MASTER 
        WHERE TR_END_DT >= '{today_str}'
        ORDER BY TR_STA_DT
    """
    active_courses = load_data(query_course)
    
    if active_courses.empty:
        return None, None, None

    # 2. 해당 과정들의 훈련생 정보
    course_ids = ",".join([f"'{x}'" for x in active_courses['TRPR_ID'].unique()])
    degrs = ",".join([str(x) for x in active_courses['TRPR_DEGR'].unique()])
    
    query_trainee = f"""
        SELECT * FROM TB_TRAINEE_INFO 
        WHERE TRPR_ID IN ({course_ids}) AND TRPR_DEGR IN ({degrs})
    """
    active_trainees = load_data(query_trainee)
    
    # 3. 최근 7일간의 출결 로그 (최신 경향 파악용)
    query_log = f"""
        SELECT * FROM TB_ATTENDANCE_LOG 
        WHERE TRPR_ID IN ({course_ids}) AND TRPR_DEGR IN ({degrs})
        ORDER BY ATEND_DT DESC
    """
    recent_logs = load_data(query_log)
    
    return active_courses, active_trainees, recent_logs

try:
    courses_df, trainees_df, logs_df = get_active_data()
except Exception as e:
    st.error(f"데이터 로드 중 오류: {e}")
    st.stop()

if courses_df is None:
    st.info("현재 진행 중인 과정이 없습니다. 꿀 같은 휴식 시간입니다! ☕")
    st.stop()

# ==========================================
# 3. 사이드바 & 과정 선택
# ==========================================
with st.sidebar:
    st.header("🎯 관리 대상 선택")
    
    # 과정 선택 (진행 중인 것만)
    selected_degr = st.selectbox(
        "관리할 회차(기수) 선택",
        courses_df['TRPR_DEGR'].unique(),
        format_func=lambda x: f"{x}회차 ({courses_df[courses_df['TRPR_DEGR']==x]['TRPR_NM'].iloc[0]})"
    )
    
    st.divider()
    st.caption("💡 Tip: '위험군'은 결석이 잦거나 휴가 소진율이 높은 학생을 의미합니다.")

# 선택된 데이터 필터링
this_course = courses_df[courses_df['TRPR_DEGR'] == selected_degr].iloc[0]
this_students = trainees_df[trainees_df['TRPR_DEGR'] == selected_degr].copy()
this_logs = logs_df[logs_df['TRPR_DEGR'] == selected_degr].copy()

# ==========================================
# 4. 대시보드 본문
# ==========================================

# [헤더 정보]
d_day = (pd.to_datetime(this_course['TR_END_DT']) - pd.to_datetime(datetime.now().date())).days
st.subheader(f"📌 {selected_degr}회차 운영 현황 (종료까지 D-{d_day}일)")
st.info(f"**과정명:** {this_course['TRPR_NM']} ({this_course['TR_STA_DT']} ~ {this_course['TR_END_DT']})")

# --- [Section 1] 오늘의 출결 속보 ---
st.markdown("#### 📢 최신 출결 속보")

if not this_logs.empty:
    last_date = this_logs['ATEND_DT'].max()
    today_logs = this_logs[this_logs['ATEND_DT'] == last_date]
    
    col1, col2, col3 = st.columns(3)
    col1.metric("기준 일자", last_date, "데이터 수집일")
    
    # 결석/지각 카운트
    absent_cnt = len(today_logs[today_logs['ATEND_STATUS'] == '결석'])
    late_cnt = len(today_logs[today_logs['ATEND_STATUS'].isin(['지각', '조퇴'])])
    
    col2.metric("결석", f"{absent_cnt}명", delta_color="inverse", delta="연락 필요" if absent_cnt > 0 else "정상")
    col3.metric("지각/조퇴", f"{late_cnt}명", delta_color="inverse", delta="확인 필요" if late_cnt > 0 else "정상")
    
    # 결석자 명단 표시
    if absent_cnt > 0:
        absent_list = today_logs[today_logs['ATEND_STATUS'] == '결석']['TRNEE_ID'].unique()
        absent_names = this_students[this_students['TRNEE_ID'].isin(absent_list)]['TRNEE_NM'].tolist()
        st.error(f"🚨 **결석자 명단:** {', '.join(absent_names)}")
    elif late_cnt > 0:
        st.warning("지각/조퇴 인원이 있습니다. 하단 로그를 확인하세요.")
    else:
        st.success("🎉 전원 출석! 특이사항 없습니다.")
else:
    st.warning("아직 수집된 출결 로그가 없습니다.")

st.divider()

# --- [Section 2] 중도탈락 위험군 탐지 (Risk Management) ---
st.markdown("#### ⚠️ 중도탈락 위험군 (집중 케어 필요)")

# 위험군 로직
# 1. 휴가 과다 사용 (남은 휴가가 1일 이하)
# 2. 최근 결석 누적 (최근 로그 기준)
risk_list = []

for idx, std in this_students.iterrows():
    risk_factors = []
    
    # (1) 휴가 체크 (데이터가 있을 경우)
    if pd.notnull(std['VCATN_CNT']) and std['VCATN_CNT'] >= 5: # 예: 6일 중 5일 씀
        risk_factors.append("휴가 소진 임박")
        
    # (2) 결석 체크 (로그 기반)
    std_logs = this_logs[this_logs['TRNEE_ID'] == std['TRNEE_ID']]
    absent_count = len(std_logs[std_logs['ATEND_STATUS'] == '결석'])
    
    if absent_count >= 3:
        risk_factors.append(f"누적 결석 {absent_count}일")
    
    # 위험 요소가 하나라도 있으면 리스트 추가
    if risk_factors:
        risk_list.append({
            "이름": std['TRNEE_NM'],
            "생년월일": std['BIRTH_DATE'],
            "현재 상태": std['TRNEE_STATUS'],
            "위험 요인": ", ".join(risk_factors),
            "휴가사용": f"{std['VCATN_CNT']}일"
        })

if risk_list:
    risk_df = pd.DataFrame(risk_list)
    st.data_editor(
        risk_df,
        column_config={
            "위험 요인": st.column_config.TextColumn("🚨 위험 사유", width="medium"),
            "휴가사용": st.column_config.ProgressColumn("휴가 사용량", min_value=0, max_value=6, format="%s")
        },
        use_container_width=True,
        hide_index=True
    )
else:
    st.success("현재 감지된 위험군 학생이 없습니다. 안정적으로 운영 중입니다! 👍")

# --- [Section 3] 전체 훈련생 현황 ---
with st.expander("📋 전체 훈련생 목록 보기 (클릭하여 펼치기)"):
    st.dataframe(
        this_students[['TRNEE_NM', 'BIRTH_DATE', 'TRNEE_STATUS', 'TOTAL_DAYS', 'VCATN_CNT']],
        use_container_width=True,
        hide_index=True
    )