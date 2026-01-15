import streamlit as st
import pandas as pd
import altair as alt
from datetime import datetime
import sys
import os

# 상위 폴더의 utils.py를 불러오기 위한 경로 설정
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils import load_data, calculate_age_at_training

# ==========================================
# 1. 페이지 설정
# ==========================================
st.set_page_config(
    page_title="기수별 성과 분석",
    page_icon="📊",
    layout="wide"
)

st.title("📊 기수별 성과 심층 분석")
st.markdown("종료된 과정의 **수료율, 취업률, 출석 패턴**을 다각도로 분석합니다.")

# ==========================================
# 2. 데이터 로드 및 전처리
# ==========================================
@st.cache_data
def get_course_list():
    """분석 가능한(종료된) 과정 목록을 가져옵니다."""
    # TR_END_DT가 오늘 이전인 과정만
    today = datetime.now().strftime('%Y-%m-%d')
    query = f"""
        SELECT DISTINCT TRPR_DEGR, TRPR_NM, TR_STA_DT, TR_END_DT 
        FROM TB_COURSE_MASTER 
        WHERE TR_END_DT < '{today}'
        ORDER BY CAST(TRPR_DEGR AS INTEGER) DESC
    """
    return load_data(query)

@st.cache_data
def get_analysis_data(degr):
    """선택한 기수의 상세 데이터를 가져옵니다."""
    
    # 1. 과정 마스터 정보
    course_df = load_data(f"SELECT * FROM TB_COURSE_MASTER WHERE TRPR_DEGR = {degr}")
    
    # 2. 훈련생 정보 + 나이 계산
    trainee_df = load_data(f"SELECT * FROM TB_TRAINEE_INFO WHERE TRPR_DEGR = {degr}")
    
    if not course_df.empty and not trainee_df.empty:
        start_date = course_df.iloc[0]['TR_STA_DT']
        trainee_df['나이'] = trainee_df['BIRTH_DATE'].apply(
            lambda x: calculate_age_at_training(x, start_date)
        )
        # 연령대 그룹핑 (20대, 30대...)
        trainee_df['연령대'] = trainee_df['나이'].apply(
            lambda x: f"{int(x // 10 * 10)}대" if pd.notnull(x) else "미상"
        )

    # 3. [고급] 출결 로그에서 학생별 '실제 출석일수' 집계
    # (TB_TRAINEE_INFO에는 총 훈련일수만 있고 실제 출석일수는 누락된 경우가 많아 직접 계산)
    log_query = f"""
        SELECT TRNEE_ID, 
               COUNT(*) as 총_로그_수,
               SUM(CASE WHEN ATEND_STATUS = '결석' THEN 1 ELSE 0 END) as 결석_횟수,
               SUM(CASE WHEN ATEND_STATUS IN ('지각', '조퇴', '외출') THEN 1 ELSE 0 END) as 지각_조퇴_횟수
        FROM TB_ATTENDANCE_LOG
        WHERE TRPR_DEGR = {degr}
        GROUP BY TRNEE_ID
    """
    attend_stats = load_data(log_query)
    
    # 훈련생 정보에 출결 통계 병합
    if not attend_stats.empty:
        trainee_df = pd.merge(trainee_df, attend_stats, on='TRNEE_ID', how='left').fillna(0)

    return course_df, trainee_df

# ==========================================
# 3. 사이드바 (필터)
# ==========================================
course_list = get_course_list()

if course_list.empty:
    st.warning("분석할 수 있는 종료된 과정 데이터가 없습니다.")
    st.stop()

with st.sidebar:
    st.header("🔍 분석 대상 선택")
    # 과정명은 하나라고 가정하고 회차만 선택 (여러 과정이면 과정명 필터 추가 필요)
    selected_degr = st.selectbox(
        "회차(기수)를 선택하세요",
        course_list['TRPR_DEGR'].unique(),
        format_func=lambda x: f"{x}회차"
    )
    
    # 선택된 과정 정보 표시
    sel_course_info = course_list[course_list['TRPR_DEGR'] == selected_degr].iloc[0]
    st.info(f"**과정명:** {sel_course_info['TRPR_NM']}\n\n"
            f"**기간:** {sel_course_info['TR_STA_DT']} ~ {sel_course_info['TR_END_DT']}")

# 데이터 로드
master_df, students_df = get_analysis_data(selected_degr)

if master_df.empty:
    st.error("해당 회차의 마스터 데이터가 없습니다.")
    st.stop()

# ==========================================
# 4. 메인 분석 화면
# ==========================================

# --- [Section 1] 성과 요약 (Scorecard) ---
st.subheader(f"🏆 {selected_degr}회차 종합 성적표")

# 데이터 전처리
row = master_df.iloc[0]
# 현원 데이터가 0이면(가끔 API 누락됨) 학생 명부 수로 대체
total_std = row['TOT_PAR_MKS'] if row.get('TOT_PAR_MKS', 0) > 0 else len(students_df) 
fini_std = row['FINI_CNT'] if row.get('FINI_CNT') else 0
dropout_std = total_std - fini_std 

# ✨ [수정] 숫자가 아닌 값('B', 'null' 등)이 와도 0.0으로 처리하는 안전 함수
def safe_float(val):
    try:
        return float(val)
    except (ValueError, TypeError):
        return 0.0

# 취업률 (EI + HRD) 계산
ei_rate = safe_float(row.get('EI_EMPL_RATE_6'))
hrd_rate = safe_float(row.get('HRD_EMPL_RATE_6'))
total_empl_rate = ei_rate + hrd_rate

# 컬럼 레이아웃
col1, col2, col3, col4 = st.columns(4)
col1.metric("수료율", f"{(fini_std/total_std*100):.1f}%" if total_std > 0 else "0%", f"{fini_std}/{total_std}명")
col2.metric("총 취업률 (6개월)", f"{total_empl_rate:.1f}%", help="고용보험 + HRD 합산")
col3.metric("중도 탈락", f"{dropout_std}명", delta_color="inverse")
col4.metric("평균 결석일", f"{students_df['결석_횟수'].mean():.1f}일" if '결석_횟수' in students_df.columns else "-")

st.divider()

# --- [Section 2] 상세 분석 탭 ---
tab1, tab2, tab3 = st.tabs(["👥 인구통계 분석", "📉 출결/이탈 분석", "📋 학생 명부"])

# [Tab 1] 인구통계 분석 (나이, 유형)
with tab1:
    c1, c2 = st.columns(2)
    
    with c1:
        st.markdown("##### 🎂 연령대별 분포")
        if '연령대' in students_df.columns:
            age_chart = alt.Chart(students_df).mark_arc(innerRadius=50).encode(
                theta=alt.Theta("count()", stack=True),
                color=alt.Color("연령대", scale=alt.Scale(scheme='category20')),
                tooltip=["연령대", "count()"]
            ).properties(height=300)
            st.altair_chart(age_chart, use_container_width=True)
    
    with c2:
        st.markdown("##### 🏷️ 훈련생 유형별 분포")
        if 'TRNEE_TYPE' in students_df.columns:
            type_counts = students_df['TRNEE_TYPE'].value_counts().reset_index()
            type_counts.columns = ['유형', '인원']
            
            type_chart = alt.Chart(type_counts).mark_bar().encode(
                x=alt.X('인원:Q'),
                y=alt.Y('유형:N', sort='-x'),
                color=alt.value('orange'),
                tooltip=['유형', '인원']
            ).properties(height=300)
            st.altair_chart(type_chart, use_container_width=True)

# [Tab 2] 출결 및 이탈 분석
with tab2:
    st.markdown("##### 📍 출석률과 수료 상태의 상관관계")
    st.caption("결석이 많을수록 '중도탈락'이나 '제적' 상태일 확률이 높습니다.")
    
    if '결석_횟수' in students_df.columns:
        # 산점도: X축=나이, Y축=결석횟수, 색상=상태
        scatter = alt.Chart(students_df).mark_circle(size=60).encode(
            x=alt.X('나이:Q', scale=alt.Scale(domain=[15, 50])),
            y=alt.Y('결석_횟수:Q', title='총 결석 일수'),
            color='TRNEE_STATUS',
            tooltip=['TRNEE_NM', '나이', '결석_횟수', 'TRNEE_STATUS']
        ).interactive().properties(height=400)
        
        st.altair_chart(scatter, use_container_width=True)
        
        # 위험군 추출 (결석 3일 이상이면서 수료하지 못한 사람)
        risk_students = students_df[
            (students_df['결석_횟수'] >= 3) & 
            (students_df['TRNEE_STATUS'] != '수료') # 상태명은 실제 데이터에 따라 조정 필요
        ]
        if not risk_students.empty:
            st.warning(f"⚠️ **출석 불량 위험군 ({len(risk_students)}명):** 결석이 3일 이상 기록된 미수료 학생들입니다.")
            st.dataframe(risk_students[['TRNEE_NM', '나이', '결석_횟수', '지각_조퇴_횟수', 'TRNEE_STATUS']], hide_index=True)

# [Tab 3] 전체 명부
with tab3:
    st.dataframe(
        students_df[['TRNEE_NM', '나이', '연령대', 'TRNEE_STATUS', '결석_횟수', 'TRNEE_TYPE']],
        column_config={
            "결석_횟수": st.column_config.NumberColumn("결석(일)", format="%d"),
        },
        use_container_width=True,
        hide_index=True
    )