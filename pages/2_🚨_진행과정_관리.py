import streamlit as st
import pandas as pd
import altair as alt
from datetime import datetime, timedelta
import sys
import os

# utils.py 경로 설정
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils import load_data, check_password

# ==========================================
# 1. 페이지 설정
# ==========================================
st.set_page_config(
    page_title="진행 과정 관리",
    page_icon="🚨",
    layout="wide"
)

check_password()

st.title("🚨 진행 과정 실시간 관리")
st.markdown("현재 운영 중인 과정의 **실시간 출결 현황** (입/퇴실)과 **특이사항**을 집중 모니터링합니다.")

# ==========================================
# 2. 데이터 로드 (진행 중인 과정만)
# ==========================================
@st.cache_data(ttl=300)
def get_active_data():
    today_str = datetime.now().strftime('%Y-%m-%d')
    
    # 1. 진행 중인 과정 목록
    query_course = """
        SELECT * FROM TB_COURSE_MASTER 
        WHERE TR_END_DT >= ?
        ORDER BY TR_STA_DT
    """
    active_courses = load_data(query_course, params=[today_str])
    
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
    
    # 3. 오늘 및 최근 로그 (실시간 현황용)
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
    
    selected_degr = st.selectbox(
        "관리할 회차(기수) 선택",
        courses_df['TRPR_DEGR'].unique(),
        format_func=lambda x: f"{x}회차 ({courses_df[courses_df['TRPR_DEGR']==x]['TRPR_NM'].iloc[0]})"
    )
    
    st.divider()
    
    if st.button("🔄 데이터 새로고침"):
        st.cache_data.clear()
        st.rerun()
    st.caption("💡 '미퇴실'은 입실은 했으나 퇴실 기록이 없는 상태입니다.")

# 데이터 필터링
this_course = courses_df[courses_df['TRPR_DEGR'] == selected_degr].iloc[0]
this_students_all = trainees_df[trainees_df['TRPR_DEGR'] == selected_degr].copy()
active_students = this_students_all[~this_students_all['TRNEE_STATUS'].isin(['중도탈락', '제적'])].copy()
this_logs = logs_df[logs_df['TRPR_DEGR'] == selected_degr].copy()

# ==========================================
# 4. 실시간 출결 집계 로직
# ==========================================

if not this_logs.empty:
    target_date = this_logs['ATEND_DT'].max()
else:
    target_date = datetime.now().strftime('%Y-%m-%d')

today_logs = this_logs[this_logs['ATEND_DT'] == target_date].copy()

df_monitor = pd.merge(
    active_students[['TRNEE_ID', 'TRNEE_NM', 'TRNEE_STATUS']], 
    today_logs[['TRNEE_ID', 'IN_TIME', 'OUT_TIME', 'ATEND_STATUS']], 
    on='TRNEE_ID', 
    how='left'
)

# ----------------------------------------------------
# 🚀 [로직] 지각 판정 (09:10 기준)
# ----------------------------------------------------
def apply_late_rule(row):
    current_status = row['ATEND_STATUS']
    in_time = row['IN_TIME']
    
    # 1. 특이사항 보호 (조퇴/외출) - 결석은 제외(입실시간 있으면 무시)
    if str(current_status).strip() in ['조퇴', '외출']:
        return current_status
    
    # 2. 이미 지각이면 유지
    if str(current_status).strip() == '지각':
        return '지각'
    
    # 3. 시간 비교
    if pd.notna(in_time):
        time_digits = ''.join(filter(str.isdigit, str(in_time)))
        if len(time_digits) >= 3:
            try:
                # 앞에서부터 4자리만 끊어서 비교 (예: 092300 -> 923)
                time_val = int(time_digits[:4])
                if time_val > 910:
                    return '지각'
            except:
                pass 

    return current_status

df_monitor['ATEND_STATUS'] = df_monitor.apply(apply_late_rule, axis=1)

# --- 지표 계산 ---
total_cnt = len(active_students)
present_cnt = len(df_monitor[df_monitor['IN_TIME'].notna()])
not_left_cnt = len(df_monitor[(df_monitor['IN_TIME'].notna()) & (df_monitor['OUT_TIME'].isna())])
late_cnt = len(df_monitor[df_monitor['ATEND_STATUS'] == '지각'])
early_cnt = len(df_monitor[df_monitor['ATEND_STATUS'] == '조퇴'])
out_cnt = len(df_monitor[df_monitor['ATEND_STATUS'] == '외출'])
absent_students = df_monitor[df_monitor['IN_TIME'].isna()]
real_absent_cnt = len(absent_students)

# ==========================================
# 5. 대시보드 화면 구성
# ==========================================

st.subheader(f"📌 {selected_degr}회차 실시간 현황 ({target_date} 기준)")
d_day = (pd.to_datetime(this_course['TR_END_DT']) - pd.to_datetime(datetime.now().date())).days
st.info(f"**과정명:** {this_course['TRPR_NM']} (D-{d_day})")

st.divider()

c1, c2, c3, c4 = st.columns(4)
c1.metric("총 재원", f"{total_cnt}명")
c2.metric("금일 입실", f"{present_cnt}명")
c3.metric("현재 미퇴실", f"{not_left_cnt}명", delta_color="off")
c4.metric("결석/미출석", f"{real_absent_cnt}명", delta_color="inverse")

c5, c6, c7, c8 = st.columns(4)
c5.metric("지각", f"{late_cnt}명", delta_color="inverse")
c6.metric("조퇴", f"{early_cnt}명", delta_color="inverse")
c7.metric("외출", f"{out_cnt}명")
c8.metric("퇴실 완료", f"{present_cnt - not_left_cnt}명")

st.divider()

with st.expander("📝 보고용 텍스트 복사", expanded=True):
    def get_names_str(df, type_):
        names = []
        if type_ == 'absent': names = df[df['IN_TIME'].isna()]['TRNEE_NM'].tolist()
        elif type_ == 'not_left': names = df[(df['IN_TIME'].notna()) & (df['OUT_TIME'].isna())]['TRNEE_NM'].tolist()
        elif type_ == 'late':
            target = df[df['ATEND_STATUS'] == '지각']
            for _, row in target.iterrows():
                clean_time = str(row['IN_TIME']).strip()
                names.append(f"{row['TRNEE_NM']}({clean_time})")
        elif type_ == 'early': names = df[df['ATEND_STATUS'] == '조퇴']['TRNEE_NM'].tolist()
        elif type_ == 'out': names = df[df['ATEND_STATUS'] == '외출']['TRNEE_NM'].tolist()
        return ", ".join(names) if names else "없음"

    last_collect = pd.to_datetime(this_logs['COLLECTED_AT']).max() + timedelta(hours=9) if 'COLLECTED_AT' in this_logs.columns else datetime.now()
    report_text = f"""[{last_collect.strftime('%H시 %M분')} 기준]

- 총인원: {total_cnt}명
 ㄴ 현 인원: {not_left_cnt}명
 ㄴ 현재 강의장에 없는 인원: {total_cnt - not_left_cnt}명

<특이사항>
지각: {late_cnt}명, 조퇴: {early_cnt}명, 외출: {out_cnt}명, 결석: {real_absent_cnt}명
[지각] {get_names_str(df_monitor, 'late')}
[조퇴] {get_names_str(df_monitor, 'early')}
[외출] {get_names_str(df_monitor, 'out')}
[결석] {get_names_str(df_monitor, 'absent')}
[미퇴실] {get_names_str(df_monitor, 'not_left')}
"""
    st.text_area("보고 양식", report_text, height=300)

st.divider()

t1, t2, t3 = st.tabs(["🚨 미퇴실/특이사항", "❌ 결석자", "📋 전체 출석부"])

with t1:
    issue_list = df_monitor[(df_monitor['OUT_TIME'].isna() & df_monitor['IN_TIME'].notna()) | (df_monitor['ATEND_STATUS'].isin(['지각', '조퇴', '외출']))].copy()
    if not issue_list.empty:
        issue_list['상태_요약'] = issue_list.apply(lambda x: '🟢 미퇴실(수업중)' if pd.isna(x['OUT_TIME']) and pd.notna(x['IN_TIME']) else x['ATEND_STATUS'], axis=1)
        st.dataframe(issue_list[['TRNEE_NM', 'IN_TIME', 'OUT_TIME', '상태_요약']], use_container_width=True, hide_index=True)
    else: st.success("특이사항 없음")

with t2:
    if not absent_students.empty: st.dataframe(absent_students[['TRNEE_NM', 'TRNEE_STATUS']], use_container_width=True, hide_index=True)
    else: st.success("전원 출석! 🎉")

with t3:
    st.dataframe(df_monitor[['TRNEE_NM', 'IN_TIME', 'OUT_TIME', 'ATEND_STATUS']], use_container_width=True, hide_index=True)