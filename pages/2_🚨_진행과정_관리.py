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
st.markdown("현재 운영 중인 과정의 **실시간 출결 현황(입/퇴실)**과 **특이사항**을 집중 모니터링합니다.")

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
    
    # 과정 선택 (진행 중인 것만)
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

# 재원생(수강중)만 필터링하여 기준 인원으로 설정
active_students = this_students_all[~this_students_all['TRNEE_STATUS'].isin(['중도탈락', '제적'])].copy()

this_logs = logs_df[logs_df['TRPR_DEGR'] == selected_degr].copy()

# ==========================================
# 4. 실시간 출결 집계 로직
# ==========================================

# 기준 날짜: DB에 있는 가장 최신 날짜 (보통 오늘)
if not this_logs.empty:
    target_date = this_logs['ATEND_DT'].max()
else:
    target_date = datetime.now().strftime('%Y-%m-%d')

# 오늘자 로그만 필터링
today_logs = this_logs[this_logs['ATEND_DT'] == target_date].copy()

# [데이터 병합] 재원생 명부 + 오늘 로그 (Left Join)
df_monitor = pd.merge(
    active_students[['TRNEE_ID', 'TRNEE_NM', 'TRNEE_STATUS']], 
    today_logs[['TRNEE_ID', 'IN_TIME', 'OUT_TIME', 'ATEND_STATUS']], 
    on='TRNEE_ID', 
    how='left'
)

# --- 지표 계산 ---
total_cnt = len(active_students) # 총 재원

# 1. 입실 (IN_TIME이 있는 사람)
present_list = df_monitor[df_monitor['IN_TIME'].notna()]
present_cnt = len(present_list)

# 2. 미퇴실 (입실O, 퇴실X) - 현재 교실에 있는 인원
not_left_list = present_list[present_list['OUT_TIME'].isna()]
not_left_cnt = len(not_left_list)

# 3. 특이사항 카운트
late_cnt = len(today_logs[today_logs['ATEND_STATUS'] == '지각'])
early_cnt = len(today_logs[today_logs['ATEND_STATUS'] == '조퇴'])
out_cnt = len(today_logs[today_logs['ATEND_STATUS'] == '외출'])

# 4. 결석 (로그 없음 or 상태 '결석')
absent_students = df_monitor[
    (df_monitor['IN_TIME'].isna()) | (df_monitor['ATEND_STATUS'] == '결석')
]
real_absent_cnt = len(absent_students)

# ==========================================
# 5. 대시보드 화면 구성
# ==========================================

# [헤더]
st.subheader(f"📌 {selected_degr}회차 실시간 현황 ({target_date} 기준)")
d_day = (pd.to_datetime(this_course['TR_END_DT']) - pd.to_datetime(datetime.now().date())).days
st.info(f"**과정명:** {this_course['TRPR_NM']} (D-{d_day})")

st.divider()

# [Row 1] 메인 현황판
c1, c2, c3, c4 = st.columns(4)
c1.metric("총 재원", f"{total_cnt}명")
c2.metric("금일 입실", f"{present_cnt}명", delta=f"{present_cnt/total_cnt*100:.1f}%")
c3.metric("현재 미퇴실(교실)", f"{not_left_cnt}명", delta_color="off", help="입실O, 퇴실X")
c4.metric("결석/미출석", f"{real_absent_cnt}명", delta_color="inverse")

# [Row 2] 상세 특이사항
c5, c6, c7, c8 = st.columns(4)
c5.metric("지각", f"{late_cnt}명", delta_color="inverse")
c6.metric("조퇴", f"{early_cnt}명", delta_color="inverse")
c7.metric("외출", f"{out_cnt}명")
c8.metric("퇴실 완료", f"{present_cnt - not_left_cnt}명")

st.divider()

# ----------------------------------------------------
# 📝 [New!] 보고용 텍스트 생성기
# ----------------------------------------------------
with st.expander("📝 보고용 텍스트 복사 (클릭하여 펼치기)", expanded=True):
    st.caption("아래 텍스트를 복사해서 메신저나 보고서에 바로 붙여넣으세요.")
    
    # 명단 추출 함수
    def get_names_str(df, status_col, status_val=None, is_absent=False):
        if is_absent:
            names = df[(df['IN_TIME'].isna()) | (df['ATEND_STATUS'] == '결석')]['TRNEE_NM'].tolist()
        else:
            names = df[df[status_col] == status_val]['TRNEE_NM'].tolist()
        
        return ", ".join(names) if names else "없음"

    # 🚀 [수정 완료] UTC 시간을 한국 시간(KST)으로 변환 (+9시간)
    if not this_logs.empty and 'COLLECTED_AT' in this_logs.columns:
        # 1. DB에 저장된 시간(UTC) 가져오기
        last_collect_dt_utc = pd.to_datetime(this_logs['COLLECTED_AT']).max()
        
        # 2. 한국 시간으로 변환 (9시간 더하기)
        last_collect_dt_kst = last_collect_dt_utc + timedelta(hours=9)
        
        ref_time_str = last_collect_dt_kst.strftime('%H시 %M분')
    else:
        ref_time_str = datetime.now().strftime('%H시 %M분')
    
    report_text = f"""[{ref_time_str} 기준]

- 총인원: {total_cnt}명
 ㄴ 현 인원 (강의실에 현재 있는 인원): {not_left_cnt}명
 ㄴ 현재 강의장에 없는 인원: {total_cnt - not_left_cnt}명

<특이사항>
지각: {late_cnt}명, 조퇴: {early_cnt}명, 결석: {real_absent_cnt}명, 외출: {out_cnt}명
[지각] {get_names_str(df_monitor, 'ATEND_STATUS', '지각')}
[조퇴] {get_names_str(df_monitor, 'ATEND_STATUS', '조퇴')}
[외출] {get_names_str(df_monitor, 'ATEND_STATUS', '외출')}
[결석] {get_names_str(df_monitor, '', is_absent=True)}
"""
    # 텍스트 영역 표시
    st.text_area("보고 양식", report_text, height=250)

st.divider()

# [Row 3] 명단 상세 보기 (탭으로 구분)
t1, t2, t3 = st.tabs(["🚨 미퇴실/특이사항 명단", "❌ 결석자 명단", "📋 전체 출석부"])

with t1:
    st.markdown("##### 📢 현재 교실에 있거나(미퇴실), 지각/조퇴한 훈련생")
    issue_list = df_monitor[
        (df_monitor['OUT_TIME'].isna() & df_monitor['IN_TIME'].notna()) | 
        (df_monitor['ATEND_STATUS'].isin(['지각', '조퇴', '외출']))
    ].copy()
    
    if not issue_list.empty:
        issue_list['상태_요약'] = issue_list.apply(
            lambda x: '🟢 미퇴실(수업중)' if pd.isna(x['OUT_TIME']) and pd.notna(x['IN_TIME']) else x['ATEND_STATUS'], axis=1
        )
        st.dataframe(issue_list[['TRNEE_NM', 'IN_TIME', 'OUT_TIME', '상태_요약']], use_container_width=True, hide_index=True)
    else:
        st.success("특이사항이 있는 훈련생이 없습니다.")

with t2:
    st.markdown("##### 📞 연락이 필요한 결석(미출석) 훈련생")
    if not absent_students.empty:
        st.dataframe(absent_students[['TRNEE_NM', 'TRNEE_STATUS']], use_container_width=True, hide_index=True)
    else:
        st.success("전원 출석하였습니다! 🎉")

with t3:
    st.markdown("##### 📋 금일 전체 훈련생 출석 현황")
    st.dataframe(df_monitor[['TRNEE_NM', 'IN_TIME', 'OUT_TIME', 'ATEND_STATUS']], use_container_width=True, hide_index=True)