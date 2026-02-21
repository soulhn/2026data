import streamlit as st
import pandas as pd
import altair as alt
from datetime import datetime, timedelta
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils import load_data, check_password
from config import (
    CACHE_TTL_REALTIME, LATE_CUTOFF_HHMM, ATTENDANCE_TARGET,
    RISK_ABSENT, RISK_LATE, RISK_EARLY_LEAVE, RECENT_TREND_DAYS,
)

st.set_page_config(page_title="운영 현황", page_icon="📋", layout="wide")
check_password()
st.title("📋 운영 현황")
st.markdown("현재 운영 중인 과정의 **실시간 출결 현황** (입/퇴실)과 **특이사항**을 집중 모니터링합니다.")


@st.cache_data(ttl=CACHE_TTL_REALTIME)
def get_active_data():
    today_str = datetime.now().strftime('%Y-%m-%d')
    query_course = (
        "SELECT * FROM TB_COURSE_MASTER "
        "WHERE TR_END_DT >= ? ORDER BY TR_STA_DT"
    )
    active_courses = load_data(query_course, params=[today_str])
    if active_courses.empty:
        return None, None, None

    course_ids = ",".join([f"'{x}'" for x in active_courses['TRPR_ID'].unique()])
    degrs = ",".join([str(x) for x in active_courses['TRPR_DEGR'].unique()])

    query_trainee = f"SELECT * FROM TB_TRAINEE_INFO WHERE TRPR_ID IN ({course_ids}) AND TRPR_DEGR IN ({degrs})"
    active_trainees = load_data(query_trainee)

    query_log = f"SELECT * FROM TB_ATTENDANCE_LOG WHERE TRPR_ID IN ({course_ids}) AND TRPR_DEGR IN ({degrs}) ORDER BY ATEND_DT DESC"
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
# [0] 전체 기수 오늘의 출결 현황
# ==========================================
st.subheader("📡 전체 기수 오늘의 출결 현황")
st.caption("운영 중인 모든 기수의 최근 출결 기준 합산 현황입니다. (입실 후 미퇴실 → 입실중 재분류)")

all_today_rows = []
for degr in courses_df['TRPR_DEGR'].unique():
    degr_logs = logs_df[logs_df['TRPR_DEGR'] == degr]
    if degr_logs.empty:
        continue
    latest_date = degr_logs['ATEND_DT'].max()
    day = degr_logs[degr_logs['ATEND_DT'] == latest_date].copy()
    day['DISPLAY_STATUS'] = day.apply(
        lambda r: '입실중'
        if r['ATEND_STATUS'] == '결석' and pd.notna(r['IN_TIME']) and str(r['IN_TIME']).strip() != ''
        else r['ATEND_STATUS'],
        axis=1,
    )
    day['TRPR_DEGR_KEY'] = degr
    all_today_rows.append(day[['TRPR_DEGR_KEY', 'TRNEE_ID', 'DISPLAY_STATUS']])

if all_today_rows:
    all_df = pd.concat(all_today_rows, ignore_index=True)
    total_logs = len(all_df)
    present   = (all_df['DISPLAY_STATUS'] == '출석').sum()
    in_class  = (all_df['DISPLAY_STATUS'] == '입실중').sum()
    absent    = (all_df['DISPLAY_STATUS'] == '결석').sum()
    late      = (all_df['DISPLAY_STATUS'] == '지각').sum()
    leave_early = (all_df['DISPLAY_STATUS'] == '조퇴').sum()
    att_rate  = (present + in_class + late) / total_logs * 100 if total_logs > 0 else 0

    ac1, ac2, ac3, ac4, ac5, ac6 = st.columns(6)
    ac1.metric("출석률", f"{att_rate:.1f}%", help="출석 + 입실중 + 지각 기준")
    ac2.metric("출석", f"{int(present)}명")
    ac3.metric("입실중", f"{int(in_class)}명", help="입실 완료, 퇴실 전 (수업 중)")
    ac4.metric("결석", f"{int(absent)}명", delta_color="inverse")
    ac5.metric("지각", f"{int(late)}명", delta_color="inverse")
    ac6.metric("조퇴", f"{int(leave_early)}명", delta_color="inverse")

    degr_stats = all_df.groupby('TRPR_DEGR_KEY').apply(
        lambda g: pd.Series({
            '총건수': len(g),
            '출석건수': g['DISPLAY_STATUS'].isin(['출석', '입실중', '지각']).sum(),
        })
    ).reset_index()
    degr_stats['출석률'] = (degr_stats['출석건수'] / degr_stats['총건수'] * 100).round(1)
    degr_stats['기수'] = degr_stats['TRPR_DEGR_KEY'].astype(str) + '회차'
    chart = alt.Chart(degr_stats).mark_bar().encode(
        x=alt.X('기수:N', title='기수'),
        y=alt.Y('출석률:Q', title='출석률 (%)', scale=alt.Scale(domain=[0, 100])),
        color=alt.condition(alt.datum.출석률 < 80, alt.value('#e74c3c'), alt.value('#2ecc71')),
        tooltip=['기수', '출석률'],
    ).properties(height=180)
    st.altair_chart(chart, use_container_width=True)
else:
    st.info("오늘 출결 데이터가 아직 수집되지 않았습니다.")

st.divider()
st.subheader("📌 기수별 상세 현황")

with st.sidebar:
    st.header("🎯 관리 대상 선택")
    selected_degr = st.selectbox(
        "관리할 회차(기수) 선택",
        courses_df['TRPR_DEGR'].unique(),
        format_func=lambda x: f"{x}회차 ({courses_df[courses_df['TRPR_DEGR']==x]['TRPR_NM'].iloc[0]})",
    )
    st.divider()
    if st.button("🔄 데이터 새로고침"):
        st.cache_data.clear()
        st.rerun()
    st.caption("💡 '미퇴실'은 입실은 했으나 퇴실 기록이 없는 상태입니다.")

this_course = courses_df[courses_df['TRPR_DEGR'] == selected_degr].iloc[0]
this_students_all = trainees_df[trainees_df['TRPR_DEGR'] == selected_degr].copy()
active_students = this_students_all[~this_students_all['TRNEE_STATUS'].isin(['중도탈락', '제적'])].copy()
this_logs = logs_df[logs_df['TRPR_DEGR'] == selected_degr].copy()

# ==========================================
# 실시간 출결 집계 로직
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
    how='left',
)


def apply_late_rule(row):
    current_status = row['ATEND_STATUS']
    in_time = row['IN_TIME']
    if str(current_status).strip() in ['조퇴', '외출']:
        return current_status
    if str(current_status).strip() == '지각':
        return '지각'
    if pd.notna(in_time):
        time_digits = ''.join(filter(str.isdigit, str(in_time)))
        if len(time_digits) >= 3:
            try:
                time_val = int(time_digits[:4])
                if time_val > LATE_CUTOFF_HHMM:
                    return '지각'
            except:
                pass
    return current_status


df_monitor['ATEND_STATUS'] = df_monitor.apply(apply_late_rule, axis=1)

total_cnt = len(active_students)
present_cnt = len(df_monitor[df_monitor['IN_TIME'].notna()])
not_left_cnt = len(df_monitor[(df_monitor['IN_TIME'].notna()) & (df_monitor['OUT_TIME'].isna())])
late_cnt = len(df_monitor[df_monitor['ATEND_STATUS'] == '지각'])
early_cnt = len(df_monitor[df_monitor['ATEND_STATUS'] == '조퇴'])
out_cnt = len(df_monitor[df_monitor['ATEND_STATUS'] == '외출'])
absent_students = df_monitor[df_monitor['IN_TIME'].isna()]
real_absent_cnt = len(absent_students)
attendance_rate = (present_cnt / total_cnt * 100) if total_cnt > 0 else 0

# ==========================================
# [1] 출석률 + KPI
# ==========================================
st.subheader(f"📌 {selected_degr}회차 실시간 현황 ({target_date} 기준)")
d_day = (pd.to_datetime(this_course['TR_END_DT']) - pd.to_datetime(datetime.now().date())).days
st.info(f"**과정명:** {this_course['TRPR_NM']} (D-{d_day})")
st.divider()

gauge_col, kpi_col = st.columns([1, 3])
with gauge_col:
    st.markdown(f"### 출석률 **{attendance_rate:.1f}%**")
    st.caption(f"{present_cnt}/{total_cnt}명 출석")

with kpi_col:
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("총 재원", f"{total_cnt}명")
    c2.metric("금일 입실", f"{present_cnt}명")
    c3.metric("미퇴실", f"{not_left_cnt}명", delta_color="off")
    c4.metric("결석/미출석", f"{real_absent_cnt}명", delta_color="inverse")

    c5, c6, c7, c8 = st.columns(4)
    c5.metric("지각", f"{late_cnt}명", delta_color="inverse")
    c6.metric("조퇴", f"{early_cnt}명", delta_color="inverse")
    c7.metric("외출", f"{out_cnt}명")
    c8.metric("퇴실 완료", f"{present_cnt - not_left_cnt}명")

st.divider()

# ==========================================
# [2] 보고용 텍스트
# ==========================================
with st.expander("📝 보고용 텍스트 복사", expanded=True):
    def get_names_str(df, type_):
        names = []
        if type_ == 'absent':
            names = df[df['IN_TIME'].isna()]['TRNEE_NM'].tolist()
        elif type_ == 'not_left':
            names = df[(df['IN_TIME'].notna()) & (df['OUT_TIME'].isna())]['TRNEE_NM'].tolist()
        elif type_ == 'late':
            target = df[df['ATEND_STATUS'] == '지각']
            for _, row in target.iterrows():
                clean_time = str(row['IN_TIME']).strip()
                names.append(f"{row['TRNEE_NM']}({clean_time})")
        elif type_ == 'early':
            target = df[df['ATEND_STATUS'] == '조퇴']
            for _, row in target.iterrows():
                clean_time = str(row['OUT_TIME']).strip() if pd.notna(row['OUT_TIME']) else ''
                if clean_time:
                    names.append(f"{row['TRNEE_NM']}({clean_time})")
                else:
                    names.append(row['TRNEE_NM'])
        elif type_ == 'out':
            names = df[df['ATEND_STATUS'] == '외출']['TRNEE_NM'].tolist()
        return ", ".join(names) if names else "없음"

    last_collect = (
        pd.to_datetime(this_logs['COLLECTED_AT']).max() + timedelta(hours=9)
        if 'COLLECTED_AT' in this_logs.columns and not this_logs.empty
        else datetime.now()
    )
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

# ==========================================
# [3] 최근 출결 추이
# ==========================================
if not this_logs.empty:
    recent_dates = sorted(this_logs['ATEND_DT'].unique())[-RECENT_TREND_DAYS:]
    weekly_data = []
    for dt in recent_dates:
        day_logs = this_logs[this_logs['ATEND_DT'] == dt]
        day_total = len(day_logs['TRNEE_ID'].unique())
        day_present = len(day_logs[day_logs['IN_TIME'].notna()]['TRNEE_ID'].unique())
        day_rate = (day_present / day_total * 100) if day_total > 0 else 0
        weekly_data.append({'날짜': dt, '출석률': round(day_rate, 1), '출석': day_present, '총원': day_total})

    if weekly_data:
        weekly_df = pd.DataFrame(weekly_data)
        st.markdown("##### 📈 최근 출결 추이")
        st.caption(f"최근 {RECENT_TREND_DAYS}일간 일별 출석률 변화입니다. 빨간 점선은 {ATTENDANCE_TARGET}% 기준선입니다.")
        line = alt.Chart(weekly_df).mark_line(point=True, color='#3498db').encode(
            x=alt.X('날짜:N', title='날짜'),
            y=alt.Y('출석률:Q', title='출석률 (%)', scale=alt.Scale(domain=[50, 100])),
            tooltip=['날짜', '출석률', '출석', '총원'],
        ).properties(height=200)
        rule = alt.Chart(pd.DataFrame({'y': [ATTENDANCE_TARGET]})).mark_rule(strokeDash=[5, 5], color='red').encode(y='y:Q')
        st.altair_chart(line + rule, use_container_width=True)
    st.divider()

# ==========================================
# [4] 누적 출결 위험 지표
# ==========================================
if not this_logs.empty:
    st.markdown("##### ⚠️ 누적 출결 위험 지표")

    # 과정 시작일부터 현재까지의 전체 출결 기록 기준
    total_dates = sorted(this_logs['ATEND_DT'].unique())
    total_days_cnt = len(total_dates)

    st.caption(
        f"**집계 기간:** 과정 전체 기간 ({total_dates[0]} ~ {total_dates[-1]}, 총 {total_days_cnt}일)  \n"
        "**위험군 기준:** 아래 조건 중 하나 이상 해당 시 위험군으로 분류  \n"
        f"- 누적 결석 **{RISK_ABSENT}회 이상** (수료 기준 출석률 80% 미달 위험)  \n"
        f"- 누적 지각 **{RISK_LATE}회 이상** (습관적 지각 패턴)  \n"
        f"- 누적 조퇴 **{RISK_EARLY_LEAVE}회 이상** (조기 이탈 패턴)"
    )

    cumul = this_logs.groupby('TRNEE_ID').agg(
        누적_결석=('ATEND_STATUS', lambda x: (x == '결석').sum()),
        누적_지각=('ATEND_STATUS', lambda x: (x == '지각').sum()),
        누적_조퇴=('ATEND_STATUS', lambda x: (x == '조퇴').sum()),
        누적_출석=('ATEND_STATUS', lambda x: (x == '출석').sum()),
        총_기록=('ATEND_DT', 'count'),
    ).reset_index()
    cumul = cumul.merge(active_students[['TRNEE_ID', 'TRNEE_NM']], on='TRNEE_ID', how='inner')
    cumul['출석률(%)'] = (cumul['누적_출석'] / cumul['총_기록'] * 100).round(1)

    risk_mask = (cumul['누적_결석'] >= RISK_ABSENT) | (cumul['누적_지각'] >= RISK_LATE) | (cumul['누적_조퇴'] >= RISK_EARLY_LEAVE)
    risk_df = cumul[risk_mask].sort_values('누적_결석', ascending=False)

    if not risk_df.empty:
        rc1, rc2 = st.columns([1, 3])
        with rc1:
            st.metric("위험군 인원", f"{len(risk_df)}명", delta_color="inverse")
        with rc2:
            st.dataframe(
                risk_df[['TRNEE_NM', '누적_결석', '누적_지각', '누적_조퇴', '총_기록', '출석률(%)']],
                column_config={
                    'TRNEE_NM': '이름',
                    '누적_결석': st.column_config.NumberColumn('결석', format="%d회"),
                    '누적_지각': st.column_config.NumberColumn('지각', format="%d회"),
                    '누적_조퇴': st.column_config.NumberColumn('조퇴', format="%d회"),
                    '총_기록': st.column_config.NumberColumn('총 기록일', format="%d일"),
                    '출석률(%)': st.column_config.ProgressColumn('출석률', min_value=0, max_value=100, format="%.1f%%"),
                },
                use_container_width=True,
                hide_index=True,
            )
    else:
        st.success("현재 출결 위험군이 없습니다. 👍")
    st.divider()

# ==========================================
# 상세 탭
# ==========================================
t1, t2, t3 = st.tabs(["🚨 미퇴실/특이사항", "❌ 결석자", "📋 전체 출석부"])

with t1:
    issue_list = df_monitor[
        (df_monitor['OUT_TIME'].isna() & df_monitor['IN_TIME'].notna())
        | (df_monitor['ATEND_STATUS'].isin(['지각', '조퇴', '외출']))
    ].copy()
    if not issue_list.empty:
        issue_list['상태_요약'] = issue_list.apply(
            lambda x: '🟢 미퇴실(수업중)' if pd.isna(x['OUT_TIME']) and pd.notna(x['IN_TIME']) else x['ATEND_STATUS'],
            axis=1,
        )
        st.dataframe(issue_list[['TRNEE_NM', 'IN_TIME', 'OUT_TIME', '상태_요약']], use_container_width=True, hide_index=True)
    else:
        st.success("특이사항 없음")

with t2:
    if not absent_students.empty:
        st.dataframe(absent_students[['TRNEE_NM', 'TRNEE_STATUS']], use_container_width=True, hide_index=True)
    else:
        st.success("전원 출석! 🎉")

with t3:
    st.dataframe(df_monitor[['TRNEE_NM', 'IN_TIME', 'OUT_TIME', 'ATEND_STATUS']], use_container_width=True, hide_index=True)
