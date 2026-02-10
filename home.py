import streamlit as st
import pandas as pd
import altair as alt
from datetime import datetime
from utils import load_data, check_password

st.set_page_config(
    page_title="HRD 교육성과 대시보드",
    page_icon="🏠",
    layout="wide",
    initial_sidebar_state="expanded"
)

check_password()


@st.cache_data(ttl=600)
def get_dashboard_data():
    df_course = load_data("SELECT * FROM TB_COURSE_MASTER ORDER BY TR_STA_DT DESC")
    df_course['TR_STA_DT'] = pd.to_datetime(df_course['TR_STA_DT'])
    df_course['TR_END_DT'] = pd.to_datetime(df_course['TR_END_DT'])
    numeric_cols = ['TOT_FXNUM', 'TOT_PAR_MKS', 'EI_EMPL_RATE_3', 'EI_EMPL_RATE_6', 'HRD_EMPL_RATE_6', 'REAL_EMPL_RATE']
    for col in numeric_cols:
        df_course[col] = pd.to_numeric(df_course[col], errors='coerce').fillna(0)
    df_course['TOTAL_RATE_6'] = df_course['EI_EMPL_RATE_6'] + df_course['HRD_EMPL_RATE_6']
    df_course['모집률'] = df_course.apply(
        lambda x: (x['TOT_PAR_MKS'] / x['TOT_FXNUM'] * 100) if x['TOT_FXNUM'] > 0 else 0, axis=1
    )
    today = pd.Timestamp(datetime.now().date())
    df_course['상태'] = df_course['TR_END_DT'].apply(lambda x: '진행중' if x >= today else '종료')
    return df_course


@st.cache_data(ttl=300)
def get_today_attendance():
    """진행 중 과정의 오늘 출결 현황 요약"""
    today_str = datetime.now().strftime('%Y-%m-%d')
    # 진행 중 과정의 최신 출결 로그
    query = (
        "SELECT a.TRPR_DEGR, a.ATEND_STATUS, COUNT(*) as CNT "
        "FROM TB_ATTENDANCE_LOG a "
        "INNER JOIN TB_COURSE_MASTER c ON a.TRPR_ID = c.TRPR_ID AND a.TRPR_DEGR = c.TRPR_DEGR "
        "WHERE c.TR_END_DT >= ? "
        "AND a.ATEND_DT = (SELECT MAX(ATEND_DT) FROM TB_ATTENDANCE_LOG WHERE TRPR_DEGR = a.TRPR_DEGR) "
        "GROUP BY a.TRPR_DEGR, a.ATEND_STATUS"
    )
    return load_data(query, params=[today_str])


try:
    df = get_dashboard_data()
except Exception as e:
    st.error(f"데이터를 불러오는 중 오류가 발생했습니다. utils.py와 DB를 확인해주세요.\n{e}")
    st.stop()

# [헤더]
st.title("📊 HRD 교육성과 종합 대시보드")
st.markdown(f"**{datetime.now().strftime('%Y년 %m월 %d일')}** 기준 운영 현황입니다.")
st.divider()

# [Section 1] 핵심 지표 (KPI)
total_courses = len(df)
total_trainees = df['TOT_PAR_MKS'].sum()
avg_rate_3 = df[df['상태'] == '종료']['REAL_EMPL_RATE'].mean()
avg_rate_6 = df[df['상태'] == '종료']['TOTAL_RATE_6'].mean()
active_courses = len(df[df['상태'] == '진행중'])

kpi1, kpi2, kpi3, kpi4, kpi5 = st.columns(5)
kpi1.metric("총 운영 과정", f"{total_courses}개", delta=f"진행중 {active_courses}개")
kpi2.metric("누적 수강생", f"{total_trainees:,}명")
kpi3.metric("평균 모집률", f"{df['모집률'].mean():.1f}%")
kpi4.metric("평균 취업률(3개월)", f"{avg_rate_3:.1f}%", help="수료 후 3개월 고용보험 가입 기준")
kpi5.metric("평균 취업률(6개월)", f"{avg_rate_6:.1f}%", help="6개월 고용보험 + HRD자체취업 합산")
st.divider()

# [Section 1.5] 오늘의 출결 현황 (신규)
if active_courses > 0:
    st.subheader("📡 오늘의 출결 현황")
    try:
        attend_df = get_today_attendance()
        if not attend_df.empty:
            total_logs = attend_df['CNT'].sum()
            present = attend_df[attend_df['ATEND_STATUS'] == '출석']['CNT'].sum()
            absent = attend_df[attend_df['ATEND_STATUS'] == '결석']['CNT'].sum()
            late = attend_df[attend_df['ATEND_STATUS'] == '지각']['CNT'].sum()
            leave_early = attend_df[attend_df['ATEND_STATUS'] == '조퇴']['CNT'].sum()
            attendance_rate = (present / total_logs * 100) if total_logs > 0 else 0

            ac1, ac2, ac3, ac4, ac5 = st.columns(5)
            ac1.metric("출석률", f"{attendance_rate:.1f}%")
            ac2.metric("출석", f"{int(present)}명")
            ac3.metric("결석", f"{int(absent)}명", delta_color="inverse")
            ac4.metric("지각", f"{int(late)}명", delta_color="inverse")
            ac5.metric("조퇴", f"{int(leave_early)}명", delta_color="inverse")

            # 기수별 출석률 바차트
            degr_total = attend_df.groupby('TRPR_DEGR')['CNT'].sum().reset_index()
            degr_total.columns = ['TRPR_DEGR', '총건수']
            degr_present = attend_df[attend_df['ATEND_STATUS'] == '출석'].groupby('TRPR_DEGR')['CNT'].sum().reset_index()
            degr_present.columns = ['TRPR_DEGR', '출석건수']
            degr_rate = degr_total.merge(degr_present, on='TRPR_DEGR', how='left').fillna(0)
            degr_rate['출석률'] = (degr_rate['출석건수'] / degr_rate['총건수'] * 100).round(1)
            degr_rate['기수'] = degr_rate['TRPR_DEGR'].astype(str) + '회차'

            chart = alt.Chart(degr_rate).mark_bar().encode(
                x=alt.X('기수:N', title='기수'),
                y=alt.Y('출석률:Q', title='출석률 (%)', scale=alt.Scale(domain=[0, 100])),
                color=alt.condition(
                    alt.datum.출석률 < 80,
                    alt.value('#e74c3c'),
                    alt.value('#2ecc71'),
                ),
                tooltip=['기수', '출석률'],
            ).properties(height=200)
            st.altair_chart(chart, use_container_width=True)
        else:
            st.info("오늘 출결 데이터가 아직 수집되지 않았습니다.")
    except Exception:
        st.info("출결 데이터를 불러올 수 없습니다.")
    st.divider()

# [Section 2] 차트와 상세 테이블
col_left, col_right = st.columns([1, 1])

with col_left:
    st.subheader("📈 연도별 운영 규모")
    df['Year'] = df['TR_STA_DT'].dt.year
    year_counts = df.groupby('Year')['TRPR_NM'].count().reset_index(name='과정수')
    chart = alt.Chart(year_counts).mark_bar().encode(
        x=alt.X('Year:O', title='연도'),
        y=alt.Y('과정수:Q', title='운영 과정 수'),
        color=alt.value('#3182bd'),
        tooltip=['Year', '과정수'],
    ).properties(height=300)
    st.altair_chart(chart, use_container_width=True)

with col_right:
    st.subheader("🏆 우수 성과 과정 (Top 5)")
    st.caption("6개월 총 취업률 기준 상위 5개 과정입니다.")
    top_courses = df[df['상태'] == '종료'].sort_values(by='TOTAL_RATE_6', ascending=False).head(5)
    st.dataframe(
        top_courses[['TRPR_DEGR', 'TRPR_NM', 'TOTAL_RATE_6', 'FINI_CNT']],
        column_config={
            "TRPR_DEGR": "회차",
            "TRPR_NM": "과정명",
            "TOTAL_RATE_6": st.column_config.ProgressColumn("6개월 취업률", format="%.1f%%", min_value=0, max_value=100),
            "FINI_CNT": st.column_config.NumberColumn("수료생", format="%d명"),
        },
        hide_index=True,
        use_container_width=True,
    )

# [Section 3] 진행 중인 과정
st.subheader("🚨 현재 운영 중인 과정 현황")
active_df = df[df['상태'] == '진행중'].copy()
if not active_df.empty:
    st.dataframe(
        active_df[['TRPR_DEGR', 'TRPR_NM', 'TR_END_DT', 'TOT_PAR_MKS', '모집률']],
        column_config={
            "TRPR_DEGR": "회차",
            "TRPR_NM": "과정명",
            "TR_END_DT": st.column_config.DateColumn("종료예정일"),
            "TOT_PAR_MKS": st.column_config.NumberColumn("현재원", format="%d명"),
            "모집률": st.column_config.ProgressColumn("모집률", format="%.1f%%", min_value=0, max_value=100),
        },
        hide_index=True,
        use_container_width=True,
    )
else:
    st.info("현재 진행 중인 과정이 없습니다. 모든 과정이 종료되었습니다.")

with st.sidebar:
    st.info("좌측 메뉴를 선택하여 상세 분석 페이지로 이동하세요.")
    st.markdown("""
    * **📊 기수별 분석:** 종료된 과정의 성과 심층 분석
    * **🚨 진행과정 관리:** 현재 운영 중인 과정의 출결/이탈 관리
    * **🔎 데이터 감사:** 원본 데이터 확인 (Audit)
    * **📈 시장 동향:** 시장 전체 훈련과정 비교 분석
    """)
