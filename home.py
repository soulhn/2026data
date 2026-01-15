import streamlit as st
import pandas as pd
import altair as alt
from datetime import datetime
from utils import load_data  # utils.py에서 공통 함수 불러오기

# ==========================================
# 1. 페이지 설정
# ==========================================
st.set_page_config(
    page_title="HRD 교육성과 대시보드",
    page_icon="🏠",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ==========================================
# 2. 데이터 로드 및 전처리
# ==========================================
@st.cache_data
def get_dashboard_data():
    # 과정 마스터 정보 가져오기
    df_course = load_data("SELECT * FROM TB_COURSE_MASTER ORDER BY TR_STA_DT DESC")
    
    # [전처리] 날짜 변환
    df_course['TR_STA_DT'] = pd.to_datetime(df_course['TR_STA_DT'])
    df_course['TR_END_DT'] = pd.to_datetime(df_course['TR_END_DT'])
    
    # [전처리] 수치형 변환
    numeric_cols = ['TOT_FXNUM', 'TOT_PAR_MKS', 'EI_EMPL_RATE_3', 'EI_EMPL_RATE_6', 'HRD_EMPL_RATE_6', 'REAL_EMPL_RATE']
    for col in numeric_cols:
        df_course[col] = pd.to_numeric(df_course[col], errors='coerce').fillna(0)

    # [전처리] 6개월 총 취업률 (EI + HRD)
    df_course['TOTAL_RATE_6'] = df_course['EI_EMPL_RATE_6'] + df_course['HRD_EMPL_RATE_6']
    
    # [전처리] 모집률 (%)
    df_course['모집률'] = df_course.apply(lambda x: (x['TOT_PAR_MKS'] / x['TOT_FXNUM'] * 100) if x['TOT_FXNUM'] > 0 else 0, axis=1)

    # [전처리] 현재 운영 상태 (진행중 vs 종료)
    today = pd.Timestamp(datetime.now().date())
    df_course['상태'] = df_course['TR_END_DT'].apply(lambda x: '진행중' if x >= today else '종료')

    return df_course

try:
    df = get_dashboard_data()
except Exception as e:
    st.error(f"데이터를 불러오는 중 오류가 발생했습니다. utils.py와 DB를 확인해주세요.\n{e}")
    st.stop()

# ==========================================
# 3. 메인 화면 UI
# ==========================================

# [헤더]
st.title("📊 HRD 교육성과 종합 대시보드")
st.markdown(f"**{datetime.now().strftime('%Y년 %m월 %d일')}** 기준 운영 현황입니다.")

st.divider()

# [Section 1] 핵심 지표 (KPI)
# 전체 누적 데이터 기준
total_courses = len(df)
total_trainees = df['TOT_PAR_MKS'].sum()
avg_rate_3 = df[df['상태']=='종료']['REAL_EMPL_RATE'].mean() # 종료된 과정만 평균
avg_rate_6 = df[df['상태']=='종료']['TOTAL_RATE_6'].mean()   # 종료된 과정만 평균

# 현재 진행 중인 과정 수
active_courses = len(df[df['상태'] == '진행중'])

kpi1, kpi2, kpi3, kpi4, kpi5 = st.columns(5)
kpi1.metric("총 운영 과정", f"{total_courses}개", delta=f"진행중 {active_courses}개")
kpi2.metric("누적 수강생", f"{total_trainees:,}명")
kpi3.metric("평균 모집률", f"{df['모집률'].mean():.1f}%")
kpi4.metric("평균 취업률(3개월)", f"{avg_rate_3:.1f}%", help="수료 후 3개월 고용보험 가입 기준")
kpi5.metric("평균 취업률(6개월)", f"{avg_rate_6:.1f}%", help="6개월 고용보험 + HRD자체취업 합산")

st.divider()

# [Section 2] 차트와 상세 테이블
col_left, col_right = st.columns([1, 1])

with col_left:
    st.subheader("📈 연도별 운영 규모")
    # 연도 추출
    df['Year'] = df['TR_STA_DT'].dt.year
    year_counts = df.groupby('Year')['TRPR_NM'].count().reset_index(name='과정수')
    
    # Altair 차트
    chart = alt.Chart(year_counts).mark_bar().encode(
        x=alt.X('Year:O', title='연도'),
        y=alt.Y('과정수:Q', title='운영 과정 수'),
        color=alt.value('#3182bd'),
        tooltip=['Year', '과정수']
    ).properties(height=300)
    st.altair_chart(chart, use_container_width=True)

with col_right:
    st.subheader("🏆 우수 성과 과정 (Top 5)")
    st.caption("6개월 총 취업률 기준 상위 5개 과정입니다.")
    
    # 취업률 높은 순 정렬
    top_courses = df[df['상태']=='종료'].sort_values(by='TOTAL_RATE_6', ascending=False).head(5)
    
    st.dataframe(
        top_courses[['TRPR_DEGR', 'TRPR_NM', 'TOTAL_RATE_6', 'FINI_CNT']],
        column_config={
            "TRPR_DEGR": "회차",
            "TRPR_NM": "과정명",
            "TOTAL_RATE_6": st.column_config.ProgressColumn("6개월 취업률", format="%.1f%%", min_value=0, max_value=100),
            "FINI_CNT": st.column_config.NumberColumn("수료생", format="%d명")
        },
        hide_index=True,
        use_container_width=True
    )

# [Section 3] 진행 중인 과정 속보
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
        use_container_width=True
    )
else:
    st.info("현재 진행 중인 과정이 없습니다. 모든 과정이 종료되었습니다.")

# [사이드바 안내]
with st.sidebar:
    st.info("좌측 메뉴를 선택하여 상세 분석 페이지로 이동하세요.")
    st.markdown("""
    * **📊 기수별 분석:** 종료된 과정의 성과 심층 분석
    * **🚨 진행과정 관리:** 현재 운영 중인 과정의 출결/이탈 관리
    * **🔎 데이터 감사:** 원본 데이터 확인 (Audit)
    """)