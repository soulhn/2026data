import streamlit as st
import pandas as pd
import altair as alt
from datetime import datetime
from utils import load_data, check_password
from config import CACHE_TTL_DEFAULT, CACHE_TTL_REALTIME

st.set_page_config(
    page_title="HRD 교육성과 대시보드",
    page_icon="🏠",
    layout="wide",
    initial_sidebar_state="expanded"
)


@st.cache_data(ttl=CACHE_TTL_DEFAULT)
def get_dashboard_data():
    df_course = load_data("""
        SELECT TRPR_ID, TRPR_DEGR, TRPR_NM, TR_STA_DT, TR_END_DT,
               TOT_FXNUM, TOT_PAR_MKS, TOT_TRP_CNT, FINI_CNT,
               EI_EMPL_RATE_3, EI_EMPL_RATE_6, HRD_EMPL_RATE_6, REAL_EMPL_RATE
        FROM TB_COURSE_MASTER ORDER BY TR_STA_DT DESC
    """)
    df_course['TR_STA_DT'] = pd.to_datetime(df_course['TR_STA_DT'])
    df_course['TR_END_DT'] = pd.to_datetime(df_course['TR_END_DT'])
    numeric_cols = ['TOT_FXNUM', 'TOT_PAR_MKS', 'EI_EMPL_RATE_3', 'EI_EMPL_RATE_6', 'HRD_EMPL_RATE_6', 'REAL_EMPL_RATE']
    for col in numeric_cols:
        df_course[col] = pd.to_numeric(df_course[col], errors='coerce').fillna(0)
    df_course['TOTAL_RATE_6'] = df_course['EI_EMPL_RATE_6'] + df_course['HRD_EMPL_RATE_6']
    df_course['FINI_CNT'] = pd.to_numeric(df_course['FINI_CNT'], errors='coerce').fillna(0)
    df_course['TOT_TRP_CNT'] = pd.to_numeric(df_course['TOT_TRP_CNT'], errors='coerce').fillna(0)
    df_course['수료율'] = (df_course['FINI_CNT'] / df_course['TOT_PAR_MKS'].replace(0, pd.NA) * 100).fillna(0)
    today = pd.Timestamp(datetime.now().date())
    df_course['상태'] = df_course['TR_END_DT'].apply(lambda x: '진행중' if x >= today else '종료')
    return df_course


@st.cache_data(ttl=CACHE_TTL_REALTIME)
def get_today_attendance():
    """진행 중 과정의 오늘 출결 현황 요약 (입실중 재분류 포함)"""
    today_str = datetime.now().strftime('%Y-%m-%d')
    # IN_TIME이 있는데 결석인 경우 → '입실중'으로 재분류
    query = (
        "WITH latest AS ("
        "  SELECT TRPR_DEGR, MAX(ATEND_DT) AS MAX_DT"
        "  FROM TB_ATTENDANCE_LOG GROUP BY TRPR_DEGR"
        ") "
        "SELECT a.TRPR_DEGR, "
        "CASE WHEN a.ATEND_STATUS = '결석' AND a.IN_TIME IS NOT NULL AND a.IN_TIME != '' "
        "THEN '입실중' ELSE a.ATEND_STATUS END as ATEND_STATUS, "
        "COUNT(*) as CNT "
        "FROM TB_ATTENDANCE_LOG a "
        "INNER JOIN TB_COURSE_MASTER c ON a.TRPR_ID = c.TRPR_ID AND a.TRPR_DEGR = c.TRPR_DEGR "
        "INNER JOIN latest ld ON a.TRPR_DEGR = ld.TRPR_DEGR AND a.ATEND_DT = ld.MAX_DT "
        "WHERE c.TR_END_DT >= ? "
        "GROUP BY a.TRPR_DEGR, "
        "CASE WHEN a.ATEND_STATUS = '결석' AND a.IN_TIME IS NOT NULL AND a.IN_TIME != '' "
        "THEN '입실중' ELSE a.ATEND_STATUS END"
    )
    return load_data(query, params=[today_str])


def _render_project_intro():
    st.markdown("## HRD 교육성과 분석 시스템")
    st.markdown("`Python` `Streamlit` `PostgreSQL` `GitHub Actions`")
    st.caption("한화시스템 BEYOND SW캠프 | K-Digital Training B2G 교육사업 | 2023.07 ~ 현재")
    st.divider()

    col1, col2 = st.columns(2)
    with col1:
        st.markdown("**🔴 해결한 문제**")
        st.markdown("""
- 출결·수료 데이터 수작업 집계 → 보고 **반나절 소요**
- 위험군(이탈 징후) 감지 **수일 지연**
- 수강생 1명 탈락 시 최대 **1,742만원** 매출 손실
        """)
    with col2:
        st.markdown("**🟢 구축한 것**")
        st.markdown("""
- HRD-Net API 자동 ETL (GitHub Actions, 평일 매시간)
- 위험군 **당일 자동 감지** · 즉시 대응 체계
- 전국 KDT **30만건+** 기반 시장 경쟁 분석
        """)

    st.divider()

    m1, m2, m3, m4, m5 = st.columns(5)
    m1.metric("총 운영 기수", "25기", "2023.07 ~ 현재")
    m2.metric("전국 순위 (2023)", "10위", "300개 중 · 상위 3.3%")
    m3.metric("전국 순위 (2024)", "14위", "611개 중 · 상위 2.3%")
    m4.metric("전국 순위 (2025)", "22위", "561개 중 · 상위 3.9%")
    m5.metric("부서 영업이익", "13.3억", "+325%")

    st.divider()


def render_dashboard():
    _render_project_intro()
    check_password()

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
    avg_completion = df[df['상태'] == '종료']['수료율'].mean()
    kpi3.metric("평균 수료율", f"{avg_completion:.1f}%", help="수료인원 / 수강인원 기준")
    kpi4.metric("평균 취업률(3개월)", f"{avg_rate_3:.1f}%", help="수료 후 3개월 고용보험 가입 기준")
    kpi5.metric("평균 취업률(6개월)", f"{avg_rate_6:.1f}%", help="6개월 고용보험 + HRD자체취업 합산")
    st.divider()

    # [Section 2] 시장 포지셔닝
    st.subheader("📍 전국 KDT 시장 포지셔닝")
    st.caption("직업능력심사평가원 KDT 훈련과정 성과평가 기준 | 전국 동일 NCS(정보통신) 과정 대상")

    r1, r2, r3 = st.columns(3)
    with r1:
        st.metric("2023년 전국 순위", "10위 / 300개", "상위 3.3%", delta_color="off")
    with r2:
        st.metric("2024년 전국 순위", "14위 / 611개", "상위 2.3%", delta_color="off")
    with r3:
        st.metric("2025년 전국 순위", "22위 / 561개", "상위 3.9%", delta_color="off")

    rank_df = pd.DataFrame([
        {"연도": "2023", "상위(%)": round(10/300*100, 1), "label": "10위"},
        {"연도": "2024", "상위(%)": 2.3, "label": "14위"},
        {"연도": "2025", "상위(%)": 3.9, "label": "22위"},
    ])
    chart = alt.Chart(rank_df).mark_bar(width=60).encode(
        x=alt.X("연도:N", title="연도", axis=alt.Axis(labelFontSize=13)),
        y=alt.Y("상위(%):Q", title="상위 % (낮을수록 우수)", scale=alt.Scale(domain=[0, 10])),
        color=alt.value("#2ecc71"),
        tooltip=[alt.Tooltip("연도:N"), alt.Tooltip("label:N", title="순위"), alt.Tooltip("상위(%):Q", title="상위 %", format=".1f")],
    ).properties(height=180)
    text = chart.mark_text(align="center", dy=-10, fontSize=13, fontWeight="bold").encode(
        text=alt.Text("label:N")
    )
    st.altair_chart((chart + text), use_container_width=True)
    st.divider()

    # [Section 3] 오늘의 출결 현황 (신규)
    if active_courses > 0:
        st.subheader("📡 오늘의 출결 현황")
        try:
            attend_df = get_today_attendance()
            if not attend_df.empty:
                total_logs = attend_df['CNT'].sum()
                present = attend_df[attend_df['ATEND_STATUS'] == '출석']['CNT'].sum()
                in_class = attend_df[attend_df['ATEND_STATUS'] == '입실중']['CNT'].sum()
                absent = attend_df[attend_df['ATEND_STATUS'] == '결석']['CNT'].sum()
                late = attend_df[attend_df['ATEND_STATUS'] == '지각']['CNT'].sum()
                leave_early = attend_df[attend_df['ATEND_STATUS'] == '조퇴']['CNT'].sum()
                # 출석률: 출석 + 입실중 + 지각을 출석으로 간주
                effective_present = present + in_class + late
                attendance_rate = (effective_present / total_logs * 100) if total_logs > 0 else 0

                ac1, ac2, ac3, ac4, ac5, ac6 = st.columns(6)
                ac1.metric("출석률", f"{attendance_rate:.1f}%", help="출석 + 입실중 + 지각 기준")
                ac2.metric("출석", f"{int(present)}명")
                ac3.metric("입실중", f"{int(in_class)}명", help="입실 완료, 퇴실 전 (수업 중)")
                ac4.metric("결석", f"{int(absent)}명", delta_color="inverse")
                ac5.metric("지각", f"{int(late)}명", delta_color="inverse")
                ac6.metric("조퇴", f"{int(leave_early)}명", delta_color="inverse")

                # 기수별 출석률 바차트 (입실중도 출석으로 간주)
                degr_total = attend_df.groupby('TRPR_DEGR')['CNT'].sum().reset_index()
                degr_total.columns = ['TRPR_DEGR', '총건수']
                degr_present = attend_df[attend_df['ATEND_STATUS'].isin(['출석', '입실중', '지각'])].groupby('TRPR_DEGR')['CNT'].sum().reset_index()
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

    # [Section 4] 차트와 상세 테이블
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

    # [Section 5] 진행 중인 과정
    st.subheader("🚨 현재 운영 중인 과정 현황")
    active_df = df[df['상태'] == '진행중'].copy()
    if not active_df.empty:
        # 제적/중도탈락 인원 집계
        trainee_stats = load_data("""
            SELECT TRPR_ID, TRPR_DEGR,
                   SUM(CASE WHEN TRNEE_STATUS = '제적' THEN 1 ELSE 0 END) AS EXPEL_CNT,
                   SUM(CASE WHEN TRNEE_STATUS = '중도탈락' THEN 1 ELSE 0 END) AS DROP_CNT
            FROM TB_TRAINEE_INFO
            GROUP BY TRPR_ID, TRPR_DEGR
        """)
        active_df = active_df.merge(trainee_stats, on=['TRPR_ID', 'TRPR_DEGR'], how='left')
        for c in ['EXPEL_CNT', 'DROP_CNT']:
            active_df[c] = pd.to_numeric(active_df[c], errors='coerce').fillna(0).astype(int)
        active_df['CURRENT_CNT'] = (active_df['TOT_PAR_MKS'] - active_df['EXPEL_CNT'] - active_df['DROP_CNT']).astype(int)
        active_df['잔여율'] = (active_df['CURRENT_CNT'] / active_df['TOT_PAR_MKS'].replace(0, pd.NA) * 100).fillna(0)
        st.dataframe(
            active_df[['TRPR_DEGR', 'TRPR_NM', 'TR_END_DT', 'TOT_TRP_CNT', 'TOT_PAR_MKS', 'EXPEL_CNT', 'DROP_CNT', 'CURRENT_CNT', '잔여율']],
            column_config={
                "TRPR_DEGR": "회차",
                "TRPR_NM": "과정명",
                "TR_END_DT": st.column_config.DateColumn("종료예정일"),
                "TOT_TRP_CNT": st.column_config.NumberColumn("수강신청", format="%d명"),
                "TOT_PAR_MKS": st.column_config.NumberColumn("개강인원", format="%d명"),
                "EXPEL_CNT": st.column_config.NumberColumn("제적", format="%d명"),
                "DROP_CNT": st.column_config.NumberColumn("중도탈락", format="%d명"),
                "CURRENT_CNT": st.column_config.NumberColumn("현재인원", format="%d명"),
                "잔여율": st.column_config.ProgressColumn("잔여율", format="%.1f%%", min_value=0, max_value=100),
            },
            hide_index=True,
            use_container_width=True,
        )
    else:
        st.info("현재 진행 중인 과정이 없습니다. 모든 과정이 종료되었습니다.")

    with st.sidebar:
        st.info("좌측 메뉴를 선택하여 상세 분석 페이지로 이동하세요.")
        st.markdown("""
* **📈 시장 분석 & 기회 발굴:** 시장 전체 훈련과정 비교 분석
* **📊 과정 성과 분석:** 종료된 과정의 성과 심층 분석
* **📋 운영 현황:** 현재 운영 중인 과정의 출결/이탈 관리
* **💰 매출 분석:** 단위기간별 훈련비 청구 현황
* **🔎 데이터 조회:** 원본 데이터 확인
        """)


pg = st.navigation([
    st.Page(render_dashboard, title="성과 대시보드", icon="🏠"),
    st.Page("pages/1_📈_시장_분석.py", title="시장 분석 & 기회 발굴", icon="📈"),
    st.Page("pages/2_📊_과정_성과_분석.py", title="과정 성과 분석", icon="📊"),
    st.Page("pages/3_📋_운영_현황.py", title="운영 현황", icon="📋"),
    st.Page("pages/4_💰_매출_분석.py", title="매출 분석", icon="💰"),
    st.Page("pages/5_🔎_데이터_조회.py", title="데이터 조회", icon="🔎"),
])
pg.run()
