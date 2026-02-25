import json
import streamlit as st
import pandas as pd
import altair as alt
from datetime import datetime
from utils import (
    load_data, load_cache_json, check_password,
    calc_attendance_rate, NOT_ATTEND_STATUSES, _attendance_penalty,
    calc_employment_rate_6,
)
from config import CACHE_TTL_DEFAULT, CACHE_TTL_REALTIME, CacheKey

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
               EI_EMPL_RATE_3, EI_EMPL_RATE_6, HRD_EMPL_RATE_6
        FROM TB_COURSE_MASTER ORDER BY TR_STA_DT DESC
    """)
    df_course['TR_STA_DT'] = pd.to_datetime(df_course['TR_STA_DT'])
    df_course['TR_END_DT'] = pd.to_datetime(df_course['TR_END_DT'])
    for col in ['TOT_FXNUM', 'TOT_PAR_MKS']:
        df_course[col] = pd.to_numeric(
            df_course[col], errors='coerce').fillna(0)
    # 취업률: NaN 유지 (상태코드 'A'=개설예정 'B'=집계중 'C'=미실시 'D'=수료자없음 → 0과 구분)
    for col in ['EI_EMPL_RATE_3', 'EI_EMPL_RATE_6', 'HRD_EMPL_RATE_6']:
        df_course[col] = pd.to_numeric(df_course[col], errors='coerce')
    # 6개월 총 취업률 = EI_6 + HRD_6 합산 (특수코드/둘다결측 → NA)
    df_course['TOTAL_RATE_6'] = df_course.apply(
        lambda r: calc_employment_rate_6(r['EI_EMPL_RATE_6'], r['HRD_EMPL_RATE_6']), axis=1
    )
    df_course['FINI_CNT'] = pd.to_numeric(
        df_course['FINI_CNT'], errors='coerce').fillna(0)
    df_course['TOT_TRP_CNT'] = pd.to_numeric(
        df_course['TOT_TRP_CNT'], errors='coerce').fillna(0)
    df_course['수료율'] = (df_course['FINI_CNT'] /
                        df_course['TOT_PAR_MKS'].replace(0, pd.NA) * 100).fillna(0)
    today = pd.Timestamp(datetime.now().date())
    df_course['상태'] = df_course['TR_END_DT'].apply(
        lambda x: '진행중' if x >= today else '종료')
    return df_course


@st.cache_data(ttl=CACHE_TTL_DEFAULT)
def get_attendance_stats():
    """종료된 기수별 출결 통계 집계.

    ETL 사전 집계 캐시(TB_MARKET_CACHE.attendance_stats) 우선 사용,
    캐시 미적중 시 기존 로직으로 fallback.
    """
    # 캐시 조회
    try:
        cache_df = load_data(
            "SELECT CACHE_DATA FROM TB_MARKET_CACHE WHERE CACHE_KEY = ?",
            params=[CacheKey.ATTENDANCE_STATS],
        )
        if not cache_df.empty and cache_df['CACHE_DATA'].iloc[0]:
            rows = json.loads(cache_df['CACHE_DATA'].iloc[0])
            if rows:
                return pd.DataFrame(rows)
    except Exception:
        pass

    # fallback: 기존 로직
    today_str = datetime.now().strftime('%Y-%m-%d')
    att_df = load_data(
        "SELECT a.TRPR_DEGR, a.TRNEE_ID, a.ATEND_STATUS, a.ATEND_DT "
        "FROM TB_ATTENDANCE_LOG a "
        "INNER JOIN TB_COURSE_MASTER c ON a.TRPR_ID = c.TRPR_ID AND a.TRPR_DEGR = c.TRPR_DEGR "
        "WHERE c.TR_END_DT < ?",
        params=[today_str],
    )
    if att_df.empty:
        return pd.DataFrame(columns=['TRPR_DEGR', 'ATT_RATE', 'PRESENT_DAYS'])
    att_df['ATEND_DT'] = pd.to_datetime(att_df['ATEND_DT'], errors='coerce').dt.date

    def _cohort_stats(grp):
        student_rates = [
            calc_attendance_rate(s_grp, raw=True)
            for _, s_grp in grp.groupby('TRNEE_ID')
        ]
        avg_rate = sum(student_rates) / len(student_rates) if student_rates else 0.0
        present = grp[~grp['ATEND_STATUS'].isin(NOT_ATTEND_STATUSES)]
        penalty = int(present['ATEND_STATUS'].apply(_attendance_penalty).sum())
        present_days = max(0, len(present) - penalty // 3)
        return pd.Series({'ATT_RATE': round(avg_rate, 1), 'PRESENT_DAYS': present_days})

    return att_df.groupby('TRPR_DEGR').apply(_cohort_stats).reset_index()


def _render_project_intro():
    st.markdown("## HRD 교육성과 분석 시스템")
    st.markdown("`Python` `Streamlit` `PostgreSQL` `GitHub Actions`")
    st.caption("한화시스템 BEYOND SW캠프 | K-Digital Training B2G 교육사업 | 2023.07 ~ 현재")
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
    avg_rate_3 = df[df['상태'] == '종료']['EI_EMPL_RATE_3'].mean()
    avg_rate_6 = df[df['상태'] == '종료']['TOTAL_RATE_6'].mean()
    active_courses = len(df[df['상태'] == '진행중'])

    att_stats = get_attendance_stats()
    df_ended = df[df['상태'] == '종료']
    avg_completion = df_ended['수료율'].mean()
    avg_att = att_stats['ATT_RATE'].mean() if not att_stats.empty else 0.0

    st.subheader("📊 핵심 성과 지표")
    st.caption("종료된 전체 기수 기준 평균값입니다.")
    kpi1, kpi2, kpi3, kpi4, kpi5, kpi6 = st.columns(6)
    kpi1.metric("총 운영 과정", f"{total_courses}개", delta=f"진행중 {active_courses}개")
    kpi2.metric("누적 수강생", f"{total_trainees:,}명")
    kpi3.metric("평균 출석률", f"{avg_att:.1f}%", help="결석/중도탈락미출석/100분의50미만출석 제외, 지각·조퇴·외출 3개 누적 시 1일 차감 (종료 기수)")
    kpi4.metric("평균 수료율", f"{avg_completion:.1f}%", help="수료인원 / 수강인원 기준")
    kpi5.metric("평균 취업률(3개월)", f"{avg_rate_3:.1f}%" if pd.notna(
        avg_rate_3) else "-", help="수료 후 3개월 고용보험 가입률 (EI_EMPL_RATE_3 기준, 집계 전 기수 제외)")
    kpi6.metric("평균 취업률(6개월)", f"{avg_rate_6:.1f}%" if pd.notna(
        avg_rate_6) else "-", help="고용보험(EI_6) + HRD 자체 취업(HRD_6) 합산 (집계 전 기수 제외)")
    st.divider()

    # [Section 3] 기수 기록
    st.subheader("🏅 기수 기록")
    st.caption("각 지표별 역대 최고 기록입니다.")
    s3c1, s3c2, s3c3, s3c4 = st.columns(4)
    if not df_ended.empty:
        best_comp = df_ended.loc[df_ended['수료율'].idxmax()]
        s3c1.metric(
            "최고 수료율", f"{best_comp['수료율']:.1f}%", f"{int(best_comp['TRPR_DEGR'])}회차")
        df_ended_empl = df_ended.dropna(subset=['TOTAL_RATE_6'])
        df_ended_empl = df_ended_empl[df_ended_empl['TOTAL_RATE_6'] > 0]
        if not df_ended_empl.empty:
            best_empl = df_ended_empl.loc[df_ended_empl['TOTAL_RATE_6'].idxmax(
            )]
            s3c2.metric(
                "최고 취업률 (6개월)", f"{best_empl['TOTAL_RATE_6']:.1f}%", f"{int(best_empl['TRPR_DEGR'])}회차")
    if not att_stats.empty:
        best_att = att_stats.loc[att_stats['ATT_RATE'].idxmax()]
        s3c3.metric(
            "최고 출석률", f"{best_att['ATT_RATE']:.1f}%", f"{int(best_att['TRPR_DEGR'])}회차")
    rev_cache = load_cache_json(CacheKey.REVENUE_ALL_TERMS)
    if rev_cache:
        rev_df = pd.DataFrame(rev_cache)
        if not rev_df.empty and 'actual_fee' in rev_df.columns:
            best_rev = rev_df.loc[rev_df['actual_fee'].idxmax()]
            _eok = int(best_rev['actual_fee'] / 1e6) / 100  # 소수 둘째 자리 버림
            s3c4.metric("단일기수 최고 매출", f"{_eok}억+",
                        f"{int(best_rev['TRPR_DEGR'])}회차", help="단위기간별 청구 기준 실제 매출액")
    st.divider()

    # [Section 4] 시장 포지셔닝
    st.subheader("📍 전국 KDT 시장 포지셔닝")
    st.caption("K-디지털 트레이닝 모집 인원 기준")

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
        x=alt.X("연도:N", title="연도", axis=alt.Axis(labelFontSize=13, labelAngle=0)),
        y=alt.Y("상위(%):Q", axis=alt.Axis(title=['상', '위', '%'], titleAngle=0),
                scale=alt.Scale(domain=[0, 10])),
        color=alt.value("#2ecc71"),
        tooltip=[alt.Tooltip("연도:N"), alt.Tooltip("label:N", title="순위"), alt.Tooltip(
            "상위(%):Q", title="상위 %", format=".1f")],
    ).properties(height=180)
    text = chart.mark_text(align="center", dy=-10, fontSize=13, fontWeight="bold").encode(
        text=alt.Text("label:N")
    )
    st.altair_chart((chart + text), use_container_width=True)
    st.divider()

    # [Section 5] 차트와 상세 테이블
    col_left, col_right = st.columns([1, 1])

    with col_left:
        st.subheader("📈 연도별 운영 규모")
        df['Year'] = df['TR_STA_DT'].dt.year
        year_counts = df.groupby(
            'Year')['TRPR_NM'].count().reset_index(name='과정수')
        chart = alt.Chart(year_counts).mark_bar().encode(
            x=alt.X('Year:O', title='연도', axis=alt.Axis(labelAngle=0)),
            y=alt.Y('과정수:Q', axis=alt.Axis(title=['운', '영', '과', '정', '수'], titleAngle=0)),
            color=alt.value('#3182bd'),
            tooltip=['Year', '과정수'],
        ).properties(height=300)
        st.altair_chart(chart, use_container_width=True)

    with col_right:
        st.subheader("🏆 우수 성과 과정 (Top 5)")
        st.caption("6개월 총 취업률 기준 상위 5개 과정입니다.")
        top_courses = df[df['상태'] == '종료'].sort_values(
            by='TOTAL_RATE_6', ascending=False).head(5)
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

    with st.sidebar:
        st.info("좌측 메뉴를 선택하여 상세 분석 페이지로 이동하세요.")
        st.markdown("""
* **📈 시장 분석:** 시장 전체 훈련과정 비교 분석
* **📊 종료과정 성과:** 종료된 과정의 성과 심층 분석
* **📋 현재 운영 현황:** 현재 운영 중인 과정의 출결/이탈 관리
* **💰 매출 분석:** 단위기간별 훈련비 청구 현황
* **🗄️ DB 명세:** 수집 현황 및 원본 데이터 확인
* **🤖 AI 리포트:** AI 기반 성과 분석 리포트 자동 생성
        """)


pg = st.navigation([
    st.Page(render_dashboard, title="성과 대시보드", icon="🏠"),
    st.Page("pages/시장_분석.py", title="시장 분석", icon="📈"),
    st.Page("pages/종료과정_성과.py", title="종료과정 성과", icon="📊"),
    st.Page("pages/현재_운영_현황.py", title="현재 운영 현황", icon="📋"),
    st.Page("pages/매출_분석.py", title="매출 분석", icon="💰"),
    st.Page("pages/DB_명세.py", title="DB 명세", icon="🗄️"),
    st.Page("pages/AI_리포트.py", title="AI 리포트", icon="🤖"),
])
pg.run()
