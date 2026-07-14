import json
from pathlib import Path

import streamlit as st
import pandas as pd
import altair as alt
from utils import check_password

st.set_page_config(
    page_title="한화시스템 BEYOND SW캠프 성과 대시보드",
    page_icon="🏆",
    layout="wide",
    initial_sidebar_state="expanded"
)

SNAPSHOT_PATH = Path(__file__).parent / "data" / "home_snapshot.json"


@st.cache_data
def load_snapshot():
    """확정 스냅샷(data/home_snapshot.json) 로드.

    1기~25기 전 과정 종료(2026-07-03)로 홈 수치는 확정 상태 — DB 조회 없이
    커밋된 스냅샷만 읽는다. 갱신(취업률 확정 등)은 `python build_home_snapshot.py`
    재실행 후 커밋으로 반영한다.
    """
    with open(SNAPSHOT_PATH, encoding='utf-8') as f:
        snap = json.load(f)
    df = pd.DataFrame(snap['courses'])
    df['TR_STA_DT'] = pd.to_datetime(df['TR_STA_DT'])
    df['TR_END_DT'] = pd.to_datetime(df['TR_END_DT'])
    # 취업률: null 유지 (HRD-Net 집계중 기수 → 0과 구분)
    for col in ('EI_EMPL_RATE_3', 'TOTAL_RATE_6'):
        df[col] = pd.to_numeric(df[col], errors='coerce')
    att_stats = pd.DataFrame(snap['attendance'])
    return snap, df, att_stats


def _render_project_intro():
    st.markdown("## 한화시스템 BEYOND SW캠프 · 성과 분석 시스템")
    st.markdown("`Python` `Streamlit` `PostgreSQL` `GitHub Actions`")
    st.caption("K-Digital Training B2G 교육사업 | 2023.10 ~ 2026.07 | 1기~25기 전 과정 종료")
    st.divider()


def render_dashboard():
    _render_project_intro()
    check_password()

    try:
        snap, df, att_stats = load_snapshot()
    except Exception as e:
        st.error(f"확정 스냅샷을 불러오지 못했습니다. data/home_snapshot.json을 확인해주세요.\n{e}")
        st.stop()

    # [헤더]
    st.title("🏆 한화시스템 BEYOND SW캠프 성과 대시보드")
    st.markdown(
        f"**1기~25기 전 과정 종료** (2023.10 ~ 2026.07) · "
        f"데이터 기준일 **{snap['generated_at']}**"
    )
    st.divider()

    # [Section 1] 핵심 지표 (KPI)
    total_courses = len(df)
    total_trainees = int(df['TOT_PAR_MKS'].sum())
    total_fxnum = int(df['TOT_FXNUM'].sum())
    total_fini = int(df['FINI_CNT'].sum())
    recruit_rate = total_trainees / total_fxnum * 100 if total_fxnum else 0.0
    completion_rate = total_fini / total_trainees * 100 if total_trainees else 0.0
    avg_rate_3 = df[df['상태'] == '종료']['EI_EMPL_RATE_3'].mean()
    avg_rate_6 = df[df['상태'] == '종료']['TOTAL_RATE_6'].mean()
    active_courses = len(df[df['상태'] == '진행중'])

    df_ended = df[df['상태'] == '종료']
    avg_completion = df_ended['수료율'].mean()
    avg_att = att_stats['ATT_RATE'].mean() if not att_stats.empty else 0.0
    bench = snap['benchmark']
    rev_cache = snap['revenue']

    st.subheader("📊 핵심 성과 지표")
    st.caption("1기~25기 전체 기준 최종 수치입니다. 모집률·만족도의 증감 표시는 전국 KDT 평균 대비 격차입니다.")
    kpi1, kpi2, kpi3, kpi4, kpi5, kpi6 = st.columns(6)
    status_note = f"진행중 {active_courses}개" if active_courses else "전 과정 종료"
    kpi1.metric("운영 기수", f"{total_courses}개", delta=status_note, delta_color="off")
    kpi2.metric("누적 수강생", f"{total_trainees:,}명",
                delta=f"정원 {total_fxnum:,}명", delta_color="off")
    if bench['mkt_recruit'] is not None:
        kpi3.metric("모집률", f"{recruit_rate:.1f}%",
                    delta=f"{recruit_rate - bench['mkt_recruit']:+.1f}%p",
                    help=f"수강 인원 {total_trainees:,}명 / 정원 {total_fxnum:,}명 · 전국 KDT 평균 {bench['mkt_recruit']:.1f}% 대비")
    else:
        kpi3.metric("모집률", f"{recruit_rate:.1f}%",
                    help=f"수강 인원 {total_trainees:,}명 / 정원 {total_fxnum:,}명")
    if bench['our_satis'] is not None and bench['mkt_satis'] is not None:
        kpi4.metric("만족도", f"{bench['our_satis']:.1f}점",
                    delta=f"{bench['our_satis'] - bench['mkt_satis']:+.1f}점",
                    help=f"HRD-Net 만족도 100점 환산 (기수 평균) · 전국 KDT 평균 {bench['mkt_satis']:.1f}점 대비")
    else:
        kpi4.metric("만족도", f"{bench['our_satis']:.1f}점" if bench['our_satis'] is not None else "-",
                    help="HRD-Net 만족도 100점 환산 (기수 평균)")
    kpi5.metric("수료율", f"{completion_rate:.1f}%",
                help=f"수료 {total_fini:,}명 / 수강 {total_trainees:,}명 합산 기준 (기수 단순평균 {avg_completion:.1f}%)")
    kpi6.metric("평균 출석률", f"{avg_att:.1f}%", help="결석/중도탈락미출석/100분의50미만출석 제외, 지각·조퇴·외출 3개 누적 시 1일 차감 (종료 기수)")

    kpi7, kpi8, kpi9, _, _, _ = st.columns(6)
    kpi7.metric("평균 취업률 (3개월)", f"{avg_rate_3:.1f}%" if pd.notna(
        avg_rate_3) else "-", help="수료 후 3개월 고용보험 가입률 (EI_EMPL_RATE_3 기준, 집계 중인 기수 제외)")
    kpi8.metric("평균 취업률 (6개월)", f"{avg_rate_6:.1f}%" if pd.notna(
        avg_rate_6) else "-", help="고용보험(EI_6) + HRD 자체 취업(HRD_6) 합산 (집계 중인 기수 제외 — 수료 후 3~6개월 소요)")
    # 누적 매출: 확정 스냅샷의 원장 고정 헤드라인 (캐시 재계산 드리프트와 무관하게 확정 표기 유지)
    kpi9.metric("누적 총 매출", f"{snap['kpi_revenue_eok']}억",
                help="단위기간별 청구 기준 실제 매출 합계 (상세: 매출 분석 페이지)")
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
    if rev_cache:
        rev_df = pd.DataFrame(rev_cache)
        if not rev_df.empty and 'actual_fee' in rev_df.columns:
            best_rev = rev_df.loc[rev_df['actual_fee'].idxmax()]
            _eok = int(best_rev['actual_fee'] / 1e6) / 100  # 소수 둘째 자리 버림
            s3c4.metric("단일기수 최고 매출", f"{_eok}억+",
                        f"{int(best_rev['TRPR_DEGR'])}회차", help="단위기간별 청구 기준 실제 매출액")
    st.divider()

    # [Section 4] 시장 포지셔닝
    st.subheader("📍 전국 KDT 시장 비교")
    if bench['kdt_cnt'] and bench['mkt_recruit'] is not None and bench['our_satis'] is not None:
        st.caption(f"전국 K-디지털 트레이닝 {bench['kdt_cnt']:,}개 과정(시장 데이터 40만+ 건) 대비 본 과정 성과입니다.")
        comp_df = pd.DataFrame([
            {"지표": "모집률(%)", "본 과정": round(recruit_rate, 1),
             "전국 KDT 평균": round(bench['mkt_recruit'], 1),
             "격차": f"{recruit_rate - bench['mkt_recruit']:+.1f}%p"},
            {"지표": "만족도(점)", "본 과정": round(bench['our_satis'], 1),
             "전국 KDT 평균": round(bench['mkt_satis'], 1),
             "격차": f"{bench['our_satis'] - bench['mkt_satis']:+.1f}점"},
        ])
        st.dataframe(comp_df, hide_index=True, width='stretch')

    st.markdown("**KDT 전국 순위 (모집 인원 기준)** — 3년 연속 상위권")
    st.caption("과정 단위 모집 인원 합 랭킹 — 2023년은 개강 분기(10~12월 개강 과정) 대상, 2024·2025년은 연간 대상 · 확정 시점 스냅샷 (이후 수집분으로 분모 ±1~2 변동 가능)")
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
    st.altair_chart((chart + text), width='stretch')
    st.divider()

    # [Section 5] 차트와 상세 테이블
    col_left, col_right = st.columns([1, 1])

    with col_left:
        st.subheader("📈 기수별 모집·수료 인원")
        cohort_df = df.sort_values('TRPR_DEGR')[
            ['TRPR_DEGR', 'TOT_PAR_MKS', 'FINI_CNT']].rename(
            columns={'TOT_PAR_MKS': '모집 인원', 'FINI_CNT': '수료 인원'})
        fold_df = cohort_df.melt('TRPR_DEGR', var_name='구분', value_name='인원')
        chart = alt.Chart(fold_df).mark_bar().encode(
            x=alt.X('TRPR_DEGR:O', title='기수', axis=alt.Axis(labelAngle=0)),
            y=alt.Y('인원:Q', axis=alt.Axis(title=['인', '원'], titleAngle=0)),
            color=alt.Color('구분:N',
                            scale=alt.Scale(domain=['모집 인원', '수료 인원'],
                                            range=['#3182bd', '#2ecc71']),
                            legend=alt.Legend(title=None, orient='top')),
            xOffset='구분:N',
            tooltip=[alt.Tooltip('TRPR_DEGR:O', title='기수'), alt.Tooltip('구분:N'),
                     alt.Tooltip('인원:Q', format=',')],
        ).properties(height=300)
        st.altair_chart(chart, width='stretch')

    with col_right:
        st.subheader("🏆 우수 성과 과정 (Top 5)")
        st.caption("취업률 (6개월) 기준 상위 5개 과정입니다.")
        top_courses = df[df['상태'] == '종료'].sort_values(
            by='TOTAL_RATE_6', ascending=False).head(5)
        st.dataframe(
            top_courses[['TRPR_DEGR', 'TRPR_NM', 'TOTAL_RATE_6', 'FINI_CNT']],
            column_config={
                "TRPR_DEGR": "회차",
                "TRPR_NM": "과정명",
                "TOTAL_RATE_6": st.column_config.ProgressColumn("취업률 (6개월)", format="%.1f%%", min_value=0, max_value=100),
                "FINI_CNT": st.column_config.NumberColumn("수료생", format="%d명"),
            },
            hide_index=True,
            width='stretch',
        )

    # [Section 6] 1기~25기 전체 성과
    st.subheader("📋 기수별 전체 성과")
    st.caption("취업률 공란은 집계 중입니다 (수료 후 3~6개월 소요).")
    detail_df = df.sort_values('TRPR_DEGR').copy()
    detail_df['기간'] = (detail_df['TR_STA_DT'].dt.strftime('%Y-%m-%d') + ' ~ '
                       + detail_df['TR_END_DT'].dt.strftime('%Y-%m-%d'))
    st.dataframe(
        detail_df[['TRPR_DEGR', '기간', 'TOT_FXNUM', 'TOT_PAR_MKS', 'FINI_CNT', '수료율', 'TOTAL_RATE_6']],
        column_config={
            "TRPR_DEGR": "회차",
            "기간": "기간",
            "TOT_FXNUM": st.column_config.NumberColumn("정원", format="%d명"),
            "TOT_PAR_MKS": st.column_config.NumberColumn("수강 인원", format="%d명"),
            "FINI_CNT": st.column_config.NumberColumn("수료 인원", format="%d명"),
            "수료율": st.column_config.ProgressColumn("수료율(%)", format="%.1f%%", min_value=0, max_value=100),
            "TOTAL_RATE_6": st.column_config.ProgressColumn("취업률 (6개월)", format="%.1f%%", min_value=0, max_value=100),
        },
        hide_index=True,
        width='stretch',
        height=430,
    )

    with st.sidebar:
        st.info("좌측 메뉴를 선택하여 상세 분석 페이지로 이동하세요.")
        st.markdown("""
**📊 성과 분석**
* 종료과정 성과 · 운영 현황 · 매출 분석

**🌏 외부 동향**
* 시장 전체 훈련과정 비교 분석

**💼 채용**
* IT 채용공고 트렌드 분석

**🛠️ 도구**
* AI 리포트 · DB 명세 · SQL Playground · 용어 사전
        """)


pg = st.navigation({
    "개요": [
        st.Page(render_dashboard, title="한화시스템 BEYOND SW캠프 성과 대시보드", icon="🏠"),
    ],
    "성과 분석": [
        st.Page("pages/종료과정_성과.py", title="종료과정 성과", icon="📊"),
        st.Page("pages/현재_운영_현황.py", title="현재 운영 현황", icon="📋"),
        st.Page("pages/매출_분석.py", title="매출 분석", icon="💰"),
    ],
    "외부 동향": [
        st.Page("pages/시장_분석.py", title="시장 분석", icon="📈"),
    ],
    "채용": [
        st.Page("pages/채용_동향.py", title="채용 동향", icon="💼"),
    ],
    "도구": [
        st.Page("pages/AI_리포트.py", title="AI 리포트", icon="🤖"),
        st.Page("pages/DB_명세.py", title="DB 명세", icon="🗄️"),
        st.Page("pages/SQL_Playground.py", title="SQL Playground", icon="🔍"),
        st.Page("pages/용어_사전.py", title="용어 사전", icon="📖"),
    ],
})
pg.run()
