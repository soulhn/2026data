import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from collections import Counter
import sys
import os

# 🚀 상위 폴더의 utils.py를 가져오기 위한 경로 설정
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils import check_password, get_connection, is_pg, load_data as _load_data

# ==========================================
# 1. 설정 및 데이터 로드
# ==========================================
st.set_page_config(page_title="시장 동향 분석", page_icon="📈", layout="wide")

check_password()

# ✅ 컬럼명 한글 매핑
COLUMN_MAP = {
    'TRPR_ID': '과정ID', 'TRPR_DEGR': '회차', 'INST_INO': '기관ID',
    'TRPR_NM': '과정명', 'TRAINST_NM': '훈련기관명',
    'TR_STA_DT': '개설일', 'TR_END_DT': '종료일',
    'NCS_CD': 'NCS코드', 'TRNG_AREA_CD': '지역코드',
    'TOT_FXNUM': '정원(명)', 'TOT_TRCO': '훈련비(원)',
    'COURSE_MAN': '수강비(원)', 'REAL_MAN': '실비(원)',
    'REG_COURSE_MAN': '등록인원',
    'EI_EMPL_RATE_3': '취업률(3개월)', 'EI_EMPL_RATE_6': '취업률(6개월)',
    'EI_EMPL_CNT_3': '취업인원(3개월)',
    'STDG_SCOR': '만족도(점)', 'GRADE': '등급',
    'CERTIFICATE': '관련자격증', 'CONTENTS': '콘텐츠',
    'ADDRESS': '주소', 'TEL_NO': '전화번호',
    'TRAIN_TARGET': '훈련유형', 'WKEND_SE': '주말구분',
    'REGION': '지역', 'YEAR_MONTH': '개설연월',
    '모집률': '모집률(%)' 
}

@st.cache_data(ttl=3600)
def load_market_data():
    df = _load_data("""
        SELECT TRPR_ID, TRPR_DEGR, TRPR_NM, TRAINST_NM,
               TR_STA_DT, TR_END_DT, NCS_CD, TRNG_AREA_CD,
               TOT_FXNUM, TOT_TRCO, COURSE_MAN, REG_COURSE_MAN,
               EI_EMPL_RATE_3, EI_EMPL_RATE_6, EI_EMPL_CNT_3,
               STDG_SCOR, GRADE, ADDRESS,
               TRAIN_TARGET, WKEND_SE
        FROM TB_MARKET_TREND
    """)

    # 1. 날짜/파생변수
    df['TR_STA_DT'] = pd.to_datetime(df['TR_STA_DT'])
    df['TR_END_DT'] = pd.to_datetime(df['TR_END_DT'], errors='coerce')
    df['YEAR_MONTH'] = df['TR_STA_DT'].dt.strftime('%Y-%m')
    df['REGION'] = df['ADDRESS'].str.split(' ').str[0]

    # 2. 수치형 변환
    cols = ['TOT_TRCO', 'EI_EMPL_RATE_3', 'STDG_SCOR', 'TOT_FXNUM', 'REG_COURSE_MAN']
    for c in cols:
        df[c] = pd.to_numeric(df[c], errors='coerce').fillna(0)

    # 3. 모집률 계산 (벡터 연산)
    df['모집률'] = (df['REG_COURSE_MAN'] / df['TOT_FXNUM'].replace(0, pd.NA) * 100).fillna(0).clip(upper=100)

    # 4. 코드값 매핑
    wk_map = {'1': '주중', '2': '주말', '3': '주중+주말'}
    df['주말구분_명'] = df['WKEND_SE'].astype(str).map(wk_map).fillna('기타')
    df['TRAIN_TARGET'] = df['TRAIN_TARGET'].fillna('기타')

    return df

@st.cache_data(ttl=3600)
def load_internal_courses():
    """TB_COURSE_MASTER에서 우리 과정 데이터 로드 (HANWHA_COURSE_ID 기반)"""
    course_id = os.getenv("HANWHA_COURSE_ID")
    if not course_id:
        try:
            course_id = st.secrets["HANWHA_COURSE_ID"]
        except (KeyError, FileNotFoundError):
            pass
    if not course_id:
        return None, None

    internal = _load_data("""
        SELECT TRPR_ID, TRPR_DEGR, TRPR_NM, TR_STA_DT, TR_END_DT,
               TOT_TRCO, TOT_FXNUM, TOT_PAR_MKS, TOT_TRP_CNT,
               FINI_CNT, EI_EMPL_RATE_3, EI_EMPL_CNT_3,
               EI_EMPL_RATE_6, EI_EMPL_CNT_6
        FROM TB_COURSE_MASTER
    """)
    if internal.empty:
        return None, course_id

    internal['TR_STA_DT'] = pd.to_datetime(internal['TR_STA_DT'], errors='coerce')
    internal['TR_END_DT'] = pd.to_datetime(internal['TR_END_DT'], errors='coerce')
    for c in ['TOT_TRCO', 'TOT_FXNUM', 'TOT_PAR_MKS', 'TOT_TRP_CNT', 'FINI_CNT']:
        internal[c] = pd.to_numeric(internal[c], errors='coerce').fillna(0)
    for c in ['EI_EMPL_RATE_3', 'EI_EMPL_RATE_6']:
        internal[c] = pd.to_numeric(internal[c], errors='coerce')
    internal['모집률'] = (internal['TOT_TRP_CNT'] / internal['TOT_FXNUM'].replace(0, pd.NA) * 100).fillna(0)
    internal['FINI_CNT'] = pd.to_numeric(internal['FINI_CNT'], errors='coerce').fillna(0)
    internal['수료율'] = (internal['FINI_CNT'] / internal['TOT_PAR_MKS'].replace(0, pd.NA) * 100).fillna(0)
    return internal, course_id

with st.spinner('30만 건의 데이터에서 인사이트를 추출 중입니다... 🚀'):
    raw_df = load_market_data()
    internal_df, course_id = load_internal_courses()

# ==========================================
# 2. 사이드바 (필터링)
# ==========================================
st.sidebar.header("🔍 상세 분석 필터")

min_date = raw_df['TR_STA_DT'].min().date()
max_date = raw_df['TR_STA_DT'].max().date()

# 🛠️ 날짜 선택 에러 수정
date_range = st.sidebar.date_input(
    "조회 기간", 
    value=[min_date, max_date], 
    min_value=min_date, 
    max_value=max_date
)

if len(date_range) == 2:
    start_date, end_date = date_range
else:
    start_date, end_date = date_range[0], date_range[0]

region_opts = ['전체'] + sorted(raw_df['REGION'].dropna().unique().tolist())
sel_region = st.sidebar.selectbox("📍 지역", region_opts)

type_opts = sorted(raw_df['TRAIN_TARGET'].unique().tolist())
sel_types = st.sidebar.multiselect("🎓 훈련 유형 (다중선택)", type_opts, default=[])

wk_opts = sorted(raw_df['주말구분_명'].unique().tolist())
sel_wkend = st.sidebar.multiselect("📅 주말/주중 (다중선택)", wk_opts, default=[])

grade_opts = sorted(raw_df['GRADE'].dropna().unique().tolist())
sel_grade = st.sidebar.multiselect("🏅 기관 등급 (다중선택)", grade_opts, default=[])

ncs_opts = ['전체'] + sorted(raw_df['NCS_CD'].unique().tolist())
sel_ncs = st.sidebar.selectbox("NCS 코드", ncs_opts)
search_kwd = st.sidebar.text_input("🔍 과정명 검색")

df = raw_df[
    (raw_df['TR_STA_DT'].dt.date >= start_date) & 
    (raw_df['TR_STA_DT'].dt.date <= end_date)
]

if sel_region != '전체': df = df[df['REGION'] == sel_region]
if sel_ncs != '전체': df = df[df['NCS_CD'] == sel_ncs]
if sel_types: df = df[df['TRAIN_TARGET'].isin(sel_types)]
if sel_wkend: df = df[df['주말구분_명'].isin(sel_wkend)]
if sel_grade: df = df[df['GRADE'].isin(sel_grade)]
if search_kwd: df = df[df['TRPR_NM'].str.contains(search_kwd, case=False)]

# ==========================================
# 3. 메인 대시보드
# ==========================================
st.title(f"📈 IT 훈련 시장 상세 분석 ({len(df):,}건)")
st.markdown("---")

# 3.1 KPI Row
c1, c2, c3, c4, c5, c6 = st.columns(6)
c1.metric("검색된 과정 수", f"{len(df):,}개")

mean_trco = df['TOT_TRCO'].mean()
if pd.isna(mean_trco): mean_trco = 0
c2.metric("평균 훈련비", f"{int(mean_trco):,}원")

mean_fxnum = df['TOT_FXNUM'].mean()
if pd.isna(mean_fxnum): mean_fxnum = 0
c3.metric("평균 정원", f"{int(mean_fxnum)}명")

avg_empl = df[df['EI_EMPL_RATE_3'] > 0]['EI_EMPL_RATE_3'].mean()
if pd.isna(avg_empl):
    c4.metric("평균 취업률", "-")
else:
    c4.metric("평균 취업률", f"{avg_empl:.1f}%")

valid_score_df = df[df['STDG_SCOR'] > 0]

# 1. 가중 평균 만족도 (시장 공급 기준)
if not valid_score_df.empty:
    weighted_score = valid_score_df['STDG_SCOR'].mean()
    c5.metric("가중 평균 만족도 (100점 환산)", f"{weighted_score/100:.1f}점", delta=f"원본: {int(weighted_score):,} / 10,000", delta_color="off")
else:
    c5.metric("가중 평균 만족도", "데이터 없음")

# 2. 과정 평균 만족도 (순수 과정 기준)
if not valid_score_df.empty:
    course_means = valid_score_df.groupby(['TRPR_NM', 'TRAINST_NM'])['STDG_SCOR'].mean()
    simple_score = course_means.mean()
    c6.metric("과정 평균 만족도 (100점 환산)", f"{simple_score/100:.1f}점", help="개별 과정들의 만족도 단순 평균")
else:
    c6.metric("과정 평균 만족도", "데이터 없음")

st.markdown("###")

# 3.2 탭 구성
tabs = st.tabs([
    "📊 시장 개요", "🏆 순위 & 모집 분석", "💎 우리 과정 vs 시장",
    "🎨 유형/일정 분석", "💰 비용/성과 분석",
    "📈 시계열 트렌드", "⚔️ 경쟁 심화도", "🎯 비용 대비 성과",
    "🏢 경쟁 현황", "☁️ 키워드", "🎓 자격증 분석", "📑 데이터 조회"
])

# [Tab 1] 시장 개요
with tabs[0]:
    col1, col2 = st.columns([2, 1])
    with col1:
        st.subheader("월별 개설 추이")
        trend = df.groupby('YEAR_MONTH').size().reset_index(name='COUNT')
        st.plotly_chart(px.line(trend, x='YEAR_MONTH', y='COUNT', markers=True), use_container_width=True)
    with col2:
        st.subheader("지역별 점유율")
        reg_cnt = df['REGION'].value_counts().reset_index()
        reg_cnt.columns = ['지역', '개수']
        st.plotly_chart(px.pie(reg_cnt, values='개수', names='지역', hole=0.4), use_container_width=True)

# [Tab 2] 🏆 순위 & 모집 분석 (만족도 추가됨 ✨)
with tabs[1]:
    st.subheader("🔎 내 기관/과정의 시장 위치 찾기")
    
    # ----------------------------------------------------
    # 1. 기관별 집계 데이터 준비
    # ----------------------------------------------------
    inst_stats = df.groupby('TRAINST_NM').agg({
        'TRPR_ID': 'count',
        'TOT_FXNUM': 'sum',      # 총 모집정원
        'REG_COURSE_MAN': 'sum', # 총 신청인원
        'EI_EMPL_RATE_3': 'mean', # 평균 취업률
        'STDG_SCOR': 'mean'       # 평균 만족도 (New!)
    }).reset_index()
    
    inst_stats['평균모집률'] = (inst_stats['REG_COURSE_MAN'] / inst_stats['TOT_FXNUM'] * 100).fillna(0).clip(upper=100)
    inst_stats['만족도(점)'] = (inst_stats['STDG_SCOR'] / 100).round(1) # 100점 만점으로 변환
    
    inst_stats = inst_stats.sort_values(by='REG_COURSE_MAN', ascending=False).reset_index(drop=True)
    inst_stats['순위'] = inst_stats.index + 1
    
    inst_stats = inst_stats.rename(columns={
        'TRAINST_NM': '기관명', 'TRPR_ID': '개설수', 
        'TOT_FXNUM': '총모집정원', 'REG_COURSE_MAN': '총신청인원', 'EI_EMPL_RATE_3': '평균취업률'
    })
    
    # ----------------------------------------------------
    # 2. 과정별 통합 집계 (Aggregation)
    # ----------------------------------------------------
    course_agg = df.groupby(['TRPR_NM', 'TRAINST_NM']).agg({
        'TRPR_ID': 'count',       # 개설 회차 수
        'TOT_FXNUM': 'sum',       # 조회 기간 총 정원
        'REG_COURSE_MAN': 'sum',  # 조회 기간 총 신청인원
        'EI_EMPL_RATE_3': 'mean', # 평균 취업률
        'STDG_SCOR': 'mean'       # 평균 만족도 (New!)
    }).reset_index()
    
    course_agg['통합모집률'] = (course_agg['REG_COURSE_MAN'] / course_agg['TOT_FXNUM'] * 100).fillna(0).clip(upper=100)
    course_agg['만족도(점)'] = (course_agg['STDG_SCOR'] / 100).round(1) # 100점 만점으로 변환
    
    course_agg = course_agg.sort_values(by='REG_COURSE_MAN', ascending=False).reset_index(drop=True)
    course_agg['순위'] = course_agg.index + 1
    
    course_agg = course_agg.rename(columns={
        'TRPR_NM': '과정명', 'TRAINST_NM': '기관명', 'TRPR_ID': '개설회차',
        'TOT_FXNUM': '총정원', 'REG_COURSE_MAN': '총신청인원', 'EI_EMPL_RATE_3': '평균취업률'
    })

    # ----------------------------------------------------
    # 3. 화면 구성 (2단 컬럼)
    # ----------------------------------------------------
    col_rank1, col_rank2 = st.columns(2)
    
    # [왼쪽] 기관 순위 섹션
    with col_rank1:
        st.markdown("##### 🏫 훈련기관 순위 (총 신청인원 기준)")
        target_inst = st.text_input("기관명 검색", placeholder="기관명 입력", key="rank_inst")
        
        # 만족도(점) 추가됨
        display_inst_cols = ['순위', '기관명', '총모집정원', '총신청인원', '평균모집률', '만족도(점)']
        
        if target_inst:
            found = inst_stats[inst_stats['기관명'].str.contains(target_inst)]
            if not found.empty:
                my_rank = found.iloc[0]
                st.info(f"**'{my_rank['기관명']}'**의 신청인원 순위는 **{my_rank['순위']}위**입니다.")
                
                idx = my_rank.name
                start = max(0, idx - 2)
                end = min(len(inst_stats), idx + 3)
                neighbor = inst_stats.iloc[start:end][display_inst_cols].copy()
                
                def style_me(s):
                    return ['background-color: #d1e7dd' if s['기관명'] == my_rank['기관명'] else '' for _ in s]
                
                st.dataframe(
                    neighbor.style.apply(style_me, axis=1).format({
                        "총모집정원": "{:,}명", "총신청인원": "{:,}명", "평균모집률": "{:.1f}%", "만족도(점)": "{:.1f}점"
                    }), 
                    use_container_width=True, hide_index=True
                )
            else:
                st.warning("검색된 기관이 없습니다.")
        else:
            # 기본 Top 5
            st.markdown("🏆 **신청인원 Top 5 기관**")
            st.dataframe(
                inst_stats.head(5)[display_inst_cols].style.format({
                    "총모집정원": "{:,}명", "총신청인원": "{:,}명", "평균모집률": "{:.1f}%", "만족도(점)": "{:.1f}점"
                }),
                use_container_width=True, hide_index=True
            )
            
        with st.expander("📋 전체 훈련기관 순위표 펼치기"):
            st.dataframe(
                inst_stats[display_inst_cols].style.format({
                    "총모집정원": "{:,}명", "총신청인원": "{:,}명", "평균모집률": "{:.1f}%", "만족도(점)": "{:.1f}점"
                }),
                use_container_width=True, hide_index=True
            )

    # [오른쪽] 과정 순위 섹션
    with col_rank2:
        st.markdown("##### 📚 인기 훈련과정 (과정 통합/신청인원 기준)")
        target_course = st.text_input("과정명 검색", placeholder="과정명 입력 (예: 한화시스템)", key="rank_course")
        
        # 만족도(점) 추가됨
        display_course_cols = ['순위', '과정명', '기관명', '개설회차', '총정원', '총신청인원', '통합모집률', '만족도(점)']
        
        if target_course:
            found_c = course_agg[course_agg['과정명'].str.contains(target_course)]
            if not found_c.empty:
                my_c = found_c.iloc[0]
                st.info(f"**'{my_c['과정명']}'** ({my_c['기관명']}) - 통합 순위 **{my_c['순위']}위**")
                
                idx_c = my_c.name
                start_c = max(0, idx_c - 2)
                end_c = min(len(course_agg), idx_c + 3)
                neighbor_c = course_agg.iloc[start_c:end_c][display_course_cols].copy()
                
                st.dataframe(
                    neighbor_c.style.format({
                        "총정원": "{:,}명", "총신청인원": "{:,}명", "통합모집률": "{:.1f}%", "개설회차": "{:,}회", "만족도(점)": "{:.1f}점"
                    }), 
                    use_container_width=True, hide_index=True
                )
            else:
                st.warning("검색된 과정이 없습니다.")
        else:
            # 기본 Top 5
            st.markdown("🏆 **신청인원 Top 5 과정 (통합)**")
            st.dataframe(
                course_agg.head(5)[display_course_cols].style.format({
                    "총정원": "{:,}명", "총신청인원": "{:,}명", "통합모집률": "{:.1f}%", "개설회차": "{:,}회", "만족도(점)": "{:.1f}점"
                }),
                use_container_width=True, hide_index=True
            )
            
        with st.expander("📋 전체 과정 순위표 펼치기"):
            st.dataframe(
                course_agg[display_course_cols].style.format({
                    "총정원": "{:,}명", "총신청인원": "{:,}명", "통합모집률": "{:.1f}%", "개설회차": "{:,}회", "만족도(점)": "{:.1f}점"
                }),
                use_container_width=True, hide_index=True
            )

    st.divider()
    
    # --- 3. 모집률 분석 히트맵 (유지) ---
    st.subheader("📊 시장 전체 모집률 & 커리큘럼 분석")
    
    row2_1, row2_2 = st.columns(2)
    
    with row2_1:
        st.markdown("**1️⃣ 훈련 유형(사업)별 모집 현황**")
        biz_table = df.groupby('TRAIN_TARGET').agg({
            'TRPR_ID': 'count', 'TOT_FXNUM': 'sum', 'REG_COURSE_MAN': 'sum'
        }).reset_index()
        biz_table['평균모집률'] = (biz_table['REG_COURSE_MAN'] / biz_table['TOT_FXNUM'] * 100).fillna(0).clip(upper=100)
        biz_table.columns = ['훈련유형', '개설수', '총모집정원', '총신청인원', '평균모집률']
        
        fig_biz = px.bar(biz_table.sort_values('평균모집률', ascending=False), x='훈련유형', y='평균모집률', 
                         color='평균모집률', text_auto='.1f', title="유형별 모집률(%)")
        st.plotly_chart(fig_biz, use_container_width=True)
        
        with st.expander("📄 상세 데이터 보기", expanded=True):
             st.dataframe(
                biz_table.sort_values('평균모집률', ascending=False), 
                use_container_width=True, hide_index=True,
                column_config={
                    "총모집정원": st.column_config.NumberColumn(format="%d명"),
                    "총신청인원": st.column_config.NumberColumn(format="%d명"),
                    "평균모집률": st.column_config.NumberColumn(format="%.1f%%")
                }
            )

    with row2_2:
        st.markdown("**2️⃣ 인기 NCS(기술)별 모집 현황**")
        ncs_counts = df['NCS_CD'].value_counts()
        valid_ncs = ncs_counts.head(10).index if len(df) < 100 else ncs_counts[ncs_counts >= 5].index
        
        ncs_df = df[df['NCS_CD'].isin(valid_ncs)]
        if not ncs_df.empty:
            ncs_table = ncs_df.groupby('NCS_CD').agg({
                'TRPR_ID': 'count', 'TOT_FXNUM': 'sum', 'REG_COURSE_MAN': 'sum'
            }).reset_index()
            ncs_table['평균모집률'] = (ncs_table['REG_COURSE_MAN'] / ncs_table['TOT_FXNUM'] * 100).fillna(0).clip(upper=100)
            ncs_table.columns = ['NCS코드', '개설수', '총모집정원', '총신청인원', '평균모집률']
            
            top_ncs = ncs_table.sort_values('평균모집률', ascending=False).head(10)
            
            fig_ncs = px.bar(top_ncs, x='NCS코드', y='평균모집률', color='평균모집률', text_auto='.1f', title="모집률 Top 10 커리큘럼")
            st.plotly_chart(fig_ncs, use_container_width=True)
            
            with st.expander("📄 상세 데이터 보기", expanded=True):
                st.dataframe(
                    top_ncs, 
                    use_container_width=True, hide_index=True,
                    column_config={
                        "총모집정원": st.column_config.NumberColumn(format="%d명"),
                        "총신청인원": st.column_config.NumberColumn(format="%d명"),
                        "평균모집률": st.column_config.NumberColumn(format="%.1f%%")
                    }
                )

# [Tab 3] 💎 우리 과정 vs 시장 비교
with tabs[2]:
    if internal_df is None:
        st.info("HANWHA_COURSE_ID가 설정되지 않았습니다. 환경변수 또는 Streamlit Secrets에 설정하면 우리 과정과 시장을 비교할 수 있습니다.")
    else:
        # 내부 과정의 NCS 코드를 시장 데이터에서 매칭
        internal_trpr_ids = internal_df['TRPR_ID'].unique()
        matched = df[df['TRPR_ID'].isin(internal_trpr_ids)]

        if matched.empty:
            st.warning("시장 데이터에서 우리 과정을 찾을 수 없습니다. 필터 기간을 확인하세요.")
        else:
            our_ncs_codes = matched['NCS_CD'].dropna().unique()

            # --- KPI 카드: 우리 vs 시장 평균 ---
            st.subheader("핵심 지표 비교")
            our_empl = internal_df['EI_EMPL_RATE_3'].dropna()
            our_empl_valid = our_empl[our_empl > 0]
            mkt_empl = df[df['EI_EMPL_RATE_3'] > 0]['EI_EMPL_RATE_3']
            our_trco = internal_df['TOT_TRCO']
            mkt_trco = df[df['TOT_TRCO'] > 0]['TOT_TRCO']
            our_recruit = internal_df['모집률']
            mkt_recruit = df[df['모집률'] > 0]['모집률']

            k1, k2, k3 = st.columns(3)
            if not our_empl_valid.empty and not mkt_empl.empty:
                our_avg_empl = our_empl_valid.mean()
                mkt_avg_empl = mkt_empl.mean()
                k1.metric("취업률 (3개월)", f"{our_avg_empl:.1f}%", delta=f"{our_avg_empl - mkt_avg_empl:+.1f}%p vs 시장")
            else:
                k1.metric("취업률 (3개월)", "-")

            if not our_trco.empty and not mkt_trco.empty:
                our_avg_trco = our_trco.mean()
                mkt_avg_trco = mkt_trco.mean()
                k2.metric("평균 훈련비", f"{int(our_avg_trco):,}원", delta=f"{int(our_avg_trco - mkt_avg_trco):,}원 vs 시장", delta_color="inverse")
            else:
                k2.metric("평균 훈련비", "-")

            if not our_recruit.empty and not mkt_recruit.empty:
                our_avg_rec = our_recruit.mean()
                mkt_avg_rec = mkt_recruit.mean()
                k3.metric("평균 모집률", f"{our_avg_rec:.1f}%", delta=f"{our_avg_rec - mkt_avg_rec:+.1f}%p vs 시장")
            else:
                k3.metric("평균 모집률", "-")

            st.divider()

            # --- 백분위 분석 ---
            st.subheader("시장 내 백분위 분석")
            p1, p2, p3 = st.columns(3)

            if not our_empl_valid.empty and not mkt_empl.empty:
                pct = (mkt_empl < our_empl_valid.mean()).mean() * 100
                p1.metric("취업률 백분위", f"상위 {100 - pct:.0f}%")
            else:
                p1.metric("취업률 백분위", "-")

            if not our_trco.empty and not mkt_trco.empty:
                pct_trco = (mkt_trco < our_trco.mean()).mean() * 100
                p2.metric("훈련비 백분위", f"상위 {pct_trco:.0f}% (높은 순)")
            else:
                p2.metric("훈련비 백분위", "-")

            if not our_recruit.empty and not mkt_recruit.empty:
                pct_rec = (mkt_recruit < our_recruit.mean()).mean() * 100
                p3.metric("모집률 백분위", f"상위 {100 - pct_rec:.0f}%")
            else:
                p3.metric("모집률 백분위", "-")

            st.divider()

            # --- 레이더 차트: 우리 vs 시장 평균 vs 시장 상위 10% ---
            st.subheader("종합 역량 레이더")
            radar_metrics = ['취업률', '모집률', '훈련비(역순)', '정원']
            our_vals, mkt_avg_vals, mkt_top10_vals = [], [], []

            # 취업률
            o_e = our_empl_valid.mean() if not our_empl_valid.empty else 0
            m_e = mkt_empl.mean() if not mkt_empl.empty else 0
            t_e = mkt_empl.quantile(0.9) if not mkt_empl.empty else 0
            our_vals.append(o_e); mkt_avg_vals.append(m_e); mkt_top10_vals.append(t_e)

            # 모집률
            o_r = our_recruit.mean() if not our_recruit.empty else 0
            m_r = mkt_recruit.mean() if not mkt_recruit.empty else 0
            t_r = mkt_recruit.quantile(0.9) if not mkt_recruit.empty else 0
            our_vals.append(o_r); mkt_avg_vals.append(m_r); mkt_top10_vals.append(t_r)

            # 훈련비 (역순: 낮을수록 좋음 → 정규화)
            if not mkt_trco.empty:
                max_trco = mkt_trco.quantile(0.95)
                o_t = max(0, (1 - our_trco.mean() / max_trco) * 100) if max_trco > 0 else 50
                m_t = max(0, (1 - mkt_trco.mean() / max_trco) * 100) if max_trco > 0 else 50
                t_t = max(0, (1 - mkt_trco.quantile(0.1) / max_trco) * 100) if max_trco > 0 else 50
            else:
                o_t, m_t, t_t = 50, 50, 50
            our_vals.append(o_t); mkt_avg_vals.append(m_t); mkt_top10_vals.append(t_t)

            # 정원
            our_fx = internal_df['TOT_FXNUM']
            mkt_fx = df[df['TOT_FXNUM'] > 0]['TOT_FXNUM']
            o_f = min(our_fx.mean(), 100) if not our_fx.empty else 0
            m_f = min(mkt_fx.mean(), 100) if not mkt_fx.empty else 0
            t_f = min(mkt_fx.quantile(0.9), 100) if not mkt_fx.empty else 0
            our_vals.append(o_f); mkt_avg_vals.append(m_f); mkt_top10_vals.append(t_f)

            fig_radar = go.Figure()
            fig_radar.add_trace(go.Scatterpolar(r=our_vals + [our_vals[0]], theta=radar_metrics + [radar_metrics[0]], fill='toself', name='우리 과정'))
            fig_radar.add_trace(go.Scatterpolar(r=mkt_avg_vals + [mkt_avg_vals[0]], theta=radar_metrics + [radar_metrics[0]], fill='toself', name='시장 평균', opacity=0.5))
            fig_radar.add_trace(go.Scatterpolar(r=mkt_top10_vals + [mkt_top10_vals[0]], theta=radar_metrics + [radar_metrics[0]], fill='toself', name='시장 상위 10%', opacity=0.3))
            fig_radar.update_layout(polar=dict(radialaxis=dict(visible=True)), title="우리 과정 vs 시장 (값이 클수록 우수)")
            st.plotly_chart(fig_radar, use_container_width=True)

            st.divider()

            # --- 회차별 상세 비교 테이블 ---
            st.subheader("회차별 상세 비교")
            detail = internal_df[['TRPR_DEGR', 'TRPR_NM', 'TR_STA_DT', 'TOT_TRCO', 'TOT_FXNUM', 'TOT_TRP_CNT', 'FINI_CNT', '수료율', 'EI_EMPL_RATE_3']].copy()
            detail.columns = ['회차', '과정명', '시작일', '훈련비', '정원', '수강신청인원', '수료인원', '수료율(%)', '취업률(%)']
            detail['시작일'] = detail['시작일'].dt.strftime('%Y-%m-%d')
            st.dataframe(
                detail.style.format({'훈련비': '{:,.0f}원', '정원': '{:,.0f}명', '수강신청인원': '{:,.0f}명', '수료인원': '{:,.0f}명', '수료율(%)': '{:.1f}%', '취업률(%)': '{:.1f}%'}),
                use_container_width=True, hide_index=True
            )

# [Tab 6] 📈 시계열 트렌드
with tabs[5]:
    st.subheader("월별 평균 취업률 추이")
    empl_valid = df[df['EI_EMPL_RATE_3'] > 0]
    if not empl_valid.empty:
        monthly_empl = empl_valid.groupby('YEAR_MONTH')['EI_EMPL_RATE_3'].mean().reset_index()
        monthly_empl.columns = ['월', '평균취업률']
        monthly_empl = monthly_empl.sort_values('월')
        monthly_empl['3개월 이동평균'] = monthly_empl['평균취업률'].rolling(3, min_periods=1).mean()

        fig_empl = go.Figure()
        fig_empl.add_trace(go.Scatter(x=monthly_empl['월'], y=monthly_empl['평균취업률'], mode='lines+markers', name='월별 평균', opacity=0.6))
        fig_empl.add_trace(go.Scatter(x=monthly_empl['월'], y=monthly_empl['3개월 이동평균'], mode='lines', name='3개월 이동평균', line=dict(width=3)))

        # 우리 과정 개설 시점 마커
        if internal_df is not None:
            for _, row in internal_df.iterrows():
                if pd.notna(row['TR_STA_DT']):
                    ym = row['TR_STA_DT'].strftime('%Y-%m')
                    fig_empl.add_vline(x=ym, line_dash="dash", line_color="red", opacity=0.5)
                    fig_empl.add_annotation(x=ym, y=1, yref="paper", text=f"{int(row['TRPR_DEGR'])}기", showarrow=False, yshift=10, font=dict(color="red", size=10))

        fig_empl.update_layout(xaxis_title='월', yaxis_title='취업률(%)', title='시장 전체 월별 취업률 추이')
        st.plotly_chart(fig_empl, use_container_width=True)
    else:
        st.info("취업률 데이터가 없습니다.")

    st.divider()

    # 월별 훈련비 추이
    st.subheader("월별 훈련비 추이")
    trco_valid = df[df['TOT_TRCO'] > 0]
    if not trco_valid.empty:
        monthly_trco = trco_valid.groupby('YEAR_MONTH')['TOT_TRCO'].agg(['median', lambda x: x.quantile(0.25), lambda x: x.quantile(0.75)]).reset_index()
        monthly_trco.columns = ['월', '중앙값', 'Q1', 'Q3']
        monthly_trco = monthly_trco.sort_values('월')

        fig_trco = go.Figure()
        fig_trco.add_trace(go.Scatter(x=monthly_trco['월'], y=monthly_trco['Q3'], mode='lines', name='75%', line=dict(width=0), showlegend=False))
        fig_trco.add_trace(go.Scatter(x=monthly_trco['월'], y=monthly_trco['Q1'], mode='lines', name='25~75% 범위', fill='tonexty', fillcolor='rgba(68,114,196,0.2)', line=dict(width=0)))
        fig_trco.add_trace(go.Scatter(x=monthly_trco['월'], y=monthly_trco['중앙값'], mode='lines+markers', name='중앙값', line=dict(color='#4472C4', width=3)))
        fig_trco.update_layout(xaxis_title='월', yaxis_title='훈련비(원)', title='월별 훈련비 분포 (중앙값 + 사분위)')
        st.plotly_chart(fig_trco, use_container_width=True)
    else:
        st.info("훈련비 데이터가 없습니다.")

    st.divider()

    # 신규 과정 개설 수 증감률
    st.subheader("신규 과정 개설 추이 및 증감률")
    monthly_count = df.groupby('YEAR_MONTH').size().reset_index(name='개설수')
    monthly_count = monthly_count.sort_values('YEAR_MONTH')
    monthly_count['전월대비(%)'] = monthly_count['개설수'].pct_change() * 100

    col_trend1, col_trend2 = st.columns(2)
    with col_trend1:
        fig_cnt = px.bar(monthly_count, x='YEAR_MONTH', y='개설수', text_auto=True, title='월별 신규 과정 개설 수')
        st.plotly_chart(fig_cnt, use_container_width=True)
    with col_trend2:
        fig_chg = px.bar(monthly_count.dropna(subset=['전월대비(%)']), x='YEAR_MONTH', y='전월대비(%)',
                         color='전월대비(%)', color_continuous_scale='RdYlGn', text_auto='.1f', title='전월 대비 증감률(%)')
        st.plotly_chart(fig_chg, use_container_width=True)

    st.divider()

    # 지역별 Top 5 개설 추이
    st.subheader("지역별 Top 5 과정 개설 추이")
    top5_regions = df['REGION'].value_counts().head(5).index.tolist()
    region_trend = df[df['REGION'].isin(top5_regions)].groupby(['YEAR_MONTH', 'REGION']).size().reset_index(name='개설수')
    if not region_trend.empty:
        fig_reg = px.line(region_trend, x='YEAR_MONTH', y='개설수', color='REGION', markers=True, title='상위 5개 지역 월별 개설 추이')
        st.plotly_chart(fig_reg, use_container_width=True)

# [Tab 7] ⚔️ 경쟁 심화도
with tabs[6]:
    if internal_df is None:
        st.info("HANWHA_COURSE_ID가 설정되지 않았습니다. 시장 전체 경쟁 분석만 표시합니다.")
        our_ncs_codes_comp = []
    else:
        internal_trpr_ids_comp = internal_df['TRPR_ID'].unique()
        matched_comp = df[df['TRPR_ID'].isin(internal_trpr_ids_comp)]
        our_ncs_codes_comp = matched_comp['NCS_CD'].dropna().unique().tolist() if not matched_comp.empty else []

    # 우리 NCS 코드 경쟁 과정 수 시계열
    if our_ncs_codes_comp:
        st.subheader("우리 NCS 분야 경쟁 과정 수 추이")
        comp_df = df[df['NCS_CD'].isin(our_ncs_codes_comp)]
        comp_monthly = comp_df.groupby(['YEAR_MONTH', 'NCS_CD']).size().reset_index(name='경쟁과정수')
        comp_monthly = comp_monthly.sort_values('YEAR_MONTH')
        fig_comp = px.line(comp_monthly, x='YEAR_MONTH', y='경쟁과정수', color='NCS_CD', markers=True, title='우리 NCS 분야 월별 경쟁 과정 수')
        st.plotly_chart(fig_comp, use_container_width=True)
        st.divider()

    # 공급-수요 매트릭스
    st.subheader("NCS별 공급-수요 매트릭스")
    st.caption("과정수(공급) vs 모집률(수요) - 우측 하단은 과잉공급 위험 영역")
    ncs_supply = df.groupby('NCS_CD').agg(
        과정수=('TRPR_ID', 'count'),
        평균모집률=('모집률', 'mean')
    ).reset_index()
    ncs_supply = ncs_supply[ncs_supply['과정수'] >= 3]

    if not ncs_supply.empty:
        ncs_supply['is_ours'] = ncs_supply['NCS_CD'].isin(our_ncs_codes_comp) if our_ncs_codes_comp else False
        fig_matrix = px.scatter(
            ncs_supply, x='과정수', y='평균모집률', text='NCS_CD',
            color='is_ours', color_discrete_map={True: 'red', False: 'steelblue'},
            size='과정수', opacity=0.7,
            labels={'is_ours': '우리 분야'},
            title='NCS별 공급(과정수) vs 수요(모집률)'
        )
        avg_recruit = ncs_supply['평균모집률'].mean()
        avg_count = ncs_supply['과정수'].mean()
        fig_matrix.add_hline(y=avg_recruit, line_dash="dash", line_color="gray", opacity=0.5, annotation_text="평균 모집률")
        fig_matrix.add_vline(x=avg_count, line_dash="dash", line_color="gray", opacity=0.5, annotation_text="평균 과정수")
        fig_matrix.update_traces(textposition='top center')
        fig_matrix.update_layout(showlegend=True)
        st.plotly_chart(fig_matrix, use_container_width=True)

        # 과잉공급 경고 (과정수 많고 모집률 낮은 NCS)
        oversupply = ncs_supply[(ncs_supply['과정수'] > avg_count) & (ncs_supply['평균모집률'] < avg_recruit)]
        if not oversupply.empty:
            st.warning(f"과잉공급 위험 NCS ({len(oversupply)}개): {', '.join(oversupply['NCS_CD'].tolist())}")
    else:
        st.info("분석할 NCS 데이터가 부족합니다.")

    st.divider()

    # 모집률 하락 추세 경고
    st.subheader("모집률 변화 추세")
    recruit_trend = df.groupby('YEAR_MONTH')['모집률'].mean().reset_index().sort_values('YEAR_MONTH')
    if len(recruit_trend) >= 3:
        recent_3 = recruit_trend.tail(3)['모집률'].values
        if recent_3[-1] < recent_3[0]:
            st.error(f"최근 3개월 모집률 하락 추세 감지: {recent_3[0]:.1f}% → {recent_3[-1]:.1f}%")
        else:
            st.success(f"최근 3개월 모집률 안정/상승: {recent_3[0]:.1f}% → {recent_3[-1]:.1f}%")
        fig_rec_trend = px.line(recruit_trend, x='YEAR_MONTH', y='모집률', markers=True, title='월별 평균 모집률 추이')
        st.plotly_chart(fig_rec_trend, use_container_width=True)

# [Tab 8] 🎯 비용 대비 성과
with tabs[7]:
    cost_valid = df[(df['TOT_TRCO'] > 0) & (df['EI_EMPL_RATE_3'] > 0)].copy()

    # 훈련비 구간별 평균 취업률
    st.subheader("훈련비 구간별 평균 취업률")
    if not cost_valid.empty:
        bins = [0, 1_000_000, 3_000_000, 5_000_000, 10_000_000, float('inf')]
        labels = ['~100만', '100~300만', '300~500만', '500~1000만', '1000만~']
        cost_valid['비용구간'] = pd.cut(cost_valid['TOT_TRCO'], bins=bins, labels=labels)
        bin_stats = cost_valid.groupby('비용구간', observed=True)['EI_EMPL_RATE_3'].agg(['mean', 'count']).reset_index()
        bin_stats.columns = ['비용구간', '평균취업률', '과정수']
        fig_bin = px.bar(bin_stats, x='비용구간', y='평균취업률', text_auto='.1f', color='과정수',
                         title='훈련비 구간별 평균 취업률(%)', labels={'평균취업률': '취업률(%)'})
        st.plotly_chart(fig_bin, use_container_width=True)
    else:
        st.info("유효한 비용/취업률 데이터가 없습니다.")

    st.divider()

    # 4분면 scatter: 비용 vs 취업률
    st.subheader("비용 vs 취업률 4분면 분석")
    if not cost_valid.empty:
        scatter_sample = cost_valid.sample(n=min(3000, len(cost_valid)), random_state=42)

        # 우리 과정 표시
        if internal_df is not None:
            internal_scatter = internal_df[
                (internal_df['TOT_TRCO'] > 0) & (internal_df['EI_EMPL_RATE_3'] > 0)
            ].copy()
        else:
            internal_scatter = pd.DataFrame()

        fig_quad = go.Figure()
        fig_quad.add_trace(go.Scatter(
            x=scatter_sample['TOT_TRCO'], y=scatter_sample['EI_EMPL_RATE_3'],
            mode='markers', name='시장 과정', opacity=0.4,
            marker=dict(size=6, color='steelblue'),
            text=scatter_sample['TRPR_NM'], hoverinfo='text+x+y'
        ))
        if not internal_scatter.empty:
            fig_quad.add_trace(go.Scatter(
                x=internal_scatter['TOT_TRCO'], y=internal_scatter['EI_EMPL_RATE_3'],
                mode='markers', name='우리 과정',
                marker=dict(size=15, color='red', symbol='star'),
                text=internal_scatter['TRPR_NM'], hoverinfo='text+x+y'
            ))
        avg_trco = cost_valid['TOT_TRCO'].median()
        avg_empl_q = cost_valid['EI_EMPL_RATE_3'].median()
        fig_quad.add_hline(y=avg_empl_q, line_dash="dash", line_color="gray", opacity=0.5)
        fig_quad.add_vline(x=avg_trco, line_dash="dash", line_color="gray", opacity=0.5)
        fig_quad.update_layout(
            xaxis_title='훈련비(원)', yaxis_title='취업률(%)',
            title='비용 vs 취업률 (우상단=고비용/고성과, 좌상단=가성비 우수)',
            annotations=[
                dict(x=avg_trco * 0.3, y=avg_empl_q * 1.5, text="가성비 우수", showarrow=False, font=dict(color="green", size=12)),
                dict(x=avg_trco * 1.8, y=avg_empl_q * 0.5, text="개선 필요", showarrow=False, font=dict(color="red", size=12)),
            ]
        )
        st.plotly_chart(fig_quad, use_container_width=True)

    st.divider()

    # 가성비 챔피언 Top 20
    st.subheader("가성비 챔피언 Top 20 (취업률 / 백만원당)")
    if not cost_valid.empty:
        champ = cost_valid.copy()
        champ['가성비'] = champ['EI_EMPL_RATE_3'] / (champ['TOT_TRCO'] / 1_000_000)
        champ = champ.nlargest(20, '가성비')[['TRPR_NM', 'TRAINST_NM', 'TOT_TRCO', 'EI_EMPL_RATE_3', '가성비']]
        champ.columns = ['과정명', '훈련기관', '훈련비', '취업률(%)', '가성비(취업률/백만원)']
        st.dataframe(
            champ.style.format({'훈련비': '{:,.0f}원', '취업률(%)': '{:.1f}%', '가성비(취업률/백만원)': '{:.2f}'}),
            use_container_width=True, hide_index=True
        )

    st.divider()

    # 가격 시뮬레이터
    st.subheader("가격 시뮬레이터: 훈련비 → 예상 취업률")
    if not cost_valid.empty and len(cost_valid) >= 10:
        from sklearn.linear_model import LinearRegression

        X = cost_valid[['TOT_TRCO']].values
        y = cost_valid['EI_EMPL_RATE_3'].values
        model = LinearRegression().fit(X, y)
        r2 = model.score(X, y)

        sim_min = int(cost_valid['TOT_TRCO'].quantile(0.05))
        sim_max = int(cost_valid['TOT_TRCO'].quantile(0.95))
        sim_val = st.slider("훈련비 설정 (원)", min_value=sim_min, max_value=sim_max, value=int((sim_min + sim_max) / 2), step=100_000, format="%d원")

        pred = model.predict([[sim_val]])[0]
        pred = max(0, min(100, pred))
        st.metric("예상 취업률", f"{pred:.1f}%", help=f"선형회귀 기반 (R²={r2:.3f})")

        # 회귀선 시각화
        x_range = np.linspace(sim_min, sim_max, 100)
        y_pred = model.predict(x_range.reshape(-1, 1))

        fig_sim = go.Figure()
        sample_sim = cost_valid.sample(n=min(2000, len(cost_valid)), random_state=42)
        fig_sim.add_trace(go.Scatter(x=sample_sim['TOT_TRCO'], y=sample_sim['EI_EMPL_RATE_3'], mode='markers', name='실제 데이터', opacity=0.3, marker=dict(size=4)))
        fig_sim.add_trace(go.Scatter(x=x_range, y=y_pred, mode='lines', name='회귀선', line=dict(color='red', width=2)))
        fig_sim.add_trace(go.Scatter(x=[sim_val], y=[pred], mode='markers', name='시뮬레이션', marker=dict(size=15, color='gold', symbol='star')))
        fig_sim.update_layout(xaxis_title='훈련비(원)', yaxis_title='취업률(%)', title=f'훈련비-취업률 선형회귀 (R²={r2:.3f})')
        st.plotly_chart(fig_sim, use_container_width=True)

        st.caption(f"회귀 계수: 훈련비 100만원 증가 시 취업률 {model.coef_[0] * 1_000_000:.2f}%p 변화 / 절편: {model.intercept_:.1f}%")
    else:
        st.info("시뮬레이션에 필요한 데이터가 부족합니다 (최소 10건).")

# [Tab 4] 유형/일정 분석
with tabs[3]:
    c1, c2 = st.columns(2)
    with c1:
        st.subheader("🎓 훈련 유형별 비중")
        type_cnt = df['TRAIN_TARGET'].value_counts().reset_index()
        type_cnt.columns = ['유형', '개수']
        st.plotly_chart(px.pie(type_cnt, values='개수', names='유형', title="K-Digital vs 일반 과정 비율"), use_container_width=True)
    with c2:
        st.subheader("📅 주말 vs 주중 개설 현황")
        wk_cnt = df['주말구분_명'].value_counts().reset_index()
        wk_cnt.columns = ['구분', '개수']
        st.plotly_chart(px.bar(wk_cnt, x='구분', y='개수', color='구분', text='개수', title="직장인 타겟(주말) 과정 수"), use_container_width=True)

# [Tab 5] 비용/성과 분석
with tabs[4]:
    st.subheader("💸 훈련비 vs 취업률 상관관계 분석")
    st.caption("원이 크면 정원이 많은 과정, 색상은 훈련 유형을 나타냅니다. (상위 2000건 샘플링)")
    valid_scatter = df[(df['EI_EMPL_RATE_3'] > 0) & (df['TOT_TRCO'] > 0)]
    if not valid_scatter.empty:
        scatter_df = valid_scatter.sample(n=min(2000, len(valid_scatter)), random_state=42)
        fig_scatter = px.scatter(
            scatter_df, x='TOT_TRCO', y='EI_EMPL_RATE_3', color='TRAIN_TARGET', size='TOT_FXNUM',
            hover_data=['TRPR_NM', 'TRAINST_NM'], opacity=0.7,
            labels={'TOT_TRCO': '훈련비(원)', 'EI_EMPL_RATE_3': '취업률(%)', 'TRAIN_TARGET': '유형'}
        )
        st.plotly_chart(fig_scatter, use_container_width=True)
    else:
        st.info("분석할 유효 데이터가 없습니다.")
    st.divider()
    st.subheader("🏅 등급별 평균 성과 비교")
    if not df['GRADE'].dropna().empty:
        grade_grp = df.groupby('GRADE')[['EI_EMPL_RATE_3', 'STDG_SCOR']].mean().reset_index()
        fig_bar = px.bar(grade_grp, x='GRADE', y=['EI_EMPL_RATE_3', 'STDG_SCOR'], barmode='group')
        st.plotly_chart(fig_bar, use_container_width=True)
    else:
        st.info("등급 데이터가 없습니다.")

# [Tab 9] 경쟁 현황 (🚀 총 신청인원 기준 Top 15로 변경)
with tabs[8]:
    st.subheader("🏆 TOP 15 훈련기관 (총 신청인원 기준)")
    
    # 1. 기관별 집계
    top_inst = df.groupby('TRAINST_NM').agg({
        'REG_COURSE_MAN': 'sum', # 신청인원
        'TOT_FXNUM': 'sum',      # 모집정원
        'TRPR_ID': 'count'       # 개설수
    }).reset_index()
    
    # 2. 모집률 계산
    top_inst['모집률'] = (top_inst['REG_COURSE_MAN'] / top_inst['TOT_FXNUM'] * 100).fillna(0).clip(upper=100)
    top_inst['모집률'] = top_inst['모집률'].round(1)
    
    # 3. 신청인원 순으로 정렬
    top_inst = top_inst.sort_values(by='REG_COURSE_MAN', ascending=False).head(15)
    top_inst.columns = ['기관명', '총신청인원', '총모집정원', '개설수', '평균모집률']
    
    # 4. 차트
    fig = px.bar(
        top_inst, y='기관명', x='총신청인원', 
        orientation='h', text='총신청인원',
        color='평균모집률', # 모집률이 높을수록 진하게
        color_continuous_scale='Bluyl',
        title="가장 많은 훈련생을 모은 기관 Top 15",
        hover_data=['총모집정원', '개설수', '평균모집률']
    )
    st.plotly_chart(fig, use_container_width=True)
    
    # 5. 표 데이터
    with st.expander("📄 Top 15 기관 상세 데이터 보기", expanded=True):
        st.dataframe(
            top_inst, 
            use_container_width=True, hide_index=True,
            column_config={
                "총신청인원": st.column_config.NumberColumn(format="%d명"),
                "총모집정원": st.column_config.NumberColumn(format="%d명"),
                "평균모집률": st.column_config.NumberColumn(format="%.1f%%")
            }
        )

# [Tab 10] 키워드
with tabs[9]:
    st.subheader("🔥 과정명 트렌드 키워드")
    text = " ".join(df['TRPR_NM'].dropna().astype(str))
    stops = ['과정', '양성', '취업', '실무', '및', '위한', '기반', '활용', '개발자', 'A', 'B', '수료', '반', '취득', '능력', '향상']
    words = [w for w in text.split() if len(w) > 1 and w not in stops]
    kwd = pd.DataFrame(Counter(words).most_common(25), columns=['키워드', '빈도'])
    st.plotly_chart(px.bar(kwd, x='키워드', y='빈도', color='빈도'), use_container_width=True)


# [Tab 11] 자격증 분석 (신규)
with tabs[10]:
    st.subheader("🎓 자격증 연계 분석")

    # CERTIFICATE 컬럼 로드
    cert_df = _load_data("""
        SELECT CERTIFICATE, EI_EMPL_RATE_3, TOT_TRCO, TRAIN_TARGET, NCS_CD
        FROM TB_MARKET_TREND
        WHERE CERTIFICATE IS NOT NULL AND CERTIFICATE != ''
    """)

    if cert_df.empty:
        st.info("자격증 데이터가 없습니다.")
    else:
        cert_df['EI_EMPL_RATE_3'] = pd.to_numeric(cert_df['EI_EMPL_RATE_3'], errors='coerce')
        cert_df['TOT_TRCO'] = pd.to_numeric(cert_df['TOT_TRCO'], errors='coerce')

        # 자격증 파싱 (쉼표/줄바꿈 구분)
        all_certs = []
        for _, row in cert_df.iterrows():
            raw = str(row['CERTIFICATE'])
            for sep in [',', '\n', '/', '·']:
                raw = raw.replace(sep, '|')
            certs = [c.strip() for c in raw.split('|') if c.strip() and len(c.strip()) > 1]
            for c in certs:
                all_certs.append({
                    '자격증': c,
                    '취업률': row['EI_EMPL_RATE_3'],
                    '훈련비': row['TOT_TRCO'],
                })

        if all_certs:
            cert_parsed = pd.DataFrame(all_certs)
            cert_stats = cert_parsed.groupby('자격증').agg(
                과정수=('자격증', 'count'),
                평균_취업률=('취업률', 'mean'),
                평균_훈련비=('훈련비', 'mean'),
            ).reset_index()
            cert_stats = cert_stats[cert_stats['과정수'] >= 5]  # 5건 이상만
            cert_stats['평균_취업률'] = cert_stats['평균_취업률'].round(1)
            cert_stats['평균_훈련비'] = cert_stats['평균_훈련비'].round(0)
            cert_stats = cert_stats.sort_values('과정수', ascending=False)

            st.metric("연계 자격증 종류", f"{len(cert_stats)}개", help="5건 이상 연계된 자격증만 표시")

            cc1, cc2 = st.columns(2)
            with cc1:
                st.markdown("##### Top 20 자격증 (과정수 기준)")
                top20 = cert_stats.head(20)
                import plotly.express as px
                fig = px.bar(
                    top20, x='과정수', y='자격증', orientation='h',
                    color='평균_취업률', color_continuous_scale='RdYlGn',
                    hover_data=['평균_취업률', '평균_훈련비'],
                )
                fig.update_layout(height=500, yaxis={'categoryorder': 'total ascending'})
                st.plotly_chart(fig, use_container_width=True)

            with cc2:
                st.markdown("##### 자격증별 평균 취업률 Top 20")
                top_empl = cert_stats[cert_stats['과정수'] >= 10].sort_values('평균_취업률', ascending=False).head(20)
                if not top_empl.empty:
                    fig2 = px.bar(
                        top_empl, x='평균_취업률', y='자격증', orientation='h',
                        color='과정수', color_continuous_scale='Blues',
                        hover_data=['과정수', '평균_훈련비'],
                    )
                    fig2.update_layout(height=500, yaxis={'categoryorder': 'total ascending'})
                    st.plotly_chart(fig2, use_container_width=True)

            st.markdown("##### 전체 자격증 데이터")
            st.dataframe(
                cert_stats,
                column_config={
                    '과정수': st.column_config.NumberColumn(format="%d"),
                    '평균_취업률': st.column_config.NumberColumn('평균 취업률(%)', format="%.1f"),
                    '평균_훈련비': st.column_config.NumberColumn('평균 훈련비(원)', format="%,.0f"),
                },
                use_container_width=True,
                hide_index=True,
            )
        else:
            st.info("파싱 가능한 자격증 데이터가 없습니다.")


# [Tab 12] 데이터 조회
with tabs[11]:
    st.subheader(f"📄 상세 데이터 ({len(df):,}건)")
    display_df = df.rename(columns=COLUMN_MAP)
    priority = ['과정명', '훈련기관명', '훈련유형', '지역', '주말구분', '훈련비(원)', '정원(명)', '등록인원', '모집률(%)', '취업률(3개월)', '개설일']
    cols = [c for c in priority if c in display_df.columns] + [c for c in display_df.columns if c not in priority]
    st.warning("⚠️ 상위 1,000건만 표시됩니다. 전체 데이터는 CSV로 다운로드하세요.")
    st.dataframe(display_df[cols].head(1000), use_container_width=True, height=600)
    csv = display_df[cols].to_csv(index=False).encode('utf-8-sig')
    st.download_button("📥 전체 데이터 다운로드 (CSV)", csv, "market_analysis.csv", "text/csv")