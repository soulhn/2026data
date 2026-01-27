import streamlit as st
import pandas as pd
import sqlite3
import plotly.express as px
from collections import Counter
import sys
import os

# 🚀 상위 폴더의 utils.py를 가져오기 위한 경로 설정
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils import DB_FILE

# ==========================================
# 1. 설정 및 데이터 로드
# ==========================================
st.set_page_config(page_title="시장 동향 분석", page_icon="📈", layout="wide")

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
    conn = sqlite3.connect(DB_FILE)
    query = "SELECT * FROM TB_MARKET_TREND"
    df = pd.read_sql(query, conn)
    conn.close()
    
    # 1. 날짜/파생변수
    df['TR_STA_DT'] = pd.to_datetime(df['TR_STA_DT'])
    df['TR_END_DT'] = pd.to_datetime(df['TR_END_DT'], errors='coerce')
    df['YEAR_MONTH'] = df['TR_STA_DT'].dt.strftime('%Y-%m')
    df['REGION'] = df['ADDRESS'].str.split(' ').str[0]
    
    # 2. 수치형 변환
    cols = ['TOT_TRCO', 'EI_EMPL_RATE_3', 'STDG_SCOR', 'TOT_FXNUM', 'REG_COURSE_MAN']
    for c in cols:
        df[c] = pd.to_numeric(df[c], errors='coerce').fillna(0)
        
    # 3. 모집률 계산 (개별 과정용)
    df['모집률'] = df.apply(
        lambda x: (x['REG_COURSE_MAN'] / x['TOT_FXNUM'] * 100) if x['TOT_FXNUM'] > 0 else 0, 
        axis=1
    )
    df['모집률'] = df['모집률'].clip(upper=100)
        
    # 4. 코드값 매핑
    wk_map = {'1': '주중', '2': '주말', '3': '주중+주말'}
    df['주말구분_명'] = df['WKEND_SE'].astype(str).map(wk_map).fillna('기타')
    df['TRAIN_TARGET'] = df['TRAIN_TARGET'].fillna('기타')
    
    return df

with st.spinner('30만 건의 데이터에서 인사이트를 추출 중입니다... 🚀'):
    raw_df = load_market_data()

# ==========================================
# 2. 사이드바 (필터링)
# ==========================================
st.sidebar.header("🔍 상세 분석 필터")

min_date = raw_df['TR_STA_DT'].min().date()
max_date = raw_df['TR_STA_DT'].max().date()

# 🛠️ [Fix 1] 날짜 선택 에러 수정 (Unpacking Error 해결)
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
c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("검색된 과정 수", f"{len(df):,}개")

# 🛠️ [Fix 2] NaN to Integer 에러 수정 (데이터가 없을 때 처리)
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
if not valid_score_df.empty:
    raw_score = valid_score_df['STDG_SCOR'].mean()
    c5.metric("평균 만족도 (100점 환산)", f"{raw_score/100:.1f}점", delta=f"원본: {int(raw_score):,} / 10,000", delta_color="off")
else:
    c5.metric("평균 만족도", "데이터 없음")

st.markdown("###")

# 3.2 탭 구성
tabs = st.tabs(["📊 시장 개요", "🏆 순위 & 모집 분석", "🎨 유형/일정 분석", "💰 비용/성과 분석", "🏢 경쟁 현황", "☁️ 키워드", "📑 데이터 조회"])

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

# [Tab 2] 🏆 순위 & 모집 분석 (통합 과정 분석 적용)
with tabs[1]:
    st.subheader("🔎 내 기관/과정의 시장 위치 찾기")
    
    # ----------------------------------------------------
    # 1. 기관별 집계 데이터 준비
    # ----------------------------------------------------
    inst_stats = df.groupby('TRAINST_NM').agg({
        'TRPR_ID': 'count',
        'TOT_FXNUM': 'sum',      # 총 모집정원
        'REG_COURSE_MAN': 'sum', # 총 신청인원
        'EI_EMPL_RATE_3': 'mean'
    }).reset_index()
    
    inst_stats['평균모집률'] = (inst_stats['REG_COURSE_MAN'] / inst_stats['TOT_FXNUM'] * 100).fillna(0).clip(upper=100)
    inst_stats = inst_stats.sort_values(by='REG_COURSE_MAN', ascending=False).reset_index(drop=True)
    inst_stats['순위'] = inst_stats.index + 1
    
    inst_stats = inst_stats.rename(columns={
        'TRAINST_NM': '기관명', 'TRPR_ID': '개설수', 
        'TOT_FXNUM': '총모집정원', 'REG_COURSE_MAN': '총신청인원', 'EI_EMPL_RATE_3': '평균취업률'
    })
    
    # ----------------------------------------------------
    # 2. 과정별 통합 집계 (Aggregation) - 핵심 변경 사항
    # ----------------------------------------------------
    # 같은 과정명(TRPR_NM)과 기관명(TRAINST_NM)으로 그룹화하여 통계 산출
    course_agg = df.groupby(['TRPR_NM', 'TRAINST_NM']).agg({
        'TRPR_ID': 'count',       # 개설 회차 수
        'TOT_FXNUM': 'sum',       # 조회 기간 총 정원
        'REG_COURSE_MAN': 'sum',  # 조회 기간 총 신청인원
        'EI_EMPL_RATE_3': 'mean'  # 평균 취업률
    }).reset_index()
    
    # 통합 모집률 계산
    course_agg['통합모집률'] = (course_agg['REG_COURSE_MAN'] / course_agg['TOT_FXNUM'] * 100).fillna(0).clip(upper=100)
    
    # 신청인원 기준으로 내림차순 정렬
    course_agg = course_agg.sort_values(by='REG_COURSE_MAN', ascending=False).reset_index(drop=True)
    course_agg['순위'] = course_agg.index + 1
    
    # 컬럼명 정리
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
        
        display_inst_cols = ['순위', '기관명', '총모집정원', '총신청인원', '평균모집률']
        
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
                        "총모집정원": "{:,}명", "총신청인원": "{:,}명", "평균모집률": "{:.1f}%"
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
                    "총모집정원": "{:,}명", "총신청인원": "{:,}명", "평균모집률": "{:.1f}%"
                }),
                use_container_width=True, hide_index=True
            )
            
        # [추가] 전체 보기 기능
        with st.expander("📋 전체 훈련기관 순위표 펼치기"):
            st.dataframe(
                inst_stats[display_inst_cols].style.format({
                    "총모집정원": "{:,}명", "총신청인원": "{:,}명", "평균모집률": "{:.1f}%"
                }),
                use_container_width=True, hide_index=True
            )

    # [오른쪽] 과정 순위 섹션 (통합된 과정 데이터 사용)
    with col_rank2:
        st.markdown("##### 📚 인기 훈련과정 (과정 통합/신청인원 기준)")
        target_course = st.text_input("과정명 검색", placeholder="과정명 입력 (예: 한화시스템)", key="rank_course")
        
        # 기관명 포함된 컬럼 구성
        display_course_cols = ['순위', '과정명', '기관명', '개설회차', '총정원', '총신청인원', '통합모집률']
        
        if target_course:
            found_c = course_agg[course_agg['과정명'].str.contains(target_course)]
            if not found_c.empty:
                # 검색 결과 중 가장 상위(인기 많은) 과정 선택
                my_c = found_c.iloc[0]
                st.info(f"**'{my_c['과정명']}'** ({my_c['기관명']}) - 통합 순위 **{my_c['순위']}위**")
                
                idx_c = my_c.name
                start_c = max(0, idx_c - 2)
                end_c = min(len(course_agg), idx_c + 3)
                neighbor_c = course_agg.iloc[start_c:end_c][display_course_cols].copy()
                
                st.dataframe(
                    neighbor_c.style.format({
                        "총정원": "{:,}명", "총신청인원": "{:,}명", "통합모집률": "{:.1f}%", "개설회차": "{:,}회"
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
                    "총정원": "{:,}명", "총신청인원": "{:,}명", "통합모집률": "{:.1f}%", "개설회차": "{:,}회"
                }),
                use_container_width=True, hide_index=True
            )
            
        # [추가] 전체 보기 기능
        with st.expander("📋 전체 과정 순위표 펼치기"):
            st.dataframe(
                course_agg[display_course_cols].style.format({
                    "총정원": "{:,}명", "총신청인원": "{:,}명", "통합모집률": "{:.1f}%", "개설회차": "{:,}회"
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

# [Tab 3] 유형/일정 분석
with tabs[2]:
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

# [Tab 4] 비용/성과 분석
with tabs[3]:
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

# [Tab 5] 경쟁 현황 (🚀 총 신청인원 기준 Top 15로 변경)
with tabs[4]:
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

# [Tab 6] 키워드
with tabs[5]:
    st.subheader("🔥 과정명 트렌드 키워드")
    text = " ".join(df['TRPR_NM'].dropna().astype(str))
    stops = ['과정', '양성', '취업', '실무', '및', '위한', '기반', '활용', '개발자', 'A', 'B', '수료', '반', '취득', '능력', '향상']
    words = [w for w in text.split() if len(w) > 1 and w not in stops]
    kwd = pd.DataFrame(Counter(words).most_common(25), columns=['키워드', '빈도'])
    st.plotly_chart(px.bar(kwd, x='키워드', y='빈도', color='빈도'), use_container_width=True)

# [Tab 7] 데이터 조회
with tabs[6]:
    st.subheader(f"📄 상세 데이터 ({len(df):,}건)")
    display_df = df.rename(columns=COLUMN_MAP)
    priority = ['과정명', '훈련기관명', '훈련유형', '지역', '주말구분', '훈련비(원)', '정원(명)', '등록인원', '모집률(%)', '취업률(3개월)', '개설일']
    cols = [c for c in priority if c in display_df.columns] + [c for c in display_df.columns if c not in priority]
    st.warning("⚠️ 상위 1,000건만 표시됩니다. 전체 데이터는 CSV로 다운로드하세요.")
    st.dataframe(display_df[cols].head(1000), use_container_width=True)
    csv = display_df[cols].to_csv(index=False).encode('utf-8-sig')
    st.download_button("📥 전체 데이터 다운로드 (CSV)", csv, "market_analysis.csv", "text/csv")