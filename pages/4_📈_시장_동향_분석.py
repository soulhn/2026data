import streamlit as st
import pandas as pd
import sqlite3
import plotly.express as px
from collections import Counter

# ==========================================
# 1. 설정 및 데이터 로드
# ==========================================
st.set_page_config(page_title="시장 동향 분석", page_icon="📈", layout="wide")

DB_FILE = "hrd_analysis.db"

# ✅ 30개 전 컬럼 한글 매핑 (숨겨진 데이터 해제!)
COLUMN_MAP = {
    # 식별자
    'TRPR_ID': '과정ID',
    'TRPR_DEGR': '회차',
    'INST_INO': '기관ID',
    
    # 기본 정보
    'TRPR_NM': '과정명',
    'TRAINST_NM': '훈련기관명',
    'TR_STA_DT': '개설일',
    'TR_END_DT': '종료일',
    'NCS_CD': 'NCS코드',
    'TRNG_AREA_CD': '지역코드',
    
    # 인원 및 비용
    'TOT_FXNUM': '정원(명)',
    'TOT_TRCO': '훈련비(원)',
    'COURSE_MAN': '수강비(원)',
    'REAL_MAN': '실비(원)',
    'REG_COURSE_MAN': '등록인원',
    
    # 성과 지표
    'EI_EMPL_RATE_3': '취업률(3개월)',
    'EI_EMPL_RATE_6': '취업률(6개월)',
    'EI_EMPL_CNT_3': '취업인원(3개월)',
    'STDG_SCOR': '만족도(점)',
    'GRADE': '등급',
    
    # 상세 정보
    'CERTIFICATE': '관련자격증',
    'CONTENTS': '콘텐츠',
    'ADDRESS': '주소',
    'TEL_NO': '전화번호',
    'TRAIN_TARGET': '훈련유형',
    'TRAIN_TARGET_CD': '유형코드',
    'WKEND_SE': '주말구분', # 1:주중, 2:주말, 3:혼합
    
    # 파생 변수 (내부용)
    'REGION': '지역',
    'YEAR_MONTH': '개설연월'
}

@st.cache_data(ttl=3600)
def load_market_data():
    conn = sqlite3.connect(DB_FILE)
    
    # 🚀 [변경] 특정 컬럼만 뽑지 않고 '*'로 전체 다 가져옵니다!
    query = "SELECT * FROM TB_MARKET_TREND"
    
    df = pd.read_sql(query, conn)
    conn.close()
    
    # 1. 날짜 변환
    df['TR_STA_DT'] = pd.to_datetime(df['TR_STA_DT'])
    df['TR_END_DT'] = pd.to_datetime(df['TR_END_DT'], errors='coerce') # 종료일 추가
    df['YEAR_MONTH'] = df['TR_STA_DT'].dt.strftime('%Y-%m')
    
    # 2. 수치형 변환 (계산 필요한 것들 안전하게 처리)
    numeric_cols = ['TOT_TRCO', 'EI_EMPL_RATE_3', 'STDG_SCOR', 'TOT_FXNUM', 'EI_EMPL_RATE_6']
    for c in numeric_cols:
        df[c] = pd.to_numeric(df[c], errors='coerce').fillna(0)
    
    # 3. 지역 정보 추출
    df['REGION'] = df['ADDRESS'].str.split(' ').str[0]
    
    return df

with st.spinner('30만 건의 전체 데이터를 로딩 중입니다... (메모리 최적화 중) 🚀'):
    raw_df = load_market_data()

# ==========================================
# 2. 사이드바 (필터링)
# ==========================================
st.sidebar.header("🔍 분석 필터")

# 기간 필터
min_date = raw_df['TR_STA_DT'].min().date()
max_date = raw_df['TR_STA_DT'].max().date()
start_date, end_date = st.sidebar.date_input(
    "조회 기간", [min_date, max_date], min_value=min_date, max_value=max_date
)

# 지역 필터
region_list = ['전체'] + sorted(raw_df['REGION'].dropna().unique().tolist())
selected_region = st.sidebar.selectbox("지역 선택", region_list)

# NCS 필터
ncs_list = ['전체'] + sorted(raw_df['NCS_CD'].unique().tolist())
selected_ncs = st.sidebar.selectbox("NCS 코드", ncs_list)

# 검색 필터
search_keyword = st.sidebar.text_input("과정명/기관명 검색")

# --- 필터링 로직 ---
df = raw_df[
    (raw_df['TR_STA_DT'].dt.date >= start_date) & 
    (raw_df['TR_STA_DT'].dt.date <= end_date)
]

if selected_region != '전체':
    df = df[df['REGION'] == selected_region]

if selected_ncs != '전체':
    df = df[df['NCS_CD'] == selected_ncs]

if search_keyword:
    # 과정명 또는 기관명에서 검색
    df = df[
        df['TRPR_NM'].str.contains(search_keyword, case=False) | 
        df['TRAINST_NM'].str.contains(search_keyword, case=False)
    ]

# ==========================================
# 3. 메인 대시보드
# ==========================================
st.title(f"📈 IT 훈련 시장 상세 분석 ({len(df):,}건)")
st.markdown("---")

# 3.1 KPI (핵심 지표)
c1, c2, c3, c4, c5 = st.columns(5) # 컬럼 하나 더 추가!
c1.metric("총 과정 수", f"{len(df):,}개")
c2.metric("평균 훈련비", f"{int(df['TOT_TRCO'].mean()):,}원")
c3.metric("평균 정원", f"{int(df['TOT_FXNUM'].mean())}명")
c4.metric("평균 취업률", f"{df[df['EI_EMPL_RATE_3']>0]['EI_EMPL_RATE_3'].mean():.1f}%")
c5.metric("평균 만족도", f"{df[df['STDG_SCOR']>0]['STDG_SCOR'].mean():.1f}점")

st.markdown("###")

# 3.2 탭 구성
t1, t2, t3, t4 = st.tabs(["📊 트렌드 분석", "🏢 경쟁사 분석", "☁️ 키워드 분석", "📑 전체 데이터 조회"])

with t1:
    col1, col2 = st.columns([2, 1])
    with col1:
        st.subheader("월별 개설 추이")
        trend = df.groupby('YEAR_MONTH').size().reset_index(name='COUNT')
        st.plotly_chart(px.line(trend, x='YEAR_MONTH', y='COUNT', markers=True), use_container_width=True)
    with col2:
        st.subheader("지역별 비중")
        reg_cnt = df['REGION'].value_counts().reset_index()
        reg_cnt.columns = ['지역', '개수']
        st.plotly_chart(px.pie(reg_cnt, values='개수', names='지역', hole=0.4), use_container_width=True)

with t2:
    st.subheader("훈련기관 순위 (개설 수 기준)")
    top_inst = df['TRAINST_NM'].value_counts().head(15).reset_index()
    top_inst.columns = ['기관명', '개설수']
    st.plotly_chart(px.bar(top_inst, y='기관명', x='개설수', orientation='h', text='개설수', color='개설수'), use_container_width=True)

with t3:
    st.subheader("과정명 키워드 워드클라우드")
    text = " ".join(df['TRPR_NM'].dropna().astype(str))
    stops = ['과정', '양성', '취업', '실무', '및', '위한', '기반', '활용', '개발자', 'A', 'B', '수료', '반', '취득']
    words = [w for w in text.split() if len(w) > 1 and w not in stops]
    kwd = pd.DataFrame(Counter(words).most_common(20), columns=['키워드', '빈도'])
    st.plotly_chart(px.bar(kwd, x='키워드', y='빈도', color='빈도'), use_container_width=True)

# ✅ [업그레이드] 모든 컬럼 다 보여주기
with t4:
    st.subheader(f"상세 데이터 리스트 (총 {len(df):,}건)")
    
    # 1. 보기 좋게 컬럼 이름 변경 (30개 컬럼 전체 적용)
    # DB에 있지만 MAP에 없는 컬럼은 그대로 영어로 나옵니다.
    display_df = df.rename(columns=COLUMN_MAP)
    
    # 2. 필요한 컬럼 순서 정렬 (보고 싶은 순서대로)
    # 이 리스트에 없는 컬럼은 뒤에 붙습니다.
    priority_cols = [
        '과정명', '훈련기관명', '개설일', '종료일', '지역', 
        '훈련비(원)', '정원(명)', '취업률(3개월)', '만족도(점)', 
        '주말구분', '전화번호', '주소'
    ]
    # 실제 존재하는 컬럼만 필터링
    final_cols = [c for c in priority_cols if c in display_df.columns]
    # 나머지 컬럼들 붙이기
    remaining_cols = [c for c in display_df.columns if c not in final_cols]
    final_display = display_df[final_cols + remaining_cols]

    # 3. 엑셀 다운로드 기능
    st.warning("⚠️ 브라우저 성능을 위해 상위 1,000개만 미리보기로 표시합니다. 전체 데이터는 아래 버튼으로 다운로드하세요.")
    st.dataframe(final_display.head(1000), use_container_width=True)
    
    # 4. CSV 다운로드 버튼 (전체 데이터)
    csv = final_display.to_csv(index=False).encode('utf-8-sig') # 엑셀 깨짐 방지(utf-8-sig)
    st.download_button(
        "📥 전체 데이터 다운로드 (CSV)",
        csv,
        f"market_data_{start_date}_{end_date}.csv",
        "text/csv",
        key='download-csv'
    )