#streamlit run dashboard.py

import streamlit as st
import pandas as pd
import sqlite3
import plotly.express as px
import plotly.graph_objects as go

# ==========================================
# 1. 설정 및 데이터 로드
# ==========================================
st.set_page_config(
    page_title="HRD 훈련과정 분석 대시보드",
    page_icon="📊",
    layout="wide"
)

DB_FILE = "hrd_analysis.db"

def get_connection():
    return sqlite3.connect(DB_FILE)

# 캐싱을 통해 속도 향상 (데이터가 변하면 버튼으로 캐시 삭제)

def load_data():
    conn = get_connection()
    
    # 1. 과정 마스터 정보 (INST_INO 제거함)
    df_course = pd.read_sql("""
        SELECT TRPR_DEGR, TRPR_NM, TR_STA_DT, TR_END_DT, FINI_CNT, REAL_EMPL_RATE 
        FROM TB_COURSE_MASTER 
        ORDER BY CAST(TRPR_DEGR AS INTEGER)
    """, conn)
    
    # 2. 훈련생 정보
    df_trainee = pd.read_sql("SELECT * FROM TB_TRAINEE_INFO", conn)
    
    # 3. 출결 로그
    df_log = pd.read_sql("SELECT * FROM TB_ATTENDANCE_LOG", conn)
    
    conn.close()
    return df_course, df_trainee, df_log

# ==========================================
# 2. 사이드바 (필터링 조건)
# ==========================================
st.sidebar.title("🎛️ 분석 조건 설정")

# 데이터 로드
df_course, df_trainee, df_log = load_data()

# 회차 선택
course_options = df_course['TRPR_DEGR'].unique()
selected_degr = st.sidebar.selectbox("훈련 회차를 선택하세요", course_options, index=0)

# 선택된 회차 정보 필터링
course_info = df_course[df_course['TRPR_DEGR'] == selected_degr].iloc[0]
target_logs = df_log[df_log['TRPR_DEGR'] == selected_degr]
target_trainees = df_trainee[df_trainee['TRPR_DEGR'] == selected_degr]

# ==========================================
# 3. 메인 대시보드 화면
# ==========================================
st.title(f"📊 {course_info['TRPR_NM']}")
st.markdown(f"**회차:** {selected_degr}회차 | **기간:** {course_info['TR_STA_DT']} ~ {course_info['TR_END_DT']}")

# 탭 구성
tab1, tab2, tab3 = st.tabs(["📈 종합 현황", "📅 일자별 출석 분석", "🚨 훈련생 관리"])

# --- TAB 1: 종합 현황 ---
with tab1:
    col1, col2, col3, col4 = st.columns(4)
    
    total_trainees = len(target_trainees)
    avg_empl_rate = course_info['REAL_EMPL_RATE']
    
    # 출석률 계산
    if not target_logs.empty:
        total_days = len(target_logs['ATEND_DT'].unique())
        attend_count = len(target_logs[target_logs['ATEND_STATUS'].isin(['출석', '지각'])])
        total_log_count = len(target_logs)
        avg_attend_rate = (attend_count / total_log_count) * 100 if total_log_count > 0 else 0
    else:
        avg_attend_rate = 0

    col1.metric("총 훈련생", f"{total_trainees}명")
    col2.metric("현재 평균 출석률", f"{avg_attend_rate:.1f}%")
    col3.metric("과정 취업률", f"{avg_empl_rate if avg_empl_rate else '-'}%", delta_color="normal")
    col4.metric("수집된 로그 수", f"{len(target_logs):,}건")

    st.divider()
    
    # 전체 회차 취업률 비교 차트
    st.subheader("🏆 전체 회차별 취업률 비교")
    fig_bar = px.bar(
        df_course[df_course['REAL_EMPL_RATE'].notnull()], 
        x='TRPR_DEGR', 
        y='REAL_EMPL_RATE',
        color='REAL_EMPL_RATE',
        labels={'TRPR_DEGR': '회차', 'REAL_EMPL_RATE': '취업률(%)'},
        title="회차별 취업률 성과",
        text_auto=True
    )
    # 선택된 회차 강조
    fig_bar.update_traces(marker_color=['red' if x == selected_degr else 'blue' for x in df_course['TRPR_DEGR']])
    st.plotly_chart(fig_bar, use_container_width=True)

# --- TAB 2: 일자별 출석 분석 ---
with tab2:
    if not target_logs.empty:
        st.subheader("📅 일자별 출석률 추이")
        
        # 일자별 집계
        daily_stats = target_logs.groupby('ATEND_DT')['ATEND_STATUS'].apply(
            lambda x: (x.isin(['출석', '지각']).sum() / len(x)) * 100
        ).reset_index(name='출석률')
        
        fig_line = px.line(
            daily_stats, x='ATEND_DT', y='출석률', markers=True,
            title=f"{selected_degr}회차 일자별 출석률 변화"
        )
        fig_line.update_yaxes(range=[0, 105]) # 0~100% 고정
        st.plotly_chart(fig_line, use_container_width=True)
        
        # 요일별 결석 분석
        st.subheader("📊 요일별 결석 빈도")
        absent_logs = target_logs[target_logs['ATEND_STATUS'] == '결석']
        if not absent_logs.empty:
            day_counts = absent_logs['DAY_NM'].value_counts().reset_index()
            day_counts.columns = ['요일', '결석수']
            
            # 요일 정렬 순서 지정
            day_order = ['월', '화', '수', '목', '금', '토', '일']
            fig_day = px.bar(
                day_counts, x='요일', y='결석수', 
                category_orders={'요일': day_order},
                color='결석수', color_continuous_scale='Reds'
            )
            st.plotly_chart(fig_day, use_container_width=True)
        else:
            st.info("결석 데이터가 없습니다.")

# --- TAB 3: 훈련생 관리 ---
with tab3:
    st.subheader("🔍 훈련생별 상세 현황")
    
    if not target_logs.empty:
        # 훈련생별 통계 계산
        trainee_stats = target_logs.groupby(['TRNEE_ID', 'TRPR_DEGR']).apply(
            lambda x: pd.Series({
                '출석': (x['ATEND_STATUS'] == '출석').sum(),
                '지각': (x['ATEND_STATUS'] == '지각').sum(),
                '결석': (x['ATEND_STATUS'] == '결석').sum(),
                '총수업일': len(x)
            }),
            include_groups=False
        ).reset_index()
        
        # 이름 매핑
        trainee_stats = trainee_stats.merge(df_trainee[['TRNEE_ID', 'TRNEE_NM']], on='TRNEE_ID', how='left')
        
        # 출석률 계산
        trainee_stats['출석률(%)'] = round((trainee_stats['출석'] + trainee_stats['지각']) / trainee_stats['총수업일'] * 100, 1)
        
        # 위험군 필터 (출석률 80% 미만)
        risk_trainees = trainee_stats[trainee_stats['출석률(%)'] < 80]
        
        if not risk_trainees.empty:
            st.error(f"🚨 중도탈락 위험군이 {len(risk_trainees)}명 감지되었습니다!")
            st.dataframe(risk_trainees[['TRNEE_NM', '출석률(%)', '결석', '총수업일']].sort_values('출석률(%)'))
        else:
            st.success("모든 훈련생의 출석률이 양호합니다 (80% 이상).")
            
        st.write("📋 전체 훈련생 목록")
        st.dataframe(
            trainee_stats[['TRNEE_NM', '출석률(%)', '출석', '지각', '결석', '총수업일']]
            .sort_values('출석률(%)', ascending=False),
            use_container_width=True
        )

# ==========================================
# 4. 실시간 새로고침 버튼
# ==========================================
st.sidebar.markdown("---")
if st.sidebar.button("🔄 데이터 새로고침"):
    st.cache_data.clear()
    st.rerun()