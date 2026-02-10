import streamlit as st
import pandas as pd
import altair as alt
from datetime import datetime
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils import load_data, calculate_age_at_training, safe_float, check_password

st.set_page_config(page_title="기수별 성과 분석", page_icon="📊", layout="wide")
check_password()
st.title("📊 기수별 성과 심층 분석")
st.markdown("종료된 과정의 **수료율, 취업률, 출석 패턴**을 다각도로 분석합니다.")


# ==========================================
# 데이터 로드 함수
# ==========================================
@st.cache_data(ttl=600)
def get_course_list():
    today = datetime.now().strftime('%Y-%m-%d')
    return load_data(
        "SELECT DISTINCT TRPR_DEGR, TRPR_NM, TR_STA_DT, TR_END_DT "
        "FROM TB_COURSE_MASTER WHERE TR_END_DT < ? "
        "ORDER BY CAST(TRPR_DEGR AS INTEGER) DESC",
        params=[today],
    )


@st.cache_data(ttl=600)
def get_analysis_data(degr):
    course_df = load_data(
        "SELECT * FROM TB_COURSE_MASTER WHERE TRPR_DEGR = ?", params=[degr]
    )
    trainee_df = load_data(
        "SELECT * FROM TB_TRAINEE_INFO WHERE TRPR_DEGR = ?", params=[degr]
    )
    if not course_df.empty and not trainee_df.empty:
        start_date = course_df.iloc[0]['TR_STA_DT']
        trainee_df['나이'] = trainee_df['BIRTH_DATE'].apply(
            lambda x: calculate_age_at_training(x, start_date)
        )
        trainee_df['연령대'] = trainee_df['나이'].apply(
            lambda x: f"{int(x // 10 * 10)}대" if pd.notnull(x) else "미상"
        )

    log_query = (
        "SELECT TRNEE_ID, "
        "COUNT(*) as 총_로그_수, "
        "SUM(CASE WHEN ATEND_STATUS = '출석' THEN 1 ELSE 0 END) as 출석_횟수, "
        "SUM(CASE WHEN ATEND_STATUS = '결석' THEN 1 ELSE 0 END) as 결석_횟수, "
        "SUM(CASE WHEN ATEND_STATUS = '지각' THEN 1 ELSE 0 END) as 지각_횟수, "
        "SUM(CASE WHEN ATEND_STATUS = '조퇴' THEN 1 ELSE 0 END) as 조퇴_횟수, "
        "SUM(CASE WHEN ATEND_STATUS = '외출' THEN 1 ELSE 0 END) as 외출_횟수, "
        "SUM(CASE WHEN ATEND_STATUS IN ('지각', '조퇴', '외출') THEN 1 ELSE 0 END) as 지각_조퇴_횟수 "
        "FROM TB_ATTENDANCE_LOG WHERE TRPR_DEGR = ? GROUP BY TRNEE_ID"
    )
    attend_stats = load_data(log_query, params=[degr])
    if not attend_stats.empty:
        trainee_df = pd.merge(trainee_df, attend_stats, on='TRNEE_ID', how='left').fillna(0)
    return course_df, trainee_df


@st.cache_data(ttl=600)
def get_daily_attendance_pattern(degr):
    return load_data(
        "SELECT DAY_NM, ATEND_STATUS, COUNT(*) as CNT "
        "FROM TB_ATTENDANCE_LOG WHERE TRPR_DEGR = ? "
        "GROUP BY DAY_NM, ATEND_STATUS",
        params=[degr],
    )


@st.cache_data(ttl=600)
def get_late_times(degr):
    return load_data(
        "SELECT IN_TIME FROM TB_ATTENDANCE_LOG "
        "WHERE TRPR_DEGR = ? AND ATEND_STATUS = '지각'",
        params=[degr],
    )


@st.cache_data(ttl=600)
def get_all_degr_attendance():
    return load_data(
        "SELECT TRPR_DEGR, ATEND_STATUS, COUNT(*) as CNT "
        "FROM TB_ATTENDANCE_LOG GROUP BY TRPR_DEGR, ATEND_STATUS"
    )


@st.cache_data(ttl=600)
def get_all_course_master():
    today = datetime.now().strftime('%Y-%m-%d')
    return load_data(
        "SELECT * FROM TB_COURSE_MASTER WHERE TR_END_DT < ? "
        "ORDER BY CAST(TRPR_DEGR AS INTEGER)",
        params=[today],
    )


# ==========================================
# 메인 로직
# ==========================================
course_list = get_course_list()
if course_list.empty:
    st.warning("분석할 수 있는 종료된 과정 데이터가 없습니다.")
    st.stop()

mode = st.radio("분석 모드", ["개별 기수 분석", "전체 기수 비교"], horizontal=True)
st.divider()


# ==========================================
# 모드 1: 개별 기수 분석
# ==========================================
if mode == "개별 기수 분석":
    with st.sidebar:
        st.header("🔍 분석 대상 선택")
        selected_degr = st.selectbox(
            "회차(기수)를 선택하세요",
            course_list['TRPR_DEGR'].unique(),
            format_func=lambda x: f"{x}회차",
        )
        sel_info = course_list[course_list['TRPR_DEGR'] == selected_degr].iloc[0]
        st.info(
            f"**과정명:** {sel_info['TRPR_NM']}\n\n"
            f"**기간:** {sel_info['TR_STA_DT']} ~ {sel_info['TR_END_DT']}"
        )

    master_df, students_df = get_analysis_data(selected_degr)
    if master_df.empty:
        st.error("해당 회차의 마스터 데이터가 없습니다.")
        st.stop()

    # --- 성과 요약 ---
    st.subheader(f"🏆 {selected_degr}회차 종합 성적표")
    row = master_df.iloc[0]
    total_std = row['TOT_PAR_MKS'] if row.get('TOT_PAR_MKS', 0) > 0 else len(students_df)
    fini_std = row['FINI_CNT'] if row.get('FINI_CNT') else 0
    dropout_std = total_std - fini_std
    ei_rate = safe_float(row.get('EI_EMPL_RATE_6'))
    hrd_rate = safe_float(row.get('HRD_EMPL_RATE_6'))
    total_empl_rate = ei_rate + hrd_rate

    mc1, mc2, mc3, mc4 = st.columns(4)
    mc1.metric(
        "수료율",
        f"{(fini_std / total_std * 100):.1f}%" if total_std > 0 else "0%",
        f"{fini_std}/{total_std}명",
    )
    mc2.metric("총 취업률 (6개월)", f"{total_empl_rate:.1f}%", help="고용보험 + HRD 합산")
    mc3.metric("중도 탈락", f"{dropout_std}명", delta_color="inverse")
    mc4.metric(
        "평균 결석일",
        f"{students_df['결석_횟수'].mean():.1f}일" if '결석_횟수' in students_df.columns else "-",
    )
    st.divider()

    # --- 5개 탭 ---
    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "👥 인구통계 분석",
        "📅 요일별 출결 패턴",
        "⏰ 시간대별 지각 분포",
        "📉 출결/이탈 분석",
        "📋 학생별 출결 현황",
    ])

    # [Tab 1] 인구통계 분석
    with tab1:
        left, right = st.columns(2)
        with left:
            st.markdown("##### 🎂 연령대별 분포")
            if '연령대' in students_df.columns:
                st.altair_chart(
                    alt.Chart(students_df).mark_arc(innerRadius=50).encode(
                        theta=alt.Theta("count()", stack=True),
                        color=alt.Color("연령대", scale=alt.Scale(scheme='category20')),
                        tooltip=["연령대", "count()"],
                    ).properties(height=300),
                    use_container_width=True,
                )
        with right:
            st.markdown("##### 🏷️ 훈련생 유형별 분포")
            if 'TRNEE_TYPE' in students_df.columns:
                tc = students_df['TRNEE_TYPE'].value_counts().reset_index()
                tc.columns = ['유형', '인원']
                st.altair_chart(
                    alt.Chart(tc).mark_bar().encode(
                        x='인원:Q',
                        y=alt.Y('유형:N', sort='-x'),
                        color=alt.value('orange'),
                        tooltip=['유형', '인원'],
                    ).properties(height=300),
                    use_container_width=True,
                )

    # [Tab 2] 요일별 출결 패턴 (신규)
    with tab2:
        st.markdown("##### 📅 요일별 출결 현황 히트맵")
        st.caption("요일에 따라 결석/지각/조퇴 패턴이 다를 수 있습니다.")
        daily_df = get_daily_attendance_pattern(selected_degr)
        if daily_df.empty:
            st.info("출결 로그 데이터가 없습니다.")
        else:
            day_order = ['월', '화', '수', '목', '금', '토', '일']
            daily_df['요일'] = daily_df['DAY_NM'].str[0]
            daily_agg = daily_df.groupby(['요일', 'ATEND_STATUS'], as_index=False)['CNT'].sum()

            base = alt.Chart(daily_agg).encode(
                x=alt.X('요일:N', sort=day_order, title='요일'),
                y=alt.Y('ATEND_STATUS:N', title='출결 상태'),
            )
            heatmap = base.mark_rect().encode(
                color=alt.Color('CNT:Q', scale=alt.Scale(scheme='orangered'), title='건수'),
                tooltip=['요일', 'ATEND_STATUS', 'CNT'],
            )
            median_val = float(daily_agg['CNT'].median())
            text = base.mark_text(baseline='middle').encode(
                text='CNT:Q',
                color=alt.condition(
                    alt.datum.CNT > median_val,
                    alt.value('white'),
                    alt.value('black'),
                ),
            )
            st.altair_chart(
                (heatmap + text).properties(height=300), use_container_width=True
            )

            st.markdown("##### 요일별 결석 건수")
            absent_df = daily_agg[daily_agg['ATEND_STATUS'] == '결석']
            if not absent_df.empty:
                st.altair_chart(
                    alt.Chart(absent_df).mark_bar(color='#e74c3c').encode(
                        x=alt.X('요일:N', sort=day_order, title='요일'),
                        y=alt.Y('CNT:Q', title='결석 건수'),
                        tooltip=['요일', 'CNT'],
                    ).properties(height=250),
                    use_container_width=True,
                )
            else:
                st.success("결석 기록이 없습니다.")

    # [Tab 3] 시간대별 지각 분포 (신규)
    with tab3:
        st.markdown("##### ⏰ 지각 입실 시간 분포")
        st.caption("지각으로 기록된 훈련생의 입실 시간 분포입니다.")
        late_df = get_late_times(selected_degr)
        if late_df.empty:
            st.info("지각 기록이 없습니다.")
        else:
            late_df = late_df[late_df['IN_TIME'].notna() & (late_df['IN_TIME'] != '')]
            if late_df.empty:
                st.info("지각 기록에 입실 시간이 기록되지 않았습니다.")
            else:
                late_df['시간'] = late_df['IN_TIME'].str.split(':').str[0]
                late_df['분'] = late_df['IN_TIME'].str.split(':').str[1]
                valid_mask = late_df['시간'].str.isdigit() & late_df['분'].str.isdigit()
                late_df = late_df[valid_mask].copy()
                late_df['시간'] = late_df['시간'].astype(int)
                late_df['분'] = late_df['분'].astype(int)
                late_df['입실시각_분'] = late_df['시간'] * 60 + late_df['분']
                if late_df.empty:
                    st.info("유효한 입실 시간 데이터가 없습니다.")
                else:
                    ca, cb = st.columns(2)
                    with ca:
                        st.altair_chart(
                            alt.Chart(late_df).mark_bar(color='#e67e22').encode(
                                x=alt.X('IN_TIME:N', sort='ascending', title='입실 시간'),
                                y=alt.Y('count()', title='건수'),
                                tooltip=['IN_TIME', 'count()'],
                            ).properties(height=350, title='지각 입실 시간별 건수'),
                            use_container_width=True,
                        )
                    with cb:
                        st.markdown("##### 지각 통계")
                        avg_min = late_df['입실시각_분'].mean()
                        avg_h, avg_m = divmod(int(avg_min), 60)
                        st.metric("평균 입실 시간", f"{avg_h:02d}:{avg_m:02d}")
                        st.metric("총 지각 건수", f"{len(late_df)}건")
                        latest_row = late_df.loc[late_df['입실시각_분'].idxmax()]
                        st.metric("가장 늦은 입실", latest_row['IN_TIME'])

    # [Tab 4] 출결 및 이탈 분석 (기존)
    with tab4:
        st.markdown("##### 📍 출석률과 수료 상태의 상관관계")
        st.caption("결석이 많을수록 '중도탈락'이나 '제적' 상태일 확률이 높습니다.")
        if '결석_횟수' in students_df.columns:
            st.altair_chart(
                alt.Chart(students_df).mark_circle(size=60).encode(
                    x=alt.X('나이:Q', scale=alt.Scale(domain=[15, 50])),
                    y=alt.Y('결석_횟수:Q', title='총 결석 일수'),
                    color='TRNEE_STATUS',
                    tooltip=['TRNEE_NM', '나이', '결석_횟수', 'TRNEE_STATUS'],
                ).interactive().properties(height=400),
                use_container_width=True,
            )
            risk = students_df[
                (students_df['결석_횟수'] >= 3)
                & (students_df['TRNEE_STATUS'] != '수료')
            ]
            if not risk.empty:
                st.warning(
                    f"⚠️ **출석 불량 위험군 ({len(risk)}명):** "
                    "결석이 3일 이상 기록된 미수료 학생들입니다."
                )
                st.dataframe(
                    risk[['TRNEE_NM', '나이', '결석_횟수', '지각_조퇴_횟수', 'TRNEE_STATUS']],
                    hide_index=True,
                )

    # [Tab 5] 학생별 출결 현황 (기존 명부 확장)
    with tab5:
        st.markdown("##### 📋 학생별 상세 출결 현황")
        st.caption("각 열 헤더를 클릭하여 정렬할 수 있습니다.")
        display_cols = ['TRNEE_NM', '나이', '연령대', 'TRNEE_STATUS']
        stat_cols = ['출석_횟수', '결석_횟수', '지각_횟수', '조퇴_횟수', '외출_횟수', '총_로그_수']
        avail = display_cols + [c for c in stat_cols if c in students_df.columns]
        if 'TRNEE_TYPE' in students_df.columns:
            avail.insert(4, 'TRNEE_TYPE')
        if '출석_횟수' in students_df.columns and '총_로그_수' in students_df.columns:
            students_df['출석률(%)'] = (
                students_df['출석_횟수'] / students_df['총_로그_수'] * 100
            ).round(1)
            avail.append('출석률(%)')
        st.dataframe(
            students_df[avail],
            column_config={
                "출석_횟수": st.column_config.NumberColumn("출석(일)", format="%d"),
                "결석_횟수": st.column_config.NumberColumn("결석(일)", format="%d"),
                "지각_횟수": st.column_config.NumberColumn("지각(일)", format="%d"),
                "조퇴_횟수": st.column_config.NumberColumn("조퇴(일)", format="%d"),
                "외출_횟수": st.column_config.NumberColumn("외출(일)", format="%d"),
                "총_로그_수": st.column_config.NumberColumn("훈련일수", format="%d"),
                "출석률(%)": st.column_config.ProgressColumn(
                    "출석률", min_value=0, max_value=100, format="%.1f%%"
                ),
            },
            use_container_width=True,
            hide_index=True,
        )


# ==========================================
# 모드 2: 전체 기수 비교
# ==========================================
else:
    st.subheader("📊 전체 기수 비교 분석")
    st.caption("수료된 전체 기수의 성과를 한눈에 비교합니다.")
    all_master = get_all_course_master()
    if all_master.empty:
        st.warning("종료된 과정 데이터가 없습니다.")
        st.stop()

    all_master['기수'] = all_master['TRPR_DEGR'].astype(str) + '회차'
    all_master['수료율'] = (all_master['FINI_CNT'] / all_master['TOT_PAR_MKS'] * 100).round(1)
    all_master['EI_취업률_6'] = all_master['EI_EMPL_RATE_6'].apply(safe_float)
    all_master['HRD_취업률_6'] = all_master['HRD_EMPL_RATE_6'].apply(safe_float)
    all_master['총_취업률'] = all_master['EI_취업률_6'] + all_master['HRD_취업률_6']
    degr_order = all_master.sort_values('TRPR_DEGR')['기수'].tolist()

    lc, rc = st.columns(2)
    with lc:
        st.markdown("##### 기수별 수료율")
        st.altair_chart(
            alt.Chart(all_master).mark_bar(color='#3498db').encode(
                x=alt.X('기수:N', sort=degr_order, title='기수'),
                y=alt.Y('수료율:Q', title='수료율 (%)'),
                tooltip=['기수', '수료율', 'FINI_CNT', 'TOT_PAR_MKS'],
            ).properties(height=350),
            use_container_width=True,
        )
    with rc:
        st.markdown("##### 기수별 총 취업률 (6개월)")
        st.altair_chart(
            alt.Chart(all_master).mark_bar(color='#2ecc71').encode(
                x=alt.X('기수:N', sort=degr_order, title='기수'),
                y=alt.Y('총_취업률:Q', title='취업률 (%)'),
                tooltip=['기수', '총_취업률', 'EI_취업률_6', 'HRD_취업률_6'],
            ).properties(height=350),
            use_container_width=True,
        )

    st.divider()
    all_attend = get_all_degr_attendance()
    if not all_attend.empty:
        all_attend['기수'] = all_attend['TRPR_DEGR'].astype(str) + '회차'
        total_per = all_attend.groupby('기수')['CNT'].sum().reset_index()
        total_per.columns = ['기수', '총건수']
        absent_per = all_attend[all_attend['ATEND_STATUS'] == '결석'][['기수', 'CNT']].copy()
        absent_per.columns = ['기수', '결석건수']
        attend_per = all_attend[all_attend['ATEND_STATUS'] == '출석'][['기수', 'CNT']].copy()
        attend_per.columns = ['기수', '출석건수']
        comp = (
            total_per
            .merge(absent_per, on='기수', how='left')
            .merge(attend_per, on='기수', how='left')
            .fillna(0)
        )
        comp['출석률'] = (comp['출석건수'] / comp['총건수'] * 100).round(1)

        lc2, rc2 = st.columns(2)
        with lc2:
            st.markdown("##### 기수별 결석 건수")
            st.altair_chart(
                alt.Chart(comp).mark_bar(color='#e74c3c').encode(
                    x=alt.X('기수:N', sort=degr_order, title='기수'),
                    y=alt.Y('결석건수:Q', title='결석 건수'),
                    tooltip=['기수', '결석건수', '총건수'],
                ).properties(height=350),
                use_container_width=True,
            )
        with rc2:
            st.markdown("##### 기수별 출석률 추이")
            st.altair_chart(
                alt.Chart(comp).mark_line(
                    point=alt.OverlayMarkDef(size=80), color='#3498db'
                ).encode(
                    x=alt.X('기수:N', sort=degr_order, title='기수'),
                    y=alt.Y('출석률:Q', title='출석률 (%)', scale=alt.Scale(domain=[80, 100])),
                    tooltip=['기수', '출석률'],
                ).properties(height=350),
                use_container_width=True,
            )

    st.markdown("##### 종합 비교 테이블")
    summary_cols = [
        '기수', 'TRPR_NM', 'TR_STA_DT', 'TR_END_DT',
        'TOT_PAR_MKS', 'FINI_CNT', '수료율', '총_취업률',
    ]
    st.dataframe(
        all_master[summary_cols].rename(columns={
            'TRPR_NM': '과정명',
            'TR_STA_DT': '시작일',
            'TR_END_DT': '종료일',
            'TOT_PAR_MKS': '현원',
            'FINI_CNT': '수료인원',
            '수료율': '수료율(%)',
            '총_취업률': '취업률(%)',
        }),
        column_config={
            "수료율(%)": st.column_config.NumberColumn(format="%.1f%%"),
            "취업률(%)": st.column_config.NumberColumn(format="%.1f%%"),
        },
        use_container_width=True,
        hide_index=True,
    )
