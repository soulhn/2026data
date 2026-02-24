import streamlit as st
import pandas as pd
import altair as alt
import plotly.graph_objects as go
import plotly.express as px
from datetime import datetime
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils import (
    load_data, calculate_age_at_training, safe_float, check_password,
    calc_attendance_rate_from_counts, calc_employment_rate_6, parse_empl_rate,
    is_completed, parse_time_to_minutes, NOT_ATTEND_STATUSES,
)
from config import CACHE_TTL_DEFAULT, EMPL_CODE_MAP, RISK_ABSENT, TRNEE_TYPE_MAP

st.set_page_config(page_title="과정 성과 분석", page_icon="📊", layout="wide")
check_password()
st.title("📊 과정 성과 분석")
st.markdown("종료된 과정의 **수료율, 취업률, 출석 패턴**을 다각도로 분석합니다.")


@st.cache_data(ttl=CACHE_TTL_DEFAULT)
def get_course_list():
    today = datetime.now().strftime('%Y-%m-%d')
    return load_data(
        "SELECT DISTINCT TRPR_DEGR, TRPR_NM, TR_STA_DT, TR_END_DT "
        "FROM TB_COURSE_MASTER WHERE TR_END_DT < ? "
        "ORDER BY CAST(TRPR_DEGR AS INTEGER) DESC",
        params=[today],
    )


@st.cache_data(ttl=CACHE_TTL_DEFAULT)
def get_analysis_data(degr):
    course_df = load_data(
        "SELECT * FROM TB_COURSE_MASTER WHERE TRPR_DEGR = ?", params=[degr]
    )
    trainee_df = load_data(
        "SELECT * FROM TB_TRAINEE_INFO WHERE TRPR_DEGR = ?", params=[degr]
    )
    if not course_df.empty and not trainee_df.empty:
        trainee_df['TRNEE_TYPE'] = trainee_df['TRNEE_TYPE'].map(TRNEE_TYPE_MAP).fillna(trainee_df['TRNEE_TYPE'])
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
        "SUM(CASE WHEN ATEND_STATUS IN ('지각', '조퇴', '외출') THEN 1 ELSE 0 END) as 지각_조퇴_횟수, "
        "SUM(CASE WHEN ATEND_STATUS = '중도탈락미출석' THEN 1 ELSE 0 END) as 중도탈락미출석_횟수, "
        "SUM(CASE WHEN ATEND_STATUS = '100분의50미만출석' THEN 1 ELSE 0 END) as lt50_횟수 "
        "FROM TB_ATTENDANCE_LOG WHERE TRPR_DEGR = ? GROUP BY TRNEE_ID"
    )
    attend_stats = load_data(log_query, params=[degr])
    if not attend_stats.empty:
        trainee_df = pd.merge(trainee_df, attend_stats, on='TRNEE_ID', how='left').fillna(0)
    return course_df, trainee_df


@st.cache_data(ttl=CACHE_TTL_DEFAULT)
def get_daily_attendance_pattern(degr):
    return load_data(
        "SELECT DAY_NM, ATEND_STATUS, COUNT(*) as CNT "
        "FROM TB_ATTENDANCE_LOG WHERE TRPR_DEGR = ? "
        "GROUP BY DAY_NM, ATEND_STATUS",
        params=[degr],
    )


@st.cache_data(ttl=CACHE_TTL_DEFAULT)
def get_late_times(degr):
    return load_data(
        "SELECT IN_TIME FROM TB_ATTENDANCE_LOG "
        "WHERE TRPR_DEGR = ? AND ATEND_STATUS = '지각'",
        params=[degr],
    )


@st.cache_data(ttl=CACHE_TTL_DEFAULT)
def get_early_leave_times(degr):
    return load_data(
        "SELECT OUT_TIME FROM TB_ATTENDANCE_LOG "
        "WHERE TRPR_DEGR = ? AND ATEND_STATUS = '조퇴'",
        params=[degr],
    )


@st.cache_data(ttl=CACHE_TTL_DEFAULT)
def get_full_attendance_with_status(degr):
    return load_data(
        "SELECT a.TRNEE_ID, a.ATEND_DT, a.ATEND_STATUS, "
        "t.TRNEE_STATUS, t.TRNEE_NM "
        "FROM TB_ATTENDANCE_LOG a "
        "JOIN TB_TRAINEE_INFO t ON a.TRNEE_ID = t.TRNEE_ID AND a.TRPR_DEGR = t.TRPR_DEGR "
        "WHERE a.TRPR_DEGR = ? "
        "ORDER BY a.TRNEE_ID, a.ATEND_DT",
        params=[degr],
    )


@st.cache_data(ttl=CACHE_TTL_DEFAULT)
def get_stay_duration(degr):
    """체류시간 분석용 데이터"""
    return load_data(
        "SELECT TRNEE_ID, ATEND_DT, IN_TIME, OUT_TIME "
        "FROM TB_ATTENDANCE_LOG "
        "WHERE TRPR_DEGR = ? AND IN_TIME IS NOT NULL AND IN_TIME != '' "
        "AND OUT_TIME IS NOT NULL AND OUT_TIME != ''",
        params=[degr],
    )


@st.cache_data(ttl=CACHE_TTL_DEFAULT)
def get_all_degr_attendance():
    return load_data(
        "SELECT TRPR_DEGR, ATEND_STATUS, COUNT(*) as CNT "
        "FROM TB_ATTENDANCE_LOG GROUP BY TRPR_DEGR, ATEND_STATUS"
    )


@st.cache_data(ttl=CACHE_TTL_DEFAULT)
def get_all_course_master():
    today = datetime.now().strftime('%Y-%m-%d')
    return load_data(
        "SELECT * FROM TB_COURSE_MASTER WHERE TR_END_DT < ? "
        "ORDER BY CAST(TRPR_DEGR AS INTEGER)",
        params=[today],
    )


@st.cache_data(ttl=CACHE_TTL_DEFAULT)
def get_all_trainee_demographics():
    today = datetime.now().strftime('%Y-%m-%d')
    return load_data(
        "SELECT t.TRPR_DEGR, t.BIRTH_DATE, c.TR_STA_DT, t.TRNEE_TYPE "
        "FROM TB_TRAINEE_INFO t "
        "JOIN TB_COURSE_MASTER c ON t.TRPR_ID = c.TRPR_ID AND t.TRPR_DEGR = c.TRPR_DEGR "
        "WHERE c.TR_END_DT < ?",
        params=[today],
    )


@st.cache_data(ttl=CACHE_TTL_DEFAULT)
def get_all_degr_absent_avg():
    today = datetime.now().strftime('%Y-%m-%d')
    return load_data(
        "SELECT a.TRPR_DEGR, "
        "CAST(SUM(CASE WHEN a.ATEND_STATUS = '결석' THEN 1 ELSE 0 END) AS FLOAT) / "
        "NULLIF(COUNT(DISTINCT a.TRNEE_ID), 0) AS 평균_결석일 "
        "FROM TB_ATTENDANCE_LOG a "
        "JOIN TB_COURSE_MASTER c ON a.TRPR_ID = c.TRPR_ID AND a.TRPR_DEGR = c.TRPR_DEGR "
        "WHERE c.TR_END_DT < ? "
        "GROUP BY a.TRPR_DEGR",
        params=[today],
    )


@st.cache_data(ttl=CACHE_TTL_DEFAULT)
def get_dropout_timing():
    """이탈 타이밍 분석용: 중도탈락자의 마지막 출석일 + 개강일."""
    today = datetime.now().strftime('%Y-%m-%d')
    return load_data(
        "SELECT a.TRPR_DEGR, a.TRNEE_ID, "
        "MAX(CASE WHEN a.ATEND_STATUS != '중도탈락미출석' THEN a.ATEND_DT END) as LAST_ATTEND_DT, "
        "c.TR_STA_DT, c.TOT_PAR_MKS "
        "FROM TB_ATTENDANCE_LOG a "
        "JOIN TB_TRAINEE_INFO t ON a.TRNEE_ID = t.TRNEE_ID AND a.TRPR_DEGR = t.TRPR_DEGR "
        "JOIN TB_COURSE_MASTER c ON a.TRPR_ID = c.TRPR_ID AND a.TRPR_DEGR = c.TRPR_DEGR "
        "WHERE t.TRNEE_STATUS LIKE ? AND c.TR_END_DT < ? "
        "GROUP BY a.TRPR_DEGR, a.TRNEE_ID, c.TR_STA_DT, c.TOT_PAR_MKS",
        params=['%중도탈락%', today],
    )


@st.cache_data(ttl=CACHE_TTL_DEFAULT)
def get_dropout_vs_completed_status():
    """이탈자 vs 수료자 출결 상태 비율 비교용."""
    today = datetime.now().strftime('%Y-%m-%d')
    return load_data(
        "SELECT a.TRPR_DEGR, t.TRNEE_STATUS, a.ATEND_STATUS, COUNT(*) as CNT "
        "FROM TB_ATTENDANCE_LOG a "
        "JOIN TB_TRAINEE_INFO t ON a.TRNEE_ID = t.TRNEE_ID AND a.TRPR_DEGR = t.TRPR_DEGR "
        "JOIN TB_COURSE_MASTER c ON a.TRPR_ID = c.TRPR_ID AND a.TRPR_DEGR = c.TRPR_DEGR "
        "WHERE c.TR_END_DT < ? AND a.ATEND_STATUS != '중도탈락미출석' "
        "GROUP BY a.TRPR_DEGR, t.TRNEE_STATUS, a.ATEND_STATUS",
        params=[today],
    )


@st.cache_data(ttl=CACHE_TTL_DEFAULT)
def get_dropout_pre_attendance():
    """이탈자의 마지막 출석일 기준 60일 전 출결 데이터."""
    today = datetime.now().strftime('%Y-%m-%d')
    return load_data(
        "SELECT a.TRPR_DEGR, a.TRNEE_ID, a.ATEND_DT, a.ATEND_STATUS, "
        "t.TRNEE_STATUS "
        "FROM TB_ATTENDANCE_LOG a "
        "JOIN TB_TRAINEE_INFO t ON a.TRNEE_ID = t.TRNEE_ID AND a.TRPR_DEGR = t.TRPR_DEGR "
        "JOIN TB_COURSE_MASTER c ON a.TRPR_ID = c.TRPR_ID AND a.TRPR_DEGR = c.TRPR_DEGR "
        "WHERE c.TR_END_DT < ? AND t.TRNEE_STATUS LIKE ? "
        "AND a.ATEND_STATUS != '중도탈락미출석'",
        params=[today, '%중도탈락%'],
    )


course_list = get_course_list()
if course_list.empty:
    st.warning("분석할 수 있는 종료된 과정 데이터가 없습니다.")
    st.stop()

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

tab_all, tab_indiv = st.tabs(["🌐 전체 기수 비교", "📌 개별 기수 분석"])


with tab_indiv:
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
    total_rate_6 = calc_employment_rate_6(row.get('EI_EMPL_RATE_6'), row.get('HRD_EMPL_RATE_6'))
    if pd.isna(total_rate_6):
        raw_ei6 = str(row.get('EI_EMPL_RATE_6') or '').strip()
        empl_label = EMPL_CODE_MAP.get(raw_ei6, "미제공")
    else:
        empl_label = f"{total_rate_6:.1f}%"

    mc1, mc2, mc3, mc4 = st.columns(4)
    mc1.metric("수료율", f"{(fini_std / total_std * 100):.1f}%" if total_std > 0 else "0%", f"{fini_std}/{total_std}명")
    mc2.metric("총 취업률 (6개월)", empl_label, help="고용보험 + HRD 합산 | A=개설예정 B=진행중 C=미실시 D=수료자없음")
    mc3.metric("중도 탈락", f"{dropout_std}명", delta_color="inverse")
    mc4.metric("평균 결석일", f"{students_df['결석_횟수'].mean():.1f}일" if '결석_횟수' in students_df.columns else "-")
    st.divider()

    # --- 6개 탭 ---
    tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
        "👥 인구통계 분석",
        "📅 요일별 출결 패턴",
        "⏰ 시간대별 지각·조퇴",
        "🕐 체류시간 분석",
        "📉 출결/이탈 분석",
        "📋 학생별 출결 현황",
    ])

    # [Tab 1] 인구통계 분석 + 유형별 성과
    with tab1:
        left, right = st.columns(2)
        with left:
            st.markdown("##### 🎂 연령대별 인원 분포")
            if '연령대' in students_df.columns:
                age_cnt = students_df['연령대'].value_counts().reset_index()
                age_cnt.columns = ['연령대', '인원']
                st.altair_chart(
                    alt.Chart(age_cnt).mark_bar(color='#5dade2').encode(
                        x=alt.X('연령대:N', sort='-y', title='연령대', axis=alt.Axis(labelAngle=0)),
                        y=alt.Y('인원:Q', axis=alt.Axis(title=['인', '원'], titleAngle=0)),
                        tooltip=['연령대', '인원'],
                    ).properties(height=280),
                    use_container_width=True,
                )
        with right:
            st.markdown("##### 🏷️ 훈련생 유형별 분포")
            if 'TRNEE_TYPE' in students_df.columns:
                tc = students_df['TRNEE_TYPE'].value_counts().reset_index()
                tc.columns = ['유형', '인원']
                st.altair_chart(
                    alt.Chart(tc).mark_bar().encode(
                        x=alt.X('유형:N', sort='-y', axis=alt.Axis(labelAngle=0)),
                        y=alt.Y('인원:Q', axis=alt.Axis(title=['인', '원'], titleAngle=0)),
                        color=alt.value('orange'),
                        tooltip=['유형', '인원'],
                    ).properties(height=280),
                    use_container_width=True,
                )

        # 연령대별 수료율 & 결석 분석
        if '연령대' in students_df.columns and '결석_횟수' in students_df.columns:
            st.divider()
            st.markdown("##### 📊 연령대별 수료율 & 평균 결석")
            age_grp = students_df.groupby('연령대').agg(
                인원=('TRNEE_ID', 'count'),
                수료=('TRNEE_STATUS', lambda x: is_completed(x).sum()),
                평균_결석=('결석_횟수', 'mean'),
            ).reset_index()
            age_grp['수료율'] = (age_grp['수료'] / age_grp['인원'] * 100).round(1)
            age_grp['평균_결석'] = age_grp['평균_결석'].round(1)
            ag1, ag2 = st.columns(2)
            with ag1:
                if age_grp['수료율'].sum() == 0:
                    st.info("수강 상태가 아직 업데이트되지 않아 수료율을 표시할 수 없습니다.\n전체 기수 비교 탭을 참고하세요.")
                else:
                    st.altair_chart(
                        alt.Chart(age_grp).mark_bar(color='#8e44ad').encode(
                            x=alt.X('연령대:N', sort='-y', title='연령대', axis=alt.Axis(labelAngle=0)),
                            y=alt.Y('수료율:Q', axis=alt.Axis(title=['수', '료', '율'], titleAngle=0)),
                            tooltip=['연령대', '수료율', '인원'],
                        ).properties(height=250, title='연령대별 수료율'),
                        use_container_width=True,
                    )
            with ag2:
                st.altair_chart(
                    alt.Chart(age_grp).mark_bar(color='#c0392b').encode(
                        x=alt.X('연령대:N', sort='-y', title='연령대', axis=alt.Axis(labelAngle=0)),
                        y=alt.Y('평균_결석:Q', axis=alt.Axis(title=['평', '균', '결', '석', '일'], titleAngle=0)),
                        tooltip=['연령대', '평균_결석', '인원'],
                    ).properties(height=250, title='연령대별 평균 결석'),
                    use_container_width=True,
                )

        # 유형별 성과 분석 (신규)
        if 'TRNEE_TYPE' in students_df.columns and '결석_횟수' in students_df.columns:
            st.divider()
            st.markdown("##### 📊 훈련생 유형별 성과 비교")
            type_stats = students_df.groupby('TRNEE_TYPE').agg(
                인원=('TRNEE_ID', 'count'),
                수료_인원=('TRNEE_STATUS', lambda x: is_completed(x).sum()),
                평균_결석=('결석_횟수', 'mean'),
                평균_지각=('지각_횟수', 'mean'),
            ).reset_index()
            type_stats['수료율'] = (type_stats['수료_인원'] / type_stats['인원'] * 100).round(1)
            type_stats['평균_결석'] = type_stats['평균_결석'].round(1)
            type_stats['평균_지각'] = type_stats['평균_지각'].round(1)

            tc1, tc2 = st.columns(2)
            with tc1:
                if type_stats['수료율'].sum() == 0:
                    st.info("수강 상태가 아직 업데이트되지 않아 수료율을 표시할 수 없습니다.\n전체 기수 비교 탭을 참고하세요.")
                else:
                    st.altair_chart(
                        alt.Chart(type_stats).mark_bar().encode(
                            x=alt.X('TRNEE_TYPE:N', title='유형', axis=alt.Axis(labelAngle=0)),
                            y=alt.Y('수료율:Q', axis=alt.Axis(title=['수', '료', '율'], titleAngle=0)),
                            color=alt.value('#3498db'),
                            tooltip=['TRNEE_TYPE', '수료율', '인원'],
                        ).properties(height=250, title='유형별 수료율'),
                        use_container_width=True,
                    )
            with tc2:
                st.altair_chart(
                    alt.Chart(type_stats).mark_bar().encode(
                        x=alt.X('TRNEE_TYPE:N', title='유형', axis=alt.Axis(labelAngle=0)),
                        y=alt.Y('평균_결석:Q', axis=alt.Axis(title=['평', '균', '결', '석', '일'], titleAngle=0)),
                        color=alt.value('#e74c3c'),
                        tooltip=['TRNEE_TYPE', '평균_결석', '평균_지각'],
                    ).properties(height=250, title='유형별 평균 결석일'),
                    use_container_width=True,
                )

    # [Tab 2] 요일별 출결 패턴
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
                x=alt.X('요일:N', sort=day_order, title='요일', axis=alt.Axis(labelAngle=0)),
                y=alt.Y('ATEND_STATUS:N', axis=alt.Axis(title=['출', '결', '상', '태'], titleAngle=0)),
            )
            heatmap = base.mark_rect().encode(
                color=alt.Color('CNT:Q', scale=alt.Scale(scheme='orangered'), title='건수'),
                tooltip=['요일', 'ATEND_STATUS', 'CNT'],
            )
            median_val = float(daily_agg['CNT'].median())
            text = base.mark_text(baseline='middle').encode(
                text='CNT:Q',
                color=alt.condition(alt.datum.CNT > median_val, alt.value('white'), alt.value('black')),
            )
            st.altair_chart((heatmap + text).properties(height=300), use_container_width=True)

            st.markdown("##### 요일별 결석 건수")
            absent_df = daily_agg[daily_agg['ATEND_STATUS'] == '결석']
            if not absent_df.empty:
                st.altair_chart(
                    alt.Chart(absent_df).mark_bar(color='#e74c3c').encode(
                        x=alt.X('요일:N', sort=day_order, title='요일', axis=alt.Axis(labelAngle=0)),
                        y=alt.Y('CNT:Q', axis=alt.Axis(title=['결', '석', '건', '수'], titleAngle=0)),
                        tooltip=['요일', 'CNT'],
                    ).properties(height=250),
                    use_container_width=True,
                )
            else:
                st.success("결석 기록이 없습니다.")

    # [Tab 3] 시간대별 지각 분포
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
                late_df['입실시각_분'] = late_df['IN_TIME'].apply(parse_time_to_minutes)
                late_df = late_df[late_df['입실시각_분'].notna()].copy()
                if late_df.empty:
                    st.info("유효한 입실 시간 데이터가 없습니다.")
                else:
                    ca, cb = st.columns(2)
                    with ca:
                        st.altair_chart(
                            alt.Chart(late_df).mark_bar(color='#e67e22').encode(
                                x=alt.X('IN_TIME:N', sort='ascending', title='입실 시간', axis=alt.Axis(labelAngle=0)),
                                y=alt.Y('count()', axis=alt.Axis(title=['건', '수'], titleAngle=0)),
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

    # [Tab 3 하단] 조퇴 시간대 분포
    with tab3:
        st.divider()
        st.markdown("##### 🏃 조퇴 퇴실 시간 분포")
        st.caption("조퇴로 기록된 훈련생의 퇴실 시간 분포입니다.")
        leave_df = get_early_leave_times(selected_degr)
        if leave_df.empty:
            st.info("조퇴 기록이 없습니다.")
        else:
            leave_df = leave_df[leave_df['OUT_TIME'].notna() & (leave_df['OUT_TIME'] != '')]
            leave_df['퇴실시각_분'] = leave_df['OUT_TIME'].apply(parse_time_to_minutes)
            leave_df = leave_df[leave_df['퇴실시각_분'].notna()].copy()
            if leave_df.empty:
                st.info("조퇴 기록에 퇴실 시간이 기록되지 않았습니다.")
            else:
                lc1, lc2 = st.columns(2)
                with lc1:
                    st.altair_chart(
                        alt.Chart(leave_df).mark_bar(color='#16a085').encode(
                            x=alt.X('OUT_TIME:N', sort='ascending', title='퇴실 시간', axis=alt.Axis(labelAngle=0)),
                            y=alt.Y('count()', axis=alt.Axis(title=['건', '수'], titleAngle=0)),
                            tooltip=['OUT_TIME', 'count()'],
                        ).properties(height=300, title='조퇴 퇴실 시간별 건수'),
                        use_container_width=True,
                    )
                with lc2:
                    st.markdown("##### 조퇴 통계")
                    avg_min = leave_df['퇴실시각_분'].mean()
                    avg_h, avg_m = divmod(int(avg_min), 60)
                    st.metric("평균 퇴실 시간", f"{avg_h:02d}:{avg_m:02d}")
                    st.metric("총 조퇴 건수", f"{len(leave_df)}건")
                    earliest = leave_df.loc[leave_df['퇴실시각_분'].idxmin()]
                    st.metric("가장 이른 조퇴", earliest['OUT_TIME'])

    # [Tab 4] 체류시간 분석 (신규)
    with tab4:
        st.markdown("##### 🕐 일일 체류시간 분석")
        st.caption("입실~퇴실 시간으로 계산한 실제 체류시간입니다.")
        stay_df = get_stay_duration(selected_degr)
        if stay_df.empty:
            st.info("입퇴실 시간 데이터가 없습니다.")
        else:
            stay_df['입실_분'] = stay_df['IN_TIME'].apply(parse_time_to_minutes)
            stay_df['퇴실_분'] = stay_df['OUT_TIME'].apply(parse_time_to_minutes)
            stay_df = stay_df[stay_df['입실_분'].notna() & stay_df['퇴실_분'].notna()].copy()
            stay_df['체류시간_분'] = stay_df['퇴실_분'] - stay_df['입실_분']
            stay_df = stay_df[stay_df['체류시간_분'] > 0].copy()

            if stay_df.empty:
                st.info("유효한 체류시간 데이터가 없습니다.")
            else:
                stay_df['체류시간'] = stay_df['체류시간_분'] / 60
                avg_stay = stay_df['체류시간'].mean()
                avg_h_stay, avg_m_stay = divmod(int(avg_stay * 60), 60)

                sc1, sc2, sc3 = st.columns(3)
                sc1.metric("평균 체류시간", f"{avg_h_stay}시간 {avg_m_stay}분")
                sc2.metric("분석 대상 로그", f"{len(stay_df):,}건")
                max_stay = stay_df['체류시간'].max()
                mh, mm = divmod(int(max_stay * 60), 60)
                sc3.metric("최장 체류", f"{mh}시간 {mm}분")

                # 체류시간 분포 히스토그램
                st.altair_chart(
                    alt.Chart(stay_df).mark_bar(color='#9b59b6').encode(
                        alt.X('체류시간:Q', bin=alt.Bin(step=0.5), title='체류시간 (시간)'),
                        alt.Y('count()', axis=alt.Axis(title=['건', '수'], titleAngle=0)),
                        tooltip=['count()'],
                    ).properties(height=300, title='체류시간 분포'),
                    use_container_width=True,
                )

                # 학생별 평균 체류시간
                st.markdown("##### 학생별 평균 체류시간")
                student_stay = stay_df.groupby('TRNEE_ID').agg(
                    평균_체류시간=('체류시간', 'mean'),
                    기록_일수=('ATEND_DT', 'count'),
                ).reset_index()
                student_stay['평균_체류시간'] = student_stay['평균_체류시간'].round(2)
                # 이름 매핑
                if not students_df.empty:
                    name_map = students_df[['TRNEE_ID', 'TRNEE_NM']].drop_duplicates()
                    student_stay = student_stay.merge(name_map, on='TRNEE_ID', how='left')
                    cols = ['TRNEE_NM', '평균_체류시간', '기록_일수']
                else:
                    cols = ['TRNEE_ID', '평균_체류시간', '기록_일수']

                student_stay = student_stay.sort_values('평균_체류시간', ascending=False)
                st.dataframe(
                    student_stay[cols],
                    column_config={
                        '평균_체류시간': st.column_config.NumberColumn('평균 체류(시간)', format="%.1f"),
                        '기록_일수': st.column_config.NumberColumn('기록 일수', format="%d"),
                    },
                    use_container_width=True,
                    hide_index=True,
                )

    # [Tab 5] 출결 및 이탈 분석
    with tab5:
        st.markdown("##### 📉 이탈 분석")

        full_df = get_full_attendance_with_status(selected_degr)
        if full_df.empty:
            st.info("출결 데이터가 없습니다.")
        else:
            full_df["ATEND_DT"] = pd.to_datetime(full_df["ATEND_DT"])
            dropout_ids = full_df[full_df["TRNEE_STATUS"].str.contains("중도탈락", na=False)]["TRNEE_ID"].unique()
            grad_ids = full_df[is_completed(full_df["TRNEE_STATUS"])]["TRNEE_ID"].unique()

            total_cnt = len(dropout_ids) + len(grad_ids)
            dropout_rate = len(dropout_ids) / total_cnt * 100 if total_cnt > 0 else 0

            if len(dropout_ids) == 0:
                st.metric("중도탈락률", "0%", f"0/{total_cnt}명")
                st.info("이 기수의 중도탈락자가 없습니다.")
            else:
                # (1) 중도탈락률 metric
                kc1, kc2 = st.columns(2)
                kc1.metric("중도탈락률", f"{dropout_rate:.1f}%", f"{len(dropout_ids)}/{total_cnt}명")
                kc2.metric("중도탈락 인원", f"{len(dropout_ids)}명")
                st.divider()

                # (2) 이탈 시점 strip chart
                dropout_data = full_df[full_df["TRNEE_ID"].isin(dropout_ids)].copy()
                dropout_data = dropout_data[dropout_data["ATEND_STATUS"] != "중도탈락미출석"]
                last_dates = dropout_data.groupby("TRNEE_ID")["ATEND_DT"].max().reset_index()
                last_dates.rename(columns={"ATEND_DT": "LAST_DT"}, inplace=True)
                first_date = full_df["ATEND_DT"].min()
                last_dates["이탈경과일"] = (last_dates["LAST_DT"] - first_date).dt.days
                last_dates = last_dates[last_dates["이탈경과일"] >= 0]

                if not last_dates.empty:
                    st.markdown("##### 이탈 시점 분포")
                    st.caption("개강일 기준 경과일 구간별 이탈자 수")
                    bins = [0, 30, 60, 90, 120, 150]
                    labels_bin = ["0~30일", "30~60일", "60~90일", "90~120일", "120~150일"]
                    last_dates["구간"] = pd.cut(last_dates["이탈경과일"], bins=bins, labels=labels_bin, right=False)
                    bin_counts = last_dates["구간"].value_counts().reindex(labels_bin, fill_value=0)

                    # 구간별 요약 metric
                    mcols = st.columns(len(labels_bin))
                    for col, label in zip(mcols, labels_bin):
                        col.metric(label, f"{bin_counts[label]}명")

                    bin_df = pd.DataFrame({"구간": bin_counts.index, "이탈자 수": bin_counts.values})
                    st.dataframe(bin_df, hide_index=True, use_container_width=True)
                st.divider()

                # (3) 이탈자 vs 수료자 출결 상태 비교
                st.markdown("##### 이탈자 vs 수료자 출결 상태 비교")
                target_statuses = ["결석", "지각", "조퇴", "외출"]
                all_active = full_df[full_df["ATEND_STATUS"] != "중도탈락미출석"].copy()
                rows_bar = []
                for label, ids in [("중도탈락", dropout_ids), ("수료", grad_ids)]:
                    grp = all_active[all_active["TRNEE_ID"].isin(ids)]
                    total = len(grp)
                    if total == 0:
                        continue
                    for status in target_statuses:
                        cnt = (grp["ATEND_STATUS"] == status).sum()
                        rows_bar.append({"그룹": label, "출결 상태": status, "비율": round(cnt / total * 100, 1)})
                bar_df = pd.DataFrame(rows_bar)
                if not bar_df.empty:
                    bars = alt.Chart(bar_df).mark_bar().encode(
                        x=alt.X("출결 상태:N", sort=target_statuses, title="출결 상태",
                                axis=alt.Axis(labelAngle=0)),
                        y=alt.Y("비율:Q", axis=alt.Axis(title=["비", "율", "(%)"], titleAngle=0)),
                        color=alt.Color("그룹:N", scale=alt.Scale(
                            domain=["중도탈락", "수료"], range=["#e74c3c", "#3498db"])),
                        xOffset="그룹:N",
                        tooltip=["그룹", "출결 상태", "비율"],
                    )
                    text_layer = bars.mark_text(dy=-8, fontSize=11).encode(
                        text=alt.Text("비율:Q", format=".1f"),
                    )
                    st.altair_chart((bars + text_layer).properties(height=350),
                                   use_container_width=True)
                st.divider()

                # (4) 이탈 전 2달 평균 출결 지표
                st.markdown("##### 이탈 전 2개월 출결 요약")
                st.caption("이탈자의 마지막 출석일 기준 60일 전 출결 데이터를 집계합니다.")
                pre_rows = []
                for tid in dropout_ids:
                    td = dropout_data[dropout_data["TRNEE_ID"] == tid]
                    if td.empty:
                        continue
                    last_dt = td["ATEND_DT"].max()
                    window = td[td["ATEND_DT"] >= last_dt - pd.Timedelta(days=60)]
                    if window.empty:
                        continue
                    pre_rows.append({
                        "총일수": len(window),
                        "결석일": (window["ATEND_STATUS"] == "결석").sum(),
                        "지각일": (window["ATEND_STATUS"] == "지각").sum(),
                        "출석일": (~window["ATEND_STATUS"].isin(NOT_ATTEND_STATUSES)).sum(),
                    })
                if pre_rows:
                    pre_df = pd.DataFrame(pre_rows)
                    pre_df["출석률"] = (pre_df["출석일"] / pre_df["총일수"] * 100).round(1)
                    pc1, pc2, pc3, pc4 = st.columns(4)
                    pc1.metric("평균 출석률", f"{pre_df['출석률'].mean():.1f}%")
                    pc2.metric("평균 결석일", f"{pre_df['결석일'].mean():.1f}일")
                    pc3.metric("평균 지각일", f"{pre_df['지각일'].mean():.1f}일")
                    pc4.metric("분석 대상", f"{len(pre_df)}명")
                else:
                    st.info("이탈 전 출결 데이터가 없습니다.")

        if "결석_횟수" in students_df.columns:
            risk = students_df[
                (students_df["결석_횟수"] >= RISK_ABSENT) &
                ~is_completed(students_df["TRNEE_STATUS"])
            ]
            if not risk.empty:
                st.divider()
                st.warning(f"⚠️ **출석 불량 위험군 ({len(risk)}명):** 결석이 {RISK_ABSENT}일 이상인 미수료 학생들입니다.")
                st.dataframe(
                    risk[["TRNEE_NM", "나이", "결석_횟수", "지각_조퇴_횟수", "TRNEE_STATUS"]],
                    hide_index=True,
                )

    # [Tab 6] 학생별 출결 현황
    with tab6:
        st.markdown("##### 📋 학생별 상세 출결 현황")
        st.caption("각 열 헤더를 클릭하여 정렬할 수 있습니다.")
        display_cols = ['TRNEE_NM', '나이', '연령대', 'TRNEE_STATUS']
        stat_cols = ['출석_횟수', '결석_횟수', '지각_횟수', '조퇴_횟수', '외출_횟수', '총_로그_수', 'OFLHD_CNT', 'VCATN_CNT']
        avail = display_cols + [c for c in stat_cols if c in students_df.columns]
        if 'TRNEE_TYPE' in students_df.columns:
            avail.insert(4, 'TRNEE_TYPE')
        if '출석_횟수' in students_df.columns and '총_로그_수' in students_df.columns:
            students_df['출석률(%)'] = students_df.apply(
                lambda r: calc_attendance_rate_from_counts(
                    total=int(r.get('총_로그_수', 0)),
                    absent=int(r.get('결석_횟수', 0)),
                    dropout_absent=int(r.get('중도탈락미출석_횟수', 0)),
                    lt50=int(r.get('lt50_횟수', 0)),
                    late=int(r.get('지각_횟수', 0)),
                    early_leave=int(r.get('조퇴_횟수', 0)),
                    out=int(r.get('외출_횟수', 0)),
                ), axis=1
            )
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
                "OFLHD_CNT": st.column_config.NumberColumn("공가(일)", format="%d"),
                "VCATN_CNT": st.column_config.NumberColumn("휴가(일)", format="%d"),
                "출석률(%)": st.column_config.ProgressColumn(
                    "출석률", min_value=0, max_value=100, format="%.1f%%"
                ),
            },
            use_container_width=True,
            hide_index=True,
        )


with tab_all:
    st.subheader("📊 전체 기수 비교 분석")
    st.caption("종료된 전체 기수의 성과를 한눈에 비교합니다.")
    all_master = get_all_course_master()
    if all_master.empty:
        st.warning("종료된 과정 데이터가 없습니다.")
        st.stop()

    # --- 기본 지표 계산 ---
    all_master['기수'] = all_master['TRPR_DEGR'].astype(str) + '회차'
    all_master['수료율'] = (all_master['FINI_CNT'] / all_master['TOT_PAR_MKS'] * 100).round(1)
    all_master['EI_취업률_6'] = all_master['EI_EMPL_RATE_6'].apply(safe_float)
    all_master['HRD_취업률_6'] = all_master['HRD_EMPL_RATE_6'].apply(safe_float)
    all_master['총_취업률'] = all_master.apply(
        lambda r: calc_employment_rate_6(r['EI_EMPL_RATE_6'], r['HRD_EMPL_RATE_6']), axis=1
    )
    all_master['취업률_3'] = all_master['EI_EMPL_RATE_3'].apply(lambda x: parse_empl_rate(x)[0])
    degr_order = all_master.sort_values('TRPR_DEGR')['기수'].tolist()

    trainee_stats = load_data("""
        SELECT TRPR_ID, TRPR_DEGR,
               SUM(CASE WHEN TRNEE_STATUS = '제적' THEN 1 ELSE 0 END) AS EXPEL_CNT,
               SUM(CASE WHEN TRNEE_STATUS = '중도탈락' THEN 1 ELSE 0 END) AS DROP_CNT
        FROM TB_TRAINEE_INFO
        GROUP BY TRPR_ID, TRPR_DEGR
    """)
    all_master = all_master.merge(trainee_stats, on=['TRPR_ID', 'TRPR_DEGR'], how='left')
    for c in ['EXPEL_CNT', 'DROP_CNT', 'TOT_TRP_CNT']:
        all_master[c] = pd.to_numeric(all_master[c], errors='coerce').fillna(0).astype(int)
    all_master['잔여율'] = (
        (all_master['TOT_PAR_MKS'] - all_master['EXPEL_CNT'] - all_master['DROP_CNT']) /
        all_master['TOT_PAR_MKS'].replace(0, pd.NA) * 100
    ).round(1).fillna(0)
    all_master['중도탈락률'] = (
        all_master['DROP_CNT'] / all_master['TOT_PAR_MKS'].replace(0, pd.NA) * 100
    ).round(1).fillna(0)

    # 출결 데이터 사전 계산
    all_attend = get_all_degr_attendance()
    comp = pd.DataFrame()
    absent_total_df = pd.DataFrame()
    if not all_attend.empty:
        all_attend['기수'] = all_attend['TRPR_DEGR'].astype(str) + '회차'
        absent_total_df = all_attend[all_attend['ATEND_STATUS'] == '결석'][['기수', 'CNT']].copy()
        absent_total_df.columns = ['기수', '결석건수']
        # 기수별 출석률: 상태별 CNT 피벗 후 표준 공식 적용 (매출_분석.py 기준)
        _piv = all_attend.pivot_table(
            index='기수', columns='ATEND_STATUS', values='CNT', aggfunc='sum', fill_value=0
        )
        def _comp_rate(row):
            total = int(row.sum())
            return calc_attendance_rate_from_counts(
                total=total,
                absent=int(row.get('결석', 0)),
                dropout_absent=int(row.get('중도탈락미출석', 0)),
                lt50=int(row.get('100분의50미만출석', 0)),
                late=int(row.get('지각', 0)),
                early_leave=int(row.get('조퇴', 0)),
                out=int(row.get('외출', 0)),
            )
        comp = _piv.apply(_comp_rate, axis=1).reset_index()
        comp.columns = ['기수', '출석률']

    # === 서브탭 구성 ===
    sub_perf, sub_drop, sub_data = st.tabs(["성과 지표", "이탈 분석", "상세 데이터"])

    # 공통 데이터 사전 계산
    absent_avg_df = get_all_degr_absent_avg()
    if not absent_avg_df.empty:
        absent_avg_df['기수'] = absent_avg_df['TRPR_DEGR'].astype(str) + '회차'
        absent_avg_df['평균_결석일'] = absent_avg_df['평균_결석일'].round(1)

    # ──────────────────────────────────────────────
    # 서브탭 1: 성과 지표
    # ──────────────────────────────────────────────
    with sub_perf:
        # (A) KPI 요약 메트릭
        avg_comp_rate = all_master['수료율'].mean()
        avg_attend = comp['출석률'].mean() if not comp.empty else None
        avg_empl = all_master['총_취업률'].mean()
        avg_dropout = all_master['중도탈락률'].mean()

        kpi1, kpi2, kpi3, kpi4 = st.columns(4)
        kpi1.metric("평균 수료율", f"{avg_comp_rate:.1f}%")
        kpi2.metric("평균 출석률", f"{avg_attend:.1f}%" if avg_attend is not None else "–")
        kpi3.metric("평균 취업률", f"{avg_empl:.1f}%" if pd.notna(avg_empl) else "–")
        kpi4.metric("평균 중도탈락률", f"{avg_dropout:.1f}%")
        st.divider()

        # (B) 출석률 & 취업률 추이 (dual-line)
        if not comp.empty:
            _corr_base = comp[['기수', '출석률']].merge(
                all_master[['기수', '총_취업률']].dropna(subset=['총_취업률']),
                on='기수', how='inner'
            )
            st.markdown("##### 기수별 출석률 & 취업률 추이")
            st.caption("두 지표 모두 % 기준 — 기수별 흐름 비교")
            if not _corr_base.empty:
                _long = pd.melt(_corr_base, id_vars='기수',
                                value_vars=['출석률', '총_취업률'],
                                var_name='지표', value_name='값')
                _long['지표'] = _long['지표'].map({'출석률': '출석률', '총_취업률': '6개월 취업률'})
                st.altair_chart(
                    alt.Chart(_long).mark_line(point=True, strokeWidth=2).encode(
                        x=alt.X('기수:N', sort=degr_order, title='기수', axis=alt.Axis(labelAngle=0)),
                        y=alt.Y('값:Q', scale=alt.Scale(domain=[0, 100]),
                                axis=alt.Axis(title=['%'], titleAngle=0)),
                        color=alt.Color('지표:N', scale=alt.Scale(
                            domain=['출석률', '6개월 취업률'],
                            range=['#3498db', '#2ecc71']
                        )),
                        tooltip=['기수', '지표', '값'],
                    ).properties(height=300),
                    use_container_width=True,
                )
            st.divider()

        # (C) 수료율 bar | 평균 결석일 bar
        c_left, c_right = st.columns(2)
        with c_left:
            st.markdown("##### 기수별 수료율")
            st.altair_chart(
                alt.Chart(all_master).mark_bar(color='#3498db').encode(
                    x=alt.X('기수:N', sort=degr_order, title='기수', axis=alt.Axis(labelAngle=0)),
                    y=alt.Y('수료율:Q', axis=alt.Axis(title=['수', '료', '율', '(%)'], titleAngle=0)),
                    tooltip=['기수', '수료율', 'FINI_CNT', 'TOT_PAR_MKS'],
                ).properties(height=300),
                use_container_width=True,
            )
        with c_right:
            st.markdown("##### 기수별 1인당 평균 결석일")
            if not absent_avg_df.empty:
                st.altair_chart(
                    alt.Chart(absent_avg_df).mark_bar(color='#e74c3c').encode(
                        x=alt.X('기수:N', sort=degr_order, title='기수', axis=alt.Axis(labelAngle=0)),
                        y=alt.Y('평균_결석일:Q', axis=alt.Axis(title=['평', '균', '결', '석', '일'], titleAngle=0)),
                        tooltip=['기수', '평균_결석일'],
                    ).properties(height=300),
                    use_container_width=True,
                )
        st.divider()

        # (D) 수강생 구성 변화 (연령대 | 유형)
        demo_df = get_all_trainee_demographics()
        if not demo_df.empty:
            demo_df['나이'] = demo_df.apply(
                lambda r: calculate_age_at_training(r['BIRTH_DATE'], r['TR_STA_DT']), axis=1
            )
            demo_df['연령대'] = demo_df['나이'].apply(
                lambda x: f"{int(x // 10 * 10)}대" if pd.notnull(x) else "미상"
            )
            demo_df['TRNEE_TYPE'] = demo_df['TRNEE_TYPE'].map(TRNEE_TYPE_MAP).fillna(demo_df['TRNEE_TYPE'])
            demo_df['기수'] = demo_df['TRPR_DEGR'].astype(str) + '회차'

            st.markdown("##### 수강생 구성 변화")
            dc1, dc2 = st.columns(2)
            with dc1:
                st.markdown("###### 기수별 연령대 구성")
                age_stack = demo_df.groupby(['기수', '연령대']).size().reset_index(name='인원')
                st.altair_chart(
                    alt.Chart(age_stack).mark_bar().encode(
                        x=alt.X('기수:N', sort=degr_order, title='기수', axis=alt.Axis(labelAngle=0)),
                        y=alt.Y('인원:Q', stack='normalize',
                                axis=alt.Axis(format='%', title=['비', '율'], titleAngle=0)),
                        color=alt.Color('연령대:N', title='연령대'),
                        tooltip=['기수', '연령대', '인원'],
                    ).properties(height=300),
                    use_container_width=True,
                )
            with dc2:
                st.markdown("###### 기수별 훈련생 유형 구성")
                type_stack = demo_df.groupby(['기수', 'TRNEE_TYPE']).size().reset_index(name='인원')
                type_stack = type_stack.rename(columns={'TRNEE_TYPE': '유형'})
                st.altair_chart(
                    alt.Chart(type_stack).mark_bar().encode(
                        x=alt.X('기수:N', sort=degr_order, title='기수', axis=alt.Axis(labelAngle=0)),
                        y=alt.Y('인원:Q', stack='normalize',
                                axis=alt.Axis(format='%', title=['비', '율'], titleAngle=0)),
                        color=alt.Color('유형:N', title='유형'),
                        tooltip=['기수', '유형', '인원'],
                    ).properties(height=300),
                    use_container_width=True,
                )

    # ──────────────────────────────────────────────
    # 서브탭 2: 이탈 분석
    # ──────────────────────────────────────────────
    with sub_drop:
        # (E) 중도탈락률 line
        st.markdown("##### 기수별 중도탈락률")
        st.altair_chart(
            alt.Chart(all_master).mark_line(
                point=alt.OverlayMarkDef(size=80), color='#e67e22'
            ).encode(
                x=alt.X('기수:N', sort=degr_order, title='기수', axis=alt.Axis(labelAngle=0)),
                y=alt.Y('중도탈락률:Q', axis=alt.Axis(title=['중', '도', '탈', '락', '률', '(%)'], titleAngle=0)),
                tooltip=['기수', '중도탈락률', 'DROP_CNT'],
            ).properties(height=300),
            use_container_width=True,
        )
        st.divider()

        # (F) 이탈 시점 strip chart
        dropout_timing = get_dropout_timing()
        if not dropout_timing.empty:
            dropout_timing['LAST_ATTEND_DT'] = pd.to_datetime(dropout_timing['LAST_ATTEND_DT'])
            dropout_timing['TR_STA_DT'] = pd.to_datetime(dropout_timing['TR_STA_DT'])
            dropout_timing['이탈경과일'] = (
                dropout_timing['LAST_ATTEND_DT'] - dropout_timing['TR_STA_DT']
            ).dt.days
            dropout_timing['기수'] = dropout_timing['TRPR_DEGR'].astype(str) + '회차'
            dropout_timing = dropout_timing[dropout_timing['이탈경과일'] >= 0]

            if not dropout_timing.empty:
                st.markdown("##### 이탈 시점 분포")
                st.caption("개강일 기준 경과일 구간별 이탈자 수")
                bins = [0, 30, 60, 90, 120, 150]
                labels_bin = ["0~30일", "30~60일", "60~90일", "90~120일", "120~150일"]
                dropout_timing["구간"] = pd.cut(dropout_timing["이탈경과일"], bins=bins, labels=labels_bin, right=False)
                pivot = dropout_timing.groupby(["기수", "구간"], observed=False).size().unstack(fill_value=0)
                pivot = pivot.reindex(columns=labels_bin, fill_value=0)
                pivot = pivot.loc[sorted(pivot.index, key=lambda x: int(x.replace("회차", "")))]
                pivot.loc["합계"] = pivot.sum()
                st.dataframe(pivot, use_container_width=True)
        else:
            st.info("이탈 타이밍 데이터가 없습니다.")
        st.divider()

        # (G) 이탈자 출결 비율 vs 수료생 비교
        st.markdown("##### 이탈자 vs 수료자 출결 상태 비교")
        st.caption("중도탈락미출석 제외, 전체 기수 합산 기준")
        status_raw = get_dropout_vs_completed_status()
        if not status_raw.empty:
            status_raw['그룹'] = status_raw['TRNEE_STATUS'].apply(
                lambda x: '중도탈락' if '중도탈락' in str(x) else ('수료' if is_completed(pd.Series([x])).iloc[0] else None)
            )
            status_raw = status_raw[status_raw['그룹'].notna()].copy()
            target_statuses = ['결석', '지각', '조퇴', '외출']
            status_raw = status_raw[status_raw['ATEND_STATUS'].isin(target_statuses + ['출석'])].copy()
            grp_total = status_raw.groupby('그룹')['CNT'].sum().reset_index().rename(columns={'CNT': 'TOTAL'})
            status_agg = status_raw.groupby(['그룹', 'ATEND_STATUS'])['CNT'].sum().reset_index()
            status_agg = status_agg.merge(grp_total, on='그룹')
            status_agg['비율'] = (status_agg['CNT'] / status_agg['TOTAL'] * 100).round(1)
            status_agg = status_agg[status_agg['ATEND_STATUS'].isin(target_statuses)]
            if not status_agg.empty:
                status_agg.rename(columns={'ATEND_STATUS': '출결 상태'}, inplace=True)
                bars = alt.Chart(status_agg).mark_bar().encode(
                    x=alt.X('출결 상태:N', sort=target_statuses, title='출결 상태',
                            axis=alt.Axis(labelAngle=0)),
                    y=alt.Y('비율:Q', axis=alt.Axis(title=['비', '율', '(%)'], titleAngle=0)),
                    color=alt.Color('그룹:N', scale=alt.Scale(
                        domain=['중도탈락', '수료'], range=['#e74c3c', '#3498db'])),
                    xOffset='그룹:N',
                    tooltip=['그룹', '출결 상태', '비율'],
                )
                text_layer = bars.mark_text(dy=-8, fontSize=11).encode(
                    text=alt.Text('비율:Q', format='.1f'),
                )
                st.altair_chart((bars + text_layer).properties(height=350),
                               use_container_width=True)
            else:
                st.info("비교할 출결 데이터가 없습니다.")
        else:
            st.info("출결 상태 데이터가 없습니다.")
        st.divider()

        # (H) 이탈 전 2달 평균 출결 지표
        st.markdown("##### 이탈 전 2개월 출결 요약")
        st.caption("이탈자의 마지막 출석일 기준 60일 전 출결 데이터를 집계합니다.")
        pre_att = get_dropout_pre_attendance()
        if not pre_att.empty:
            pre_att['ATEND_DT'] = pd.to_datetime(pre_att['ATEND_DT'])
            last_dates = pre_att.groupby(['TRPR_DEGR', 'TRNEE_ID'])['ATEND_DT'].max().reset_index()
            last_dates.rename(columns={'ATEND_DT': 'LAST_DT'}, inplace=True)
            pre_att = pre_att.merge(last_dates, on=['TRPR_DEGR', 'TRNEE_ID'])
            pre_att = pre_att[pre_att['ATEND_DT'] >= pre_att['LAST_DT'] - pd.Timedelta(days=60)]
            if not pre_att.empty:
                per_person = pre_att.groupby(['TRPR_DEGR', 'TRNEE_ID']).apply(
                    lambda g: pd.Series({
                        '총일수': len(g),
                        '결석일': (g['ATEND_STATUS'] == '결석').sum(),
                        '지각일': (g['ATEND_STATUS'] == '지각').sum(),
                        '출석일': (~g['ATEND_STATUS'].isin(NOT_ATTEND_STATUSES)).sum(),
                    }),
                    include_groups=False,
                ).reset_index()
                per_person['출석률'] = (per_person['출석일'] / per_person['총일수'] * 100).round(1)

                mc1, mc2, mc3, mc4 = st.columns(4)
                mc1.metric("평균 출석률", f"{per_person['출석률'].mean():.1f}%")
                mc2.metric("평균 결석일", f"{per_person['결석일'].mean():.1f}일")
                mc3.metric("평균 지각일", f"{per_person['지각일'].mean():.1f}일")
                mc4.metric("분석 대상", f"{len(per_person)}명")
            else:
                st.info("이탈 전 60일 이내 출결 데이터가 없습니다.")
        else:
            st.info("이탈자 출결 데이터가 없습니다.")

    # ──────────────────────────────────────────────
    # 서브탭 3: 상세 데이터
    # ──────────────────────────────────────────────
    with sub_data:
        # (I) 종합 비교 테이블 (+출석률)
        st.markdown("##### 종합 비교 테이블")
        _tbl = all_master.copy()
        if not comp.empty:
            _tbl = _tbl.merge(comp[['기수', '출석률']], on='기수', how='left')
        else:
            _tbl['출석률'] = pd.NA
        summary_cols = ['기수', 'TRPR_NM', 'TR_STA_DT', 'TR_END_DT', 'TOT_TRP_CNT', 'TOT_PAR_MKS', 'EXPEL_CNT', 'DROP_CNT', '잔여율', 'FINI_CNT', '수료율', '출석률', '취업률_3', '총_취업률']
        st.dataframe(
            _tbl[summary_cols].rename(columns={
                'TRPR_NM': '과정명', 'TR_STA_DT': '시작일', 'TR_END_DT': '종료일',
                'TOT_TRP_CNT': '수강신청', 'TOT_PAR_MKS': '수강인원',
                'EXPEL_CNT': '제적', 'DROP_CNT': '중도탈락',
                '잔여율': '잔여율(%)', 'FINI_CNT': '수료인원',
                '수료율': '수료율(%)', '출석률': '출석률(%)',
                '취업률_3': '3개월 취업률(%)', '총_취업률': '6개월 취업률(%)',
            }),
            column_config={
                "수강신청": st.column_config.NumberColumn(format="%d명"),
                "수강인원": st.column_config.NumberColumn(format="%d명"),
                "제적": st.column_config.NumberColumn(format="%d명"),
                "중도탈락": st.column_config.NumberColumn(format="%d명"),
                "잔여율(%)": st.column_config.NumberColumn(format="%.1f%%"),
                "수료인원": st.column_config.NumberColumn(format="%d명"),
                "수료율(%)": st.column_config.NumberColumn(format="%.1f%%"),
                "출석률(%)": st.column_config.NumberColumn(format="%.1f%%"),
                "3개월 취업률(%)": st.column_config.NumberColumn(format="%.1f%%"),
                "6개월 취업률(%)": st.column_config.NumberColumn(format="%.1f%%"),
            },
            use_container_width=True,
            hide_index=True,
        )
        st.caption("💡 특정 기수를 심층 분석하려면 사이드바에서 회차를 선택 후 '개별 기수 분석' 탭을 확인하세요.")
