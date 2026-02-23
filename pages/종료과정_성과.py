import streamlit as st
import pandas as pd
import altair as alt
from datetime import datetime
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils import load_data, calculate_age_at_training, safe_float, check_password
from config import CACHE_TTL_DEFAULT, EMPL_CODE_MAP, RISK_ABSENT

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


def parse_time_to_minutes(t):
    """HH:MM 형태의 시간을 분으로 변환"""
    try:
        parts = str(t).split(':')
        if len(parts) == 2 and parts[0].isdigit() and parts[1].isdigit():
            return int(parts[0]) * 60 + int(parts[1])
    except Exception:
        pass
    return None


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
    _EMPL_STATUS = EMPL_CODE_MAP
    raw_ei6 = str(row.get('EI_EMPL_RATE_6') or '').strip()
    raw_hrd6 = str(row.get('HRD_EMPL_RATE_6') or '').strip()
    if raw_ei6 in _EMPL_STATUS:
        empl_label = _EMPL_STATUS[raw_ei6]
    else:
        total_empl_rate = safe_float(raw_ei6) + safe_float(raw_hrd6)
        empl_label = f"{total_empl_rate:.1f}%"

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
                        y=alt.Y('인원:Q', axis=alt.Axis(title='인\n원', titleAngle=0)),
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
                        x='인원:Q',
                        y=alt.Y('유형:N', sort='-x'),
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
                수료=('TRNEE_STATUS', lambda x: (x == '수료').sum()),
                평균_결석=('결석_횟수', 'mean'),
            ).reset_index()
            age_grp['수료율(%)'] = (age_grp['수료'] / age_grp['인원'] * 100).round(1)
            age_grp['평균_결석'] = age_grp['평균_결석'].round(1)
            ag1, ag2 = st.columns(2)
            with ag1:
                st.altair_chart(
                    alt.Chart(age_grp).mark_bar(color='#8e44ad').encode(
                        x=alt.X('연령대:N', sort='-y', title='연령대', axis=alt.Axis(labelAngle=0)),
                        y=alt.Y('수료율(%):Q', axis=alt.Axis(title='수\n료\n율\n(%)', titleAngle=0)),
                        tooltip=['연령대', '수료율(%)', '인원'],
                    ).properties(height=250, title='연령대별 수료율'),
                    use_container_width=True,
                )
            with ag2:
                st.altair_chart(
                    alt.Chart(age_grp).mark_bar(color='#c0392b').encode(
                        x=alt.X('연령대:N', sort='-y', title='연령대', axis=alt.Axis(labelAngle=0)),
                        y=alt.Y('평균_결석:Q', axis=alt.Axis(title='평\n균\n결\n석\n일', titleAngle=0)),
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
                수료_인원=('TRNEE_STATUS', lambda x: (x == '수료').sum()),
                평균_결석=('결석_횟수', 'mean'),
                평균_지각=('지각_횟수', 'mean'),
            ).reset_index()
            type_stats['수료율(%)'] = (type_stats['수료_인원'] / type_stats['인원'] * 100).round(1)
            type_stats['평균_결석'] = type_stats['평균_결석'].round(1)
            type_stats['평균_지각'] = type_stats['평균_지각'].round(1)

            tc1, tc2 = st.columns(2)
            with tc1:
                st.altair_chart(
                    alt.Chart(type_stats).mark_bar().encode(
                        x=alt.X('TRNEE_TYPE:N', title='유형', axis=alt.Axis(labelAngle=0)),
                        y=alt.Y('수료율(%):Q', axis=alt.Axis(title='수\n료\n율\n(%)', titleAngle=0)),
                        color=alt.value('#3498db'),
                        tooltip=['TRNEE_TYPE', '수료율(%)', '인원'],
                    ).properties(height=250, title='유형별 수료율'),
                    use_container_width=True,
                )
            with tc2:
                st.altair_chart(
                    alt.Chart(type_stats).mark_bar().encode(
                        x=alt.X('TRNEE_TYPE:N', title='유형', axis=alt.Axis(labelAngle=0)),
                        y=alt.Y('평균_결석:Q', axis=alt.Axis(title='평\n균\n결\n석\n일', titleAngle=0)),
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
                y=alt.Y('ATEND_STATUS:N', axis=alt.Axis(title='출\n결\n상\n태', titleAngle=0)),
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
                        y=alt.Y('CNT:Q', axis=alt.Axis(title='결\n석\n건\n수', titleAngle=0)),
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
                                y=alt.Y('count()', axis=alt.Axis(title='건\n수', titleAngle=0)),
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
                            y=alt.Y('count()', axis=alt.Axis(title='건\n수', titleAngle=0)),
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
                        alt.Y('count()', axis=alt.Axis(title='건\n수', titleAngle=0)),
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
        st.markdown("##### 📍 출석률과 수료 상태의 상관관계")
        st.caption("결석이 많을수록 '중도탈락'이나 '제적' 상태일 확률이 높습니다.")
        if '결석_횟수' in students_df.columns:
            st.altair_chart(
                alt.Chart(students_df).mark_circle(size=60).encode(
                    x=alt.X('나이:Q', scale=alt.Scale(domain=[15, 50])),
                    y=alt.Y('결석_횟수:Q', axis=alt.Axis(title='총\n결\n석\n일\n수', titleAngle=0)),
                    color='TRNEE_STATUS',
                    tooltip=['TRNEE_NM', '나이', '결석_횟수', 'TRNEE_STATUS'],
                ).interactive().properties(height=400),
                use_container_width=True,
            )
            risk = students_df[
                (students_df['결석_횟수'] >= RISK_ABSENT) & (students_df['TRNEE_STATUS'] != '수료')
            ]
            if not risk.empty:
                st.warning(
                    f"⚠️ **출석 불량 위험군 ({len(risk)}명):** "
                    f"결석이 {RISK_ABSENT}일 이상 기록된 미수료 학생들입니다."
                )
                st.dataframe(
                    risk[['TRNEE_NM', '나이', '결석_횟수', '지각_조퇴_횟수', 'TRNEE_STATUS']],
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
    # 상태코드(A/B/C/D) 기수는 취업률 미집계 → NaN (차트에서 공백으로 표시)
    _status_mask = all_master['EI_EMPL_RATE_6'].apply(lambda x: str(x).strip() in ('A', 'B', 'C', 'D'))
    all_master.loc[_status_mask, '총_취업률'] = pd.NA
    # 3개월 취업률 (고용보험)
    all_master['취업률_3'] = pd.to_numeric(all_master['EI_EMPL_RATE_3'], errors='coerce')
    _status_mask_3 = all_master['EI_EMPL_RATE_3'].apply(lambda x: str(x).strip() in ('A', 'B', 'C', 'D'))
    all_master.loc[_status_mask_3, '취업률_3'] = pd.NA
    degr_order = all_master.sort_values('TRPR_DEGR')['기수'].tolist()

    lc, rc = st.columns(2)
    with lc:
        st.markdown("##### 기수별 수료율")
        st.altair_chart(
            alt.Chart(all_master).mark_bar(color='#3498db').encode(
                x=alt.X('기수:N', sort=degr_order, title='기수', axis=alt.Axis(labelAngle=0)),
                y=alt.Y('수료율:Q', axis=alt.Axis(title='수\n료\n율\n(%)', titleAngle=0)),
                tooltip=['기수', '수료율', 'FINI_CNT', 'TOT_PAR_MKS'],
            ).properties(height=350),
            use_container_width=True,
        )
    with rc:
        st.markdown("##### 기수별 총 취업률 (6개월)")
        st.altair_chart(
            alt.Chart(all_master).mark_bar(color='#2ecc71').encode(
                x=alt.X('기수:N', sort=degr_order, title='기수', axis=alt.Axis(labelAngle=0)),
                y=alt.Y('총_취업률:Q', axis=alt.Axis(title='취\n업\n률\n(%)', titleAngle=0)),
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
            total_per.merge(absent_per, on='기수', how='left')
            .merge(attend_per, on='기수', how='left').fillna(0)
        )
        comp['출석률'] = (comp['출석건수'] / comp['총건수'] * 100).round(1)

        lc2, rc2 = st.columns(2)
        with lc2:
            st.markdown("##### 기수별 결석 건수")
            st.altair_chart(
                alt.Chart(comp).mark_bar(color='#e74c3c').encode(
                    x=alt.X('기수:N', sort=degr_order, title='기수', axis=alt.Axis(labelAngle=0)),
                    y=alt.Y('결석건수:Q', axis=alt.Axis(title='결\n석\n건\n수', titleAngle=0)),
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
                    x=alt.X('기수:N', sort=degr_order, title='기수', axis=alt.Axis(labelAngle=0)),
                    y=alt.Y('출석률:Q', axis=alt.Axis(title='출\n석\n률\n(%)', titleAngle=0), scale=alt.Scale(domain=[80, 100])),
                    tooltip=['기수', '출석률'],
                ).properties(height=350),
                use_container_width=True,
            )

    st.markdown("##### 종합 비교 테이블")
    # 제적/중도탈락 인원 집계
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
    all_master['잔여율'] = ((all_master['TOT_PAR_MKS'] - all_master['EXPEL_CNT'] - all_master['DROP_CNT']) / all_master['TOT_PAR_MKS'].replace(0, pd.NA) * 100).round(1).fillna(0)
    summary_cols = ['기수', 'TRPR_NM', 'TR_STA_DT', 'TR_END_DT', 'TOT_TRP_CNT', 'TOT_PAR_MKS', 'EXPEL_CNT', 'DROP_CNT', '잔여율', 'FINI_CNT', '수료율', '취업률_3', '총_취업률']
    st.dataframe(
        all_master[summary_cols].rename(columns={
            'TRPR_NM': '과정명', 'TR_STA_DT': '시작일', 'TR_END_DT': '종료일',
            'TOT_TRP_CNT': '수강신청', 'TOT_PAR_MKS': '수강인원',
            'EXPEL_CNT': '제적', 'DROP_CNT': '중도탈락',
            '잔여율': '잔여율(%)', 'FINI_CNT': '수료인원',
            '수료율': '수료율(%)', '취업률_3': '3개월 취업률(%)', '총_취업률': '6개월 취업률(%)',
        }),
        column_config={
            "수강신청": st.column_config.NumberColumn(format="%d명"),
            "수강인원": st.column_config.NumberColumn(format="%d명"),
            "제적": st.column_config.NumberColumn(format="%d명"),
            "중도탈락": st.column_config.NumberColumn(format="%d명"),
            "잔여율(%)": st.column_config.NumberColumn(format="%.1f%%"),
            "수료인원": st.column_config.NumberColumn(format="%d명"),
            "수료율(%)": st.column_config.NumberColumn(format="%.1f%%"),
            "3개월 취업률(%)": st.column_config.NumberColumn(format="%.1f%%"),
            "6개월 취업률(%)": st.column_config.NumberColumn(format="%.1f%%"),
        },
        use_container_width=True,
        hide_index=True,
    )
