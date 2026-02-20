import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
import datetime
import sys
import os
import math

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils import load_data, check_password, get_billing_periods, calc_revenue
from config import CACHE_TTL_DEFAULT, DAILY_TRAINING_FEE, REVENUE_FULL_THRESHOLD

st.set_page_config(page_title="매출 분석", page_icon="💰", layout="wide")
check_password()
st.title("💰 매출 분석")
st.markdown("30일 단위 청구 기간(단위기간) × 수강생별 출석률로 산출한 훈련비 매출을 분석합니다.")


# ─────────────────── 데이터 로딩 ───────────────────

@st.cache_data(ttl=CACHE_TTL_DEFAULT)
def load_course_list():
    """전체 과정 목록 (완료+진행 모두)"""
    return load_data(
        "SELECT TRPR_ID, TRPR_DEGR, TRPR_NM, TR_STA_DT, TR_END_DT "
        "FROM TB_COURSE_MASTER "
        "ORDER BY CAST(TRPR_DEGR AS INTEGER) DESC"
    )


@st.cache_data(ttl=CACHE_TTL_DEFAULT)
def load_all_attendance(trpr_id, trpr_degr):
    """과정 전체 출결 로그 1회 쿼리 (JOIN 대신 Python merge로 이름 결합)"""
    att_df = load_data(
        "SELECT TRNEE_ID, ATEND_DT, ATEND_STATUS "
        "FROM TB_ATTENDANCE_LOG "
        "WHERE TRPR_ID = ? AND TRPR_DEGR = ?",
        params=[trpr_id, trpr_degr],
    )
    trainee_df = load_data(
        "SELECT TRNEE_ID, TRNEE_NM "
        "FROM TB_TRAINEE_INFO "
        "WHERE TRPR_ID = ? AND TRPR_DEGR = ?",
        params=[trpr_id, trpr_degr],
    )
    if not att_df.empty and not trainee_df.empty:
        att_df = att_df.merge(trainee_df[['TRNEE_ID', 'TRNEE_NM']], on='TRNEE_ID', how='left')
    elif not att_df.empty:
        att_df['TRNEE_NM'] = att_df['TRNEE_ID']
    return att_df


@st.cache_data(ttl=CACHE_TTL_DEFAULT)
def build_revenue_df(trpr_id, trpr_degr, start_dt, end_dt):
    """단위기간별 수강생 매출 계산 → DataFrame"""
    periods = get_billing_periods(start_dt, end_dt)
    att_df = load_all_attendance(trpr_id, trpr_degr)
    if att_df.empty or not periods:
        return pd.DataFrame(), periods

    att_df = att_df.copy()
    att_df['ATEND_DT'] = pd.to_datetime(att_df['ATEND_DT']).dt.date

    # 출석 인정: 출석, 지각, 입실중 (조퇴 제외 - home.py 동일)
    ATTEND_STATUSES = {'출석', '지각', '입실중'}

    rows = []
    for p in periods:
        mask = (att_df['ATEND_DT'] >= p['start']) & (att_df['ATEND_DT'] <= p['end'])
        period_df = att_df[mask]

        # 훈련일수 = 해당 기간에 실제 수업이 있었던 날짜 수 (공휴일 자동 제외)
        training_days = period_df['ATEND_DT'].nunique()

        for trnee_id, grp in period_df.groupby('TRNEE_ID'):
            trnee_nm = grp['TRNEE_NM'].iloc[0] if not grp['TRNEE_NM'].isna().all() else trnee_id
            attend_days = grp[grp['ATEND_STATUS'].isin(ATTEND_STATUSES)]['ATEND_DT'].nunique()
            fee, rate, status = calc_revenue(attend_days, training_days)
            rows.append({
                'TRNEE_ID': trnee_id,
                'TRNEE_NM': trnee_nm,
                'period_num': p['period_num'],
                'period_label': p['label'],
                'period_status': p['status'],
                'period_start': p['start'],
                'period_end': p['end'],
                'attend_days': attend_days,
                'training_days': training_days,
                'rate': rate,
                'fee': fee,
                'status': status,
            })

    if not rows:
        return pd.DataFrame(), periods
    return pd.DataFrame(rows), periods


@st.cache_data(ttl=CACHE_TTL_DEFAULT)
def build_all_terms_revenue(course_df):
    """전체 기수 매출 집계"""
    results = []
    for _, row in course_df.iterrows():
        rev_df, periods = build_revenue_df(
            row['TRPR_ID'], row['TRPR_DEGR'], row['TR_STA_DT'], row['TR_END_DT']
        )
        if rev_df.empty:
            continue
        total_base = sum(p['end'] - p['start'] for p in periods)  # unused, calc below
        base_fee = sum(
            p['training_days_total'] for p in _enrich_periods(rev_df, periods)
        )
        actual_fee = rev_df.groupby(['TRNEE_ID', 'period_num'])['fee'].sum().reset_index()['fee'].sum()
        n_students = rev_df['TRNEE_ID'].nunique()
        full_cnt = (rev_df.groupby(['TRNEE_ID', 'period_num'])['status'].first() == '전액').sum()
        prop_cnt = (rev_df.groupby(['TRNEE_ID', 'period_num'])['status'].first() == '비례').sum()
        none_cnt = (rev_df.groupby(['TRNEE_ID', 'period_num'])['status'].first() == '미청구').sum()
        base_fee_total = rev_df.groupby(['TRNEE_ID', 'period_num']).apply(
            lambda g: g.iloc[0]['training_days'] * DAILY_TRAINING_FEE
        ).sum()
        results.append({
            'TRPR_DEGR': row['TRPR_DEGR'],
            'TRPR_NM': row['TRPR_NM'],
            'TR_STA_DT': row['TR_STA_DT'],
            'TR_END_DT': row['TR_END_DT'],
            'n_students': n_students,
            'base_fee': base_fee_total,
            'actual_fee': actual_fee,
            'achievement': round(actual_fee / base_fee_total * 100, 1) if base_fee_total > 0 else 0,
            'loss_prop': base_fee_total - actual_fee,
            'full_cnt': int(full_cnt),
            'prop_cnt': int(prop_cnt),
            'none_cnt': int(none_cnt),
        })
    return pd.DataFrame(results)


def _enrich_periods(rev_df, periods):
    """periods에 training_days_total 추가 (첫 번째 수강생 기준)"""
    if rev_df.empty:
        for p in periods:
            p['training_days_total'] = 0
        return periods
    td = rev_df.groupby('period_num')['training_days'].first().to_dict()
    for p in periods:
        p['training_days_total'] = td.get(p['period_num'], 0)
    return periods


# ─────────────────── 공통 포맷 ───────────────────

def fmt_won(v):
    return f"₩{v:,.0f}"


STATUS_COLOR = {'전액': '#2ecc71', '비례': '#f39c12', '미청구': '#e74c3c', '해당없음': '#95a5a6'}
STATUS_BG = {'전액': 'background-color:#d5f5e3', '비례': 'background-color:#fdebd0',
             '미청구': 'background-color:#fadbd8', '해당없음': ''}


# ─────────────────── 메인 ───────────────────

course_list = load_course_list()
if course_list.empty:
    st.warning("과정 데이터가 없습니다. ETL을 먼저 실행하세요.")
    st.stop()

mode = st.radio("분석 모드", ["개별 기수 분석", "전체 기수 비교"], horizontal=True)
st.divider()


# ══════════════════════════════════════════════
#  개별 기수 분석
# ══════════════════════════════════════════════
if mode == "개별 기수 분석":

    with st.sidebar:
        st.header("🔍 분석 대상 선택")
        selected_degr = st.selectbox(
            "회차(기수)를 선택하세요",
            course_list['TRPR_DEGR'].unique(),
            format_func=lambda x: f"{x}회차",
        )
        sel_row = course_list[course_list['TRPR_DEGR'] == selected_degr].iloc[0]
        st.info(
            f"**과정명:** {sel_row['TRPR_NM']}\n\n"
            f"**기간:** {sel_row['TR_STA_DT']} ~ {sel_row['TR_END_DT']}"
        )

    trpr_id = sel_row['TRPR_ID']
    trpr_degr = sel_row['TRPR_DEGR']
    start_dt = sel_row['TR_STA_DT']
    end_dt = sel_row['TR_END_DT']

    rev_df, periods = build_revenue_df(trpr_id, trpr_degr, start_dt, end_dt)
    periods = _enrich_periods(rev_df, periods)

    if rev_df.empty:
        st.warning("출결 데이터가 없어 매출 계산을 할 수 없습니다.")
        st.stop()

    # ── KPI 집계 ──
    period_student_df = rev_df.groupby(['TRNEE_ID', 'period_num']).agg(
        fee=('fee', 'first'),
        status=('status', 'first'),
        training_days=('training_days', 'first'),
    ).reset_index()

    base_fee_total = int((period_student_df['training_days'] * DAILY_TRAINING_FEE).sum())
    actual_fee_total = int(period_student_df['fee'].sum())
    achievement = round(actual_fee_total / base_fee_total * 100, 1) if base_fee_total > 0 else 0
    loss_total = base_fee_total - actual_fee_total
    full_cnt = int((period_student_df['status'] == '전액').sum())
    prop_cnt = int((period_student_df['status'] == '비례').sum())
    none_cnt = int((period_student_df['status'] == '미청구').sum())
    n_students = rev_df['TRNEE_ID'].nunique()
    avg_fee = int(actual_fee_total / n_students) if n_students > 0 else 0

    st.subheader(f"📊 {selected_degr}회차 매출 분석")

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("기준 매출 (전액 기준)", fmt_won(base_fee_total))
    c2.metric("실제 매출", fmt_won(actual_fee_total),
              delta=f"-{fmt_won(loss_total)}" if loss_total > 0 else None,
              delta_color="inverse" if loss_total > 0 else "normal")
    c3.metric("달성률", f"{achievement:.1f}%")
    c4.metric("비례 손실액", fmt_won(loss_total))

    c5, c6, c7, c8 = st.columns(4)
    c5.metric("전액 청구 건수", f"{full_cnt}건")
    c6.metric("비례 청구 건수", f"{prop_cnt}건")
    c7.metric("미청구 건수", f"{none_cnt}건")
    c8.metric("수강생당 평균", fmt_won(avg_fee))

    st.divider()

    tab1, tab2, tab3, tab4 = st.tabs(["📈 매출 개요", "👥 수강생별 상세", "📅 단위기간 상세", "⚠️ 위험 현황"])

    # ── 탭 1: 매출 개요 ──────────────────────────────────
    with tab1:
        # 단위기간별 집계
        period_summary = rev_df.groupby(['period_num', 'period_label', 'period_status']).agg(
            training_days=('training_days', 'first'),
            n_students=('TRNEE_ID', 'nunique'),
            actual_fee=('fee', 'sum'),
            full_cnt=('status', lambda s: (s == '전액').sum()),
            prop_cnt=('status', lambda s: (s == '비례').sum()),
            none_cnt=('status', lambda s: (s == '미청구').sum()),
        ).reset_index()
        period_summary['base_fee'] = period_summary['training_days'] * DAILY_TRAINING_FEE * period_summary['n_students']
        period_summary['achievement'] = (
            period_summary['actual_fee'] / period_summary['base_fee'] * 100
        ).round(1).fillna(0)

        # 바차트: 단위기간별 전액/비례 스택 + 기준 목표선
        period_rev = rev_df.copy()
        full_by_period = period_rev[period_rev['status'] == '전액'].groupby('period_num')['fee'].sum()
        prop_by_period = period_rev[period_rev['status'] == '비례'].groupby('period_num')['fee'].sum()
        base_by_period = period_summary.set_index('period_num')['base_fee']
        labels = period_summary.set_index('period_num')['period_label'].to_dict()
        statuses_map = period_summary.set_index('period_num')['period_status'].to_dict()

        x_labels = [labels.get(p, str(p)) for p in period_summary['period_num']]

        opacity_map = {'완료': 1.0, '진행중': 0.7, '예정': 0.4}
        fig_bar = go.Figure()

        full_vals = [full_by_period.get(p, 0) for p in period_summary['period_num']]
        prop_vals = [prop_by_period.get(p, 0) for p in period_summary['period_num']]
        base_vals = [base_by_period.get(p, 0) for p in period_summary['period_num']]
        opacities = [opacity_map.get(statuses_map.get(p, '완료'), 1.0) for p in period_summary['period_num']]

        fig_bar.add_trace(go.Bar(
            name='전액', x=x_labels, y=full_vals,
            marker_color='#2ecc71',
            opacity=0.9,
        ))
        fig_bar.add_trace(go.Bar(
            name='비례', x=x_labels, y=prop_vals,
            marker_color='#f39c12',
            opacity=0.9,
        ))
        fig_bar.add_trace(go.Scatter(
            name='기준(전액)', x=x_labels, y=base_vals,
            mode='lines+markers',
            line=dict(color='#e74c3c', dash='dash', width=2),
        ))
        fig_bar.update_layout(
            barmode='stack', title='단위기간별 매출 (전액/비례 스택)',
            xaxis_title='단위기간', yaxis_title='매출액 (원)',
            yaxis_tickformat=',',
            legend=dict(orientation='h', yanchor='bottom', y=1.02),
            height=380,
        )
        st.plotly_chart(fig_bar, use_container_width=True)

        # 누적 매출 라인차트
        period_summary_sorted = period_summary.sort_values('period_num')
        period_summary_sorted['cumulative_actual'] = period_summary_sorted['actual_fee'].cumsum()
        period_summary_sorted['cumulative_base'] = period_summary_sorted['base_fee'].cumsum()

        fig_line = go.Figure()
        fig_line.add_trace(go.Scatter(
            name='누적 실제 매출', x=x_labels,
            y=period_summary_sorted['cumulative_actual'].tolist(),
            mode='lines+markers', line=dict(color='#3498db', width=2),
        ))
        fig_line.add_trace(go.Scatter(
            name='누적 기준 매출', x=x_labels,
            y=period_summary_sorted['cumulative_base'].tolist(),
            mode='lines+markers', line=dict(color='#e74c3c', dash='dash', width=2),
        ))
        fig_line.update_layout(
            title='누적 매출 추이 (실제 vs 기준)',
            xaxis_title='단위기간', yaxis_title='누적 매출 (원)',
            yaxis_tickformat=',',
            legend=dict(orientation='h', yanchor='bottom', y=1.02),
            height=330,
        )
        st.plotly_chart(fig_line, use_container_width=True)

        # 요약 테이블
        st.subheader("단위기간 요약")
        summary_display = period_summary[['period_label', 'period_status', 'training_days',
                                          'n_students', 'full_cnt', 'prop_cnt', 'none_cnt',
                                          'actual_fee', 'achievement']].copy()
        summary_display.columns = ['기간', '상태', '훈련일', '수강생', '전액', '비례', '미청구',
                                    '실제 매출', '달성률(%)']
        summary_display['실제 매출'] = summary_display['실제 매출'].apply(fmt_won)
        st.dataframe(summary_display, use_container_width=True, hide_index=True)

    # ── 탭 2: 수강생별 상세 ──────────────────────────────
    with tab2:
        st.subheader("수강생별 단위기간 매출 매트릭스")

        # 수강생 × 단위기간 피벗
        pivot_data = {}
        trainee_order = rev_df.drop_duplicates('TRNEE_ID')[['TRNEE_ID', 'TRNEE_NM']].copy()

        for _, p in enumerate(periods):
            pn = p['period_num']
            p_df = rev_df[rev_df['period_num'] == pn]
            for _, r in p_df.iterrows():
                tid = r['TRNEE_ID']
                if tid not in pivot_data:
                    pivot_data[tid] = {}
                pivot_data[tid][pn] = {
                    'attend': r['attend_days'],
                    'rate': r['rate'],
                    'fee': r['fee'],
                    'status': r['status'],
                    'training_days': r['training_days'],
                }

        # 평면 DataFrame 생성
        flat_rows = []
        for _, tr_row in trainee_order.iterrows():
            tid = tr_row['TRNEE_ID']
            nm = tr_row['TRNEE_NM']
            row_data = {'훈련생ID': tid, '이름': nm}
            total_fee = 0
            for p in periods:
                pn = p['period_num']
                col_base = f"{pn}단위"
                cell = pivot_data.get(tid, {}).get(pn, {})
                if cell:
                    row_data[f"{col_base}_출석"] = cell['attend']
                    row_data[f"{col_base}_출석률"] = f"{cell['rate']*100:.1f}%"
                    row_data[f"{col_base}_훈련비"] = cell['fee']
                    row_data[f"{col_base}_상태"] = cell['status']
                    total_fee += cell['fee']
                else:
                    row_data[f"{col_base}_출석"] = "-"
                    row_data[f"{col_base}_출석률"] = "-"
                    row_data[f"{col_base}_훈련비"] = 0
                    row_data[f"{col_base}_상태"] = "해당없음"
            row_data['합계'] = total_fee
            flat_rows.append(row_data)

        if flat_rows:
            # 합계 행 추가
            sum_row = {'훈련생ID': '', '이름': '합계'}
            for p in periods:
                pn = p['period_num']
                col_base = f"{pn}단위"
                sum_row[f"{col_base}_출석"] = "-"
                sum_row[f"{col_base}_출석률"] = "-"
                period_fee_sum = sum(r.get(f"{col_base}_훈련비", 0) for r in flat_rows)
                sum_row[f"{col_base}_훈련비"] = period_fee_sum
                sum_row[f"{col_base}_상태"] = ""
            sum_row['합계'] = sum(r['합계'] for r in flat_rows)
            flat_rows.append(sum_row)

            detail_df = pd.DataFrame(flat_rows)

            # 훈련비 컬럼 포맷팅
            fee_cols = [c for c in detail_df.columns if '_훈련비' in c or c == '합계']
            for col in fee_cols:
                detail_df[col] = detail_df[col].apply(
                    lambda v: fmt_won(v) if isinstance(v, (int, float)) and v != 0 else (v if v != 0 else "-")
                )

            st.dataframe(detail_df.drop(columns=['훈련생ID']), use_container_width=True, hide_index=True)

            # CSV 다운로드
            csv_df = pd.DataFrame(flat_rows[:-1])  # 합계 행 제외
            csv = csv_df.to_csv(index=False, encoding='utf-8-sig')
            st.download_button(
                "📥 수강생별 매출 CSV 다운로드",
                data=csv,
                file_name=f"revenue_{trpr_degr}회차.csv",
                mime="text/csv",
            )

    # ── 탭 3: 단위기간 상세 ──────────────────────────────
    with tab3:
        period_options = {p['period_num']: p['label'] for p in periods}
        selected_period = st.selectbox(
            "단위기간 선택",
            options=list(period_options.keys()),
            format_func=lambda x: period_options[x],
        )

        p_data = rev_df[rev_df['period_num'] == selected_period].copy()
        sel_period = next(p for p in periods if p['period_num'] == selected_period)

        if p_data.empty:
            st.info("해당 기간 데이터가 없습니다.")
        else:
            col_l, col_r = st.columns(2)

            with col_l:
                # 출석률 분포 히스토그램
                fig_hist = go.Figure()
                fig_hist.add_trace(go.Histogram(
                    x=p_data['rate'] * 100,
                    nbinsx=20,
                    name='수강생',
                    marker_color='#3498db',
                ))
                fig_hist.add_vline(x=80, line_color='red', line_dash='dash', line_width=2)
                fig_hist.add_annotation(
                    x=80, y=1, yref='paper',
                    text='전액 기준 (80%)',
                    showarrow=False, xanchor='left', font=dict(color='red'),
                )
                fig_hist.update_layout(
                    title=f'{sel_period["label"]} 출석률 분포',
                    xaxis_title='출석률 (%)', yaxis_title='수강생 수',
                    height=300,
                )
                st.plotly_chart(fig_hist, use_container_width=True)

            with col_r:
                # 전액/비례/미청구 파이차트
                status_cnt = p_data['status'].value_counts().reset_index()
                status_cnt.columns = ['status', 'cnt']
                colors = [STATUS_COLOR.get(s, '#95a5a6') for s in status_cnt['status']]
                fig_pie = go.Figure(go.Pie(
                    labels=status_cnt['status'], values=status_cnt['cnt'],
                    marker_colors=colors,
                    textinfo='label+percent+value',
                ))
                fig_pie.update_layout(title='청구 유형 분포', height=300)
                st.plotly_chart(fig_pie, use_container_width=True)

            # 수강생별 테이블
            st.subheader(f"{sel_period['label']} 수강생별 상세")
            p_display = p_data[['TRNEE_NM', 'attend_days', 'training_days', 'rate', 'fee', 'status']].copy()
            p_display.columns = ['이름', '출석일', '훈련일', '출석률', '훈련비', '상태']
            p_display['출석률'] = (p_display['출석률'] * 100).round(1).astype(str) + '%'
            p_display['훈련비'] = p_display['훈련비'].apply(fmt_won)
            p_display = p_display.sort_values('출석일', ascending=False)
            st.dataframe(p_display, use_container_width=True, hide_index=True)

    # ── 탭 4: 위험 현황 ──────────────────────────────────
    with tab4:
        today = datetime.date.today()

        # 진행중 단위기간
        active_periods = [p for p in periods if p['status'] == '진행중']
        done_periods = [p for p in periods if p['status'] == '완료']

        if active_periods:
            st.subheader("⚠️ 진행중 단위기간 위험 수강생")
            for ap in active_periods:
                p_risk = rev_df[rev_df['period_num'] == ap['period_num']].copy()
                risk_students = p_risk[p_risk['rate'] < REVENUE_FULL_THRESHOLD].copy()

                if risk_students.empty:
                    st.success(f"✅ {ap['label']}: 모든 수강생 전액 청구 기준 충족")
                    continue

                st.warning(f"🚨 {ap['label']}: {len(risk_students)}명 위험")

                # 전액까지 필요한 추가 출석일 계산
                def needed_days(row):
                    td = row['training_days']
                    needed = max(0, math.ceil(REVENUE_FULL_THRESHOLD * td - row['attend_days']))
                    remaining = (ap['end'] - today).days + 1
                    return needed, remaining

                risk_rows = []
                for _, r in risk_students.iterrows():
                    needed, remaining = needed_days(r)
                    recoverable = needed <= remaining
                    loss = r['training_days'] * DAILY_TRAINING_FEE - r['fee']
                    risk_rows.append({
                        '이름': r['TRNEE_NM'],
                        '출석일': r['attend_days'],
                        '훈련일': r['training_days'],
                        '출석률': f"{r['rate']*100:.1f}%",
                        '현재 훈련비': fmt_won(r['fee']),
                        '추가 필요 출석': needed,
                        '잔여 훈련일': remaining,
                        '회복 가능': '✅' if recoverable else '❌',
                        '손실액': fmt_won(loss),
                    })
                st.dataframe(pd.DataFrame(risk_rows), use_container_width=True, hide_index=True)
        else:
            st.info("현재 진행중인 단위기간이 없습니다.")

        if done_periods:
            st.subheader("📋 완료 기간 비례 청구 내역")
            done_pnums = [p['period_num'] for p in done_periods]
            done_df = rev_df[
                (rev_df['period_num'].isin(done_pnums)) & (rev_df['status'].isin(['비례', '미청구']))
            ].copy()
            if done_df.empty:
                st.success("완료된 모든 기간에서 비례/미청구 건수가 없습니다.")
            else:
                done_display = done_df[['period_label', 'TRNEE_NM', 'attend_days',
                                         'training_days', 'rate', 'fee', 'status']].copy()
                done_display.columns = ['기간', '이름', '출석일', '훈련일', '출석률', '훈련비', '상태']
                done_display['출석률'] = (done_display['출석률'] * 100).round(1).astype(str) + '%'
                done_display['훈련비'] = done_display['훈련비'].apply(fmt_won)
                done_display['손실액'] = done_df.apply(
                    lambda r: fmt_won(r['training_days'] * DAILY_TRAINING_FEE - r['fee']), axis=1
                ).values
                st.dataframe(done_display, use_container_width=True, hide_index=True)


# ══════════════════════════════════════════════
#  전체 기수 비교
# ══════════════════════════════════════════════
else:
    st.subheader("📊 전체 기수 매출 비교")

    with st.spinner("전체 기수 매출 집계 중..."):
        all_rev = build_all_terms_revenue(course_list)

    if all_rev.empty:
        st.warning("매출 집계 데이터가 없습니다.")
        st.stop()

    all_rev_sorted = all_rev.sort_values('TRPR_DEGR')
    x_labels = [f"{r}회차" for r in all_rev_sorted['TRPR_DEGR']]

    col_l, col_r = st.columns(2)

    with col_l:
        # 기수별 총 매출 바차트
        fig_total = go.Figure()
        fig_total.add_trace(go.Bar(
            name='실제 매출', x=x_labels,
            y=all_rev_sorted['actual_fee'].tolist(),
            marker_color='#3498db',
        ))
        fig_total.add_trace(go.Bar(
            name='기준 매출', x=x_labels,
            y=all_rev_sorted['base_fee'].tolist(),
            marker_color='#bdc3c7',
            opacity=0.6,
        ))
        fig_total.update_layout(
            barmode='overlay', title='기수별 총 매출',
            xaxis_title='기수', yaxis_title='매출 (원)',
            yaxis_tickformat=',',
            height=360,
        )
        st.plotly_chart(fig_total, use_container_width=True)

    with col_r:
        # 전액/비례/미청구 스택 바 (비율)
        total_cases = all_rev_sorted['full_cnt'] + all_rev_sorted['prop_cnt'] + all_rev_sorted['none_cnt']
        fig_stack = go.Figure()
        fig_stack.add_trace(go.Bar(
            name='전액', x=x_labels,
            y=(all_rev_sorted['full_cnt'] / total_cases * 100).round(1).tolist(),
            marker_color='#2ecc71',
        ))
        fig_stack.add_trace(go.Bar(
            name='비례', x=x_labels,
            y=(all_rev_sorted['prop_cnt'] / total_cases * 100).round(1).tolist(),
            marker_color='#f39c12',
        ))
        fig_stack.add_trace(go.Bar(
            name='미청구', x=x_labels,
            y=(all_rev_sorted['none_cnt'] / total_cases * 100).round(1).tolist(),
            marker_color='#e74c3c',
        ))
        fig_stack.update_layout(
            barmode='stack', title='기수별 청구 유형 비율 (%)',
            xaxis_title='기수', yaxis_title='비율 (%)',
            height=360,
        )
        st.plotly_chart(fig_stack, use_container_width=True)

    # 종합 비교 테이블
    st.subheader("종합 비교 테이블")
    table_df = all_rev_sorted[[
        'TRPR_DEGR', 'TRPR_NM', 'TR_STA_DT', 'TR_END_DT',
        'n_students', 'base_fee', 'actual_fee', 'achievement', 'loss_prop'
    ]].copy()
    table_df.columns = ['기수', '과정명', '시작일', '종료일',
                         '수강생', '기준매출', '실제매출', '달성률(%)', '손실액']
    table_df['기준매출'] = table_df['기준매출'].apply(fmt_won)
    table_df['실제매출'] = table_df['실제매출'].apply(fmt_won)
    table_df['손실액'] = table_df['손실액'].apply(fmt_won)
    st.dataframe(table_df, use_container_width=True, hide_index=True)
