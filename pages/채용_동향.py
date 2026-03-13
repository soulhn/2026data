"""채용 동향 — 사람인 채용공고 데이터 기반 IT 채용 시장 분석"""
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import datetime as dt

import streamlit as st
import pandas as pd
import plotly.express as px
from utils import check_password, load_cache_json, load_data, page_error_boundary
from config import CACHE_TTL_SARAMIN, CacheKey

with page_error_boundary():
    check_password()

    st.title("채용 동향")
    st.caption("사람인 API 기반 IT 채용공고 분석")

    # ── 데이터 존재 확인 ──
    kpi_data = load_cache_json(CacheKey.SARAMIN_KPI)
    if not kpi_data:
        st.warning("채용 데이터가 아직 수집되지 않았습니다. ETL 실행 후 다시 확인해주세요.")
        st.stop()

    kpi = kpi_data[0] if kpi_data else {}
    total_cnt = int(kpi.get('CNT') or 0)
    active_cnt = int(kpi.get('ACTIVE_CNT') or 0)
    expired_cnt = int(kpi.get('EXPIRED_CNT') or 0)
    company_cnt = int(kpi.get('COMPANY_CNT') or 0)

    # KPI 4열
    st.subheader("핵심 지표")
    k1, k2, k3, k4 = st.columns(4)
    k1.metric("총 수집 공고", f"{total_cnt:,}건")
    k2.metric("진행중 공고", f"{active_cnt:,}건")
    k3.metric("종료 공고", f"{expired_cnt:,}건")
    k4.metric("기업 수", f"{company_cnt:,}개")
    st.divider()

    tab1, tab2, tab3 = st.tabs(["진행중 공고", "종료 공고 분석", "키워드 분석"])

    # ================================================================
    # 탭 1: 진행중 공고
    # ================================================================
    with tab1:
        # 지역별 + 경력 요건 (2열)
        loc_data = load_cache_json(CacheKey.SARAMIN_ACTIVE_LOC)
        col_left, col_right = st.columns(2)

        with col_left:
            st.subheader("지역별 분포")
            if loc_data:
                df_loc = pd.DataFrame(loc_data)
                if not df_loc.empty:
                    fig = px.pie(
                        df_loc.head(10), names='REGION', values='CNT',
                        hole=0.4,
                    )
                    fig.update_layout(height=350)
                    st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("진행중 공고 지역 데이터가 없습니다.")

        with col_right:
            st.subheader("경력 요건 분포")
            today_str = dt.date.today().isoformat()

            @st.cache_data(ttl=CACHE_TTL_SARAMIN)
            def get_active_exp_dist():
                return load_data(f"""
                    SELECT EXPERIENCE_NM, COUNT(*) AS CNT
                    FROM TB_JOB_POSTING
                    WHERE EXPERIENCE_NM IS NOT NULL AND EXPERIENCE_NM != ''
                      AND ((EXPIRATION_DT IS NULL OR EXPIRATION_DT >= '{today_str}') AND ACTIVE = 1)
                    GROUP BY EXPERIENCE_NM ORDER BY CNT DESC
                """)

            df_exp = get_active_exp_dist()
            if not df_exp.empty:
                fig = px.pie(
                    df_exp.head(10), names='EXPERIENCE_NM', values='CNT',
                    hole=0.4,
                )
                fig.update_layout(height=350)
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("경력 요건 데이터가 없습니다.")

        st.divider()

        # 직무별 분포 (가로 막대)
        job_data = load_cache_json(CacheKey.SARAMIN_ACTIVE_JOB_CD)
        if job_data:
            st.subheader("직무별 분포")
            df_job = pd.DataFrame(job_data)
            if not df_job.empty:
                fig = px.bar(
                    df_job.head(15), x='CNT', y='JOB_MID_NM',
                    orientation='h',
                    labels={'CNT': '공고수', 'JOB_MID_NM': '직무'},
                )
                fig.update_layout(yaxis={'categoryorder': 'total ascending'}, height=400)
                st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("진행중 공고 직무 데이터가 없습니다.")

    # ================================================================
    # 탭 2: 종료 공고 분석
    # ================================================================
    with tab2:
        if expired_cnt == 0:
            st.info("종료된 공고가 아직 없습니다. 시간이 지나면서 종료 공고가 자연 누적됩니다.")
        else:
            # 월별 종료 공고 추이
            exp_monthly = load_cache_json(CacheKey.SARAMIN_EXPIRED_MONTHLY)
            if exp_monthly:
                st.subheader("월별 종료 공고 추이")
                df_em = pd.DataFrame(exp_monthly)
                if not df_em.empty:
                    fig = px.bar(
                        df_em, x='YEAR_MONTH', y='CNT',
                        labels={'YEAR_MONTH': '마감월', 'CNT': '공고수'},
                    )
                    fig.update_xaxes(type='category')
                    fig.update_layout(showlegend=False, height=350)
                    st.plotly_chart(fig, use_container_width=True)
                st.divider()

            # 직무별 종료 공고 분포
            exp_job = load_cache_json(CacheKey.SARAMIN_EXPIRED_JOB_CD)
            if exp_job:
                st.subheader("직무별 종료 공고 분포")
                df_ej = pd.DataFrame(exp_job)
                if not df_ej.empty:
                    fig = px.bar(
                        df_ej.head(15), x='CNT', y='JOB_MID_NM',
                        orientation='h',
                        labels={'CNT': '공고수', 'JOB_MID_NM': '직무'},
                    )
                    fig.update_layout(yaxis={'categoryorder': 'total ascending'}, height=400)
                    st.plotly_chart(fig, use_container_width=True)
                st.divider()

            # 직무별 평균 게시 기간
            dur_data = load_cache_json(CacheKey.SARAMIN_POSTING_DURATION)
            if dur_data:
                st.subheader("직무별 평균 게시 기간")
                df_dur = pd.DataFrame(dur_data)
                if not df_dur.empty:
                    df_dur['AVG_DAYS'] = pd.to_numeric(df_dur['AVG_DAYS'], errors='coerce')
                    fig = px.bar(
                        df_dur.head(15), x='AVG_DAYS', y='JOB_MID_NM',
                        orientation='h',
                        labels={'AVG_DAYS': '평균 게시일', 'JOB_MID_NM': '직무'},
                    )
                    fig.update_layout(yaxis={'categoryorder': 'total ascending'}, height=400)
                    st.plotly_chart(fig, use_container_width=True)

    # ================================================================
    # 탭 3: 키워드 분석
    # ================================================================
    with tab3:
        # 키워드별 추이
        kw_data = load_cache_json(CacheKey.SARAMIN_KEYWORD_TREND)
        if kw_data:
            st.subheader("키워드별 월별 공고 추이")
            df_kw = pd.DataFrame(kw_data)
            if not df_kw.empty:
                top_kws = df_kw.groupby('SEARCH_KEYWORD')['CNT'].sum().nlargest(8).index.tolist()
                df_kw_top = df_kw[df_kw['SEARCH_KEYWORD'].isin(top_kws)]
                fig = px.line(
                    df_kw_top, x='YEAR_MONTH', y='CNT', color='SEARCH_KEYWORD',
                    labels={'YEAR_MONTH': '월', 'CNT': '공고수', 'SEARCH_KEYWORD': '키워드'},
                )
                fig.update_xaxes(type='category')
                fig.update_layout(height=400)
                st.plotly_chart(fig, use_container_width=True)
            st.divider()

        # 학력별 분포
        st.subheader("학력 요건 분포")

        @st.cache_data(ttl=CACHE_TTL_SARAMIN)
        def get_edu_dist():
            return load_data("""
                SELECT EDU_LV_NM, COUNT(*) AS CNT
                FROM TB_JOB_POSTING
                WHERE EDU_LV_NM IS NOT NULL AND EDU_LV_NM != ''
                GROUP BY EDU_LV_NM ORDER BY CNT DESC
            """)

        df_edu = get_edu_dist()
        if not df_edu.empty:
            fig = px.bar(
                df_edu, x='EDU_LV_NM', y='CNT',
                labels={'EDU_LV_NM': '학력', 'CNT': '공고수'},
            )
            fig.update_layout(height=350)
            st.plotly_chart(fig, use_container_width=True)
