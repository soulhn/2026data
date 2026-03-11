"""채용 동향 — 사람인 채용공고 데이터 기반 IT 채용 시장 분석"""
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

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

    tab1, tab2 = st.tabs(["채용 시장 개요", "직무별 분석"])

    # ================================================================
    # 탭 1: 채용 시장 개요
    # ================================================================
    with tab1:
        # KPI
        kpi = kpi_data[0] if kpi_data else {}
        total_cnt = int(kpi.get('CNT') or 0)
        active_cnt = int(kpi.get('ACTIVE_CNT') or 0)
        company_cnt = int(kpi.get('COMPANY_CNT') or 0)

        st.subheader("핵심 지표")
        k1, k2, k3 = st.columns(3)
        k1.metric("총 수집 공고", f"{total_cnt:,}건")
        k2.metric("진행중 공고", f"{active_cnt:,}건")
        k3.metric("기업 수", f"{company_cnt:,}개")
        st.divider()

        # 월별 공고 추이
        monthly_data = load_cache_json(CacheKey.SARAMIN_MONTHLY)
        if monthly_data:
            st.subheader("월별 공고 추이")
            df_monthly = pd.DataFrame(monthly_data)
            if not df_monthly.empty:
                fig = px.bar(
                    df_monthly, x='YEAR_MONTH', y='CNT',
                    labels={'YEAR_MONTH': '월', 'CNT': '공고수'},
                )
                fig.update_xaxes(type='category')
                fig.update_layout(showlegend=False, height=350)
                st.plotly_chart(fig, use_container_width=True)
            st.divider()

        # 지역별 분포
        loc_data = load_cache_json(CacheKey.SARAMIN_LOC)
        if loc_data:
            col_left, col_right = st.columns(2)

            with col_left:
                st.subheader("지역별 공고 분포")
                df_loc = pd.DataFrame(loc_data)
                if not df_loc.empty:
                    fig = px.pie(
                        df_loc.head(10), names='REGION', values='CNT',
                        hole=0.4,
                    )
                    fig.update_layout(height=350)
                    st.plotly_chart(fig, use_container_width=True)

            # 경력/학력 분포 (직접 쿼리)
            with col_right:
                st.subheader("경력 요건 분포")

                @st.cache_data(ttl=CACHE_TTL_SARAMIN)
                def get_exp_dist():
                    return load_data("""
                        SELECT EXPERIENCE_NM, COUNT(*) AS CNT
                        FROM TB_JOB_POSTING
                        WHERE EXPERIENCE_NM IS NOT NULL AND EXPERIENCE_NM != ''
                        GROUP BY EXPERIENCE_NM ORDER BY CNT DESC
                    """)

                df_exp = get_exp_dist()
                if not df_exp.empty:
                    fig = px.pie(
                        df_exp.head(10), names='EXPERIENCE_NM', values='CNT',
                        hole=0.4,
                    )
                    fig.update_layout(height=350)
                    st.plotly_chart(fig, use_container_width=True)

    # ================================================================
    # 탭 2: 직무별 분석
    # ================================================================
    with tab2:
        # 직무코드별 순위
        job_data = load_cache_json(CacheKey.SARAMIN_JOB_CD)
        if job_data:
            st.subheader("상위 직무 분류별 공고수")
            df_job = pd.DataFrame(job_data)
            if not df_job.empty:
                fig = px.bar(
                    df_job.head(15), x='CNT', y='JOB_MID_NM',
                    orientation='h',
                    labels={'CNT': '공고수', 'JOB_MID_NM': '직무'},
                )
                fig.update_layout(yaxis={'categoryorder': 'total ascending'}, height=400)
                st.plotly_chart(fig, use_container_width=True)
            st.divider()

        # 키워드별 추이
        kw_data = load_cache_json(CacheKey.SARAMIN_KEYWORD_TREND)
        if kw_data:
            st.subheader("키워드별 월별 공고 추이")
            df_kw = pd.DataFrame(kw_data)
            if not df_kw.empty:
                # 상위 키워드만 필터
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
