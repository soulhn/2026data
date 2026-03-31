"""채용 동향 — 사람인 채용공고 데이터 기반 IT 채용 시장 분석"""
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import datetime as dt

import streamlit as st
import pandas as pd
import plotly.express as px
from utils import check_password, is_pg, load_data, load_cache_json, page_error_boundary
from config import CACHE_TTL_SARAMIN, CacheKey

with page_error_boundary():
    check_password()

    st.title("채용 동향")
    st.caption("사람인 API 기반 IT 채용공고 분석")

    today_str = dt.date.today().isoformat()
    active_where = f"((EXPIRATION_DT IS NULL OR EXPIRATION_DT >= '{today_str}') AND ACTIVE = 1)"
    expired_where = f"(EXPIRATION_DT < '{today_str}' OR ACTIVE = 0)"

    # ── KPI ──
    @st.cache_data(ttl=CACHE_TTL_SARAMIN)
    def get_kpi():
        return load_data(f"""
            SELECT COUNT(*) AS CNT,
                   SUM(CASE WHEN {active_where} THEN 1 ELSE 0 END) AS ACTIVE_CNT,
                   SUM(CASE WHEN {expired_where} THEN 1 ELSE 0 END) AS EXPIRED_CNT,
                   COUNT(DISTINCT COMPANY_NM) AS COMPANY_CNT
            FROM TB_JOB_POSTING
        """)

    df_kpi = get_kpi()
    if df_kpi.empty or int(df_kpi.iloc[0].get('CNT') or 0) == 0:
        st.warning("채용 데이터가 아직 수집되지 않았습니다. ETL 실행 후 다시 확인해주세요.")
        st.stop()

    row = df_kpi.iloc[0]
    total_cnt = int(row.get('CNT') or 0)
    active_cnt = int(row.get('ACTIVE_CNT') or 0)
    expired_cnt = int(row.get('EXPIRED_CNT') or 0)
    company_cnt = int(row.get('COMPANY_CNT') or 0)

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
        col_left, col_right = st.columns(2)

        with col_left:
            st.subheader("지역별 분포")

            @st.cache_data(ttl=CACHE_TTL_SARAMIN)
            def get_active_loc():
                return load_data(f"""
                    SELECT REGION, COUNT(*) AS CNT
                    FROM TB_JOB_POSTING
                    WHERE REGION IS NOT NULL AND REGION != ''
                      AND {active_where}
                    GROUP BY REGION ORDER BY CNT DESC
                """)

            df_loc = get_active_loc()
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

            @st.cache_data(ttl=CACHE_TTL_SARAMIN)
            def get_active_exp_dist():
                return load_data(f"""
                    SELECT EXPERIENCE_NM, COUNT(*) AS CNT
                    FROM TB_JOB_POSTING
                    WHERE EXPERIENCE_NM IS NOT NULL AND EXPERIENCE_NM != ''
                      AND {active_where}
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
        st.subheader("직무별 분포")

        @st.cache_data(ttl=CACHE_TTL_SARAMIN)
        def get_active_job():
            return load_data(f"""
                SELECT JOB_MID_NM, COUNT(*) AS CNT
                FROM TB_JOB_POSTING
                WHERE JOB_MID_NM IS NOT NULL AND JOB_MID_NM != ''
                  AND {active_where}
                GROUP BY JOB_MID_NM ORDER BY CNT DESC
            """)

        df_job = get_active_job()
        if not df_job.empty:
            fig = px.bar(
                df_job.head(15), x='CNT', y='JOB_MID_NM',
                orientation='h',
            )
            fig.update_layout(
                yaxis={'categoryorder': 'total ascending', 'title': None},
                xaxis_title=None, height=400,
            )
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
            st.subheader("월별 종료 공고 추이")

            @st.cache_data(ttl=CACHE_TTL_SARAMIN)
            def get_expired_monthly():
                if is_pg():
                    month_expr = "TO_CHAR(EXPIRATION_DT::date, 'YYYY-MM')"
                else:
                    month_expr = "SUBSTR(EXPIRATION_DT, 1, 7)"
                return load_data(f"""
                    SELECT {month_expr} AS YEAR_MONTH, COUNT(*) AS CNT
                    FROM TB_JOB_POSTING
                    WHERE EXPIRATION_DT IS NOT NULL AND {expired_where}
                    GROUP BY {month_expr} ORDER BY YEAR_MONTH
                """)

            df_em = get_expired_monthly()
            if not df_em.empty:
                fig = px.bar(
                    df_em, x='YEAR_MONTH', y='CNT',
                )
                fig.update_xaxes(type='category')
                fig.update_layout(
                    showlegend=False, height=350,
                    xaxis_title=None, yaxis_title=None,
                )
                st.plotly_chart(fig, use_container_width=True)
            st.divider()

            # 직무별 종료 공고 분포
            st.subheader("직무별 종료 공고 분포")

            @st.cache_data(ttl=CACHE_TTL_SARAMIN)
            def get_expired_job():
                return load_data(f"""
                    SELECT JOB_MID_NM, COUNT(*) AS CNT
                    FROM TB_JOB_POSTING
                    WHERE JOB_MID_NM IS NOT NULL AND JOB_MID_NM != ''
                      AND {expired_where}
                    GROUP BY JOB_MID_NM ORDER BY CNT DESC
                """)

            df_ej = get_expired_job()
            if not df_ej.empty:
                fig = px.bar(
                    df_ej.head(15), x='CNT', y='JOB_MID_NM',
                    orientation='h',
                )
                fig.update_layout(
                    yaxis={'categoryorder': 'total ascending', 'title': None},
                    xaxis_title=None, height=400,
                )
                st.plotly_chart(fig, use_container_width=True)
            st.divider()

            # 직무별 평균 게시 기간
            st.subheader("직무별 평균 게시 기간")

            @st.cache_data(ttl=CACHE_TTL_SARAMIN)
            def get_posting_duration():
                if is_pg():
                    dur_expr = "(EXPIRATION_DT::date - POSTING_DT::date)"
                else:
                    dur_expr = "(JULIANDAY(EXPIRATION_DT) - JULIANDAY(POSTING_DT))"
                return load_data(f"""
                    SELECT JOB_MID_NM, AVG({dur_expr}) AS AVG_DAYS
                    FROM TB_JOB_POSTING
                    WHERE JOB_MID_NM IS NOT NULL AND JOB_MID_NM != ''
                      AND EXPIRATION_DT IS NOT NULL AND POSTING_DT IS NOT NULL
                      AND {expired_where}
                    GROUP BY JOB_MID_NM ORDER BY AVG_DAYS DESC
                """)

            df_dur = get_posting_duration()
            if not df_dur.empty:
                df_dur['AVG_DAYS'] = pd.to_numeric(df_dur['AVG_DAYS'], errors='coerce')
                fig = px.bar(
                    df_dur.head(15), x='AVG_DAYS', y='JOB_MID_NM',
                    orientation='h',
                )
                fig.update_layout(
                    yaxis={'categoryorder': 'total ascending', 'title': None},
                    xaxis_title=None, height=400,
                )
                st.plotly_chart(fig, use_container_width=True)

    # ================================================================
    # 탭 3: 키워드 분석
    # ================================================================
    with tab3:
        # 검색 키워드별 공고 수
        st.subheader("검색 키워드별 공고 수")

        @st.cache_data(ttl=CACHE_TTL_SARAMIN)
        def get_keyword_dist():
            cached = load_cache_json(CacheKey.SARAMIN_KEYWORD_DIST)
            if cached:
                return pd.DataFrame(cached)
            return load_data("""
                SELECT SEARCH_KEYWORD, COUNT(*) AS CNT
                FROM TB_JOB_POSTING
                WHERE SEARCH_KEYWORD IS NOT NULL AND SEARCH_KEYWORD != ''
                GROUP BY SEARCH_KEYWORD ORDER BY CNT DESC
            """)

        df_kd = get_keyword_dist()
        if not df_kd.empty:
            fig = px.bar(
                df_kd, x='CNT', y='SEARCH_KEYWORD', orientation='h',
            )
            fig.update_layout(
                yaxis={'categoryorder': 'total ascending', 'title': None},
                xaxis_title=None, height=400,
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("키워드 데이터가 아직 수집되지 않았습니다.")
        st.divider()

        # 키워드별 월별 추이
        st.subheader("키워드별 월별 공고 추이")

        @st.cache_data(ttl=CACHE_TTL_SARAMIN)
        def get_keyword_trend():
            cached = load_cache_json(CacheKey.SARAMIN_KEYWORD_TREND)
            if cached:
                return pd.DataFrame(cached)
            return load_data("""
                SELECT SEARCH_KEYWORD, YEAR_MONTH, COUNT(*) AS CNT
                FROM TB_JOB_POSTING
                WHERE SEARCH_KEYWORD IS NOT NULL AND SEARCH_KEYWORD != ''
                  AND YEAR_MONTH IS NOT NULL
                GROUP BY SEARCH_KEYWORD, YEAR_MONTH
                ORDER BY YEAR_MONTH
            """)

        df_kw = get_keyword_trend()
        if not df_kw.empty:
            top_kws = df_kw.groupby('SEARCH_KEYWORD')['CNT'].sum().nlargest(8).index.tolist()
            df_kw_top = df_kw[df_kw['SEARCH_KEYWORD'].isin(top_kws)]
            fig = px.line(
                df_kw_top, x='YEAR_MONTH', y='CNT', color='SEARCH_KEYWORD',
            )
            fig.update_xaxes(type='category')
            fig.update_layout(
                height=400, xaxis_title=None, yaxis_title=None,
                legend_title_text=None,
            )
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
            )
            fig.update_layout(
                height=350, xaxis_title=None, yaxis_title=None,
            )
            st.plotly_chart(fig, use_container_width=True)
