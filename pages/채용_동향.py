"""채용 동향 — AI캠퍼스 3개 과정(MLE·AIO·MLO)의 취업 방향별 채용공고 분석 (사람인 API)

수집된 공고는 saramin_etl.tag_tracks()가 과정 트랙으로 태깅한다(TB_JOB_POSTING_TRACK).
이 페이지는 운영 PostgreSQL 전용 — 분포·목록은 트랙·신입 필터를 파라미터로 직접 조회하고,
보존 삭제 후에도 유지돼야 하는 월별 추이만 누적 캐시를 읽는다.
"""
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import datetime as dt

import streamlit as st
import pandas as pd
import plotly.express as px
from utils import check_password, load_data, load_cache_json, page_error_boundary
from config import (
    CACHE_TTL_SARAMIN, SARAMIN_RETENTION_EXPIRED_DAYS,
    SARAMIN_TRACKS, SARAMIN_TRACK_ORDER, CacheKey,
)

TRACK_LABELS = {'ALL': '전체'}
TRACK_LABELS.update({k: f"{k} · {SARAMIN_TRACKS[k]['name']}" for k in SARAMIN_TRACK_ORDER})
TRACK_OPTIONS = ['ALL'] + list(SARAMIN_TRACK_ORDER)
EXPERIENCE_GROUP_SQL = """CASE WHEN jp.EXPERIENCE_CD = '1' THEN '신입'
                               WHEN jp.EXPERIENCE_CD = '0' THEN '경력무관'
                               WHEN jp.EXPERIENCE_CD = '3' THEN '신입/경력'
                               ELSE '경력' END"""


def _track_filter(track, entry_only):
    """WHERE 절 조각과 파라미터. 'ALL'은 어느 트랙이든 붙은 공고(중복 제거)."""
    frag = "jp.JOB_ID IN (SELECT JOB_ID FROM TB_JOB_POSTING_TRACK WHERE 1 = 1"
    params = []
    if track != 'ALL':
        frag += " AND TRACK = ?"
        params.append(track)
    if entry_only:
        frag += " AND ENTRY_LEVEL = 1"
    frag += ")"
    # 빈 리스트 대신 None — psycopg2는 빈 파라미터에도 % 포맷팅을 시도한다
    return frag, (params or None)


with page_error_boundary():
    check_password()

    st.title("채용 동향")
    st.caption("AI캠퍼스 3개 과정의 취업 방향별 채용공고 — 사람인 API 수집, 과정 트랙 규칙으로 분류")

    today_str = dt.date.today().isoformat()
    active_where = f"((jp.EXPIRATION_DT IS NULL OR jp.EXPIRATION_DT >= '{today_str}') AND jp.ACTIVE = 1)"

    # ── 데이터 존재 확인 ──
    @st.cache_data(ttl=CACHE_TTL_SARAMIN)
    def get_track_summary():
        """트랙별 진행중·신입 가능·기업 수 (과정 비교용, 전체 행 포함)."""
        return load_data(f"""
            SELECT t.TRACK AS TRACK,
                   COUNT(*) AS CNT,
                   SUM(CASE WHEN {active_where} THEN 1 ELSE 0 END) AS ACTIVE_CNT,
                   SUM(CASE WHEN {active_where} AND t.ENTRY_LEVEL = 1 THEN 1 ELSE 0 END) AS ENTRY_CNT,
                   COUNT(DISTINCT CASE WHEN {active_where} THEN jp.COMPANY_NM END) AS COMPANY_CNT
            FROM TB_JOB_POSTING_TRACK t
            JOIN TB_JOB_POSTING jp ON t.JOB_ID = jp.JOB_ID
            GROUP BY t.TRACK
            UNION ALL
            SELECT 'ALL' AS TRACK,
                   COUNT(*) AS CNT,
                   SUM(CASE WHEN {active_where} THEN 1 ELSE 0 END) AS ACTIVE_CNT,
                   SUM(CASE WHEN {active_where} AND jp.JOB_ID IN
                        (SELECT JOB_ID FROM TB_JOB_POSTING_TRACK WHERE ENTRY_LEVEL = 1)
                        THEN 1 ELSE 0 END) AS ENTRY_CNT,
                   COUNT(DISTINCT CASE WHEN {active_where} THEN jp.COMPANY_NM END) AS COMPANY_CNT
            FROM TB_JOB_POSTING jp
            WHERE jp.JOB_ID IN (SELECT JOB_ID FROM TB_JOB_POSTING_TRACK)
        """)

    @st.cache_data(ttl=CACHE_TTL_SARAMIN)
    def get_monthly():
        cached = load_cache_json(CacheKey.SARAMIN_TRACK_MONTHLY)
        return pd.DataFrame(cached) if cached else pd.DataFrame(columns=['TRACK', 'YEAR_MONTH', 'CNT', 'ENTRY_CNT'])

    df_summary = get_track_summary()
    if df_summary.empty or int(df_summary['CNT'].sum() or 0) == 0:
        st.warning("아직 수집·태깅된 채용공고가 없습니다. 사람인 ETL(매일 04:43) 실행 후 다시 확인해주세요.")
        st.stop()

    # ── 컨트롤 ──
    c1, c2 = st.columns([3, 1])
    with c1:
        track = st.radio(
            "과정", TRACK_OPTIONS, horizontal=True,
            format_func=lambda k: TRACK_LABELS[k],
        )
    with c2:
        entry_only = st.toggle("신입 지원 가능만", value=False,
                               help="경력무관 · 신입 · 신입/경력 공고만 표시합니다.")

    # ── 과정 카드 ──
    def _track_card(key):
        info = SARAMIN_TRACKS[key]
        st.markdown(f"**{key} · {info['name']}** — {info['direction']}")
        st.caption(info['course'])
        st.markdown("\n".join(f"- {j}" for j in info['jobs']))
        st.caption(f"핵심 스택: {info['stacks']}")

    if track == 'ALL':
        cols = st.columns(len(SARAMIN_TRACK_ORDER))
        for col, key in zip(cols, SARAMIN_TRACK_ORDER):
            with col:
                _track_card(key)
    else:
        _track_card(track)
    st.divider()

    # ── KPI ──
    row = df_summary[df_summary['TRACK'] == track]
    row = row.iloc[0] if not row.empty else None
    active_cnt = int(row['ACTIVE_CNT'] or 0) if row is not None else 0
    entry_cnt = int(row['ENTRY_CNT'] or 0) if row is not None else 0
    company_cnt = int(row['COMPANY_CNT'] or 0) if row is not None else 0
    df_monthly = get_monthly()
    df_m_track = df_monthly[df_monthly['TRACK'] == track] if not df_monthly.empty else df_monthly
    cumulative_cnt = int(df_m_track['CNT'].sum()) if not df_m_track.empty else \
        (int(row['CNT'] or 0) if row is not None else 0)
    entry_ratio = (entry_cnt / active_cnt * 100) if active_cnt else 0.0

    st.subheader("핵심 지표")
    k1, k2, k3, k4 = st.columns(4)
    k1.metric("진행중 공고", f"{active_cnt:,}건")
    k2.metric("신입 가능 공고", f"{entry_cnt:,}건", delta=f"{entry_ratio:.1f}%", delta_color="off",
              help="진행중 공고 중 경력무관 · 신입 · 신입/경력 비율입니다.")
    k3.metric("기업 수", f"{company_cnt:,}개", help="진행중 공고 기준입니다.")
    k4.metric("누적 수집 공고", f"{cumulative_cnt:,}건",
              help=f"수집 이후 전체 누적(누적 캐시). 마감 후 {SARAMIN_RETENTION_EXPIRED_DAYS}일이 지나 원본에서 삭제된 공고도 포함합니다.")
    st.divider()

    where_frag, where_params = _track_filter(track, entry_only)
    scope_label = "신입 가능 · 진행중" if entry_only else "진행중"

    tab1, tab2, tab3, tab4, tab5 = st.tabs(["과정 비교", "진행중 공고 분석", "월별 추이", "공고 목록", "수집 현황"])

    # ================================================================
    # 탭 1: 과정 비교
    # ================================================================
    with tab1:
        st.subheader("과정별 진행중 공고")
        df_cmp = df_summary[df_summary['TRACK'].isin(SARAMIN_TRACK_ORDER)].copy()
        if not df_cmp.empty:
            df_cmp['과정'] = df_cmp['TRACK'].map(TRACK_LABELS)
            df_cmp['순서'] = df_cmp['TRACK'].map({k: i for i, k in enumerate(SARAMIN_TRACK_ORDER)})
            df_cmp = df_cmp.sort_values('순서')
            df_long = df_cmp.melt(
                id_vars=['과정'], value_vars=['ACTIVE_CNT', 'ENTRY_CNT'],
                var_name='구분', value_name='건수',
            )
            df_long['구분'] = df_long['구분'].map({'ACTIVE_CNT': '진행중 전체', 'ENTRY_CNT': '신입 가능'})
            fig = px.bar(df_long, x='과정', y='건수', color='구분', barmode='group', text='건수')
            fig.update_xaxes(type='category')
            fig.update_layout(height=380, xaxis_title=None, yaxis_title=None, legend_title_text=None)
            st.plotly_chart(fig, width='stretch')

            df_tbl = df_cmp[['과정', 'ACTIVE_CNT', 'ENTRY_CNT', 'COMPANY_CNT']].copy()
            df_tbl['신입 가능 비율(%)'] = (df_tbl['ENTRY_CNT'] / df_tbl['ACTIVE_CNT'].replace(0, pd.NA) * 100).round(1)
            df_tbl = df_tbl.rename(columns={
                'ACTIVE_CNT': '진행중 공고', 'ENTRY_CNT': '신입 가능 공고', 'COMPANY_CNT': '기업 수',
            })
            st.dataframe(df_tbl, hide_index=True, width='stretch')
            st.caption("한 공고가 여러 과정에 해당할 수 있어 과정별 합은 전체보다 큽니다. "
                       "분류 규칙은 config.SARAMIN_TRACK_RULES, 설계 근거는 docs/track_job_mapping.md 참고.")
        else:
            st.info("과정별 집계가 없습니다.")

    # ================================================================
    # 탭 2: 진행중 공고 분석 (트랙·신입 필터 적용)
    # ================================================================
    with tab2:
        st.caption(f"범위: {TRACK_LABELS[track]} · {scope_label}")

        @st.cache_data(ttl=CACHE_TTL_SARAMIN)
        def get_region_dist(track, entry_only):
            frag, params = _track_filter(track, entry_only)
            return load_data(f"""
                SELECT jr.REGION AS REGION, COUNT(*) AS CNT
                FROM TB_JOB_POSTING jp
                JOIN TB_JOB_POSTING_REGION jr ON jr.JOB_ID = jp.JOB_ID
                WHERE {active_where} AND {frag}
                GROUP BY jr.REGION ORDER BY CNT DESC
            """, params=params)

        @st.cache_data(ttl=CACHE_TTL_SARAMIN)
        def get_experience_dist(track, entry_only):
            frag, params = _track_filter(track, entry_only)
            return load_data(f"""
                SELECT {EXPERIENCE_GROUP_SQL} AS EXPERIENCE_GROUP, COUNT(*) AS CNT
                FROM TB_JOB_POSTING jp
                WHERE {active_where} AND {frag}
                GROUP BY {EXPERIENCE_GROUP_SQL} ORDER BY CNT DESC
            """, params=params)

        @st.cache_data(ttl=CACHE_TTL_SARAMIN)
        def get_job_keyword_dist(track, entry_only):
            """사람인 직무 키워드(JOB_NM, 쉼표 목록)를 토큰으로 풀어 상위 20개."""
            frag, params = _track_filter(track, entry_only)
            return load_data(f"""
                SELECT TRIM(tok) AS JOB_KEYWORD, COUNT(*) AS CNT
                FROM TB_JOB_POSTING jp,
                     UNNEST(STRING_TO_ARRAY(jp.JOB_NM, ',')) AS tok
                WHERE {active_where} AND {frag}
                  AND jp.JOB_NM IS NOT NULL AND jp.JOB_NM != ''
                GROUP BY TRIM(tok) ORDER BY CNT DESC LIMIT 20
            """, params=params)

        @st.cache_data(ttl=CACHE_TTL_SARAMIN)
        def get_company_dist(track, entry_only):
            frag, params = _track_filter(track, entry_only)
            return load_data(f"""
                SELECT jp.COMPANY_NM AS COMPANY_NM, COUNT(*) AS CNT
                FROM TB_JOB_POSTING jp
                WHERE {active_where} AND {frag}
                  AND jp.COMPANY_NM IS NOT NULL AND jp.COMPANY_NM != ''
                GROUP BY jp.COMPANY_NM ORDER BY CNT DESC LIMIT 20
            """, params=params)

        col_left, col_right = st.columns(2)
        with col_left:
            st.subheader("지역별 분포")
            df_loc = get_region_dist(track, entry_only)
            if not df_loc.empty:
                fig = px.pie(df_loc.head(10), names='REGION', values='CNT', hole=0.4)
                fig.update_layout(height=350)
                st.plotly_chart(fig, width='stretch')
            else:
                st.info("해당 범위의 지역 데이터가 없습니다.")
        with col_right:
            st.subheader("경력 요건 분포")
            df_exp = get_experience_dist(track, entry_only)
            if not df_exp.empty:
                fig = px.pie(df_exp, names='EXPERIENCE_GROUP', values='CNT', hole=0.4)
                fig.update_layout(height=350)
                st.plotly_chart(fig, width='stretch')
            else:
                st.info("해당 범위의 경력 데이터가 없습니다.")

        st.divider()
        col_a, col_b = st.columns(2)
        with col_a:
            st.subheader("직무 키워드 상위 20")
            st.caption("사람인 공고에 붙은 직무·스킬 키워드 기준")
            df_jk = get_job_keyword_dist(track, entry_only)
            if not df_jk.empty:
                fig = px.bar(df_jk, x='CNT', y='JOB_KEYWORD', orientation='h')
                fig.update_layout(yaxis={'categoryorder': 'total ascending', 'title': None},
                                  xaxis_title=None, height=520)
                st.plotly_chart(fig, width='stretch')
            else:
                st.info("직무 키워드 데이터가 없습니다.")
        with col_b:
            st.subheader("채용 기업 상위 20")
            df_co = get_company_dist(track, entry_only)
            if not df_co.empty:
                fig = px.bar(df_co, x='CNT', y='COMPANY_NM', orientation='h')
                fig.update_layout(yaxis={'categoryorder': 'total ascending', 'title': None},
                                  xaxis_title=None, height=520)
                st.plotly_chart(fig, width='stretch')
            else:
                st.info("기업 데이터가 없습니다.")

    # ================================================================
    # 탭 3: 월별 추이 — 누적 캐시 (보존 삭제와 무관하게 시계열 유지)
    # ================================================================
    with tab3:
        st.subheader("월별 신규 공고 추이")
        st.caption("게시월 기준 누적 캐시입니다. 가장 최근 월은 수집이 진행 중인 값입니다. "
                   "과정 특화 수집은 2026-09-08부터 시작했습니다.")
        if df_monthly.empty:
            st.info("월별 추이 데이터가 아직 없습니다. ETL 실행 후 생성됩니다.")
        elif track == 'ALL':
            df_lines = df_monthly[df_monthly['TRACK'].isin(SARAMIN_TRACK_ORDER)].copy()
            df_lines['과정'] = df_lines['TRACK'].map(TRACK_LABELS)
            ycol = 'ENTRY_CNT' if entry_only else 'CNT'
            fig = px.line(df_lines.sort_values('YEAR_MONTH'), x='YEAR_MONTH', y=ycol, color='과정', markers=True)
            fig.update_xaxes(type='category', categoryorder='category ascending')
            fig.update_layout(height=400, xaxis_title=None, yaxis_title=None, legend_title_text=None)
            st.plotly_chart(fig, width='stretch')
        else:
            df_one = df_m_track.sort_values('YEAR_MONTH')
            df_long = df_one.melt(id_vars=['YEAR_MONTH'], value_vars=['CNT', 'ENTRY_CNT'],
                                  var_name='구분', value_name='건수')
            df_long['구분'] = df_long['구분'].map({'CNT': '전체', 'ENTRY_CNT': '신입 가능'})
            fig = px.bar(df_long, x='YEAR_MONTH', y='건수', color='구분', barmode='group')
            fig.update_xaxes(type='category', categoryorder='category ascending')
            fig.update_layout(height=400, xaxis_title=None, yaxis_title=None, legend_title_text=None)
            st.plotly_chart(fig, width='stretch')

    # ================================================================
    # 탭 4: 공고 목록
    # ================================================================
    with tab4:
        st.caption(f"범위: {TRACK_LABELS[track]} · {scope_label} · 최신 게시순 최대 500건")

        @st.cache_data(ttl=CACHE_TTL_SARAMIN)
        def get_postings(track, entry_only):
            frag, params = _track_filter(track, entry_only)
            df = load_data(f"""
                SELECT jp.JOB_ID AS JOB_ID, jp.POSITION_TITLE AS POSITION_TITLE, jp.COMPANY_NM AS COMPANY_NM,
                       jp.REGION AS REGION, jp.EXPERIENCE_NM AS EXPERIENCE_NM, jp.EDU_LV_NM AS EDU_LV_NM,
                       jp.POSTING_DT AS POSTING_DT, jp.EXPIRATION_DT AS EXPIRATION_DT,
                       jp.POSITION_URL AS POSITION_URL,
                       (SELECT STRING_AGG(t2.TRACK, ', ' ORDER BY t2.TRACK)
                          FROM TB_JOB_POSTING_TRACK t2 WHERE t2.JOB_ID = jp.JOB_ID) AS TRACKS
                FROM TB_JOB_POSTING jp
                WHERE {active_where} AND {frag}
                ORDER BY jp.POSTING_DT DESC NULLS LAST, jp.JOB_ID DESC
                LIMIT 500
            """, params=params)
            return df

        df_list = get_postings(track, entry_only)
        if df_list.empty:
            st.info("해당 범위의 진행중 공고가 없습니다.")
        else:
            f1, f2 = st.columns([2, 1])
            with f1:
                q = st.text_input("제목 · 기업명 검색", placeholder="예: LLM, 데이터 엔지니어, 기업명")
            with f2:
                regions = sorted(df_list['REGION'].dropna().unique().tolist())
                sel_regions = st.multiselect("지역", regions)
            df_show = df_list
            if q:
                mask = df_show['POSITION_TITLE'].str.contains(q, case=False, na=False) | \
                    df_show['COMPANY_NM'].str.contains(q, case=False, na=False)
                df_show = df_show[mask]
            if sel_regions:
                df_show = df_show[df_show['REGION'].isin(sel_regions)]

            st.caption(f"{len(df_show):,}건 표시")
            df_view = df_show.rename(columns={
                'POSITION_TITLE': '공고 제목', 'COMPANY_NM': '기업명', 'REGION': '지역',
                'EXPERIENCE_NM': '경력', 'EDU_LV_NM': '학력', 'POSTING_DT': '게시일',
                'EXPIRATION_DT': '마감일', 'POSITION_URL': '링크', 'TRACKS': '해당 과정',
            })[['공고 제목', '기업명', '지역', '경력', '학력', '게시일', '마감일', '해당 과정', '링크']]
            st.dataframe(
                df_view, hide_index=True, width='stretch', height=600,
                column_config={'링크': st.column_config.LinkColumn('링크', display_text='보기')},
            )
            st.download_button(
                "CSV 다운로드", df_view.to_csv(index=False).encode('utf-8-sig'),
                file_name=f"채용공고_{track}_{today_str}.csv", mime="text/csv",
            )

    # ================================================================
    # 탭 5: 수집 현황
    # ================================================================
    with tab5:
        st.subheader("수집 쿼리별 보유 공고")
        st.caption("직무 코드(예: 데이터엔지니어(83))와 키워드 두 축으로 수집합니다. "
                   "어느 과정 공고인지는 수집 쿼리가 아니라 분류 규칙이 정하므로, 이 표는 수집 폭을 점검하는 용도입니다.")

        @st.cache_data(ttl=CACHE_TTL_SARAMIN)
        def get_query_hits():
            cached = load_cache_json(CacheKey.SARAMIN_QUERY_HITS)
            return pd.DataFrame(cached) if cached else pd.DataFrame(columns=['SEARCH_KEYWORD', 'CNT'])

        @st.cache_data(ttl=CACHE_TTL_SARAMIN)
        def get_tag_status():
            return load_data("""
                SELECT COUNT(*) AS CNT, COUNT(DISTINCT JOB_ID) AS JOB_CNT, MAX(TAGGED_AT) AS TAGGED_AT
                FROM TB_JOB_POSTING_TRACK
            """)

        df_hits = get_query_hits()
        if not df_hits.empty:
            fig = px.bar(df_hits, x='CNT', y='SEARCH_KEYWORD', orientation='h')
            fig.update_layout(yaxis={'categoryorder': 'total ascending', 'title': None},
                              xaxis_title=None, height=max(400, 18 * len(df_hits)))
            st.plotly_chart(fig, width='stretch')
        else:
            st.info("수집 현황 캐시가 아직 없습니다. ETL 실행 후 생성됩니다.")

        df_tag = get_tag_status()
        if not df_tag.empty:
            r = df_tag.iloc[0]
            st.caption(f"트랙 태깅: 공고 {int(r['JOB_CNT'] or 0):,}건 · 태그 {int(r['CNT'] or 0):,}건 · "
                       f"마지막 태깅 {r['TAGGED_AT']}")
