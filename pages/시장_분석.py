import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from collections import Counter
from datetime import datetime
import sys
import os

# 🚀 상위 폴더의 utils.py를 가져오기 위한 경로 설정
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils import check_password, get_connection, is_pg, load_data as _load_data, adapt_query
from config import (
    CACHE_TTL_MARKET, COST_BINS, COST_BIN_LABELS,
    SCATTER_SAMPLE_LIMIT, REGRESSION_SAMPLE_LIMIT,
    NCS_MIN_COURSES, CERT_MIN_COURSES, CERT_EMPL_MIN_COURSES, TOP_CERTS_LIMIT,
    EMPL_CODE_MAP,
)

# ==========================================
# 0. 헬퍼 함수
# ==========================================

def render_ranking_table(stats_df, display_cols, format_dict, search_col, search_label,
                         search_placeholder, top_n, title, expander_title, st_key):
    """순위 테이블 렌더링: 검색 → 주변 순위 / 기본 Top N → expander 전체."""
    st.markdown(f"##### {title}")
    query = st.text_input(search_label, placeholder=search_placeholder, key=st_key)

    if query:
        found = stats_df[stats_df[search_col].str.contains(query)]
        if not found.empty:
            top = found.iloc[0]
            st.info(f"**'{top[search_col]}'** - 순위 **{top['순위']}위**")
            idx = top.name
            neighbor = stats_df.iloc[max(0, idx-2):min(len(stats_df), idx+3)][display_cols].copy()
            st.dataframe(
                neighbor.style.format(format_dict),
                use_container_width=True, hide_index=True
            )
        else:
            st.warning(f"검색 결과가 없습니다.")
    else:
        st.dataframe(
            stats_df.head(top_n)[display_cols].style.format(format_dict),
            use_container_width=True, hide_index=True
        )

    with st.expander(expander_title):
        st.dataframe(
            stats_df[display_cols].style.format(format_dict),
            use_container_width=True, hide_index=True
        )


def render_scatter_with_overlay(market_sample, internal_scatter, x_col, y_col,
                                title, x_label, y_label, name_col='TRPR_NM',
                                quadrant_labels=None):
    """시장 산점도 + 우리 과정 오버레이 + 4분면 라인."""
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=market_sample[x_col], y=market_sample[y_col],
        mode='markers', name='시장 과정', opacity=0.4,
        marker=dict(size=6, color='steelblue'),
        text=market_sample[name_col], hoverinfo='text+x+y'
    ))
    if internal_scatter is not None and not internal_scatter.empty:
        fig.add_trace(go.Scatter(
            x=internal_scatter[x_col], y=internal_scatter[y_col],
            mode='markers', name='우리 과정',
            marker=dict(size=15, color='red', symbol='star'),
            text=internal_scatter[name_col], hoverinfo='text+x+y'
        ))
    med_x = market_sample[x_col].median()
    med_y = market_sample[y_col].median()
    fig.add_hline(y=med_y, line_dash="dash", line_color="gray", opacity=0.5)
    fig.add_vline(x=med_x, line_dash="dash", line_color="gray", opacity=0.5)

    annotations = []
    if quadrant_labels:
        for label_cfg in quadrant_labels:
            annotations.append(dict(
                x=label_cfg['x'], y=label_cfg['y'], text=label_cfg['text'],
                showarrow=False, font=dict(color=label_cfg.get('color', 'gray'), size=12)
            ))
    fig.update_layout(xaxis_title=x_label, yaxis_title=y_label, title=title, annotations=annotations)
    st.plotly_chart(fig, use_container_width=True)
    return med_x, med_y


def _vert_ytitle(fig, text, margin_l=80, xshift=-50):
    """Y축 타이틀을 글자 단위로 세로 적층 표시 (annotation 방식)."""
    import re
    items = []
    for part in re.split(r'(\([^)]+\))', text):
        if part.startswith('(') and part.endswith(')'):
            items.append(part)
        else:
            items.extend(c for c in part if c.strip())
    fig.update_layout(yaxis_title='', margin=dict(l=margin_l))
    fig.add_annotation(
        x=0, y=0.5, xref='paper', yref='paper',
        text='<br>'.join(items), showarrow=False, textangle=0,
        font=dict(size=11), align='center',
        xanchor='center', xshift=xshift,
    )
    return fig


# ==========================================
# 0-B. SQL 집계 헬퍼
# ==========================================

def build_where_clause(start_dt, end_dt, region, ncs, types, wkends, grades, keyword):
    """사이드바 필터를 SQL WHERE 절로 변환."""
    clauses, params = [], []
    if start_dt:
        clauses.append("TR_STA_DT >= ?")
        params.append(start_dt.strftime('%Y-%m-%d'))
    if end_dt:
        clauses.append("TR_STA_DT <= ?")
        params.append(end_dt.strftime('%Y-%m-%d'))
    if region and region != '전체':
        clauses.append("REGION = ?")
        params.append(region)
    if ncs and ncs != '전체':
        clauses.append("NCS_CD = ?")
        params.append(ncs)
    if types:
        clauses.append(f"TRAIN_TARGET IN ({','.join('?' * len(types))})")
        params.extend(types)
    if wkends:
        wk_reverse = {'주중': '1', '주말': '2', '주중+주말': '3'}
        codes = [wk_reverse.get(w, w) for w in wkends]
        clauses.append(f"WKEND_SE IN ({','.join('?' * len(codes))})")
        params.extend(codes)
    if grades:
        clauses.append(f"GRADE IN ({','.join('?' * len(grades))})")
        params.extend(grades)
    if keyword:
        clauses.append("TRPR_NM LIKE ?")
        params.append(f'%{keyword}%')
    where = "WHERE " + " AND ".join(clauses) if clauses else ""
    return where, params


def _sql_query(sql, params=None):
    """adapt_query를 거친 SQL 쿼리 실행."""
    return _load_data(sql, params=params)



@st.cache_data(ttl=CACHE_TTL_MARKET, show_spinner=False)
def get_market_cache(key: str):
    """TB_MARKET_CACHE에서 ETL이 pre-compute한 집계를 DataFrame으로 반환. 없으면 None."""
    try:
        import json as _json
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(adapt_query("SELECT CACHE_DATA FROM TB_MARKET_CACHE WHERE CACHE_KEY = ?"), [key])
        row = cursor.fetchone()
        conn.close()
        if row and row[0]:
            data = _json.loads(row[0])
            if data:
                return pd.DataFrame(data)
    except Exception:
        pass
    return None


@st.cache_data(ttl=CACHE_TTL_MARKET)
def load_filter_options():
    """사이드바 필터 옵션용 경량 쿼리."""
    return _sql_query("""
        SELECT DISTINCT REGION, TRAIN_TARGET, WKEND_SE, GRADE, NCS_CD,
               MIN(TR_STA_DT) as MIN_DT, MAX(TR_STA_DT) as MAX_DT
        FROM TB_MARKET_TREND
        GROUP BY REGION, TRAIN_TARGET, WKEND_SE, GRADE, NCS_CD
    """)


@st.cache_data(ttl=CACHE_TTL_MARKET, show_spinner=False)
def load_kpi_data(where, params):
    """KPI 섹션용 집계 쿼리."""
    if where == "":
        _c = get_market_cache("kpi")
        if _c is not None:
            return _c
    return _sql_query(f"""
        SELECT COUNT(*) as CNT,
               AVG(CASE WHEN TOT_TRCO > 0 THEN TOT_TRCO END) as AVG_TRCO,
               AVG(TOT_FXNUM) as AVG_FXNUM,
               AVG(CASE WHEN EI_EMPL_RATE_3 > 0 THEN EI_EMPL_RATE_3 END) as AVG_EMPL,
               AVG(CASE WHEN STDG_SCOR > 0 THEN STDG_SCOR END) as AVG_SCORE,
               AVG(CASE WHEN TOT_FXNUM > 0
                   THEN CAST(REG_COURSE_MAN AS REAL) / TOT_FXNUM * 100 END) as AVG_RECRUIT
        FROM TB_MARKET_TREND {where}
    """, params=params)


@st.cache_data(ttl=CACHE_TTL_MARKET, show_spinner=False)
def load_course_avg_score(where, params):
    """과정 평균 만족도 (과정별 평균의 평균)."""
    return _sql_query(f"""
        SELECT AVG(avg_score) as SIMPLE_SCORE FROM (
            SELECT AVG(STDG_SCOR) as avg_score
            FROM TB_MARKET_TREND {where}
              {"AND" if where else "WHERE"} STDG_SCOR > 0
            GROUP BY TRPR_NM, TRAINST_NM
        ) sub
    """, params=params)


@st.cache_data(ttl=CACHE_TTL_MARKET, show_spinner=False)
def load_monthly_counts(where, params):
    """Tab 1, 6: 월별 개설 수."""
    if where == "":
        _c = get_market_cache("monthly_counts")
        if _c is not None:
            return _c
    return _sql_query(f"""
        SELECT YEAR_MONTH, COUNT(*) as COUNT
        FROM TB_MARKET_TREND {where}
        GROUP BY YEAR_MONTH ORDER BY YEAR_MONTH
    """, params=params)


@st.cache_data(ttl=CACHE_TTL_MARKET, show_spinner=False)
def load_region_counts(where, params):
    """Tab 1: 지역별 과정 수."""
    if where == "":
        _c = get_market_cache("region_counts")
        if _c is not None:
            return _c
    return _sql_query(f"""
        SELECT REGION as 지역, COUNT(*) as 개수
        FROM TB_MARKET_TREND {where}
        GROUP BY REGION ORDER BY 개수 DESC
    """, params=params)


@st.cache_data(ttl=CACHE_TTL_MARKET, show_spinner=False)
def load_inst_stats(where, params):
    """Tab 2, 9: 기관별 집계."""
    if where == "":
        _c = get_market_cache("inst_stats")
        if _c is not None:
            return _c
    return _sql_query(f"""
        SELECT TRAINST_NM,
               COUNT(*) as TRPR_CNT,
               COALESCE(SUM(TOT_FXNUM), 0) as TOT_FXNUM,
               COALESCE(SUM(REG_COURSE_MAN), 0) as REG_COURSE_MAN,
               AVG(CASE WHEN EI_EMPL_RATE_3 > 0 THEN EI_EMPL_RATE_3 END) as AVG_EMPL,
               AVG(CASE WHEN STDG_SCOR > 0 THEN STDG_SCOR END) as AVG_SCORE
        FROM TB_MARKET_TREND {where}
        GROUP BY TRAINST_NM
    """, params=params)


@st.cache_data(ttl=CACHE_TTL_MARKET, show_spinner=False)
def load_course_agg(where, params):
    """Tab 2: 과정별 집계."""
    return _sql_query(f"""
        SELECT TRPR_NM, TRAINST_NM,
               COUNT(*) as TRPR_CNT,
               COALESCE(SUM(TOT_FXNUM), 0) as TOT_FXNUM,
               COALESCE(SUM(REG_COURSE_MAN), 0) as REG_COURSE_MAN,
               AVG(CASE WHEN EI_EMPL_RATE_3 > 0 THEN EI_EMPL_RATE_3 END) as AVG_EMPL,
               AVG(CASE WHEN STDG_SCOR > 0 THEN STDG_SCOR END) as AVG_SCORE
        FROM TB_MARKET_TREND {where}
        GROUP BY TRPR_NM, TRAINST_NM
    """, params=params)


@st.cache_data(ttl=CACHE_TTL_MARKET, show_spinner=False)
def load_type_agg(where, params):
    """Tab 2: 훈련유형별 집계."""
    return _sql_query(f"""
        SELECT TRAIN_TARGET,
               COUNT(*) as CNT,
               COALESCE(SUM(TOT_FXNUM), 0) as TOT_FXNUM,
               COALESCE(SUM(REG_COURSE_MAN), 0) as REG_COURSE_MAN
        FROM TB_MARKET_TREND {where}
        GROUP BY TRAIN_TARGET
    """, params=params)


@st.cache_data(ttl=CACHE_TTL_MARKET, show_spinner=False)
def load_ncs_agg(where, params, min_courses=5):
    """Tab 2, 7: NCS별 집계."""
    if where == "" and min_courses == 5:
        _c = get_market_cache("ncs_agg")
        if _c is not None:
            return _c
    return _sql_query(f"""
        SELECT NCS_CD,
               COUNT(*) as CNT,
               COALESCE(SUM(TOT_FXNUM), 0) as TOT_FXNUM,
               COALESCE(SUM(REG_COURSE_MAN), 0) as REG_COURSE_MAN,
               AVG(CASE WHEN REG_COURSE_MAN > 0 AND TOT_FXNUM > 0
                   THEN CAST(REG_COURSE_MAN AS REAL) / TOT_FXNUM * 100 END) as AVG_RECRUIT
        FROM TB_MARKET_TREND {where}
        GROUP BY NCS_CD HAVING COUNT(*) >= ?
        ORDER BY CNT DESC
    """, params=list(params) + [min_courses])


@st.cache_data(ttl=CACHE_TTL_MARKET, show_spinner=False)
def load_monthly_empl(where, params):
    """Tab 6: 월별 평균 취업률."""
    if where == "":
        _c = get_market_cache("monthly_empl")
        if _c is not None:
            return _c
    return _sql_query(f"""
        SELECT YEAR_MONTH as 월, AVG(EI_EMPL_RATE_3) as 평균취업률
        FROM TB_MARKET_TREND {where}
          {"AND" if where else "WHERE"} EI_EMPL_RATE_3 > 0
        GROUP BY YEAR_MONTH ORDER BY YEAR_MONTH
    """, params=params)


@st.cache_data(ttl=CACHE_TTL_MARKET, show_spinner=False)
def load_monthly_trco_stats(where, params):
    """Tab 6: 월별 훈련비 통계 (중앙값은 SQL에서 어려우므로 최소한의 데이터만 로드)."""
    return _sql_query(f"""
        SELECT YEAR_MONTH, TOT_TRCO
        FROM TB_MARKET_TREND {where}
          {"AND" if where else "WHERE"} TOT_TRCO > 0
    """, params=params)


@st.cache_data(ttl=CACHE_TTL_MARKET, show_spinner=False)
def load_monthly_region_trend(where, params, top_regions):
    """Tab 6: 지역별 월별 개설 추이."""
    if not top_regions:
        return pd.DataFrame()
    placeholders = ','.join('?' * len(top_regions))
    return _sql_query(f"""
        SELECT YEAR_MONTH, REGION, COUNT(*) as 개설수
        FROM TB_MARKET_TREND {where}
          {"AND" if where else "WHERE"} REGION IN ({placeholders})
        GROUP BY YEAR_MONTH, REGION ORDER BY YEAR_MONTH
    """, params=list(params) + top_regions)


@st.cache_data(ttl=CACHE_TTL_MARKET, show_spinner=False)
def load_competition_monthly(where, params, ncs_codes):
    """Tab 7: NCS 코드별 월별 경쟁 과정 수."""
    if not ncs_codes:
        return pd.DataFrame()
    placeholders = ','.join('?' * len(ncs_codes))
    return _sql_query(f"""
        SELECT YEAR_MONTH, NCS_CD, COUNT(*) as 경쟁과정수
        FROM TB_MARKET_TREND {where}
          {"AND" if where else "WHERE"} NCS_CD IN ({placeholders})
        GROUP BY YEAR_MONTH, NCS_CD ORDER BY YEAR_MONTH
    """, params=list(params) + list(ncs_codes))


@st.cache_data(ttl=CACHE_TTL_MARKET, show_spinner=False)
def load_monthly_recruit(where, params):
    """Tab 7: 월별 평균 모집률."""
    if where == "":
        _c = get_market_cache("monthly_recruit")
        if _c is not None:
            return _c
    return _sql_query(f"""
        SELECT YEAR_MONTH,
               AVG(CASE WHEN TOT_FXNUM > 0
                   THEN CAST(REG_COURSE_MAN AS REAL) / TOT_FXNUM * 100 END) as 모집률
        FROM TB_MARKET_TREND {where}
        GROUP BY YEAR_MONTH ORDER BY YEAR_MONTH
    """, params=params)


@st.cache_data(ttl=CACHE_TTL_MARKET, show_spinner=False)
def load_scatter_sample(where, params, limit=3000):
    """Tab 5, 8: 산점도용 샘플 데이터."""
    # SQLite와 PG 모두 지원하는 랜덤 샘플링
    random_fn = "RANDOM()" if is_pg() else "RANDOM()"
    return _sql_query(f"""
        SELECT TRPR_NM, TRAINST_NM, TOT_TRCO, EI_EMPL_RATE_3, TOT_FXNUM, TRAIN_TARGET
        FROM TB_MARKET_TREND {where}
          {"AND" if where else "WHERE"} TOT_TRCO > 0 AND EI_EMPL_RATE_3 > 0
        ORDER BY {random_fn} LIMIT ?
    """, params=list(params) + [limit])


@st.cache_data(ttl=CACHE_TTL_MARKET, show_spinner=False)
def load_grade_stats(where, params):
    """Tab 5: 등급별 평균 성과."""
    return _sql_query(f"""
        SELECT GRADE, AVG(EI_EMPL_RATE_3) as EI_EMPL_RATE_3, AVG(STDG_SCOR) as STDG_SCOR
        FROM TB_MARKET_TREND {where}
          {"AND" if where else "WHERE"} GRADE IS NOT NULL
        GROUP BY GRADE
    """, params=params)


@st.cache_data(ttl=CACHE_TTL_MARKET, show_spinner=False)
def load_summary_kpi(where, params):
    """Tab 1: 전체 요약 KPI (과정수, 평균취업률, 모집률, 훈련비)."""
    return _sql_query(f"""
        SELECT
            COUNT(*) as 총과정수,
            ROUND(CAST(AVG(CASE WHEN EI_EMPL_RATE_3 > 0 THEN EI_EMPL_RATE_3 END) AS NUMERIC), 1) as 평균취업률,
            ROUND(CAST(AVG(CASE WHEN TOT_FXNUM > 0 AND REG_COURSE_MAN > 0
                THEN CAST(REG_COURSE_MAN AS FLOAT) / TOT_FXNUM * 100 END) AS NUMERIC), 1) as 평균모집률,
            ROUND(CAST(AVG(CASE WHEN TOT_TRCO > 0 THEN TOT_TRCO END) AS NUMERIC), 0) as 평균훈련비
        FROM TB_MARKET_TREND {where}
    """, params=params)


@st.cache_data(ttl=CACHE_TTL_MARKET, show_spinner=False)
def load_type_performance(where, params):
    """Tab 4: 훈련 유형별 평균 취업률·모집률."""
    return _sql_query(f"""
        SELECT TRAIN_TARGET as 유형, COUNT(*) as 과정수,
            ROUND(CAST(AVG(CASE WHEN EI_EMPL_RATE_3 > 0 THEN EI_EMPL_RATE_3 END) AS NUMERIC), 1) as 평균취업률,
            ROUND(CAST(AVG(CASE WHEN TOT_FXNUM > 0 AND REG_COURSE_MAN > 0
                THEN CAST(REG_COURSE_MAN AS FLOAT) / TOT_FXNUM * 100 END) AS NUMERIC), 1) as 평균모집률
        FROM TB_MARKET_TREND
        {"WHERE" if not where else where.replace("WHERE","WHERE") + " AND"} TRAIN_TARGET IS NOT NULL AND TRAIN_TARGET != ''
        GROUP BY TRAIN_TARGET ORDER BY 과정수 DESC
    """, params=params)


@st.cache_data(ttl=CACHE_TTL_MARKET, show_spinner=False)
def load_type_counts(where, params):
    """Tab 4: 훈련유형별/주말구분별 건수."""
    return _sql_query(f"""
        SELECT TRAIN_TARGET, WKEND_SE, COUNT(*) as CNT
        FROM TB_MARKET_TREND {where}
        GROUP BY TRAIN_TARGET, WKEND_SE
    """, params=params)


@st.cache_data(ttl=CACHE_TTL_MARKET, show_spinner=False)
def load_keyword_names(where, params):
    """키워드 탭: 과정명 + 취업률 + 연월 로드."""
    return _sql_query(f"""
        SELECT TRPR_NM, EI_EMPL_RATE_3, YEAR_MONTH, TR_STA_DT FROM TB_MARKET_TREND {where}
        ORDER BY RANDOM() LIMIT 8000
    """, params=params)


@st.cache_data(ttl=CACHE_TTL_MARKET, show_spinner=False)
def load_region_opp(where, params):
    """사업기회 탭: 지역별 수요(모집률)·공급(과정수)·성과(취업률)"""
    if where == "":
        _c = get_market_cache("region_opp")
        if _c is not None:
            return _c
    return _sql_query(f"""
        SELECT REGION,
               COUNT(*) as 과정수,
               SUM(COALESCE(TOT_FXNUM, 0)) as 총신청인원,
               AVG(CASE WHEN TOT_FXNUM > 0
                   THEN CAST(REG_COURSE_MAN AS REAL) / TOT_FXNUM * 100 END) as 평균모집률,
               AVG(CASE WHEN EI_EMPL_RATE_3 > 0 THEN EI_EMPL_RATE_3 END) as 평균취업률
        FROM TB_MARKET_TREND {where}
        GROUP BY REGION
        HAVING COUNT(*) >= 5
    """, params=params)



@st.cache_data(ttl=CACHE_TTL_MARKET, show_spinner=False)
def load_ncs_opp_matrix(where, params):
    """사업기회 탭: NCS별 취업률·모집률·경쟁도 (기회 매트릭스용)"""
    if where == "":
        _c = get_market_cache("ncs_opp_matrix")
        if _c is not None:
            return _c
    return _sql_query(f"""
        SELECT NCS_CD,
               COUNT(*) as 경쟁과정수,
               AVG(CASE WHEN EI_EMPL_RATE_3 > 0 THEN EI_EMPL_RATE_3 END) as 평균취업률,
               AVG(CASE WHEN TOT_FXNUM > 0
                   THEN CAST(REG_COURSE_MAN AS REAL) / TOT_FXNUM * 100 END) as 평균모집률
        FROM TB_MARKET_TREND {where}
        GROUP BY NCS_CD
        HAVING COUNT(*) >= 3
    """, params=params)




# ==========================================
# 1. 설정 및 데이터 로드
# ==========================================
st.set_page_config(page_title="시장 분석 & 기회 발굴", page_icon="📈", layout="wide")

check_password()

# ✅ 컬럼명 한글 매핑
COLUMN_MAP = {
    'TRPR_ID': '과정ID', 'TRPR_DEGR': '회차', 'INST_INO': '기관ID',
    'TRPR_NM': '과정명', 'TRAINST_NM': '훈련기관명',
    'TR_STA_DT': '개설일', 'TR_END_DT': '종료일',
    'NCS_CD': 'NCS코드', 'TRNG_AREA_CD': '지역코드',
    'TOT_FXNUM': '정원(명)', 'TOT_TRCO': '훈련비(원)',
    'COURSE_MAN': '수강비(원)', 'REAL_MAN': '실비(원)',
    'REG_COURSE_MAN': '등록인원',
    'EI_EMPL_RATE_3': '취업률(3개월)', 'EI_EMPL_RATE_6': '취업률(6개월)',
    'EI_EMPL_CNT_3': '취업인원(3개월)',
    'STDG_SCOR': '만족도(점)', 'GRADE': '등급',
    'CERTIFICATE': '관련자격증', 'CONTENTS': '콘텐츠',
    'ADDRESS': '주소', 'TEL_NO': '전화번호',
    'TRAIN_TARGET': '훈련유형', 'WKEND_SE': '주말구분',
    'REGION': '지역', 'YEAR_MONTH': '개설연월',
    '모집률': '모집률(%)'
}

WK_MAP = {'1': '주중', '2': '주말', '3': '주중+주말'}

@st.cache_data(ttl=CACHE_TTL_MARKET)
def load_internal_courses():
    """TB_COURSE_MASTER에서 우리 과정 데이터 로드 (HANWHA_COURSE_ID 기반)"""
    course_id = os.getenv("HANWHA_COURSE_ID")
    if not course_id:
        try:
            course_id = st.secrets["HANWHA_COURSE_ID"]
        except (KeyError, FileNotFoundError):
            pass
    if not course_id:
        return None, None

    internal = _load_data("""
        SELECT TRPR_ID, TRPR_DEGR, TRPR_NM, TR_STA_DT, TR_END_DT,
               TOT_TRCO, TOT_FXNUM, TOT_PAR_MKS, TOT_TRP_CNT,
               FINI_CNT, EI_EMPL_RATE_3, EI_EMPL_CNT_3,
               EI_EMPL_RATE_6, EI_EMPL_CNT_6, HRD_EMPL_RATE_6
        FROM TB_COURSE_MASTER
    """)
    if internal.empty:
        return None, course_id

    internal['TR_STA_DT'] = pd.to_datetime(internal['TR_STA_DT'], errors='coerce')
    internal['TR_END_DT'] = pd.to_datetime(internal['TR_END_DT'], errors='coerce')
    for c in ['TOT_TRCO', 'TOT_FXNUM', 'TOT_PAR_MKS', 'TOT_TRP_CNT', 'FINI_CNT']:
        internal[c] = pd.to_numeric(internal[c], errors='coerce').fillna(0)
    for c in ['EI_EMPL_RATE_3', 'EI_EMPL_RATE_6']:
        internal[f'{c}_LABEL'] = internal[c].astype(str).str.strip().map(
            lambda v: EMPL_CODE_MAP.get(v, '')
        )
        internal[c] = pd.to_numeric(internal[c], errors='coerce')
    # HRD 6개월 (고용보험 미가입) 합산 → home.py와 동일 방식
    internal['HRD_EMPL_RATE_6'] = pd.to_numeric(internal['HRD_EMPL_RATE_6'], errors='coerce')
    internal['TOTAL_RATE_6'] = internal['EI_EMPL_RATE_6'].fillna(0) + internal['HRD_EMPL_RATE_6'].fillna(0)
    _no6 = internal['EI_EMPL_RATE_6'].isna() & internal['HRD_EMPL_RATE_6'].isna()
    internal.loc[_no6, 'TOTAL_RATE_6'] = pd.NA
    internal['모집률'] = (internal['TOT_TRP_CNT'] / internal['TOT_FXNUM'].replace(0, pd.NA) * 100).fillna(0)
    internal['FINI_CNT'] = pd.to_numeric(internal['FINI_CNT'], errors='coerce').fillna(0)
    internal['수료율'] = (internal['FINI_CNT'] / internal['TOT_PAR_MKS'].replace(0, pd.NA) * 100).fillna(0)
    return internal, course_id

# 필터 옵션 로드 (경량)
with st.spinner('필터 옵션을 로드 중입니다...'):
    filter_opts = load_filter_options()
    internal_df, course_id = load_internal_courses()

# ==========================================
# 2. 사이드바 (필터링) — DB 경량 쿼리 기반
# ==========================================
st.sidebar.header("🔍 상세 분석 필터")

from datetime import datetime

# 날짜 범위 옵션 (form 밖에서 계산)
all_dates = filter_opts['MIN_DT'].dropna().tolist() + filter_opts['MAX_DT'].dropna().tolist()
if all_dates:
    min_date = pd.to_datetime(min(all_dates)).date()
    max_date = pd.to_datetime(max(all_dates)).date()
else:
    min_date = max_date = datetime.now().date()

region_opts = ['전체'] + sorted(filter_opts['REGION'].dropna().unique().tolist())
type_opts = sorted(filter_opts['TRAIN_TARGET'].dropna().unique().tolist())
_kdt = "K-디지털 트레이닝"
kdt_default = [_kdt] if _kdt in type_opts else []
wk_codes = filter_opts['WKEND_SE'].dropna().unique().tolist()
wk_opts = sorted(set(WK_MAP.get(str(c), '기타') for c in wk_codes))
grade_opts = sorted([g for g in filter_opts['GRADE'].dropna().unique().tolist() if g and str(g).strip()])
ncs_opts = ['전체'] + sorted(filter_opts['NCS_CD'].dropna().unique().tolist())

# 필터 위젯을 form으로 묶음 → 조회 버튼 클릭 시에만 쿼리 실행
with st.sidebar.form("market_filter_form"):
    date_range = st.date_input(
        "조회 기간",
        value=[min_date, max_date],
        min_value=min_date,
        max_value=max_date
    )
    sel_region = st.selectbox("📍 지역", region_opts)
    sel_types = st.multiselect("🎓 훈련 유형 (다중선택)", type_opts, default=kdt_default)
    sel_wkend = st.multiselect("📅 주말/주중 (다중선택)", wk_opts, default=[])
    if grade_opts:
        sel_grade = st.multiselect("🏅 기관 등급 (다중선택)", grade_opts, default=[])
    else:
        sel_grade = []
    sel_ncs = st.selectbox("NCS 코드", ncs_opts)
    search_kwd = st.text_input("🔍 과정명 검색")
    submitted = st.form_submit_button("🔍 조회", type="primary", use_container_width=True)

if len(date_range) == 2:
    start_date, end_date = date_range
else:
    start_date, end_date = date_range[0], date_range[0]

# WHERE 절 생성: 조회 클릭 시 또는 첫 방문(KDT 기본값 자동 적용) 시에만 갱신
if submitted or 'mkt_where' not in st.session_state:
    where, params = build_where_clause(
        start_date, end_date, sel_region, sel_ncs,
        sel_types, sel_wkend, sel_grade, search_kwd
    )
    st.session_state['mkt_where'] = where
    st.session_state['mkt_params'] = list(params)

where = st.session_state['mkt_where']
params = st.session_state['mkt_params']

# ==========================================
# 3. 메인 대시보드
# ==========================================
with st.status("📊 시장 데이터 분석 중...", expanded=True) as _ld:
    _ld.write("🔢 기본 지표 집계 중 (30만건+ 대상)...")
    kpi_df = load_kpi_data(where, params)
    total_count = int(kpi_df['CNT'].iloc[0]) if not kpi_df.empty else 0
    _ld.update(label=f"✅ {total_count:,}건 분석 완료", state="complete", expanded=False)

st.title(f"📈 IT 훈련 시장 상세 분석 ({total_count:,}건)")
st.markdown("---")

# 3.1 KPI Row
c1, c2, c3, c4, c5, c6 = st.columns(6)
c1.metric("검색된 과정 수", f"{total_count:,}개")

if not kpi_df.empty:
    mean_trco = kpi_df['AVG_TRCO'].iloc[0]
    mean_trco = 0 if pd.isna(mean_trco) else mean_trco
    c2.metric("평균 훈련비", f"{int(mean_trco):,}원")

    mean_fxnum = kpi_df['AVG_FXNUM'].iloc[0]
    mean_fxnum = 0 if pd.isna(mean_fxnum) else mean_fxnum
    c3.metric("평균 정원", f"{int(mean_fxnum)}명")

    avg_recruit = kpi_df['AVG_RECRUIT'].iloc[0]
    c4.metric("평균 모집률", f"{avg_recruit:.1f}%" if pd.notna(avg_recruit) else "-")

    avg_score = kpi_df['AVG_SCORE'].iloc[0]
    if pd.notna(avg_score) and avg_score > 0:
        c5.metric("가중 평균 만족도 (100점 환산)", f"{avg_score/100:.1f}점", delta=f"원본: {int(avg_score):,} / 10,000", delta_color="off")
    else:
        c5.metric("가중 평균 만족도", "데이터 없음")

    # 과정 평균 만족도
    score_df = load_course_avg_score(where, params)
    if not score_df.empty and pd.notna(score_df['SIMPLE_SCORE'].iloc[0]):
        simple_score = score_df['SIMPLE_SCORE'].iloc[0]
        c6.metric("과정 평균 만족도 (100점 환산)", f"{simple_score/100:.1f}점", help="개별 과정들의 만족도 단순 평균")
    else:
        c6.metric("과정 평균 만족도", "데이터 없음")

st.markdown("###")

# 취업률 데이터 가용 여부 사전 체크 (필터 적용 후)
_kpi_pre = load_summary_kpi(where, params)
no_empl_data = _kpi_pre.empty or pd.isna(_kpi_pre.iloc[0].get('평균취업률'))

# 공통 데이터 사전 로드 (복수 탭 공유)
type_perf_data = load_type_performance(where, params)

names_df_shared = load_keyword_names(where, params)
if not names_df_shared.empty:
    _text_shared = " ".join(names_df_shared['TRPR_NM'].dropna().astype(str))
    _stops = ['과정', '양성', '취업', '실무', '및', '위한', '기반', '활용', '개발자', 'A', 'B', '수료', '반', '취득', '능력', '향상', '전문가', '심화', '기초', '교육', '훈련', '산업', '구직자']
    _words_shared = [w for w in _text_shared.split() if len(w) > 1 and w not in _stops]
    _freq_shared = Counter(_words_shared)
    kwd_shared = pd.DataFrame(_freq_shared.most_common(25), columns=['키워드', '빈도'])
    top_words_shared = [w for w, _ in _freq_shared.most_common(25)]
else:
    kwd_shared = pd.DataFrame()
    top_words_shared = []

# 연도별 키워드 트렌드 계산
kwd_year_df = pd.DataFrame()
if not names_df_shared.empty and top_words_shared:
    # YEAR_MONTH 우선, 없으면 TR_STA_DT에서 연도 추출 (TR_STA_DT는 WHERE 절 기준으로 항상 존재)
    _yr_col = None
    for _col in ('YEAR_MONTH', 'TR_STA_DT'):
        if _col in names_df_shared.columns:
            _cand = names_df_shared[_col].dropna().str[:4].replace('', pd.NA).dropna()
            if not _cand.empty:
                _yr_col = _col
                break
    if _yr_col:
        names_df_shared['_year'] = names_df_shared[_yr_col].dropna().str[:4]
        _years = sorted(names_df_shared['_year'].dropna().unique())
        _top_kws = [w for w, _ in _freq_shared.most_common(15)]
        _year_rows = []
        for _yr in _years:
            _yr_names = names_df_shared[names_df_shared['_year'] == _yr]['TRPR_NM']
            _yr_words = [w for w in ' '.join(_yr_names.dropna().astype(str)).split()
                         if len(w) > 1 and w not in _stops]
            _yr_total = max(len(_yr_words), 1)
            _yr_cnt = Counter(_yr_words)
            for _kw in _top_kws:
                _year_rows.append({
                    '연도': _yr,
                    '키워드': _kw,
                    '빈도': _yr_cnt.get(_kw, 0),
                    '비율(천건당)': round(_yr_cnt.get(_kw, 0) / _yr_total * 100, 2),
                })
        kwd_year_df = pd.DataFrame(_year_rows)
# 자격증 데이터: K-디지털 트레이닝 등 주요 사업유형에 해당 없음 → 제거

# 3.2 탭 구성
tabs = st.tabs([
    "📊 시장 개요 & 사업 기회", "🏆 순위 & 유형",
    "☁️ 키워드 분석"
])

# ─────────────────────────────────────────
# [Tab 0] 📊 시장 개요 & 사업 기회
# ─────────────────────────────────────────
with tabs[0]:
    # ── 월별 개설 추이 + 평균 모집률 ──
    st.subheader("신규 과정 개설 추이")
    monthly_count = load_monthly_counts(where, params)
    recruit_trend = load_monthly_recruit(where, params)
    _this_month = datetime.now().strftime('%Y-%m')
    col_trend1, col_trend2 = st.columns(2)
    with col_trend1:
        if not monthly_count.empty:
            monthly_count = monthly_count.rename(columns={'COUNT': '개설수'}).sort_values('YEAR_MONTH')
            monthly_count = monthly_count[monthly_count['YEAR_MONTH'] < _this_month]
            fig_cnt = px.bar(monthly_count, x='YEAR_MONTH', y='개설수', text_auto=True, title='월별 신규 과정 개설 수')
            _ym = monthly_count['YEAR_MONTH'].tolist()
            _tv = [m for m in _ym if str(m).endswith('-01')]
            fig_cnt.update_xaxes(type='category', tickvals=_tv, ticktext=[m[:4] for m in _tv])
            _vert_ytitle(fig_cnt, '개설수')
            st.plotly_chart(fig_cnt, use_container_width=True)
    with col_trend2:
        if not recruit_trend.empty:
            recruit_trend = recruit_trend.sort_values('YEAR_MONTH')
            recruit_trend = recruit_trend[recruit_trend['YEAR_MONTH'] < _this_month]
            fig_rec = px.line(recruit_trend, x='YEAR_MONTH', y='모집률', markers=True, title='월별 평균 모집률 추이')
            _ym2 = recruit_trend['YEAR_MONTH'].tolist()
            _tv2 = [m for m in _ym2 if str(m).endswith('-01')]
            fig_rec.update_xaxes(type='category', tickvals=_tv2, ticktext=[m[:4] for m in _tv2])
            _vert_ytitle(fig_rec, '평균 모집률 (%)')
            st.plotly_chart(fig_rec, use_container_width=True)
    st.divider()

    # ── 지역별: 바차트 + Top5 시계열 ──
    st.subheader("지역별 개설 현황")
    reg_cnt = load_region_counts(where, params)
    col_r1, col_r2 = st.columns([2, 3])
    with col_r1:
        if not reg_cnt.empty:
            fig_reg_bar = px.bar(
                reg_cnt.head(15).sort_values('개수'),
                x='개수', y='지역', orientation='h',
                color_discrete_sequence=['#5dade2'],
                title='지역별 개설 수',
            )
            fig_reg_bar.update_layout(height=380, margin=dict(t=40, b=30), xaxis_title='개설 수', yaxis_title='')
            st.plotly_chart(fig_reg_bar, use_container_width=True)
    with col_r2:
        if not reg_cnt.empty:
            top5_regions = reg_cnt.head(5)['지역'].tolist()
            region_trend = load_monthly_region_trend(where, params, top5_regions)
            if not region_trend.empty:
                region_trend['YEAR_MONTH'] = pd.to_datetime(region_trend['YEAR_MONTH'], format='%Y-%m', errors='coerce')
                region_trend = region_trend.dropna(subset=['YEAR_MONTH'])
                all_months = pd.date_range(region_trend['YEAR_MONTH'].min(), region_trend['YEAR_MONTH'].max(), freq='MS')
                regions_list = region_trend['REGION'].unique()
                idx_full = pd.MultiIndex.from_product([all_months, regions_list], names=['YEAR_MONTH', 'REGION'])
                region_trend = region_trend.set_index(['YEAR_MONTH', 'REGION']).reindex(idx_full, fill_value=0).reset_index()
                fig_reg = px.line(
                    region_trend, x='YEAR_MONTH', y='개설수', color='REGION',
                    markers=True, title='상위 5개 지역 월별 개설 추이',
                )
                fig_reg.update_traces(mode='lines+markers', marker=dict(size=5))
                fig_reg.update_layout(hovermode='x unified', height=380)
                fig_reg.update_xaxes(dtick='M12', tickformat='%Y')
                _vert_ytitle(fig_reg, '개설수')
                st.plotly_chart(fig_reg, use_container_width=True)
    st.divider()

    # ── 기관 경쟁력 매트릭스 ──
    st.subheader("🏢 훈련기관 경쟁력 매트릭스")
    st.caption("버블 크기: 개설 과정 수 | X축: 평균 모집률 | Y축: 평균 만족도. 우측 상단이 고만족·고수요 기관입니다.")
    inst_all = load_inst_stats(where, params)
    if not inst_all.empty:
        inst_all['평균모집률'] = (inst_all['REG_COURSE_MAN'] / inst_all['TOT_FXNUM'].replace(0, pd.NA) * 100).fillna(0).clip(upper=100)
        inst_all['평균만족도'] = (pd.to_numeric(inst_all['AVG_SCORE'], errors='coerce').fillna(0) / 100).round(1)
        inst_alt = inst_all[inst_all['평균만족도'] > 0].rename(columns={'TRAINST_NM': '기관명', 'TRPR_CNT': '개설수'})
        inst_alt = inst_alt.nlargest(50, '개설수')
        if not inst_alt.empty:
            fig_alt = px.scatter(
                inst_alt, x='평균모집률', y='평균만족도', size='개설수',
                hover_name='기관명', size_max=40,
                color='평균만족도', color_continuous_scale='RdYlGn',
                labels={'평균모집률': '평균 모집률 (%)', '평균만족도': '평균 만족도 (100점)'},
            )
            med_r = inst_alt['평균모집률'].median()
            med_s = inst_alt['평균만족도'].median()
            fig_alt.add_hline(y=med_s, line_dash='dash', line_color='gray', line_width=1)
            fig_alt.add_vline(x=med_r, line_dash='dash', line_color='gray', line_width=1)
            fig_alt.update_layout(height=480, coloraxis_showscale=False)
            _vert_ytitle(fig_alt, '평균 만족도 (100점)')
            st.plotly_chart(fig_alt, use_container_width=True)
            st.caption("📌 기준선: 중앙값 기준 — 우상단(고모집·고만족도), 좌하단(저모집·저만족도)")
            with st.expander("📄 기관 상세 데이터 보기"):
                show_inst = inst_alt[['기관명', '개설수', '평균모집률', '평균만족도']].sort_values('평균만족도', ascending=False)
                st.dataframe(show_inst, hide_index=True, use_container_width=True,
                    column_config={
                        '개설수': st.column_config.NumberColumn(format="%d개"),
                        '평균모집률': st.column_config.NumberColumn(format="%.1f%%"),
                        '평균만족도': st.column_config.NumberColumn(format="%.1f점"),
                    })
        else:
            st.info("기관 분석 데이터가 없습니다.")
    else:
        st.info("기관 분석 데이터가 없습니다.")

    st.divider()

    # ── 지역별 수요-공급 갭 ──
    st.subheader("📍 지역별 수요-공급 갭")
    st.caption("모집률 높고 과정수 적은 지역(🔵) = 공급 부족 → 신규 진입 기회")
    with st.spinner("📍 지역별 분석 중..."):
        region_opp = load_region_opp(where, params)
    if not region_opp.empty:
        for col in ['과정수', '총신청인원', '평균모집률']:
            region_opp[col] = pd.to_numeric(region_opp[col], errors='coerce').fillna(0)
        avg_c = region_opp['과정수'].mean()
        avg_r = region_opp['평균모집률'].mean()
        med_c = region_opp['과정수'].median()
        med_r = region_opp['평균모집률'].median()
        st.markdown(f"📊 모집률 — 평균: **{avg_r:.1f}%** | 중앙값: **{med_r:.1f}%**")
        st.markdown(f"🏫 과정수 — 평균: **{avg_c:.0f}개** | 중앙값: **{med_c:.0f}개**")
        st.markdown("기준선: 🔴 빨간 파선 = 평균 / 🟡 노란 점선 = 중앙값")
        # 모집률 내림차순 정렬 → 두 차트 공통 y축 순서
        region_opp = region_opp.sort_values('평균모집률', ascending=True)
        region_opp['기회'] = (region_opp['과정수'] < avg_c) & (region_opp['평균모집률'] > avg_r)
        colors = ['#2980b9' if v else '#d5d8dc' for v in region_opp['기회']]
        chart_h = max(300, len(region_opp) * 28)

        col_l, col_r = st.columns(2)
        with col_l:
            fig_recruit = go.Figure(go.Bar(
                x=region_opp['평균모집률'],
                y=region_opp['REGION'],
                orientation='h',
                marker_color=colors,
                text=region_opp['평균모집률'].round(1).astype(str) + '%',
                textposition='outside',
            ))
            fig_recruit.add_vline(x=avg_r, line_dash="dash", line_color="#e74c3c", opacity=0.8,
                                  annotation_text=f"평균 {avg_r:.1f}%", annotation_position="top right")
            fig_recruit.add_vline(x=med_r, line_dash="dot", line_color="#f1c40f", opacity=0.9,
                                  annotation_text=f"중앙값 {med_r:.1f}%", annotation_position="bottom right")
            fig_recruit.update_layout(
                title='모집률 (%) — 높을수록 수요 ↑',
                xaxis_title='모집률 (%)',
                height=chart_h,
                margin=dict(r=10),
            )
            st.plotly_chart(fig_recruit, use_container_width=True)

        with col_r:
            supply_colors = ['#2980b9' if v else '#d5d8dc' for v in region_opp['기회']]
            fig_supply = go.Figure(go.Bar(
                x=region_opp['과정수'],
                y=region_opp['REGION'],
                orientation='h',
                marker_color=supply_colors,
                text=region_opp['과정수'].astype(int).astype(str) + '개',
                textposition='outside',
            ))
            fig_supply.add_vline(x=avg_c, line_dash="dash", line_color="#e74c3c", opacity=0.8,
                                 annotation_text=f"평균 {avg_c:.0f}개", annotation_position="top right")
            fig_supply.add_vline(x=med_c, line_dash="dot", line_color="#f1c40f", opacity=0.9,
                                 annotation_text=f"중앙값 {med_c:.0f}개", annotation_position="bottom right")
            fig_supply.update_layout(
                title='과정수 (개) — 적을수록 공급 ↓',
                xaxis_title='과정수 (개)',
                height=chart_h,
                margin=dict(r=10),
            )
            st.plotly_chart(fig_supply, use_container_width=True)

        opp_regions = region_opp[region_opp['기회']].sort_values('평균모집률', ascending=False)
        if not opp_regions.empty:
            st.success("🎯 **진입 기회 지역**: " + ", ".join(opp_regions['REGION'].tolist()))
    else:
        st.info("지역 데이터가 부족합니다.")

    st.divider()

# ─────────────────────────────────────────
# [Tab 1] 🏆 순위 & 유형  (구 순위 & 모집 + 유형 & 일정 통합)
# ─────────────────────────────────────────
with tabs[1]:
    st.subheader("🔎 내 기관/과정의 시장 위치 찾기")

    raw_inst = load_inst_stats(where, params)
    if not raw_inst.empty:
        raw_inst['평균모집률'] = (raw_inst['REG_COURSE_MAN'] / raw_inst['TOT_FXNUM'].replace(0, pd.NA) * 100).fillna(0).clip(upper=100)
        raw_inst['만족도(점)'] = (raw_inst['AVG_SCORE'] / 100).round(1)
        raw_inst = raw_inst.sort_values(by='REG_COURSE_MAN', ascending=False).reset_index(drop=True)
        raw_inst['순위'] = raw_inst.index + 1
        inst_stats = raw_inst.rename(columns={
            'TRAINST_NM': '기관명', 'TRPR_CNT': '개설수',
            'TOT_FXNUM': '총모집정원', 'REG_COURSE_MAN': '총신청인원', 'AVG_EMPL': '평균취업률'
        })

    raw_course = load_course_agg(where, params)
    if not raw_course.empty:
        raw_course['통합모집률'] = (raw_course['REG_COURSE_MAN'] / raw_course['TOT_FXNUM'].replace(0, pd.NA) * 100).fillna(0).clip(upper=100)
        raw_course['만족도(점)'] = (raw_course['AVG_SCORE'] / 100).round(1)
        raw_course = raw_course.sort_values(by='REG_COURSE_MAN', ascending=False).reset_index(drop=True)
        raw_course['순위'] = raw_course.index + 1
        course_agg = raw_course.rename(columns={
            'TRPR_NM': '과정명', 'TRAINST_NM': '기관명', 'TRPR_CNT': '개설회차',
            'TOT_FXNUM': '총정원', 'REG_COURSE_MAN': '총신청인원', 'AVG_EMPL': '평균취업률'
        })

    col_rank1, col_rank2 = st.columns(2)
    with col_rank1:
        if not raw_inst.empty:
            render_ranking_table(
                inst_stats,
                display_cols=['순위', '기관명', '총모집정원', '총신청인원', '평균모집률', '만족도(점)'],
                format_dict={"총모집정원": "{:,}명", "총신청인원": "{:,}명", "평균모집률": "{:.1f}%", "만족도(점)": "{:.1f}점"},
                search_col='기관명', search_label="기관명 검색",
                search_placeholder="기관명 입력", top_n=5,
                title="🏫 훈련기관 순위 (총 신청인원 기준)",
                expander_title="📋 전체 훈련기관 순위표 펼치기", st_key="rank_inst",
            )
    with col_rank2:
        if not raw_course.empty:
            render_ranking_table(
                course_agg,
                display_cols=['순위', '과정명', '기관명', '개설회차', '총정원', '총신청인원', '통합모집률', '만족도(점)'],
                format_dict={"총정원": "{:,}명", "총신청인원": "{:,}명", "통합모집률": "{:.1f}%", "개설회차": "{:,}회", "만족도(점)": "{:.1f}점"},
                search_col='과정명', search_label="과정명 검색",
                search_placeholder="과정명 입력 (예: 한화시스템)", top_n=5,
                title="📚 인기 훈련과정 (과정 통합/신청인원 기준)",
                expander_title="📋 전체 과정 순위표 펼치기", st_key="rank_course",
            )
    st.divider()

    # ── 훈련 유형별 개설 수 + 주말/주중 ──
    type_wk = load_type_counts(where, params)
    c1, c2 = st.columns(2)
    with c1:
        st.subheader("🎓 훈련 유형별 개설 수")
        if not type_wk.empty:
            type_cnt = type_wk.groupby('TRAIN_TARGET')['CNT'].sum().reset_index()
            type_cnt.columns = ['유형', '개수']
            type_cnt = type_cnt.sort_values('개수')
            fig_type = px.bar(type_cnt, x='개수', y='유형', orientation='h',
                              color='개수', color_continuous_scale='Blues',
                              labels={'개수': '개설 수', '유형': ''})
            fig_type.update_layout(height=300, margin=dict(t=10, b=30), coloraxis_showscale=False)
            st.plotly_chart(fig_type, use_container_width=True)
    with c2:
        st.subheader("📅 주말 vs 주중 개설 현황")
        if not type_wk.empty:
            wk_cnt = type_wk.groupby('WKEND_SE')['CNT'].sum().reset_index()
            wk_cnt['구분'] = wk_cnt['WKEND_SE'].astype(str).map(WK_MAP).fillna('기타')
            fig_wk = px.bar(wk_cnt, x='구분', y='CNT', color='구분', text='CNT',
                            title="직장인 타겟(주말) 과정 수", labels={'CNT': '개수'})
            _vert_ytitle(fig_wk, '개수')
            st.plotly_chart(fig_wk, use_container_width=True)
    st.divider()

    # ── NCS별 모집 현황 ──
    st.subheader("📊 인기 NCS(기술)별 모집 현황")
    ncs_data = load_ncs_agg(where, params, min_courses=NCS_MIN_COURSES if total_count >= 100 else 1)
    if not ncs_data.empty:
        ncs_data['NCS_CD'] = ncs_data['NCS_CD'].apply(lambda x: str(int(float(x))) if pd.notna(x) and str(x) not in ('', 'nan') else '')
        ncs_data['평균모집률'] = (ncs_data['REG_COURSE_MAN'] / ncs_data['TOT_FXNUM'].replace(0, pd.NA) * 100).fillna(0).clip(upper=100)
        ncs_top = ncs_data.head(10).rename(columns={'NCS_CD': 'NCS코드', 'CNT': '개설수', 'TOT_FXNUM': '총모집정원', 'REG_COURSE_MAN': '총신청인원'})
        ncs_top = ncs_top.sort_values('평균모집률', ascending=True)
        fig = px.bar(ncs_top, x='평균모집률', y='NCS코드', orientation='h',
                     color='평균모집률', color_continuous_scale='Blues',
                     text='평균모집률', labels={'평균모집률': '평균 모집률 (%)', 'NCS코드': ''})
        fig.update_traces(texttemplate='%{x:.1f}%', textposition='outside')
        fig.update_yaxes(type='category')
        fig.update_layout(height=360, margin=dict(t=10, b=30), coloraxis_showscale=False)
        st.plotly_chart(fig, use_container_width=True)
        with st.expander("📄 상세 데이터 보기"):
            st.dataframe(ncs_top, use_container_width=True, hide_index=True, column_config={
                "총모집정원": st.column_config.NumberColumn(format="%d명"),
                "총신청인원": st.column_config.NumberColumn(format="%d명"),
                "평균모집률": st.column_config.NumberColumn(format="%.1f%%"),
            })

# ─────────────────────────────────────────
# [Tab 2] ☁️ 키워드 분석
# ─────────────────────────────────────────
with tabs[2]:
    # §1: 키워드 빈도 Top 25
    st.subheader("🔥 과정명 트렌드 키워드 Top 25")
    st.caption("훈련과정명에서 자주 등장하는 키워드 빈도입니다.")
    if not kwd_shared.empty:
        fig_kwd = px.bar(kwd_shared.sort_values('빈도'), x='빈도', y='키워드', orientation='h',
                         color='빈도', color_continuous_scale='Blues')
        fig_kwd.update_layout(height=520, coloraxis_showscale=False, margin=dict(t=10, b=20))
        _vert_ytitle(fig_kwd, '키워드', margin_l=160, xshift=-120)
        st.plotly_chart(fig_kwd, use_container_width=True)
    else:
        st.info("키워드 데이터가 없습니다.")

    st.divider()

    # §2: 연도별 트렌드 키워드
    st.subheader("📈 연도별 키워드 트렌드")
    st.caption("Top 15 키워드의 연도별 등장 비율(%)입니다. 선이 올라갈수록 해당 연도에 많이 쓰인 키워드입니다.")
    if not kwd_year_df.empty and len(kwd_year_df['연도'].unique()) >= 2:
        fig_yr = px.line(
            kwd_year_df, x='연도', y='비율(천건당)', color='키워드',
            markers=True,
            labels={'비율(천건당)': '등장 비율 (%)', '연도': '연도'},
        )
        fig_yr.update_layout(height=420, legend=dict(
            orientation='v', x=1.01, y=1, font=dict(size=11)
        ))
        _vert_ytitle(fig_yr, '등장 비율 (%)')
        st.plotly_chart(fig_yr, use_container_width=True)
    else:
        st.info("연도별 트렌드 분석에 필요한 데이터가 부족합니다 (2개 연도 이상 필요).")

    st.divider()

    # §3: 키워드별 빈도 통계 테이블
    st.subheader("📋 키워드별 빈도 통계")
    if not kwd_shared.empty:
        st.dataframe(
            kwd_shared.sort_values('빈도', ascending=False),
            column_config={
                '키워드': '키워드',
                '빈도': st.column_config.NumberColumn('빈도 (샘플 8000건 기준)', format="%d"),
            },
            use_container_width=True,
            hide_index=True,
        )
