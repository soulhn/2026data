import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from collections import Counter
import sys
import os

# 🚀 상위 폴더의 utils.py를 가져오기 위한 경로 설정
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils import check_password, get_connection, is_pg, load_data as _load_data, adapt_query
from config import (
    CACHE_TTL_MARKET, COST_BINS, COST_BIN_LABELS,
    SCATTER_SAMPLE_LIMIT, REGRESSION_SAMPLE_LIMIT,
    NCS_MIN_COURSES, CERT_MIN_COURSES, CERT_EMPL_MIN_COURSES, TOP_CERTS_LIMIT,
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
               AVG(CASE WHEN STDG_SCOR > 0 THEN STDG_SCOR END) as AVG_SCORE
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
               AVG(EI_EMPL_RATE_3) as AVG_EMPL,
               AVG(STDG_SCOR) as AVG_SCORE
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
               AVG(EI_EMPL_RATE_3) as AVG_EMPL,
               AVG(STDG_SCOR) as AVG_SCORE
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
def load_type_counts(where, params):
    """Tab 4: 훈련유형별/주말구분별 건수."""
    return _sql_query(f"""
        SELECT TRAIN_TARGET, WKEND_SE, COUNT(*) as CNT
        FROM TB_MARKET_TREND {where}
        GROUP BY TRAIN_TARGET, WKEND_SE
    """, params=params)


@st.cache_data(ttl=CACHE_TTL_MARKET, show_spinner=False)
def load_keyword_names(where, params):
    """Tab 10: 과정명만 로드."""
    return _sql_query(f"""
        SELECT TRPR_NM FROM TB_MARKET_TREND {where}
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
def load_ncs_growth(where, params):
    """사업기회 탭: NCS별 최근 6개월 vs 이전 6개월 개설 증가율"""
    if where == "":
        _c = get_market_cache("ncs_growth")
        if _c is not None:
            return _c
    max_ym_df = _sql_query(f"""
        SELECT MAX(YEAR_MONTH) as MAX_YM FROM TB_MARKET_TREND {where}
    """, params=params)
    if max_ym_df.empty or pd.isna(max_ym_df['MAX_YM'].iloc[0]):
        return pd.DataFrame()
    try:
        max_dt = pd.to_datetime(str(max_ym_df['MAX_YM'].iloc[0])[:7] + '-01')
        mid_dt = max_dt - pd.DateOffset(months=6)
        start_dt = max_dt - pd.DateOffset(months=12)
        mid_ym = mid_dt.strftime('%Y-%m')
        start_ym = start_dt.strftime('%Y-%m')
        max_ym = max_dt.strftime('%Y-%m')
    except Exception:
        return pd.DataFrame()
    and_or = "AND" if where else "WHERE"
    recent_df = _sql_query(f"""
        SELECT NCS_CD, COUNT(*) as 최근6개월
        FROM TB_MARKET_TREND {where}
          {and_or} YEAR_MONTH > ? AND YEAR_MONTH <= ?
        GROUP BY NCS_CD HAVING COUNT(*) >= 3
    """, params=list(params) + [mid_ym, max_ym])
    prev_df = _sql_query(f"""
        SELECT NCS_CD, COUNT(*) as 이전6개월
        FROM TB_MARKET_TREND {where}
          {and_or} YEAR_MONTH > ? AND YEAR_MONTH <= ?
        GROUP BY NCS_CD HAVING COUNT(*) >= 3
    """, params=list(params) + [start_ym, mid_ym])
    if recent_df.empty:
        return pd.DataFrame()
    merged = recent_df.merge(
        prev_df if not prev_df.empty else pd.DataFrame(columns=['NCS_CD', '이전6개월']),
        on='NCS_CD', how='left'
    ).fillna(0)
    merged['증가율(%)'] = (
        (merged['최근6개월'] - merged['이전6개월']) / merged['이전6개월'].replace(0, 1) * 100
    ).round(1)
    return merged.sort_values('증가율(%)', ascending=False)


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


@st.cache_data(ttl=CACHE_TTL_MARKET, show_spinner=False)
def load_data_preview(where, params, limit=1000):
    """Tab 12: 데이터 조회용."""
    return _sql_query(f"""
        SELECT TRPR_ID, TRPR_DEGR, TRPR_NM, TRAINST_NM,
               TR_STA_DT, TR_END_DT, NCS_CD, TRNG_AREA_CD,
               TOT_FXNUM, TOT_TRCO, COURSE_MAN, REG_COURSE_MAN,
               EI_EMPL_RATE_3, EI_EMPL_RATE_6, EI_EMPL_CNT_3,
               STDG_SCOR, GRADE, ADDRESS,
               TRAIN_TARGET, WKEND_SE,
               YEAR_MONTH, REGION
        FROM TB_MARKET_TREND {where}
        ORDER BY TR_STA_DT DESC LIMIT ?
    """, params=list(params) + [limit])


@st.cache_data(ttl=CACHE_TTL_MARKET, show_spinner=False)
def load_data_full_csv(where, params):
    """Tab 12: CSV 다운로드용 전체 데이터."""
    return _sql_query(f"""
        SELECT TRPR_ID, TRPR_DEGR, TRPR_NM, TRAINST_NM,
               TR_STA_DT, TR_END_DT, NCS_CD, TRNG_AREA_CD,
               TOT_FXNUM, TOT_TRCO, COURSE_MAN, REG_COURSE_MAN,
               EI_EMPL_RATE_3, EI_EMPL_RATE_6, EI_EMPL_CNT_3,
               STDG_SCOR, GRADE, ADDRESS,
               TRAIN_TARGET, WKEND_SE,
               YEAR_MONTH, REGION
        FROM TB_MARKET_TREND {where}
        ORDER BY TR_STA_DT DESC
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
               EI_EMPL_RATE_6, EI_EMPL_CNT_6
        FROM TB_COURSE_MASTER
    """)
    if internal.empty:
        return None, course_id

    internal['TR_STA_DT'] = pd.to_datetime(internal['TR_STA_DT'], errors='coerce')
    internal['TR_END_DT'] = pd.to_datetime(internal['TR_END_DT'], errors='coerce')
    for c in ['TOT_TRCO', 'TOT_FXNUM', 'TOT_PAR_MKS', 'TOT_TRP_CNT', 'FINI_CNT']:
        internal[c] = pd.to_numeric(internal[c], errors='coerce').fillna(0)
    for c in ['EI_EMPL_RATE_3', 'EI_EMPL_RATE_6']:
        internal[c] = pd.to_numeric(internal[c], errors='coerce')
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

    avg_empl = kpi_df['AVG_EMPL'].iloc[0]
    if pd.isna(avg_empl):
        c4.metric("평균 취업률", "-")
    else:
        c4.metric("평균 취업률", f"{avg_empl:.1f}%")

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

# 3.2 탭 구성
tabs = st.tabs([
    "📊 시장 개요", "🏆 순위 & 모집 분석", "💎 우리 과정 vs 시장",
    "🎨 유형/일정 분석", "💰 비용/성과 분석",
    "📈 시계열 트렌드", "⚔️ 경쟁 심화도", "🎯 비용 대비 성과",
    "🏢 경쟁 현황", "☁️ 키워드", "🎓 자격증 분석", "🔭 사업기회 발굴", "📑 데이터 조회"
])

# [Tab 1] 시장 개요
with tabs[0]:
    col1, col2 = st.columns([2, 1])
    with col1:
        st.subheader("월별 개설 추이")
        trend = load_monthly_counts(where, params)
        if not trend.empty:
            st.plotly_chart(px.line(trend, x='YEAR_MONTH', y='COUNT', markers=True), use_container_width=True)
    with col2:
        st.subheader("지역별 점유율")
        reg_cnt = load_region_counts(where, params)
        if not reg_cnt.empty:
            st.plotly_chart(px.pie(reg_cnt, values='개수', names='지역', hole=0.4), use_container_width=True)

# [Tab 2] 🏆 순위 & 모집 분석
with tabs[1]:
    st.subheader("🔎 내 기관/과정의 시장 위치 찾기")

    # 1. 기관별 집계
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

    # 2. 과정별 집계
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

    # 모집률 분석
    st.subheader("📊 시장 전체 모집률 & 커리큘럼 분석")

    row2_1, row2_2 = st.columns(2)

    with row2_1:
        st.markdown("**1️⃣ 훈련 유형(사업)별 모집 현황**")
        type_data = load_type_agg(where, params)
        if not type_data.empty:
            type_data['평균모집률'] = (type_data['REG_COURSE_MAN'] / type_data['TOT_FXNUM'].replace(0, pd.NA) * 100).fillna(0).clip(upper=100)
            type_data = type_data.rename(columns={'TRAIN_TARGET': '훈련유형', 'CNT': '개설수', 'TOT_FXNUM': '총모집정원', 'REG_COURSE_MAN': '총신청인원'})
            type_data = type_data.sort_values('평균모집률', ascending=False)
            fig = px.bar(type_data, x='훈련유형', y='평균모집률', color='평균모집률', text_auto='.1f', title="유형별 모집률(%)")
            st.plotly_chart(fig, use_container_width=True)
            with st.expander("📄 상세 데이터 보기", expanded=True):
                st.dataframe(type_data, use_container_width=True, hide_index=True, column_config={
                    "총모집정원": st.column_config.NumberColumn(format="%d명"),
                    "총신청인원": st.column_config.NumberColumn(format="%d명"),
                    "평균모집률": st.column_config.NumberColumn(format="%.1f%%"),
                })

    with row2_2:
        st.markdown("**2️⃣ 인기 NCS(기술)별 모집 현황**")
        ncs_data = load_ncs_agg(where, params, min_courses=NCS_MIN_COURSES if total_count >= 100 else 1)
        if not ncs_data.empty:
            ncs_data['평균모집률'] = (ncs_data['REG_COURSE_MAN'] / ncs_data['TOT_FXNUM'].replace(0, pd.NA) * 100).fillna(0).clip(upper=100)
            ncs_top = ncs_data.head(10).rename(columns={'NCS_CD': 'NCS코드', 'CNT': '개설수', 'TOT_FXNUM': '총모집정원', 'REG_COURSE_MAN': '총신청인원'})
            ncs_top = ncs_top.sort_values('평균모집률', ascending=False)
            fig = px.bar(ncs_top, x='NCS코드', y='평균모집률', color='평균모집률', text_auto='.1f', title="모집률 Top 10 커리큘럼")
            st.plotly_chart(fig, use_container_width=True)
            with st.expander("📄 상세 데이터 보기", expanded=True):
                st.dataframe(ncs_top, use_container_width=True, hide_index=True, column_config={
                    "총모집정원": st.column_config.NumberColumn(format="%d명"),
                    "총신청인원": st.column_config.NumberColumn(format="%d명"),
                    "평균모집률": st.column_config.NumberColumn(format="%.1f%%"),
                })

# [Tab 3] 💎 우리 과정 vs 시장 비교
with tabs[2]:
    if internal_df is None:
        st.info("HANWHA_COURSE_ID가 설정되지 않았습니다. 환경변수 또는 Streamlit Secrets에 설정하면 우리 과정과 시장을 비교할 수 있습니다.")
    else:
        # 내부 과정의 TRPR_ID로 시장 데이터에서 NCS 코드 매칭
        internal_trpr_ids = internal_df['TRPR_ID'].unique().tolist()
        if internal_trpr_ids:
            placeholders = ','.join('?' * len(internal_trpr_ids))
            matched = _sql_query(f"""
                SELECT DISTINCT NCS_CD FROM TB_MARKET_TREND
                WHERE TRPR_ID IN ({placeholders}) AND NCS_CD IS NOT NULL
            """, params=internal_trpr_ids)
        else:
            matched = pd.DataFrame()

        if matched.empty:
            st.warning("시장 데이터에서 우리 과정을 찾을 수 없습니다. 필터 기간을 확인하세요.")
        else:
            our_ncs_codes = matched['NCS_CD'].unique()

            # KPI: 시장 평균 (SQL에서 이미 가져옴)
            st.subheader("핵심 지표 비교")
            our_empl = internal_df['EI_EMPL_RATE_3'].dropna()
            our_empl_valid = our_empl[our_empl > 0]

            # 시장 평균값은 kpi_df에서 가져옴
            mkt_avg_empl = kpi_df['AVG_EMPL'].iloc[0] if not kpi_df.empty else None
            mkt_avg_trco = kpi_df['AVG_TRCO'].iloc[0] if not kpi_df.empty else None

            our_trco = internal_df['TOT_TRCO']
            our_recruit = internal_df['모집률']

            # 시장 모집률 평균 (SQL)
            mkt_recruit_df = _sql_query(f"""
                SELECT AVG(CASE WHEN TOT_FXNUM > 0
                    THEN CAST(REG_COURSE_MAN AS REAL) / TOT_FXNUM * 100 END) as AVG_RECRUIT
                FROM TB_MARKET_TREND {where}
            """, params=params)
            mkt_avg_recruit = mkt_recruit_df['AVG_RECRUIT'].iloc[0] if not mkt_recruit_df.empty else None

            k1, k2, k3 = st.columns(3)
            if not our_empl_valid.empty and pd.notna(mkt_avg_empl):
                our_avg_empl = our_empl_valid.mean()
                k1.metric("취업률 (3개월)", f"{our_avg_empl:.1f}%", delta=f"{our_avg_empl - mkt_avg_empl:+.1f}%p vs 시장")
            else:
                k1.metric("취업률 (3개월)", "-")

            if not our_trco.empty and pd.notna(mkt_avg_trco):
                our_avg_trco = our_trco.mean()
                k2.metric("평균 훈련비", f"{int(our_avg_trco):,}원", delta=f"{int(our_avg_trco - mkt_avg_trco):,}원 vs 시장", delta_color="inverse")
            else:
                k2.metric("평균 훈련비", "-")

            if not our_recruit.empty and pd.notna(mkt_avg_recruit):
                our_avg_rec = our_recruit.mean()
                k3.metric("평균 모집률", f"{our_avg_rec:.1f}%", delta=f"{our_avg_rec - mkt_avg_recruit:+.1f}%p vs 시장")
            else:
                k3.metric("평균 모집률", "-")

            st.divider()

            # 백분위 분석 — 시장 분포 통계를 SQL로 가져와서 비교
            st.subheader("시장 내 백분위 분석")
            pct_df = _sql_query(f"""
                SELECT
                    COUNT(CASE WHEN EI_EMPL_RATE_3 > 0 THEN 1 END) as EMPL_CNT,
                    COUNT(CASE WHEN TOT_TRCO > 0 THEN 1 END) as TRCO_CNT,
                    COUNT(CASE WHEN TOT_FXNUM > 0 AND REG_COURSE_MAN > 0 THEN 1 END) as RECRUIT_CNT
                FROM TB_MARKET_TREND {where}
            """, params=params)

            # 간단한 백분위: SQL percentile 대신 비율 계산
            p1, p2, p3 = st.columns(3)

            if not our_empl_valid.empty:
                below_cnt = _sql_query(f"""
                    SELECT COUNT(*) as CNT FROM TB_MARKET_TREND {where}
                    {"AND" if where else "WHERE"} EI_EMPL_RATE_3 > 0 AND EI_EMPL_RATE_3 < ?
                """, params=list(params) + [float(our_empl_valid.mean())])
                total_empl = pct_df['EMPL_CNT'].iloc[0]
                if total_empl > 0:
                    pct = below_cnt['CNT'].iloc[0] / total_empl * 100
                    p1.metric("취업률 백분위", f"상위 {100 - pct:.0f}%")
                else:
                    p1.metric("취업률 백분위", "-")
            else:
                p1.metric("취업률 백분위", "-")

            if not our_trco.empty:
                below_trco = _sql_query(f"""
                    SELECT COUNT(*) as CNT FROM TB_MARKET_TREND {where}
                    {"AND" if where else "WHERE"} TOT_TRCO > 0 AND TOT_TRCO < ?
                """, params=list(params) + [float(our_trco.mean())])
                total_trco = pct_df['TRCO_CNT'].iloc[0]
                if total_trco > 0:
                    pct_trco = below_trco['CNT'].iloc[0] / total_trco * 100
                    p2.metric("훈련비 백분위", f"상위 {pct_trco:.0f}% (높은 순)")
                else:
                    p2.metric("훈련비 백분위", "-")
            else:
                p2.metric("훈련비 백분위", "-")

            if not our_recruit.empty:
                below_rec = _sql_query(f"""
                    SELECT COUNT(*) as CNT FROM TB_MARKET_TREND {where}
                    {"AND" if where else "WHERE"} TOT_FXNUM > 0
                    AND CAST(REG_COURSE_MAN AS REAL) / TOT_FXNUM * 100 < ?
                """, params=list(params) + [float(our_recruit.mean())])
                total_rec = pct_df['RECRUIT_CNT'].iloc[0]
                if total_rec > 0:
                    pct_rec = below_rec['CNT'].iloc[0] / total_rec * 100
                    p3.metric("모집률 백분위", f"상위 {100 - pct_rec:.0f}%")
                else:
                    p3.metric("모집률 백분위", "-")
            else:
                p3.metric("모집률 백분위", "-")

            st.divider()

            # 레이더 차트
            st.subheader("종합 역량 레이더")
            radar_metrics = ['취업률', '모집률', '훈련비(역순)', '정원']
            our_vals, mkt_avg_vals, mkt_top10_vals = [], [], []

            # 시장 분포 통계
            dist_df = _sql_query(f"""
                SELECT
                    AVG(CASE WHEN EI_EMPL_RATE_3 > 0 THEN EI_EMPL_RATE_3 END) as M_EMPL,
                    AVG(CASE WHEN TOT_FXNUM > 0 AND REG_COURSE_MAN > 0
                        THEN CAST(REG_COURSE_MAN AS REAL) / TOT_FXNUM * 100 END) as M_RECRUIT,
                    AVG(CASE WHEN TOT_TRCO > 0 THEN TOT_TRCO END) as M_TRCO,
                    AVG(CASE WHEN TOT_FXNUM > 0 THEN TOT_FXNUM END) as M_FXNUM
                FROM TB_MARKET_TREND {where}
            """, params=params)

            m_e = float(dist_df['M_EMPL'].iloc[0] or 0)
            m_r = float(dist_df['M_RECRUIT'].iloc[0] or 0)
            m_trco = float(dist_df['M_TRCO'].iloc[0] or 1)
            m_fx = float(dist_df['M_FXNUM'].iloc[0] or 0)

            o_e = our_empl_valid.mean() if not our_empl_valid.empty else 0
            o_r = our_recruit.mean() if not our_recruit.empty else 0
            o_t_raw = our_trco.mean() if not our_trco.empty else 0
            o_fx = min(internal_df['TOT_FXNUM'].mean(), 100) if not internal_df['TOT_FXNUM'].empty else 0

            # 훈련비 역순 정규화
            max_trco = m_trco * 2 if m_trco > 0 else 1
            o_t = max(0, (1 - o_t_raw / max_trco) * 100)
            m_t = max(0, (1 - m_trco / max_trco) * 100)

            our_vals = [o_e, o_r, o_t, min(o_fx, 100)]
            mkt_avg_vals = [m_e, m_r, m_t, min(m_fx, 100)]
            # Top 10% approximation
            mkt_top10_vals = [m_e * 1.3, m_r * 1.3, m_t * 0.7, min(m_fx * 1.3, 100)]

            fig_radar = go.Figure()
            fig_radar.add_trace(go.Scatterpolar(r=our_vals + [our_vals[0]], theta=radar_metrics + [radar_metrics[0]], fill='toself', name='우리 과정'))
            fig_radar.add_trace(go.Scatterpolar(r=mkt_avg_vals + [mkt_avg_vals[0]], theta=radar_metrics + [radar_metrics[0]], fill='toself', name='시장 평균', opacity=0.5))
            fig_radar.add_trace(go.Scatterpolar(r=mkt_top10_vals + [mkt_top10_vals[0]], theta=radar_metrics + [radar_metrics[0]], fill='toself', name='시장 상위 10%', opacity=0.3))
            fig_radar.update_layout(polar=dict(radialaxis=dict(visible=True)), title="우리 과정 vs 시장 (값이 클수록 우수)")
            st.plotly_chart(fig_radar, use_container_width=True)

            st.divider()

            # 회차별 상세 비교 테이블
            st.subheader("회차별 상세 비교")
            detail = internal_df[['TRPR_DEGR', 'TRPR_NM', 'TR_STA_DT', 'TOT_TRCO', 'TOT_FXNUM', 'TOT_TRP_CNT', 'FINI_CNT', '수료율', 'EI_EMPL_RATE_3']].copy()
            detail.columns = ['회차', '과정명', '시작일', '훈련비', '정원', '수강신청인원', '수료인원', '수료율(%)', '취업률(%)']
            detail['시작일'] = detail['시작일'].dt.strftime('%Y-%m-%d')
            st.dataframe(
                detail.style.format({'훈련비': '{:,.0f}원', '정원': '{:,.0f}명', '수강신청인원': '{:,.0f}명', '수료인원': '{:,.0f}명', '수료율(%)': '{:.1f}%', '취업률(%)': '{:.1f}%'}),
                use_container_width=True, hide_index=True
            )

# [Tab 6] 📈 시계열 트렌드
with tabs[5]:
    st.subheader("월별 평균 취업률 추이")
    monthly_empl = load_monthly_empl(where, params)
    if not monthly_empl.empty:
        monthly_empl = monthly_empl.sort_values('월')
        monthly_empl['3개월 이동평균'] = monthly_empl['평균취업률'].rolling(3, min_periods=1).mean()

        fig_empl = go.Figure()
        fig_empl.add_trace(go.Scatter(x=monthly_empl['월'], y=monthly_empl['평균취업률'], mode='lines+markers', name='월별 평균', opacity=0.6))
        fig_empl.add_trace(go.Scatter(x=monthly_empl['월'], y=monthly_empl['3개월 이동평균'], mode='lines', name='3개월 이동평균', line=dict(width=3)))

        # 우리 과정 개설 시점 마커
        if internal_df is not None:
            for _, row in internal_df.iterrows():
                if pd.notna(row['TR_STA_DT']):
                    ym = row['TR_STA_DT'].strftime('%Y-%m')
                    fig_empl.add_vline(x=ym, line_dash="dash", line_color="red", opacity=0.5)
                    fig_empl.add_annotation(x=ym, y=1, yref="paper", text=f"{int(row['TRPR_DEGR'])}기", showarrow=False, yshift=10, font=dict(color="red", size=10))

        fig_empl.update_layout(xaxis_title='월', yaxis_title='취업률(%)', title='시장 전체 월별 취업률 추이')
        st.plotly_chart(fig_empl, use_container_width=True)
    else:
        st.info("취업률 데이터가 없습니다.")

    st.divider()

    # 월별 훈련비 추이 (중앙값/사분위 → 여전히 Python 필요)
    st.subheader("월별 훈련비 추이")
    trco_raw = load_monthly_trco_stats(where, params)
    if not trco_raw.empty:
        trco_raw['TOT_TRCO'] = pd.to_numeric(trco_raw['TOT_TRCO'], errors='coerce')
        monthly_trco = trco_raw.groupby('YEAR_MONTH')['TOT_TRCO'].agg(['median', lambda x: x.quantile(0.25), lambda x: x.quantile(0.75)]).reset_index()
        monthly_trco.columns = ['월', '중앙값', 'Q1', 'Q3']
        monthly_trco = monthly_trco.sort_values('월')

        fig_trco = go.Figure()
        fig_trco.add_trace(go.Scatter(x=monthly_trco['월'], y=monthly_trco['Q3'], mode='lines', name='75%', line=dict(width=0), showlegend=False))
        fig_trco.add_trace(go.Scatter(x=monthly_trco['월'], y=monthly_trco['Q1'], mode='lines', name='25~75% 범위', fill='tonexty', fillcolor='rgba(68,114,196,0.2)', line=dict(width=0)))
        fig_trco.add_trace(go.Scatter(x=monthly_trco['월'], y=monthly_trco['중앙값'], mode='lines+markers', name='중앙값', line=dict(color='#4472C4', width=3)))
        fig_trco.update_layout(xaxis_title='월', yaxis_title='훈련비(원)', title='월별 훈련비 분포 (중앙값 + 사분위)')
        st.plotly_chart(fig_trco, use_container_width=True)
    else:
        st.info("훈련비 데이터가 없습니다.")

    st.divider()

    # 신규 과정 개설 수 증감률
    st.subheader("신규 과정 개설 추이 및 증감률")
    monthly_count = load_monthly_counts(where, params)
    if not monthly_count.empty:
        monthly_count = monthly_count.rename(columns={'COUNT': '개설수'})
        monthly_count = monthly_count.sort_values('YEAR_MONTH')
        monthly_count['전월대비(%)'] = monthly_count['개설수'].pct_change() * 100

        col_trend1, col_trend2 = st.columns(2)
        with col_trend1:
            fig_cnt = px.bar(monthly_count, x='YEAR_MONTH', y='개설수', text_auto=True, title='월별 신규 과정 개설 수')
            st.plotly_chart(fig_cnt, use_container_width=True)
        with col_trend2:
            fig_chg = px.bar(monthly_count.dropna(subset=['전월대비(%)']), x='YEAR_MONTH', y='전월대비(%)',
                             color='전월대비(%)', color_continuous_scale='RdYlGn', text_auto='.1f', title='전월 대비 증감률(%)')
            st.plotly_chart(fig_chg, use_container_width=True)

    st.divider()

    # 지역별 Top 5 개설 추이
    st.subheader("지역별 Top 5 과정 개설 추이")
    top5_reg = load_region_counts(where, params)
    if not top5_reg.empty:
        top5_regions = top5_reg.head(5)['지역'].tolist()
        region_trend = load_monthly_region_trend(where, params, top5_regions)
        if not region_trend.empty:
            fig_reg = px.line(region_trend, x='YEAR_MONTH', y='개설수', color='REGION', markers=True, title='상위 5개 지역 월별 개설 추이')
            st.plotly_chart(fig_reg, use_container_width=True)

# [Tab 7] ⚔️ 경쟁 심화도
with tabs[6]:
    if internal_df is None:
        st.info("HANWHA_COURSE_ID가 설정되지 않았습니다. 시장 전체 경쟁 분석만 표시합니다.")
        our_ncs_codes_comp = []
    else:
        internal_trpr_ids_comp = internal_df['TRPR_ID'].unique().tolist()
        if internal_trpr_ids_comp:
            placeholders = ','.join('?' * len(internal_trpr_ids_comp))
            matched_comp = _sql_query(f"""
                SELECT DISTINCT NCS_CD FROM TB_MARKET_TREND
                WHERE TRPR_ID IN ({placeholders}) AND NCS_CD IS NOT NULL
            """, params=internal_trpr_ids_comp)
            our_ncs_codes_comp = matched_comp['NCS_CD'].dropna().unique().tolist() if not matched_comp.empty else []
        else:
            our_ncs_codes_comp = []

    # 우리 NCS 코드 경쟁 과정 수 시계열
    if our_ncs_codes_comp:
        st.subheader("우리 NCS 분야 경쟁 과정 수 추이")
        comp_monthly = load_competition_monthly(where, params, our_ncs_codes_comp)
        if not comp_monthly.empty:
            comp_monthly = comp_monthly.sort_values('YEAR_MONTH')
            fig_comp = px.line(comp_monthly, x='YEAR_MONTH', y='경쟁과정수', color='NCS_CD', markers=True, title='우리 NCS 분야 월별 경쟁 과정 수')
            st.plotly_chart(fig_comp, use_container_width=True)
        st.divider()

    # 공급-수요 매트릭스
    st.subheader("NCS별 공급-수요 매트릭스")
    st.caption("과정수(공급) vs 모집률(수요) - 우측 하단은 과잉공급 위험 영역")
    ncs_supply = load_ncs_agg(where, params, min_courses=3)
    if not ncs_supply.empty:
        ncs_supply = ncs_supply.rename(columns={'CNT': '과정수', 'AVG_RECRUIT': '평균모집률'})
        ncs_supply['is_ours'] = ncs_supply['NCS_CD'].isin(our_ncs_codes_comp) if our_ncs_codes_comp else False
        fig_matrix = px.scatter(
            ncs_supply, x='과정수', y='평균모집률', text='NCS_CD',
            color='is_ours', color_discrete_map={True: 'red', False: 'steelblue'},
            size='과정수', opacity=0.7,
            labels={'is_ours': '우리 분야'},
            title='NCS별 공급(과정수) vs 수요(모집률)'
        )
        avg_recruit = ncs_supply['평균모집률'].mean()
        avg_count = ncs_supply['과정수'].mean()
        fig_matrix.add_hline(y=avg_recruit, line_dash="dash", line_color="gray", opacity=0.5, annotation_text="평균 모집률")
        fig_matrix.add_vline(x=avg_count, line_dash="dash", line_color="gray", opacity=0.5, annotation_text="평균 과정수")
        fig_matrix.update_traces(textposition='top center')
        fig_matrix.update_layout(showlegend=True)
        st.plotly_chart(fig_matrix, use_container_width=True)

        # 과잉공급 경고
        oversupply = ncs_supply[(ncs_supply['과정수'] > avg_count) & (ncs_supply['평균모집률'] < avg_recruit)]
        if not oversupply.empty:
            st.warning(f"과잉공급 위험 NCS ({len(oversupply)}개): {', '.join(oversupply['NCS_CD'].tolist())}")
    else:
        st.info("분석할 NCS 데이터가 부족합니다.")

    st.divider()

    # 모집률 하락 추세 경고
    st.subheader("모집률 변화 추세")
    recruit_trend = load_monthly_recruit(where, params)
    if len(recruit_trend) >= 3:
        recruit_trend = recruit_trend.sort_values('YEAR_MONTH')
        recent_3 = recruit_trend.tail(3)['모집률'].values
        if recent_3[-1] < recent_3[0]:
            st.error(f"최근 3개월 모집률 하락 추세 감지: {recent_3[0]:.1f}% → {recent_3[-1]:.1f}%")
        else:
            st.success(f"최근 3개월 모집률 안정/상승: {recent_3[0]:.1f}% → {recent_3[-1]:.1f}%")
        fig_rec_trend = px.line(recruit_trend, x='YEAR_MONTH', y='모집률', markers=True, title='월별 평균 모집률 추이')
        st.plotly_chart(fig_rec_trend, use_container_width=True)

# [Tab 8] 🎯 비용 대비 성과
with tabs[7]:
    # 산점도/시뮬레이터는 raw 샘플 필요
    cost_sample = load_scatter_sample(where, params, limit=SCATTER_SAMPLE_LIMIT)
    cost_sample['TOT_TRCO'] = pd.to_numeric(cost_sample['TOT_TRCO'], errors='coerce')
    cost_sample['EI_EMPL_RATE_3'] = pd.to_numeric(cost_sample['EI_EMPL_RATE_3'], errors='coerce')
    cost_sample['TOT_FXNUM'] = pd.to_numeric(cost_sample['TOT_FXNUM'], errors='coerce')

    # 훈련비 구간별 평균 취업률
    st.subheader("훈련비 구간별 평균 취업률")
    if not cost_sample.empty:
        cost_sample['비용구간'] = pd.cut(cost_sample['TOT_TRCO'], bins=COST_BINS, labels=COST_BIN_LABELS)
        bin_stats = cost_sample.groupby('비용구간', observed=True)['EI_EMPL_RATE_3'].agg(['mean', 'count']).reset_index()
        bin_stats.columns = ['비용구간', '평균취업률', '과정수']
        fig_bin = px.bar(bin_stats, x='비용구간', y='평균취업률', text_auto='.1f', color='과정수',
                         title='훈련비 구간별 평균 취업률(%)', labels={'평균취업률': '취업률(%)'})
        st.plotly_chart(fig_bin, use_container_width=True)
    else:
        st.info("유효한 비용/취업률 데이터가 없습니다.")

    st.divider()

    # 4분면 scatter
    st.subheader("비용 vs 취업률 4분면 분석")
    if not cost_sample.empty:
        int_scatter = (
            internal_df[(internal_df['TOT_TRCO'] > 0) & (internal_df['EI_EMPL_RATE_3'] > 0)].copy()
            if internal_df is not None else pd.DataFrame()
        )
        med_x, med_y = render_scatter_with_overlay(
            cost_sample, int_scatter,
            x_col='TOT_TRCO', y_col='EI_EMPL_RATE_3',
            title='비용 vs 취업률 (우상단=고비용/고성과, 좌상단=가성비 우수)',
            x_label='훈련비(원)', y_label='취업률(%)',
            quadrant_labels=[
                {'x': cost_sample['TOT_TRCO'].median() * 0.3, 'y': cost_sample['EI_EMPL_RATE_3'].median() * 1.5, 'text': '가성비 우수', 'color': 'green'},
                {'x': cost_sample['TOT_TRCO'].median() * 1.8, 'y': cost_sample['EI_EMPL_RATE_3'].median() * 0.5, 'text': '개선 필요', 'color': 'red'},
            ]
        )

    st.divider()

    # 가성비 챔피언 Top 20
    st.subheader("가성비 챔피언 Top 20 (취업률 / 백만원당)")
    if not cost_sample.empty:
        champ = cost_sample.copy()
        champ['가성비'] = champ['EI_EMPL_RATE_3'] / (champ['TOT_TRCO'] / 1_000_000)
        champ = champ.nlargest(20, '가성비')[['TRPR_NM', 'TRAINST_NM', 'TOT_TRCO', 'EI_EMPL_RATE_3', '가성비']]
        champ.columns = ['과정명', '훈련기관', '훈련비', '취업률(%)', '가성비(취업률/백만원)']
        st.dataframe(
            champ.style.format({'훈련비': '{:,.0f}원', '취업률(%)': '{:.1f}%', '가성비(취업률/백만원)': '{:.2f}'}),
            use_container_width=True, hide_index=True
        )

    st.divider()

    # 가격 시뮬레이터
    st.subheader("가격 시뮬레이터: 훈련비 → 예상 취업률")
    if not cost_sample.empty and len(cost_sample) >= 10:
        X = cost_sample[['TOT_TRCO']].values
        y = cost_sample['EI_EMPL_RATE_3'].values
        from sklearn.linear_model import LinearRegression
        model = LinearRegression().fit(X, y)
        r2 = model.score(X, y)

        sim_min = int(cost_sample['TOT_TRCO'].quantile(0.05))
        sim_max = int(cost_sample['TOT_TRCO'].quantile(0.95))
        sim_val = st.slider("훈련비 설정 (원)", min_value=sim_min, max_value=sim_max, value=int((sim_min + sim_max) / 2), step=100_000, format="%d원")

        pred = model.predict([[sim_val]])[0]
        pred = max(0, min(100, pred))
        st.metric("예상 취업률", f"{pred:.1f}%", help=f"선형회귀 기반 (R²={r2:.3f})")

        # 회귀선 시각화
        x_range = np.linspace(sim_min, sim_max, 100)
        y_pred = model.predict(x_range.reshape(-1, 1))

        fig_sim = go.Figure()
        sample_sim = cost_sample.sample(n=min(REGRESSION_SAMPLE_LIMIT, len(cost_sample)), random_state=42)
        fig_sim.add_trace(go.Scatter(x=sample_sim['TOT_TRCO'], y=sample_sim['EI_EMPL_RATE_3'], mode='markers', name='실제 데이터', opacity=0.3, marker=dict(size=4)))
        fig_sim.add_trace(go.Scatter(x=x_range, y=y_pred, mode='lines', name='회귀선', line=dict(color='red', width=2)))
        fig_sim.add_trace(go.Scatter(x=[sim_val], y=[pred], mode='markers', name='시뮬레이션', marker=dict(size=15, color='gold', symbol='star')))
        fig_sim.update_layout(xaxis_title='훈련비(원)', yaxis_title='취업률(%)', title=f'훈련비-취업률 선형회귀 (R²={r2:.3f})')
        st.plotly_chart(fig_sim, use_container_width=True)

        st.caption(f"회귀 계수: 훈련비 100만원 증가 시 취업률 {model.coef_[0] * 1_000_000:.2f}%p 변화 / 절편: {model.intercept_:.1f}%")
    else:
        st.info("시뮬레이션에 필요한 데이터가 부족합니다 (최소 10건).")

# [Tab 4] 유형/일정 분석
with tabs[3]:
    type_wk = load_type_counts(where, params)
    c1, c2 = st.columns(2)
    with c1:
        st.subheader("🎓 훈련 유형별 비중")
        if not type_wk.empty:
            type_cnt = type_wk.groupby('TRAIN_TARGET')['CNT'].sum().reset_index()
            type_cnt.columns = ['유형', '개수']
            st.plotly_chart(px.pie(type_cnt, values='개수', names='유형', title="K-Digital vs 일반 과정 비율"), use_container_width=True)
    with c2:
        st.subheader("📅 주말 vs 주중 개설 현황")
        if not type_wk.empty:
            wk_cnt = type_wk.groupby('WKEND_SE')['CNT'].sum().reset_index()
            wk_cnt['구분'] = wk_cnt['WKEND_SE'].astype(str).map(WK_MAP).fillna('기타')
            st.plotly_chart(px.bar(wk_cnt, x='구분', y='CNT', color='구분', text='CNT', title="직장인 타겟(주말) 과정 수", labels={'CNT': '개수'}), use_container_width=True)

# [Tab 5] 비용/성과 분석
with tabs[4]:
    st.subheader("💸 훈련비 vs 취업률 상관관계 분석")
    st.caption("원이 크면 정원이 많은 과정, 색상은 훈련 유형을 나타냅니다. (상위 2000건 샘플링)")
    scatter_5 = load_scatter_sample(where, params, limit=2000)
    if not scatter_5.empty:
        scatter_5['TOT_TRCO'] = pd.to_numeric(scatter_5['TOT_TRCO'], errors='coerce')
        scatter_5['EI_EMPL_RATE_3'] = pd.to_numeric(scatter_5['EI_EMPL_RATE_3'], errors='coerce')
        scatter_5['TOT_FXNUM'] = pd.to_numeric(scatter_5['TOT_FXNUM'], errors='coerce')
        fig_scatter = px.scatter(
            scatter_5, x='TOT_TRCO', y='EI_EMPL_RATE_3', color='TRAIN_TARGET', size='TOT_FXNUM',
            hover_data=['TRPR_NM', 'TRAINST_NM'], opacity=0.7,
            labels={'TOT_TRCO': '훈련비(원)', 'EI_EMPL_RATE_3': '취업률(%)', 'TRAIN_TARGET': '유형'}
        )
        st.plotly_chart(fig_scatter, use_container_width=True)
    else:
        st.info("분석할 유효 데이터가 없습니다.")
    st.divider()
    st.subheader("🏅 등급별 평균 성과 비교")
    grade_grp = load_grade_stats(where, params)
    if not grade_grp.empty:
        grade_grp['EI_EMPL_RATE_3'] = pd.to_numeric(grade_grp['EI_EMPL_RATE_3'], errors='coerce')
        grade_grp['STDG_SCOR'] = pd.to_numeric(grade_grp['STDG_SCOR'], errors='coerce')
        fig_bar = px.bar(grade_grp, x='GRADE', y=['EI_EMPL_RATE_3', 'STDG_SCOR'], barmode='group')
        st.plotly_chart(fig_bar, use_container_width=True)
    else:
        st.info("등급 데이터가 없습니다.")

# [Tab 9] 경쟁 현황
with tabs[8]:
    st.subheader("🏆 TOP 15 훈련기관 (총 신청인원 기준)")
    inst_top15 = load_inst_stats(where, params)
    if not inst_top15.empty:
        inst_top15['평균모집률'] = (inst_top15['REG_COURSE_MAN'] / inst_top15['TOT_FXNUM'].replace(0, pd.NA) * 100).fillna(0).clip(upper=100)
        inst_top15 = inst_top15.sort_values('REG_COURSE_MAN', ascending=False).head(15)
        inst_top15 = inst_top15.rename(columns={'TRAINST_NM': '기관명', 'REG_COURSE_MAN': '총신청인원', 'TOT_FXNUM': '총모집정원', 'TRPR_CNT': '개설수'})
        fig = px.bar(inst_top15, x='총신청인원', y='기관명', orientation='h', text='총신청인원',
                     color='평균모집률', color_continuous_scale='Bluyl',
                     hover_data=['총모집정원', '개설수', '평균모집률'],
                     title="가장 많은 훈련생을 모은 기관 Top 15")
        st.plotly_chart(fig, use_container_width=True)
        with st.expander("📄 Top 15 기관 상세 데이터 보기", expanded=True):
            st.dataframe(inst_top15[['기관명', '총신청인원', '총모집정원', '개설수', '평균모집률']], use_container_width=True, hide_index=True, column_config={
                "총신청인원": st.column_config.NumberColumn(format="%d명"),
                "총모집정원": st.column_config.NumberColumn(format="%d명"),
                "평균모집률": st.column_config.NumberColumn(format="%.1f%%"),
            })

# [Tab 10] 키워드
with tabs[9]:
    st.subheader("🔥 과정명 트렌드 키워드")
    names_df = load_keyword_names(where, params)
    if not names_df.empty:
        text = " ".join(names_df['TRPR_NM'].dropna().astype(str))
        stops = ['과정', '양성', '취업', '실무', '및', '위한', '기반', '활용', '개발자', 'A', 'B', '수료', '반', '취득', '능력', '향상']
        words = [w for w in text.split() if len(w) > 1 and w not in stops]
        kwd = pd.DataFrame(Counter(words).most_common(25), columns=['키워드', '빈도'])
        st.plotly_chart(px.bar(kwd, x='키워드', y='빈도', color='빈도'), use_container_width=True)


# [Tab 11] 자격증 분석
with tabs[10]:
    st.subheader("🎓 자격증 연계 분석")

    cert_df = _load_data("""
        SELECT CERTIFICATE, EI_EMPL_RATE_3, TOT_TRCO, TRAIN_TARGET, NCS_CD
        FROM TB_MARKET_TREND
        WHERE CERTIFICATE IS NOT NULL AND CERTIFICATE != ''
    """)

    if cert_df.empty:
        st.info("자격증 데이터가 없습니다.")
    else:
        cert_df['EI_EMPL_RATE_3'] = pd.to_numeric(cert_df['EI_EMPL_RATE_3'], errors='coerce')
        cert_df['TOT_TRCO'] = pd.to_numeric(cert_df['TOT_TRCO'], errors='coerce')

        # 자격증 파싱 (벡터 연산)
        cert_clean = (cert_df['CERTIFICATE'].astype(str)
            .str.replace(',', '|', regex=False)
            .str.replace('\n', '|', regex=False)
            .str.replace('/', '|', regex=False)
            .str.replace('·', '|', regex=False))
        exploded = (cert_df.assign(자격증=cert_clean.str.split('|'))
            .explode('자격증'))
        exploded['자격증'] = exploded['자격증'].str.strip()
        exploded = exploded[exploded['자격증'].str.len() > 1]
        cert_parsed = exploded.rename(columns={'EI_EMPL_RATE_3': '취업률', 'TOT_TRCO': '훈련비'})

        if not cert_parsed.empty:
            cert_stats = cert_parsed.groupby('자격증').agg(
                과정수=('자격증', 'count'),
                평균_취업률=('취업률', 'mean'),
                평균_훈련비=('훈련비', 'mean'),
            ).reset_index()
            cert_stats = cert_stats[cert_stats['과정수'] >= CERT_MIN_COURSES]
            cert_stats['평균_취업률'] = cert_stats['평균_취업률'].round(1)
            cert_stats['평균_훈련비'] = cert_stats['평균_훈련비'].round(0)
            cert_stats = cert_stats.sort_values('과정수', ascending=False)

            st.metric("연계 자격증 종류", f"{len(cert_stats)}개", help="5건 이상 연계된 자격증만 표시")

            cc1, cc2 = st.columns(2)
            with cc1:
                st.markdown(f"##### Top {TOP_CERTS_LIMIT} 자격증 (과정수 기준)")
                top20 = cert_stats.head(TOP_CERTS_LIMIT)
                fig = px.bar(
                    top20, x='과정수', y='자격증', orientation='h',
                    color='평균_취업률', color_continuous_scale='RdYlGn',
                    hover_data=['평균_취업률', '평균_훈련비'],
                )
                fig.update_layout(height=500, yaxis={'categoryorder': 'total ascending'})
                st.plotly_chart(fig, use_container_width=True)

            with cc2:
                st.markdown(f"##### 자격증별 평균 취업률 Top {TOP_CERTS_LIMIT}")
                top_empl = cert_stats[cert_stats['과정수'] >= CERT_EMPL_MIN_COURSES].sort_values('평균_취업률', ascending=False).head(TOP_CERTS_LIMIT)
                if not top_empl.empty:
                    fig2 = px.bar(
                        top_empl, x='평균_취업률', y='자격증', orientation='h',
                        color='과정수', color_continuous_scale='Blues',
                        hover_data=['과정수', '평균_훈련비'],
                    )
                    fig2.update_layout(height=500, yaxis={'categoryorder': 'total ascending'})
                    st.plotly_chart(fig2, use_container_width=True)

            st.markdown("##### 전체 자격증 데이터")
            st.dataframe(
                cert_stats,
                column_config={
                    '과정수': st.column_config.NumberColumn(format="%d"),
                    '평균_취업률': st.column_config.NumberColumn('평균 취업률(%)', format="%.1f"),
                    '평균_훈련비': st.column_config.NumberColumn('평균 훈련비(원)', format="%,.0f"),
                },
                use_container_width=True,
                hide_index=True,
            )
        else:
            st.info("파싱 가능한 자격증 데이터가 없습니다.")


# [Tab 12] 🔭 사업기회 발굴
with tabs[11]:
    st.caption("수요(모집률)가 높고 공급(경쟁 과정 수)이 낮은 영역을 찾아 신규 교육사업 진입 기회를 도출합니다.")

    # 우리 NCS 코드 (한 번만 조회)
    _our_ncs_opp = []
    if internal_df is not None:
        _ids_opp = internal_df['TRPR_ID'].unique().tolist()
        if _ids_opp:
            _ph = ','.join('?' * len(_ids_opp))
            _m = _sql_query(
                f"SELECT DISTINCT NCS_CD FROM TB_MARKET_TREND WHERE TRPR_ID IN ({_ph}) AND NCS_CD IS NOT NULL",
                params=_ids_opp
            )
            _our_ncs_opp = _m['NCS_CD'].dropna().unique().tolist() if not _m.empty else []

    # ── 섹션 1: 지역별 수요-공급 갭 ──
    st.subheader("📍 지역별 수요-공급 갭")
    st.caption("좌상단(과정 적고 모집률 높음) = 공급 부족 지역 → 신규 진입 기회")

    with st.spinner("📍 지역별 수요-공급 갭 분석 중..."):
        region_opp = load_region_opp(where, params)
    if not region_opp.empty:
        for col in ['과정수', '총신청인원', '평균모집률', '평균취업률']:
            region_opp[col] = pd.to_numeric(region_opp[col], errors='coerce').fillna(0)
        avg_c = region_opp['과정수'].mean()
        avg_r = region_opp['평균모집률'].mean()

        fig_reg = px.scatter(
            region_opp, x='과정수', y='평균모집률',
            size='총신청인원', text='REGION',
            color='평균취업률', color_continuous_scale='RdYlGn',
            labels={'REGION': '지역', '과정수': '공급(과정 수)', '평균모집률': '수요(모집률 %)'},
            title='지역별 공급(과정 수) vs 수요(모집률)'
        )
        fig_reg.add_hline(y=avg_r, line_dash="dash", line_color="gray", opacity=0.5)
        fig_reg.add_vline(x=avg_c, line_dash="dash", line_color="gray", opacity=0.5)
        fig_reg.add_annotation(
            x=region_opp['과정수'].quantile(0.1), y=region_opp['평균모집률'].quantile(0.85),
            text="🟢 고수요·저공급 (진입 기회)", showarrow=False, font=dict(color='green', size=11)
        )
        fig_reg.add_annotation(
            x=region_opp['과정수'].quantile(0.85), y=region_opp['평균모집률'].quantile(0.1),
            text="🔴 과잉공급 (경쟁 심화)", showarrow=False, font=dict(color='red', size=11)
        )
        fig_reg.update_traces(textposition='top center')
        st.plotly_chart(fig_reg, use_container_width=True)

        opp_regions = region_opp[
            (region_opp['과정수'] < avg_c) & (region_opp['평균모집률'] > avg_r)
        ].sort_values('평균모집률', ascending=False)
        if not opp_regions.empty:
            st.success("🎯 **진입 기회 지역**: " + ", ".join(opp_regions['REGION'].tolist()))
    else:
        st.info("지역 데이터가 부족합니다.")

    st.divider()

    # ── 섹션 2: 성장 중인 NCS 분야 ──
    st.subheader("📈 성장 중인 NCS 분야")
    st.caption("최근 6개월 개설 수 증가율 (이전 6개월 대비) — 빠르게 성장하는 분야 = 선제 진입 기회")

    with st.spinner("📈 NCS 성장 분야 분석 중..."):
        ncs_growth = load_ncs_growth(where, params)
    if not ncs_growth.empty:
        col_g1, col_g2 = st.columns(2)
        with col_g1:
            top_up = ncs_growth[ncs_growth['증가율(%)'] > 0].head(10)
            if not top_up.empty:
                fig_up = px.bar(
                    top_up, x='증가율(%)', y='NCS_CD', orientation='h',
                    color='증가율(%)', color_continuous_scale='Greens',
                    text='증가율(%)', title='🚀 급성장 NCS Top 10',
                    labels={'NCS_CD': 'NCS 분야'}
                )
                fig_up.update_layout(yaxis={'categoryorder': 'total ascending'}, height=350)
                st.plotly_chart(fig_up, use_container_width=True)
        with col_g2:
            top_dn = ncs_growth[ncs_growth['증가율(%)'] < 0].tail(10)
            if not top_dn.empty:
                fig_dn = px.bar(
                    top_dn, x='증가율(%)', y='NCS_CD', orientation='h',
                    color='증가율(%)', color_continuous_scale='Reds_r',
                    text='증가율(%)', title='📉 수요 감소 NCS Top 10',
                    labels={'NCS_CD': 'NCS 분야'}
                )
                fig_dn.update_layout(yaxis={'categoryorder': 'total descending'}, height=350)
                st.plotly_chart(fig_dn, use_container_width=True)
        with st.expander("전체 NCS 성장률 데이터"):
            st.dataframe(ncs_growth, use_container_width=True, hide_index=True,
                         column_config={'증가율(%)': st.column_config.NumberColumn(format="%.1f%%")})
    else:
        st.info("NCS 성장 분석 데이터가 부족합니다.")

    st.divider()

    # ── 섹션 3: 고성과·저경쟁 NCS 매트릭스 ──
    st.subheader("🎯 고성과·저경쟁 NCS 기회 매트릭스")
    st.caption("좌상단(경쟁 적고 취업률 높음) = 신규 진입 최적 영역. 버블 크기 = 모집률")

    with st.spinner("🎯 NCS 기회 매트릭스 분석 중..."):
        ncs_mat = load_ncs_opp_matrix(where, params)
    if not ncs_mat.empty:
        for col in ['경쟁과정수', '평균취업률', '평균모집률']:
            ncs_mat[col] = pd.to_numeric(ncs_mat[col], errors='coerce')
        ncs_mat = ncs_mat.dropna(subset=['평균취업률', '평균모집률'])

        if not ncs_mat.empty:
            ncs_mat = ncs_mat.copy()
            ncs_mat['분류'] = ncs_mat['NCS_CD'].apply(
                lambda x: '우리 분야' if x in _our_ncs_opp else '시장'
            )
            fig_mat = px.scatter(
                ncs_mat, x='경쟁과정수', y='평균취업률',
                size='평균모집률', text='NCS_CD',
                color='분류', color_discrete_map={'우리 분야': 'red', '시장': 'steelblue'},
                labels={'경쟁과정수': '경쟁 강도 (과정 수)', '평균취업률': '성과 (취업률 %)'},
                title='NCS별 경쟁강도 vs 취업률 (버블 크기 = 모집률)'
            )
            med_comp = ncs_mat['경쟁과정수'].median()
            med_empl = ncs_mat['평균취업률'].median()
            fig_mat.add_hline(y=med_empl, line_dash="dash", line_color="gray", opacity=0.5)
            fig_mat.add_vline(x=med_comp, line_dash="dash", line_color="gray", opacity=0.5)
            fig_mat.add_annotation(
                x=ncs_mat['경쟁과정수'].quantile(0.1), y=ncs_mat['평균취업률'].quantile(0.9),
                text="🌟 최적 진입 영역", showarrow=False, font=dict(color='green', size=12)
            )
            fig_mat.update_traces(textposition='top center')
            st.plotly_chart(fig_mat, use_container_width=True)

            best_ncs = ncs_mat[
                (ncs_mat['경쟁과정수'] < med_comp) & (ncs_mat['평균취업률'] > med_empl)
            ].sort_values('평균취업률', ascending=False)
            if not best_ncs.empty:
                st.success("🌟 **최적 진입 영역 NCS**: " + ", ".join(best_ncs['NCS_CD'].head(8).tolist()))
    else:
        st.info("기회 매트릭스 데이터가 부족합니다.")

    st.divider()

    # ── 섹션 4: 종합 기회 지수 ──
    st.subheader("🏆 종합 사업기회 지수 Top 15")
    st.caption("취업률(40점) + 모집률(40점) − 경쟁도(20점) 기준 정규화 합산")

    if not ncs_mat.empty and len(ncs_mat) >= 3:
        score_df = ncs_mat.copy()

        def _minmax(s):
            lo, hi = s.min(), s.max()
            return (s - lo) / (hi - lo) if hi > lo else pd.Series(0.5, index=s.index)

        score_df['기회지수'] = (
            _minmax(score_df['평균취업률']) * 40
            + _minmax(score_df['평균모집률']) * 40
            - _minmax(score_df['경쟁과정수']) * 20
        ).round(1)
        top15 = score_df.nlargest(15, '기회지수').copy()
        top15['순위'] = range(1, len(top15) + 1)
        if _our_ncs_opp:
            top15['비고'] = top15['NCS_CD'].apply(lambda x: '⭐ 우리 분야' if x in _our_ncs_opp else '')

        disp_cols = ['순위', 'NCS_CD', '기회지수', '평균취업률', '평균모집률', '경쟁과정수']
        if '비고' in top15.columns:
            disp_cols.append('비고')
        st.dataframe(
            top15[disp_cols].reset_index(drop=True),
            use_container_width=True, hide_index=True,
            column_config={
                '기회지수': st.column_config.ProgressColumn('기회지수 (80점 만점)', min_value=0, max_value=80, format="%.1f"),
                '평균취업률': st.column_config.NumberColumn(format="%.1f%%"),
                '평균모집률': st.column_config.NumberColumn(format="%.1f%%"),
                '경쟁과정수': st.column_config.NumberColumn(format="%d개"),
            }
        )
    else:
        st.info("종합 기회지수 산출에 데이터가 부족합니다.")


# [Tab 13] 데이터 조회
with tabs[12]:
    st.subheader(f"📄 상세 데이터 ({total_count:,}건)")

    preview_df = load_data_preview(where, params, limit=1000)
    if not preview_df.empty:
        # 모집률 계산
        preview_df['TOT_FXNUM'] = pd.to_numeric(preview_df['TOT_FXNUM'], errors='coerce').fillna(0)
        preview_df['REG_COURSE_MAN'] = pd.to_numeric(preview_df['REG_COURSE_MAN'], errors='coerce').fillna(0)
        preview_df['모집률'] = (preview_df['REG_COURSE_MAN'] / preview_df['TOT_FXNUM'].replace(0, pd.NA) * 100).fillna(0).clip(upper=100)
        # 주말구분 매핑
        preview_df['주말구분_명'] = preview_df['WKEND_SE'].astype(str).map(WK_MAP).fillna('기타')

    display_df = preview_df.rename(columns=COLUMN_MAP) if not preview_df.empty else pd.DataFrame()
    priority = ['과정명', '훈련기관명', '훈련유형', '지역', '주말구분', '훈련비(원)', '정원(명)', '등록인원', '모집률(%)', '취업률(3개월)', '개설일']
    cols = [c for c in priority if c in display_df.columns] + [c for c in display_df.columns if c not in priority]
    st.warning("⚠️ 상위 1,000건만 표시됩니다. 전체 데이터는 CSV로 다운로드하세요.")
    if not display_df.empty:
        st.dataframe(display_df[cols], use_container_width=True, height=600)

    # CSV 다운로드 (전체 데이터)
    if st.button("📥 CSV 다운로드 준비"):
        full_df = load_data_full_csv(where, params)
        if not full_df.empty:
            full_df['TOT_FXNUM'] = pd.to_numeric(full_df['TOT_FXNUM'], errors='coerce').fillna(0)
            full_df['REG_COURSE_MAN'] = pd.to_numeric(full_df['REG_COURSE_MAN'], errors='coerce').fillna(0)
            full_df['모집률'] = (full_df['REG_COURSE_MAN'] / full_df['TOT_FXNUM'].replace(0, pd.NA) * 100).fillna(0).clip(upper=100)
            csv_df = full_df.rename(columns=COLUMN_MAP)
            csv_cols = [c for c in priority if c in csv_df.columns] + [c for c in csv_df.columns if c not in priority]
            csv = csv_df[csv_cols].to_csv(index=False).encode('utf-8-sig')
            st.download_button("📥 전체 데이터 다운로드 (CSV)", csv, "market_analysis.csv", "text/csv")
