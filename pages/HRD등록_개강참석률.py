import streamlit as st
import pandas as pd
import altair as alt
from datetime import datetime
from zoneinfo import ZoneInfo
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils import check_password, page_error_boundary, calc_recruit_rate
from hrd_api import get_course_history_with_fallback, get_institutions, fetch_all_roster_counts
from config import CACHE_TTL_API, COURSE_SHORT_NAMES

st.set_page_config(page_title="HRD 등록 대비 개강 참석률", page_icon="🎯", layout="wide")
check_password()
with page_error_boundary():
    st.title("🎯 HRD 등록 대비 개강 참석률")
    st.markdown(
        "HRD-Net 집계 기준 **정원 → 수강신청(HRD 등록) → 개강 인원(확정 신고) → 수료** 퍼널을 "
        "한화·AI캠퍼스(엔코아) 전 회차에 대해 한눈에 봅니다. "
        "회사 운영 페이지와 교차 검증하는 용도이며, HRD-Net이 현재 시점에 제공하는 값 그대로 표시합니다."
    )

    # ─────────────────── 데이터 로딩 ───────────────────

    @st.cache_data(ttl=CACHE_TTL_API, show_spinner="HRD-Net 과정 이력 조회 중…")
    def load_history():
        # 실패 사유는 캐시 대상 안에서 함께 반환 — 캐시 히트 시 모듈 변수는 이미 비어 있다
        return get_course_history_with_fallback()

    history_df, data_source, history_error = load_history()

    if data_source == "API":
        st.caption("실시간 (API)")
    elif data_source == "DB_FALLBACK":
        st.warning(
            "실시간 조회에 실패해 DB 기준으로 표시합니다. "
            "DB에는 한화 과정만 수집되므로 **AI캠퍼스(엔코아) 과정은 빠져 있습니다.**"
        )
    else:
        st.caption("DB 기준 (API 키 미설정 — 한화 과정만 표시)")

    if history_error and data_source == "API":
        st.warning(f"일부 과정 조회 실패 — 해당 과정은 표에서 빠져 있습니다: {history_error}")

    if history_df is None or history_df.empty:
        if data_source == "DB_FALLBACK" or history_error:
            st.error("실시간 조회에 실패했고 DB에도 과정 데이터가 없습니다. 잠시 후 다시 시도해주세요.")
        else:
            st.info("표시할 과정 데이터가 없습니다.")
        st.stop()

    # ─────────────────── 파생 지표 ───────────────────

    df = history_df.copy()
    for c in ['TOT_FXNUM', 'TOT_TRP_CNT', 'TOT_PAR_MKS', 'FINI_CNT']:
        # 개설예정 회차는 TOT_PAR_MKS가 None으로 온다 → 0으로 간주
        df[c] = pd.to_numeric(df[c], errors='coerce').fillna(0).astype(int)
    df['TRPR_DEGR'] = pd.to_numeric(df['TRPR_DEGR'], errors='coerce').fillna(0).astype(int)

    def _course_label(row):
        short = COURSE_SHORT_NAMES.get(row['TRPR_ID'])
        if short:
            return short
        nm = str(row['TRPR_NM'] or '')
        # "한화시스템 BEYOND SW 캠프 - “…”" → 대시 앞 부분만
        return nm.split(' - ')[0].strip() or row['TRPR_ID']

    df['과정'] = df.apply(_course_label, axis=1)
    df['회차'] = df['TRPR_DEGR'].astype(str) + '회차'

    today_str = datetime.now(ZoneInfo("Asia/Seoul")).strftime("%Y-%m-%d")

    def _status(row):
        sta, end = str(row['TR_STA_DT'] or ''), str(row['TR_END_DT'] or '')
        if sta > today_str:
            return '개설예정'
        if end and end < today_str:
            return '종료'
        return '진행중'

    df['상태'] = df.apply(_status, axis=1)
    df['신청 이탈'] = df['TOT_TRP_CNT'] - df['TOT_PAR_MKS']

    def _pct(numer, denom):
        return (numer / denom.replace(0, pd.NA) * 100).astype('Float64').round(1)

    df['개강 참석률'] = _pct(df['TOT_PAR_MKS'], df['TOT_TRP_CNT'])
    df['모집률'] = calc_recruit_rate(df['TOT_TRP_CNT'], df['TOT_FXNUM']).round(1)
    df['정원 충원율'] = _pct(df['TOT_PAR_MKS'], df['TOT_FXNUM'])
    # 수료율은 종료 회차에서만 의미가 있다 (진행중은 FINI_CNT가 0으로 내려옴)
    df['수료율'] = _pct(df['FINI_CNT'], df['TOT_PAR_MKS']).where(df['상태'] == '종료')

    # ── 이탈 인원 · 80%이상수료 (명부 API 상태 집계) ──
    # 회차 집계 API(_3.jsp)에는 이탈 수가 없다. 명부(_4.jsp)의 훈련생 상태를 회차별로 세어 붙인다.
    # 개강 인원이 0인 개설예정 회차는 명부가 비므로 호출하지 않는다.

    @st.cache_data(ttl=CACHE_TTL_API, show_spinner="HRD-Net 명부 조회 중… (회차별 이탈 인원)")
    def load_roster_counts(rounds):
        return fetch_all_roster_counts(get_institutions(), list(rounds))

    rounds = tuple(
        (r.TRPR_ID, int(r.TRPR_DEGR))
        for r in df[df['TOT_PAR_MKS'] > 0][['TRPR_ID', 'TRPR_DEGR']].itertuples(index=False)
    )
    roster_counts, roster_error = load_roster_counts(rounds) if data_source == "API" and rounds else (None, None)
    if roster_counts is not None and not roster_counts.empty:
        df = df.merge(roster_counts, on=['TRPR_ID', 'TRPR_DEGR'], how='left')
    else:
        df['DROPOUT_CNT'] = pd.NA
        df['PARTIAL_FINI_CNT'] = pd.NA
    # 명부를 못 읽은 종료 회차는 개강 인원 − 수료로 대신한다. HRD 수료(finiCnt)에 조기취업이 빠져 있어
    # 조기취업자가 있는 회차는 실제 이탈보다 그만큼 크게 잡힌다 (한화 18회차: 27 − 24 = 3, 명부 중도탈락 1 + 조기취업 2)
    ended_fallback = (df['TOT_PAR_MKS'] - df['FINI_CNT']).where(df['상태'] == '종료')
    df['이탈 인원'] = df['DROPOUT_CNT'].astype('Float64').fillna(ended_fallback.astype('Float64')).astype('Int64')
    df['80%이상수료'] = df['PARTIAL_FINI_CNT'].astype('Float64').astype('Int64')
    df['이탈률'] = (df['이탈 인원'].astype('Float64')
                  / df['TOT_PAR_MKS'].astype('Float64').replace(0, pd.NA) * 100).round(1)

    if roster_error:
        st.warning(f"일부 회차 명부 조회 실패 — 해당 회차의 이탈 인원·80%이상수료는 비어 있습니다: {roster_error}")
    elif roster_counts is None and data_source != "API":
        st.caption("명부 API를 쓸 수 없어 이탈 인원은 종료 회차만 개강 인원 − 수료로 표시합니다. 80%이상수료는 비어 있습니다.")

    df = df.sort_values(['TR_STA_DT', 'TRPR_ID', 'TRPR_DEGR']).reset_index(drop=True)

    # ─────────────────── 필터 ───────────────────

    course_options = df['과정'].unique().tolist()
    status_options = ['개설예정', '진행중', '종료']
    f1, f2 = st.columns([2, 2])
    with f1:
        sel_courses = st.multiselect("과정", course_options, default=course_options)
    with f2:
        sel_status = st.multiselect("상태", status_options, default=status_options)

    view = df[df['과정'].isin(sel_courses) & df['상태'].isin(sel_status)].copy()
    if view.empty:
        st.info("선택한 조건에 해당하는 회차가 없습니다.")
        st.stop()

    # ─────────────────── KPI ───────────────────

    st.subheader("📌 모집 퍼널 요약")
    st.caption("선택한 회차 합산 기준 · 개강 참석률 = 개강 인원 / 수강신청 · 수료율은 종료 회차만 합산")

    tot_fx = int(view['TOT_FXNUM'].sum())
    tot_trp = int(view['TOT_TRP_CNT'].sum())
    tot_par = int(view['TOT_PAR_MKS'].sum())
    ended = view[view['상태'] == '종료']
    tot_fini = int(ended['FINI_CNT'].sum())
    par_of_ended = int(ended['TOT_PAR_MKS'].sum())

    k1, k2, k3, k4, k5 = st.columns(5)
    k1.metric("회차 수", f"{len(view)}개")
    k2.metric("정원 합계", f"{tot_fx:,}명")
    k3.metric("수강신청 합계", f"{tot_trp:,}명", help="HRD-Net 수강신청(HRD 등록) 누적 인원")
    k4.metric("개강 인원 합계", f"{tot_par:,}명", f"신청 이탈 {tot_trp - tot_par:,}명", delta_color="off",
              help="확정자 신고 기준 수강인원 (훈련생 명부와 일치)")
    k5.metric("수료 합계 (종료)", f"{tot_fini:,}명")

    known = view[view['이탈 인원'].notna()]
    tot_drop = int(known['이탈 인원'].sum())
    par_of_known = int(known['TOT_PAR_MKS'].sum())

    r1, r2, r3, r4, r5 = st.columns(5)
    r1.metric("개강 참석률", f"{tot_par / tot_trp * 100:.1f}%" if tot_trp else "-",
              help="개강 인원 / 수강신청 × 100")
    r5.metric("이탈률", f"{tot_drop / par_of_known * 100:.1f}%" if par_of_known else "-",
              f"이탈 {tot_drop:,}명", delta_color="inverse",
              help="이탈 인원 / 개강 인원 × 100 — 이탈 인원이 확인된 회차만 합산")
    r2.metric("모집률", f"{min(tot_trp / tot_fx * 100, 100):.1f}%" if tot_fx else "-",
              help="수강신청 / 정원 × 100 (상한 100%)")
    r3.metric("정원 충원율", f"{tot_par / tot_fx * 100:.1f}%" if tot_fx else "-",
              help="개강 인원 / 정원 × 100")
    r4.metric("수료율 (종료)", f"{tot_fini / par_of_ended * 100:.1f}%" if par_of_ended else "-",
              help="종료 회차의 수료 / 개강 인원 × 100")

    st.divider()

    # ─────────────────── 차트 ───────────────────

    c_left, c_right = st.columns(2)

    with c_left:
        st.subheader("🔻 과정별 퍼널")
        stage_order = ['정원', '수강신청', '개강 인원', '수료']
        funnel = (
            view.groupby('과정')[['TOT_FXNUM', 'TOT_TRP_CNT', 'TOT_PAR_MKS', 'FINI_CNT']].sum()
            .rename(columns={'TOT_FXNUM': '정원', 'TOT_TRP_CNT': '수강신청',
                             'TOT_PAR_MKS': '개강 인원', 'FINI_CNT': '수료'})
            .reset_index()
            .melt(id_vars='과정', var_name='단계', value_name='인원')
        )
        funnel_chart = alt.Chart(funnel).mark_bar().encode(
            x=alt.X('단계:N', sort=stage_order, title=None),
            y=alt.Y('인원:Q', axis=alt.Axis(title=['인', '원', '(명)'], titleAngle=0)),
            color=alt.Color('단계:N', sort=stage_order, legend=None),
            column=alt.Column('과정:N', title=None),
            tooltip=['과정', '단계', '인원'],
        ).properties(height=220)
        st.altair_chart(funnel_chart)
        st.caption("수료는 종료 회차만 값이 있어 진행중 과정은 0으로 표시됩니다.")

    with c_right:
        st.subheader("📈 회차별 개강 참석률")
        trend = view[view['TOT_TRP_CNT'] > 0].copy()
        trend['라벨'] = trend['과정'] + ' ' + trend['회차']
        if trend.empty:
            st.info("수강신청이 있는 회차가 없어 추이를 표시할 수 없습니다.")
        else:
            label_order = trend['라벨'].tolist()
            base = alt.Chart(trend).encode(
                x=alt.X('라벨:N', sort=label_order, title=None, axis=alt.Axis(labelAngle=-45)),
            )
            bars = base.mark_bar().encode(
                y=alt.Y('개강 참석률:Q', axis=alt.Axis(title=['참', '석', '률', '(%)'], titleAngle=0),
                        scale=alt.Scale(domain=[0, 100])),
                color=alt.Color('과정:N', title='과정'),
                tooltip=['과정', '회차', '상태', 'TOT_TRP_CNT', 'TOT_PAR_MKS', '개강 참석률'],
            )
            rule = alt.Chart(pd.DataFrame({'y': [tot_par / tot_trp * 100 if tot_trp else 0]})).mark_rule(
                strokeDash=[4, 4], color='#e74c3c'
            ).encode(y='y:Q')
            st.altair_chart((bars + rule).properties(height=260), width='stretch')
            st.caption("점선 = 선택 회차 합산 개강 참석률")

    st.divider()

    # ─────────────────── 회차별 상세 표 ───────────────────

    st.subheader("📋 회차별 모집·등록·수강 현황")
    st.caption(
        "HRD-Net 훈련일정 상세 API 집계값 + 명부 상태 집계(이탈 인원·80%이상수료). "
        "이탈 인원 = 개강 후 확정 인원 중 중도탈락·제적. 80%이상수료는 수료에 포함된 인원이라 이탈이 아닙니다. "
        "표 우측 상단에서 CSV로 내려받아 회사 페이지와 대조할 수 있습니다."
    )

    table_cols = ['과정', '회차', '상태', 'TR_STA_DT', 'TR_END_DT',
                  'TOT_FXNUM', 'TOT_TRP_CNT', 'TOT_PAR_MKS', '신청 이탈', '이탈 인원', 'FINI_CNT', '80%이상수료',
                  '개강 참석률', '모집률', '정원 충원율', '수료율', '이탈률']
    st.dataframe(
        view.sort_values(['TR_STA_DT', 'TRPR_DEGR'], ascending=False)[table_cols],
        column_config={
            "TR_STA_DT":   st.column_config.DateColumn("개강일"),
            "TR_END_DT":   st.column_config.DateColumn("종료일"),
            "TOT_FXNUM":   st.column_config.NumberColumn("정원", format="%d명"),
            "TOT_TRP_CNT": st.column_config.NumberColumn("수강신청", format="%d명", help="HRD 등록 인원"),
            "TOT_PAR_MKS": st.column_config.NumberColumn("개강 인원", format="%d명", help="확정 신고 인원"),
            "신청 이탈":    st.column_config.NumberColumn("신청 이탈", format="%d명", help="수강신청 − 개강 인원"),
            "이탈 인원":    st.column_config.NumberColumn("이탈 인원", format="%d명",
                                                       help="명부 상태 중도탈락·제적 (개강 후 확정 인원 기준). 명부를 못 읽은 종료 회차는 개강 인원 − 수료 (조기취업이 섞여 실제보다 클 수 있음)"),
            "FINI_CNT":    st.column_config.NumberColumn("수료", format="%d명", help="HRD-Net finiCnt = 정상수료 + 80%이상수료. 조기취업은 포함되지 않음"),
            "80%이상수료":  st.column_config.NumberColumn("80%이상수료", format="%d명",
                                                       help="수료에 포함된 인원 중 80%이상수료 상태. 회사 운영표의 '80%수료(비용O)' 항목과 대응"),
            "개강 참석률":  st.column_config.ProgressColumn("개강 참석률(%)", format="%.1f%%", min_value=0, max_value=100),
            "모집률":       st.column_config.ProgressColumn("모집률(%)", format="%.1f%%", min_value=0, max_value=100),
            "정원 충원율":  st.column_config.ProgressColumn("정원 충원율(%)", format="%.1f%%", min_value=0, max_value=100),
            "수료율":       st.column_config.ProgressColumn("수료율(%)", format="%.1f%%", min_value=0, max_value=100),
            "이탈률":       st.column_config.ProgressColumn("이탈률(%)", format="%.1f%%", min_value=0, max_value=100,
                                                         help="이탈 인원 / 개강 인원 × 100"),
        },
        hide_index=True,
        width='stretch',
        height=min(80 + 36 * len(view), 720),
    )

    st.divider()

    # ─────────────────── 참고: 교차 검증 가이드 ───────────────────

    st.subheader("📝 참고: 회사 운영 페이지와 교차 검증할 때")
    st.caption("이 페이지가 무엇을 어떻게 가공했는지, 대조할 때 무엇을 봐야 하는지, 그 결과로 무엇을 결정해야 하는지를 한곳에 모았습니다.")

    n_courses = history_df['TRPR_ID'].nunique()
    pairs = get_institutions()
    pre_open = df[(df['상태'] == '개설예정') & (df['TOT_PAR_MKS'] > 0)]
    over_capacity = df[df['TOT_TRP_CNT'] > df['TOT_FXNUM']]
    fx_values = sorted(df['TOT_FXNUM'].unique().tolist())

    ref1, ref2, ref3 = st.tabs(["① 이 화면의 가공·제외 규칙", "② 교차 검증 체크리스트", "③ 결정할 항목"])

    with ref1:
        st.markdown(f"""
**데이터 출처** — HRD-Net 훈련일정 상세 API(`HRDPOA60_3.jsp`)를 조회 시점에 직접 호출한 스냅샷 (캐시 5분). 등록된 (기관키, 과정ID) 쌍 {len(pairs)}개 → 조회된 과정 {n_courses}개, 회차 {len(history_df)}개.

| 화면 컬럼 | HRD-Net 필드 | 이 화면에서의 해석 |
|---|---|---|
| 정원 | `totFxnum` | 승인 정원 |
| 수강신청 | `totTrpCnt` | HRD 등록(수강신청) 누적 인원 |
| 개강 인원 | `totParMks` | 확정 신고 인원으로 해석. **훈련생 명부(API 3) 건수와 정확히 일치**하는 것을 실측으로 확인 |
| 수료 | `finiCnt` | 수료 인원 = 정상수료 + 80%이상수료. **조기취업은 빠져 있음** (명부 실측: 한화 3회차 정상 22 + 80% 1 = 23 = finiCnt, 조기취업 1 제외). 진행중 회차는 0으로 내려옴 |
| 이탈 인원 | 명부 API(`_4.jsp`) `trneeSttusNm` | 회차별 명부에서 **중도탈락·제적** 상태를 센 값. 개강 후 확정 인원 기준 이탈이며, 회사 운영표의 **중도이탈**과 같은 단계 (개강 전 초기이탈은 명부에 없어 안 잡힘) |
| 80%이상수료 | 명부 API `trneeSttusNm` | 수료 중 `80%이상수료` 상태만 센 값. 이탈이 아니라 수료에 포함되며, 회사 운영표의 '80%수료(비용O)' 항목 대조용 |
| 상태 | `trStaDt`/`trEndDt` | 오늘(KST) 기준으로 개설예정 · 진행중 · 종료를 이 화면이 계산 |

**제외한 것**

- **회차 0 레코드 (과정마다 1건, 총 {n_courses}건)** — 개강일·종료일이 없고 수강신청 0, 정원만 30으로 내려오는 과정 헤더(템플릿) 행입니다. 실제 기수가 아니므로 표·합계·차트 모두에서 뺐습니다. 운영 현황 페이지도 같은 규칙입니다.
- **개인 식별 정보** — 명부는 상태를 회차별로 세는 데만 쓰고 이름·ID는 화면에 올리지 않습니다. 출결 기록은 다루지 않습니다.
- **명부 조회 실패 회차** — 이탈 인원·80%이상수료가 비어 보입니다(상단 경고). 종료 회차만 개강 인원 − 수료로 이탈 인원을 대신 채우는데, 조기취업자가 섞여 실제 이탈보다 클 수 있습니다.
- **DB 폴백 시 엔코아 과정** — ETL이 한화 과정만 수집하므로 API 실패 시 한화만 보입니다 (상단 경고로 표시).

**가공한 것**

- 개설예정 회차의 개강 인원이 `None`이면 0으로 표시했습니다.
- 수료율은 종료 회차에만 계산했습니다. 진행중·개설예정은 빈칸입니다.
- 모집률은 기존 규칙대로 100%에서 잘랐습니다. 잘리기 전 값이 필요하면 표의 수강신청 ÷ 정원으로 보세요. 현재 정원 초과 신청 회차: {len(over_capacity)}개.
- 정원 값의 종류: {', '.join(f'{v}명' for v in fx_values)} — 회차마다 다르면 표에서 확인하세요.

**API가 못 주는 것**

- 신청만 하고 확정되지 않은 사람이 **누구인지** (명부에 없음 → 인원 차이로만 관측)
- 확정 신고가 **언제** 반영됐는지 (스냅샷만 제공, 상태 변경 일시 없음)
- **합격자 수** (HRD-Net 밖의 회사 내부 단계)
        """)

    with ref2:
        st.markdown("""
회사 페이지의 같은 회차 숫자를 옆에 두고 아래 순서로 보세요. 표 우측 상단 CSV 내려받기를 쓰면 편합니다.

| # | 대조 항목 | 이 화면 | 회사 페이지 | 다르면 의미하는 것 |
|---|---|---|---|---|
| 1 | 확정자 수 | 개강 인원 | 확정자 신고 인원 | 회차별로 같아야 정상. 다르면 `totParMks`가 "확정 신고"가 아니라 다른 단계(기관 승인 등)를 세는 것 |
| 2 | HRD 등록 수 | 수강신청 | HRD 등록(수강신청) 인원 | 이 화면이 더 크면 `totTrpCnt`는 취소·반려분까지 포함한 누적치 |
| 3 | 합격 → 등록 전환 | (없음) | 합격자 수 | API에 없으므로 합격자 대비 등록률은 회사 데이터로만 계산 가능 |
| 4 | 수료 인원 | 수료 (종료 회차만) | 수료·조기취업 인원 | 조기취업이 수료에 포함되는지 확인 |
| 5 | 정원 | 정원 | 승인 정원 | 회차별 변경 여부 |
| 6 | 기준 시점 | 조회 시각 스냅샷 | 회사 페이지 기준일 | 며칠 차이만으로도 진행중 회차 수치가 달라짐 |
| 7 | 이탈 인원 | 이탈 인원 (명부 중도탈락·제적) | 중도이탈 | 같아야 정상. 회사 페이지의 초기이탈은 명부에 없으므로 여기엔 안 잡힘 |
| 8 | 80%이상수료 | 80%이상수료 | 80%수료(비용O) | 건수가 같아야 정상. 다르면 상태 반영 시점 차이 |
        """)
        if not pre_open.empty:
            rows = ", ".join(
                f"{r['과정']} {r['회차']} (개강 {r['TR_STA_DT']}, 신청 {int(r['TOT_TRP_CNT'])} / 개강 인원 {int(r['TOT_PAR_MKS'])})"
                for _, r in pre_open.iterrows()
            )
            st.warning(
                f"**지금 확인이 필요한 회차** — 아직 개강하지 않았는데 개강 인원이 이미 잡혀 있습니다: {rows}. "
                "운영 프로세스(개강 1주 후 확정자 신고)대로라면 0이어야 합니다. 회사 페이지의 확정자 수가 0이면 "
                "`totParMks`는 확정 신고가 아니라 개강 전 승인 인원을 세는 것이므로 컬럼 이름을 바꿔야 합니다."
            )
        else:
            st.info("개강 전인데 개강 인원이 잡힌 회차는 현재 없습니다.")

    with ref3:
        st.markdown("""
교차 검증 결과에 따라 아래를 정하면 다음 설계 변경에 그대로 반영합니다.

| # | 결정 항목 | 선택지 | 기본값(현재) |
|---|---|---|---|
| 1 | `totParMks` 컬럼 이름 | 개강 인원(확정 신고) 유지 / "승인 인원"으로 변경 / 둘 다 표기 | 개강 인원 |
| 2 | `totTrpCnt` 해석 | 누적 신청(취소 포함) / 유효 신청 | 누적 신청으로 표기 |
| 3 | 개설예정 회차 | 표에 포함 / 기본 필터에서 제외 | 포함 |
| 4 | 개강 참석률 분모 | 수강신청 / 합격자 수(회사 데이터 수기 입력) | 수강신청 |
| 5 | 시점 이력 저장 | 매시간 ETL에 정원·신청·개강·수료 4개 값을 수집 시각과 함께 append (전환 시점 추적 가능) / 현행 스냅샷 유지 | 스냅샷 |
| 6 | 회사 페이지 전용 컬럼 | 합격자·취소 등 API에 없는 값을 수기 업로드해 병기 / 회사 페이지에서만 관리 | 회사 페이지에서만 |
        """)
