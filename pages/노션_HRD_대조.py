import streamlit as st
import altair as alt
from datetime import datetime
from zoneinfo import ZoneInfo
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils import check_password, page_error_boundary
from hrd_api import get_course_history_with_fallback, get_active_data_with_fallback, get_funnel_institutions, _get_secret
from notion_ops import (
    NotionFetchError, fetch_ops_table, load_ops_csv, compare_ops, roster_current_counts,
)
from config import CACHE_TTL_API, NOTION_OPS_DB_ID, COURSE_GROUP_KEYWORDS

st.set_page_config(page_title="노션 운영현황 × HRD-Net 대조", page_icon="🔁", layout="wide")
check_password()
with page_error_boundary():
    st.title("🔁 노션 운영현황 × HRD-Net 대조")
    st.markdown(
        "회사 노션 **운영현황표(매일 10시 기준)** 와 HRD-Net 훈련일정 상세 API 집계를 같은 기수끼리 나란히 놓습니다. "
        "**과정 그룹 + 개강일**이 같으면 같은 기수로 봅니다. 노션은 읽기만 하고 절대 쓰지 않습니다."
    )

    # ─────────────────── 데이터 로딩 ───────────────────

    @st.cache_data(ttl=CACHE_TTL_API, show_spinner="HRD-Net 과정 이력 조회 중…")
    def load_history():
        return get_course_history_with_fallback(get_funnel_institutions())

    @st.cache_data(ttl=CACHE_TTL_API, show_spinner="노션 운영현황표 조회 중…")
    def load_notion(token):
        # 실패 사유는 캐시 안에 같이 담는다 — 예외를 캐시 밖으로 던지면 매번 재호출된다
        try:
            return fetch_ops_table(token), None
        except NotionFetchError as e:
            return None, str(e)

    @st.cache_data(ttl=CACHE_TTL_API, show_spinner="HRD-Net 명부 조회 중… (최대 2분)")
    def load_active():
        courses, trainees, logs, source = get_active_data_with_fallback()
        return trainees, source

    history_df, hrd_source, history_error = load_history()

    token = _get_secret("NOTION_TOKEN")
    uploaded = st.file_uploader(
        "노션 CSV 업로드 (토큰 없이 대조하려면 운영현황표 → 내보내기 → CSV)",
        type="csv", help="업로드하면 API 대신 이 파일을 노션 기준값으로 씁니다.",
    )

    notion_df, notion_source, notion_error = None, None, None
    if uploaded is not None:
        try:
            notion_df, notion_source = load_ops_csv(uploaded), "CSV"
        except Exception as e:
            notion_error = f"CSV를 읽지 못했습니다: {type(e).__name__}: {e}"
    elif token:
        notion_df, notion_error = load_notion(token)
        notion_source = "API"

    # 출처 표시
    src_left, src_right = st.columns(2)
    with src_left:
        if hrd_source == "API":
            st.caption("HRD-Net: 실시간 (API)")
        elif hrd_source == "DB_FALLBACK":
            st.warning("HRD-Net 실시간 조회 실패 → DB 기준. **AI캠퍼스(엔코아) 회차는 빠져 있습니다.**")
        else:
            st.caption("HRD-Net: DB 기준 (API 키 미설정 — 한화 과정만)")
        if history_error and hrd_source == "API":
            st.warning(f"일부 과정 조회 실패: {history_error}")
    with src_right:
        if notion_source == "API" and notion_df is not None:
            st.caption(f"노션: 실시간 (API) · {len(notion_df)}행")
        elif notion_source == "CSV":
            st.caption(f"노션: 업로드한 CSV · {len(notion_df)}행")
        elif notion_error:
            st.error(f"노션: {notion_error}")
        else:
            st.info("노션: 연결 안 됨 — `NOTION_TOKEN`을 설정하거나 CSV를 업로드하세요. (아래 설정 안내)")

    if notion_df is None:
        with st.expander("🔧 노션 연결 설정 안내", expanded=True):
            st.markdown(f"""
**방법 1 — Notion API (권장, 자동 갱신)**

1. [notion.so/profile/integrations](https://www.notion.so/profile/integrations)에서 **내부 통합**을 만들고 권한은 *콘텐츠 읽기*만 켭니다.
2. 노션에서 운영현황표(`{NOTION_OPS_DB_ID}`) 페이지 우측 상단 `…` → **연결(Connections)** → 방금 만든 통합을 추가합니다.
3. 통합의 시크릿을 Streamlit secrets(또는 `.env`)에 `NOTION_TOKEN`으로 넣습니다.

**방법 2 — CSV 업로드 (일회성)**

노션 운영현황표 우측 상단 `…` → **내보내기** → 형식 CSV → 받은 파일을 위 업로드 칸에 넣습니다.
날짜가 `2026년 7월 9일` 형식이어도 자동으로 읽습니다.
            """)
        st.stop()

    # ─────────────────── 대조 ───────────────────

    today_str = datetime.now(ZoneInfo("Asia/Seoul")).strftime("%Y-%m-%d")
    cmp = compare_ops(history_df, notion_df, today=today_str)

    with_roster = st.checkbox(
        "HRD-Net 명부 기준 '훈련중' 인원도 대조 (노션 현재인원과 비교, 최대 2분 소요)", value=False,
    )
    if with_roster:
        trainees_df, roster_source = load_active()
        counts = roster_current_counts(trainees_df)
        cmp = cmp.merge(counts, on=["TRPR_ID", "TRPR_DEGR"], how="left")
        cmp["현재_차이"] = (cmp["HRD_훈련중"] - cmp["노션_현재인원"]).astype("Float64")
        if roster_source != "API":
            st.warning("명부를 DB 기준으로 읽었습니다 — AI캠퍼스(엔코아) 회차의 훈련중 인원은 비어 있을 수 있습니다.")

    groups = list(COURSE_GROUP_KEYWORDS.keys())
    f1, f2 = st.columns([2, 2])
    with f1:
        sel_groups = st.multiselect("과정 그룹", groups, default=groups)
    with f2:
        sel_match = st.multiselect("매칭", ["양쪽", "HRD만", "노션만"], default=["양쪽", "HRD만", "노션만"])
    view = cmp[cmp["그룹"].isin(sel_groups) & cmp["매칭"].isin(sel_match)].copy()

    n_skipped = int((notion_df["그룹"].isna()).sum())
    st.caption(
        f"대조 키 = 과정 그룹({', '.join(groups)}) + 개강일 · "
        f"그룹에 안 걸리는 노션 과정 {n_skipped}개(HRD 미추적 단기 과정 등)는 제외"
    )

    if view.empty:
        st.info("선택한 조건에 해당하는 기수가 없습니다.")
        st.stop()

    # ─────────────────── KPI ───────────────────

    st.subheader("📌 대조 요약")
    both = view[view["매칭"] == "양쪽"]
    n_ok = int((both["판정"] == "✅ 일치").sum())
    n_bad = int((both["판정"] == "⚠️ 불일치").sum())
    k1, k2, k3, k4, k5 = st.columns(5)
    k1.metric("양쪽 모두 있는 기수", f"{len(both)}개")
    k2.metric("확정 인원 일치", f"{n_ok}개", help="HRD 개강 인원(totParMks) = 노션 확정자신고")
    k3.metric("확정 인원 불일치", f"{n_bad}개", delta_color="inverse")
    k4.metric("HRD에만 있음", f"{int((view['매칭'] == 'HRD만').sum())}개",
              help="노션 운영현황표에 같은 그룹·개강일 행이 없음 (개설예정·종료 기수 포함)")
    k5.metric("노션에만 있음", f"{int((view['매칭'] == '노션만').sum())}개",
              help="HRD-Net 이력에 같은 그룹·개강일 회차가 없음")

    if n_bad:
        bad = both[both["판정"] == "⚠️ 불일치"]
        st.warning(
            "확정 인원이 다른 기수: " + " · ".join(
                f"{r['그룹']} {r['회차']} (HRD {int(r['HRD_확정'])} / 노션 {int(r['노션_확정자신고'])})"
                for _, r in bad.iterrows()
            )
        )

    st.divider()

    # ─────────────────── 대조 표 ───────────────────

    st.subheader("📋 기수별 인원 대조")
    st.caption(
        "HRD 컬럼은 HRD-Net 집계값, 노션 컬럼은 운영현황표 값. "
        "**확정 차이 = HRD 개강 인원 − 노션 확정자신고** (0이면 일치). 실제 개강 참석률 = 노션 개강인원 ÷ HRD 수강신청."
    )

    table_cols = ["그룹", "회차", "노션_과정명", "판정", "상태", "개강일", "HRD_종료일", "노션_종강일",
                  "정원", "HRD_수강신청", "노션_개강인원", "노션_초기이탈", "노션_추가인원",
                  "노션_확정자신고", "HRD_확정", "확정_차이", "노션_중도이탈", "노션_현재인원"]
    if with_roster:
        table_cols += ["HRD_훈련중", "현재_차이"]
    table_cols += ["HRD_수료", "등록확정전환율", "개강참석률_노션", "등록대비개강차이"]

    st.dataframe(
        view[table_cols],
        column_config={
            "그룹": st.column_config.TextColumn("과정"),
            "노션_과정명": st.column_config.TextColumn("노션 과정명"),
            "HRD_종료일": st.column_config.DateColumn("종료일 (HRD)"),
            "노션_종강일": st.column_config.DateColumn("종강일 (노션)"),
            "정원": st.column_config.NumberColumn("정원 (HRD)", format="%d명"),
            "HRD_수강신청": st.column_config.NumberColumn("수강신청 (HRD)", format="%d명", help="HRD 등록 인원"),
            "노션_개강인원": st.column_config.NumberColumn("개강인원 (노션)", format="%d명", help="첫날 실제 출석 인원"),
            "노션_초기이탈": st.column_config.NumberColumn("초기이탈 (노션)", format="%d명"),
            "노션_추가인원": st.column_config.NumberColumn("추가인원 (노션)", format="%d명", help="개강~확정자신고 사이 합류"),
            "노션_확정자신고": st.column_config.NumberColumn("확정자신고 (노션)", format="%d명",
                                                         help="개강인원 − 초기이탈 + 추가인원"),
            "HRD_확정": st.column_config.NumberColumn("개강 인원 (HRD)", format="%d명", help="totParMks = 확정 신고 인원"),
            "확정_차이": st.column_config.NumberColumn("확정 차이", format="%+d명"),
            "노션_중도이탈": st.column_config.NumberColumn("중도이탈 (노션)", format="%d명"),
            "노션_현재인원": st.column_config.NumberColumn("현재인원 (노션)", format="%d명", help="확정자신고 − 중도이탈"),
            "HRD_훈련중": st.column_config.NumberColumn("훈련중 (HRD 명부)", format="%d명",
                                                     help="명부에서 수료·조기취업·중도탈락·제적 제외"),
            "현재_차이": st.column_config.NumberColumn("현재 차이", format="%+d명", help="HRD 훈련중 − 노션 현재인원"),
            "HRD_수료": st.column_config.NumberColumn("수료 (HRD)", format="%d명"),
            "등록확정전환율": st.column_config.ProgressColumn("등록→확정 전환율(%)", format="%.1f%%", min_value=0, max_value=100,
                                                        help="HRD 개강 인원 ÷ HRD 수강신청. 기존 페이지의 '개강 참석률'"),
            "개강참석률_노션": st.column_config.NumberColumn("실제 개강 참석률(%)", format="%.1f%%",
                                                      help="노션 개강인원 ÷ HRD 수강신청. 100% 초과면 등록 없이 개강 참석한 인원이 있거나 신청 취소분이 빠진 것"),
            "등록대비개강차이": st.column_config.NumberColumn("개강 − 등록", format="%+d명", help="노션 개강인원 − HRD 수강신청"),
        },
        hide_index=True,
        width='stretch',
        height=min(80 + 36 * len(view), 720),
    )

    st.divider()

    # ─────────────────── 차트 ───────────────────

    chart_df = both[both["HRD_수강신청"].notna()].copy()
    if not chart_df.empty:
        st.subheader("📊 기수별 단계 인원 (HRD vs 노션)")
        chart_df["기수"] = chart_df["그룹"] + " " + chart_df["회차"].fillna("") + " (" + chart_df["개강일"] + ")"
        stages = {
            "HRD_수강신청": "① 수강신청 (HRD)",
            "노션_개강인원": "② 개강인원 (노션)",
            "노션_확정자신고": "③ 확정자신고 (노션)",
            "HRD_확정": "③ 개강 인원 (HRD)",
            "노션_현재인원": "④ 현재인원 (노션)",
        }
        long = chart_df.melt(id_vars="기수", value_vars=list(stages.keys()), var_name="단계", value_name="인원")
        long["단계"] = long["단계"].map(stages)
        long = long[long["인원"].notna()]
        order = list(stages.values())
        chart = alt.Chart(long).mark_bar().encode(
            x=alt.X("단계:N", sort=order, title=None, axis=alt.Axis(labelAngle=-30)),
            y=alt.Y("인원:Q", axis=alt.Axis(title=["인", "원", "(명)"], titleAngle=0)),
            color=alt.Color("단계:N", sort=order, legend=None),
            column=alt.Column("기수:N", title=None),
            tooltip=["기수", "단계", "인원"],
        ).properties(height=220)
        st.altair_chart(chart)
        st.caption("③이 두 개면 HRD 개강 인원과 노션 확정자신고가 같은 단계라는 뜻 — 높이가 같아야 정상입니다.")
        st.divider()

    # ─────────────────── 용어 매핑 ───────────────────

    st.subheader("📝 용어 매핑 (이 앱 ↔ 노션)")
    st.markdown("""
| 이 앱 (HRD-Net 기준) | 노션 운영현황표 | 관계 |
|---|---|---|
| 수강신청 (`totTrpCnt`) | — | 노션은 HRD 등록 단계를 관리하지 않음 |
| **개강 인원** (`totParMks`) | **확정자신고** = 개강인원 − 초기이탈 + 추가인원 | 같은 값 (2026-09-08 실측, AIO 1·2기 / MLE 1·2기 모두 일치). 노션 "개강인원"과는 **다른 단계** |
| — | 개강인원 | 첫날 실제 출석 인원. API에 없음 → 이 화면에서만 볼 수 있음 |
| 신청 이탈 = 수강신청 − 개강 인원 | 초기이탈 / 추가인원 | 노션 "이탈"은 개강 이후 단계. 신청 이탈은 등록 후 미참석까지 섞인 값이라 다른 개념 |
| — | 중도이탈, 현재인원, 이탈합계 | 확정 이후 운영 지표. 이 화면은 명부 '훈련중' 인원과 대조 가능 (체크박스) |
| 개강 참석률 = 개강 인원 ÷ 수강신청 | — | 노션 용어로는 **등록→확정 전환율**. 진짜 개강 참석률은 노션 개강인원 ÷ 수강신청 |
| 종료일 / 상태 "종료" | 종강일 | 명칭만 다름 |
| N회차 (HRD 회차 번호) | N기 (기수) | 번호 체계가 다름 → 개강일로 맞춤 |
| 정원, 모집률, 정원 충원율 | — | 노션에 정원 개념 없음 |
| 수료 (`finiCnt`) | — (수료목표인원만 있고 미입력) | 대조 불가 |
    """)

    with st.expander("한쪽에만 있는 기수 보기"):
        only_h = cmp[cmp["매칭"] == "HRD만"]
        only_n = cmp[cmp["매칭"] == "노션만"]
        c1, c2 = st.columns(2)
        with c1:
            st.markdown(f"**HRD에만 있음 ({len(only_h)}개)** — 노션 운영현황표에 같은 그룹·개강일 행이 없음")
            st.dataframe(only_h[["그룹", "회차", "상태", "개강일", "HRD_종료일", "HRD_수강신청", "HRD_확정"]],
                         hide_index=True, width='stretch')
        with c2:
            st.markdown(f"**노션에만 있음 ({len(only_n)}개)** — HRD-Net 이력에 같은 그룹·개강일 회차가 없음")
            st.dataframe(only_n[["그룹", "노션_과정명", "상태", "개강일", "노션_종강일", "노션_개강인원", "노션_확정자신고"]],
                         hide_index=True, width='stretch')
        st.caption("개강일이 하루라도 다르면 매칭되지 않습니다. 양쪽에 같은 기수가 따로 보이면 개강일 입력을 먼저 확인하세요.")

    csv_bytes = view[table_cols].to_csv(index=False).encode("utf-8-sig")
    st.download_button("대조 결과 CSV 내려받기", csv_bytes, file_name=f"notion_hrd_compare_{today_str}.csv", mime="text/csv")
