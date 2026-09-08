"""노션 운영현황표 조회 · HRD-Net 회차 집계 대조 (읽기 전용).

회사 노션의 "운영현황표 (매일 10시 기준)"과 HRD-Net 훈련일정 상세 API(`_3.jsp`)가
같은 기수를 어떻게 다르게 세는지 한 화면에서 보기 위한 모듈.

노션 데이터는 두 경로 중 하나로 들어온다.
  1) Notion API — `NOTION_TOKEN`(내부 통합 토큰) + 운영현황표에 통합 연결. 페이지네이션으로 전량 조회
  2) 노션 CSV 내보내기 업로드 — 토큰 없이도 대조 가능
어느 경로든 `NOTION_OPS_COLUMNS` 구조로 정규화한 뒤 (과정 그룹, 개강일)을 키로 HRD 회차와 맞춘다.

2026-09-08 실측: HRD `totParMks`(이 앱의 "개강 인원") = 노션 `확정자신고`(개강인원 − 초기이탈 + 추가인원).
AIO 1·2기, MLE 1·2기 모두 정확히 일치. 노션의 `개강인원`(첫날 출석 인원)과는 다른 값이다.

노션 쪽은 절대 쓰지 않는다 — 조회(`POST .../query`)만 호출한다.
"""
import logging
import re
from datetime import datetime

import pandas as pd
import requests

import config

logger = logging.getLogger(__name__)

# 정규화된 노션 행 구조. 사람 이름이 들어가는 속성(PM, 강사, 지각/결석 메모)은 가져오지 않는다.
NOTION_OPS_COLUMNS = [
    "NOTION_URL", "과정명", "구분", "팀", "강의실", "개강일", "종강일", "교육일수",
    "개강인원", "초기이탈", "추가인원", "확정자신고", "중도이탈", "현재인원", "이탈합계",
    "지각외출", "결석",
]

# 노션 속성명 → 정규화 컬럼명. 속성 이름이 노션에서 바뀌면 여기만 고친다.
_PROP_MAP = {
    "과정명": "과정명",
    "구분": "구분",
    "팀": "팀",
    "강의실": "강의실",
    "개강일": "개강일",
    "종강일": "종강일",
    "교육일수": "교육일수",
    "개강인원": "개강인원",
    "초기이탈": "초기이탈",
    "추가인원 (개강~확정자신고)": "추가인원",
    "확정자신고": "확정자신고",
    "중도이탈": "중도이탈",
    "현재인원": "현재인원",
    "이탈합계": "이탈합계",
    "지각/외출": "지각외출",
    "결석": "결석",
}
_NUMERIC_COLS = ["개강인원", "초기이탈", "추가인원", "확정자신고", "중도이탈", "현재인원",
                 "이탈합계", "지각외출", "결석"]


class NotionFetchError(RuntimeError):
    """노션 조회 실패. 메시지는 화면에 그대로 보여줄 수 있는 한글 안내."""


# ── 과정 그룹 ─────────────────────────────────────────────────────────


def course_group(name):
    """과정명(노션·HRD 어느 쪽이든)을 `config.COURSE_GROUP_KEYWORDS` 그룹으로 매핑. 없으면 None."""
    text = str(name or "")
    for group, keywords in config.COURSE_GROUP_KEYWORDS.items():
        if any(k in text for k in keywords):
            return group
    return None


# ── Notion API ────────────────────────────────────────────────────────


def _prop_value(prop):
    """Notion 속성 객체 → 파이썬 값. 지원하지 않는 타입은 None."""
    t = prop.get("type")
    if t in ("title", "rich_text"):
        return "".join(x.get("plain_text", "") for x in prop.get(t) or []) or None
    if t == "number":
        return prop.get("number")
    if t == "select":
        sel = prop.get("select")
        return sel.get("name") if sel else None
    if t == "multi_select":
        return ", ".join(x.get("name", "") for x in prop.get("multi_select") or []) or None
    if t == "date":
        d = prop.get("date")
        return d.get("start") if d else None
    if t == "formula":
        f = prop.get("formula") or {}
        ft = f.get("type")
        if ft == "date":
            return (f.get("date") or {}).get("start")
        return f.get(ft) if ft else None
    return None


def parse_ops_pages(pages):
    """`databases/{id}/query` 응답의 `results` 목록 → 정규화 DataFrame."""
    rows = []
    for page in pages:
        props = page.get("properties") or {}
        row = {"NOTION_URL": page.get("url")}
        for prop_name, col in _PROP_MAP.items():
            row[col] = _prop_value(props[prop_name]) if prop_name in props else None
        rows.append(row)
    return _finalize(pd.DataFrame(rows, columns=NOTION_OPS_COLUMNS))


def fetch_ops_table(token, db_id=None, since=None, session=None):
    """노션 운영현황표 전량 조회 (읽기 전용, 페이지네이션).

    Args:
        token: Notion 내부 통합 토큰 (`NOTION_TOKEN`). 운영현황표에 통합이 연결돼 있어야 한다.
        db_id: 데이터베이스 ID. 기본 `config.NOTION_OPS_DB_ID`.
        since: 이 날짜(YYYY-MM-DD) 이후 개강 행만. 기본 `config.NOTION_OPS_SINCE`.

    Raises:
        NotionFetchError: 인증·권한·네트워크 실패. 메시지는 화면 안내용 한글.
    """
    db_id = db_id or config.NOTION_OPS_DB_ID
    since = since or config.NOTION_OPS_SINCE
    http = session or requests
    url = f"{config.NOTION_API_BASE}/databases/{db_id}/query"
    headers = {
        "Authorization": f"Bearer {token}",
        "Notion-Version": config.NOTION_API_VERSION,
        "Content-Type": "application/json",
    }
    body = {
        "page_size": config.NOTION_PAGE_SIZE,
        "filter": {"property": "개강일", "date": {"on_or_after": since}},
        "sorts": [{"property": "개강일", "direction": "descending"}],
    }

    pages, cursor = [], None
    while True:
        payload = dict(body, **({"start_cursor": cursor} if cursor else {}))
        try:
            resp = http.post(url, headers=headers, json=payload, timeout=config.NOTION_TIMEOUT)
        except requests.RequestException as e:
            raise NotionFetchError(f"노션 API 연결 실패: {type(e).__name__}") from e
        if resp.status_code == 401:
            raise NotionFetchError("노션 토큰이 유효하지 않습니다 (401). NOTION_TOKEN을 확인하세요.")
        if resp.status_code in (403, 404):
            raise NotionFetchError(
                "운영현황표를 찾을 수 없거나 통합에 공유되지 않았습니다 "
                f"({resp.status_code}). 노션에서 운영현황표 → 연결(Connections)에 통합을 추가하세요."
            )
        if resp.status_code != 200:
            raise NotionFetchError(f"노션 API 오류 {resp.status_code}: {resp.text[:200]}")
        data = resp.json()
        pages.extend(data.get("results") or [])
        if not data.get("has_more"):
            break
        cursor = data.get("next_cursor")
        if not cursor:
            break
    logger.info(f"노션 운영현황표 {len(pages)}행 조회 (since={since})")
    return parse_ops_pages(pages)


# ── CSV 내보내기 ──────────────────────────────────────────────────────

_KO_DATE = re.compile(r"(\d{4})\s*년\s*(\d{1,2})\s*월\s*(\d{1,2})\s*일")


def parse_notion_date(value):
    """노션 CSV 날짜 → 'YYYY-MM-DD'. 한글('2026년 7월 9일')·ISO·영문 형식 지원, 실패 시 None."""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    s = str(value).strip()
    if not s:
        return None
    m = _KO_DATE.search(s)
    if m:
        y, mo, d = (int(x) for x in m.groups())
        return f"{y:04d}-{mo:02d}-{d:02d}"
    s = s.split("→")[0].strip()  # 기간 표기면 시작일만
    ts = pd.to_datetime(s, errors="coerce")
    return None if pd.isna(ts) else ts.strftime("%Y-%m-%d")


def load_ops_csv(file_like):
    """노션 '내보내기 → CSV' 파일 → 정규화 DataFrame. 알 수 없는 컬럼은 무시."""
    raw = pd.read_csv(file_like, dtype=str)
    raw.columns = [str(c).replace("﻿", "").strip() for c in raw.columns]
    df = pd.DataFrame(index=raw.index)
    df["NOTION_URL"] = None
    for prop_name, col in _PROP_MAP.items():
        df[col] = raw[prop_name] if prop_name in raw.columns else None
    for col in ("개강일", "종강일"):
        df[col] = df[col].map(parse_notion_date)
    return _finalize(df[NOTION_OPS_COLUMNS])


def _finalize(df):
    """숫자 변환 + 수식 값이 비었을 때의 보정 (확정자신고·현재인원·이탈합계)."""
    df = df.copy()
    for col in _NUMERIC_COLS:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    # 수식 컬럼은 API·CSV에 따라 비어 올 수 있어 원본 숫자로 다시 계산해 채운다
    base = df["개강인원"].fillna(0) - df["초기이탈"].fillna(0) + df["추가인원"].fillna(0)
    df["확정자신고"] = df["확정자신고"].fillna(base.where(df["개강인원"].notna()))
    df["현재인원"] = df["현재인원"].fillna(df["확정자신고"] - df["중도이탈"].fillna(0))
    df["이탈합계"] = df["이탈합계"].fillna(df["초기이탈"].fillna(0) + df["중도이탈"].fillna(0))
    df["그룹"] = df["과정명"].map(course_group)
    return df.reset_index(drop=True)


# ── HRD-Net 회차와 대조 ───────────────────────────────────────────────


def _status(sta, end, today):
    sta, end = str(sta or ""), str(end or "")
    if sta and sta > today:
        return "개설예정"
    if end and end < today:
        return "종료"
    return "진행중"


def _pct(numer, denom):
    return (numer / denom.replace(0, pd.NA) * 100).astype("Float64").round(1)


def compare_ops(hrd_df, notion_df, today=None):
    """HRD 회차 집계(`get_course_history_with_fallback` 결과)와 노션 운영현황표를 (그룹, 개강일)로 대조.

    Returns:
        DataFrame — 한 행이 한 기수. 주요 컬럼:
          매칭: '양쪽' / 'HRD만' / '노션만'
          HRD_수강신청 · HRD_확정(totParMks) · 노션_개강인원 · 노션_확정자신고 · 확정_차이(HRD − 노션)
          등록확정전환율 = HRD_확정 / HRD_수강신청 (기존 페이지의 '개강 참석률')
          개강참석률_노션 = 노션_개강인원 / HRD_수강신청 (실제 첫날 출석 기준)
        그룹이 없는 노션 행(SK네트웍스 등 HRD 미추적 과정)은 제외한다.
    """
    today = today or datetime.now().strftime("%Y-%m-%d")

    h = hrd_df.copy() if hrd_df is not None else pd.DataFrame(columns=["TRPR_ID", "TRPR_DEGR", "TRPR_NM",
                                                                       "TR_STA_DT", "TR_END_DT", "TOT_FXNUM",
                                                                       "TOT_TRP_CNT", "TOT_PAR_MKS", "FINI_CNT"])
    h["TRPR_DEGR"] = pd.to_numeric(h["TRPR_DEGR"], errors="coerce").fillna(0).astype(int)
    h = h[h["TRPR_DEGR"] > 0].copy()
    for c in ("TOT_FXNUM", "TOT_TRP_CNT", "TOT_PAR_MKS", "FINI_CNT"):
        h[c] = pd.to_numeric(h[c], errors="coerce")
    h["그룹"] = [config.COURSE_SHORT_NAMES.get(tid) or course_group(nm)
                 for tid, nm in zip(h["TRPR_ID"], h["TRPR_NM"])]
    h["개강일"] = h["TR_STA_DT"].astype(str).str[:10]
    h = h.rename(columns={
        "TR_END_DT": "HRD_종료일", "TOT_FXNUM": "정원", "TOT_TRP_CNT": "HRD_수강신청",
        "TOT_PAR_MKS": "HRD_확정", "FINI_CNT": "HRD_수료",
    })[["그룹", "개강일", "TRPR_ID", "TRPR_DEGR", "HRD_종료일", "정원", "HRD_수강신청", "HRD_확정", "HRD_수료"]]

    n = notion_df.copy() if notion_df is not None else pd.DataFrame(columns=NOTION_OPS_COLUMNS + ["그룹"])
    n = n[n["그룹"].notna() & n["개강일"].notna()].copy()
    n = n.rename(columns={
        "과정명": "노션_과정명", "종강일": "노션_종강일", "개강인원": "노션_개강인원",
        "초기이탈": "노션_초기이탈", "추가인원": "노션_추가인원", "확정자신고": "노션_확정자신고",
        "중도이탈": "노션_중도이탈", "현재인원": "노션_현재인원", "이탈합계": "노션_이탈합계",
    })[["그룹", "개강일", "노션_과정명", "구분", "강의실", "노션_종강일", "노션_개강인원", "노션_초기이탈",
        "노션_추가인원", "노션_확정자신고", "노션_중도이탈", "노션_현재인원", "노션_이탈합계", "NOTION_URL"]]

    # 빈 프레임이면 키 컬럼이 float로 잡혀 merge가 dtype 불일치로 실패한다 → 양쪽 모두 object로 고정
    h = h.astype({"그룹": object, "개강일": object})
    n = n.astype({"그룹": object, "개강일": object})
    m = h.merge(n, on=["그룹", "개강일"], how="outer", indicator=True)
    m["매칭"] = m["_merge"].map({"both": "양쪽", "left_only": "HRD만", "right_only": "노션만"})
    m = m.drop(columns="_merge")

    m["회차"] = m["TRPR_DEGR"].map(lambda d: f"{int(d)}회차" if pd.notna(d) else None)
    m["상태"] = [_status(s, e if pd.notna(e) else e2, today)
                 for s, e, e2 in zip(m["개강일"], m["HRD_종료일"], m["노션_종강일"])]
    m["확정_차이"] = (m["HRD_확정"] - m["노션_확정자신고"]).astype("Float64")
    m["판정"] = m.apply(
        lambda r: "✅ 일치" if r["매칭"] == "양쪽" and pd.notna(r["확정_차이"]) and r["확정_차이"] == 0
        else ("⚠️ 불일치" if r["매칭"] == "양쪽" else "— 한쪽만"), axis=1)
    m["등록확정전환율"] = _pct(m["HRD_확정"], m["HRD_수강신청"])
    m["개강참석률_노션"] = _pct(m["노션_개강인원"], m["HRD_수강신청"])
    m["등록대비개강차이"] = (m["노션_개강인원"] - m["HRD_수강신청"]).astype("Float64")

    order = ["그룹", "회차", "노션_과정명", "매칭", "판정", "상태", "개강일", "HRD_종료일", "노션_종강일",
             "정원", "HRD_수강신청", "노션_개강인원", "노션_초기이탈", "노션_추가인원",
             "노션_확정자신고", "HRD_확정", "확정_차이", "노션_중도이탈", "노션_현재인원", "노션_이탈합계",
             "HRD_수료", "등록확정전환율", "개강참석률_노션", "등록대비개강차이",
             "구분", "강의실", "TRPR_ID", "TRPR_DEGR", "NOTION_URL"]
    return m[order].sort_values(["개강일", "그룹"], ascending=[False, True]).reset_index(drop=True)


def roster_current_counts(trainees_df):
    """HRD 명부 → (TRPR_ID, TRPR_DEGR)별 훈련중 인원. 운영 현황 페이지의 '현재 인원' 정의와 동일.

    재원 = 훈련중만. 수료·조기취업·중도탈락·제적은 제외 (CLAUDE.md '현재 재원').
    """
    from utils import is_completed

    cols = ["TRPR_ID", "TRPR_DEGR", "HRD_훈련중"]
    if trainees_df is None or trainees_df.empty:
        return pd.DataFrame(columns=cols)
    t = trainees_df.copy()
    status = t["TRNEE_STATUS"].astype(str)
    t["_active"] = ~(is_completed(status) | status.str.contains("중도탈락|제적", na=False))
    t["TRPR_DEGR"] = pd.to_numeric(t["TRPR_DEGR"], errors="coerce").fillna(0).astype(int)
    out = t.groupby(["TRPR_ID", "TRPR_DEGR"])["_active"].sum().reset_index()
    return out.rename(columns={"_active": "HRD_훈련중"})[cols]
