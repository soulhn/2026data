"""노션 「HRD 등록자 관리」 페이지 발행 — 한 번이라도 HRD에 등록한 사람을 기수별로.

페이지 구조 (2026-09-21, 사용자 설계)
  상단 콜아웃  마지막 갱신 시각 + 읽는 법 세 줄
  「기수」 표   기수 · 개강일 · API 신청인원 · 노션 수집 등록 인원 · 일치 여부 · 개강일 출석 인원 · 개강 참석률(%) · 확정 신고(API) · 확정자 신고율(%) · 승인 인원(현재) · 갱신 시각
                확정 신고(API)는 개강 + 7일 0시(KST) 이후 첫 스냅샷의 totParMks로 고정(2026-09-28). 승인 인원(현재)는 지금 명부 인원 — 둘이 다르면 신고 뒤 명부가 바뀐 것
                + 개강인원(운영현황표, 오프라인) · 초기이탈(노션) · 추가인원(노션) · 확정 신고(노션 기준) = 담당자 운영현황표(회사 노션, 읽기만)의 수기 값을 그대로 옮김 (2026-09-28). 조회 실패 시 그 실행은 두 열을 건드리지 않는다
  └ 기수 페이지  (사용자가 만든 템플릿의 「등록자」 필터 보기) — 등록일 · 개강날 출석 여부
  「등록자」 DB  한 사람 한 줄 (전체 페이지, 기수 표에서 관계로 연결)

누가 '등록자'인가
  노션에서 HRD신청·HRD등록을 한 번이라도 거쳤거나 HRD 신청/등록 일자가 있는 사람  ∪  HRD-Net 명부에 한 번이라도 잡힌 사람.
  노션과 명부는 (기수, 이름 해시)로 맞춘다. 명부에만 있으면 '노션에 없음'으로 표시한다.

원칙: 담당자 DB(신청자 리스트·운영현황표)는 절대 쓰지 않는다. 쓰는 곳은 이 페이지 하나. 실명·연락처는 저장하지 않는다.
(2026-09-15~21의 「모집 KPI」 페이지는 이 페이지로 대체·삭제됐다. 노션 쓰기 공용 헬퍼는 notion_publish.py)

실행: python notion_registry_publish.py   (hrd_etl.yml 하루 2회 + 노션 웹훅 → kpi_poll.yml)
"""
import json
import logging
import os
import time
from datetime import datetime, timedelta, timezone

from dotenv import load_dotenv

import config
from init_db import init_all_tables
from notion_applicants_etl import get_sync_state, set_sync_state
from notion_publish import (
    _num, _request, _rt, content_hash, create_database, ensure_database_layout, ensure_properties, get_database,
    publish, remove_properties,
)
from notion_ops import NotionFetchError, cohort_number, course_group, fetch_ops_table
from utils import _clean_secret, adapt_query, get_connection, load_data

load_dotenv()
logger = logging.getLogger(__name__)
logging.basicConfig(format="%(asctime)s %(levelname)s %(name)s %(message)s", level=logging.INFO)

COHORT_DB_KEY = "notion_reg_cohort_db"
PERSON_DB_KEY = "notion_reg_person_db"
UPDATED_BLOCK_KEY = "notion_reg_updated_block"
GUIDE_BLOCK_KEY = "notion_reg_guide_block"
GUIDE_HASH_KEY = "notion_reg_guide_hash"     # 읽는 법 문구 해시 — 바뀐 실행에서만 콜아웃을 다시 쓴다
COHORT_PUB = "reg_cohort"
PERSON_PUB = "reg_person"
BODY_PUB = "reg_body"            # 기수 페이지 본문(명단 표) — ROW_KEY 기수, NOTION_PAGE_ID 에 블록 ID 목록(JSON)
BODY_BLOCK_TYPES = ("table", "paragraph")   # 우리가 본문에 만드는 블록 종류 — 이것 외에는 절대 보관 처리하지 않는다
BODY_COLUMNS = ("이름", "등록일", "개강날 출석", "노션 상태", "HRD 승인")
_ATTEND_ORDER = {"출석": 0, "미출석": 1, "개강 전": 2}
REMOVED = {COHORT_PUB: (), PERSON_PUB: ()}   # 스키마에서 뺀 속성 — 기존 DB에 남아 있으면 지운다

KST = timezone(timedelta(hours=9))

COHORT_SCHEMA = {
    "기수": {"title": {}},
    "과정": {"select": {"options": [{"name": c} for c in config.NOTION_KPI_COURSES]}},
    "상태": {"select": {"options": [{"name": "개설예정"}, {"name": "진행중"}, {"name": "종료"}]}},
    "개강일": {"date": {}},
    "API 신청인원": _num(),
    "노션 수집 등록 인원": _num(),
    "일치 여부": {"select": {"options": [{"name": "일치"}, {"name": "불일치"}, {"name": "미확인"}]}},
    "개강일 출석 인원": _num(),
    "개강 참석률(%)": _num(),
    "확정 신고(API)": _num(),
    "확정자 신고율(%)": _num(),
    "승인 인원(현재)": _num(),
    "개강인원(운영현황표, 오프라인)": _num(),
    "확정 신고(노션 기준)": _num(),
    "초기이탈(노션)": _num(),
    "추가인원(노션)": _num(),
    "갱신 시각": {"date": {}},
}
COHORT_DESC = "기수 하나가 한 줄. API 신청인원 = HRD-Net에 한 번이라도 수강신청한 사람(누적). 노션 수집 등록 인원과 같아야 정상. 기수를 열면 등록자 명단"

PERSON_SCHEMA = {
    "이름": {"title": {}},
    "기수": {"relation": {"database_id": None, "dual_property": {}}},   # database_id는 생성 시 채운다 (기수 DB)
    "등록일": {"date": {}},
    "개강날 출석": {"select": {"options": [{"name": "출석"}, {"name": "미출석"}, {"name": "개강 전"}, {"name": "기록 없음"}]}},
    "노션 상태": {"rich_text": {}},
    "HRD 승인": {"checkbox": {}},
    "원본 링크": {"url": {}},   # 신청자 리스트 원본 페이지. 관계 대신 URL — SKN DB는 데이터 소스가 둘이라 2022 API로 관계를 못 건다
    "갱신 시각": {"date": {}},
    "메모": {"rich_text": {}},   # 담당자 입력 — 파이프라인은 절대 쓰지 않는다
}
PERSON_DESC = "한 번이라도 HRD에 등록한 사람 한 명이 한 줄 (노션 HRD신청·HRD등록 이력 ∪ HRD 명부). 이름은 가려서 저장. '메모'는 담당자 열"

GUIDE_TITLE = "읽는 법 — 열별 수집 기준"
GUIDE_LINES = [
    "보는 기준 — 2026-10-07 개강 MLO3부터가 자동 수집 이후 기수(신청·등록·취소가 HRD 일자와 함께 기록됨). 그 전 기수는 취소자에게 HRD 일자가 없어 "
    "노션 수집 인원에 확정자만 남고 '불일치'가 정상. 두 그룹의 일치 여부·등록 인원은 서로 비교하지 않는다. API 값(신청·확정·출석)은 기수 구분 없이 같은 품질",
    "기수 · 과정 · 상태 · 개강일 = HRD-Net 훈련일정 상세 API. 상태는 개강일·종료일을 오늘과 비교(개설예정 · 진행중 · 종료)",
    "API 신청인원 = totTrpCnt. 한 번이라도 수강신청한 사람의 누적 수라 취소자 포함. 최신 스냅샷 값",
    "노션 수집 등록 인원 = 신청자 리스트(AI·SKN)에서 최종결과가 HRD신청·HRD등록을 한 번이라도 거쳤거나 HRD 신청/등록 일자가 있는 사람 수. 취소자도 일자가 있으면 포함",
    "일치 여부 = API 신청인원과 노션 수집 등록 인원이 같으면 일치. 다르면 HRD-Net에는 있는데 노션에 일자 없이 빠진 사람이 있는 것(대부분 수집 전 기수의 취소자)",
    "개강일 출석 인원 = HRD-Net 출결 API에서 개강 당일 입실 기록이 있는 명부 인원. 개강 다음 날부터 값이 생김. 개강 참석률(%) = 개강일 출석 ÷ API 신청인원 × 100",
    "확정 신고(API) = 개강 + 7일 0시(KST) 이후 첫 스냅샷의 HRD-Net 승인 명부 인원(totParMks). 그 뒤 명부가 바뀌어도 이 값은 고정 (2026-09-28 규칙). "
    "추적 시작(2026-09-15) 전에 +7일이 지난 기수는 첫 스냅샷 값. 확정자 신고율(%) = 확정 신고 ÷ API 신청인원 × 100",
    "승인 인원(현재) = 지금 HRD-Net 승인 명부에 있는 사람 수(totParMks 최신값). 개강 전에도 값이 있고, 확정 신고(API)와 다르면 신고 뒤 승인 취소·추가가 있었다는 뜻",
    "개강인원(운영현황표, 오프라인) · 초기이탈(노션) · 추가인원(노션) · 확정 신고(노션 기준) = 담당자가 「운영현황」 페이지의 운영현황표에 손으로 적은 값을 그대로 옮김(읽기만, 하루 2회). 개강인원은 첫날 강의실에 온 사람 수라 HRD 미등록자도 포함돼 API 개강일 출석보다 크다. 초기이탈·추가인원은 개강~확정자 신고 사이의 이탈·추가. 확정 신고(노션 기준) = 개강인원 − 초기이탈 + 추가인원. "
    "운영현황표에 그 기수가 없거나 비어 있으면 빈칸",
    "갱신 시각 = 그 행을 마지막으로 다시 쓴 시각. 값이 안 바뀐 행은 건너뛰므로 오래돼 보여도 정상. 전체 실행 시각은 위 '마지막 갱신' 콜아웃",
    "등록자 DB — 이름: 가린 이름 · 기수 / 등록일: 노션 HRD 신청 일시(없으면 등록 일자, 그것도 없으면 명부에 처음 보인 날) / "
    "개강날 출석: 출석 · 미출석 · 개강 전 · 기록 없음(그 기수 출결을 못 읽음) / 노션 상태: 신청자 리스트 최종결과, '노션에 없음'은 명부에만 있는 사람 / "
    "HRD 승인: 지금 HRD-Net 명부에 있음(중도탈락도 명부에 남아 ✓) / 원본 링크: 신청자 리스트 페이지 / 메모: 담당자 열, 파이프라인은 쓰지 않음",
]

HRD_STATUSES = ("HRD신청", "HRD등록")


# ── 조회 ─────────────────────────────────────────────────────────────


def _s(v):
    return None if v is None or str(v) in ("", "None", "nan", "NaT") else str(v)


def _i(v):
    return None if v is None or str(v) in ("", "None", "nan") else int(float(v))


def _status(sta, end, today):
    sta, end = _s(sta) or "", _s(end) or ""
    if sta and sta > today:
        return "개설예정"
    if end and end < today:
        return "종료"
    return "진행중"


def cohort_key(trpr_id, degr):
    short = config.COURSE_SHORT_NAMES.get(trpr_id)
    return f"{short}{int(degr)}" if short else None


def ever_registered_pages():
    """노션 기준 '한 번이라도 HRD 등록' 페이지 ID 집합 — 현재 상태 ∪ 전이 로그 ∪ 신청/등록 일자."""
    now = load_data("SELECT NOTION_PAGE_ID FROM TB_APPLICANT WHERE STATUS IN ('HRD신청', 'HRD등록') "
                    "OR HRD_APPLY_AT IS NOT NULL OR HRD_REG_AT IS NOT NULL")
    logged = load_data("SELECT DISTINCT NOTION_PAGE_ID FROM TB_APPLICANT_STATUS_LOG WHERE FIELD = 'STATUS' "
                       "AND (NEW_VALUE IN ('HRD신청', 'HRD등록') OR OLD_VALUE IN ('HRD신청', 'HRD등록'))")
    return set(now["NOTION_PAGE_ID"]) | set(logged["NOTION_PAGE_ID"])


def load_snapshots():
    """회차별 최신 스냅샷 (KPI 과정만)."""
    snap = load_data("""
        SELECT s.* FROM TB_COURSE_SNAPSHOT s
        JOIN (SELECT TRPR_ID, TRPR_DEGR, MAX(SNAP_AT) AS MAX_AT FROM TB_COURSE_SNAPSHOT GROUP BY TRPR_ID, TRPR_DEGR) m
          ON m.TRPR_ID = s.TRPR_ID AND m.TRPR_DEGR = s.TRPR_DEGR AND m.MAX_AT = s.SNAP_AT""")
    snap = snap[snap["TRPR_ID"].map(lambda t: config.COURSE_SHORT_NAMES.get(t) in config.NOTION_KPI_COURSES)]
    return snap[snap["TR_STA_DT"].astype(str).str[:10] >= config.NOTION_REGISTRY_SINCE]


def load_confirmed_at_due():
    """회차별 '확정 신고' 고정값 — 개강 + NOTION_KPI_CONFIRM_DAYS일 0시(KST) 이후 첫 스냅샷의 totParMks.
    그 뒤 명부가 바뀌어도 움직이지 않는다. 추적 시작(첫 스냅샷) 전에 +7일이 지난 회차는 첫 스냅샷 값이 된다.
    아직 그 시점 스냅샷이 없는 회차는 없음. 반환: {(TRPR_ID, DEGR): (값, 스냅샷 날짜 KST)}"""
    snap = load_data("SELECT TRPR_ID, TRPR_DEGR, SNAP_AT, TR_STA_DT, TOT_PAR_MKS FROM TB_COURSE_SNAPSHOT ORDER BY SNAP_AT")
    out = {}
    for s in snap.itertuples(index=False):
        key = (s.TRPR_ID, int(s.TRPR_DEGR))
        start = _s(s.TR_STA_DT)
        if key in out or not start or _i(s.TOT_PAR_MKS) is None:
            continue
        cutoff = datetime.fromisoformat(start[:10]) + timedelta(days=config.NOTION_KPI_CONFIRM_DAYS) - timedelta(hours=9)   # KST 0시 → UTC
        snap_at = datetime.fromisoformat(str(s.SNAP_AT)[:19])
        if snap_at >= cutoff:
            out[key] = (_i(s.TOT_PAR_MKS), (snap_at + timedelta(hours=9)).strftime("%Y-%m-%d"))
    return out


OPS_OPEN_COL = "개강인원(운영현황표, 오프라인)"
OPS_CONFIRM_COL = "확정 신고(노션 기준)"
OPS_EARLY_COL = "초기이탈(노션)"
OPS_ADDED_COL = "추가인원(노션)"


def load_ops_counts(token, session=None):
    """담당자 운영현황표(회사 노션, 읽기만) → {기수 키: {"개강인원": n, "확정자신고": n}}.
    기수 키는 과정명의 과정 그룹 + 'N기'(health_check의 대조와 같은 규칙). 그룹이나 기수 번호를 못 읽는 행은 버린다."""
    ops = fetch_ops_table(token, session=session)
    out = {}
    for r in ops.itertuples(index=False):
        g, n = course_group(r.과정명), cohort_number(r.과정명)
        if g and n:
            out[f"{g}{n}"] = {"개강인원": _i(r.개강인원), "확정자신고": _i(r.확정자신고), "초기이탈": _i(r.초기이탈), "추가인원": _i(r.추가인원)}
    return out


def attend_verdict(first_attend_dt, start, today, known=True):
    """개강날 출석 여부. 개강일에 입실했으면 출석, 개강이 지났으면 미출석, 아니면 개강 전.
    known=False(그 회차 출결을 한 번도 못 읽음)이면 개강이 지났어도 '기록 없음'."""
    start = (_s(start) or "")[:10]
    if first_attend_dt and str(first_attend_dt)[:8] == start.replace("-", ""):
        return "출석"
    if not (start and start < today):
        return "개강 전"
    return "미출석" if known else "기록 없음"


def build_cohort_rows(today=None, ops=None):
    """기수 표 행. ops = load_ops_counts() 결과. None이면(조회 실패·점검 호출) 운영현황표 두 열을 행에 넣지 않아 노션 값을 건드리지 않는다."""
    today = today or datetime.now(KST).strftime("%Y-%m-%d")
    snap = load_snapshots()
    frozen = load_confirmed_at_due()
    roster = load_data("""
        SELECT TRPR_ID, TRPR_DEGR,
               SUM(CASE WHEN FIRST_ATTEND_DT IS NOT NULL AND FIRST_ATTEND_DT = REPLACE(TR_STA_DT, '-', '') THEN 1 ELSE 0 END) AS DAY1_CNT,
               SUM(CASE WHEN FIRST_ATTEND_DT IS NOT NULL OR DAY1_STATUS IS NOT NULL THEN 1 ELSE 0 END) AS ATT_KNOWN
        FROM TB_ROSTER_MEMBER GROUP BY TRPR_ID, TRPR_DEGR""")
    # 출결을 한 번도 못 읽은 회차(추적 시작 전 종료)는 0이 아니라 '모름'
    day1_map = {(r.TRPR_ID, int(r.TRPR_DEGR)): (_i(r.DAY1_CNT) if _i(r.ATT_KNOWN) else None) for r in roster.itertuples(index=False)}
    ever = ever_registered_pages()
    apps = load_data("SELECT NOTION_PAGE_ID, COHORT FROM TB_APPLICANT WHERE COHORT IS NOT NULL")
    collected = apps[apps["NOTION_PAGE_ID"].isin(ever)].groupby("COHORT").size().to_dict()
    has_source = set(apps["COHORT"])

    rows = []
    for s in snap.itertuples(index=False):
        key = cohort_key(s.TRPR_ID, s.TRPR_DEGR)
        if not key:
            continue
        start = _s(s.TR_STA_DT)
        applied = _i(s.TOT_TRP_CNT)
        notion = collected.get(key, 0) if key in has_source or collected.get(key) else None
        day1 = day1_map.get((s.TRPR_ID, int(s.TRPR_DEGR)))
        started = bool(start) and start[:10] < today
        approved_now = _i(s.TOT_PAR_MKS)
        confirm_due = bool(start) and (datetime.fromisoformat(start[:10]) + timedelta(days=config.NOTION_KPI_CONFIRM_DAYS)).strftime("%Y-%m-%d") <= today
        confirmed = frozen.get((s.TRPR_ID, int(s.TRPR_DEGR)), (None, None))[0] if confirm_due else None
        rows.append({
            "KEY": key, "기수": key, "과정": config.COURSE_SHORT_NAMES[s.TRPR_ID],
            "상태": _status(start, s.TR_END_DT, today), "개강일": start,
            "API 신청인원": applied, "노션 수집 등록 인원": notion,
            "일치 여부": "미확인" if applied is None or notion is None else ("일치" if applied == notion else "불일치"),
            "개강일 출석 인원": day1 if started else None,
            "개강 참석률(%)": round(day1 / applied * 100, 1) if started and day1 is not None and applied else None,
            "확정 신고(API)": confirmed,
            "확정자 신고율(%)": round(confirmed / applied * 100, 1) if confirmed is not None and applied else None,
            "승인 인원(현재)": approved_now,
            "갱신 시각": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        })
        if ops is not None:
            o = ops.get(key) or {}
            rows[-1][OPS_OPEN_COL] = o.get("개강인원")
            rows[-1][OPS_CONFIRM_COL] = o.get("확정자신고")
            rows[-1][OPS_EARLY_COL] = o.get("초기이탈")
            rows[-1][OPS_ADDED_COL] = o.get("추가인원")
    return rows


def build_person_rows(cohort_pages, today=None):
    """등록자 행. cohort_pages: 기수 키 → 노션 기수 페이지 ID (관계 연결용)."""
    today = today or datetime.now(KST).strftime("%Y-%m-%d")
    ever = ever_registered_pages()
    apps = load_data("SELECT NOTION_PAGE_ID, NOTION_URL, SOURCE_KEY, NAME_HASH, NAME_MASKED, COHORT, STATUS, HRD_APPLY_AT, HRD_REG_AT "
                     "FROM TB_APPLICANT WHERE COHORT IS NOT NULL")
    apps = apps[apps["NOTION_PAGE_ID"].isin(ever)]
    members = load_data("SELECT TRPR_ID, TRPR_DEGR, TRNEE_ID, NAME_HASH, NAME_MASKED, STATUS, FIRST_SEEN_AT, GONE_AT, "
                        "FIRST_ATTEND_DT, DAY1_STATUS, TR_STA_DT FROM TB_ROSTER_MEMBER")
    members = members[members["TRPR_ID"].map(lambda t: config.COURSE_SHORT_NAMES.get(t) in config.NOTION_KPI_COURSES)
                      & (members["TR_STA_DT"].astype(str).str[:10] >= config.NOTION_REGISTRY_SINCE)]
    tracking_start = str(members["FIRST_SEEN_AT"].min())[:10] if not members.empty else None
    # 회차별로 출결을 읽은 적이 있나 — 없으면 그 회차 사람은 '미출석'이 아니라 '기록 없음'
    known_rounds = set()
    for m in members.itertuples(index=False):
        if _s(m.FIRST_ATTEND_DT) or _s(getattr(m, "DAY1_STATUS", None)):
            known_rounds.add((m.TRPR_ID, int(m.TRPR_DEGR)))
    by_key, used = {}, set()
    for m in members.itertuples(index=False):
        by_key.setdefault((cohort_key(m.TRPR_ID, m.TRPR_DEGR), m.NAME_HASH), []).append(m)
    now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    rows = []
    for a in apps.itertuples(index=False):
        if a.COHORT not in cohort_pages:
            continue
        matches = by_key.get((a.COHORT, a.NAME_HASH), [])
        m = next((x for x in matches if not _s(x.GONE_AT)), matches[0] if matches else None)
        for x in matches:
            used.add((x.TRPR_ID, int(x.TRPR_DEGR), str(x.TRNEE_ID)))
        reg_dt = (_s(a.HRD_APPLY_AT) or _s(a.HRD_REG_AT) or "")[:10] or None
        if not reg_dt and m is not None and tracking_start and str(m.FIRST_SEEN_AT)[:10] > tracking_start:
            reg_dt = str(m.FIRST_SEEN_AT)[:10]      # 노션에 날짜가 없으면 명부에 처음 보인 날 (추적 시작 이후만)
        rows.append({
            "KEY": a.NOTION_PAGE_ID, "이름": f"{a.NAME_MASKED or '?'} · {a.COHORT}", "기수": cohort_pages[a.COHORT],
            "등록일": reg_dt,
            "개강날 출석": attend_verdict(m.FIRST_ATTEND_DT if m is not None else None,
                                     m.TR_STA_DT if m is not None else _start_of(a.COHORT), today,
                                     known=(m is None) or ((m.TRPR_ID, int(m.TRPR_DEGR)) in known_rounds)),
            "노션 상태": a.STATUS, "HRD 승인": bool(m is not None and not _s(m.GONE_AT)),
            "원본 링크": _s(a.NOTION_URL),
            "갱신 시각": now_iso,
        })
    # 명부에만 있는 사람 — 노션 신청자 리스트에서 못 찾음
    for m in members.itertuples(index=False):
        key = cohort_key(m.TRPR_ID, m.TRPR_DEGR)
        if key not in cohort_pages or (m.TRPR_ID, int(m.TRPR_DEGR), str(m.TRNEE_ID)) in used:
            continue
        rows.append({
            "KEY": f"roster:{m.TRPR_ID}:{int(m.TRPR_DEGR)}:{m.TRNEE_ID}", "이름": f"{m.NAME_MASKED or '?'} · {key}",
            "기수": cohort_pages[key],
            "등록일": str(m.FIRST_SEEN_AT)[:10] if tracking_start and str(m.FIRST_SEEN_AT)[:10] > tracking_start else None,
            "개강날 출석": attend_verdict(m.FIRST_ATTEND_DT, m.TR_STA_DT, today, known=(m.TRPR_ID, int(m.TRPR_DEGR)) in known_rounds),
            "노션 상태": "노션에 없음", "HRD 승인": not _s(m.GONE_AT),
            "원본 링크": None, "갱신 시각": now_iso,
        })
    return rows


_START_CACHE = {}


def _start_of(cohort):
    if not _START_CACHE:
        for s in load_snapshots().itertuples(index=False):
            k = cohort_key(s.TRPR_ID, s.TRPR_DEGR)
            if k:
                _START_CACHE[k] = _s(s.TR_STA_DT)
    return _START_CACHE.get(cohort)


# ── 페이지·DB 준비 ────────────────────────────────────────────────────


def ensure_page(token, conn, session=None):
    """상단 '마지막 갱신' 콜아웃과 읽는 법 콜아웃을 한 번 만들고 ID를 저장. 반환: 갱신 콜아웃 블록 ID."""
    page = config.NOTION_REGISTRY_PAGE_ID
    upd = get_sync_state(conn, UPDATED_BLOCK_KEY)
    if not upd:
        res = _request(token, "PATCH", f"/blocks/{page}/children", {"children": [
            {"type": "callout", "callout": {"rich_text": _rt("마지막 갱신: 아직 없음"), "icon": {"type": "emoji", "emoji": "🕒"}}},
            {"type": "callout", "callout": {"rich_text": _rt(GUIDE_TITLE), "icon": {"type": "emoji", "emoji": "📖"},
                                            "children": [{"type": "bulleted_list_item", "bulleted_list_item": {"rich_text": _rt(t)}} for t in GUIDE_LINES]}},
        ]}, session)
        ids = [b["id"] for b in res.get("results", [])]
        upd = ids[0]
        set_sync_state(conn, upd, UPDATED_BLOCK_KEY)
        if len(ids) > 1:
            set_sync_state(conn, ids[1], GUIDE_BLOCK_KEY)
        logger.info("[등록자 발행] 페이지 상단 콜아웃 생성")
    return upd


def ensure_guide(token, conn, session=None):
    """읽는 법 콜아웃의 글머리 항목을 GUIDE_LINES와 맞춘다 — 문구가 바뀐 실행에서만 기존 항목을 보관하고 다시 쓴다."""
    block = get_sync_state(conn, GUIDE_BLOCK_KEY)
    if not block:
        return
    h = content_hash({"title": GUIDE_TITLE, "lines": list(GUIDE_LINES)})
    if get_sync_state(conn, GUIDE_HASH_KEY) == h:
        return
    res = _request(token, "GET", f"/blocks/{block}/children?page_size=100", None, session)
    for child in res.get("results", []):
        if child.get("type") == "bulleted_list_item" and not child.get("archived"):
            _request(token, "PATCH", f"/blocks/{child['id']}", {"archived": True}, session)
    _request(token, "PATCH", f"/blocks/{block}", {"callout": {"rich_text": _rt(GUIDE_TITLE)}}, session)
    _request(token, "PATCH", f"/blocks/{block}/children",
             {"children": [{"type": "bulleted_list_item", "bulleted_list_item": {"rich_text": _rt(t)}} for t in GUIDE_LINES]}, session)
    set_sync_state(conn, h, GUIDE_HASH_KEY)
    logger.info("[등록자 발행] 읽는 법 콜아웃 갱신")


def touch_updated(token, block_id, text, session=None):
    _request(token, "PATCH", f"/blocks/{block_id}", {"callout": {"rich_text": _rt(text)}}, session)


def ensure_databases(token, conn, session=None):
    """기수 DB → 등록자 DB(기수 관계 양방향) 순으로 만들고, 기수 DB의 자동 생성 관계 이름을 '등록자'로. 반환: (cohort_db, person_db)."""
    page = config.NOTION_REGISTRY_PAGE_ID
    cohort_db = get_sync_state(conn, COHORT_DB_KEY)
    meta = get_database(token, cohort_db, session) if cohort_db else None
    if meta is None:
        cohort_db = create_database(token, page, "기수", COHORT_SCHEMA, session)
        set_sync_state(conn, cohort_db, COHORT_DB_KEY)
        _clear_pub(conn, COHORT_PUB)
        logger.info(f"[등록자 발행] 기수 DB 생성 {cohort_db}")
        meta = {}
    ensure_database_layout(token, cohort_db, COHORT_DESC, meta, session)
    ensure_properties(token, cohort_db, COHORT_SCHEMA, meta or get_database(token, cohort_db, session), session)
    remove_properties(token, cohort_db, REMOVED[COHORT_PUB], meta, session)

    person_db = get_sync_state(conn, PERSON_DB_KEY)
    pmeta = get_database(token, person_db, session) if person_db else None
    schema = dict(PERSON_SCHEMA)
    schema["기수"] = {"relation": {"database_id": cohort_db, "dual_property": {}}}
    if pmeta is None:
        person_db = create_database(token, page, "등록자", schema, session)
        set_sync_state(conn, person_db, PERSON_DB_KEY)
        _clear_pub(conn, PERSON_PUB)
        logger.info(f"[등록자 발행] 등록자 DB 생성 {person_db}")
        # 전체 페이지 DB로 두되 설명은 단다 (기수 표만 인라인). 기수 DB에 자동 생긴 관계 속성을 '등록자'로 이름 변경
        _request(token, "PATCH", f"/databases/{person_db}",
                 {"description": [{"type": "text", "text": {"content": PERSON_DESC}}]}, session)
        cmeta = get_database(token, cohort_db, session) or {}
        for name, prop in (cmeta.get("properties") or {}).items():
            if prop.get("type") == "relation" and name != "등록자":
                _request(token, "PATCH", f"/databases/{cohort_db}", {"properties": {name: {"name": "등록자"}}}, session)
                break
        pmeta = {}
    else:
        ensure_properties(token, person_db, schema, pmeta, session)
        remove_properties(token, person_db, REMOVED[PERSON_PUB], pmeta, session)
    return cohort_db, person_db


def _clear_pub(conn, db_key):
    cur = conn.cursor()
    cur.execute(adapt_query("DELETE FROM TB_NOTION_PUBLISH WHERE DB_KEY = ?"), [db_key])
    conn.commit()


def cohort_page_map(conn):
    cur = conn.cursor()
    cur.execute(adapt_query("SELECT ROW_KEY, NOTION_PAGE_ID FROM TB_NOTION_PUBLISH WHERE DB_KEY = ?"), [COHORT_PUB])
    return {r[0]: r[1] for r in cur.fetchall()}


# ── 기수 페이지 본문: 명단 표 (자동) ──────────────────────────────────
# 노션 API는 '필터된 보기'를 못 만들어 기수 페이지 안에 고정 표를 직접 쓴다. 명단·값이 바뀐 기수만 다시 쓴다.
# 갱신 시각은 표에 넣지 않는다 — 페이지 상단 속성 '갱신 시각'이 이미 보이고, 넣으면 매 실행 22개 표를 전부 다시 써야 한다.


def body_rows(person_rows, cohort_page_id):
    rows = [r for r in person_rows if r["기수"] == cohort_page_id]
    rows.sort(key=lambda r: (_ATTEND_ORDER.get(r["개강날 출석"], 9), r["등록일"] or "9999", r["이름"]))
    return [[r["이름"].split(" · ")[0], r["등록일"] or "", r["개강날 출석"] or "", r["노션 상태"] or "", "✓" if r["HRD 승인"] else ""]
            for r in rows]


def _table_row(cells):
    return {"type": "table_row", "table_row": {"cells": [_rt(str(c)) for c in cells]}}


def body_blocks(rows, person_db):
    """명단 표 + 안내 문단. 표 행이 90개를 넘으면 (호출당 블록 상한) 뒤는 따로 붙인다 → (첫 요청 블록들, 남은 행들)."""
    head = [list(BODY_COLUMNS)] + rows
    first, rest = head[:90], head[90:]
    table = {"type": "table", "table": {"table_width": len(BODY_COLUMNS), "has_column_header": True, "has_row_header": False,
                                        "children": [_table_row(r) for r in first]}}
    note = {"type": "paragraph", "paragraph": {"rich_text": [
        {"type": "text", "text": {"content": f"총 {len(rows)}명 · 자동 갱신 표(정렬·필터 불가). 메모·정렬은 "}},
        {"type": "mention", "mention": {"type": "database", "database": {"id": person_db}}},
        {"type": "text", "text": {"content": " 에서 이 기수로 필터해서."}}]}}
    return [table, note], rest


def _archive_body_blocks(token, ids, session=None):
    for block_id in ids:
        try:
            block = _request(token, "GET", f"/blocks/{block_id}", None, session)
        except NotionFetchError as e:
            if "404" in str(e):
                continue
            raise
        if block.get("type") in BODY_BLOCK_TYPES and not block.get("archived"):
            _request(token, "PATCH", f"/blocks/{block_id}", {"archived": True}, session)
        elif block.get("type") not in BODY_BLOCK_TYPES:
            logger.warning(f"[등록자 발행] 본문 기록에 {block.get('type')} 블록이 있어 건너뜀: {block_id}")


def publish_cohort_bodies(token, conn, cohort_pages, person_rows, person_db, session=None):
    """기수 페이지마다 명단 표를 쓴다. 반환: (다시 쓴 기수 수, 건너뛴 기수 수)."""
    cur = conn.cursor()
    cur.execute(adapt_query("SELECT ROW_KEY, NOTION_PAGE_ID, CONTENT_HASH FROM TB_NOTION_PUBLISH WHERE DB_KEY = ?"), [BODY_PUB])
    known = {r[0]: (r[1], r[2]) for r in cur.fetchall()}
    upsert = adapt_query(
        "INSERT INTO TB_NOTION_PUBLISH (DB_KEY, ROW_KEY, NOTION_PAGE_ID, CONTENT_HASH, UPDATED_AT) VALUES (?, ?, ?, ?, ?) "
        "ON CONFLICT(DB_KEY, ROW_KEY) DO UPDATE SET NOTION_PAGE_ID = excluded.NOTION_PAGE_ID, "
        "CONTENT_HASH = excluded.CONTENT_HASH, UPDATED_AT = excluded.UPDATED_AT")
    written = skipped = 0
    for cohort, page_id in cohort_pages.items():
        rows = body_rows(person_rows, page_id)
        h = content_hash({"KEY": cohort, "rows": rows})
        prev_ids, prev_hash = known.get(cohort, (None, None))
        if prev_hash == h:
            skipped += 1
            continue
        if prev_ids:
            _archive_body_blocks(token, json.loads(prev_ids), session)
        blocks, rest = body_blocks(rows, person_db)
        res = _request(token, "PATCH", f"/blocks/{page_id}/children", {"children": blocks}, session)
        ids, want = [], [b["type"] for b in blocks]
        for r in res.get("results", []):                     # `after` 없이 붙여도 같은 규칙으로 우리 블록만 집는다
            if len(ids) < len(want) and r.get("type") == want[len(ids)]:
                ids.append(r["id"])
        for i in range(0, len(rest), 90):
            _request(token, "PATCH", f"/blocks/{ids[0]}/children", {"children": [_table_row(r) for r in rest[i:i + 90]]}, session)
        cur.execute(upsert, [BODY_PUB, cohort, json.dumps(ids), h, datetime.now(timezone.utc).replace(tzinfo=None)])
        conn.commit()
        written += 1
    return written, skipped


# ── 실행 ─────────────────────────────────────────────────────────────


def main():
    token = _clean_secret(os.getenv("NOTION_TOKEN"))
    if not token:
        logger.warning("NOTION_TOKEN 없음 — 등록자 페이지 발행을 건너뜁니다")
        return
    t0 = time.monotonic()
    init_all_tables(include_market=False)
    conn = get_connection(timeout=30)
    try:
        upd_block = ensure_page(token, conn)
        ensure_guide(token, conn)
        cohort_db, person_db = ensure_databases(token, conn)
        try:
            ops = load_ops_counts(token)
        except NotionFetchError as e:
            logger.warning(f"[등록자 발행] 운영현황표 조회 실패 — 이번 실행은 수기 두 열을 건드리지 않음: {e}")
            ops = None
        c = publish(token, conn, COHORT_PUB, cohort_db, COHORT_SCHEMA, build_cohort_rows(ops=ops))
        pages = cohort_page_map(conn)
        persons = build_person_rows(pages)
        p = publish(token, conn, PERSON_PUB, person_db, {**PERSON_SCHEMA, "기수": {"relation": {}}}, persons)
        b = publish_cohort_bodies(token, conn, pages, persons, person_db)
        stamp = datetime.now(KST).strftime("%Y-%m-%d %H:%M")
        touch_updated(token, upd_block, f"마지막 갱신: {stamp} (KST) · 기수 {len(pages)}개 · 등록자 {sum(p)}명 — 하루 2회 + 노션 HRD등록 변경 즉시")
        set_sync_state(conn, datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"), "notion_registry_last_publish")   # 점검용
    except NotionFetchError as e:
        logger.error(f"[등록자 발행] {e}")
        return
    finally:
        conn.close()
    logger.info(f"[등록자 발행] 기수 생성 {c[0]} · 갱신 {c[1]} · 건너뜀 {c[2]} | 등록자 생성 {p[0]} · 갱신 {p[1]} · 건너뜀 {p[2]} | "
                f"기수 명단 표 다시 씀 {b[0]} · 건너뜀 {b[1]} ({time.monotonic() - t0:.1f}s)")


if __name__ == "__main__":
    main()
