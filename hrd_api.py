"""HRD-Net API 직접 호출 모듈 (실시간 운영 현황용).

ETL(hrd_etl.py)과 달리 DB에 쓰지 않고 DataFrame을 반환한다.
API 실패 시 DB 폴백을 제공한다.
"""
import json
import logging
import os
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

import pandas as pd
from dotenv import load_dotenv

import config
from utils import clean_time, get_retry_session, load_data

load_dotenv()

logger = logging.getLogger(__name__)

BASE_URL_COURSE = "https://hrd.work24.go.kr/jsp/HRDP/HRDPO00/HRDPOA60/HRDPOA60_3.jsp"
BASE_URL_DETAIL = "https://hrd.work24.go.kr/jsp/HRDP/HRDPO00/HRDPOA60/HRDPOA60_4.jsp"

COURSE_COLUMNS = ["TRPR_ID", "TRPR_DEGR", "TRPR_NM", "TR_STA_DT", "TR_END_DT",
                  "TOT_FXNUM", "TOT_PAR_MKS", "TOT_TRP_CNT"]
ROSTER_COLUMNS = ["TRPR_ID", "TRPR_DEGR", "TRNEE_ID", "TRNEE_NM", "TRNEE_STATUS"]
ATTEND_COLUMNS = ["TRPR_ID", "TRPR_DEGR", "TRNEE_ID", "ATEND_DT", "IN_TIME",
                  "OUT_TIME", "ATEND_STATUS", "COLLECTED_AT"]


# ── 운영기관 설정 ──────────────────────────────────────────────────────


def _get_secret(name):
    """환경변수 우선, 없으면 Streamlit secrets에서 조회."""
    val = os.getenv(name)
    if val:
        return val
    try:
        import streamlit as st
        return st.secrets.get(name)
    except Exception:
        return None


def get_institutions():
    """운영기관별 (인증키, 과정ID) 쌍 목록.

    명부/출결 API는 인증키가 소속 기관의 과정만 조회하도록 막혀 있어
    (`요청하신 훈련기관에서 운영하는 과정만 조회가 가능합니다`)
    기관마다 키와 과정 ID를 짝지어야 한다. 과정 목록 API는 이 제약이 없다.
    """
    pairs = []
    hanwha_key = _get_secret("HRD_API_KEY")
    hanwha_course = _get_secret("HANWHA_COURSE_ID")
    if hanwha_key and hanwha_course:
        pairs.append((hanwha_key, hanwha_course))

    encore_key = _get_secret("ENCORE_API_KEY")
    if encore_key:
        for cid in (_get_secret("ENCORE_COURSE_IDS") or "").split(","):
            cid = cid.strip()
            if cid:
                pairs.append((encore_key, cid))
    return pairs


# ── 개별 API 함수 ──────────────────────────────────────────────────────


def fetch_course_list(session, api_key, course_id):
    """과정 목록 조회 → DataFrame.

    Returns:
        활성 과정(TR_END_DT >= today)만 필터된 DataFrame.
        컬럼: TRPR_ID, TRPR_DEGR, TRPR_NM, TR_STA_DT, TR_END_DT,
              TOT_FXNUM, TOT_PAR_MKS, TOT_TRP_CNT
    """
    res = session.get(BASE_URL_COURSE, params={
        "returnType": "JSON", "authKey": api_key,
        "srchTrprId": course_id, "outType": "2",
    }, timeout=config.API_TIMEOUT)
    parsed = json.loads(res.json()["returnJSON"])
    if isinstance(parsed, list):
        course_list = parsed
    elif isinstance(parsed, dict):
        # API가 딕셔너리 래퍼로 감쌀 때 내부 리스트 추출 (trneList/atabList 패턴)
        inner = next((v for v in parsed.values() if isinstance(v, list)), None)
        course_list = inner if inner is not None else [parsed]
        logger.info(f"과정 목록 API 응답이 dict 래퍼: keys={list(parsed.keys())}")
    else:
        course_list = []

    rows = []
    today_str = datetime.now().strftime("%Y-%m-%d")
    for c in course_list:
        try:
            degr = int(c.get("trprDegr", 0))
        except (ValueError, TypeError):
            continue
        if degr == 0:
            continue
        end_dt = c.get("trEndDt", "")
        if end_dt < today_str:
            continue
        rows.append({
            "TRPR_ID": c.get("trprId"),
            "TRPR_DEGR": degr,
            "TRPR_NM": c.get("trprNm"),
            "TR_STA_DT": c.get("trStaDt"),
            "TR_END_DT": end_dt,
            "TOT_FXNUM": c.get("totFxnum"),
            "TOT_PAR_MKS": c.get("totParMks"),
            "TOT_TRP_CNT": c.get("totTrpCnt"),
        })
    df = pd.DataFrame(rows, columns=COURSE_COLUMNS)
    if not df.empty:
        df = df.sort_values("TRPR_DEGR", ascending=False).reset_index(drop=True)
    return df


def fetch_trainee_roster(session, api_key, course_id, trpr_degr):
    """기수별 훈련생 명부 → DataFrame.

    컬럼: TRPR_ID, TRPR_DEGR, TRNEE_ID, TRNEE_NM, TRNEE_STATUS
    """
    res = session.get(BASE_URL_DETAIL, params={
        "returnType": "JSON", "authKey": api_key, "outType": "2",
        "srchTrprId": course_id, "srchTrprDegr": trpr_degr,
    }, timeout=config.API_TIMEOUT)
    raw_json = res.json().get("returnJSON")
    if not raw_json:
        return pd.DataFrame(columns=ROSTER_COLUMNS)
    roster_data = json.loads(raw_json)
    trne_list = roster_data if isinstance(roster_data, list) else roster_data.get("trneList", [])
    if not isinstance(trne_list, list):
        trne_list = []

    rows = []
    for t in trne_list:
        if not isinstance(t, dict):
            continue
        rows.append({
            "TRPR_ID": course_id,
            "TRPR_DEGR": trpr_degr,
            "TRNEE_ID": str(t.get("trneeCstmrId")),
            "TRNEE_NM": t.get("trneeCstmrNm"),
            "TRNEE_STATUS": t.get("trneeSttusNm"),
        })
    return pd.DataFrame(rows, columns=ROSTER_COLUMNS)


def fetch_attendance_month(session, api_key, course_id, trpr_degr, yyyymm):
    """기수별 월간 출결 → DataFrame.

    컬럼: TRPR_ID, TRPR_DEGR, TRNEE_ID, ATEND_DT, IN_TIME, OUT_TIME,
          ATEND_STATUS, COLLECTED_AT
    """
    res = session.get(BASE_URL_DETAIL, params={
        "returnType": "JSON", "authKey": api_key, "outType": "2",
        "srchTrprId": course_id, "srchTrprDegr": trpr_degr,
        "srchTorgId": "student_detail", "atendMo": yyyymm,
    }, timeout=config.API_TIMEOUT)
    raw_json = res.json().get("returnJSON")
    if not raw_json:
        return pd.DataFrame(columns=ATTEND_COLUMNS)
    atab_data = json.loads(raw_json)
    atab_list = atab_data if isinstance(atab_data, list) else atab_data.get("atabList", [])
    if not isinstance(atab_list, list):
        atab_list = []

    now_str = datetime.now().isoformat()
    rows = []
    for log in atab_list:
        if not isinstance(log, dict):
            continue
        rows.append({
            "TRPR_ID": course_id,
            "TRPR_DEGR": trpr_degr,
            "TRNEE_ID": str(log.get("trneeCstmrId")),
            "ATEND_DT": log.get("atendDe"),
            "IN_TIME": clean_time(log.get("lpsilTime")),
            "OUT_TIME": clean_time(log.get("levromTime")),
            "ATEND_STATUS": log.get("atendSttusNm"),
            "COLLECTED_AT": now_str,
        })
    return pd.DataFrame(rows, columns=ATTEND_COLUMNS)


# ── 오케스트레이터 ─────────────────────────────────────────────────────


def fetch_active_data_realtime(api_key, course_id):
    """활성 기수의 과정/명부/출결을 병렬 조회.

    Returns:
        (courses_df, trainees_df, logs_df) — 현재 운영 현황 페이지와 동일 컬럼 구조.
    """
    session = get_retry_session(retries=2, backoff_factor=0.5)

    # 1) 과정 목록
    courses_df = fetch_course_list(session, api_key, course_id)
    if courses_df.empty:
        return courses_df, pd.DataFrame(), pd.DataFrame()

    # 2) 활성 기수별 명부 + 당월 출결 병렬 호출
    current_month = datetime.now().strftime("%Y%m")
    futures_roster = []
    futures_attend = []

    with ThreadPoolExecutor(max_workers=config.API_MAX_WORKERS) as executor:
        for degr in courses_df["TRPR_DEGR"].unique():
            futures_roster.append(
                executor.submit(fetch_trainee_roster, session, api_key, course_id, degr)
            )
            futures_attend.append(
                executor.submit(fetch_attendance_month, session, api_key, course_id, degr, current_month)
            )

    roster_dfs = [f.result() for f in futures_roster]
    attend_dfs = [f.result() for f in futures_attend]

    trainees_df = pd.concat(roster_dfs, ignore_index=True) if roster_dfs else pd.DataFrame(columns=ROSTER_COLUMNS)
    logs_df = pd.concat(attend_dfs, ignore_index=True) if attend_dfs else pd.DataFrame(columns=ATTEND_COLUMNS)

    return courses_df, trainees_df, logs_df


def fetch_all_institutions(pairs):
    """기관별 (인증키, 과정ID) 쌍을 순회하며 실시간 데이터를 병합.

    한 과정이 실패해도 나머지는 살린다. 전부 실패해야 예외 → DB 폴백.

    Raises:
        RuntimeError: 모든 쌍이 실패한 경우.
    """
    courses, trainees, logs = [], [], []
    failures = []
    for api_key, course_id in pairs:
        try:
            c_df, t_df, l_df = fetch_active_data_realtime(api_key, course_id)
        except Exception as e:
            logger.warning(f"과정({course_id}) 실시간 조회 실패, 건너뜀: {e}")
            failures.append(course_id)
            continue
        for src, dst in ((c_df, courses), (t_df, trainees), (l_df, logs)):
            if not src.empty:
                dst.append(src)

    if failures and len(failures) == len(pairs):
        raise RuntimeError(f"모든 과정 조회 실패: {failures}")

    def _merge(frames, columns):
        return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=columns)

    return (
        _merge(courses, COURSE_COLUMNS),
        _merge(trainees, ROSTER_COLUMNS),
        _merge(logs, ATTEND_COLUMNS),
    )


# ── 누적 데이터 병합 ───────────────────────────────────────────────────


def get_full_attendance_logs(courses_df, api_logs_df):
    """API의 당월 데이터 + DB의 과거월 데이터를 병합.

    겹치는 날짜는 API 데이터 우선.

    Returns:
        병합된 출결 DataFrame. API/DB 모두 없으면 빈 DataFrame.
    """
    if courses_df.empty:
        return api_logs_df

    course_ids = ",".join(f"'{x}'" for x in courses_df["TRPR_ID"].unique())
    degrs = ",".join(str(x) for x in courses_df["TRPR_DEGR"].unique())

    db_logs = load_data(
        f"SELECT TRPR_ID, TRPR_DEGR, TRNEE_ID, ATEND_DT, IN_TIME, OUT_TIME, "
        f"ATEND_STATUS, COLLECTED_AT "
        f"FROM TB_ATTENDANCE_LOG WHERE TRPR_ID IN ({course_ids}) AND TRPR_DEGR IN ({degrs}) "
        f"ORDER BY ATEND_DT DESC"
    )

    if db_logs.empty:
        return api_logs_df
    if api_logs_df.empty:
        return db_logs

    # DB에서 API 당월 날짜를 제외하고 병합
    combined = pd.concat([db_logs, api_logs_df], ignore_index=True)
    combined = combined.drop_duplicates(
        subset=["TRPR_ID", "TRPR_DEGR", "TRNEE_ID", "ATEND_DT"],
        keep="last",
    )
    return combined.sort_values("ATEND_DT", ascending=False).reset_index(drop=True)


# ── 폴백 래퍼 ──────────────────────────────────────────────────────────


def _get_active_data_from_db():
    """기존 DB 기반 활성 과정 데이터 로딩 (폴백용)."""
    today_str = datetime.now().strftime("%Y-%m-%d")
    active_courses = load_data(
        "SELECT TRPR_ID, TRPR_DEGR, TRPR_NM, TR_STA_DT, TR_END_DT, "
        "TOT_FXNUM, TOT_PAR_MKS, TOT_TRP_CNT "
        "FROM TB_COURSE_MASTER WHERE TR_END_DT >= ? ORDER BY TR_STA_DT",
        params=[today_str],
    )
    if active_courses.empty:
        return None, None, None

    course_ids = ",".join(f"'{x}'" for x in active_courses["TRPR_ID"].unique())
    degrs = ",".join(str(x) for x in active_courses["TRPR_DEGR"].unique())
    active_trainees = load_data(
        f"SELECT TRPR_ID, TRPR_DEGR, TRNEE_ID, TRNEE_NM, TRNEE_STATUS "
        f"FROM TB_TRAINEE_INFO WHERE TRPR_ID IN ({course_ids}) AND TRPR_DEGR IN ({degrs})"
    )
    recent_logs = load_data(
        f"SELECT TRPR_ID, TRPR_DEGR, TRNEE_ID, ATEND_DT, IN_TIME, OUT_TIME, "
        f"ATEND_STATUS, COLLECTED_AT "
        f"FROM TB_ATTENDANCE_LOG WHERE TRPR_ID IN ({course_ids}) AND TRPR_DEGR IN ({degrs}) "
        f"ORDER BY ATEND_DT DESC"
    )
    return active_courses, active_trainees, recent_logs


def get_active_data_with_fallback():
    """API 우선, 실패 시 DB 폴백.

    Returns:
        (courses_df, trainees_df, logs_df, source)
        source: "API" 또는 "DB"
    """
    pairs = get_institutions()
    if not pairs:
        logger.info("API 키/과정 ID 없음 → DB 폴백")
        c, t, l = _get_active_data_from_db()
        return c, t, l, "DB"

    try:
        courses_df, trainees_df, logs_df = fetch_all_institutions(pairs)
        if courses_df.empty:
            return None, None, None, "API"
        return courses_df, trainees_df, logs_df, "API"
    except Exception as e:
        logger.warning(f"API 호출 실패, DB 폴백: {e}")
        c, t, l = _get_active_data_from_db()
        return c, t, l, "DB"
