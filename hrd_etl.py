import sqlite3
import json
import logging
import os
import time
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from dotenv import load_dotenv
import requests

from utils import (
    get_connection, get_retry_session, adapt_query, is_pg, load_data,
    calc_attendance_rate, NOT_ATTEND_STATUSES, _attendance_penalty,
    get_billing_periods, calc_revenue,
)
from init_db import init_all_tables
from config import ETL_BATCH_PAGE_SIZE, ETL_UPDATE_CUTOFF_DAYS, ETL_FULL_SKIP_MONTHS, CacheKey

logger = logging.getLogger(__name__)
logging.basicConfig(
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
    level=logging.INFO,
)


def batch_execute(cursor, sql, data_list):
    """PG: execute_batch로 네트워크 왕복 최소화, SQLite: executemany.
    배치 실패 시 row-by-row 폴백. (success, errors) 반환."""
    if not data_list:
        return 0, 0
    resolved_sql = adapt_query(sql) if is_pg() else sql
    try:
        if is_pg():
            from psycopg2.extras import execute_batch
            execute_batch(cursor, resolved_sql, data_list, page_size=ETL_BATCH_PAGE_SIZE)
        else:
            cursor.executemany(resolved_sql, data_list)
        return len(data_list), 0
    except Exception as e:
        logger.warning(f"[batch_execute] 배치 실패, row-by-row 폴백: {e}")
        success, errors = 0, 0
        for row in data_list:
            try:
                cursor.execute(resolved_sql, row)
                success += 1
            except Exception:
                errors += 1
        return success, errors

load_dotenv()

API_KEY = os.getenv("HRD_API_KEY")
COURSE_ID = os.getenv("HANWHA_COURSE_ID")

def clean_time(time_str):
    if not time_str or time_str == '0000' or len(time_str) != 4: return None
    return f"{time_str[:2]}:{time_str[2:]}"

def get_month_list(start_date_str, end_date_str):
    if not start_date_str or not end_date_str: return []
    start = datetime.strptime(start_date_str, "%Y-%m-%d")
    end = datetime.strptime(end_date_str, "%Y-%m-%d")
    date_list = []
    curr = start
    while curr.strftime("%Y%m") <= end.strftime("%Y%m"):
        date_list.append(curr.strftime("%Y%m"))
        curr += relativedelta(months=1)
    return date_list

def run_etl():
    if not API_KEY or not COURSE_ID:
        logger.error("HRD_API_KEY 또는 HANWHA_COURSE_ID 환경변수가 설정되지 않았습니다.")
        return
    init_all_tables()
    conn = get_connection(timeout=30, row_factory=sqlite3.Row)  # PG에서는 RealDictCursor로 자동 변환
    cursor = conn.cursor()
    session = get_retry_session()
    logger.info(f"[ETL Start] 과정ID({COURSE_ID}) 데이터 수집 시작...")
    _t0 = time.monotonic()
    total_success, total_errors = 0, 0

    BASE_URL_COURSE = "https://hrd.work24.go.kr/jsp/HRDP/HRDPO00/HRDPOA60/HRDPOA60_3.jsp"
    BASE_URL_DETAIL = "https://hrd.work24.go.kr/jsp/HRDP/HRDPO00/HRDPOA60/HRDPOA60_4.jsp"

    try:
        res = session.get(BASE_URL_COURSE, params={
            "returnType": "JSON", "authKey": API_KEY,
            "srchTrprId": COURSE_ID, "outType": "2"
        }, timeout=60)
        course_list = json.loads(res.json()['returnJSON'])
        logger.info(f"과정 목록({len(course_list)}건) 조회 성공.")
    except (requests.RequestException, json.JSONDecodeError, KeyError) as e:
        logger.error(f"과정 목록 조회 실패: {e}")
        return

    update_cutoff_date = (datetime.now() - timedelta(days=ETL_UPDATE_CUTOFF_DAYS)).strftime('%Y-%m-%d')
    full_skip_cutoff = (datetime.now() - relativedelta(months=ETL_FULL_SKIP_MONTHS)).strftime('%Y-%m-%d')

    for idx, course in enumerate(course_list, 1):
        try: trpr_degr = int(course.get('trprDegr', 0))
        except (ValueError, TypeError) as e:
            logger.warning(f"회차 변환 실패 (trprDegr={course.get('trprDegr')}): {e}")
            continue
        if trpr_degr == 0: continue
        logger.info(f"[{idx}/{len(course_list)}] {trpr_degr}회차 처리 시작...")

        ei_rate = course.get('eiEmplRate3')

        cursor.execute(adapt_query('''
            INSERT INTO TB_COURSE_MASTER (
                TRPR_ID, TRPR_DEGR, TRPR_NM, TR_STA_DT, TR_END_DT,
                TOT_TRCO, FINI_CNT, TOT_FXNUM, TOT_PAR_MKS, TOT_TRP_CNT, INST_INO,
                EI_EMPL_RATE_3, EI_EMPL_CNT_3, EI_EMPL_RATE_6, EI_EMPL_CNT_6,
                HRD_EMPL_RATE_6, HRD_EMPL_CNT_6
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(TRPR_ID, TRPR_DEGR) DO UPDATE SET
                TRPR_NM=excluded.TRPR_NM,
                TOT_TRCO=excluded.TOT_TRCO,
                FINI_CNT=excluded.FINI_CNT,
                TOT_FXNUM=excluded.TOT_FXNUM,
                TOT_PAR_MKS=excluded.TOT_PAR_MKS,
                TOT_TRP_CNT=excluded.TOT_TRP_CNT,
                EI_EMPL_RATE_3=excluded.EI_EMPL_RATE_3,
                EI_EMPL_CNT_3=excluded.EI_EMPL_CNT_3,
                EI_EMPL_RATE_6=excluded.EI_EMPL_RATE_6,
                EI_EMPL_CNT_6=excluded.EI_EMPL_CNT_6,
                HRD_EMPL_RATE_6=excluded.HRD_EMPL_RATE_6,
                HRD_EMPL_CNT_6=excluded.HRD_EMPL_CNT_6,
                COLLECTED_AT=CURRENT_TIMESTAMP
        '''), (
            course.get('trprId'), trpr_degr, course.get('trprNm'),
            course.get('trStaDt'), course.get('trEndDt'),
            course.get('totTrco'), course.get('finiCnt'),
            course.get('totFxnum'), course.get('totParMks'), course.get('totTrpCnt'), course.get('instIno'),
            ei_rate, course.get('eiEmplCnt3'), course.get('eiEmplRate6'), course.get('eiEmplCnt6'),
            course.get('hrdEmplRate6'), course.get('hrdEmplCnt6')
        ))

        trpr_id = course.get('trprId')
        end_date = course.get('trEndDt', '9999-12-31')

        # 종료 후 7개월 초과: 취업률 확정 → API 호출 전체 스킵
        # 단, '수강중' 상태 훈련생이 남아있으면 명부 업데이트 1회 더 실행
        if end_date < full_skip_cutoff:
            cursor.execute(adapt_query(
                "SELECT COUNT(*) AS cnt FROM TB_TRAINEE_INFO WHERE TRPR_ID=? AND TRPR_DEGR=? AND TRNEE_STATUS='수강중'"
            ), (trpr_id, trpr_degr))
            row = cursor.fetchone()
            still_active = (row['cnt'] if row else 0) > 0
            if not still_active:
                logger.info(f"{trpr_degr}회차: 종료 7개월 초과({end_date}). API 호출 전체 스킵.")
                conn.commit()
                continue
            logger.info(f"{trpr_degr}회차: 종료 7개월 초과({end_date})이나 미확정 훈련생 있음 → 명부 업데이트 진행.")

        cursor.execute(adapt_query("SELECT 1 FROM TB_ATTENDANCE_LOG WHERE TRPR_ID=? AND TRPR_DEGR=? LIMIT 1"), (trpr_id, trpr_degr))
        is_data_exists = cursor.fetchone() is not None

        # 종료된 과정은 출결 수집만 스킵 — 명부(TRNEE_STATUS)는 항상 업데이트
        skip_attendance = (end_date < update_cutoff_date) and is_data_exists
        if skip_attendance:
            logger.info(f"{trpr_degr}회차: 종료된 과정({end_date}). 출결 스킵, 명부 상태 업데이트 진행.")

        try:
            res_roster = session.get(BASE_URL_DETAIL, params={
                "returnType": "JSON", "authKey": API_KEY, "outType": "2",
                "srchTrprId": COURSE_ID, "srchTrprDegr": trpr_degr
            }, timeout=60)
            raw_json = res_roster.json().get('returnJSON')
            if raw_json:
                roster_data = json.loads(raw_json)
                trne_list = roster_data if isinstance(roster_data, list) else roster_data.get('trneList', [])
                if not isinstance(trne_list, list): trne_list = []

                trainee_rows = []
                for trnee in trne_list:
                    if not isinstance(trnee, dict): continue
                    trainee_rows.append((
                        COURSE_ID, trpr_degr, str(trnee.get('trneeCstmrId')), trnee.get('trneeCstmrNm'),
                        trnee.get('trneeSttusNm'), trnee.get('trneeTracseSe'),
                        trnee.get('lifyeaMd'), trnee.get('traingDeCnt'),
                        trnee.get('oflhdCnt'), trnee.get('vcatnCnt'),
                        trnee.get('absentCnt'), trnee.get('atendCnt')
                    ))
                s, e = batch_execute(cursor, '''
                    INSERT INTO TB_TRAINEE_INFO (
                        TRPR_ID, TRPR_DEGR, TRNEE_ID, TRNEE_NM,
                        TRNEE_STATUS, TRNEE_TYPE, BIRTH_DATE,
                        TOTAL_DAYS, OFLHD_CNT, VCATN_CNT,
                        ABSENT_CNT, ATEND_CNT
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(TRPR_ID, TRPR_DEGR, TRNEE_ID) DO UPDATE SET
                        TRNEE_STATUS = excluded.TRNEE_STATUS,
                        TRNEE_TYPE = excluded.TRNEE_TYPE,
                        BIRTH_DATE = excluded.BIRTH_DATE,
                        TOTAL_DAYS = excluded.TOTAL_DAYS,
                        OFLHD_CNT = excluded.OFLHD_CNT,
                        VCATN_CNT = excluded.VCATN_CNT,
                        ABSENT_CNT = excluded.ABSENT_CNT,
                        ATEND_CNT = excluded.ATEND_CNT,
                        COLLECTED_AT = CURRENT_TIMESTAMP
                ''', trainee_rows)
                total_success += s; total_errors += e
                logger.info(f"{trpr_degr}회차 명부: {len(trainee_rows)}건 수집 완료")
        except (requests.RequestException, json.JSONDecodeError, KeyError) as e:
            logger.warning(f"{trpr_degr}회차 명부 수집 실패: {e}")

        if skip_attendance:
            conn.commit()
            continue  # 출결 수집 스킵 (종료 과정 + 데이터 존재)

        target_months = get_month_list(course.get('trStaDt'), course.get('trEndDt'))
        logger.info(f"{trpr_degr}회차 출결: {len(target_months)}개월 수집 시작")
        trnee_ignore_rows = []
        atab_rows = []
        for yyyymm in target_months:
            try:
                res_attend = session.get(BASE_URL_DETAIL, params={
                    "returnType": "JSON", "authKey": API_KEY, "outType": "2",
                    "srchTrprId": COURSE_ID, "srchTrprDegr": trpr_degr,
                    "srchTorgId": "student_detail", "atendMo": yyyymm
                }, timeout=60)
                raw_json = res_attend.json().get('returnJSON')
                if not raw_json: continue

                atab_data = json.loads(raw_json)
                atab_list = atab_data if isinstance(atab_data, list) else atab_data.get('atabList', [])
                if not isinstance(atab_list, list): atab_list = []

                for log in atab_list:
                    if not isinstance(log, dict): continue
                    trnee_id = str(log.get('trneeCstmrId'))
                    trnee_ignore_rows.append((COURSE_ID, trpr_degr, trnee_id, log.get('cstmrNm')))
                    atab_rows.append((
                        COURSE_ID, trpr_degr, trnee_id, log.get('atendDe'), log.get('korDayNm'),
                        clean_time(log.get('lpsilTime')), clean_time(log.get('levromTime')),
                        log.get('atendSttusNm'), log.get('atendSttusCd')
                    ))
            except (requests.RequestException, json.JSONDecodeError, KeyError) as e:
                logger.warning(f"{trpr_degr}회차 출결({yyyymm}) 수집 실패: {e}")
                continue

        s, e = batch_execute(cursor,
            'INSERT OR IGNORE INTO TB_TRAINEE_INFO (TRPR_ID, TRPR_DEGR, TRNEE_ID, TRNEE_NM) VALUES (?, ?, ?, ?)',
            trnee_ignore_rows)
        total_success += s; total_errors += e
        s, e = batch_execute(cursor, '''
            INSERT INTO TB_ATTENDANCE_LOG (
                TRPR_ID, TRPR_DEGR, TRNEE_ID, ATEND_DT, DAY_NM, IN_TIME, OUT_TIME, ATEND_STATUS, ATEND_STATUS_CD
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(TRPR_ID, TRPR_DEGR, TRNEE_ID, ATEND_DT) DO UPDATE SET
                IN_TIME=excluded.IN_TIME, OUT_TIME=excluded.OUT_TIME,
                ATEND_STATUS=excluded.ATEND_STATUS, COLLECTED_AT=CURRENT_TIMESTAMP
        ''', atab_rows)
        total_success += s; total_errors += e
        logger.info(f"{trpr_degr}회차 출결: 총 {len(atab_rows)}건 수집 완료")
        conn.commit()

    conn.close()
    logger.info(f"[ETL Summary] 성공: {total_success:,}건, 실패: {total_errors:,}건")
    logger.info("[Complete] 스마트 ETL 수집 완료! (최신 데이터만 업데이트됨)")

    logger.info(f"총 소요: {time.monotonic() - _t0:.1f}초")
    _cache_hrd_stats()


def _cache_hrd_stats():
    """ETL 완료 후 출결 통계 + DB 분포를 TB_MARKET_CACHE에 사전 집계."""
    import pandas as pd

    logger.info("[집계 캐시] HRD 통계 집계 시작...")
    conn = get_connection()
    cursor = conn.cursor()

    upsert_sql = adapt_query("""
        INSERT INTO TB_MARKET_CACHE (CACHE_KEY, CACHE_DATA, COMPUTED_AT)
        VALUES (?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(CACHE_KEY) DO UPDATE SET
            CACHE_DATA=excluded.CACHE_DATA,
            COMPUTED_AT=excluded.COMPUTED_AT
    """)

    saved = 0

    # ── 1) 기수별 출석률 (home.py KPI용) ──
    try:
        today_str = datetime.now().strftime('%Y-%m-%d')
        sql = adapt_query(
            "SELECT a.TRPR_DEGR, a.TRNEE_ID, a.ATEND_STATUS, a.ATEND_DT "
            "FROM TB_ATTENDANCE_LOG a "
            "INNER JOIN TB_COURSE_MASTER c "
            "ON a.TRPR_ID = c.TRPR_ID AND a.TRPR_DEGR = c.TRPR_DEGR "
            "WHERE c.TR_END_DT < ?"
        )
        att_df = pd.read_sql(sql, conn, params=[today_str])
        if is_pg():
            att_df.columns = [c.upper() for c in att_df.columns]

        if not att_df.empty:
            att_df['ATEND_DT'] = pd.to_datetime(att_df['ATEND_DT'], errors='coerce').dt.date

            rows = []
            for degr, grp in att_df.groupby('TRPR_DEGR'):
                student_rates = [
                    calc_attendance_rate(s_grp)
                    for _, s_grp in grp.groupby('TRNEE_ID')
                ]
                avg_rate = sum(student_rates) / len(student_rates) if student_rates else 0.0
                present = grp[~grp['ATEND_STATUS'].isin(NOT_ATTEND_STATUSES)]
                penalty = int(present['ATEND_STATUS'].apply(_attendance_penalty).sum())
                present_days = max(0, len(present) - penalty // 3)
                rows.append({
                    'TRPR_DEGR': int(degr),
                    'ATT_RATE': round(avg_rate, 1),
                    'PRESENT_DAYS': present_days,
                })

            data_json = json.dumps(rows, ensure_ascii=False)
            cursor.execute(upsert_sql, (CacheKey.ATTENDANCE_STATS, data_json))
            conn.commit()
            logger.info(f"[캐시] attendance_stats: {len(rows)}행 저장")
            saved += 1
    except Exception as e:
        logger.error(f"[캐시] attendance_stats 실패: {e}")
        if is_pg():
            conn.rollback()

    # ── 2) 출결 상태 분포 (DB 명세용) ──
    try:
        sql = adapt_query(
            "SELECT ATEND_STATUS, COUNT(*) AS CNT "
            "FROM TB_ATTENDANCE_LOG GROUP BY ATEND_STATUS ORDER BY CNT DESC"
        )
        cursor.execute(sql)
        cols = [d[0].upper() for d in cursor.description]
        dist_rows = [dict(zip(cols, r)) for r in cursor.fetchall()]
        data_json = json.dumps(dist_rows, ensure_ascii=False, default=str)
        cursor.execute(upsert_sql, (CacheKey.DB_ATTEND_DIST, data_json))
        conn.commit()
        logger.info(f"[캐시] db_attend_dist: {len(dist_rows)}행 저장")
        saved += 1
    except Exception as e:
        logger.error(f"[캐시] db_attend_dist 실패: {e}")
        if is_pg():
            conn.rollback()

    # ── 3) 훈련생 상태 분포 (DB 명세용) ──
    try:
        sql = adapt_query(
            "SELECT TRNEE_STATUS, COUNT(*) AS CNT "
            "FROM TB_TRAINEE_INFO GROUP BY TRNEE_STATUS ORDER BY CNT DESC"
        )
        cursor.execute(sql)
        cols = [d[0].upper() for d in cursor.description]
        dist_rows = [dict(zip(cols, r)) for r in cursor.fetchall()]
        data_json = json.dumps(dist_rows, ensure_ascii=False, default=str)
        cursor.execute(upsert_sql, (CacheKey.DB_TRAINEE_DIST, data_json))
        conn.commit()
        logger.info(f"[캐시] db_trainee_dist: {len(dist_rows)}행 저장")
        saved += 1
    except Exception as e:
        logger.error(f"[캐시] db_trainee_dist 실패: {e}")
        if is_pg():
            conn.rollback()

    # ── 4) 전체 기수 매출 집계 (매출_분석.py용) ──
    try:
        from config import DAILY_TRAINING_FEE
        course_df = pd.read_sql(
            adapt_query(
                "SELECT TRPR_ID, TRPR_DEGR, TRPR_NM, TR_STA_DT, TR_END_DT "
                "FROM TB_COURSE_MASTER ORDER BY CAST(TRPR_DEGR AS INTEGER) DESC"
            ),
            conn,
        )
        if is_pg():
            course_df.columns = [c.upper() for c in course_df.columns]

        rev_results = []
        for _, row in course_df.iterrows():
            trpr_id = str(row['TRPR_ID'])
            trpr_degr = int(row['TRPR_DEGR'])
            att_df = pd.read_sql(
                adapt_query(
                    "SELECT TRNEE_ID, ATEND_DT, ATEND_STATUS "
                    "FROM TB_ATTENDANCE_LOG WHERE TRPR_ID = ? AND TRPR_DEGR = ?"
                ),
                conn,
                params=[trpr_id, trpr_degr],
            )
            if is_pg():
                att_df.columns = [c.upper() for c in att_df.columns]
            if att_df.empty:
                continue
            att_df['ATEND_DT'] = pd.to_datetime(att_df['ATEND_DT']).dt.date
            periods = get_billing_periods(str(row['TR_STA_DT']), str(row['TR_END_DT']))
            rev_rows = []
            for p in periods:
                mask = (att_df['ATEND_DT'] >= p['start']) & (att_df['ATEND_DT'] <= p['end'])
                period_df = att_df[mask]
                training_days = period_df[
                    ~period_df['ATEND_STATUS'].isin({'중도탈락미출석'})
                ]['ATEND_DT'].nunique()
                for tid, grp in period_df.groupby('TRNEE_ID'):
                    student_td = grp[
                        ~grp['ATEND_STATUS'].isin({'중도탈락미출석'})
                    ]['ATEND_DT'].nunique()
                    has_dropout = grp['ATEND_STATUS'].isin({'중도탈락미출석'}).any()
                    rate_td = training_days if has_dropout else student_td
                    present = grp[~grp['ATEND_STATUS'].isin(NOT_ATTEND_STATUSES)]
                    base_attend = present['ATEND_DT'].nunique()
                    penalty = int(present['ATEND_STATUS'].apply(_attendance_penalty).sum())
                    attend_days = max(0, base_attend - penalty // 3)
                    fee, rate, status = calc_revenue(
                        attend_days, rate_td, period_training_days=training_days
                    )
                    rev_rows.append({
                        'TRNEE_ID': tid,
                        'period_num': p['period_num'],
                        'fee': fee,
                        'status': status,
                        'training_days': training_days,
                    })
            if not rev_rows:
                continue
            rev_df = pd.DataFrame(rev_rows)
            _p_raw = rev_df.groupby('period_num')['fee'].sum()
            actual_fee = int(((_p_raw // 10) * 10).sum())
            n_students = rev_df['TRNEE_ID'].nunique()
            full_cnt = (rev_df.groupby(['TRNEE_ID', 'period_num'])['status'].first() == '전액').sum()
            prop_cnt = (rev_df.groupby(['TRNEE_ID', 'period_num'])['status'].first() == '비례').sum()
            none_cnt = (rev_df.groupby(['TRNEE_ID', 'period_num'])['status'].first() == '미청구').sum()
            base_fee_total = rev_df.groupby(['TRNEE_ID', 'period_num']).apply(
                lambda g: g.iloc[0]['training_days'] * DAILY_TRAINING_FEE
            ).sum()
            rev_results.append({
                'TRPR_DEGR': int(row['TRPR_DEGR']),
                'TRPR_NM': str(row['TRPR_NM']),
                'TR_STA_DT': str(row['TR_STA_DT']),
                'TR_END_DT': str(row['TR_END_DT']),
                'n_students': n_students,
                'base_fee': int(base_fee_total),
                'actual_fee': actual_fee,
                'achievement': round(actual_fee / base_fee_total * 100, 1) if base_fee_total > 0 else 0,
                'loss_prop': int(base_fee_total) - actual_fee,
                'full_cnt': int(full_cnt),
                'prop_cnt': int(prop_cnt),
                'none_cnt': int(none_cnt),
            })

        if rev_results:
            data_json = json.dumps(rev_results, ensure_ascii=False)
            cursor.execute(upsert_sql, (CacheKey.REVENUE_ALL_TERMS, data_json))
            conn.commit()
            logger.info(f"[캐시] revenue_all_terms: {len(rev_results)}행 저장")
            saved += 1
    except Exception as e:
        logger.error(f"[캐시] revenue_all_terms 실패: {e}")
        if is_pg():
            conn.rollback()

    # ── 5) 컬럼별 채움률 (DB 명세용) ──
    try:
        from pages import DB_명세 as _db_spec
        fill_out = {}
        for tbl, info in _db_spec.SCHEMAS.items():
            exprs = []
            for col, dtype, _ in info["columns"]:
                if dtype == "TEXT":
                    exprs.append(
                        f"ROUND(SUM(CASE WHEN {col} IS NOT NULL AND {col} != '' THEN 1.0 ELSE 0 END) * 100.0 / COUNT(*), 1) AS \"{col}\""
                    )
                else:
                    exprs.append(
                        f"ROUND(SUM(CASE WHEN {col} IS NOT NULL THEN 1.0 ELSE 0 END) * 100.0 / COUNT(*), 1) AS \"{col}\""
                    )
            sql = f"SELECT COUNT(*) AS _TOTAL, {', '.join(exprs)} FROM {tbl}"
            cursor.execute(sql)
            r = cursor.fetchone()
            desc = [d[0].upper() for d in cursor.description]
            if r:
                total = int(r[0]) if r[0] else 0
                rates = {desc[i]: (float(r[i]) if r[i] is not None else 0.0)
                         for i in range(1, len(desc))}
                fill_out[tbl] = {"_total": total, **rates}

        data_json = json.dumps(fill_out, ensure_ascii=False)
        cursor.execute(upsert_sql, (CacheKey.DB_FILL_RATES, data_json))
        conn.commit()
        logger.info(f"[캐시] db_fill_rates: {len(fill_out)}개 테이블 저장")
        saved += 1
    except Exception as e:
        logger.error(f"[캐시] db_fill_rates 실패: {e}")
        if is_pg():
            conn.rollback()

    # ── 6) 카테고리 컬럼 예시값 (DB 명세용) ──
    try:
        from pages import DB_명세 as _db_spec
        sample_out = {}
        for tbl, cols in _db_spec.SAMPLE_COLS.items():
            sample_out[tbl] = {}
            for col in cols:
                cursor.execute(
                    f"SELECT DISTINCT {col} FROM {tbl} "
                    f"WHERE {col} IS NOT NULL AND {col} != '' "
                    f"ORDER BY {col} LIMIT 10"
                )
                sample_out[tbl][col] = [str(r[0]) for r in cursor.fetchall()]

        data_json = json.dumps(sample_out, ensure_ascii=False)
        cursor.execute(upsert_sql, (CacheKey.DB_SAMPLE_VALUES, data_json))
        conn.commit()
        logger.info(f"[캐시] db_sample_values: {len(sample_out)}개 테이블 저장")
        saved += 1
    except Exception as e:
        logger.error(f"[캐시] db_sample_values 실패: {e}")
        if is_pg():
            conn.rollback()

    conn.close()
    logger.info(f"[집계 캐시] HRD 통계 {saved}개 완료")


if __name__ == "__main__":
    run_etl()
