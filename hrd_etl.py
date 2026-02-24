import sqlite3
import json
import os
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from dotenv import load_dotenv

from utils import (
    get_connection, get_retry_session, adapt_query, is_pg,
    calc_attendance_rate, NOT_ATTEND_STATUSES, _attendance_penalty,
)
from init_db import init_all_tables
from config import ETL_BATCH_PAGE_SIZE, ETL_UPDATE_CUTOFF_DAYS, ETL_FULL_SKIP_MONTHS


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
        print(f"   [batch_execute] 배치 실패, row-by-row 폴백: {e}")
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
        print("오류: HRD_API_KEY 또는 HANWHA_COURSE_ID 환경변수가 설정되지 않았습니다.")
        return
    init_all_tables()
    conn = get_connection(timeout=30, row_factory=sqlite3.Row)  # PG에서는 RealDictCursor로 자동 변환
    cursor = conn.cursor()
    session = get_retry_session()
    print(f"[ETL Start] 과정ID({COURSE_ID}) 데이터 수집 시작...")
    total_success, total_errors = 0, 0

    BASE_URL_COURSE = "https://hrd.work24.go.kr/jsp/HRDP/HRDPO00/HRDPOA60/HRDPOA60_3.jsp"
    BASE_URL_DETAIL = "https://hrd.work24.go.kr/jsp/HRDP/HRDPO00/HRDPOA60/HRDPOA60_4.jsp"

    try:
        res = session.get(BASE_URL_COURSE, params={
            "returnType": "JSON", "authKey": API_KEY,
            "srchTrprId": COURSE_ID, "outType": "2"
        })
        course_list = json.loads(res.json()['returnJSON'])
        print(f"과정 목록({len(course_list)}건) 조회 성공.")
    except Exception as e:
        print(f"과정 목록 조회 실패: {e}")
        return

    update_cutoff_date = (datetime.now() - timedelta(days=ETL_UPDATE_CUTOFF_DAYS)).strftime('%Y-%m-%d')
    full_skip_cutoff = (datetime.now() - relativedelta(months=ETL_FULL_SKIP_MONTHS)).strftime('%Y-%m-%d')

    for idx, course in enumerate(course_list, 1):
        try: trpr_degr = int(course.get('trprDegr', 0))
        except (ValueError, TypeError) as e:
            print(f"   회차 변환 실패 (trprDegr={course.get('trprDegr')}): {e}")
            continue
        if trpr_degr == 0: continue
        print(f"\n[{idx}/{len(course_list)}] {trpr_degr}회차 처리 시작...")

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
                print(f"   {trpr_degr}회차: 종료 7개월 초과({end_date}). API 호출 전체 스킵.")
                conn.commit()
                continue
            print(f"   {trpr_degr}회차: 종료 7개월 초과({end_date})이나 미확정 훈련생 있음 → 명부 업데이트 진행.")

        cursor.execute(adapt_query("SELECT 1 FROM TB_ATTENDANCE_LOG WHERE TRPR_ID=? AND TRPR_DEGR=? LIMIT 1"), (trpr_id, trpr_degr))
        is_data_exists = cursor.fetchone() is not None

        # 종료된 과정은 출결 수집만 스킵 — 명부(TRNEE_STATUS)는 항상 업데이트
        skip_attendance = (end_date < update_cutoff_date) and is_data_exists
        if skip_attendance:
            print(f"   {trpr_degr}회차: 종료된 과정({end_date}). 출결 스킵, 명부 상태 업데이트 진행.")

        try:
            res_roster = session.get(BASE_URL_DETAIL, params={
                "returnType": "JSON", "authKey": API_KEY, "outType": "2",
                "srchTrprId": COURSE_ID, "srchTrprDegr": trpr_degr
            })
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
                print(f"   >> {trpr_degr}회차 명부: {len(trainee_rows)}건 수집 완료")
        except Exception as e:
            print(f"   ⚠️ {trpr_degr}회차 명부 수집 실패: {e}")

        if skip_attendance:
            conn.commit()
            continue  # 출결 수집 스킵 (종료 과정 + 데이터 존재)

        target_months = get_month_list(course.get('trStaDt'), course.get('trEndDt'))
        print(f"   >> {trpr_degr}회차 출결: {len(target_months)}개월 수집 시작")
        trnee_ignore_rows = []
        atab_rows = []
        for yyyymm in target_months:
            try:
                res_attend = session.get(BASE_URL_DETAIL, params={
                    "returnType": "JSON", "authKey": API_KEY, "outType": "2",
                    "srchTrprId": COURSE_ID, "srchTrprDegr": trpr_degr,
                    "srchTorgId": "student_detail", "atendMo": yyyymm
                })
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
            except Exception as e:
                print(f"   ⚠️ {trpr_degr}회차 출결({yyyymm}) 수집 실패: {e}")
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
        print(f"   >> {trpr_degr}회차 출결: 총 {len(atab_rows)}건 수집 완료")
        conn.commit()

    conn.close()
    print(f"[ETL Summary] 성공: {total_success:,}건, 실패: {total_errors:,}건")
    print("[Complete] 스마트 ETL 수집 완료! (최신 데이터만 업데이트됨)")

    _cache_hrd_stats()


def _cache_hrd_stats():
    """ETL 완료 후 출결 통계 + DB 분포를 TB_MARKET_CACHE에 사전 집계."""
    import pandas as pd

    print("\n[집계 캐시] HRD 통계 집계 시작...")
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
            cursor.execute(upsert_sql, ('attendance_stats', data_json))
            conn.commit()
            print(f"  [캐시] attendance_stats: {len(rows)}행 저장")
            saved += 1
    except Exception as e:
        print(f"  [캐시] attendance_stats 실패: {e}")
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
        cursor.execute(upsert_sql, ('db_attend_dist', data_json))
        conn.commit()
        print(f"  [캐시] db_attend_dist: {len(dist_rows)}행 저장")
        saved += 1
    except Exception as e:
        print(f"  [캐시] db_attend_dist 실패: {e}")
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
        cursor.execute(upsert_sql, ('db_trainee_dist', data_json))
        conn.commit()
        print(f"  [캐시] db_trainee_dist: {len(dist_rows)}행 저장")
        saved += 1
    except Exception as e:
        print(f"  [캐시] db_trainee_dist 실패: {e}")
        if is_pg():
            conn.rollback()

    conn.close()
    print(f"[집계 캐시] HRD 통계 {saved}개 완료")


if __name__ == "__main__":
    run_etl()
