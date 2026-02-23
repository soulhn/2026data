import sqlite3
import json
import os
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from dotenv import load_dotenv

from utils import get_connection, get_retry_session, adapt_query, is_pg
from init_db import init_all_tables
from config import ETL_BATCH_PAGE_SIZE, ETL_UPDATE_CUTOFF_DAYS


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

    for idx, course in enumerate(course_list, 1):
        try: trpr_degr = int(course.get('trprDegr', 0))
        except (ValueError, TypeError) as e:
            print(f"   회차 변환 실패 (trprDegr={course.get('trprDegr')}): {e}")
            continue
        if trpr_degr == 0: continue
        print(f"\n[{idx}/{len(course_list)}] {trpr_degr}회차 처리 시작...")

        ei_rate = course.get('eiEmplRate3')
        real_rate = None
        try:
            if ei_rate and ei_rate not in ['A', 'B', 'C', 'D', 'null']: real_rate = float(ei_rate)
        except (ValueError, TypeError) as e:
            print(f"   {trpr_degr}회차 취업률 변환 실패 (ei_rate={ei_rate}): {e}")

        cursor.execute(adapt_query('''
            INSERT INTO TB_COURSE_MASTER (
                TRPR_ID, TRPR_DEGR, TRPR_NM, TR_STA_DT, TR_END_DT,
                TOT_TRCO, FINI_CNT, TOT_FXNUM, TOT_PAR_MKS, TOT_TRP_CNT, INST_INO,
                EI_EMPL_RATE_3, EI_EMPL_CNT_3, EI_EMPL_RATE_6, EI_EMPL_CNT_6,
                HRD_EMPL_RATE_6, HRD_EMPL_CNT_6, REAL_EMPL_RATE
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                REAL_EMPL_RATE=excluded.REAL_EMPL_RATE,
                COLLECTED_AT=CURRENT_TIMESTAMP
        '''), (
            course.get('trprId'), trpr_degr, course.get('trprNm'), 
            course.get('trStaDt'), course.get('trEndDt'),
            course.get('totTrco'), course.get('finiCnt'),
            course.get('totFxnum'), course.get('totParMks'), course.get('totTrpCnt'), course.get('instIno'),
            ei_rate, course.get('eiEmplCnt3'), course.get('eiEmplRate6'), course.get('eiEmplCnt6'),
            course.get('hrdEmplRate6'), course.get('hrdEmplCnt6'), real_rate
        ))

        trpr_id = course.get('trprId')
        end_date = course.get('trEndDt', '9999-12-31')
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

if __name__ == "__main__":
    run_etl()