import sqlite3
import requests
import json
import os
from datetime import datetime
from dateutil.relativedelta import relativedelta
from dotenv import load_dotenv

# .env 파일 로드
load_dotenv()

# ==========================================
# 1. 설정 정보 (환경변수 사용)
# ==========================================
API_KEY = os.getenv("HRD_API_KEY")
COURSE_ID = os.getenv("HANWHA_COURSE_ID")  # 👈 .env에서 "한화 과정" ID를 가져옴
DB_FILE = "hrd_analysis.db"

# (혹시 몰라 안전장치 추가: 키가 없으면 멈춤)
if not API_KEY or not COURSE_ID:
    print("❌ 오류: .env 파일에서 API_KEY 또는 COURSE_ID를 찾을 수 없습니다.")
    exit()

# ==========================================
# 2. 공통 함수
# ==========================================
def get_db_connection():
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    return conn

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

def init_db():
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # TRPR_DEGR(회차)를 INTEGER(숫자)로 선언
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS TB_COURSE_MASTER (
            TRPR_ID TEXT, 
            TRPR_DEGR INTEGER, 
            TRPR_NM TEXT, 
            TR_STA_DT TEXT, TR_END_DT TEXT, 
            TOT_TRCO INTEGER, FINI_CNT INTEGER, 
            TOT_FXNUM INTEGER, TOT_PAR_MKS INTEGER, TOT_TRP_CNT INTEGER, INST_INO TEXT,
            EI_EMPL_RATE_3 TEXT, EI_EMPL_CNT_3 INTEGER, EI_EMPL_RATE_6 TEXT, EI_EMPL_CNT_6 INTEGER, 
            HRD_EMPL_RATE_6 TEXT, HRD_EMPL_CNT_6 INTEGER, REAL_EMPL_RATE REAL,
            COLLECTED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (TRPR_ID, TRPR_DEGR)
        )
    ''')
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS TB_TRAINEE_INFO (
            TRPR_ID TEXT, 
            TRPR_DEGR INTEGER, 
            TRNEE_ID TEXT, TRNEE_NM TEXT,
            TRNEE_STATUS TEXT, TRNEE_TYPE TEXT, BIRTH_DATE TEXT,
            TOTAL_DAYS INTEGER, OFLHD_CNT INTEGER, VCATN_CNT INTEGER,
            COLLECTED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (TRPR_ID, TRPR_DEGR, TRNEE_ID)
        )
    ''')
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS TB_ATTENDANCE_LOG (
            TRPR_ID TEXT, 
            TRPR_DEGR INTEGER, 
            TRNEE_ID TEXT, 
            ATEND_DT TEXT, DAY_NM TEXT, IN_TIME TEXT, OUT_TIME TEXT, 
            ATEND_STATUS TEXT, ATEND_STATUS_CD TEXT,
            COLLECTED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(TRPR_ID, TRPR_DEGR, TRNEE_ID, ATEND_DT)
        )
    ''')
    conn.commit()
    conn.close()

# ==========================================
# 3. 메인 ETL 로직
# ==========================================
def run_etl():
    init_db()
    conn = get_db_connection()
    cursor = conn.cursor()
    
    print(f"🚀 [ETL Start] 과정ID({COURSE_ID}) 데이터 수집 시작...")

    try:
        url_course = f"https://hrd.work24.go.kr/jsp/HRDP/HRDPO00/HRDPOA60/HRDPOA60_3.jsp?returnType=JSON&authKey={API_KEY}&srchTrprId={COURSE_ID}&outType=2"
        res = requests.get(url_course)
        course_list = json.loads(res.json()['returnJSON'])
        print(f"📋 과정 목록({len(course_list)}건) 조회 성공.")
    except Exception as e:
        print(f"❌ 과정 목록 조회 실패: {e}")
        return

    for course in course_list:
        try:
            trpr_degr = int(course.get('trprDegr', 0))
        except:
            continue
            
        if trpr_degr == 0: continue 

        ei_rate = course.get('eiEmplRate3')
        real_rate = None
        try: 
            if ei_rate and ei_rate not in ['A', 'B', 'C', 'D', 'null']: real_rate = float(ei_rate)
        except: pass

        cursor.execute('''
            INSERT INTO TB_COURSE_MASTER (
                TRPR_ID, TRPR_DEGR, TRPR_NM, TR_STA_DT, TR_END_DT, 
                TOT_TRCO, FINI_CNT, TOT_FXNUM, TOT_PAR_MKS, TOT_TRP_CNT, INST_INO,
                EI_EMPL_RATE_3, EI_EMPL_CNT_3, EI_EMPL_RATE_6, EI_EMPL_CNT_6, 
                HRD_EMPL_RATE_6, HRD_EMPL_CNT_6, REAL_EMPL_RATE
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(TRPR_ID, TRPR_DEGR) DO UPDATE SET 
                FINI_CNT=excluded.FINI_CNT, REAL_EMPL_RATE=excluded.REAL_EMPL_RATE,
                COLLECTED_AT=CURRENT_TIMESTAMP
        ''', (
            course.get('trprId'), trpr_degr, course.get('trprNm'), 
            course.get('trStaDt'), course.get('trEndDt'),
            course.get('totTrco'), course.get('finiCnt'),
            course.get('totFxnum'), course.get('totParMks'), course.get('totTrpCnt'), course.get('instIno'),
            ei_rate, course.get('eiEmplCnt3'), course.get('eiEmplRate6'), course.get('eiEmplCnt6'),
            course.get('hrdEmplRate6'), course.get('hrdEmplCnt6'), real_rate
        ))

        # ---------------------------------------------------------
        # 명부 수집
        # ---------------------------------------------------------
        url_roster = f"https://hrd.work24.go.kr/jsp/HRDP/HRDPO00/HRDPOA60/HRDPOA60_4.jsp?returnType=JSON&authKey={API_KEY}&outType=2&srchTrprId={COURSE_ID}&srchTrprDegr={trpr_degr}"
        
        try:
            res_roster = requests.get(url_roster)
            raw_json = res_roster.json().get('returnJSON')
            
            if raw_json:
                roster_data = json.loads(raw_json)
                if isinstance(roster_data, list): trne_list = roster_data
                elif isinstance(roster_data, dict): trne_list = roster_data.get('trneList', [])
                else: trne_list = []

                valid_cnt = 0
                for trnee in trne_list:
                    if not isinstance(trnee, dict): continue
                    valid_cnt += 1
                    cursor.execute('''
                        INSERT INTO TB_TRAINEE_INFO (
                            TRPR_ID, TRPR_DEGR, TRNEE_ID, TRNEE_NM,
                            TRNEE_STATUS, TRNEE_TYPE, BIRTH_DATE,
                            TOTAL_DAYS, OFLHD_CNT, VCATN_CNT
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT(TRPR_ID, TRPR_DEGR, TRNEE_ID) DO UPDATE SET
                            TRNEE_STATUS = excluded.TRNEE_STATUS,
                            TRNEE_TYPE = excluded.TRNEE_TYPE,
                            BIRTH_DATE = excluded.BIRTH_DATE,
                            TOTAL_DAYS = excluded.TOTAL_DAYS,
                            OFLHD_CNT = excluded.OFLHD_CNT,
                            VCATN_CNT = excluded.VCATN_CNT,
                            COLLECTED_AT = CURRENT_TIMESTAMP
                    ''', (
                        COURSE_ID, trpr_degr, str(trnee.get('trneeCstmrId')), trnee.get('trneeCstmrNm'),
                        trnee.get('trneeSttusNm'), trnee.get('trneeTracseSe'), 
                        trnee.get('lifyeaMd'), trnee.get('traingDeCnt'), 
                        trnee.get('oflhdCnt'), trnee.get('vcatnCnt')
                    ))
                print(f"   >> {trpr_degr}회차 명부: {valid_cnt}건")
        except: pass

        # ---------------------------------------------------------
        # 출석부 수집
        # ---------------------------------------------------------
        target_months = get_month_list(course.get('trStaDt'), course.get('trEndDt'))
        for yyyymm in target_months:
            url_attend = f"https://hrd.work24.go.kr/jsp/HRDP/HRDPO00/HRDPOA60/HRDPOA60_4.jsp?returnType=JSON&authKey={API_KEY}&outType=2&srchTrprId={COURSE_ID}&srchTrprDegr={trpr_degr}&srchTorgId=student_detail&atendMo={yyyymm}"
            try:
                res_attend = requests.get(url_attend)
                raw_json = res_attend.json().get('returnJSON')
                if not raw_json: continue
                
                atab_data = json.loads(raw_json)
                atab_list = atab_data if isinstance(atab_data, list) else atab_data.get('atabList', [])
                if not isinstance(atab_list, list): atab_list = []

                for log in atab_list:
                    if not isinstance(log, dict): continue
                    trnee_id = str(log.get('trneeCstmrId'))
                    
                    cursor.execute('INSERT OR IGNORE INTO TB_TRAINEE_INFO (TRPR_ID, TRPR_DEGR, TRNEE_ID, TRNEE_NM) VALUES (?, ?, ?, ?)', 
                                   (COURSE_ID, trpr_degr, trnee_id, log.get('cstmrNm')))

                    cursor.execute('''
                        INSERT INTO TB_ATTENDANCE_LOG (
                            TRPR_ID, TRPR_DEGR, TRNEE_ID, ATEND_DT, DAY_NM, IN_TIME, OUT_TIME, ATEND_STATUS, ATEND_STATUS_CD
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT(TRPR_ID, TRPR_DEGR, TRNEE_ID, ATEND_DT) DO UPDATE SET 
                            IN_TIME=excluded.IN_TIME, OUT_TIME=excluded.OUT_TIME, 
                            ATEND_STATUS=excluded.ATEND_STATUS, COLLECTED_AT=CURRENT_TIMESTAMP
                    ''', (
                        COURSE_ID, trpr_degr, trnee_id, log.get('atendDe'), log.get('korDayNm'), 
                        clean_time(log.get('lpsilTime')), clean_time(log.get('levromTime')), 
                        log.get('atendSttusNm'), log.get('atendSttusCd')
                    ))
            except: continue
        conn.commit()

    conn.close()
    print("🎉 [Complete] 정형화된 DB 구축 완료!")

if __name__ == "__main__":
    run_etl()