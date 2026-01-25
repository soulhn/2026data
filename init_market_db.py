import sqlite3
import os

DB_FILE = "hrd_analysis.db"

def init_market_table():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    # 시장 동향 데이터 테이블 (TB_MARKET_TREND)
    # API 명세서의 30개 항목 + 수집일시(1개) = 총 31개 컬럼
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS TB_MARKET_TREND (
            -- [1] 핵심 식별자 (PK)
            TRPR_ID TEXT,           -- 훈련과정ID (trprId)
            TRPR_DEGR INTEGER,      -- 훈련과정 순차 (trprDegr)
            
            -- [2] 과정 기본 정보
            TRPR_NM TEXT,           -- 제목 (title)
            TRAINST_NM TEXT,        -- 부 제목/기관명 (subTitle)
            TR_STA_DT TEXT,         -- 훈련시작일자 (traStartDate)
            TR_END_DT TEXT,         -- 훈련종료일자 (traEndDate)
            NCS_CD TEXT,            -- NCS 코드 (ncsCd)
            TRNG_AREA_CD TEXT,      -- 지역코드 (trngAreaCd)
            
            -- [3] 인원 및 비용 정보
            TOT_FXNUM INTEGER,      -- 정원 (yardMan)
            TOT_TRCO REAL,          -- 실제 훈련비 (realMan)
            COURSE_MAN REAL,        -- 수강비 (courseMan)
            REG_COURSE_MAN INTEGER, -- 수강신청 인원 (regCourseMan)
            
            -- [4] 성과 지표 (취업률/만족도)
            EI_EMPL_RATE_3 REAL,    -- 3개월 취업률 (eiEmplRate3)
            EI_EMPL_RATE_6 REAL,    -- 6개월 취업률 (eiEmplRate6)
            EI_EMPL_CNT_3 INTEGER,  -- 3개월 취업인원 (eiEmplCnt3)
            EI_EMPL_CNT_3_GT10 TEXT,-- 3개월 취업누적 10인 이하 여부 (eiEmplCnt3Gt10)
            STDG_SCOR REAL,         -- 만족도 점수 (stdgScor)
            GRADE TEXT,             -- 등급 (grade)
            
            -- [5] 상세 정보
            CERTIFICATE TEXT,       -- 자격증 (certificate)
            CONTENTS TEXT,          -- 컨텐츠 (contents)
            ADDRESS TEXT,           -- 주소 (address)
            TEL_NO TEXT,            -- 전화번호 (telNo)
            INST_INO TEXT,          -- 훈련기관 코드 (instCd)
            TRAINST_CST_ID TEXT,    -- 훈련기관ID (trainstCstId)
            TRAIN_TARGET TEXT,      -- 훈련대상 (trainTarget)
            TRAIN_TARGET_CD TEXT,   -- 훈련구분 (trainTargetCd)
            WKEND_SE TEXT,          -- 주말/주중 구분 (wkendSe) ⭐ [New!]
            
            -- [6] 기타 링크 및 아이콘
            TITLE_ICON TEXT,        -- 제목 아이콘 (titleIcon)
            TITLE_LINK TEXT,        -- 제목 링크 (titleLink)
            SUB_TITLE_LINK TEXT,    -- 부 제목 링크 (subTitleLink)
            
            -- [7] 관리용
            COLLECTED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP, -- 수집 시각
            
            PRIMARY KEY (TRPR_ID, TRPR_DEGR) 
        )
    ''')
    
    # 조회 속도 향상을 위한 인덱스 (자주 쓰는 검색 조건)
    cursor.execute('CREATE INDEX IF NOT EXISTS IDX_MARKET_NCS ON TB_MARKET_TREND (NCS_CD)')
    cursor.execute('CREATE INDEX IF NOT EXISTS IDX_MARKET_DATE ON TB_MARKET_TREND (TR_STA_DT)')
    cursor.execute('CREATE INDEX IF NOT EXISTS IDX_MARKET_AREA ON TB_MARKET_TREND (TRNG_AREA_CD)')
    
    conn.commit()
    conn.close()
    print(f"✅ [TB_MARKET_TREND] 테이블 생성 완료! (총 31개 컬럼, DB: {DB_FILE})")

if __name__ == "__main__":
    init_market_table()