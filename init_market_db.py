import sqlite3
from utils import DB_FILE  # 👈 utils에서 설정을 가져옴

def init_market_table():
    conn = sqlite3.connect(DB_FILE) # 👈 가져온 변수 사용
    cursor = conn.cursor()
    
    # 시장 동향 데이터 테이블 (TB_MARKET_TREND)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS TB_MARKET_TREND (
            -- [1] 핵심 식별자 (PK)
            TRPR_ID TEXT,           -- 훈련과정ID
            TRPR_DEGR INTEGER,      -- 훈련과정 순차
            
            -- [2] 과정 기본 정보
            TRPR_NM TEXT,           -- 제목
            TRAINST_NM TEXT,        -- 훈련기관명
            TR_STA_DT TEXT,         -- 훈련시작일자
            TR_END_DT TEXT,         -- 훈련종료일자
            NCS_CD TEXT,            -- NCS 코드
            TRNG_AREA_CD TEXT,      -- 지역코드
            
            -- [3] 인원 및 비용 정보
            TOT_FXNUM INTEGER,      -- 정원
            TOT_TRCO REAL,          -- 훈련비
            COURSE_MAN REAL,        -- 수강비
            REG_COURSE_MAN INTEGER, -- 등록인원
            
            -- [4] 성과 지표
            EI_EMPL_RATE_3 REAL,    -- 3개월 취업률
            EI_EMPL_RATE_6 REAL,    -- 6개월 취업률
            EI_EMPL_CNT_3 INTEGER,  -- 3개월 취업인원
            EI_EMPL_CNT_3_GT10 TEXT,-- 10인 미만 여부
            STDG_SCOR REAL,         -- 만족도 점수
            GRADE TEXT,             -- 등급
            
            -- [5] 상세 정보
            CERTIFICATE TEXT,       -- 자격증
            CONTENTS TEXT,          -- 컨텐츠
            ADDRESS TEXT,           -- 주소
            TEL_NO TEXT,            -- 전화번호
            INST_INO TEXT,          -- 기관 코드
            TRAINST_CST_ID TEXT,    -- 기관ID
            TRAIN_TARGET TEXT,      -- 훈련유형 (K-Digital 등)
            TRAIN_TARGET_CD TEXT,   -- 유형코드
            WKEND_SE TEXT,          -- 주말구분
            
            -- [6] 기타
            TITLE_ICON TEXT,        -- 아이콘
            TITLE_LINK TEXT,        -- 링크
            SUB_TITLE_LINK TEXT,    -- 부제목 링크
            
            -- [7] 관리용
            COLLECTED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            
            PRIMARY KEY (TRPR_ID, TRPR_DEGR) 
        )
    ''')
    
    # 인덱스 생성
    cursor.execute('CREATE INDEX IF NOT EXISTS IDX_MARKET_NCS ON TB_MARKET_TREND (NCS_CD)')
    cursor.execute('CREATE INDEX IF NOT EXISTS IDX_MARKET_DATE ON TB_MARKET_TREND (TR_STA_DT)')
    cursor.execute('CREATE INDEX IF NOT EXISTS IDX_MARKET_AREA ON TB_MARKET_TREND (TRNG_AREA_CD)')
    
    conn.commit()
    conn.close()
    print(f"✅ [TB_MARKET_TREND] 테이블 생성 완료! (DB: {DB_FILE})")

if __name__ == "__main__":
    init_market_table()