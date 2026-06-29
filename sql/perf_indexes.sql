-- 운영 중인 Supabase(PostgreSQL)에 누락 인덱스를 추가하는 스크립트.
-- init_db.py 의 인덱스 정의는 신규 초기화에만 적용되므로, 이미 떠 있는 DB에는
-- 이 파일을 Supabase SQL Editor 에서 1회 실행한다. 모두 IF NOT EXISTS 라 재실행 안전.
--
-- 목적: 페이지 콜드 로드 시 자주 쓰는 필터/조인/집계 컬럼 인덱싱으로 쿼리 가속.

-- AI 리포트: TB_TRAINEE_INFO WHERE TRPR_DEGR = ?
CREATE INDEX IF NOT EXISTS IDX_TRAINEE_DEGR ON TB_TRAINEE_INFO (TRPR_DEGR);

-- 출결 조인/그룹바이: TB_ATTENDANCE_LOG 의 TRNEE_ID
CREATE INDEX IF NOT EXISTS IDX_ATTEND_TRNEE ON TB_ATTENDANCE_LOG (TRNEE_ID);

-- 연도별 집계: TB_COURSE_MASTER 의 TR_STA_DT (SUBSTR(TR_STA_DT,1,4))
CREATE INDEX IF NOT EXISTS IDX_COURSE_STA_DT ON TB_COURSE_MASTER (TR_STA_DT);

-- 실행 후 통계 갱신 (선택)
ANALYZE TB_TRAINEE_INFO;
ANALYZE TB_ATTENDANCE_LOG;
ANALYZE TB_COURSE_MASTER;
