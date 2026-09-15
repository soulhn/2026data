"""시장 동향 테이블(TB_MARKET_TREND)을 메인 DB에서 시장 DB로 옮기는 1회성 마이그레이션.

배경: 2026-09 기준 메인 Supabase 352 MB / 500 MB 중 시장 테이블이 315 MB(46.6만 행).
      KPI 데이터를 얹기 전에 시장 테이블만 두 번째 무료 프로젝트(DATABASE_URL_MARKET)로 분리한다.

사용 (순서대로, 각 단계는 다시 실행해도 안전):
  python scripts/migrate_market_db.py copy      # 시장 DB에 스키마 생성 + COPY로 전량 복사 (PK 충돌은 건너뜀)
  python scripts/migrate_market_db.py verify    # 행 수·기간·연도별 분포가 양쪽에서 같은지 확인
  python scripts/migrate_market_db.py drop      # 메인 DB의 TB_MARKET_TREND 삭제 + VACUUM (확인 문구 입력 필요)

전제: .env(또는 환경변수)에 DATABASE_URL 과 DATABASE_URL_MARKET 이 **서로 다른** 프로젝트로 설정.
"""
import io
import os
import sys
import time

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from dotenv import load_dotenv  # noqa: E402

load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

import psycopg2  # noqa: E402

from utils import get_database_url, is_market_db_separate, MARKET_DB  # noqa: E402
from init_db import init_market_tables  # noqa: E402

TABLE = "TB_MARKET_TREND"
CHUNK_ROWS = 50_000


def _conn(db):
    c = psycopg2.connect(get_database_url(db), connect_timeout=15)
    c.autocommit = False
    return c


def _columns(conn):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = %s ORDER BY ordinal_position", (TABLE.lower(),)
        )
        return [r[0] for r in cur.fetchall()]


def _stats(conn):
    with conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*), MIN(TR_STA_DT), MAX(TR_STA_DT) FROM {TABLE}")
        cnt, mn, mx = cur.fetchone()
        cur.execute(f"SELECT substr(TR_STA_DT,1,4) AS y, COUNT(*) FROM {TABLE} GROUP BY 1 ORDER BY 1")
        years = dict(cur.fetchall())
    return cnt, mn, mx, years


def _require_separate():
    if not is_market_db_separate():
        sys.exit("중단: DATABASE_URL_MARKET 이 없거나 DATABASE_URL 과 같습니다. 두 번째 프로젝트 접속 문자열을 먼저 설정하세요.")


def cmd_copy():
    _require_separate()
    print("[1/3] 시장 DB에 스키마 생성")
    init_market_tables()

    src, dst = _conn("main"), _conn(MARKET_DB)
    try:
        src_cols = _columns(src)
        dst_cols = set(_columns(dst))
        cols = [c for c in src_cols if c in dst_cols]
        col_list = ", ".join(cols)
        before = _stats(dst)[0]
        total = _stats(src)[0]
        print(f"[2/3] 복사 시작: 메인 {total:,}행 → 시장 DB (현재 {before:,}행), 컬럼 {len(cols)}개")

        # 임시 테이블로 COPY 후 INSERT ... ON CONFLICT DO NOTHING — 재실행해도 중복이 생기지 않는다
        with dst.cursor() as dcur:
            dcur.execute(f"CREATE TEMP TABLE _stage (LIKE {TABLE} INCLUDING DEFAULTS) ON COMMIT PRESERVE ROWS")
        dst.commit()

        t0 = time.time()
        copied = 0
        with src.cursor(name="market_copy") as scur:
            scur.itersize = CHUNK_ROWS
            scur.execute(f"SELECT {col_list} FROM {TABLE} ORDER BY TRPR_ID, TRPR_DEGR")
            while True:
                rows = scur.fetchmany(CHUNK_ROWS)
                if not rows:
                    break
                buf = io.StringIO()
                for r in rows:
                    buf.write("\t".join(_esc(v) for v in r) + "\n")
                buf.seek(0)
                with dst.cursor() as dcur:
                    dcur.execute("TRUNCATE _stage")
                    dcur.copy_expert(f"COPY _stage ({col_list}) FROM STDIN WITH (FORMAT text, NULL '\\N')", buf)
                    dcur.execute(
                        f"INSERT INTO {TABLE} ({col_list}) SELECT {col_list} FROM _stage "
                        f"ON CONFLICT (TRPR_ID, TRPR_DEGR) DO NOTHING"
                    )
                dst.commit()
                copied += len(rows)
                print(f"  {copied:,}/{total:,} ({time.time() - t0:.0f}s)")
        after = _stats(dst)[0]
        print(f"[3/3] 완료: 시장 DB {before:,} → {after:,}행 ({time.time() - t0:.0f}s). 다음: verify")
    finally:
        src.close()
        dst.close()


def _esc(v):
    """COPY text 포맷 이스케이프. None → \\N."""
    if v is None:
        return "\\N"
    s = str(v)
    return (s.replace("\\", "\\\\").replace("\t", "\\t").replace("\n", "\\n").replace("\r", "\\r"))


def cmd_verify():
    _require_separate()
    src, dst = _conn("main"), _conn(MARKET_DB)
    try:
        s, d = _stats(src), _stats(dst)
    finally:
        src.close()
        dst.close()
    print(f"메인   : {s[0]:,}행  {s[1]} ~ {s[2]}  연도별 {s[3]}")
    print(f"시장 DB: {d[0]:,}행  {d[1]} ~ {d[2]}  연도별 {d[3]}")
    ok = s[0] == d[0] and s[1] == d[1] and s[2] == d[2] and s[3] == d[3]
    print("결과:", "일치 — drop 단계로 진행 가능" if ok else "불일치 — copy를 다시 실행하거나 원인을 확인하세요")
    return ok


def cmd_drop():
    _require_separate()
    if not cmd_verify():
        sys.exit("중단: verify 불일치 상태에서는 삭제하지 않습니다.")
    answer = input(f"메인 DB에서 {TABLE} 을 삭제합니다. 되돌릴 수 없습니다. 'DROP' 을 입력하세요: ")
    if answer.strip() != "DROP":
        sys.exit("취소")
    src = _conn("main")
    try:
        src.autocommit = True
        with src.cursor() as cur:
            cur.execute(f"DROP TABLE {TABLE}")
            print("삭제 완료. VACUUM 중… (수십 초)")
            cur.execute("VACUUM")
            cur.execute("SELECT pg_size_pretty(pg_database_size(current_database()))")
            print("메인 DB 크기:", cur.fetchone()[0])
    finally:
        src.close()


if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else ""
    {"copy": cmd_copy, "verify": cmd_verify, "drop": cmd_drop}.get(
        cmd, lambda: sys.exit(__doc__)
    )()
