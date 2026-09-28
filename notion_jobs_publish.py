"""노션 「채용 동향」 페이지에 과정 트랙 태깅 공고를 DB로 발행 (2026-09-21).

대상: TB_JOB_POSTING 중 TB_JOB_POSTING_TRACK에 한 번이라도 태깅된 공고(= 대시보드 채용 동향 페이지가 다루는 범위).
수집 원본 전체(6천 건+, 트랙 무관 공고 포함)는 올리지 않는다. 행 키는 사람인 JOB_ID, 내용 해시가 같으면 API 호출 없음.
쓰기 헬퍼·upsert 기록(TB_NOTION_PUBLISH, DB_KEY='jobs')은 notion_publish.py 공용. '메모'는 담당자 열 — 절대 쓰지 않는다.

    python notion_jobs_publish.py            # 생성·갱신 (재실행 안전)
"""
import logging
import os
import time
from datetime import datetime, timedelta, timezone

from dotenv import load_dotenv

import config
from init_db import init_all_tables
from notion_applicants_etl import get_sync_state, set_sync_state
from notion_ops import NotionFetchError
from notion_publish import (
    _request, _rt, create_database, ensure_database_layout, ensure_properties, get_database, publish,
)
from utils import _clean_secret, get_connection, load_data

load_dotenv()
logger = logging.getLogger(__name__)
logging.basicConfig(format="%(asctime)s %(levelname)s %(name)s %(message)s", level=logging.INFO)

DB_KEY = "notion_jobs_db"
UPDATED_BLOCK_KEY = "notion_jobs_updated_block"
PUB = "jobs"
KST = timezone(timedelta(hours=9))

SCHEMA = {
    "공고 제목": {"title": {}},
    "기업명": {"rich_text": {}},
    "해당 과정": {"multi_select": {"options": [{"name": k} for k in config.SARAMIN_TRACK_ORDER]}},
    "신입 가능": {"checkbox": {}},
    "상태": {"select": {"options": [{"name": "진행중"}, {"name": "마감"}]}},
    "경력": {"select": {}},
    "학력": {"select": {}},
    "지역": {"select": {}},
    "근무지": {"rich_text": {}},
    "고용형태": {"multi_select": {}},   # 사람인 값이 쉼표 목록 — 노션 select 옵션명엔 쉼표가 못 들어간다
    "업종": {"multi_select": {}},
    "직무 키워드": {"rich_text": {}},
    "급여": {"rich_text": {}},
    "게시일": {"date": {}},
    "마감일": {"date": {}},
    "마감 구분": {"select": {}},
    "링크": {"url": {}},
    "공고 ID": {"rich_text": {}},
    "갱신 시각": {"date": {}},
    "메모": {"rich_text": {}},   # 담당자 입력 — 파이프라인은 절대 쓰지 않는다
}
DESC = ("사람인 API로 매일 수집한 채용공고 중 AI캠퍼스 과정(MLE·AIO·MLO·COMMON) 규칙에 걸린 공고 한 건이 한 줄. "
        "신입 가능 = 경력무관·신입·신입/경력. 마감 후에도 행은 남기고 '상태'만 마감으로 바뀐다. '메모'는 담당자 열")
TRACK_NAMES = {k: f"{k} · {v['name']}" for k, v in config.SARAMIN_TRACKS.items()}


def _s(v):
    return None if v is None or (isinstance(v, float) and v != v) else str(v)


def _i(v):
    """정수 컬럼 — NULL·NaN은 0. ACTIVE·ENTRY_LEVEL이 비어 있어도 int()에서 터지지 않게."""
    return 0 if v is None or (isinstance(v, float) and v != v) else int(v)


def _split(v):
    """쉼표 목록 → 중복 제거한 리스트 (순서 유지)."""
    return list(dict.fromkeys(x.strip() for x in (_s(v) or "").split(",") if x.strip()))


def build_rows(today=None):
    today = today or datetime.now(KST).date().isoformat()
    df = load_data("""
        SELECT jp.JOB_ID, jp.POSITION_TITLE, jp.COMPANY_NM, jp.REGION, jp.LOC_NM, jp.EXPERIENCE_NM, jp.EDU_LV_NM,
               jp.JOB_TYPE_NM, jp.IND_NM, jp.JOB_NM, jp.SALARY_NM, jp.CLOSE_TYPE_NM,
               jp.POSTING_DT, jp.EXPIRATION_DT, jp.POSITION_URL, jp.ACTIVE,
               (SELECT STRING_AGG(t.TRACK, ',' ORDER BY t.TRACK) FROM TB_JOB_POSTING_TRACK t WHERE t.JOB_ID = jp.JOB_ID) AS TRACKS,
               (SELECT MAX(t.ENTRY_LEVEL) FROM TB_JOB_POSTING_TRACK t WHERE t.JOB_ID = jp.JOB_ID) AS ENTRY_LEVEL
        FROM TB_JOB_POSTING jp
        WHERE jp.JOB_ID IN (SELECT JOB_ID FROM TB_JOB_POSTING_TRACK)
        ORDER BY jp.POSTING_DT DESC NULLS LAST, jp.JOB_ID DESC
    """)
    rows = []
    for r in df.to_dict("records"):
        exp = _s(r["EXPIRATION_DT"])
        active = _i(r["ACTIVE"]) == 1 and (exp is None or exp[:10] >= today)
        rows.append({
            "KEY": str(r["JOB_ID"]),
            "공고 제목": _s(r["POSITION_TITLE"]) or "(제목 없음)",
            "기업명": _s(r["COMPANY_NM"]),
            "해당 과정": [t for t in (r["TRACKS"] or "").split(",") if t],
            "신입 가능": _i(r["ENTRY_LEVEL"]) == 1,
            "상태": "진행중" if active else "마감",
            "경력": _s(r["EXPERIENCE_NM"]),
            "학력": (_s(r["EDU_LV_NM"]) or "").replace(",", "·") or None,   # '대학졸업(2,3년)이상' — 옵션명 쉼표 금지
            "지역": _s(r["REGION"]),
            "근무지": (_s(r["LOC_NM"]) or "").replace("&gt;", ">") or None,
            "고용형태": _split(r["JOB_TYPE_NM"]),
            "업종": _split(r["IND_NM"]),
            "직무 키워드": _s(r["JOB_NM"]),
            "급여": _s(r["SALARY_NM"]),
            "게시일": _s(r["POSTING_DT"]),
            "마감일": exp,
            "마감 구분": _s(r["CLOSE_TYPE_NM"]),
            "링크": _s(r["POSITION_URL"]),
            "공고 ID": str(r["JOB_ID"]),
            "갱신 시각": datetime.now(KST).strftime("%Y-%m-%dT%H:%M:%S+09:00"),
        })
    return rows


def ensure_page(token, conn, session=None):
    """상단 '마지막 갱신' 콜아웃을 한 번 만들고 블록 ID를 저장."""
    upd = get_sync_state(conn, UPDATED_BLOCK_KEY)
    if not upd:
        guide = [
            "출처: 사람인 채용공고 API — 대시보드 채용 동향 페이지와 같은 원본(직무 코드 13개 + 키워드 29개로 매일 수집)",
            "해당 과정: " + " / ".join(f"{k} = {v['direction']}" for k, v in config.SARAMIN_TRACKS.items()),
            "상태: 마감일이 지났거나 사람인에서 내려간 공고는 '마감'. 상시채용은 마감일이 먼 미래로 들어온다",
            "한 공고가 여러 과정에 해당할 수 있어 과정별 개수 합은 전체보다 크다",
        ]
        res = _request(token, "PATCH", f"/blocks/{config.NOTION_JOBS_PAGE_ID}/children", {"children": [
            {"type": "callout", "callout": {"rich_text": _rt("마지막 갱신: 아직 없음"), "icon": {"type": "emoji", "emoji": "🕒"}}},
            {"type": "callout", "callout": {"rich_text": _rt("읽는 법"), "icon": {"type": "emoji", "emoji": "📖"},
                                            "children": [{"type": "bulleted_list_item", "bulleted_list_item": {"rich_text": _rt(t)}} for t in guide]}},
        ]}, session)
        upd = res["results"][0]["id"]
        set_sync_state(conn, upd, UPDATED_BLOCK_KEY)
        logger.info("[채용 발행] 페이지 상단 콜아웃 생성")
    return upd


def ensure_database(token, conn, session=None):
    db_id = get_sync_state(conn, DB_KEY)
    meta = get_database(token, db_id, session) if db_id else None
    if meta is None:
        db_id = create_database(token, config.NOTION_JOBS_PAGE_ID, "채용공고", SCHEMA, session)
        set_sync_state(conn, db_id, DB_KEY)
        cur = conn.cursor()
        from utils import adapt_query
        cur.execute(adapt_query("DELETE FROM TB_NOTION_PUBLISH WHERE DB_KEY = ?"), [PUB])
        conn.commit()
        logger.info(f"[채용 발행] 채용공고 DB 생성 {db_id}")
        meta = {}
    ensure_database_layout(token, db_id, DESC, meta, session)
    ensure_properties(token, db_id, SCHEMA, meta or get_database(token, db_id, session), session)
    return db_id


def main():
    token = _clean_secret(os.getenv("NOTION_TOKEN"))
    if not token:
        logger.warning("NOTION_TOKEN 없음 — 채용공고 발행을 건너뜁니다")
        return
    t0 = time.monotonic()
    init_all_tables(include_market=False)
    conn = get_connection(timeout=30)
    try:
        upd_block = ensure_page(token, conn)
        db_id = ensure_database(token, conn)
        rows = build_rows()
        c = publish(token, conn, PUB, db_id, SCHEMA, rows)
        active = sum(1 for r in rows if r["상태"] == "진행중")
        stamp = datetime.now(KST).strftime("%Y-%m-%d %H:%M")
        _request(token, "PATCH", f"/blocks/{upd_block}",
                 {"callout": {"rich_text": _rt(f"마지막 갱신: {stamp} (KST) · 공고 {len(rows)}건 (진행중 {active}건)")}})
        set_sync_state(conn, datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"), "notion_jobs_last_publish")
    except NotionFetchError as e:
        logger.error(f"[채용 발행] {e}")
        return
    finally:
        conn.close()
    logger.info(f"[채용 발행] 생성 {c[0]} · 갱신 {c[1]} · 건너뜀 {c[2]} ({time.monotonic() - t0:.1f}s)")


if __name__ == "__main__":
    main()
