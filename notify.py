"""디스코드 웹훅 알림 (선택). `DISCORD_WEBHOOK_URL`이 없으면 조용히 아무것도 하지 않는다.

ETL이 감지한 변화(노션 최종결과 전이, HRD 명부 승인·이탈)를 한 번의 실행당 한 메시지로 묶어 보낸다.
개인정보 원칙: 메시지에는 가린 이름(홍*동)과 기수만 넣는다. 실명·연락처는 어디에도 쓰지 않는다.
"""
import logging
import os

import requests

from utils import _clean_secret

logger = logging.getLogger(__name__)

DISCORD_LIMIT = 1900          # 디스코드 메시지 상한 2000자 — 여유를 둔다
TIMEOUT = 10


def webhook_url():
    return _clean_secret(os.getenv("DISCORD_WEBHOOK_URL"))


def format_sections(sections):
    """[(제목, [줄, …]), …] → 메시지 문자열 목록 (상한에 맞춰 나눔). 빈 섹션은 뺀다."""
    lines = []
    for title, items in sections:
        if not items:
            continue
        lines.append(f"**{title}** · {len(items)}건")
        lines += [f"• {i}" for i in items]
        lines.append("")
    if not lines:
        return []
    messages, buf = [], ""
    for line in lines:
        if len(buf) + len(line) + 1 > DISCORD_LIMIT:
            messages.append(buf.rstrip())
            buf = ""
        buf += line + "\n"
    if buf.strip():
        messages.append(buf.rstrip())
    return messages


def discord_post(sections, url=None, session=None):
    """섹션 목록을 디스코드로 보낸다. 반환: 보낸 메시지 수. URL이 없거나 보낼 내용이 없으면 0."""
    url = url or webhook_url()
    messages = format_sections(sections)
    if not url or not messages:
        return 0
    http = session or requests
    sent = 0
    for m in messages:
        try:
            resp = http.post(url, json={"content": m}, timeout=TIMEOUT)
            if resp.status_code >= 400:
                logger.warning(f"[디스코드] 전송 실패 {resp.status_code}: {resp.text[:120]}")
                continue
            sent += 1
        except requests.RequestException as e:
            logger.warning(f"[디스코드] 연결 실패: {type(e).__name__}")
    return sent
