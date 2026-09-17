"""notify.py — 디스코드 웹훅: URL 없으면 무동작, 섹션 묶기, 길이 분할, 실패 무시."""
from unittest.mock import MagicMock

import notify
from notify import discord_post, format_sections


class TestFormat:
    def test_skips_empty_sections_and_counts(self):
        msgs = format_sections([("A", []), ("B", ["x", "y"])])
        assert len(msgs) == 1 and msgs[0].startswith("**B** · 2건") and "• x" in msgs[0] and "A" not in msgs[0]

    def test_nothing_to_send(self):
        assert format_sections([("A", [])]) == []

    def test_splits_long_messages(self):
        msgs = format_sections([("A", ["가" * 100] * 40)])
        assert len(msgs) >= 2 and all(len(m) <= notify.DISCORD_LIMIT for m in msgs)


class TestPost:
    def test_no_url_is_noop(self, monkeypatch):
        monkeypatch.delenv("DISCORD_WEBHOOK_URL", raising=False)
        session = MagicMock()
        assert discord_post([("A", ["x"])], session=session) == 0
        session.post.assert_not_called()

    def test_posts_content_json(self):
        session = MagicMock()
        session.post.return_value = MagicMock(status_code=204)
        assert discord_post([("A", ["x"])], url="https://discord.test/hook", session=session) == 1
        assert session.post.call_args.kwargs["json"]["content"].startswith("**A**")

    def test_http_error_is_logged_not_raised(self):
        session = MagicMock()
        session.post.return_value = MagicMock(status_code=400, text="bad")
        assert discord_post([("A", ["x"])], url="https://discord.test/hook", session=session) == 0
