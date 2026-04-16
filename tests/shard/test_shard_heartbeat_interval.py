"""Tests for the shard heartbeat interval mechanism."""

import asyncio
from unittest.mock import patch, MagicMock


class TestHeartbeatInterval:
    def test_heartbeat_sleeps_5_seconds_between_calls(self):
        # given a mock orchestrator endpoint and a patched sleep
        sleep_args = []
        original_sleep = asyncio.sleep

        async def tracking_sleep(seconds):
            sleep_args.append(seconds)
            # Don't actually sleep — just record the argument
            # After a few iterations, cancel by raising CancelledError
            if len(sleep_args) >= 2:
                raise asyncio.CancelledError()

        with patch("shard.orchestrator_command.httpx.post") as mock_post:
            mock_post.return_value = MagicMock(status_code=200)

            # when we run the heartbeat coroutine with patched sleep
            from shard.app import _heartbeat_loop
            with patch("shard.app.asyncio.sleep", side_effect=tracking_sleep):
                try:
                    asyncio.get_event_loop().run_until_complete(_heartbeat_loop())
                except (asyncio.CancelledError, RuntimeError):
                    pass

        # then asyncio.sleep was called with 5 (the heartbeat interval)
        assert len(sleep_args) >= 1
        for arg in sleep_args:
            assert arg == 5
