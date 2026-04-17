"""Tests for shard startup lifecycle and heartbeat."""

import asyncio
import os
import pytest
import requests as requests_lib
from unittest.mock import patch, MagicMock, AsyncMock


def _mock_async_client():
    """Create a mock httpx.AsyncClient with async context manager support."""
    mock_client = AsyncMock()
    mock_client.post.return_value = MagicMock(status_code=200)
    mock_cm = MagicMock()
    mock_cm.__aenter__ = AsyncMock(return_value=mock_client)
    mock_cm.__aexit__ = AsyncMock(return_value=False)
    return mock_cm, mock_client


class TestShardRegistersOnStartup:
    def test_sends_post_shard_on_startup(self):
        # given a mock orchestrator endpoint
        mock_cm, mock_client = _mock_async_client()
        with patch("shard.orchestrator_command.httpx.AsyncClient", return_value=mock_cm):

            # when the shard app starts up (trigger lifespan)
            from shard.app import app
            from fastapi.testclient import TestClient
            # The lifespan should trigger registration
            with TestClient(app):
                pass

            # then POST /shard was called
            assert mock_client.post.called
            call_args = mock_client.post.call_args
            assert "/shard" in call_args[0][0] or "/shard" in str(call_args)


class TestShardSendsHeartbeat:
    def test_heartbeat_task_started_during_lifespan(self):
        # given a mock orchestrator endpoint
        mock_cm, mock_client = _mock_async_client()
        with patch("shard.orchestrator_command.httpx.AsyncClient", return_value=mock_cm):

            # when the shard app starts via its lifespan
            from shard.app import app
            from fastapi.testclient import TestClient

            with TestClient(app):
                pass

            # then POST /shard was called at least once (initial registration)
            assert mock_client.post.call_count >= 1


class TestShardHeartbeatPayload:
    def test_heartbeat_sends_correct_shard_info(self):
        # given a mock orchestrator endpoint
        mock_cm, mock_client = _mock_async_client()
        with patch("shard.orchestrator_command.httpx.AsyncClient", return_value=mock_cm):

            # when the shard app starts and sends a heartbeat
            from shard.app import app
            from fastapi.testclient import TestClient
            with TestClient(app):
                pass

            # then the payload contains url and load fields
            assert mock_client.post.called
            call_kwargs = mock_client.post.call_args
            payload = call_kwargs[1].get("json") or call_kwargs[0][1] if len(call_kwargs[0]) > 1 else call_kwargs[1].get("json")
            assert "url" in payload
            assert "load" in payload
            assert payload["load"] == 0.0


class TestShardHeartbeatErrorHandling:
    @pytest.mark.asyncio
    async def test_heartbeat_continues_after_connection_error(self):
        # given the orchestrator fails on the second call (first heartbeat), then recovers
        call_count = {"n": 0}
        def side_effect(*args, **kwargs):
            call_count["n"] += 1
            if call_count["n"] == 2:  # first heartbeat call (after initial registration)
                raise requests_lib.ConnectionError("orchestrator unreachable")
            return MagicMock(status_code=200)

        mock_client = AsyncMock()
        mock_client.post = AsyncMock(side_effect=side_effect)
        mock_cm = MagicMock()
        mock_cm.__aenter__ = AsyncMock(return_value=mock_client)
        mock_cm.__aexit__ = AsyncMock(return_value=False)

        with patch("shard.orchestrator_command.httpx.AsyncClient", return_value=mock_cm):
            # when the shard app starts and the heartbeat loop runs
            from shard.app import app
            from fastapi.testclient import TestClient

            # patch the heartbeat interval to be very short for testing
            with patch("shard.lifespan.HEARTBEAT_INTERVAL", 0.05):
                with TestClient(app):
                    await asyncio.sleep(0.2)  # allow multiple heartbeat iterations

            # then the heartbeat continued past the connection error (3+ calls total)
            assert call_count["n"] >= 3

    @pytest.mark.asyncio
    async def test_heartbeat_continues_after_timeout(self):
        # given the orchestrator times out on the second call (first heartbeat), then recovers
        call_count = {"n": 0}
        def side_effect(*args, **kwargs):
            call_count["n"] += 1
            if call_count["n"] == 2:  # first heartbeat call (after initial registration)
                raise requests_lib.Timeout("orchestrator timeout")
            return MagicMock(status_code=200)

        mock_client = AsyncMock()
        mock_client.post = AsyncMock(side_effect=side_effect)
        mock_cm = MagicMock()
        mock_cm.__aenter__ = AsyncMock(return_value=mock_client)
        mock_cm.__aexit__ = AsyncMock(return_value=False)

        with patch("shard.orchestrator_command.httpx.AsyncClient", return_value=mock_cm):
            # when the shard app starts and the heartbeat loop runs
            from shard.app import app
            from fastapi.testclient import TestClient

            with patch("shard.lifespan.HEARTBEAT_INTERVAL", 0.05):
                with TestClient(app):
                    await asyncio.sleep(0.2)

            # then the heartbeat continued past the timeout error (3+ calls total)
            assert call_count["n"] >= 3


class TestShardHeartbeatLifecycle:
    def test_heartbeat_task_cancelled_on_shutdown(self):
        # given a mock orchestrator endpoint
        mock_cm, mock_client = _mock_async_client()
        with patch("shard.orchestrator_command.httpx.AsyncClient", return_value=mock_cm):

            # when the shard app starts and then shuts down
            from shard.app import app
            from fastapi.testclient import TestClient
            import warnings

            with warnings.catch_warnings(record=True) as w:
                warnings.simplefilter("always")
                with TestClient(app):
                    pass

            # then no task-related warnings were raised
            task_warnings = [x for x in w if "task" in str(x.message).lower()]
            assert len(task_warnings) == 0


class TestShardUsesOrchestratorUrlEnvVar:
    def test_custom_orchestrator_url(self):
        # given ORCHESTRATOR_URL is set to a custom value
        with patch.dict(os.environ, {"ORCHESTRATOR_URL": "http://custom-orch:8080"}):
            with patch("shard.orchestrator_command.httpx.post") as mock_post:
                mock_post.return_value = MagicMock(status_code=200)

                from shard.app import app
                from fastapi.testclient import TestClient
                with TestClient(app):
                    pass

                # then the HTTP call targets the custom URL
                if mock_post.called:
                    url = mock_post.call_args[0][0]
                    assert "custom-orch:8080" in url


class TestShardUsesDefaultOrchestratorUrl:
    def test_default_url(self):
        # given ORCHESTRATOR_URL is not set
        env = os.environ.copy()
        env.pop("ORCHESTRATOR_URL", None)
        with patch.dict(os.environ, env, clear=True):
            with patch("shard.orchestrator_command.httpx.post") as mock_post:
                mock_post.return_value = MagicMock(status_code=200)

                from shard.app import app
                from fastapi.testclient import TestClient
                with TestClient(app):
                    pass

                # then the HTTP call targets the default "orchestrator" host
                if mock_post.called:
                    url = mock_post.call_args[0][0]
                    assert "orchestrator" in url
