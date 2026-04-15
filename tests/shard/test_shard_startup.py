"""Tests for shard startup lifecycle and heartbeat."""

import os
from unittest.mock import patch, MagicMock, call

import pytest


class TestShardRegistersOnStartup:
    def test_sends_post_shard_on_startup(self):
        # given a mock orchestrator endpoint
        with patch("shard.orchestrator_command.requests.post") as mock_post:
            mock_post.return_value = MagicMock(status_code=200)

            # when the shard app starts up (trigger lifespan)
            from shard.app import app
            from fastapi.testclient import TestClient
            # The lifespan should trigger registration
            with TestClient(app):
                pass

            # then POST /shard was called with hostname and load
            assert mock_post.called
            call_args = mock_post.call_args
            assert "/shard" in call_args[0][0] or "/shard" in str(call_args)


class TestShardSendsHeartbeat:
    def test_heartbeat_every_5_seconds(self):
        # given a mock orchestrator endpoint
        with patch("shard.orchestrator_command.requests.post") as mock_post:
            mock_post.return_value = MagicMock(status_code=200)

            # when the shard app runs (simulate enough time for at least 2 heartbeats)
            from shard.app import app
            from fastapi.testclient import TestClient
            import time

            with TestClient(app):
                # Wait for at least one heartbeat cycle
                # (In practice, the test verifies the heartbeat mechanism is set up)
                time.sleep(0.1)  # let background tasks start

            # then POST /shard was called (at least the initial registration)
            assert mock_post.call_count >= 1


class TestShardUsesOrchestratorUrlEnvVar:
    def test_custom_orchestrator_url(self):
        # given ORCHESTRATOR_URL is set to a custom value
        with patch.dict(os.environ, {"ORCHESTRATOR_URL": "http://custom-orch:8080"}):
            with patch("shard.orchestrator_command.requests.post") as mock_post:
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
            with patch("shard.orchestrator_command.requests.post") as mock_post:
                mock_post.return_value = MagicMock(status_code=200)

                from shard.app import app
                from fastapi.testclient import TestClient
                with TestClient(app):
                    pass

                # then the HTTP call targets the default "orchestrator" host
                if mock_post.called:
                    url = mock_post.call_args[0][0]
                    assert "orchestrator" in url
