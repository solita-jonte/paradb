"""Tests for the orchestrator's background stale shard monitoring and partition redistribution."""

import asyncio
import time
import warnings

import pytest
from unittest.mock import patch, MagicMock
from fastapi.testclient import TestClient

from orchestrator.app import app
from orchestrator.partitions import init as partitions_init, get_free_partitions, INDEX_TO_PARTITION
from orchestrator.shards import URL_TO_SHARD, Shard, init as shards_init, fetch_shard, remove_stale_shards
from orchestrator.shard_command import ShardBroadcastCommand
from shared.types.shard import ShardInfo


@pytest.fixture(autouse=True)
def reset_state():
    """Reset global orchestrator state before each test."""
    partitions_init()
    shards_init()
    URL_TO_SHARD.clear()
    yield
    URL_TO_SHARD.clear()


def _register_shard(url: str, partition_indices: list[int] | None = None) -> Shard:
    """Helper to register a shard and optionally assign specific partitions."""
    shard_info = ShardInfo(url=url, load=0.0)
    shard = fetch_shard(shard_info)
    if partition_indices is not None:
        partitions = [INDEX_TO_PARTITION[i] for i in partition_indices]
        shard.add_partitions(partitions)
    return shard


class TestStaleMonitorTask:
    def test_monitoring_task_calls_remove_stale_shards(self):
        # given a mock remove_stale_shards
        with patch("orchestrator.app.remove_stale_shards", return_value=[]) as mock_remove:
            with patch("orchestrator.app.MONITOR_INTERVAL", 0.05):
                # when the orchestrator app starts with its lifespan
                with TestClient(app):
                    import time
                    time.sleep(0.2)  # let the monitoring task run

                # then remove_stale_shards was called at least once
                assert mock_remove.call_count >= 1


class TestPartitionRedistributionAfterStaleRemoval:
    def test_free_partitions_redistributed_to_remaining_shards(self):
        # given three shards, each with roughly 1/3 of partitions
        s1 = _register_shard("http://shard-1:3357", list(range(0, 342)))
        s2 = _register_shard("http://shard-2:3357", list(range(342, 684)))
        s3 = _register_shard("http://shard-3:3357", list(range(684, 1024)))

        # when one shard goes stale and is removed
        s2.last_heartbeat = time.time() - 16
        with patch("orchestrator.shard_command.httpx.post"):
            with patch("orchestrator.shard_command.httpx.delete"):
                from orchestrator.app import _redistribute_free_partitions
                remove_stale_shards()
                _redistribute_free_partitions()

        # then no partitions are free (all reassigned)
        free = list(get_free_partitions())
        assert len(free) == 0

        # and remaining shards have approximately equal partitions
        counts = sorted([len(s.partitions) for s in URL_TO_SHARD.values()])
        assert counts[-1] - counts[0] <= 1

    def test_round_robin_distribution_is_balanced(self):
        # given two remaining shards with 100 partitions each, plus 824 free
        s1 = _register_shard("http://shard-1:3357", list(range(0, 100)))
        s2 = _register_shard("http://shard-2:3357", list(range(100, 200)))

        # when free partitions are redistributed
        with patch("orchestrator.shard_command.httpx.post"):
            from orchestrator.app import _redistribute_free_partitions
            _redistribute_free_partitions()

        # then each shard has 512 partitions (1024 / 2)
        assert len(s1.partitions) == 512
        assert len(s2.partitions) == 512

    def test_redistribution_broadcasts_to_all_shards(self):
        # given two shards, one stale
        s1 = _register_shard("http://shard-1:3357", list(range(0, 512)))
        s2 = _register_shard("http://shard-2:3357", list(range(512, 1024)))
        s2.last_heartbeat = time.time() - 16

        # when stale removal and redistribution happen
        with patch("orchestrator.shard_command.httpx.post") as mock_post:
            from orchestrator.app import _redistribute_free_partitions
            remove_stale_shards()
            _redistribute_free_partitions()

        # then a broadcast was sent (POST to /cmd/partitions on remaining shards)
        partition_posts = [c for c in mock_post.call_args_list if "/cmd/partitions" in str(c)]
        assert len(partition_posts) >= 1

    def test_no_redistribution_when_no_shards_removed(self):
        # given two fresh shards
        s1 = _register_shard("http://shard-1:3357", list(range(0, 512)))
        s2 = _register_shard("http://shard-2:3357", list(range(512, 1024)))

        # when remove_stale_shards finds nothing stale
        removed = remove_stale_shards()

        # then no redistribution needed (removed list is empty)
        assert len(removed) == 0
        # and partition counts unchanged
        assert len(s1.partitions) == 512
        assert len(s2.partitions) == 512

    def test_no_crash_when_all_shards_stale(self):
        # given one shard that goes stale (no remaining shards)
        s1 = _register_shard("http://shard-1:3357", list(range(0, 1024)))
        s1.last_heartbeat = time.time() - 16

        # when stale removal and redistribution happen
        with patch("orchestrator.shard_command.httpx.post"):
            from orchestrator.app import _redistribute_free_partitions
            remove_stale_shards()
            # then no crash occurs
            _redistribute_free_partitions()

        # and all partitions are free
        free = list(get_free_partitions())
        assert len(free) == 1024
        assert len(URL_TO_SHARD) == 0


class TestStaleMonitorLifecycle:
    def test_monitoring_task_cancelled_on_shutdown(self):
        # given the orchestrator app with a monitoring task
        with patch("orchestrator.app.remove_stale_shards", return_value=[]):
            with patch("orchestrator.app.MONITOR_INTERVAL", 0.05):
                with warnings.catch_warnings(record=True) as w:
                    warnings.simplefilter("always")
                    # when the app starts and then shuts down
                    with TestClient(app):
                        import time
                        time.sleep(0.1)

                # then no task-related warnings were raised
                task_warnings = [x for x in w if "task" in str(x.message).lower()]
                assert len(task_warnings) == 0

    def test_monitoring_interval_is_5_seconds(self):
        # given a mock asyncio.sleep and remove_stale_shards
        sleep_args = []

        async def tracking_sleep(seconds):
            sleep_args.append(seconds)
            if len(sleep_args) >= 2:
                raise asyncio.CancelledError()

        with patch("orchestrator.app.remove_stale_shards", return_value=[]):
            from orchestrator.app import _monitor_stale_shards
            with patch("orchestrator.app.asyncio.sleep", side_effect=tracking_sleep):
                # when the monitoring coroutine runs
                try:
                    asyncio.get_event_loop().run_until_complete(_monitor_stale_shards())
                except (asyncio.CancelledError, RuntimeError):
                    pass

        # then asyncio.sleep was called with 5
        assert len(sleep_args) >= 1
        for arg in sleep_args:
            assert arg == 5
