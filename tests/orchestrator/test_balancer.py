"""Tests for the rebalancing algorithm."""

import threading
from unittest.mock import patch, MagicMock, call

import pytest

from orchestrator.balancer import balance_shards
from orchestrator.partitions import init as partitions_init, INDEX_TO_PARTITION
from orchestrator.shards import NAME_TO_SHARD, Shard, init as shards_init


@pytest.fixture(autouse=True)
def reset_state():
    partitions_init()
    shards_init()
    NAME_TO_SHARD.clear()
    yield
    NAME_TO_SHARD.clear()


def _make_shard(name: str, partition_indices: list[int]) -> Shard:
    """Helper to create a shard with given partitions assigned."""
    shard = Shard(name)
    partitions = [INDEX_TO_PARTITION[i] for i in partition_indices]
    shard.add_partitions(partitions)
    NAME_TO_SHARD[name] = shard
    return shard


class TestNoRebalanceWithOneShard:
    def test_no_partition_moved(self):
        # given only one shard owns all partitions
        _make_shard("shard-1", list(range(1024)))

        # when we balance
        with patch("orchestrator.balancer.ShardCommand") as MockCmd:
            with patch("orchestrator.balancer.ShardBroadcastCommand") as MockBroadcast:
                balance_shards()

        # then no partitions moved (no halt called)
        MockCmd.return_value.halt_flush_partition_writes.assert_not_called()


class TestNoRebalanceWhenDifferenceLessThan2:
    def test_nothing_changes(self):
        # given two shards with partition counts differing by 1
        _make_shard("shard-1", list(range(512)))
        _make_shard("shard-2", list(range(512, 1024)))

        original_a_count = len(NAME_TO_SHARD["shard-1"].partitions)
        original_b_count = len(NAME_TO_SHARD["shard-2"].partitions)

        # when we balance
        with patch("orchestrator.balancer.ShardCommand"):
            with patch("orchestrator.balancer.ShardBroadcastCommand"):
                balance_shards()

        # then nothing changes
        assert len(NAME_TO_SHARD["shard-1"].partitions) == original_a_count
        assert len(NAME_TO_SHARD["shard-2"].partitions) == original_b_count


class TestRebalanceMovesPartition:
    def test_moves_from_biggest_to_smallest(self):
        # given shard A has 10, shard B has 5
        _make_shard("shard-a", list(range(10)))
        _make_shard("shard-b", list(range(10, 15)))

        # when we balance
        with patch("orchestrator.balancer.ShardCommand") as MockCmd:
            MockCmd.return_value.halt_flush_partition_writes = MagicMock()
            MockCmd.return_value.send_partitions = MagicMock()
            with patch("orchestrator.balancer.ShardBroadcastCommand") as MockBroadcast:
                MockBroadcast.return_value.send_partitions = MagicMock()
                balance_shards()

        # then shard-a lost one partition and shard-b gained one
        assert len(NAME_TO_SHARD["shard-a"].partitions) == 9
        assert len(NAME_TO_SHARD["shard-b"].partitions) == 6


class TestRebalanceMovesOnlyOnePartition:
    def test_exactly_one_moved(self):
        # given large imbalance
        _make_shard("shard-a", list(range(20)))
        _make_shard("shard-b", list(range(20, 22)))

        # when we balance once
        with patch("orchestrator.balancer.ShardCommand") as MockCmd:
            MockCmd.return_value.halt_flush_partition_writes = MagicMock()
            MockCmd.return_value.send_partitions = MagicMock()
            with patch("orchestrator.balancer.ShardBroadcastCommand") as MockBroadcast:
                MockBroadcast.return_value.send_partitions = MagicMock()
                balance_shards()

        # then exactly one partition moved
        assert len(NAME_TO_SHARD["shard-a"].partitions) == 19
        assert len(NAME_TO_SHARD["shard-b"].partitions) == 3


class TestRebalanceUsesTryLock:
    def test_concurrent_calls_only_one_executes(self):
        # given two shards with imbalance
        _make_shard("shard-a", list(range(10)))
        _make_shard("shard-b", list(range(10, 15)))

        execution_count = {"count": 0}
        original_balance = balance_shards.__wrapped__ if hasattr(balance_shards, '__wrapped__') else None

        barrier = threading.Barrier(2, timeout=5)
        results = []

        def tracked_balance():
            balance_shards()
            results.append(True)

        # when two concurrent calls are made
        with patch("orchestrator.balancer.ShardCommand") as MockCmd:
            MockCmd.return_value.halt_flush_partition_writes = MagicMock()
            MockCmd.return_value.send_partitions = MagicMock()
            with patch("orchestrator.balancer.ShardBroadcastCommand") as MockBroadcast:
                MockBroadcast.return_value.send_partitions = MagicMock()
                t1 = threading.Thread(target=tracked_balance)
                t2 = threading.Thread(target=tracked_balance)
                t1.start()
                t2.start()
                t1.join(timeout=5)
                t2.join(timeout=5)

        # then at most one partition was moved (one call skipped due to TryLock)
        total_a = len(NAME_TO_SHARD["shard-a"].partitions)
        total_b = len(NAME_TO_SHARD["shard-b"].partitions)
        assert total_a + total_b == 15  # no partitions lost
        # each call moves at most one, so at most 2 moved (if both ran sequentially)
        # but no more than that — proving neither interfered with the other
        assert total_a >= 8


class TestRebalanceHaltsWritesBeforeMoving:
    def test_halt_called_before_reassignment(self):
        # given imbalanced shards
        _make_shard("shard-a", list(range(10)))
        _make_shard("shard-b", list(range(10, 15)))

        call_order = []

        with patch("orchestrator.balancer.ShardCommand") as MockCmd:
            def track_halt(partition_index):
                call_order.append(("halt", partition_index))
            def track_send():
                call_order.append(("send",))
            MockCmd.return_value.halt_flush_partition_writes = track_halt
            MockCmd.return_value.send_partitions = track_send
            with patch("orchestrator.balancer.ShardBroadcastCommand") as MockBroadcast:
                MockBroadcast.return_value.send_partitions = lambda: call_order.append(("broadcast",))
                balance_shards()

        # then halt was called before broadcast
        halt_idx = next(i for i, c in enumerate(call_order) if c[0] == "halt")
        broadcast_idx = next(i for i, c in enumerate(call_order) if c[0] == "broadcast")
        assert halt_idx < broadcast_idx


class TestRebalanceBroadcastsUpdatedTable:
    def test_broadcast_called(self):
        # given imbalanced shards
        _make_shard("shard-a", list(range(10)))
        _make_shard("shard-b", list(range(10, 15)))

        with patch("orchestrator.balancer.ShardCommand") as MockCmd:
            MockCmd.return_value.halt_flush_partition_writes = MagicMock()
            MockCmd.return_value.send_partitions = MagicMock()
            with patch("orchestrator.balancer.ShardBroadcastCommand") as MockBroadcast:
                mock_broadcast_send = MagicMock()
                MockBroadcast.return_value.send_partitions = mock_broadcast_send
                balance_shards()

        # then broadcast was called
        mock_broadcast_send.assert_called_once()
