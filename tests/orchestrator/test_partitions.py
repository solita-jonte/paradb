"""Tests for partition initialization and the free-partition pool."""

import pytest

from orchestrator.partitions import init as partitions_init, get_free_partitions, INDEX_TO_PARTITION
from orchestrator.shards import Shard


@pytest.fixture(autouse=True)
def reset_partitions():
    partitions_init()
    yield


class TestInitCreates1024Partitions:
    def test_partition_count_and_range(self):
        # given partitions were initialized
        # then there are 1024 partitions indexed 0–1023
        assert len(INDEX_TO_PARTITION) == 1024
        assert set(INDEX_TO_PARTITION.keys()) == set(range(1024))

    def test_all_partitions_unowned(self):
        # given partitions were initialized
        # then all partitions are unowned (free)
        for p in INDEX_TO_PARTITION.values():
            assert p.owner == ""


class TestGetFreePartitions:
    def test_returns_only_unassigned(self):
        # given some partitions are assigned
        shard = Shard("shard-1")
        assigned = [INDEX_TO_PARTITION[i] for i in range(10)]
        shard.add_partitions(assigned)

        # when we get free partitions
        free = list(get_free_partitions())

        # then only unassigned ones are returned
        assert len(free) == 1024 - 10
        for p in free:
            assert p.owner == ""


class TestPartitionRelease:
    def test_released_partition_becomes_free(self):
        # given a partition assigned to a shard
        partition = INDEX_TO_PARTITION[42]
        shard = Shard("shard-1")
        shard.add_partitions([partition])
        assert partition.owner == "shard-1"

        # when the partition is released
        partition.release()

        # then it appears in free partitions
        free_indices = {p.index for p in get_free_partitions()}
        assert 42 in free_indices
