"""Tests for partition bit derivation from UUIDs."""

import uuid

from shared.types.partition_bits import PARTITION_COUNT, PARTITION_MASK, PARTITION_INDEXES
from shard.document_storage import partition_index_for_id, partition_dir_name


class TestPartitionIndexFromUUID:
    """Partition index from UUID uses lowest 10 bits."""

    def test_known_uuid_lowest_bits(self):
        # given a UUID whose lowest 10 bits are 0x3E9 (1001)
        known_int = 0x3E9
        doc_id = uuid.UUID(int=known_int)

        # when we compute the partition index
        index = partition_index_for_id(doc_id)

        # then it equals 0x3E9
        assert index == 0x3E9

    def test_uuid_with_zero_low_bits(self):
        # given a UUID whose lowest 10 bits are 0
        doc_id = uuid.UUID(int=1024)  # 0x400 → lowest 10 bits = 0

        # when we compute the partition index
        index = partition_index_for_id(doc_id)

        # then it equals 0
        assert index == 0

    def test_uuid_with_max_partition_bits(self):
        # given a UUID whose lowest 10 bits are all 1s (1023)
        doc_id = uuid.UUID(int=0x3FF)

        # when we compute the partition index
        index = partition_index_for_id(doc_id)

        # then it equals 1023
        assert index == 1023

    def test_large_uuid_uses_only_low_bits(self):
        # given a UUID with a large int value but known low bits
        doc_id = uuid.UUID(int=(0xDEADBEEF << 10) | 42)

        # when we compute the partition index
        index = partition_index_for_id(doc_id)

        # then only the lowest 10 bits matter
        assert index == 42


class TestPartitionDirName:
    """Partition directory name is hex of index."""

    def test_hex_name_for_1001(self):
        # given partition index 1001 (0x3e9)
        # when we get the directory name
        name = partition_dir_name(0x3E9)

        # then it is the hex representation
        assert name == "3e9"

    def test_hex_name_for_zero(self):
        # given partition index 0
        # when we get the directory name
        name = partition_dir_name(0)

        # then it is "0"
        assert name == "0"

    def test_hex_name_for_max(self):
        # given partition index 1023 (0x3ff)
        # when we get the directory name
        name = partition_dir_name(1023)

        # then it is "3ff"
        assert name == "3ff"


class TestPartitionConstants:
    """All partition indexes are in range 0–1023."""

    def test_partition_indexes_range(self):
        # given the module constants
        # then PARTITION_INDEXES covers 0–1023
        assert PARTITION_INDEXES == tuple(range(1024))

    def test_partition_mask(self):
        # given the module constants
        # then PARTITION_MASK is 0x3FF
        assert PARTITION_MASK == 0x3FF

    def test_partition_count(self):
        # given the module constants
        # then PARTITION_COUNT is 1024
        assert PARTITION_COUNT == 1024
