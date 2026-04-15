from shared.types.partition_bits import PARTITION_INDEXES


class Partition:
    def __init__(self, index):
        self.index: int = index
        self.owner: str = ''

    def release(self):
        self.owner = ''


INDEX_TO_PARTITION: dict[int, 'Partition'] = {}


def init():
    INDEX_TO_PARTITION.clear()
    INDEX_TO_PARTITION.update({pi: Partition(pi) for pi in PARTITION_INDEXES})


def get_free_partitions():
    for partition in INDEX_TO_PARTITION.values():
        if not partition.owner:
            yield partition
