"""Stable ownership of each fruit across independently started processes."""

from hashlib import sha256


def fruit_partition(fruit, partition_count):
    if partition_count <= 0:
        raise ValueError("Partition count must be positive")
    # Python's built-in hash for strings varies between interpreter processes.
    digest = sha256(fruit.encode("utf-8")).digest()
    return int.from_bytes(digest, byteorder="big") % partition_count
