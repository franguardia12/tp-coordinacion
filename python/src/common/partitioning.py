"""Stable ownership of each fruit across independently started processes."""

FNV_OFFSET_BASIS = 2166136261
FNV_PRIME = 16777619
UINT32_MASK = 4294967295


def fruit_partition(fruit, partition_count):
    if partition_count <= 0:
        raise ValueError("Partition count must be positive")
    # FNV-1a over UTF-8 is deterministic across independent Python processes.
    hash_value = FNV_OFFSET_BASIS
    for byte in fruit.encode("utf-8"):
        # Retain 32 bits after each multiplication, as defined by FNV-1a.
        hash_value = ((hash_value ^ byte) * FNV_PRIME) & UINT32_MASK
    return hash_value % partition_count
