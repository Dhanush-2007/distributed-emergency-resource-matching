#!/usr/bin/env python3
"""Generate a fresh key for a specific shard."""

import argparse
import uuid

from ems_common.sharding import shard_of


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Print a fresh <prefix>:<uuid> key mapping to a target shard."
    )
    parser.add_argument("shardId", type=int, help="Target shard ID (e.g. 0, 1, 2)")
    parser.add_argument(
        "--prefix",
        type=str,
        default="request",
        help="Key prefix (default: request)",
    )
    parser.add_argument(
        "--shards",
        type=int,
        default=3,
        help="Number of shards (default: 3)",
    )
    args = parser.parse_args()

    target_shard = args.shardId
    prefix = args.prefix
    n_shards = args.shards

    while True:
        key = f"{prefix}:{uuid.uuid4()}"
        if shard_of(key, n_shards) == target_shard:
            print(key)
            return


if __name__ == "__main__":
    main()
