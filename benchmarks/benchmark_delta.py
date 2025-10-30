from __future__ import annotations

import argparse
import os
import random
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from pysync.stats import SyncStats
from pysync.strategy import DeltaStrategy


def _write_pattern(path: Path, size_bytes: int, *, chunk_size: int = 4 * 1024 * 1024) -> None:
  path.parent.mkdir(parents=True, exist_ok=True)
  pattern = (b'0123456789ABCDEF' * (chunk_size // 16 + 1))[:chunk_size]
  remaining = size_bytes

  with path.open('wb') as fh:
    while remaining > 0:
      to_write = min(chunk_size, remaining)
      fh.write(pattern[:to_write])
      remaining -= to_write


def _mutate_offsets(
  size_bytes: int, mutation_count: int, *, chunk_len: int, rng: random.Random
) -> Iterable[int]:
  if mutation_count <= 0 or size_bytes == 0:
    return []
  max_offset = max(size_bytes - chunk_len, 0)
  return (rng.randint(0, max_offset) for _ in range(mutation_count))


def _mutate_file(path: Path, offsets: Iterable[int], *, chunk_size: int = 64) -> None:
  with path.open('r+b') as fh:
    for offset in offsets:
      fh.seek(offset)
      fh.write(os.urandom(chunk_size))


@dataclass(slots=True)
class BenchmarkResult:
  initial_time: float
  delta_time: float
  initial_stats: SyncStats
  delta_stats: SyncStats


def run_benchmark(
  *,
  size_mb: int,
  mutation_count: int,
  chunk_size: int,
  block_size: int,
  seed: int,
) -> BenchmarkResult:
  size_bytes = size_mb * 1024 * 1024

  with tempfile.TemporaryDirectory() as workspace:
    workspace_path = Path(workspace)
    source = workspace_path / 'source.bin'
    destination = workspace_path / 'destination.bin'

    _write_pattern(source, size_bytes, chunk_size=chunk_size)

    syncer = DeltaStrategy(block_size=block_size)

    start = time.perf_counter()
    syncer.sync_file(source, destination)
    initial_time = time.perf_counter() - start
    initial_stats = syncer.get_stats_for(destination)
    if initial_stats is None:
      raise RuntimeError('Missing sync stats after initial run')

    rng = random.Random(seed)
    mutation_chunk = min(64, max(size_bytes // 1024, 1))
    offsets = list(_mutate_offsets(size_bytes, mutation_count, chunk_len=mutation_chunk, rng=rng))
    if offsets:
      _mutate_file(source, offsets, chunk_size=mutation_chunk)

    start = time.perf_counter()
    syncer.sync_file(source, destination)
    delta_time = time.perf_counter() - start
    delta_stats = syncer.get_stats_for(destination)
    if delta_stats is None:
      raise RuntimeError('Missing sync stats after delta run')

  return BenchmarkResult(
    initial_time=initial_time,
    delta_time=delta_time,
    initial_stats=initial_stats,
    delta_stats=delta_stats,
  )


def _format_bytes(value: int) -> str:
  return f'{value / (1024 * 1024):.2f} MiB'


def main() -> None:
  parser = argparse.ArgumentParser(description='Benchmark the delta sync strategy on large files.')
  parser.add_argument('--size-mb', type=int, default=256, help='Size of the source file in MiB')
  parser.add_argument(
    '--mutations', type=int, default=4, help='Number of small mutations to apply before delta sync'
  )
  parser.add_argument(
    '--pattern-chunk',
    type=int,
    default=4 * 1024 * 1024,
    help='Chunk size to use when materialising the source pattern (bytes)',
  )
  parser.add_argument(
    '--block-size',
    type=int,
    default=64 * 1024,
    help='Block size for DeltaSynchronizer (bytes)',
  )
  parser.add_argument('--seed', type=int, default=1337, help='Seed for mutation placement')

  args = parser.parse_args()

  result = run_benchmark(
    size_mb=args.size_mb,
    mutation_count=args.mutations,
    chunk_size=args.pattern_chunk,
    block_size=args.block_size,
    seed=args.seed,
  )

  print('=== Delta Synchronizer Benchmark ===')
  print(f'Source size      : {args.size_mb} MiB')
  print(f'Block size       : {args.block_size} bytes')
  print(f'Mutations applied: {args.mutations}')
  print()
  print(f'Full sync time   : {result.initial_time:.2f}s')
  print(f'  Transferred    : {_format_bytes(result.initial_stats.bytes_transferred)}')
  print(f'Delta sync time  : {result.delta_time:.2f}s')
  print(f'  Transferred    : {_format_bytes(result.delta_stats.bytes_transferred)}')
  print(f'  Reused         : {_format_bytes(result.delta_stats.bytes_reused)}')


if __name__ == '__main__':
  main()
