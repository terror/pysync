from __future__ import annotations

import argparse
import sys
from pathlib import Path

from pysync.sync import DeltaSynchronizer, FileCopier, SyncError, SyncStrategy, sync


def _build_strategy(args: argparse.Namespace) -> SyncStrategy:
  if args.strategy == 'copy':
    if args.block_size is not None:
      raise SyncError('--block-size can only be used with --strategy delta')
    return FileCopier()

  if args.block_size is not None and args.block_size <= 0:
    raise SyncError('--block-size must be a positive integer')

  block_size = args.block_size if args.block_size is not None else 64 * 1024
  return DeltaSynchronizer(block_size=block_size)


def _print_delta_stats(strategy: DeltaSynchronizer, destination: Path) -> None:
  stats = strategy.stats()
  if not stats:
    print('Delta transfer stats: no files processed.')
    return

  dest_root = destination.resolve()
  total_bytes = 0
  transferred = 0
  reused = 0

  print('Delta transfer stats:')
  for path, entry in sorted(stats.items()):
    total_bytes += entry.total_bytes
    transferred += entry.bytes_transferred
    reused += entry.bytes_reused
    try:
      display = path.relative_to(dest_root)
    except ValueError:
      display = path
    print(
      f'  {display}: transferred {entry.bytes_transferred} bytes, '
      f'reused {entry.bytes_reused} bytes, saved {entry.bytes_saved} bytes'
    )

  bytes_saved = max(total_bytes - transferred, 0)
  print(f'Total: transferred {transferred} bytes, reused {reused} bytes, saved {bytes_saved} bytes')


def main() -> int:
  parser = argparse.ArgumentParser(description='Synchronise two local directories.')
  parser.add_argument('source', type=Path, help='Path to the source directory')
  parser.add_argument('destination', type=Path, help='Path to the destination directory')
  parser.add_argument(
    '--strategy',
    choices=('copy', 'delta'),
    default='copy',
    help='Copy files wholesale (default) or send rolling deltas.',
  )
  parser.add_argument(
    '--block-size',
    type=int,
    help='Block size (bytes) for the delta strategy.',
  )

  args = parser.parse_args()

  try:
    strategy = _build_strategy(args)
    sync(args.source, args.destination, strategy=strategy)
  except SyncError as exc:
    print(f'pysync: {exc}', file=sys.stderr)
    return 1
  except Exception as exc:  # pragma: no cover - CLI guardrail
    print(f'pysync: {exc}', file=sys.stderr)
    return 1

  if isinstance(strategy, DeltaSynchronizer):
    _print_delta_stats(strategy, args.destination)

  return 0


if __name__ == '__main__':
  raise SystemExit(main())
