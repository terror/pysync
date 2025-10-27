from __future__ import annotations

import argparse
from pathlib import Path
from typing import Callable, Optional, Tuple

from rich.console import Console
from rich.progress import (
  BarColumn,
  MofNCompleteColumn,
  Progress,
  TaskProgressColumn,
  TextColumn,
  TimeRemainingColumn,
)
from rich.table import Table

from pysync.sync import DeltaSynchronizer, FileCopier, SyncAction, SyncError, SyncStrategy, sync


class _ProgressStrategy(SyncStrategy):
  """Wraps another strategy to surface per-file progress updates."""

  def __init__(self, delegate: SyncStrategy, progress: Progress, task_id: int) -> None:
    self.delegate = delegate
    self.progress = progress
    self.task_id = task_id

  def sync_file(self, source: Path, destination: Path) -> None:
    self.delegate.sync_file(source, destination)
    self.progress.advance(self.task_id)


def _build_strategy(args: argparse.Namespace) -> SyncStrategy:
  if args.strategy == 'copy':
    if args.block_size is not None:
      raise SyncError('--block-size can only be used with --strategy delta')
    return FileCopier()

  if args.block_size is not None and args.block_size <= 0:
    raise SyncError('--block-size must be a positive integer')

  block_size = args.block_size if args.block_size is not None else 64 * 1024
  return DeltaSynchronizer(block_size=block_size)


def _print_delta_stats(strategy: DeltaSynchronizer, destination: Path, console: Console) -> None:
  stats = strategy.stats()
  if not stats:
    console.print('[bold cyan]Delta transfer stats:[/] no files processed.')
    return

  dest_root = destination.resolve()
  total_bytes = 0
  transferred = 0
  reused = 0

  table = Table(show_lines=True)
  table.add_column('File', overflow='fold')
  table.add_column('Transferred')
  table.add_column('Reused')
  table.add_column('Saved')

  for path, entry in sorted(stats.items()):
    total_bytes += entry.total_bytes
    transferred += entry.bytes_transferred
    reused += entry.bytes_reused
    try:
      display = path.relative_to(dest_root)
    except ValueError:
      display = path
    table.add_row(
      str(display),
      f'{entry.bytes_transferred:,} B',
      f'{entry.bytes_reused:,} B',
      f'{entry.bytes_saved:,} B',
    )

  bytes_saved = max(total_bytes - transferred, 0)
  console.print(table)
  console.print(
    '[bold green]Total:[/] '
    f'transferred {transferred:,} bytes | '
    f'reused {reused:,} bytes | '
    f'saved {bytes_saved:,} bytes'
  )


def _count_source_files(source: Path) -> int:
  try:
    return sum(1 for item in source.rglob('*') if item.is_file())
  except FileNotFoundError:
    return 0


def _format_relative(path: Path, root: Path) -> str:
  resolved_root = root.resolve()
  try:
    return str(path.resolve().relative_to(resolved_root)) or '.'
  except Exception:
    return str(path.resolve())


def _make_console_reporter(
  console: Console, source_root: Path, dest_root: Path, dry_run: bool
) -> Callable[[SyncAction], None]:
  src_root = source_root.resolve()
  dst_root = dest_root.resolve()
  labels = {
    'create_dir': 'create dir',
    'copy_file': 'copy file',
    'update_file': 'update file',
    'remove_file': 'remove file',
    'remove_dir': 'remove dir',
    'skip_file': 'skip file',
    'skip_dir': 'skip dir',
  }
  prefix = 'DRY RUN: ' if dry_run else ''

  def reporter(action: SyncAction) -> None:
    label = labels[action.kind]
    target_display = _format_relative(action.path, dst_root)
    message = f'{label}: {target_display}'
    if action.source is not None:
      message += f' (from {_format_relative(action.source, src_root)})'
    console.print(prefix + message)

  return reporter


def _wrap_with_progress(
  strategy: SyncStrategy, source: Path, console: Console, *, enable_progress: bool = True
) -> Tuple[SyncStrategy, Optional[Progress]]:
  total_files = _count_source_files(source)
  progress = Progress(
    TextColumn('[progress.description]{task.description}'),
    BarColumn(),
    TaskProgressColumn(),
    MofNCompleteColumn(),
    TimeRemainingColumn(),
    console=console,
    transient=True,
    disable=(not enable_progress) or (not console.is_interactive) or total_files == 0,
  )

  if progress.disable:
    return strategy, None

  task_id = progress.add_task('Syncing', total=total_files)

  wrapped = _ProgressStrategy(strategy, progress, task_id)
  return wrapped, progress


def main() -> int:
  console = Console()
  err_console = Console(stderr=True)
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
  parser.add_argument(
    '--dry-run',
    action='store_true',
    help='Preview sync actions without modifying the destination.',
  )
  parser.add_argument(
    '-v',
    '--verbose',
    action='store_true',
    help='Log each action as it occurs.',
  )

  args = parser.parse_args()

  try:
    base_strategy = _build_strategy(args)
    progress_enabled = not (args.dry_run or args.verbose)
    strategy, progress_cm = _wrap_with_progress(
      base_strategy, args.source, console, enable_progress=progress_enabled
    )
    reporter = None
    if args.dry_run or args.verbose:
      reporter = _make_console_reporter(console, args.source, args.destination, args.dry_run)
    if progress_cm is not None:
      with progress_cm:
        sync(
          args.source,
          args.destination,
          strategy=strategy,
          dry_run=args.dry_run,
          reporter=reporter,
          verbose=args.verbose,
        )
    else:
      sync(
        args.source,
        args.destination,
        strategy=strategy,
        dry_run=args.dry_run,
        reporter=reporter,
        verbose=args.verbose,
      )
  except SyncError as exc:
    err_console.print(f'[bold red]error:[/] {exc}')
    return 1
  except Exception as exc:  # pragma: no cover - CLI guardrail
    err_console.print(f'[bold red]error:[/] {exc}')
    return 1

  if args.dry_run:
    console.print('[bold yellow]Dry run complete; no changes were made.[/]')
    return 0

  if isinstance(base_strategy, DeltaSynchronizer):
    _print_delta_stats(base_strategy, args.destination, console)

  return 0


if __name__ == '__main__':
  raise SystemExit(main())
