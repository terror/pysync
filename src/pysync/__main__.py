from __future__ import annotations

from pathlib import Path
from typing import Callable, Optional, Tuple

from rich.console import Console
from rich.progress import (
  BarColumn,
  MofNCompleteColumn,
  Progress,
  TaskID,
  TaskProgressColumn,
  TextColumn,
  TimeRemainingColumn,
)
from rich.table import Table

from pysync.arguments import Arguments, Strategy
from pysync.strategy import DeltaStrategy, FileCopierStrategy
from pysync.sync import SyncAction, SyncError, SyncStrategy, sync


class _ProgressStrategy(SyncStrategy):
  """Wraps another strategy to surface per-file progress updates."""

  def __init__(self, delegate: SyncStrategy, progress: Progress, task_id: TaskID) -> None:
    self.delegate = delegate
    self.progress = progress
    self.task_id: TaskID = task_id

  def sync_file(self, source: Path, destination: Path) -> None:
    self.delegate.sync_file(source, destination)
    self.progress.advance(self.task_id)


def _build_strategy(args: Arguments) -> SyncStrategy:
  if args.strategy == Strategy.COPY:
    if args.block_size is not None:
      raise SyncError('--block-size can only be used with --strategy delta')

    return FileCopierStrategy()

  if args.block_size is not None and args.block_size <= 0:
    raise SyncError('--block-size must be a positive integer')

  block_size = args.block_size if args.block_size is not None else 64 * 1024

  return DeltaStrategy(block_size=block_size)


def _print_delta_stats(strategy: DeltaStrategy, destination: Path, console: Console) -> None:
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
    'create_symlink': 'create symlink',
    'update_symlink': 'update symlink',
    'remove_file': 'remove file',
    'remove_dir': 'remove dir',
    'skip_file': 'skip file',
    'skip_dir': 'skip dir',
    'skip_symlink': 'skip symlink',
  }

  prefix = 'DRY RUN: ' if dry_run else ''

  def reporter(action: SyncAction) -> None:
    message = f'{labels[action.kind]}: {_format_relative(action.path, dst_root)}'

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

  task_id: TaskID = progress.add_task('Syncing', total=total_files)

  return _ProgressStrategy(strategy, progress, task_id), progress


def main() -> int:
  arguments = Arguments.from_args()

  console, err_console = Console(), Console(stderr=True)

  try:
    base_strategy = _build_strategy(arguments)

    progress_enabled = not (arguments.dry_run or arguments.verbose)

    strategy, progress_cm = _wrap_with_progress(
      base_strategy, arguments.source, console, enable_progress=progress_enabled
    )

    reporter = None

    if arguments.dry_run or arguments.verbose:
      reporter = _make_console_reporter(
        console, arguments.source, arguments.destination, arguments.dry_run
      )
    if progress_cm is not None:
      with progress_cm:
        sync(
          arguments.source,
          arguments.destination,
          strategy=strategy,
          dry_run=arguments.dry_run,
          reporter=reporter,
          verbose=arguments.verbose,
        )
    else:
      sync(
        arguments.source,
        arguments.destination,
        strategy=strategy,
        dry_run=arguments.dry_run,
        reporter=reporter,
        verbose=arguments.verbose,
      )
  except SyncError as exc:
    err_console.print(f'[bold red]error:[/] {exc}')
    return 1
  except Exception as exc:  # pragma: no cover - CLI guardrail
    err_console.print(f'[bold red]error:[/] {exc}')
    return 1

  if arguments.dry_run:
    console.print('[bold yellow]Dry run complete; no changes were made.[/]')
    return 0

  if isinstance(base_strategy, DeltaStrategy):
    _print_delta_stats(base_strategy, arguments.destination, console)

  return 0


if __name__ == '__main__':
  raise SystemExit(main())
