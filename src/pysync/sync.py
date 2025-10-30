from __future__ import annotations

import filecmp
import os
import shutil
from pathlib import Path

from .action import ActionReporter, SyncAction, report_action
from .error import SyncError
from .strategy import FileCopierStrategy, SyncStrategy


def sync(
  source: Path | str,
  destination: Path | str,
  strategy: SyncStrategy | None = None,
  *,
  dry_run: bool = False,
  reporter: ActionReporter | None = None,
  verbose: bool = False,
) -> None:
  """
  Mirror the contents of ``source`` into ``destination``.

  Missing directories are created, files are copied when their content differs,
  and files/directories that are absent in ``source`` are removed from
  ``destination``. Symbolic links are reproduced as links rather than being
  dereferenced.
  """
  src = Path(source)
  dst = Path(destination)

  if not src.exists():
    raise SyncError(f'Source directory does not exist: {src}')

  if not src.is_dir():
    raise SyncError(f'Source path is not a directory: {src}')

  synchroniser = strategy or FileCopierStrategy()
  tracked_dirs: set[Path] = set()

  if dst.is_symlink():
    raise SyncError(f'Destination path is a symbolic link: {dst}')

  if dst.exists():
    if not dst.is_dir():
      raise SyncError(f'Destination path is not a directory: {dst}')

    if verbose:
      report_action(reporter, SyncAction('skip_dir', dst))
  else:
    report_action(reporter, SyncAction('create_dir', dst))
    tracked_dirs.add(dst)
    if not dry_run:
      dst.mkdir(parents=True, exist_ok=True)

  try:
    _copy_missing_and_updated(
      src,
      dst,
      synchroniser,
      dry_run=dry_run,
      reporter=reporter,
      verbose=verbose,
      created_dirs=tracked_dirs,
    )

    if dst.exists():
      _remove_extraneous(src, dst, dry_run=dry_run, reporter=reporter)
  except OSError as exc:
    raise SyncError(str(exc)) from exc


def _copy_missing_and_updated(
  src: Path,
  dst: Path,
  strategy: SyncStrategy,
  *,
  dry_run: bool,
  reporter: ActionReporter | None,
  verbose: bool,
  created_dirs: set[Path],
) -> None:
  tracked_dirs = created_dirs
  metadata_targets: dict[Path, Path] = {dst: src}

  def ensure_directory(path: Path, source_dir: Path | None = None) -> None:
    was_symlink = path.is_symlink()

    if was_symlink:
      report_action(reporter, SyncAction('remove_file', path))
      if not dry_run:
        path.unlink()

    if not was_symlink and path.exists():
      if not path.is_dir():
        raise SyncError(f'Cannot create directory because a file exists at {path}')
      return

    if path in tracked_dirs:
      return

    tracked_dirs.add(path)
    report_action(reporter, SyncAction('create_dir', path))

    if not dry_run:
      path.mkdir(parents=True, exist_ok=True)

    if source_dir is not None:
      metadata_targets[path] = source_dir

  def handle_symlink(item: Path) -> None:
    relative = item.relative_to(src)
    target = dst / relative

    ensure_directory(target.parent, source_dir=item.parent)

    link_target = os.readlink(item)
    target_is_symlink = target.is_symlink()
    target_exists = target_is_symlink or target.exists()

    same_link = False

    if target_is_symlink:
      try:
        same_link = os.readlink(target) == link_target
      except OSError:
        same_link = False

    if same_link:
      if verbose:
        report_action(reporter, SyncAction('skip_symlink', target, source=item))
      return

    action = 'create_symlink' if not target_exists else 'update_symlink'
    report_action(reporter, SyncAction(action, target, source=item))

    if dry_run:
      return

    if target_exists:
      if target_is_symlink:
        target.unlink()
      elif target.is_dir():
        shutil.rmtree(target)
      else:
        target.unlink()

    target_is_directory = item.is_dir()
    os.symlink(link_target, target, target_is_directory=target_is_directory)

  for root, dirnames, filenames in os.walk(src, topdown=True, followlinks=False):
    root_path = Path(root)

    for name in list(dirnames):
      item = root_path / name

      if item.is_symlink():
        handle_symlink(item)
        dirnames.remove(name)
        continue

      relative = item.relative_to(src)
      target = dst / relative

      metadata_targets[target] = item

      if target.is_symlink() or not target.exists():
        ensure_directory(target, source_dir=item)
      elif verbose:
        report_action(reporter, SyncAction('skip_dir', target))

    for name in filenames:
      item = root_path / name

      if item.is_symlink():
        handle_symlink(item)
        continue

      relative = item.relative_to(src)
      target = dst / relative

      ensure_directory(target.parent, source_dir=item.parent)

      target_is_symlink = target.is_symlink()
      target_exists = target.exists() if not target_is_symlink else False

      if target_is_symlink:
        report_action(reporter, SyncAction('remove_file', target))

        if not dry_run:
          target.unlink()

      changed = True

      if target_exists:
        try:
          changed = not filecmp.cmp(item, target, shallow=False)
        except OSError:
          changed = True

      if changed:
        action = 'copy_file' if not target_exists else 'update_file'
        report_action(reporter, SyncAction(action, target, source=item))
      elif verbose:
        report_action(reporter, SyncAction('skip_file', target, source=item))

      if dry_run:
        continue

      strategy.sync_file(item, target)

  if dry_run:
    return

  def _dir_depth(path: Path) -> int:
    if path == dst:
      return 0
    return len(path.relative_to(dst).parts)

  for target_path, source_path in sorted(
    metadata_targets.items(), key=lambda pair: _dir_depth(pair[0]), reverse=True
  ):
    if target_path.exists():
      shutil.copystat(source_path, target_path, follow_symlinks=False)


def _remove_extraneous(
  src: Path,
  dst: Path,
  *,
  dry_run: bool,
  reporter: ActionReporter | None,
) -> None:
  def _depth(p: Path) -> int:
    return len(p.relative_to(dst).parts)

  if not dst.exists():
    return

  for item in sorted(dst.rglob('*'), key=_depth, reverse=True):
    origin = src / item.relative_to(dst)

    if origin.exists():
      continue

    if item.is_dir() and not item.is_symlink():
      action_kind = 'remove_dir'
      remover = item.rmdir
    else:
      action_kind = 'remove_file'
      remover = item.unlink

    report_action(reporter, SyncAction(action_kind, item))

    if dry_run:
      continue

    remover()
