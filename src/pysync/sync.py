from __future__ import annotations

import filecmp
import shutil
from pathlib import Path


class SyncError(Exception):
  """Raised when directory synchronisation fails."""


def sync(source: Path | str, destination: Path | str) -> None:
  """
  Mirror the contents of ``source`` into ``destination``.

  Missing directories are created, files are copied when their content differs,
  and files/directories that are absent in ``source`` are removed from
  ``destination``.
  """
  src = Path(source)
  dst = Path(destination)

  if not src.exists():
    raise SyncError(f'Source directory does not exist: {src}')
  if not src.is_dir():
    raise SyncError(f'Source path is not a directory: {src}')

  dst.mkdir(parents=True, exist_ok=True)

  try:
    _copy_missing_and_updated(src, dst)
    _remove_extraneous(src, dst)
  except OSError as exc:
    raise SyncError(str(exc)) from exc


def _copy_missing_and_updated(src: Path, dst: Path) -> None:
  for item in src.rglob('*'):
    relative = item.relative_to(src)
    target = dst / relative

    if item.is_dir():
      target.mkdir(parents=True, exist_ok=True)
      continue

    target.parent.mkdir(parents=True, exist_ok=True)

    if not target.exists() or not filecmp.cmp(item, target, shallow=False):
      shutil.copy2(item, target)


def _remove_extraneous(src: Path, dst: Path) -> None:
  def _depth(p: Path) -> int:
    return len(p.relative_to(dst).parts)

  for item in sorted(dst.rglob('*'), key=_depth, reverse=True):
    origin = src / item.relative_to(dst)
    if origin.exists():
      continue

    if item.is_dir():
      item.rmdir()
    else:
      item.unlink()
