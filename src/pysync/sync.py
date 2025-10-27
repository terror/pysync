from __future__ import annotations

import filecmp
import hashlib
import os
import shutil
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Literal, Mapping, Protocol


class SyncError(Exception):
  """Raised when directory synchronisation fails."""


class SyncStrategy(Protocol):
  """
  Strategy interface for updating an individual file in the destination tree.

  Concrete implementations decide how to reconcile differences between the
  source and destination files.
  """

  def sync_file(self, source: Path, destination: Path) -> None: ...


class FileCopier:
  """Default strategy that mirrors files via whole-file copies."""

  def sync_file(self, source: Path, destination: Path) -> None:
    if not destination.exists() or not filecmp.cmp(source, destination, shallow=False):
      shutil.copy2(source, destination)


@dataclass(frozen=True)
class _CopyOp:
  block_index: int


@dataclass(frozen=True)
class _LiteralOp:
  data: bytes


@dataclass(frozen=True)
class SyncStats:
  total_bytes: int
  bytes_transferred: int
  bytes_reused: int

  @property
  def bytes_saved(self) -> int:
    return max(self.total_bytes - self.bytes_transferred, 0)


class _RollingChecksum:
  """Implements the rolling checksum described in the rsync algorithm."""

  _MOD = 1 << 16

  def __init__(self, block: bytes, block_size: int):
    self.block_size = block_size
    self.s1 = sum(block) % self._MOD
    self.s2 = (
      sum((self.block_size - idx + 1) * byte for idx, byte in enumerate(block, start=1)) % self._MOD
    )

  def digest(self) -> int:
    return (self.s2 << 16) | self.s1

  def roll(self, out_byte: int, in_byte: int) -> None:
    self.s1 = (self.s1 - out_byte + in_byte) % self._MOD
    self.s2 = (self.s2 - self.block_size * out_byte + self.s1) % self._MOD


class DeltaSynchronizer:
  """Synchronise files by transferring only changed blocks."""

  def __init__(self, block_size: int = 64 * 1024):
    if block_size <= 0:
      raise ValueError('block_size must be positive')
    self.block_size = block_size
    self._stats: dict[Path, SyncStats] = {}

  def sync_file(self, source: Path, destination: Path) -> None:
    destination_path = destination.resolve()

    src_bytes = source.read_bytes()

    if not destination.exists():
      shutil.copy2(source, destination)
      self._record_stats(destination_path, len(src_bytes), len(src_bytes), 0)
      return

    dst_bytes = destination.read_bytes()

    if not src_bytes:
      destination.write_bytes(b'')
      shutil.copystat(source, destination, follow_symlinks=False)
      self._record_stats(destination_path, 0, 0, 0)
      return

    if src_bytes == dst_bytes:
      shutil.copystat(source, destination, follow_symlinks=False)
      self._record_stats(destination_path, len(src_bytes), 0, len(src_bytes))
      return

    ops, dst_blocks = self._build_delta(src_bytes, dst_bytes)

    if not ops:
      # Files are identical; preserve metadata to mirror FileCopier behaviour.
      shutil.copystat(source, destination, follow_symlinks=False)
      self._record_stats(destination_path, len(src_bytes), 0, len(src_bytes))
      return

    literal_bytes = sum(len(op.data) for op in ops if isinstance(op, _LiteralOp))
    total_bytes = len(src_bytes)
    reused_bytes = max(total_bytes - literal_bytes, 0)
    self._apply_operations(destination, ops, dst_blocks, source)
    self._record_stats(destination_path, total_bytes, literal_bytes, reused_bytes)

  def _build_delta(
    self, src_bytes: bytes, dst_bytes: bytes
  ) -> tuple[list[_CopyOp | _LiteralOp], list[bytes]]:
    block_size = self.block_size

    if not dst_bytes or len(src_bytes) < block_size:
      return ([_LiteralOp(src_bytes)] if src_bytes else []), []

    dst_blocks = [dst_bytes[i : i + block_size] for i in range(0, len(dst_bytes), block_size)]
    signatures = self._index_destination_blocks(dst_blocks)

    ops: list[_CopyOp | _LiteralOp] = []
    src_len = len(src_bytes)
    idx = 0
    last_emitted = 0

    # Initialise rolling checksum for the first window.
    window = src_bytes[0:block_size]
    checksum = _RollingChecksum(window, block_size)

    while idx + block_size <= src_len:
      match_index = self._find_match(
        signatures, checksum.digest(), src_bytes[idx : idx + block_size]
      )

      if match_index is not None:
        if last_emitted < idx:
          ops.append(_LiteralOp(src_bytes[last_emitted:idx]))
        ops.append(_CopyOp(match_index))
        idx += block_size
        last_emitted = idx

        if idx + block_size <= src_len:
          window = src_bytes[idx : idx + block_size]
          checksum = _RollingChecksum(window, block_size)
        else:
          break
        continue

      if idx + block_size >= src_len:
        break

      out_byte = src_bytes[idx]
      in_byte = src_bytes[idx + block_size]
      checksum.roll(out_byte, in_byte)
      idx += 1

    if last_emitted < src_len:
      ops.append(_LiteralOp(src_bytes[last_emitted:]))

    return ops, dst_blocks

  def _index_destination_blocks(
    self, dst_blocks: list[bytes]
  ) -> dict[int, list[tuple[bytes, int]]]:
    signatures: dict[int, list[tuple[bytes, int]]] = {}

    for index, block in enumerate(dst_blocks):
      if not block:
        continue
      checksum = _RollingChecksum(block, len(block)).digest()
      strong = hashlib.md5(block).digest()
      signatures.setdefault(checksum, []).append((strong, index))

    return signatures

  def _find_match(
    self,
    signatures: dict[int, list[tuple[bytes, int]]],
    weak_checksum: int,
    window: bytes,
  ) -> int | None:
    candidates = signatures.get(weak_checksum)
    if not candidates:
      return None

    strong = hashlib.md5(window).digest()
    for candidate_strong, index in candidates:
      if candidate_strong == strong:
        return index
    return None

  def _apply_operations(
    self,
    destination: Path,
    ops: list[_CopyOp | _LiteralOp],
    dst_blocks: list[bytes],
    source: Path,
  ) -> None:
    with tempfile.NamedTemporaryFile(delete=False, dir=destination.parent) as tmp:
      for op in ops:
        if isinstance(op, _CopyOp):
          tmp.write(dst_blocks[op.block_index])
        else:
          tmp.write(op.data)
      temp_name = tmp.name

    os.replace(temp_name, destination)
    shutil.copystat(source, destination, follow_symlinks=False)

  def get_stats_for(self, path: Path) -> SyncStats | None:
    return self._stats.get(path.resolve())

  def stats(self) -> Mapping[Path, SyncStats]:
    return dict(self._stats)

  def _record_stats(self, destination: Path, total: int, transferred: int, reused: int) -> None:
    self._stats[destination] = SyncStats(
      total_bytes=total, bytes_transferred=transferred, bytes_reused=reused
    )


@dataclass(frozen=True)
class SyncAction:
  kind: Literal[
    'create_dir',
    'copy_file',
    'update_file',
    'remove_file',
    'remove_dir',
    'skip_file',
    'skip_dir',
  ]
  path: Path
  source: Path | None = None


ActionReporter = Callable[[SyncAction], None]


def _report_action(reporter: ActionReporter | None, action: SyncAction) -> None:
  if reporter is not None:
    reporter(action)


def _copy_directory_metadata(source: Path, destination: Path) -> None:
  """Replicate metadata like permissions and timestamps from source to destination."""
  shutil.copystat(source, destination, follow_symlinks=False)


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
  ``destination``.
  """
  src = Path(source)
  dst = Path(destination)

  if not src.exists():
    raise SyncError(f'Source directory does not exist: {src}')
  if not src.is_dir():
    raise SyncError(f'Source path is not a directory: {src}')

  synchroniser = strategy or FileCopier()
  tracked_dirs: set[Path] = set()

  if dst.exists():
    if not dst.is_dir():
      raise SyncError(f'Destination path is not a directory: {dst}')
    if verbose:
      _report_action(reporter, SyncAction('skip_dir', dst))
  else:
    _report_action(reporter, SyncAction('create_dir', dst))
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
    if path.exists():
      if not path.is_dir():
        raise SyncError(f'Cannot create directory because a file exists at {path}')
      return
    if path in tracked_dirs:
      return
    tracked_dirs.add(path)
    _report_action(reporter, SyncAction('create_dir', path))
    if not dry_run:
      path.mkdir(parents=True, exist_ok=True)
    if source_dir is not None:
      metadata_targets[path] = source_dir

  for item in src.rglob('*'):
    relative = item.relative_to(src)
    target = dst / relative

    if item.is_dir():
      metadata_targets[target] = item
      if not target.exists():
        ensure_directory(target, source_dir=item)
      elif verbose:
        _report_action(reporter, SyncAction('skip_dir', target))
      continue

    ensure_directory(target.parent, source_dir=item.parent)

    target_exists = target.exists()
    changed = True
    if target_exists:
      try:
        changed = not filecmp.cmp(item, target, shallow=False)
      except OSError:
        changed = True

    if changed:
      action = 'copy_file' if not target_exists else 'update_file'
      _report_action(reporter, SyncAction(action, target, source=item))
    elif verbose:
      _report_action(reporter, SyncAction('skip_file', target, source=item))

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
      _copy_directory_metadata(source_path, target_path)


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

    _report_action(reporter, SyncAction(action_kind, item))

    if dry_run:
      continue

    remover()
