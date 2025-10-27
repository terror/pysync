from __future__ import annotations

import filecmp
import hashlib
import mmap
import os
import shutil
import tempfile
from contextlib import contextmanager, suppress
from dataclasses import dataclass
from pathlib import Path
from typing import IO, Callable, Iterator, Literal, Mapping, Protocol

from .rolling_checksum import RollingChecksum


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
    if destination.is_symlink():
      raise SyncError(f'Refusing to write through symbolic link: {destination}')
    if not destination.exists() or not filecmp.cmp(source, destination, shallow=False):
      shutil.copy2(source, destination)


@dataclass(frozen=True)
class _BlockSignature:
  strong: bytes
  offset: int
  length: int


@dataclass(frozen=True)
class SyncStats:
  total_bytes: int
  bytes_transferred: int
  bytes_reused: int

  @property
  def bytes_saved(self) -> int:
    return max(self.total_bytes - self.bytes_transferred, 0)


_Buffer = bytes | mmap.mmap
_LITERAL_CHUNK_SIZE = 1 << 20  # 1 MiB chunks for streaming writes


@contextmanager
def _open_readonly_buffer(path: Path) -> Iterator[_Buffer]:
  size = path.stat().st_size
  if size == 0:
    yield b''
    return

  with path.open('rb') as fh:
    with mmap.mmap(fh.fileno(), length=0, access=mmap.ACCESS_READ) as mm:
      yield mm


class DeltaSynchronizer:
  """Synchronise files by transferring only changed blocks."""

  def __init__(self, block_size: int = 64 * 1024):
    if block_size <= 0:
      raise ValueError('block_size must be positive')
    self.block_size = block_size
    self._stats: dict[Path, SyncStats] = {}

  def sync_file(self, source: Path, destination: Path) -> None:
    if destination.is_symlink():
      raise SyncError(f'Refusing to write through symbolic link: {destination}')
    destination_path = destination.resolve()

    if not destination.exists():
      shutil.copy2(source, destination)
      size = source.stat().st_size
      self._record_stats(destination_path, size, size, 0)
      return

    src_size = source.stat().st_size

    if src_size == 0:
      if destination.exists():
        if destination.stat().st_size != 0:
          destination.write_bytes(b'')
        shutil.copystat(source, destination, follow_symlinks=False)
      self._record_stats(destination_path, 0, 0, 0)
      return

    if filecmp.cmp(source, destination, shallow=False):
      shutil.copystat(source, destination, follow_symlinks=False)
      self._record_stats(destination_path, src_size, 0, src_size)
      return

    with _open_readonly_buffer(source) as src_buf:
      src_len = len(src_buf)
      if src_len == 0:
        destination.write_bytes(b'')
        shutil.copystat(source, destination, follow_symlinks=False)
        self._record_stats(destination_path, 0, 0, 0)
        return

      with _open_readonly_buffer(destination) as dst_buf:
        temp_path, literal_bytes = self._write_delta(destination, src_buf, dst_buf)

    try:
      os.replace(temp_path, destination)
    except Exception:
      with suppress(FileNotFoundError):
        os.unlink(temp_path)
      raise

    shutil.copystat(source, destination, follow_symlinks=False)
    reused_bytes = max(src_len - literal_bytes, 0)
    self._record_stats(destination_path, src_len, literal_bytes, reused_bytes)

  def _write_delta(
    self,
    destination: Path,
    src_buf: _Buffer,
    dst_buf: _Buffer,
  ) -> tuple[str, int]:
    block_size = self.block_size
    src_len = len(src_buf)
    signatures = self._index_destination_blocks(dst_buf)

    literal_bytes = 0
    temp_path = ''

    try:
      with tempfile.NamedTemporaryFile(delete=False, dir=destination.parent) as tmp:
        temp_path = tmp.name
        if not signatures or src_len < block_size:
          literal_bytes = self._write_slice(tmp, src_buf, 0, src_len)
        else:
          idx = 0
          last_emitted = 0
          window = src_buf[0:block_size]
          checksum = RollingChecksum(window, block_size)

          while idx + block_size <= src_len:
            match = self._find_match(signatures, checksum.digest(), src_buf[idx : idx + block_size])

            if match is not None:
              if last_emitted < idx:
                literal_bytes += self._write_slice(tmp, src_buf, last_emitted, idx)
              self._copy_block(tmp, dst_buf, match.offset, match.length)
              idx += block_size
              last_emitted = idx

              if idx + block_size <= src_len:
                window = src_buf[idx : idx + block_size]
                checksum = RollingChecksum(window, block_size)
              else:
                break
              continue

            if idx + block_size >= src_len:
              break

            out_byte = src_buf[idx]
            in_byte = src_buf[idx + block_size]
            checksum.roll(out_byte, in_byte)
            idx += 1

          if last_emitted < src_len:
            literal_bytes += self._write_slice(tmp, src_buf, last_emitted, src_len)
    except Exception:
      if temp_path:
        Path(temp_path).unlink(missing_ok=True)
      raise

    return temp_path, literal_bytes

  def _index_destination_blocks(self, buffer: _Buffer) -> dict[int, list[_BlockSignature]]:
    block_size = self.block_size
    length = len(buffer)

    if length == 0:
      return {}

    signatures: dict[int, list[_BlockSignature]] = {}
    offset = 0

    while offset < length:
      end = min(offset + block_size, length)
      block = buffer[offset:end]
      if not block:
        break

      checksum = RollingChecksum(block, len(block)).digest()
      strong = hashlib.md5(block).digest()
      signatures.setdefault(checksum, []).append(
        _BlockSignature(strong=strong, offset=offset, length=len(block))
      )
      offset += block_size

    return signatures

  def _find_match(
    self,
    signatures: dict[int, list[_BlockSignature]],
    weak_checksum: int,
    window: bytes,
  ) -> _BlockSignature | None:
    candidates = signatures.get(weak_checksum)
    if not candidates:
      return None

    strong = hashlib.md5(window).digest()
    for candidate in candidates:
      if candidate.strong == strong:
        return candidate
    return None

  def _write_slice(self, writer: IO[bytes], buffer: _Buffer, start: int, end: int) -> int:
    if end <= start:
      return 0

    written = 0
    chunk_start = start
    while chunk_start < end:
      chunk_end = min(chunk_start + _LITERAL_CHUNK_SIZE, end)
      writer.write(buffer[chunk_start:chunk_end])
      written += chunk_end - chunk_start
      chunk_start = chunk_end
    return written

  def _copy_block(self, writer: IO[bytes], buffer: _Buffer, offset: int, length: int) -> None:
    end = offset + max(length, 0)
    self._write_slice(writer, buffer, offset, end)

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
    'create_symlink',
    'update_symlink',
    'remove_file',
    'remove_dir',
    'skip_file',
    'skip_dir',
    'skip_symlink',
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
  ``destination``. Symbolic links are reproduced as links rather than being
  dereferenced.
  """
  src = Path(source)
  dst = Path(destination)

  if not src.exists():
    raise SyncError(f'Source directory does not exist: {src}')
  if not src.is_dir():
    raise SyncError(f'Source path is not a directory: {src}')

  synchroniser = strategy or FileCopier()
  tracked_dirs: set[Path] = set()

  if dst.is_symlink():
    raise SyncError(f'Destination path is a symbolic link: {dst}')
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
    was_symlink = path.is_symlink()
    if was_symlink:
      _report_action(reporter, SyncAction('remove_file', path))
      if not dry_run:
        path.unlink()

    if not was_symlink and path.exists():
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
        _report_action(reporter, SyncAction('skip_symlink', target, source=item))
      return

    action = 'create_symlink' if not target_exists else 'update_symlink'
    _report_action(reporter, SyncAction(action, target, source=item))

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
        _report_action(reporter, SyncAction('skip_dir', target))

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
        _report_action(reporter, SyncAction('remove_file', target))
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
