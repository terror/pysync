import filecmp
import hashlib
import mmap
import os
import shutil
import tempfile
from contextlib import contextmanager, suppress
from dataclasses import dataclass
from pathlib import Path
from typing import IO, Iterator, Mapping, Protocol

from .error import SyncError
from .rolling_checksum import RollingChecksum
from .stats import SyncStats


class SyncStrategy(Protocol):
  """
  Strategy interface for updating an individual file in the destination tree.

  Concrete implementations decide how to reconcile differences between the
  source and destination files.
  """

  def sync_file(self, source: Path, destination: Path) -> None: ...


class FileCopierStrategy:
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


class DeltaStrategy:
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
