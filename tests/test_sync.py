import sys
from pathlib import Path

import pytest

from pysync.__main__ import main as cli_main
from pysync.sync import DeltaSynchronizer, SyncError, SyncStats, sync


def create_file(path: Path, content: str) -> None:
  path.parent.mkdir(parents=True, exist_ok=True)
  path.write_text(content)


def test_sync_copies_new_files(tmp_path: Path) -> None:
  src = tmp_path / 'src'
  dst = tmp_path / 'dst'
  src.mkdir()

  create_file(src / 'example.txt', 'hello')

  sync(src, dst)

  assert (dst / 'example.txt').read_text() == 'hello'


def test_sync_updates_changed_files(tmp_path: Path) -> None:
  src = tmp_path / 'src'
  dst = tmp_path / 'dst'
  src.mkdir()
  dst.mkdir()

  create_file(src / 'file.txt', 'new content')
  create_file(dst / 'file.txt', 'old content')

  sync(src, dst)

  assert (dst / 'file.txt').read_text() == 'new content'


def test_sync_removes_extraneous_files(tmp_path: Path) -> None:
  src = tmp_path / 'src'
  dst = tmp_path / 'dst'
  src.mkdir()
  dst.mkdir()

  create_file(src / 'kept.txt', 'keep me')
  create_file(dst / 'remove.txt', 'to be removed')

  sync(src, dst)

  assert (dst / 'kept.txt').exists()
  assert not (dst / 'remove.txt').exists()


def test_sync_handles_nested_directories(tmp_path: Path) -> None:
  src = tmp_path / 'src'
  dst = tmp_path / 'dst'
  src.mkdir()

  create_file(src / 'a' / 'b' / 'c.txt', 'nested')

  sync(src, dst)

  assert (dst / 'a' / 'b' / 'c.txt').read_text() == 'nested'


def test_sync_raises_for_missing_source(tmp_path: Path) -> None:
  src = tmp_path / 'missing'
  dst = tmp_path / 'dst'

  with pytest.raises(SyncError):
    sync(src, dst)


def test_delta_sync_reuses_existing_blocks(tmp_path: Path) -> None:
  src_dir = tmp_path / 'src'
  dst_dir = tmp_path / 'dst'
  src_dir.mkdir()
  dst_dir.mkdir()

  src_file = src_dir / 'file.bin'
  dst_file = dst_dir / 'file.bin'

  block_size = 4
  original = b'AAAA' + b'BBBB' + b'CCCC' + b'DDDD' + b'EEEE'
  modified = b'AAAA' + b'ZZZZ' + b'CCCC' + b'DDDD' + b'EEEE'

  dst_file.write_bytes(original)
  src_file.write_bytes(modified)

  strategy = DeltaSynchronizer(block_size=block_size)

  sync(src_dir, dst_dir, strategy=strategy)

  assert dst_file.read_bytes() == modified
  stats = strategy.get_stats_for(dst_file)
  assert isinstance(stats, SyncStats)
  assert stats.total_bytes == len(modified)
  assert stats.bytes_transferred == block_size
  assert stats.bytes_reused == len(modified) - block_size


def test_delta_sync_handles_missing_destination(tmp_path: Path) -> None:
  src_dir = tmp_path / 'src'
  dst_dir = tmp_path / 'dst'
  src_dir.mkdir()

  src_file = src_dir / 'file.txt'
  src_file.write_text('content')

  strategy = DeltaSynchronizer()
  assert strategy.get_stats_for(dst_dir / 'file.txt') is None

  sync(src_dir, dst_dir, strategy=strategy)
  result_file = dst_dir / 'file.txt'

  assert result_file.read_text() == 'content'

  stats = strategy.get_stats_for(result_file)
  assert isinstance(stats, SyncStats)
  assert stats.total_bytes == len('content')
  assert stats.bytes_transferred == len('content')
  assert stats.bytes_reused == 0


def test_delta_sync_truncates_when_source_shrinks(tmp_path: Path) -> None:
  src_dir = tmp_path / 'src'
  dst_dir = tmp_path / 'dst'
  src_dir.mkdir()
  dst_dir.mkdir()

  src_file = src_dir / 'file.txt'
  dst_file = dst_dir / 'file.txt'

  dst_file.write_text('some longer content')
  src_file.write_text('')

  strategy = DeltaSynchronizer()
  assert strategy.get_stats_for(dst_dir / 'file.txt') is None

  sync(src_dir, dst_dir, strategy=strategy)
  result_file = dst_dir / 'file.txt'

  assert result_file.read_text() == ''

  stats = strategy.get_stats_for(result_file)
  assert isinstance(stats, SyncStats)
  assert stats.total_bytes == 0
  assert stats.bytes_transferred == 0
  assert stats.bytes_reused == 0


def test_cli_copy_strategy_succeeds_without_stats(
  tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
  src = tmp_path / 'src'
  dst = tmp_path / 'dst'
  src.mkdir()
  dst.mkdir()

  create_file(src / 'file.txt', 'hello')

  monkeypatch.setattr(sys, 'argv', ['pysync', str(src), str(dst), '--strategy', 'copy'])

  exit_code = cli_main()

  assert exit_code == 0
  captured = capsys.readouterr()
  assert 'Total: transferred' not in captured.out


def test_cli_delta_strategy_reports_stats(
  tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
  src = tmp_path / 'src'
  dst = tmp_path / 'dst'
  src.mkdir()
  dst.mkdir()

  create_file(src / 'file.bin', 'abcdZzzz')
  (dst / 'file.bin').write_text('abcdBBBB')

  monkeypatch.setattr(
    sys, 'argv', ['pysync', str(src), str(dst), '--strategy', 'delta', '--block-size', '4']
  )

  exit_code = cli_main()

  assert exit_code == 0
  captured = capsys.readouterr()
  assert 'file.bin' in captured.out
  assert 'Total: transferred' in captured.out
