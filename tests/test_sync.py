import importlib
import os
import stat
import sys
from pathlib import Path

import pytest

from pysync.__main__ import main as cli_main
from pysync.stats import SyncStats
from pysync.strategy import DeltaStrategy
from pysync.sync import SyncAction, SyncError, sync

sync_module = importlib.import_module('pysync.sync')


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


@pytest.mark.skipif(not hasattr(os, 'symlink'), reason='symlink not supported')
def test_sync_removes_extraneous_directory_symlinks(tmp_path: Path) -> None:
  src = tmp_path / 'src'
  dst = tmp_path / 'dst'
  src.mkdir()
  dst.mkdir()

  target = tmp_path / 'target'
  target.mkdir()
  link = dst / 'link'
  os.symlink(target, link, target_is_directory=True)

  sync(src, dst)

  assert not link.exists()


@pytest.mark.skipif(not hasattr(os, 'symlink'), reason='symlink not supported')
def test_sync_preserves_file_symlinks(tmp_path: Path) -> None:
  src = tmp_path / 'src'
  dst = tmp_path / 'dst'
  src.mkdir()
  dst.mkdir()

  target = tmp_path / 'target.txt'
  create_file(target, 'target')

  link = src / 'link.txt'
  os.symlink(target, link)

  existing = dst / 'link.txt'
  create_file(existing, 'stale')

  sync(src, dst)

  dest_link = dst / 'link.txt'
  assert dest_link.is_symlink()
  assert os.readlink(dest_link) == os.readlink(link)


@pytest.mark.skipif(not hasattr(os, 'symlink'), reason='symlink not supported')
def test_sync_preserves_directory_symlinks(tmp_path: Path) -> None:
  src = tmp_path / 'src'
  dst = tmp_path / 'dst'
  src.mkdir()
  dst.mkdir()

  target_dir = tmp_path / 'target'
  target_dir.mkdir()
  create_file(target_dir / 'file.txt', 'content')

  link = src / 'link'
  os.symlink(target_dir, link, target_is_directory=True)

  stale_dir = dst / 'link'
  stale_dir.mkdir()
  create_file(stale_dir / 'stale.txt', 'stale')

  sync(src, dst)

  dest_link = dst / 'link'
  assert dest_link.is_symlink()
  assert os.readlink(dest_link) == os.readlink(link)
  assert not (dest_link / 'stale.txt').exists()


@pytest.mark.skipif(not hasattr(os, 'symlink'), reason='symlink not supported')
def test_sync_updates_changed_symlink_targets(tmp_path: Path) -> None:
  src = tmp_path / 'src'
  dst = tmp_path / 'dst'
  src.mkdir()
  dst.mkdir()

  old_target = tmp_path / 'old.txt'
  new_target = tmp_path / 'new.txt'
  create_file(old_target, 'old')
  create_file(new_target, 'new')

  link_name = 'link.txt'
  os.symlink(old_target, src / link_name)
  os.symlink(new_target, dst / link_name)

  sync(src, dst)

  dest_link = dst / link_name
  assert dest_link.is_symlink()
  assert os.readlink(dest_link) == os.readlink(src / link_name)


@pytest.mark.skipif(not hasattr(os, 'symlink'), reason='symlink not supported')
def test_sync_removes_extraneous_file_symlinks(tmp_path: Path) -> None:
  src = tmp_path / 'src'
  dst = tmp_path / 'dst'
  src.mkdir()
  dst.mkdir()

  target = tmp_path / 'target.txt'
  create_file(target, 'content')

  stale_link = dst / 'stale.txt'
  os.symlink(target, stale_link)

  sync(src, dst)

  assert not stale_link.exists()


@pytest.mark.skipif(not hasattr(os, 'symlink'), reason='symlink not supported')
def test_sync_rejects_symlink_destination_root(tmp_path: Path) -> None:
  src = tmp_path / 'src'
  dst_real = tmp_path / 'dst-real'
  dst_link = tmp_path / 'dst'
  src.mkdir()
  dst_real.mkdir()

  create_file(src / 'file.txt', 'content')

  os.symlink(dst_real, dst_link, target_is_directory=True)

  with pytest.raises(SyncError, match='symbolic link'):
    sync(src, dst_link)


@pytest.mark.skipif(not hasattr(os, 'symlink'), reason='symlink not supported')
def test_sync_replaces_destination_file_symlinks(tmp_path: Path) -> None:
  src = tmp_path / 'src'
  dst = tmp_path / 'dst'
  src.mkdir()
  dst.mkdir()

  create_file(src / 'subdir' / 'file.txt', 'new content')

  outside = tmp_path / 'outside.txt'
  create_file(outside, 'outside')

  (dst / 'subdir').mkdir()
  os.symlink(outside, dst / 'subdir' / 'file.txt')

  sync(src, dst)

  target_file = dst / 'subdir' / 'file.txt'
  assert target_file.exists()
  assert not target_file.is_symlink()
  assert target_file.read_text() == 'new content'
  assert outside.read_text() == 'outside'


@pytest.mark.skipif(not hasattr(os, 'symlink'), reason='symlink not supported')
def test_sync_replaces_destination_directory_symlinks(tmp_path: Path) -> None:
  src = tmp_path / 'src'
  dst = tmp_path / 'dst'
  src.mkdir()
  dst.mkdir()

  create_file(src / 'subdir' / 'file.txt', 'content')

  outside_dir = tmp_path / 'outside'
  outside_dir.mkdir()

  os.symlink(outside_dir, dst / 'subdir', target_is_directory=True)

  sync(src, dst)

  dest_subdir = dst / 'subdir'
  assert dest_subdir.exists()
  assert dest_subdir.is_dir()
  assert not dest_subdir.is_symlink()
  assert (dest_subdir / 'file.txt').read_text() == 'content'
  assert not (outside_dir / 'file.txt').exists()


def test_sync_handles_nested_directories(tmp_path: Path) -> None:
  src = tmp_path / 'src'
  dst = tmp_path / 'dst'
  src.mkdir()

  create_file(src / 'a' / 'b' / 'c.txt', 'nested')

  sync(src, dst)

  assert (dst / 'a' / 'b' / 'c.txt').read_text() == 'nested'


def test_sync_preserves_directory_metadata(tmp_path: Path) -> None:
  src = tmp_path / 'src'
  dst = tmp_path / 'dst'
  src.mkdir()

  nested = src / 'nested'
  nested.mkdir()

  src_mode = 0o742
  nested_mode = 0o751

  src_timestamp = 1_700_000_000
  nested_timestamp = 1_700_000_100

  os.utime(src, (src_timestamp, src_timestamp))
  src.chmod(src_mode)

  os.utime(nested, (nested_timestamp, nested_timestamp))
  nested.chmod(nested_mode)

  src_stat = src.stat()
  nested_stat = nested.stat()

  sync(src, dst)

  dst_stat = dst.stat()
  dst_nested = dst / 'nested'
  dst_nested_stat = dst_nested.stat()

  assert dst_nested.exists()
  assert stat.S_IMODE(dst_stat.st_mode) == stat.S_IMODE(src_stat.st_mode)
  assert stat.S_IMODE(dst_nested_stat.st_mode) == stat.S_IMODE(nested_stat.st_mode)
  assert dst_stat.st_mtime == pytest.approx(src_stat.st_mtime, abs=1)
  assert dst_nested_stat.st_mtime == pytest.approx(nested_stat.st_mtime, abs=1)


def test_sync_raises_for_missing_source(tmp_path: Path) -> None:
  src = tmp_path / 'missing'
  dst = tmp_path / 'dst'

  with pytest.raises(SyncError):
    sync(src, dst)


def test_sync_wraps_permission_errors(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
  src = tmp_path / 'src'
  dst = tmp_path / 'dst'
  src.mkdir()

  create_file(src / 'file.txt', 'content')

  def fail_copy(*_: object, **__: object) -> None:
    raise PermissionError('permission denied')

  monkeypatch.setattr(sync_module.shutil, 'copy2', fail_copy)

  with pytest.raises(SyncError) as excinfo:
    sync(src, dst)

  assert 'permission denied' in str(excinfo.value)


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

  strategy = DeltaStrategy(block_size=block_size)

  sync(src_dir, dst_dir, strategy=strategy)

  assert dst_file.read_bytes() == modified


def test_cli_reports_sync_errors(
  tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
  src = tmp_path / 'src'
  dst = tmp_path / 'dst'
  src.mkdir()

  def fail_sync(
    *args: object, **kwargs: object
  ) -> None:  # pragma: no cover - behaviour verified via CLI expectation
    raise SyncError('boom')

  monkeypatch.setattr('pysync.__main__.sync', fail_sync)
  monkeypatch.setattr(sys, 'argv', ['pysync', str(src), str(dst)])

  exit_code = cli_main()
  captured = capsys.readouterr()

  assert exit_code == 1
  assert 'error:' in captured.err
  assert 'boom' in captured.err


def test_delta_sync_handles_missing_destination(tmp_path: Path) -> None:
  src_dir = tmp_path / 'src'
  dst_dir = tmp_path / 'dst'
  src_dir.mkdir()

  src_file = src_dir / 'file.txt'
  src_file.write_text('content')

  strategy = DeltaStrategy()
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

  strategy = DeltaStrategy()
  assert strategy.get_stats_for(dst_dir / 'file.txt') is None

  sync(src_dir, dst_dir, strategy=strategy)
  result_file = dst_dir / 'file.txt'

  assert result_file.read_text() == ''

  stats = strategy.get_stats_for(result_file)
  assert isinstance(stats, SyncStats)
  assert stats.total_bytes == 0
  assert stats.bytes_transferred == 0
  assert stats.bytes_reused == 0


def test_sync_dry_run_reports_actions(tmp_path: Path) -> None:
  src = tmp_path / 'src'
  dst = tmp_path / 'dst'
  src.mkdir()
  dst.mkdir()

  create_file(src / 'new.txt', 'new')
  create_file(src / 'changed.txt', 'updated')
  create_file(src / 'nested' / 'child.txt', 'nested')
  create_file(dst / 'changed.txt', 'stale')
  create_file(dst / 'remove.txt', 'remove me')
  create_file(src / 'unchanged.txt', 'same')
  create_file(dst / 'unchanged.txt', 'same')

  actions: list[SyncAction] = []

  def reporter(action: SyncAction) -> None:
    actions.append(action)

  sync(src, dst, dry_run=True, reporter=reporter)

  def has_action(kind: str, path: Path) -> bool:
    return any(a.kind == kind and a.path == path for a in actions)

  assert has_action('copy_file', dst / 'new.txt')
  assert has_action('update_file', dst / 'changed.txt')
  assert has_action('remove_file', dst / 'remove.txt')
  assert has_action('create_dir', dst / 'nested')

  assert not (dst / 'new.txt').exists()
  assert (dst / 'changed.txt').read_text() == 'stale'
  assert (dst / 'remove.txt').exists()
  assert not (dst / 'nested').exists()


def test_sync_verbose_logs_skips(tmp_path: Path) -> None:
  src = tmp_path / 'src'
  dst = tmp_path / 'dst'
  src.mkdir()
  dst.mkdir()

  create_file(src / 'file.txt', 'same')
  create_file(dst / 'file.txt', 'same')

  logged: list[SyncAction] = []

  def reporter(action: SyncAction) -> None:
    logged.append(action)

  sync(src, dst, reporter=reporter, verbose=True)

  assert any(action.kind == 'skip_file' for action in logged)


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


def test_cli_dry_run_outputs_actions(
  tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
  src = tmp_path / 'src'
  dst = tmp_path / 'dst'
  src.mkdir()
  dst.mkdir()

  create_file(src / 'new.txt', 'new')
  create_file(dst / 'remove.txt', 'old')

  monkeypatch.setattr(sys, 'argv', ['pysync', str(src), str(dst), '--dry-run'])

  exit_code = cli_main()

  assert exit_code == 0
  captured = capsys.readouterr()
  assert 'DRY RUN: copy file' in captured.out
  assert 'DRY RUN: remove file' in captured.out
  assert 'Dry run complete' in captured.out
  assert not (dst / 'new.txt').exists()
  assert (dst / 'remove.txt').exists()


def test_cli_verbose_outputs_actions(
  tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
  src = tmp_path / 'src'
  dst = tmp_path / 'dst'
  src.mkdir()

  create_file(src / 'file.txt', 'hello')

  monkeypatch.setattr(sys, 'argv', ['pysync', str(src), str(dst), '--verbose'])

  exit_code = cli_main()

  assert exit_code == 0
  captured = capsys.readouterr()
  assert 'copy file:' in captured.out
  assert 'Dry run complete' not in captured.out
  assert (dst / 'file.txt').read_text() == 'hello'
