from pathlib import Path

import pytest

from pysync.sync import SyncError, sync


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
