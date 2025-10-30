from __future__ import annotations

import os
from pathlib import Path

import pytest

from pysync.error import SyncError
from pysync.strategy import FileCopierStrategy


def test_file_copier_raises_for_symlink_destination(tmp_path: Path) -> None:
  source = tmp_path / 'src.txt'
  destination = tmp_path / 'dst.txt'

  source.write_text('content')
  os.symlink(source, destination)

  copier = FileCopierStrategy()

  with pytest.raises(SyncError):
    copier.sync_file(source, destination)


def test_file_copier_copies_without_filecmp(
  tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
  source = tmp_path / 'src.txt'
  destination = tmp_path / 'dst.txt'

  source.write_text('new')
  destination.write_text('old')

  def explode(*_: object, **__: object) -> bool:
    raise AssertionError('filecmp.cmp should not be called')

  monkeypatch.setattr('pysync.strategy.filecmp.cmp', explode)

  copier = FileCopierStrategy()
  copier.sync_file(source, destination)

  assert destination.read_text() == 'new'
