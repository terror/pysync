from __future__ import annotations

import io

from rich.console import Console

from pysync.__main__ import _make_console_reporter
from pysync.sync import SyncAction


def test_console_reporter_handles_symlink_actions(tmp_path) -> None:
  src = tmp_path / 'src'
  dst = tmp_path / 'dst'

  src.mkdir()
  dst.mkdir()

  stream = io.StringIO()
  console = Console(file=stream, force_terminal=False, color_system=None, highlight=False)
  reporter = _make_console_reporter(console, src, dst, dry_run=True)

  actions = [
    SyncAction('create_symlink', dst / 'link', source=src / 'link-target'),
    SyncAction('update_symlink', dst / 'other-link', source=src / 'other-target'),
    SyncAction('skip_symlink', dst / 'skipped-link', source=src / 'skipped-target'),
  ]

  for action in actions:
    reporter(action)

  output = stream.getvalue().strip().splitlines()

  assert output == [
    'DRY RUN: create symlink: link (from link-target)',
    'DRY RUN: update symlink: other-link (from other-target)',
    'DRY RUN: skip symlink: skipped-link (from skipped-target)',
  ]
