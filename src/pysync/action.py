from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Literal

SyncActionKind = Literal[
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


@dataclass(frozen=True)
class SyncAction:
  kind: SyncActionKind
  path: Path
  source: Path | None = None


ActionReporter = Callable[[SyncAction], None]


def report_action(reporter: ActionReporter | None, action: SyncAction) -> None:
  if reporter is not None:
    reporter(action)
