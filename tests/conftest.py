from __future__ import annotations

import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

import pytest

from pysync.__main__ import main as cli_main


@dataclass(slots=True)
class CompletedRun:
  exit_code: int
  stdout: str
  stderr: str


@pytest.fixture
def run_cli(
  monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> Callable[..., CompletedRun]:
  """
  Execute the CLI with arguments while capturing output.

  The first argument should be a working directory (typically ``tmp_path``) so tests may control
  the execution environment. Additional positional arguments are passed to the CLI after being
  converted to strings, allowing ``Path`` instances to be supplied directly.
  """

  def _run_cli(working_dir: Path, *args: object) -> CompletedRun:
    argv = ['pysync', *(str(arg) for arg in args)]
    monkeypatch.setattr(sys, 'argv', argv)
    monkeypatch.chdir(working_dir)

    exit_code = cli_main()
    captured = capsys.readouterr()

    return CompletedRun(exit_code=exit_code, stdout=captured.out, stderr=captured.err)

  return _run_cli
