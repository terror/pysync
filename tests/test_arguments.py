from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pytest

from pysync.arguments import Arguments, HelpFormatter, Strategy


def test_help_formatter_usage_lists_flags_separately() -> None:
  parser = argparse.ArgumentParser(prog='pysync', formatter_class=HelpFormatter)
  parser.add_argument('source')
  parser.add_argument('destination')
  parser.add_argument('--flag', action='store_true')
  parser.add_argument('--opt', type=int)

  usage = parser.format_usage()

  assert usage.startswith('usage: pysync source destination')
  assert usage.endswith('\n')
  assert '[--flag]' in usage
  assert '[--opt opt]' in usage


def test_help_formatter_invocation_variants() -> None:
  parser = argparse.ArgumentParser(prog='pysync', formatter_class=HelpFormatter)
  parser.add_argument('source')
  parser.add_argument('--required-opt', required=True, metavar='VALUE')
  parser.add_argument('--flag', action='store_true')

  formatter = parser._get_formatter()
  actions = {action.dest: action for action in parser._actions}

  assert formatter._format_action_invocation(actions['source']) == 'source'
  assert formatter._format_action_invocation(actions['help']) == '-h --help'
  assert formatter._format_action_invocation(actions['required_opt']) == '--required-opt VALUE'
  assert formatter._format_action_invocation(actions['flag']) == '--flag'


def test_help_formatter_formats_action_help_with_defaults() -> None:
  parser = argparse.ArgumentParser(prog='pysync', formatter_class=HelpFormatter)
  parser.add_argument('-v', '--verbose', action='store_true', help='Verbose output.')
  parser.add_argument('--count', default=3, help='Number of runs.')

  formatter = parser._get_formatter()
  actions = {action.dest: action for action in parser._actions}

  verbose_line = formatter._format_action(actions['verbose'])
  count_line = formatter._format_action(actions['count'])
  help_line = formatter._format_action(actions['help'])

  assert verbose_line == '  -v Verbose output. (default: False)\n'
  assert count_line == '  --count Number of runs. (default: 3)\n'
  assert help_line == '  -h --help Show this help message and exit\n'


def test_arguments_from_args_parses_all_fields(
  tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
  source = tmp_path / 'src'
  destination = tmp_path / 'dst'
  source.mkdir()
  destination.mkdir()

  monkeypatch.setattr(
    sys,
    'argv',
    [
      'pysync',
      str(source),
      str(destination),
      '--strategy',
      'delta',
      '--block-size',
      '4096',
      '--dry-run',
      '--verbose',
    ],
  )

  args = Arguments.from_args()

  assert args.source == source
  assert args.destination == destination
  assert args.strategy is Strategy.DELTA
  assert args.block_size == 4096
  assert args.dry_run is True
  assert args.verbose is True


def test_arguments_from_args_uses_defaults(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
  source = tmp_path / 'src'
  destination = tmp_path / 'dst'
  source.mkdir()
  destination.mkdir()

  monkeypatch.setattr(sys, 'argv', ['pysync', str(source), str(destination)])

  args = Arguments.from_args()

  assert args.strategy is Strategy.COPY
  assert args.block_size is None
  assert args.dry_run is False
  assert args.verbose is False
