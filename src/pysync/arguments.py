from __future__ import annotations

import argparse
import typing as t
from dataclasses import dataclass
from enum import Enum
from pathlib import Path


class HelpFormatter(argparse.HelpFormatter):
  """
  Custom help formatter that aligns option strings and help text with multi-line usage.
  """

  def __init__(
    self,
    prog: str,
    indent_increment: int = 2,
    max_help_position: int = 50,
    width: t.Optional[int] = None,
  ):
    super().__init__(prog, indent_increment, max_help_position, width)

  def _format_usage(
    self,
    usage: t.Optional[str],
    actions: t.Iterable[argparse.Action],
    groups: t.Iterable[argparse._MutuallyExclusiveGroup],
    prefix: t.Optional[str],
  ) -> str:
    """
    Format usage section with one flag per line.
    """
    if usage is not None:
      return usage

    _ = groups

    parts: list[str] = []

    for action in actions:
      if isinstance(action, argparse._HelpAction):
        continue

      display: str

      if action.option_strings:
        option = action.option_strings[0]

        if action.nargs == 0:
          display = option
        else:
          metavar = self._metavar_formatter(action, action.dest)(1)[0]
          display = f'{option} {metavar}'
      else:
        display = self._format_args(action, action.dest)

      if not action.required:
        display = f'[{display}]'

      parts.append(display)

    usage_prefix = prefix or 'usage: '
    usage_line = f'{usage_prefix}{self._prog} {" ".join(parts)}\n\n'

    return usage_line

  def _format_action_invocation(self, action: argparse.Action) -> str:
    """
    Formats the action invocation with simplified display.
    """
    if not action.option_strings:
      return self._format_args(action, action.dest)

    if isinstance(action, argparse._HelpAction):
      return '-h --help'

    if action.nargs == 0:
      return action.option_strings[0]

    if action.required:
      metavar = self._metavar_formatter(action, action.dest)(1)[0]
      return f'{action.option_strings[0]} {metavar}'

    return action.option_strings[0]

  def _format_action(self, action: argparse.Action) -> str:
    """
    Formats each action (argument) with help text.
    """
    help_text = (
      'Show this help message and exit'
      if isinstance(action, argparse._HelpAction)
      else (action.help or '')
    )

    if action.default is not None and action.default != argparse.SUPPRESS:
      if isinstance(action.default, bool):
        help_text = f'{help_text} (default: {str(action.default)})'
      else:
        help_text = f'{help_text} (default: {action.default})'

    return f'  {self._format_action_invocation(action)} {help_text}\n'


class Strategy(str, Enum):
  """Enum for sync strategy options."""

  COPY = 'copy'
  DELTA = 'delta'


@dataclass
class Arguments:
  """
  A wrapper class providing concrete types for parsed command-line arguments.
  """

  source: Path
  destination: Path
  strategy: Strategy
  block_size: t.Optional[int]
  dry_run: bool
  verbose: bool

  @staticmethod
  def from_args() -> Arguments:
    parser = argparse.ArgumentParser(
      description='Synchronise two local directories.',
      formatter_class=HelpFormatter,
    )

    parser.add_argument('source', type=Path, help='Path to the source directory')

    parser.add_argument('destination', type=Path, help='Path to the destination directory')

    parser.add_argument(
      '--strategy',
      type=Strategy,
      choices=list(Strategy),
      default=Strategy.COPY,
      help='Copy files wholesale (default) or send rolling deltas.',
    )

    parser.add_argument(
      '--block-size',
      type=int,
      help='Block size (bytes) for the delta strategy.',
    )

    parser.add_argument(
      '--dry-run',
      action='store_true',
      help='Preview sync actions without modifying the destination.',
    )

    parser.add_argument(
      '-v',
      '--verbose',
      action='store_true',
      help='Log each action as it occurs.',
    )

    return Arguments(**vars(parser.parse_args()))
