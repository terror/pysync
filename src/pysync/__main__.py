from __future__ import annotations

import argparse
import sys
from pathlib import Path

from pysync import sync


def main() -> int:
  parser = argparse.ArgumentParser(description='Synchronise two local directories.')
  parser.add_argument('source', type=Path, help='Path to the source directory')
  parser.add_argument('destination', type=Path, help='Path to the destination directory')

  args = parser.parse_args()

  try:
    sync(args.source, args.destination)
  except Exception as exc:  # pragma: no cover - CLI guardrail
    print(f'pysync: {exc}', file=sys.stderr)
    return 1

  return 0


if __name__ == '__main__':
  raise SystemExit(main())
