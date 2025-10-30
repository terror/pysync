## pysync

[![CI](https://github.com/terror/pysync/actions/workflows/ci.yaml/badge.svg)](https://github.com/terror/pysync/actions/workflows/ci.yaml)
[![codecov](https://codecov.io/gh/terror/pysync/graph/badge.svg?token=7CH4XDXO7Z)](https://codecov.io/gh/terror/pysync)

**pysync** is a modern command-line tool for synchronizing files and directories 
locally or over a network.

## Installation

You can install **pysync** using [pip](https://pip.pypa.io/en/stable/installation/), the Python package manager:

```bash
pip install pysync
```

## Usage

**pysync** exposes both a command-line tool and library interface.

### CLI

Below is the standard output of `pysync --help`, which elaborates on the various
command-line arguments the tool accepts:

```present uv run src/pysync --help
usage: pysync source destination [--strategy {Strategy.COPY,Strategy.DELTA}] [--block-size block_size] [--dry-run] [-v]

Synchronise two local directories.

positional arguments:
  source Path to the source directory
  destination Path to the destination directory

options:
  -h --help Show this help message and exit
  --strategy Copy files wholesale (default) or send rolling deltas. (default: Strategy.COPY)
  --block-size Block size (bytes) for the delta strategy.
  --dry-run Preview sync actions without modifying the destination. (default: False)
  -v Log each action as it occurs. (default: False)
```

### Library

We expose various structures that make it easy to synchronize two directories
fast.

```python
from pathlib import Path

from pysync import DeltaStrategy, SyncAction, sync

def log_action(action: SyncAction) -> None:
  print(f"{action.kind}: {action.path}")

strategy = DeltaStrategy(block_size=64 * 1024)

sync(
  Path("./source"),
  Path("./destination"),
  strategy=strategy,
  dry_run=False,
  reporter=log_action,
  verbose=True,
)
```

The function signature is `sync(source, destination, strategy=None, *, dry_run=False, reporter=None, verbose=False)`.

- `source` and `destination` accept `pathlib.Path` instances or strings.
- `strategy` accepts any object that implements `sync_file(source, destination)`. It defaults to `FileCopierStrategy`, which mirrors files using standard copy semantics. Use `DeltaStrategy` to transfer only changed blocks while tracking transfer statistics.
- `dry_run=True` reports the planned actions without modifying the destination tree.
- `reporter` receives `SyncAction` objects describing each operation. When `verbose=True`, the reporter also observes skipped files and directories.
- Errors raise `SyncError`, allowing callers to retry or surface a user-friendly message.

When using `DeltaStrategy` you can inspect per-file transfer stats:

```python
from pysync import DeltaStrategy, SyncStats, sync

strategy = DeltaStrategy()

sync("assets", "build/assets", strategy=strategy)

stats: SyncStats = strategy.get_stats_for(Path("build/assets/logo.png"))

if stats:
  print(f"Transferred {stats.bytes_transferred} of {stats.total_bytes} bytes;")
  print(f"saved {stats.bytes_saved} bytes by reusing existing data.")
```

`stats()` returns a mapping of every touched destination path to its `SyncStats` record should you need an aggregated view.

## Prior Art

This project is heavily inspired by [rsync(1)](https://linux.die.net/man/1/rsync), 
a fast, versatile, remote (and local) file-copying tool. I wanted to demystify 
some of the concepts behind file synchronization, so I decided to write my own tool.
