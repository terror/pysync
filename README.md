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

## Prior Art

This project is heavily inspired by [rsync(1)](https://linux.die.net/man/1/rsync), 
a fast, versatile, remote (and local) file-copying tool. I wanted to demystify 
some of the concepts behind file synchronization, so I decided to write my own tool.
