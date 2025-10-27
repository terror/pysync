set dotenv-load

export EDITOR := 'nvim'

default:
  just --list

fmt:
   uv run ruff check --select I --fix && uv run ruff format

typecheck:
  uv run ty check
