set dotenv-load

export EDITOR := 'nvim'

alias f := fmt
alias t := test
alias tc := typecheck

default:
  just --list

fmt:
   uv run ruff check --select I --fix && uv run ruff format

test:
  uv run pytest

typecheck:
  uv run ty check
