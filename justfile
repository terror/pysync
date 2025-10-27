set dotenv-load

export EDITOR := 'nvim'

alias f := fmt
alias t := test
alias tc := typecheck
alias cov := coverage

default:
  just --list

ci: fmt test typecheck

coverage:
  ./bin/coverage --verbose

fmt:
   uv run ruff check --select I --fix && uv run ruff format

test:
  uv run pytest

typecheck:
  uv run ty check
