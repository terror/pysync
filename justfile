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

dev-deps:
  cargo install present

fmt:
   uv run ruff check --select I --fix && uv run ruff format

readme:
  present --in-place README.md

test:
  uv run pytest

typecheck:
  uv run ty check
