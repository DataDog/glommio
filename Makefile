SHELL := /bin/bash
PROJECT_DIR:=$(shell dirname $(realpath $(lastword $(MAKEFILE_LIST))))

BIN_PATH = $(CURDIR)/bin
CARGO_TARGET_DIR ?= $(CURDIR)/target

# Build-time environment, captured for reporting by the application binary
BUILD_INFO_GIT_FALLBACK := "Unknown (no git or not git repo)"
BUILD_INFO_RUSTC_FALLBACK := "Unknown"
export SCIPIO_ENABLE_FEATURES := ${ENABLE_FEATURES}
export SCIPIO_BUILD_TIME := $(shell date -u '+%Y-%m-%d %I:%M:%S')
export SCIPIO_BUILD_RUSTC_VERSION := $(shell rustc --version 2> /dev/null || echo ${BUILD_INFO_RUSTC_FALLBACK})
export SCIPIO_BUILD_GIT_HASH ?= $(shell git rev-parse HEAD 2> /dev/null || echo ${BUILD_INFO_GIT_FALLBACK})
export SCIPIO_BUILD_GIT_TAG ?= $(shell git describe --tag || echo ${BUILD_INFO_GIT_FALLBACK})
export SCIPIO_BUILD_GIT_BRANCH ?= $(shell git rev-parse --abbrev-ref HEAD 2> /dev/null || echo ${BUILD_INFO_GIT_FALLBACK})

# https://internals.rust-lang.org/t/evaluating-pipelined-rustc-compilation/10199/68
export CARGO_BUILD_PIPELINING=true

default: release

.PHONY: all

clean:
	cargo clean
	rm -rf bin dist


## Development builds
## ------------------

all: format build test

dev: format clippy
	@env FAIL_POINT=1 make test

build:
	cargo build

## Static analysis
## ---------------

unset-override:
	@# unset first in case of any previous overrides
	@if rustup override list | grep `pwd` > /dev/null; then rustup override unset; fi

pre-format: unset-override
	@rustup component add rustfmt

format: pre-format
	@cargo fmt --all -- --check >/dev/null || \
	cargo fmt --all

doc:
	@cargo doc --workspace --document-private-items

pre-clippy: unset-override
	@rustup component add clippy

clippy: pre-clippy
	@cargo clippy --workspace --all-targets

pre-audit:
	$(eval LATEST_AUDIT_VERSION := $(strip $(shell cargo search cargo-audit | head -n 1 | awk '{ gsub(/"/, "", $$3); print $$3 }')))
	$(eval CURRENT_AUDIT_VERSION = $(strip $(shell (cargo audit --version 2> /dev/null || echo "noop 0") | awk '{ print $$2 }')))
	@if [ "$(LATEST_AUDIT_VERSION)" != "$(CURRENT_AUDIT_VERSION)" ]; then \
		cargo install cargo-audit --force; \
	fi

# Check for security vulnerabilities
audit: pre-audit
	cargo audit
