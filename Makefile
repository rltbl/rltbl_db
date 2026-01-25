MAKEFLAGS += --warn-undefined-variables
SHELL := bash
.DEFAULT_GOAL := build
.DELETE_ON_ERROR:
.SUFFIXES:

.PHONY: test test_ignored test_all crate_docs crate_docs_postgres sqlite_only postgres

test:
	cargo test -- --no-capture
	cargo test --no-default-features --features libsql -- --no-capture

test_ignored:
	cargo test -- --no-capture --ignored
	cargo test --no-default-features --features libsql -- --no-capture --ignored

test_all:
	cargo test -- --no-capture --include-ignored
	cargo test --no-default-features --features libsql -- --no-capture --include-ignored

crate_docs:
	RUSTDOCFLAGS="-D warnings" cargo doc

build:
	cargo build

build_libsql:
	cargo build --no-default-features --features libsql
