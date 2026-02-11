MAKEFLAGS += --warn-undefined-variables
SHELL := bash
.DEFAULT_GOAL := postgres
.DELETE_ON_ERROR:
.SUFFIXES:

.PHONY: test test_ignored test_all crate_docs crate_docs_postgres sqlite_only postgres

test:
	cargo test -- --no-capture

test_ignored:
	cargo test -- --no-capture --ignored

test_all:
	cargo test -- --no-capture --include-ignored

crate_docs:
	RUSTDOCFLAGS="-D warnings" cargo doc

crate_docs_postgres:
	RUSTDOCFLAGS="-D warnings" cargo doc --features tokio-postgres

sqlite_only:
	cargo build

postgres:
	cargo build --features tokio-postgres
