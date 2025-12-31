MAKEFLAGS += --warn-undefined-variables
SHELL := bash
.DEFAULT_GOAL := postgres
.DELETE_ON_ERROR:
.SUFFIXES:

.PHONY: test test_sqlite test_postgres crate_docs postgres

test: test_sqlite test_postgres

test_all: test_sqlite_all test_postgres_all

test_sqlite:
	cargo test --features rusqlite -- --no-capture

test_sqlite_all:
	cargo test --features rusqlite -- --no-capture --include-ignored

test_postgres:
	cargo test --features tokio-postgres -- --no-capture

test_postgres_all:
	cargo test --features tokio-postgres -- --no-capture --include-ignored

crate_docs:
	RUSTDOCFLAGS="-D warnings" cargo doc

crate_docs_postgres:
	RUSTDOCFLAGS="-D warnings" cargo doc --features tokio-postgres

postgres:
	cargo build --features tokio-postgres
