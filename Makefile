MAKEFLAGS += --warn-undefined-variables
SHELL := bash
.DEFAULT_GOAL := postgres
.DELETE_ON_ERROR:
.SUFFIXES:

.PHONY: test test_sqlite test_postgres crate_docs postgres

test: test_sqlite test_postgres

test_sqlite:
	cargo test

test_postgres:
	cargo test --features postgres

crate_docs:
	RUSTDOCFLAGS="-D warnings" cargo doc

crate_docs_postgres:
	RUSTDOCFLAGS="-D warnings" cargo doc --features postgres

postgres:
	cargo build --features postgres
