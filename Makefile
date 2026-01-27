MAKEFLAGS += --warn-undefined-variables
SHELL := bash
.DEFAULT_GOAL := build
.DELETE_ON_ERROR:
.SUFFIXES:

# TODO Add .PHONY

test: test_default test_libsql test_sqlx

test_ignored: test_default_ignored test_libsql_ignored test_sqlx_ignored

test_default:
	@echo "Running unit tests using default features."
	cargo test -- --no-capture
	@echo "Default unit tests succeeded."

test_default_ignored:
	@echo "Running ignored unit tests using default features."
	cargo test -- --no-capture --ignored
	@echo "Ignored default unit tests succeeded."

test_libsql:
	@echo "Running unit tests using Libsql."
	cargo test --no-default-features --features libsql -- --no-capture
	@echo "Libsql unit tests succeeded."

test_libsql_ignored:
	@echo "Running ignored unit tests using Libsql."
	cargo test --no-default-features --features libsql -- --no-capture --ignored
	@echo "Ignored Libsql unit tests succeeded."

test_sqlx:
	@echo "Running unit tests using Sqlx."
	cargo test --no-default-features --features sqlx -- --no-capture
	@echo "Sqlx unit tests succeeded."

test_sqlx_ignored:
	@echo "Running ignored unit tests using Sqlx."
	cargo test --no-default-features --features sqlx -- --no-capture --ignored
	@echo "Ignored Sqlx unit tests succeeded."

crate_docs:
	@echo "Testing documentation comments."
	RUSTDOCFLAGS="-D warnings" cargo doc
	@echo "Documentation comments are ok."

build:
	cargo build

build_libsql:
	cargo build --no-default-features --features libsql

build_sqlx:
	cargo build --no-default-features --features sqlx
