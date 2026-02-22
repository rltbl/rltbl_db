MAKEFLAGS += --warn-undefined-variables
SHELL := bash
.DEFAULT_GOAL := build
.DELETE_ON_ERROR:
.SUFFIXES:

.PHONY: check crate_docs build build_libsql
.PHONY: test test_default test_libsql
.PHONY: test_ignored test_default_ignored test_libsql_ignored

test: test_default test_libsql

ad_hoc:
	rm -f mike_temp_test.db
	cargo test test_view_caching -- --no-capture
	sqlite3 -header mike_temp_test.db "select * from rltbl_db_cache"

test_default:
	@echo "Running unit tests using default features."
	cargo test -- --no-capture
	@echo "Default unit tests succeeded."

test_libsql:
	@echo "Running unit tests using Libsql."
	cargo test --no-default-features --features libsql -- --no-capture
	@echo "Libsql unit tests succeeded."

test_ignored: test_default_ignored test_libsql_ignored

test_default_ignored:
	@echo "Running ignored unit tests using default features."
	cargo test -- --no-capture --ignored
	@echo "Ignored default unit tests succeeded."

test_libsql_ignored:
	@echo "Running ignored unit tests using Libsql."
	cargo test --no-default-features --features libsql -- --no-capture --ignored
	@echo "Ignored Libsql unit tests succeeded."

crate_docs:
	@echo "Testing documentation comments."
	RUSTDOCFLAGS="-D warnings" cargo doc --features libsql
	@echo "Documentation comments are ok."

build:
	cargo build

build_libsql:
	cargo build --no-default-features --features libsql
