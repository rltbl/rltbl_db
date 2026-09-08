MAKEFLAGS += --warn-undefined-variables
SHELL := bash
.DEFAULT_GOAL := build
.DELETE_ON_ERROR:
.SUFFIXES:

# Tests

## Standard tests

.PHONY: test test_default test_libsql

test: test_default # test_libsql

test_default: | tests/input/table1.csv
	@echo "Running unit tests using default features."
	cargo test
	@echo "Default unit tests succeeded."

test_libsql: | tests/input/table1.csv
	@echo "Running unit tests using Libsql."
	cargo test --no-default-features --features libsql
	@echo "Libsql unit tests succeeded."

tests/input/table1.csv: tests/input/table1.tsv
	csvtool -t TAB -u COMMA cat $< > $@

## Performance tests

.PHONY: test_caching_perf test_import_perf

test_caching_perf:
	@echo "Running caching_performance test using default features."
	cargo test -- --no-capture --ignored test_caching_performance
	cargo test --no-default-features --features libsql \
		-- --no-capture --ignored test_caching_performance
	@echo "Tests succeeded."

test_import_perf:
	@echo "Running import_performance test using default features."
	cargo test -- --no-capture --ignored test_import_performance
	cargo test --no-default-features --features libsql \
		-- --no-capture --ignored test_import_performance
	@echo "Tests succeeded."

## All ignored tests:

.PHONY: test_ignored test_default_ignored test_libsql_ignored

test_ignored: test_default_ignored # test_libsql_ignored

test_default_ignored:
	@echo "Running all (including normally) ignored unit tests using default features."
	cargo test -- --include-ignored

test_libsql_ignored:
	@echo "Running all (including normally) ignored unit tests using default features."
	cargo test -- --include-ignored

# Documentation

.PHONY: crate_docs

crate_docs:
	@echo "Testing documentation comments."
	RUSTDOCFLAGS="-D warnings" cargo doc --features libsql
	@echo "Documentation comments are ok."

# Build

.PHONY: build build_libsql

build:
	cargo build

build_libsql:
	cargo build --no-default-features --features libsql
