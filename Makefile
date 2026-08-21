MAKEFLAGS += --warn-undefined-variables
SHELL := bash
.DEFAULT_GOAL := build
.DELETE_ON_ERROR:
.SUFFIXES:

.PHONY: check crate_docs build build_libsql
.PHONY: test test_default test_libsql
.PHONY: test_caching_perf_and_max_params

# Main test

tests/input/table1.csv: tests/input/table1.tsv
	csvtool -t TAB -u COMMA cat $< > $@

test: test_default # test_libsql

test_default: | tests/input/table1.csv
	@echo "Running unit tests using default features."
	cargo test
	@echo "Default unit tests succeeded."

test_libsql: | tests/input/table1.csv
	@echo "Running unit tests using Libsql."
	cargo test --no-default-features --features libsql
	@echo "Libsql unit tests succeeded."

# Caching performance test

test_caching_perf_and_max_params:
	@echo "Running caching_performance and max_params tests using default features."
	cargo test -- --no-capture --ignored test_max_params test_caching_performance
	cargo test --no-default-features --features libsql \
		-- --no-capture --ignored test_max_params test_caching_performance
	@echo "Tests succeeded."

# Documentation

crate_docs:
	@echo "Testing documentation comments."
	RUSTDOCFLAGS="-D warnings" cargo doc --features libsql
	@echo "Documentation comments are ok."

# Build

build:
	cargo build

build_libsql:
	cargo build --no-default-features --features libsql
