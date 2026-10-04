MAKEFLAGS += --warn-undefined-variables
SHELL := bash
.DEFAULT_GOAL := build
.DELETE_ON_ERROR:
.SUFFIXES:

# Tests

## Standard tests

.PHONY: test test_default test_libsql

check:
	cargo check
	cargo check --no-default-features --features libsql

test: test_default test_libsql

test_default: | tests/data/table1.csv
	@echo "Running unit tests using default features."
	cargo test
	@echo "Default unit tests succeeded."

test_libsql: | tests/data/table1.csv
	@echo "Running unit tests using Libsql."
	cargo test --no-default-features --features libsql
	@echo "Libsql unit tests succeeded."

tests/data/table1.csv: tests/data/table1.tsv
	csvtool -t TAB -u COMMA cat $< > $@

## Tests that are normally ignored

.PHONY: test_ignored test_default_ignored test_libsql_ignored

test_ignored: test_default_ignored test_libsql_ignored

test_default_ignored:
	@echo "Running all (including normally) ignored unit tests using default features."
	cargo test -- --no-capture --include-ignored

test_libsql_ignored:
	@echo "Running all (including normally) ignored unit tests using default features."
	cargo test --no-default-features --features libsql \
		-- --no-capture --include-ignored

## Performance tests

###################################################
# TODO: Replace these with as many of the benchmark tests as you can stabilize.
.PHONY: old_test_caching_perf old_test_import_perf

old_test_caching_perf:
	@echo "Running caching performance test using default features."
	cargo test -- --no-capture --ignored test_caching_perf
	cargo test --no-default-features --features libsql \
		-- --no-capture --ignored test_caching_perf
	@echo "Tests succeeded."

old_test_import_perf:
	@echo "Running import performance test using default features."
	cargo test -- --no-capture --ignored test_import_perf
	cargo test --no-default-features --features libsql \
		-- --no-capture --ignored test_import_perf
	@echo "Tests succeeded."
###################################################

rltbl_db_benchmarks:
	@echo -n "Please clone or copy the https://github.com/lmcmicu/rltbl_db_benchmarks "
	@echo "repository into the current directory."
	@echo "Press enter when this has been done. "
	@read enter
	@test -d $@ && test -f $@/Cargo.toml || \
		(echo "Failed to detect $@ repository. Did you clone/copy it?" && false)

test_driver_perf: | rltbl_db_benchmarks
	cd $| && make rltbl_tokio
	cd $| && make tokio_raw
	cd $| && make rltbl_rusqlite
	cd $| && make rusqlite_raw
	cd $| && make libsql_raw


# Documentation

.PHONY: crate_docs

crate_docs:
	@echo "Testing documentation comments."
	RUSTDOCFLAGS="-D warnings" cargo doc
	RUSTDOCFLAGS="-D warnings" cargo doc --no-default-features --features libsql
	@echo "Documentation comments are ok."

# Build

.PHONY: build build_libsql

build:
	cargo build

build_libsql:
	cargo build --no-default-features --features libsql
