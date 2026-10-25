MAKEFLAGS += --warn-undefined-variables
SHELL := bash
.DEFAULT_GOAL := build
.DELETE_ON_ERROR:
.SUFFIXES:

# Tests

## Standard tests

.PHONY: check check_default check_libsql test test_default test_libsql

check: check_default check_libsql

check_default:
	cargo check

check_libsql:
	cargo check --no-default-features --features libsql

tests/output:
	mkdir -p $@

test: test_default test_libsql

test_default: | tests/data/table1.csv tests/output
	@echo "Running unit tests using default features."
	cargo test
	@echo "Default unit tests succeeded."

test_libsql: | tests/data/table1.csv tests/output
	@echo "Running unit tests using Libsql."
	cargo test --no-default-features --features libsql
	@echo "Libsql unit tests succeeded."

tests/data/table1.csv: tests/data/table1.tsv
	csvtool -t TAB -u COMMA cat $< > $@

## Performance tests

.PHONY: test_driver_perf

rltbl_db_benchmarks:
	@echo -n "Please clone or copy the https://github.com/rltbl/rltbl_db_benchmarks "
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

# Clean

.PHONY: clean

clean:
	rm -Rf tests/output
