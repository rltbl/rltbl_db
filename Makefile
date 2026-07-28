MAKEFLAGS += --warn-undefined-variables
SHELL := bash
.DEFAULT_GOAL := build
.DELETE_ON_ERROR:
.SUFFIXES:

.PHONY: check crate_docs build build_libsql
.PHONY: test test_default test_libsql
.PHONY: test_ignored test_default_ignored test_libsql_ignored
.PHONY: test_import_perf

tests/penguins/src/data/penguin.tsv:
	cd tests/penguins && ./generate.py 100000

test_import_perf: tests/penguins/src/data/penguin.tsv
	cargo build
	echo "Loading data using rusqlite ..."
	time -p cargo test --no-default-features --features rusqlite test_import_perf
	# echo "Loading data using libsql ..."
	# time -p cargo test --no-default-features --features libsql test_import_perf
	echo "Loading data using tokio-postgres ..."
	time -p cargo test --no-default-features --features tokio-postgres test_import_perf

tests/input/table1.csv: tests/input/table1.tsv
	csvtool -t TAB -u COMMA cat $< > $@


test: test_default test_libsql

test_default: | tests/input/table1.csv
	@echo "Running unit tests using default features."
	cargo test
	@echo "Default unit tests succeeded."

test_libsql: | tests/input/table1.csv
	@echo "Running unit tests using Libsql."
	cargo test --no-default-features --features libsql
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
