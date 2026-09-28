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

##### TODO: Use this section to replace the older tests below it.
## Performance tests

.PHONY: rltbl_tokio tokio_raw caching_perf_new

baselines:
	mkdir -p $@

output:
	mkdir -p $@

rltbl_tokio: | baselines output
	cargo run --bin rltbl_db_perf -- \
		--seed 0 --collector silent --warmup 10 \
		--duration 1m \
		--output json --output-file output/driver-rltbl-tokio-postgres-v0.1.0.json \
		--baseline-file baselines/driver-rltbl-tokio-postgres-v0.1.0.json \
		rltbl-driver tokio-postgres

tokio_raw: | baselines output
	cargo run --bin rltbl_db_perf -- \
		--seed 0 --collector silent --warmup 10 \
		--duration 1m \
		--output json --output-file output/driver-tokio-postgres-raw-v0.1.0.json \
		--baseline-file baselines/driver-tokio-postgres-raw-v0.1.0.json \
		tokio-postgres-driver

caching_perf_new: | baselines output
	cargo run --no-default-features --features tokio-postgres -- \
		--seed 0 --collector silent --warmup 10 \
		--noise-threshold 5 \
		--baseline-file baselines/caching-postgresql-none-v0.1.0.json \
		--output json --output-file output/caching-postgresql-none-v0.1.0.json \
		caching --totals-file baselines/caching-totals-v0.1.0.json postgres truncate

##### TODO: Replace these with the benchmark tests:
.PHONY: test_caching_perf test_import_perf

test_caching_perf:
	@echo "Running caching performance test using default features."
	cargo test -- --no-capture --ignored test_caching_perf
	cargo test --no-default-features --features libsql \
		-- --no-capture --ignored test_caching_perf
	@echo "Tests succeeded."

test_import_perf:
	@echo "Running import performance test using default features."
	cargo test -- --no-capture --ignored test_import_perf
	cargo test --no-default-features --features libsql \
		-- --no-capture --ignored test_import_perf
	@echo "Tests succeeded."

## All ignored tests:

.PHONY: test_ignored test_default_ignored test_libsql_ignored

test_ignored: test_default_ignored test_libsql_ignored

test_default_ignored:
	@echo "Running all (including normally) ignored unit tests using default features."
	cargo test -- --no-capture --include-ignored

test_libsql_ignored:
	@echo "Running all (including normally) ignored unit tests using default features."
	cargo test --no-default-features --features libsql \
		-- --no-capture --include-ignored

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
