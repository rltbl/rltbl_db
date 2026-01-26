MAKEFLAGS += --warn-undefined-variables
SHELL := bash
.DEFAULT_GOAL := build
.DELETE_ON_ERROR:
.SUFFIXES:

# TODO Add .PHONY

test: test_default test_libsql test_sqlx

test_ignored: test_default_ignored test_libsql_ignored test_sqlx_ignored

test_default:
	cargo test -- --no-capture

test_default_ignored:
	cargo test -- --no-capture --ignored

test_libsql:
	cargo test --no-default-features --features libsql -- --no-capture

test_libsql_ignored:
	cargo test --no-default-features --features libsql -- --no-capture --ignored

test_sqlx:
	cargo test --no-default-features --features sqlx -- --no-capture

test_sqlx_ignored:
	cargo test --no-default-features --features sqlx -- --no-capture --ignored

crate_docs:
	RUSTDOCFLAGS="-D warnings" cargo doc

build:
	cargo build

build_libsql:
	cargo build --no-default-features --features libsql

build_sqlx:
	cargo build --no-default-features --features sqlx
