.PHONY: all build install

all: build install

build:
	cargo build --release --package proven-host

install:
	@sudo cp target/release/proven-host /usr/sbin/proven
	@echo "Copied target/release/proven-host to /usr/sbin/"
