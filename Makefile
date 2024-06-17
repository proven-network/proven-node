.PHONY: all build install enclave

all: build install enclave

build:
  cargo build --release

install:
  @sudo cp target/release/proven-host /usr/sbin/proven
	@echo "Copied target/release/proven-host to /usr/sbin/"

enclave:
	make -C ./build
