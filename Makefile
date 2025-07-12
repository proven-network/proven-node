.PHONY: all build install install-service cloc

all: build install

build:
	cargo build --release --package proven-host

install:
	@sudo cp target/release/proven-host /usr/sbin/proven
	@echo "Copied target/release/proven-host to /usr/sbin/"

install-service:
	@sudo cp proven.service /etc/systemd/system/proven.service
	@echo "Copied proven.service to /etc/systemd/system/"
	@sudo systemctl daemon-reload
	@sudo systemctl enable proven.service
	@echo "Enabled proven.service"

cloc:
	cloc \
		--exclude-ext=js,mjs,json,yaml,md,toml \
		--not-match-d='node_modules' \
		--not-match-d='dist' \
		--not-match-d='target' \
		--not-match-d='target-devcontainer' \
		--not-match-d='vendor' \
		--not-match-f='Cargo.lock' \
		--not-match-f='codegen.rs' \
		.
