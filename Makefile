.PHONY: all build install install-service

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
