prog := proven-node
version := $(shell git describe --always)
image_tag := $(prog):$(version)
image_tar := $(prog)-$(version)-kaniko.tar
image_eif := $(image_tar:%.tar=%.eif)

dest_dir := /var/lib/proven
dest_file := $(dest_dir)/enclave.eif

RUSTARCH ?= $(shell uname -m)
RUSTTARGET ?= $(RUSTARCH)-unknown-linux-musl
ARCH ?= $(shell uname -m)
ifeq ($(ARCH),aarch64)
	override ARCH=arm64
endif
ifeq ($(ARCH),x86_64)
	override ARCH=x86_64
endif

.PHONY: all
all: install

.PHONY: image
image: $(image_tar)

$(image_tar): Dockerfile
	docker run \
		-v $(PWD):/workspace \
		gcr.io/kaniko-project/executor:v1.23.1 \
		--reproducible \
		--single-snapshot \
		--no-push \
		--tarPath $(image_tar) \
		--destination $(image_tag) \
		--build-arg COMMIT=$(version) \
		--build-arg RUSTARCH=$(RUSTARCH) \
		--build-arg RUSTTARGET=$(RUSTTARGET) \
		--build-arg TARGETPLATFORM=linux/$(ARCH) \
		--build-arg TARGETOS=linux \
		--build-arg TARGET=$(ARCH) \
		--custom-platform linux/$(ARCH)

$(image_eif): $(image_tar)
	docker load -i $<
	nitro-cli build-enclave \
		--docker-uri $(image_tag) \
		--output-file $(image_eif)

.PHONY: install
install: $(image_eif)
	@sudo mkdir -p $(dest_dir)
	@sudo cp $(image_eif) $(dest_file)
	@echo "Copied $(image_eif) to $(dest_file)"

.PHONY: clean
clean:
	rm -f $(image_tar) $(image_eif)
