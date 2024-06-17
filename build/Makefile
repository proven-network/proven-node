prog := proven-node
version := $(shell git describe --tag --dirty --always)
image_tag := $(prog):$(version)
image_tar := $(prog)-$(version)-kaniko.tar
image_eif := $(image_tar:%.tar=%.eif)

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
all: run

.PHONY: image
image: $(image_tar)

$(image_tar): Dockerfile
	docker run \
		-v $(PWD):/workspace \
		gcr.io/kaniko-project/executor:v1.23.1 \
		--reproducible \
		--no-push \
		--tarPath $(image_tar) \
		--destination $(image_tag) \
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

.PHONY: run
run: $(image_eif)
	nitro-cli terminate-enclave --all
	./run-enclave.sh $(image_eif)

.PHONY: clean
clean:
	rm -f $(image_tar) $(image_eif)
