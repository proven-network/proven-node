# Makefile to build Linux kernel version 6.9 with custom config

KERNEL_VERSION = 6.10.10
BUILD_DIR = build
OUTPUT_DIR = output
BLOBS_DIR = /usr/share/nitro_enclaves/blobs
KERNEL_SOURCE = ${BUILD_DIR}/linux-$(KERNEL_VERSION)
KERNEL_ARCHIVE = $(KERNEL_SOURCE).tar.xz
KERNEL_URL = https://cdn.kernel.org/pub/linux/kernel/v6.x/linux-$(KERNEL_VERSION).tar.xz

all: setup download extract config build

setup:
	@mkdir -p $(BUILD_DIR)
	@mkdir -p $(OUTPUT_DIR)

download:
	@if [ ! -f "$(KERNEL_ARCHIVE)" ]; then \
		echo "Downloading Linux kernel $(KERNEL_VERSION)"; \
		wget -O $(KERNEL_ARCHIVE) $(KERNEL_URL) || curl -o $(KERNEL_ARCHIVE) $(KERNEL_URL); \
	else \
		echo "$(KERNEL_ARCHIVE) already exists, skipping download."; \
	fi

extract:
	@if [ ! -d "$(KERNEL_SOURCE)" ]; then \
		echo "Extracting Linux kernel source to $(BUILD_DIR)"; \
		mkdir -p $(KERNEL_SOURCE) && tar xf $(KERNEL_ARCHIVE) -C $(KERNEL_SOURCE) --strip-components=1; \
	else \
		echo "$(KERNEL_SOURCE) directory already exists, skipping extraction."; \
	fi

config: extract
	@echo "Applying custom config"
	@if [ `uname -m` = "x86_64" ]; then \
		echo "Detected x86_64 architecture. Copying base config."; \
		cp configs/microvm-kernel-config-x86_64.config $(KERNEL_SOURCE)/.config; \
	elif [ `uname -m` = "aarch64" ]; then \
		echo "Detected aarch64 architecture. Copying base config."; \
		cp configs/microvm-kernel-config-aarch64.config $(KERNEL_SOURCE)/.config; \
	else \
		echo "Unsupported architecture."; \
	fi
	cd $(KERNEL_SOURCE) && \
	scripts/config --enable VIRTIO_MMIO && \
	scripts/config --enable VIRTIO_MENU && \
	scripts/config --enable VIRTIO_MMIO_CMDLINE_DEVICES && \
	scripts/config --enable NET && \
	scripts/config --enable VSOCKETS && \
	scripts/config --enable VIRTIO_VSOCKETS && \
	scripts/config --enable CONFIG_IO_URING && \
	scripts/config --enable CONFIG_FUSE_FS && \
	echo "CONFIG_NSM=m" >> .config && \
	make olddefconfig

build: config
	@echo "Building Linux kernel $(KERNEL_VERSION) with custom config"
	cd $(KERNEL_SOURCE) && make -j`nproc`
	@echo "Copying kernel image and config to output directory"
	@if [ `uname -m` = "x86_64" ]; then \
		echo "Detected x86_64 architecture. Copying bzImage."; \
		cp $(KERNEL_SOURCE)/arch/x86/boot/bzImage $(OUTPUT_DIR)/bzImage; \
		cp $(KERNEL_SOURCE)/.config $(OUTPUT_DIR)/bzImage.config; \
		cp $(KERNEL_SOURCE)/drivers/misc/nsm.ko $(OUTPUT_DIR)/nsm.ko; \
	elif [ `uname -m` = "aarch64" ]; then \
		echo "Detected aarch64 architecture. Copying Image."; \
		cp $(KERNEL_SOURCE)/arch/arm64/boot/Image $(OUTPUT_DIR)/Image; \
		cp $(KERNEL_SOURCE)/.config $(OUTPUT_DIR)/Image.config; \
		cp $(KERNEL_SOURCE)/drivers/misc/nsm.ko $(OUTPUT_DIR)/nsm.ko; \
		echo "Patching Image to support loading by Nitro Enclave."; \
		printf "\x00\x00\x20\x00\x00\x00\x00\x00" | dd of=$(OUTPUT_DIR)/Image bs=1 count=8 seek=8 conv=notrunc; \
	else \
		echo "Unsupported architecture."; \
	fi

install:
	@if [ `uname -m` = "x86_64" ]; then \
		sudo cp $(OUTPUT_DIR)/bzImage ${BLOBS_DIR}/bzImage; \
		sudo cp $(OUTPUT_DIR)/bzImage.config ${BLOBS_DIR}/bzImage.config; \
		sudo cp $(OUTPUT_DIR)/nsm.ko ${BLOBS_DIR}/nsm.ko; \
	elif [ `uname -m` = "aarch64" ]; then \
		sudo cp $(OUTPUT_DIR)/Image ${BLOBS_DIR}/Image; \
		sudo cp $(OUTPUT_DIR)/Image.config ${BLOBS_DIR}/Image.config; \
		sudo cp $(OUTPUT_DIR)/nsm.ko ${BLOBS_DIR}/nsm.ko; \
	else \
		echo "Unsupported architecture."; \
	fi
	@echo "Copied output blobs to ${BLOBS_DIR}"

clean:
	@echo "Cleaning up"
	rm -rf $(BUILD_DIR)

.PHONY: all download extract config build install clean
