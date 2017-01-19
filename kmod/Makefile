M := $(shell pwd)
NVME_STROM_PKGNAME := $(shell grep ^PACKAGE_NAME= $M/dkms.conf |	\
                        sed -e 's/PACKAGE_NAME=//g' -e 's/"//g' | head -1)
NVME_STROM_VERSION := $(shell grep ^PACKAGE_VERSION= $M/dkms.conf |	\
                        sed -e 's/PACKAGE_VERSION=//g' -e 's/"//g' | head -1)
NVME_STROM_BUILD_TIMESTAMP = $(shell date --utc)
DKMS_DEST=/usr/src/$(NVME_STROM_PKGNAME)-$(NVME_STROM_VERSION)

KERNEL_VER := $(shell uname -r)
KERNEL_DIR := /lib/modules/$(KERNEL_VER)
KERNEL_SOURCE := $(KERNEL_DIR)/build
# MEMO: Older version of CUDA driver provided the kernel header files
# at '/usr/src/nvidia-*/nv-p2p.h'.
NVIDIA_SOURCE := $(shell ls -St /usr/src/nvidia-*/nv-p2p.h \
                                /usr/src/nvidia-*/nvidia/nv-p2p.h 2>/dev/null \
                       | sed 's/\/nv-p2p\.h$$//g' | head -1)

CUDA_PATH_LIST := /usr/local/cuda /usr/local/cuda-*
CUDA_PATH := $(shell for x in $(CUDA_PATH_LIST);    \
	do test -e "$$x/include/cuda.h" && echo $$x; done | head -1)
USERSPACE_FLAGS := -g -I $(CUDA_PATH)/include -L $(CUDA_PATH)/lib64

EXTRA_CLEAN := nvme_test

KMOD_SOURCE :=	nvme_strom.h nvme_strom.c extra_ksyms.c		\
		nvme_strom.rhel7.c md.rhel7.h raid0.rhel7.h

obj-m := nvme_strom.o
ccflags-y := -I. -I$(NVIDIA_SOURCE) 				\
	-DNVME_STROM_VERSION='"$(NVME_STROM_VERSION)"'		\
	-DNVME_STROM_BUILD_TIMESTAMP='"$(NVME_STROM_BUILD_TIMESTAMP)"'

default: modules nvme_test

nvme_test: nvme_test.c nvme_strom.h
	$(CC) -Wall nvme_test.c -o $@ $(USERSPACE_FLAGS) -lcuda -lpthread

# TODO:
# Run 'dkms install $(NVME_STROM_PKGNAME)/$(NVME_STROM_VERSION)' here.
# However, dkms command raises an error if old version is already
# installed. So, we use manual operations instead.
#
install-dkms:
	mkdir -p $(DKMS_DEST)
	install -m 0644 dkms.conf Makefile $(KMOD_SOURCE) $(DKMS_DEST)
	install -m 0644 nvme_strom.modprobe.conf /etc/modprobe.d

clean:
	rm -f $(EXTRA_CLEAN)
	$(MAKE) -C $(KERNEL_SOURCE) M=$(PWD) $@

%:
	$(MAKE) -C $(KERNEL_SOURCE) \
	KBUILD_EXTRA_SYMBOLS=$(NVIDIA_SOURCE)/Module.symvers \
	NVIDIA_SOURCE=$(NVIDIA_SOURCE) M=$(PWD) $@

.PHONY: default
