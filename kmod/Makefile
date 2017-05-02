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
KERNEL_VERSION_NUM := $(shell echo $(KERNEL_VER)	\
	| sed -s 's/-/ /g' | awk '{print $$1}'		\
	| sed -e 's/\./ /g' -e 's/[A-Za-z].*$$//g'	\
	| awk '{printf "%d%02d%02d", $$1, $$2, (NF >=3) ? $$3 : 0}')
KERNEL_RELEASE_NUM := $(shell echo $(KERNEL_VER)	\
	| sed -s 's/-/ /g' | awk '{print $$2}'		\
	| sed -e 's/\./ /g' -e 's/[A-Za-z].*$$//g'	\
	| awk '{printf "%d", $$1}')

KMOD_SOURCE :=	nvme_strom.h nvme_strom.c extra_ksyms.c pmemmap.c \
	rhel7_local.h \
	$(shell cd $(M) && ls */md.h */raid0.h */nvme.h)

obj-m := nvme_strom.o
ccflags-y := -I.									\
	-DNVME_STROM_VERSION='"$(NVME_STROM_VERSION)"'	\
	-DNVME_STROM_BUILD_TIMESTAMP='"$(NVME_STROM_BUILD_TIMESTAMP)"' \
	-DKERNEL_VERSION_NUM=$(KERNEL_VERSION_NUM)		\
	-DKERNEL_RELEASE_NUM=$(KERNEL_RELEASE_NUM)

default: modules

# TODO:
# Run 'dkms install $(NVME_STROM_PKGNAME)/$(NVME_STROM_VERSION)' here.
# However, dkms command raises an error if old version is already
# installed. So, we use manual operations instead.
#
install-dkms:
	mkdir -p $(DKMS_DEST)
	@(cd $(M) && tar zc -p $(KMOD_SOURCE)) | (cd $(DKMS_DEST) && tar zxv)
	install -m 0644 dkms.conf Makefile $(DKMS_DEST)
	install -m 0644 nvme_strom.modprobe.conf /etc/modprobe.d/
	install -m 0644 nvme_strom.modload.conf  /etc/modules-load.d/

clean:
	$(MAKE) -C $(KERNEL_SOURCE) M=$(PWD) $@

modules:
	$(MAKE) -C $(KERNEL_SOURCE) M=$(PWD) $@

.PHONY: default
