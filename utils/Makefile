CUDA_PATH_LIST := /usr/local/cuda /usr/local/cuda-*
CUDA_PATH := $(shell for x in $(CUDA_PATH_LIST);    \
	do test -e "$$x/include/cuda.h" && echo $$x; done | head -1)
USERSPACE_FLAGS := -g -I $(CUDA_PATH)/include -L $(CUDA_PATH)/lib64
UTILS = ssd2ram_test

all: $(UTILS)

ssd2ram_test: ../kmod/nvme_strom.h ssd2ram_test.c
	$(CC) -Wall ssd2ram_test.c -o $@ $(USERSPACE_FLAGS)

clean:
	rm -f $(UTILS)
