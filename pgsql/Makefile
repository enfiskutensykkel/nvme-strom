PG_CONFIG := pg_config

MODULES = nvme_strom
EXTENSION = nvme_strom
#PG_CPPFLAGS := -O0 -g

PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
