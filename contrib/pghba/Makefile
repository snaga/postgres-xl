# contrib/pghba/Makefile

MODULE_big = pghba
OBJS = pghba.o

EXTENSION = pghba
DATA = pghba--1.0.sql pghba--unpackaged--1.0.sql

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/pghba
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
