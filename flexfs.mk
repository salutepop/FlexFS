
flexfs_SOURCES-y = \
	fs/fs_zenfs.cc \
	fs/zbd_zenfs.cc \
	fs/io_zenfs.cc \
	fs/zonefs_zenfs.cc \
	fs/zbdlib_zenfs.cc \
	fs/uringlib_zenfs.cc \
	fs/uring_cmd.cc \
	fs/fdpnvme.cc \
	fs/util.cc

flexfs_HEADERS-y = \
	fs/fs_zenfs.h \
	fs/zbd_zenfs.h \
	fs/io_zenfs.h \
	fs/version.h \
	fs/metrics.h \
	fs/snapshot.h \
	fs/filesystem_utility.h \
	fs/zonefs_zenfs.h \
	fs/zbdlib_zenfs.h \
	fs/uringlib_zenfs.h \
	fs/uring_cmd.h \
	fs/fdpnvme.h \
	fs/util.h

flexfs_PKGCONFIG_REQUIRES-y += "libzbd >= 1.5.0"
flexfs_PKGCONFIG_REQUIRES-y += "liburing >= 1.5.0"

flexfs_EXPORT_PROMETHEUS ?= n
flexfs_HEADERS-$(flexfs_EXPORT_PROMETHEUS) += fs/metrics_prometheus.h
flexfs_SOURCES-$(flexfs_EXPORT_PROMETHEUS) += fs/metrics_prometheus.cc
flexfs_CXXFLAGS-$(flexfs_EXPORT_PROMETHEUS) += -Dflexfs_EXPORT_PROMETHEUS=1
flexfs_PKGCONFIG_REQUIRES-$(flexfs_EXPORT_PROMETHEUS) += ", prometheus-cpp-pull == 1.1.0"

flexfs_SOURCES += $(flexfs_SOURCES-y)
flexfs_HEADERS += $(flexfs_HEADERS-y)
flexfs_CXXFLAGS += $(flexfs_CXXFLAGS-y)
flexfs_LDFLAGS += -u flexfs_filesystem_reg

flexfs_ROOT_DIR := $(shell dirname $(realpath $(lastword $(MAKEFILE_LIST))))

$(shell cd $(flexfs_ROOT_DIR) && ./generate-version.sh)
ifneq ($(.SHELLSTATUS),0)
$(error Generating ZenFS version failed)
endif

flexfs_PKGCONFIG_REQUIRES = $(flexfs_PKGCONFIG_REQUIRES-y)
