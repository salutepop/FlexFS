cmake_minimum_required(VERSION 3.4)

set(flexfs_SOURCES "fs/fs_zenfs.cc" "fs/zbd_zenfs.cc" "fs/io_zenfs.cc" "fs/zonefs_zenfs.cc"
    "fs/uringlib_zenfs.cc" "fs/uring_cmd.cc" "fs/fdpnvme.cc" "fs/util.cc"
    "fs/zbdlib_zenfs.cc" PARENT_SCOPE)
set(flexfs_HEADERS "fs/fs_zenfs.h" "fs/zbd_zenfs.h" "fs/io_zenfs.h" "fs/version.h" "fs/metrics.h"
    "fs/uringlib_zenfs.h" "fs/uring_cmd.h" "fs/fdpnvme.h" "fs/util.h"
    "fs/snapshot.h" "fs/filesystem_utility.h" "fs/zonefs_zenfs.h" "fs/zbdlib_zenfs.h" PARENT_SCOPE)
set(flexfs_LIBS "zbd" "uring" PARENT_SCOPE)
set(flexfs_CMAKE_EXE_LINKER_FLAGS "-u zenfs_filesystems_reg" PARENT_SCOPE)

if(NOT flexfs_SKIP_LIBZBD_CHECK)
    find_package(PkgConfig REQUIRED)
    pkg_check_modules(ZBD REQUIRED libzbd>=1.5.0)
    pkg_check_modules(URING REQUIRED liburing>=1.5.0)
endif()

execute_process(WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    RESULT_VARIABLE GENVER_RESULT
    COMMAND ./generate-version.sh)
if(GENVER_RESULT AND NOT GENVER_RESULT EQUAL 0)
    message(FATAL_ERROR "Generating ZenFS version failed")
endif()
