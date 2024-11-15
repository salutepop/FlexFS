// Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
// Copyright (c) 2019-present, Western Digital Corporation
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#if !defined(ROCKSDB_LITE) && defined(OS_LINUX)

#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "fdpnvme.h"
#include "rocksdb/io_status.h"
#include "uring_cmd.h"
#include "zbd_zenfs.h"

namespace ROCKSDB_NAMESPACE {

class UringlibBackend : public ZonedBlockDeviceBackend {
 private:
  std::string filename_;

  // _f_ : character device
  // _bf_ : block device
  int read_f_;
  int read_direct_f_;
  int write_f_;
  int write_bf_;

  FdpNvme fdp_;
  static thread_local std::unique_ptr<UringCmd> uringCmd_;

 public:
  explicit UringlibBackend(std::string bdevname);
  ~UringlibBackend() {
    // LOG("Close Uringlib backend, fd = ", write_f_);

    close(read_f_);
    close(read_direct_f_);
    close(write_f_);
  }

  void initializeUringCmd();  // 객체 초기화 함수
  bool isUringCmdInitialized() const;
  IOStatus Open(bool readonly, bool exclusive, unsigned int *max_active_zones,
                unsigned int *max_open_zones);
  std::unique_ptr<ZoneList> ListZones();
  IOStatus Reset(uint64_t start, bool *offline, uint64_t *max_capacity);
  IOStatus Finish(uint64_t start);
  IOStatus Close(uint64_t start);
  IOStatus Delete(uint64_t start, uint64_t size);
  int Read(char *buf, int size, uint64_t pos, bool direct);
  int Write(char *data, uint32_t size, uint64_t pos, uint32_t whint = 0);
  int InvalidateCache(uint64_t pos, uint64_t size);
  int RequestPrefetch(char *buf, int size, uint64_t pos, uint64_t userdata);
  int WaitPrefetch(uint64_t userdata);

  bool ZoneIsSwr(std::unique_ptr<ZoneList> &zones, unsigned int idx) {
    struct zbd_zone *z = &((struct zbd_zone *)zones->GetData())[idx];
    return zbd_zone_type(z) == ZBD_ZONE_TYPE_SWR;
  };

  bool ZoneIsOffline(std::unique_ptr<ZoneList> &zones, unsigned int idx) {
    struct zbd_zone *z = &((struct zbd_zone *)zones->GetData())[idx];
    return zbd_zone_offline(z);
  };

  bool ZoneIsWritable(std::unique_ptr<ZoneList> &zones, unsigned int idx) {
    struct zbd_zone *z = &((struct zbd_zone *)zones->GetData())[idx];
    return !(zbd_zone_full(z) || zbd_zone_offline(z) || zbd_zone_rdonly(z));
  };

  bool ZoneIsActive(std::unique_ptr<ZoneList> &zones, unsigned int idx) {
    struct zbd_zone *z = &((struct zbd_zone *)zones->GetData())[idx];
    return zbd_zone_imp_open(z) || zbd_zone_exp_open(z) || zbd_zone_closed(z);
  };

  bool ZoneIsOpen(std::unique_ptr<ZoneList> &zones, unsigned int idx) {
    struct zbd_zone *z = &((struct zbd_zone *)zones->GetData())[idx];
    return zbd_zone_imp_open(z) || zbd_zone_exp_open(z);
  };

  uint64_t ZoneStart(std::unique_ptr<ZoneList> &zones, unsigned int idx) {
    struct zbd_zone *z = &((struct zbd_zone *)zones->GetData())[idx];
    return zbd_zone_start(z);
  };

  uint64_t ZoneMaxCapacity(std::unique_ptr<ZoneList> &zones, unsigned int idx) {
    struct zbd_zone *z = &((struct zbd_zone *)zones->GetData())[idx];
    return zbd_zone_capacity(z);
  };

  uint64_t ZoneWp(std::unique_ptr<ZoneList> &zones, unsigned int idx) {
    struct zbd_zone *z = &((struct zbd_zone *)zones->GetData())[idx];
    return zbd_zone_wp(z);
  };

  std::string GetFilename() { return filename_; }

  // std::vector<uint64_t> GetWritePointer();

 private:
  IOStatus CheckScheduler();
  std::string ErrorToString(int err);
  uint64_t DummyFunc(uint64_t value) { return value; }
};

}  // namespace ROCKSDB_NAMESPACE

#endif  // !defined(ROCKSDB_LITE) && defined(OS_LINUX)
