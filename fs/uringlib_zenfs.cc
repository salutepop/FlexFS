// Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
// Copyright (c) 2019-present, Western Digital Corporation
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#if !defined(ROCKSDB_LITE) && !defined(OS_WIN)

#include "uringlib_zenfs.h"

#include <errno.h>
#include <fcntl.h>
#include <libzbd/zbd.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <fstream>
#include <string>

#include "rocksdb/env.h"
#include "rocksdb/io_status.h"

namespace ROCKSDB_NAMESPACE {

thread_local std::unique_ptr<UringCmd> UringlibBackend::uringCmd_ = nullptr;

UringlibBackend::UringlibBackend(std::string bdevname)
    : filename_("/dev/" + bdevname),
      read_f_(-1),
      read_direct_f_(-1),
      write_f_(-1),
      fdp_(filename_) {}
// INFO: Shared ring
//      ,uringCmd_(32, fdp_.getNvmeData().blockSize(),
//                fdp_.getNvmeData().lbaShift(), io_uring_params{}) {}

void UringlibBackend::initializeUringCmd() {
  uringCmd_ = std::make_unique<UringCmd>(32, fdp_.getNvmeData().blockSize(),
                                         fdp_.getNvmeData().lbaShift(),
                                         io_uring_params{});
}

bool UringlibBackend::isUringCmdInitialized() const {
  return uringCmd_ != nullptr;
}
std::string UringlibBackend::ErrorToString(int err) {
  char *err_str = strerror(err);
  if (err_str != nullptr) return std::string(err_str);
  return "";
}

IOStatus UringlibBackend::CheckScheduler() { return IOStatus::OK(); }

IOStatus UringlibBackend::Open(bool readonly, bool exclusive,
                               unsigned int *max_active_zones,
                               unsigned int *max_open_zones) {
  /* The non-direct file descriptor acts as an exclusive-use semaphore */
  if (exclusive) {
    read_f_ = fdp_.openNvmeDevice(true, filename_.c_str(), O_RDONLY | O_EXCL);
    // INFO: uringCmdRead -> pread (block align..)
    // read_f_ = fdp_.openNvmeDevice(false, filename_.c_str(), O_RDONLY |
    // O_EXCL);
  } else {
    read_f_ = fdp_.openNvmeDevice(true, filename_.c_str(), O_RDONLY);
    // INFO: uringCmdRead -> pread (block align..)
    // read_f_ = fdp_.openNvmeDevice(false, filename_.c_str(), O_RDONLY |
    // O_EXCL);
    //  WARN: 임시로 o_direct 실험
    //   read_f_ = fdp_.openNvmeDevice(false, filename_.c_str(), O_RDONLY);
  }

  if (read_f_ < 0) {
    return IOStatus::InvalidArgument(
        "Failed to open zoned block device for read: " + ErrorToString(errno));
  }

  // read_direct_f_ = fdp_.openNvmeDevice(true, filename_.c_str(), O_RDONLY |
  // O_DIRECT);
  read_direct_f_ = read_f_;  // O_DIRECT flag support is not present with
                             // nvme-ns charatcer devices

  if (read_direct_f_ < 0) {
    return IOStatus::InvalidArgument(
        "Failed to open zoned block device for direct read: " +
        ErrorToString(errno));
  }

  if (readonly) {
    write_f_ = -1;
    write_bf_ = -1;
  } else {
    write_f_ = fdp_.openNvmeDevice(true, filename_.c_str(), O_WRONLY);
    write_bf_ = fdp_.openNvmeDevice(false, filename_.c_str(), O_WRONLY);
    if ((write_f_ < 0) || (write_bf_ < 0)) {
      return IOStatus::InvalidArgument(
          "Failed to open character or block device for write: " +
          ErrorToString(errno));
    }
  }

  // TODO: Don't use
  IOStatus ios = CheckScheduler();
  if (ios != IOStatus::OK()) return ios;

  NvmeData nvmeData = fdp_.getNvmeData();

  // TODO: NUSE:hard-coding(RU_SIZE), 스펙 문서 163페이지 참고
  block_sz_ = nvmeData.blockSize();
  // zone_sz_ = RU_SIZE / block_sz_;
  zone_sz_ = RU_SIZE;
  nr_zones_ = nvmeData.nuse() / (zone_sz_ / block_sz_);
  *max_active_zones = fdp_.getMaxPid() + 1;
  *max_open_zones = fdp_.getMaxPid() + 1;

  /*
  std::stringstream info;
  info << "Open Backend" << "\n ";
  info << "read_f : " << read_f_ << "\n ";
  info << "write_f : " << write_f_ << "\n ";
  info << "block_sz : " << block_sz_ << "\n ";
  info << "zone_sz : " << zone_sz_ << "\n ";
  info << "nr_zones : " << nr_zones_ << "\n ";
  info << "max_active_zones : " << *max_active_zones << "\n ";
  info << "max_open_zones : " << *max_open_zones << "\n ";
  info << "max pid : " << fdp_.getMaxPid() << "\n ";
  info << "nvmeData nuse : " << nvmeData.nuse() << "\n ";
  info << "RU SIZE : " << RU_SIZE << " ";
  LOG("[DBG]", info.str());
  */

  return IOStatus::OK();
}

std::unique_ptr<ZoneList> UringlibBackend::ListZones() {
  struct zbd_zone *zones;
  struct zbd_zone zone;
  // unsigned int shift = ilog2(block_sz_);

  /* Allocate zone array */
  zones = (struct zbd_zone *)calloc(nr_zones_, sizeof(struct zbd_zone));
  if (!zones) {
    return nullptr;
  }

  for (int n = 0; n < (int)nr_zones_; n++) {
    // zone.start = (n * zone_sz_) << shift;
    // zone.len = zone_sz_ << shift;
    //  zone.capacity = zone_sz_ << shift;
    zone.start = (n * zone_sz_);
    zone.len = zone_sz_;
    zone.capacity = zone_sz_;
    // if (n == 0) {
    if (n < 2) {
      // superblock * 2
      zone.wp = zone.start + block_sz_ * 2;
    } else {
      zone.wp = zone.start;
      // zone.wp = zone.start + zone.len - 1;
    }

    zone.type = ZBD_ZONE_TYPE_SWR;
    zone.cond = ZBD_ZONE_COND_EMPTY;
    zone.flags = 0;
    zone.flags |= ZBD_ZONE_RWP_RECOMMENDED;
    zone.flags |= ZBD_ZONE_NON_SEQ_RESOURCES;

    memcpy(&zones[n], &zone, sizeof(zone));
  }

  std::unique_ptr<ZoneList> zl(new ZoneList(zones, nr_zones_));

  return zl;
}

IOStatus UringlibBackend::Reset(uint64_t start, bool *offline,
                                uint64_t *max_capacity) {
  //  LOG("[Reset-Discard] Zone", start / zone_sz_);
  int err;
  err = uringCmd_->uringDiscard(write_bf_, start, zone_sz_);
  if (err) {
    return IOStatus::IOError("Discard fail");
  }
  /*
  unsigned int report = 1;
  struct zbd_zone z;
  int ret;

  ret = zbd_reset_zones(write_f_, start, zone_sz_);
  if (ret) return IOStatus::IOError("Zone reset failed\n");

  ret = zbd_report_zones(read_f_, start, zone_sz_, ZBD_RO_ALL, &z, &report);

  if (ret || (report != 1)) return IOStatus::IOError("Zone report failed\n");

  // TODO: 언제 offline이 되는거지? full?
  if (zbd_zone_offline(&z)) {
    *offline = true;
    *max_capacity = 0;
  } else {
    *offline = false;
    *max_capacity = zbd_zone_capacity(&z);
  }
  */
  *offline = false;
  *max_capacity = zone_sz_;
  // unsigned int shift = ilog2(block_sz_);
  //*max_capacity = zone_sz_ << shift;

  // DummyFunc(start);
  return IOStatus::OK();
}

IOStatus UringlibBackend::Finish(uint64_t start) {
  // int ret;

  // ret = zbd_finish_zones(write_f_, start, zone_sz_);
  // if (ret) return IOStatus::IOError("Zone finish failed\n");

  DummyFunc(start);
  return IOStatus::OK();
}

IOStatus UringlibBackend::Close(uint64_t start) {
  // int ret;

  // ret = zbd_close_zones(write_f_, start, zone_sz_);
  // if (ret) return IOStatus::IOError("Zone close failed\n");

  DummyFunc(start);
  return IOStatus::OK();
}

int UringlibBackend::InvalidateCache(uint64_t pos, uint64_t size) {
  DummyFunc(pos);
  DummyFunc(size);
  return 0;
}

int UringlibBackend::Read(char *buf, int size, uint64_t pos, bool direct) {
  if (!isUringCmdInitialized()) {
    initializeUringCmd();
  }
  return uringCmd_->uringCmdRead(direct ? read_direct_f_ : read_f_,
                                 fdp_.getNvmeData().nsId(), pos, size, buf);
}

int UringlibBackend::Write(char *data, uint32_t size, uint64_t pos) {
  if (!isUringCmdInitialized()) {
    initializeUringCmd();
  }
  uint32_t dspec = 0;
  int ret = 0;
  ret = uringCmd_->uringCmdWrite(write_f_, fdp_.getNvmeData().nsId(), pos, size,
                                 data, dspec);
  return ret;
}

}  // namespace ROCKSDB_NAMESPACE

#endif  // !defined(ROCKSDB_LITE) && !defined(OS_WIN)
