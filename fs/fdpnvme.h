/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include "util.h"
#include <liburing.h>
#include <linux/nvme_ioctl.h>
#include <sys/ioctl.h>
#include <vector>

// Reference: https://github.com/axboe/fio/blob/master/engines/nvme.h
// If the uapi headers installed on the system lacks nvme uring command
// support, use the local version to prevent compilation issues.
#define NVME_DEFAULT_IOCTL_TIMEOUT 0
#define NVME_IDENTIFY_DATA_SIZE 4096
#define NVME_IDENTIFY_CSI_SHIFT 24
#define NVME_IDENTIFY_CNS_NS 0
#define NVME_CSI_NVM 0

#define BLK_DEF_MAX_SECTORS 256 // 256KB
#define FDP_MAX_RUHS 128
// TODO: get FDP Config (Reclaim Unit Nominal Size (RUNS))
#define RU_SIZE 13079937024 // bytes

struct nvme_lbaf {
  __le16 ms;
  __u8 ds;
  __u8 rp;
};

struct nvme_id_ns {
  __le64 nsze;
  __le64 ncap;
  __le64 nuse;
  __u8 nsfeat;
  __u8 nlbaf;
  __u8 flbas;
  __u8 mc;
  __u8 dpc;
  __u8 dps;
  __u8 nmic;
  __u8 rescap;
  __u8 fpi;
  __u8 dlfeat;
  __le16 nawun;
  __le16 nawupf;
  __le16 nacwu;
  __le16 nabsn;
  __le16 nabo;
  __le16 nabspf;
  __le16 noiob;
  __u8 nvmcap[16];
  __le16 npwg;
  __le16 npwa;
  __le16 npdg;
  __le16 npda;
  __le16 nows;
  __le16 mssrl;
  __le32 mcl;
  __u8 msrc;
  __u8 rsvd81[11];
  __le32 anagrpid;
  __u8 rsvd96[3];
  __u8 nsattr;
  __le16 nvmsetid;
  __le16 endgid;
  __u8 nguid[16];
  __u8 eui64[8];
  struct nvme_lbaf lbaf[16];
  __u8 rsvd192[192];
  __u8 vs[3712];
};

static inline int ilog2(uint32_t i) {
  int log = -1;

  while (i) {
    i >>= 1;
    log++;
  }
  return log;
}

enum nvme_io_mgmt_recv_mo {
  NVME_IO_MGMT_RECV_RUH_STATUS = 0x1,
};

struct nvme_fdp_ruh_status_desc {
  uint16_t pid;
  uint16_t ruhid;
  uint32_t earutr;
  uint64_t ruamw;
  uint8_t rsvd16[16];
};

struct nvme_fdp_ruh_status {
  uint8_t rsvd0[14];
  uint16_t nruhsd;
  struct nvme_fdp_ruh_status_desc ruhss[];
};

enum nvme_admin_opcode {
  nvme_admin_identify = 0x06,
};

// NVMe specific data for a device
//
// This is needed because FDP-IO have to be sent through Io_Uring_Cmd interface.
// So NVMe data is needed for initialization and IO cmd formation.
class NvmeData {
public:
  NvmeData() = default;
  NvmeData &operator=(const NvmeData &) = default;

  explicit NvmeData(uint32_t nsId, uint64_t nuse, uint32_t blockSize,
                    uint32_t lbaShift, uint32_t maxTfrSize, uint64_t startLba)
      : nsId_(nsId), nuse_(nuse), blockSize_(blockSize), lbaShift_(lbaShift),
        maxTfrSize_(maxTfrSize), startLba_(startLba) {}

  // NVMe Namespace ID
  uint32_t nsId() const { return nsId_; }

  uint32_t blockSize() const { return blockSize_; }
  // LBA shift number to calculate blocksize
  uint32_t lbaShift() const { return lbaShift_; }

  // Get the max transfer size of NVMe device.
  uint32_t maxTfrSize() { return maxTfrSize_; }

  // Start LBA of the disk partition.
  // It will be 0, if there is no partition and just an NS.
  uint64_t startLba() const { return startLba_; }
  uint64_t nuse() const { return nuse_; }

private:
  uint32_t nsId_;
  uint64_t nuse_; // Namespace Utilization
  uint32_t blockSize_;
  uint32_t lbaShift_;
  uint32_t maxTfrSize_;
  uint64_t startLba_;
};

// FDP specific info and handling
//
// This embeds the FDP semantics and specific io-handling.
// Note: IO with FDP semantics need to be sent through Io_Uring_cmd interface
// as of now; and not supported through conventional block interfaces.
class FdpNvme {
public:
  explicit FdpNvme(const std::string &fileName, bool isTest = false);

  FdpNvme(const FdpNvme &) = delete;
  FdpNvme &operator=(const FdpNvme &) = delete;

  int openNvmeDevice(bool isChar, const std::string &bdevName, int flags);
  // Allocates an FDP specific placement handle. This handle will be
  // interpreted by the device for data placement.
  int allocateFdpHandle();

  // Get the max IO transfer size of NVMe device.
  uint32_t getMaxIOSize() { return nvmeData_.maxTfrSize(); }

  // Get the NVMe specific info on this device.
  NvmeData &getNvmeData() { return nvmeData_; }

  // Prepares the Uring_Cmd sqe for read command.
  void prepReadUringCmdSqe(struct io_uring_sqe &sqe, void *buf, size_t size,
                           off_t start);

  // Prepares the Uring_Cmd sqe for write command with FDP handle.
  void prepWriteUringCmdSqe(struct io_uring_sqe &sqe, void *buf, size_t size,
                            off_t start, int handle);
  int cfd() { return cfd_; }
  int bfd() { return bfd_; }
  io_uring *getRing() { return &ring_; }
  uint16_t getMaxPid() { return maxPIDIdx_; }

private:
  std::string getNvmeCharDevice(const std::string &bdevName);
  // Open Nvme Character device for the given block dev @fileName.
  void openNvmeDevice(const std::string &fileName);

  // Prepares the Uring_Cmd sqe for read/write command with FDP directives.
  void prepFdpUringCmdSqe(struct io_uring_sqe &sqe, void *buf, size_t size,
                          off_t start, uint8_t opcode, uint8_t dtype,
                          uint16_t dspec);

  // Get FDP PlacementID for a NVMe NS specific PHNDL
  uint16_t getFdpPID(uint16_t fdpPHNDL) { return placementIDs_[fdpPHNDL]; }

  // Reads NvmeData for a NVMe device
  NvmeData readNvmeInfo(const std::string &blockDevice);

  // Initialize the FDP device and populate necessary info.
  void initializeFDP(const std::string &blockDevice);
  void initializeIoUring(uint32_t qdepth);

  // Generic NVMe IO mgmnt receive cmd
  int nvmeIOMgmtRecv(uint32_t cfd, uint32_t nsid, void *data, uint32_t data_len,
                     uint8_t op, uint16_t op_specific);

  // 0u is considered as the default placement ID
  static constexpr uint16_t kDefaultPIDIdx = 0u;

  // The mapping table of PHNDL: PID in a Namespace
  std::vector<uint16_t> placementIDs_{};

  uint16_t maxPIDIdx_{0};
  uint16_t nextPIDIdx_{kDefaultPIDIdx + 1};
  NvmeData nvmeData_{};

  int cfd_; /* char device, ng0n1 */
  int bfd_; /* block device, nvme0n1 */

  // for io_uring
  struct io_uring ring_;
};

struct nvme_data {
  __u32 nsid;
  __u32 lba_shift;
  __u32 lba_size;
  __u32 lba_ext;
  __u16 ms;
  __u16 pi_size;
  __u8 pi_type;
  __u8 guard_type;
  __u8 pi_loc;
};
