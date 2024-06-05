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

#include "fdpnvme.h"
#include "uring_cmd.h"

#include <endian.h>

FdpNvme::FdpNvme(const std::string &bdevName, bool isTest) {
  if (isTest) {
    openNvmeDevice(bdevName);
  }
  initializeFDP(bdevName);
}

int FdpNvme::allocateFdpHandle() {
  uint16_t phndl;

  // Get NS specific Fdp Placement Handle(PHNDL)
  if (nextPIDIdx_ <= maxPIDIdx_) {
    phndl = nextPIDIdx_++;
  } else {
    phndl = kDefaultPIDIdx;
  }

  return static_cast<uint16_t>(phndl);
}

void FdpNvme::initializeIoUring(uint32_t qdepth) {
  int err;
  err = io_uring_queue_init(qdepth, &ring_, 0);
  if (err) {
    throw std::invalid_argument("Failed to initialize IoUring");
  }
}

void FdpNvme::initializeFDP(const std::string &bdevName) {
  struct nvme_fdp_ruh_status *ruh_status;
  int cfd, bytes, err;
  cfd = openNvmeDevice(true, getNvmeCharDevice(bdevName).c_str(), O_RDONLY);

  nvmeData_ = readNvmeInfo(bdevName);

  bytes = sizeof(*ruh_status) +
          FDP_MAX_RUHS * sizeof(struct nvme_fdp_ruh_status_desc);
  ruh_status = (nvme_fdp_ruh_status *)malloc(bytes);

  err = nvmeIOMgmtRecv(cfd, nvmeData_.nsId(), ruh_status, bytes,
                       NVME_IO_MGMT_RECV_RUH_STATUS, 0);
  close(cfd);

  if (err) {
    throw std::invalid_argument("Failed to initialize FDP; nruhsd is 0");
  } else {
    std::cout << ruh_status->nruhsd << std::endl;
  }
  placementIDs_.reserve(ruh_status->nruhsd);
  maxPIDIdx_ = ruh_status->nruhsd - 1;
  for (uint16_t i = 0; i <= maxPIDIdx_; ++i) {
    placementIDs_[i] = ruh_status->ruhss[i].pid;
  }
}

// NVMe IO Mnagement Receive fn for specific config reading
int FdpNvme::nvmeIOMgmtRecv(uint32_t cfd, uint32_t nsid, void *data,
                            uint32_t data_len, uint8_t op,
                            uint16_t op_specific) {
  // Build the I/O management receive command
  // For further details on the CDB format, consult the specification
  // available as "TP4146 Flexible Data Placement 2022.11.30 Ratified"
  // in the following link:
  // https://nvmexpress.org/wp-content/uploads/NVM-Express-2.0-Ratified-TPs_20230111.zip
  uint32_t cdw10 = (op & 0xf) | (op_specific & 0xff << 16);
  uint32_t cdw11 = (data_len >> 2) - 1; // cdw11 is 0 based

  struct nvme_passthru_cmd cmd = {
      .opcode = nvme_cmd_io_mgmt_recv,
      .nsid = nsid,
      .addr = (uint64_t)(uintptr_t)data,
      .data_len = data_len,
      .cdw10 = cdw10,
      .cdw11 = cdw11,
      .timeout_ms = NVME_DEFAULT_IOCTL_TIMEOUT,
  };

  return ioctl(cfd, NVME_IOCTL_IO_CMD, &cmd);
}

void FdpNvme::prepFdpUringCmdSqe(struct io_uring_sqe &sqe, void *buf,
                                 size_t size, off_t start, uint8_t opcode,
                                 uint8_t dtype, uint16_t dspec) {
  uint32_t maxTfrSize = nvmeData_.maxTfrSize();
  if ((maxTfrSize != 0) && (size > maxTfrSize)) {
    throw std::invalid_argument("Exceeds max Transfer size");
  }
  // Clear the SQE entry to avoid some arbitrary flags being set.
  memset(&sqe, 0, sizeof(struct io_uring_sqe));

  sqe.fd = cfd_;
  sqe.opcode = IORING_OP_URING_CMD;
  sqe.cmd_op = NVME_URING_CMD_IO;

  struct nvme_uring_cmd *cmd = (struct nvme_uring_cmd *)&sqe.cmd;
  if (cmd == nullptr) {
    throw std::invalid_argument("Uring cmd is NULL!");
  }
  memset(cmd, 0, sizeof(struct nvme_uring_cmd));
  cmd->opcode = opcode;

  // start LBA of the IO = Req_start (offset in partition) + Partition_start
  uint64_t sLba = (start >> nvmeData_.lbaShift()) + nvmeData_.startLba();
  uint32_t nLb = (size >> nvmeData_.lbaShift()) - 1; // nLb is 0 based

  /* cdw10 and cdw11 represent starting lba */
  cmd->cdw10 = sLba & 0xffffffff;
  cmd->cdw11 = sLba >> 32;
  /* cdw12 represent number of lba's for read/write */
  cmd->cdw12 = (dtype & 0xFF) << 20 | nLb;
  cmd->cdw13 = (dspec << 16);
  cmd->addr = (uint64_t)buf;
  cmd->data_len = size;

  cmd->nsid = nvmeData_.nsId();
}

void FdpNvme::prepReadUringCmdSqe(struct io_uring_sqe &sqe, void *buf,
                                  size_t size, off_t start) {
  // Placement Handle is not used for read.
  prepFdpUringCmdSqe(sqe, buf, size, start, nvme_cmd_read, 0, 0);
}

void FdpNvme::prepWriteUringCmdSqe(struct io_uring_sqe &sqe, void *buf,
                                   size_t size, off_t start, int handle) {
  static constexpr uint8_t kPlacementMode = 2;
  uint16_t pid;

  if (handle == -1) {
    pid = getFdpPID(kDefaultPIDIdx); // Use the default stream
  } else if (handle >= 0 && handle <= maxPIDIdx_) {
    pid = getFdpPID(static_cast<uint16_t>(handle));
  } else {
    throw std::invalid_argument("Invalid placement identifier");
  }

  prepFdpUringCmdSqe(sqe, buf, size, start, nvme_cmd_write, kPlacementMode,
                     pid);
}

// Reads the NVMe related info from a valid NVMe device path
NvmeData FdpNvme::readNvmeInfo(const std::string &bdevName) {
  struct nvme_id_ns ns;
  int fd;
  __u32 nsid = 0, lba_size = 0, lba_shift = 0;
  uint64_t nuse = 0;
  uint64_t startLba{0};

  try {
    fd = open(bdevName.c_str(), O_RDONLY);
    nsid = ioctl(fd, NVME_IOCTL_ID);

    struct nvme_passthru_cmd cmd = {
        .opcode = nvme_admin_identify,
        .nsid = nsid,
        .addr = (__u64)(uintptr_t)&ns,
        .data_len = NVME_IDENTIFY_DATA_SIZE,
        .cdw10 = NVME_IDENTIFY_CNS_NS,
        .cdw11 = NVME_CSI_NVM << NVME_IDENTIFY_CSI_SHIFT,
        .timeout_ms = NVME_DEFAULT_IOCTL_TIMEOUT,
    };

    ioctl(fd, NVME_IOCTL_ADMIN_CMD, &cmd);

    lba_size = 1 << ns.lbaf[(ns.flbas & 0x0f)].ds;
    lba_shift = ilog2(lba_size);
    nuse = ns.nuse;

    close(fd);
  } catch (const std::exception &e) {
    std::cout << e.what() << std::endl;
  }

  return NvmeData{nsid,    nuse, lba_size, lba_shift, BLK_DEF_MAX_SECTORS,
                  startLba};
}

// Converts an nvme block device name (ex: /dev/nvme0n1p1) to corresponding
// nvme char device name (ex: /dev/ng0n1), to use Nvme FDP directives.
std::string FdpNvme::getNvmeCharDevice(const std::string &bdevName) {
  // Extract dev and NS IDs, and ignore partition ID.
  // Example: extract the string '0n1' from '/dev/nvme0n1p1'
  size_t devPos = bdevName.find_first_of("0123456789");
  size_t pPos = bdevName.find('p', devPos);

  return "/dev/ng" + bdevName.substr(devPos, pPos - devPos);
}

// Open Nvme Character device for the given block dev @bdevName.
// Throws std::system_error if failed.
void FdpNvme::openNvmeDevice(const std::string &bdevName) {
  // int flags{O_RDONLY};
  int flags{O_RDWR};

  try {
    auto cdevName = getNvmeCharDevice(bdevName);
    cfd_ = open(cdevName.c_str(), flags);
    bfd_ = open(bdevName.c_str(), flags);
  } catch (const std::system_error &) {
    throw;
  }
}

int FdpNvme::openNvmeDevice(bool isChar, const std::string &bdevName,
                            int flags) {
  int fd = -1;
  try {
    if (isChar) {
      fd = open(getNvmeCharDevice(bdevName).c_str(), flags);
    } else {
      fd = open(bdevName.c_str(), flags | O_DIRECT);
    }
  } catch (const std::system_error &) {
    throw;
  }
  return fd;
}
