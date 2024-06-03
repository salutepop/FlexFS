#include "uring_cmd.h"

#include <liburing.h>

UringCmd::UringCmd(uint32_t qd, uint32_t blocksize, uint32_t lbashift,
                   io_uring_params params)
    : qd_(qd),
      blocksize_(blocksize),
      lbashift_(lbashift),
      req_limitmax_(qd),
      req_limitlow_(qd >> 1),
      req_inflight_(0) {
  initBuffer();
  initUring(params);
}

/* unused, 나중에 fixed buffer 적용 시 사용 예정 */
void UringCmd::initBuffer() {
  int err;
  void *buf;

  iovecs_ = (struct iovec *)calloc(qd_, sizeof(struct iovec));

  for (int i = 0; i < roundup_pow2(qd_); i++) {
    // std::cout << "POW i = " << i << std::endl;
    err = posix_memalign(&buf, PAGE_SIZE, blocksize_ * qd_);
    if (err) {
      std::cerr << "failed mem align, err= " << err << std::endl;
    }
    // std::cout << "i, buf= " << i << ", " << buf << std::endl;
    iovecs_[i].iov_base = buf;
    iovecs_[i].iov_len = BS;
  }
}

void UringCmd::initUring(io_uring_params &params) {
  // 모든 멤버가 0으로 초기화된 io_uring_params 구조체를 생성
  io_uring_params empty_params;
  memset(&empty_params, 0, sizeof(empty_params));

  // params가 비어 있는지 확인
  if (memcmp(&params, &empty_params, sizeof(io_uring_params)) == 0) {
    struct io_uring_params p;
    memset(&p, 0, sizeof(p));
    p.flags |= IORING_SETUP_SQE128;
    p.flags |= IORING_SETUP_CQE32;

    p.flags |= IORING_SETUP_CQSIZE;
    p.cq_entries = qd_ * 2;  // cq size = sq size * 2, to dealwith cq overflow

    p.flags |= IORING_SETUP_COOP_TASKRUN;
    // p.flags |= IORING_SETUP_SINGLE_ISSUER | IORING_SETUP_DEFER_TASKRUN;
    p.flags |= IORING_SETUP_SINGLE_ISSUER;

    params_ = p;
  } else {
    params_ = params;
  }

  io_uring_queue_init_params(qd_, &ring_, &params_);
}

void UringCmd::prepUringCmd(int fd, int ns, bool is_read, off_t offset,
                            size_t size, void *buf, uint32_t dtype,
                            uint32_t dspec) {
  struct io_uring_sqe *sqe = io_uring_get_sqe(&ring_);
  struct nvme_uring_cmd *cmd;
  // struct iovec iovec;
  // iovec.iov_base = buf;
  // iovec.iov_len = size;
  memset(sqe, 0, sizeof(*sqe));
  sqe->fd = fd;
  sqe->cmd_op = NVME_URING_CMD_IO;
  // sqe->cmd_op = NVME_URING_CMD_IO_VEC;
  sqe->opcode = IORING_OP_URING_CMD;
  sqe->user_data = 0;

  cmd = (struct nvme_uring_cmd *)sqe->cmd;
  memset(cmd, 0, sizeof(struct nvme_uring_cmd));
  cmd->opcode = is_read ? nvme_cmd_read : nvme_cmd_write;
  __u64 slba;
  __u32 nlb;
  slba = offset >> lbashift_;
  // if (size < blocksize_) {
  //   size = blocksize_;
  // }
  if (size < blocksize_) {
    nlb = 0;
  } else {
    nlb = (size >> lbashift_) - 1;
  }

  // std::cout << "slba, nlba, lba_shift : " << slba << ", " << nlb << ", "
  //           << lbashift_ << std::endl;

  cmd->cdw10 = slba & 0xffffffff;
  cmd->cdw11 = slba >> 32;
  // cmd->cdw12 = nlb; //non fdp
  cmd->cdw12 = (dtype & 0xFF) << 20 | nlb;
  cmd->cdw13 = (dspec << 16);

  // cmd->addr = (__u64)(uintptr_t)iovecs[0].iov_base;
  // cmd->data_len = iovecs[0].iov_len;

  // cmd->addr = (__u64)(uintptr_t)&iovec[0];
  cmd->addr = (__u64)(uintptr_t)buf;
  cmd->data_len = size;
  cmd->nsid = ns;
}

void UringCmd::prepUring(int fd, bool is_read, off_t offset, size_t size,
                         void *buf) {
  struct io_uring_sqe *sqe = io_uring_get_sqe(&ring_);
  struct iovec iov;
  iov.iov_base = buf;
  iov.iov_len = size;

  if (is_read) {
    io_uring_prep_read(sqe, fd, iov.iov_base, iov.iov_len, offset);
  } else {
    io_uring_prep_write(sqe, fd, iov.iov_base, iov.iov_len, offset);
  }
}

int UringCmd::submitCommand(int nr_reqs) {
  int err;

  /*
  if (((*ring_.sq.kflags) & IORING_SQ_CQ_OVERFLOW)) {
    DBG("uring_submit", err);
    DBG("flag", ring_.sq.kflags);
    WaitCompleted();
  }
  */
  if (nr_reqs > 0) {
    err = io_uring_submit_and_wait(&ring_, nr_reqs);
  } else {
    err = io_uring_submit(&ring_);
  }
  DBG("uring_submit", err);
  return err;
}

int UringCmd::waitCompleted() {
  struct io_uring_cqe *cqe = NULL;
  int err;
  struct __kernel_timespec timeout;
  timeout.tv_sec = 1;
  timeout.tv_nsec = 0;

  // err = io_uring_wait_cqe_nr(&ring_, &cqe, qd_);
  // err = io_uring_wait_cqe(&ring_, &cqe);
  err = io_uring_wait_cqe_timeout(&ring_, &cqe, &timeout);
  if (err != 0) {
    LOG("uring_wait_cqe", err);
  }
  if (cqe->res < 0) {
    LOG("cqe->res", cqe->res);
  }
  // DBG("[ERR] cq_has_overflow", io_uring_cq_has_overflow(&ring_));
  io_uring_cqe_seen(&ring_, cqe);
  return cqe->res;
}

int UringCmd::uringRead(int fd, off_t offset, size_t size, void *buf) {
  prepUring(fd, op_read, offset, size, buf);
  submitCommand();
  return waitCompleted();
}
int UringCmd::uringWrite(int fd, off_t offset, size_t size, void *buf) {
  prepUring(fd, op_write, offset, size, buf);
  submitCommand();
  return waitCompleted();
}
int UringCmd::uringCmdRead(int fd, int ns, off_t offset, size_t size,
                           void *buf) {
  int ret;
  off_t zOffset = (offset / blocksize_) * blocksize_;
  off_t lastOffset = offset + size - 1;
  uint32_t nBlocks = ((lastOffset - zOffset) / blocksize_) + 1;
  uint64_t nSize = nBlocks * blocksize_;
  uint32_t maxTfrbytes = 64 * blocksize_;  // mdts :6 (2^6) blocks

  std::stringstream info;
  info << "offset: " << offset << ", ";
  info << "size: " << size << ", ";
  info << "zOffset: " << zOffset << ", ";
  info << "nSize: " << nSize << ", ";
  info << "lastOffset: " << lastOffset << ", ";
  info << "nBlocks: " << nBlocks;
  LOG("READ Arguments", info.str());

  if (nSize > maxTfrbytes) {
    return -EINVAL;
  }
  bool isAligned =
      // requested offset == zero based offset
      (zOffset == offset) &&
      // if not zero, need memcpy
      ((size % blocksize_) == 0);

  if (isAligned) {
    prepUringCmd(fd, ns, op_read, offset, size, buf);
    submitCommand();
    ret = waitCompleted();
  } else {
    LOG("Read warining, isn't aligned data (size or offset)", size);
    void *tempBuf;
    if (posix_memalign((void **)&tempBuf, PAGE_SIZE, nSize)) {
      LOG("[ERROR]", "MEM Align");
    }
    prepUringCmd(fd, ns, op_read, zOffset, nSize, tempBuf);
    submitCommand();
    ret = waitCompleted();
    if (ret == 0) {
      memcpy(buf, (char *)tempBuf + (offset - zOffset), size);
      ret = size;
    }
    free(tempBuf);
  }

  if (ret == 0) {
    ret = size;
  }
  return ret;
}
int UringCmd::uringCmdWrite(int fd, int ns, off_t offset, size_t size,
                            void *buf, uint32_t dspec) {
  const uint32_t kPlacementMode = 2;
  int ret;
  off_t zOffset = (offset / blocksize_) * blocksize_;
  off_t lastOffset = offset + size - 1;
  uint32_t nBlocks = ((lastOffset - zOffset) / blocksize_) + 1;
  uint64_t nSize = nBlocks * blocksize_;
  uint32_t maxTfrbytes = 64 * blocksize_;  // mdts :6 (2^6) blocks

  std::stringstream info;
  info << "offset: " << offset << ", ";
  info << "size: " << size << ", ";
  info << "zOffset: " << zOffset << ", ";
  info << "nSize: " << nSize << ", ";
  info << "lastOffset: " << lastOffset << ", ";
  info << "nBlocks: " << nBlocks;
  LOG("Write Arguments", info.str());

  if (nSize > maxTfrbytes) {
    return -EINVAL;
  }
  bool isAligned =
      // requested offset == zero based offset
      (zOffset == offset) &&
      // if not zero, need memcpy
      ((size % blocksize_) == 0);

  void *tempBuf;
  if (!isAligned) {
    LOG("Write Warning, isn't aligned data (size or offset), do RMW, offset",
        offset);

    // INFO: Read-Modify-Write
    if (posix_memalign((void **)&tempBuf, PAGE_SIZE, nSize)) {
      LOG("[ERROR]", "MemAlign fail");
      free(tempBuf);
      return -EINVAL;
    }
    ret = uringCmdRead(fd, ns, zOffset, nSize, tempBuf);
    if (ret != (int)nSize) {
      LOG("[ERROR]", "RMW-Read fail");
      free(tempBuf);
      return -EINVAL;
    }
    memset(tempBuf, 0, nSize);
    memcpy((char *)tempBuf + (offset - zOffset), buf, size);
    LOG("Write data(buf)", buf);
    LOG("Write data(tempBuf)", buf);
    prepUringCmd(fd, ns, op_write, zOffset, nSize, tempBuf, kPlacementMode,
                 dspec);
  } else {
    prepUringCmd(fd, ns, op_write, offset, size, buf, kPlacementMode, dspec);
  }
  submitCommand();
  ret = waitCompleted();
  if (ret == 0) {
    ret = size;
  }
  // free(tempBuf);
  return ret;
}
int UringCmd::isCqOverflow() { return io_uring_cq_has_overflow(&ring_); }
