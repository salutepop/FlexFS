#include "uring_cmd.h"

#include <liburing.h>

std::atomic_int cnt = 0;
UringCmd::UringCmd(uint32_t qd, uint32_t blocksize, uint32_t lbashift,
                   io_uring_params params)
    : qd_(qd),
      blocksize_(blocksize),
      lbashift_(lbashift),
      req_limitmax_(qd),
      req_limitlow_(qd >> 1),
      req_inflight_(0) {
  req_id_ = 0;
  LOG("Uring Construction", std::this_thread::get_id());
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

    // p.flags |= IORING_SETUP_COOP_TASKRUN;
    // p.flags |= IORING_SETUP_SINGLE_ISSUER | IORING_SETUP_DEFER_TASKRUN;
    // p.flags |= IORING_SETUP_SINGLE_ISSUER;

    params_ = p;
  } else {
    params_ = params;
  }

  int ret = io_uring_queue_init_params(qd_, &ring_, &params_);
  if (ret < 0) {
    LOG("ERR init ring", ret);
  }
}

void UringCmd::prepUringCmd(int fd, int ns, bool is_read, off_t offset,
                            size_t size, void *buf, uint64_t userdata,
                            uint32_t dtype, uint32_t dspec) {
  struct io_uring_sqe *sqe = io_uring_get_sqe(&ring_);
  struct nvme_uring_cmd *cmd;
  // struct iovec iovec;
  // iovec.iov_base = buf;
  // iovec.iov_len = size;
  if (sqe == NULL) {
    LOG("ERROR", "sqe is null");
  }
  memset(sqe, 0, sizeof(*sqe));

  sqe->fd = fd;
  sqe->cmd_op = NVME_URING_CMD_IO;
  // sqe->cmd_op = NVME_URING_CMD_IO_VEC;
  sqe->opcode = IORING_OP_URING_CMD;
  sqe->user_data = userdata;
  // sqe->flags |= IOSQE_IO_LINK;

  cmd = (struct nvme_uring_cmd *)sqe->cmd;
  memset(cmd, 0, sizeof(struct nvme_uring_cmd));
  cmd->opcode = is_read ? nvme_cmd_read : nvme_cmd_write;
  __u64 slba;
  __u32 nlb;
  slba = offset >> lbashift_;
  if (size < blocksize_) {
    nlb = 0;
  } else {
    nlb = (size >> lbashift_) - 1;
  }

  cmd->cdw10 = slba & 0xffffffff;
  cmd->cdw11 = slba >> 32;
  // cmd->cdw12 = nlb; //non fdp
  cmd->cdw12 = (dtype & 0xFF) << 20 | nlb;
  cmd->cdw13 = (dspec << 16);

  // cmd->addr = (__u64)(uintptr_t)iovecs[0].iov_base;
  // cmd->data_len = iovecs[0].iov_len;

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

  if (sqe == NULL) {
    LOG("ERROR", "sqe is null");
  }

  if (is_read) {
    io_uring_prep_read(sqe, fd, iov.iov_base, iov.iov_len, offset);
  } else {
    io_uring_prep_write(sqe, fd, iov.iov_base, iov.iov_len, offset);
  }
}

int UringCmd::submitCommand(int nr_reqs) {
  int err;

  if (nr_reqs > 0) {
    err = io_uring_submit_and_wait(&ring_, nr_reqs);
  } else {
    err = io_uring_submit(&ring_);
  }
  return err;
}

int UringCmd::waitCompleted(int nr_reqs) {
  struct io_uring_cqe *cqe = NULL;
  int err;

  if (nr_reqs > 1) {
    // INFO: get multiple cqe from system call
    /*
    err = io_uring_wait_cqe_nr(&ring_, &cqe, nr_reqs);
    if (err != 0) {
      LOG("uring_wait_cqe", err);
      io_uring_cqe_seen(&ring_, cqe);
      return cqe->res;
    }
    io_uring_cqe_seen(&ring_, cqe);
    */

    // INFO: get cqe from userspace
    for (int i = 0; i < nr_reqs; i++) {
      err = io_uring_wait_cqe(&ring_, &cqe);
      if (err != 0) {
        LOG("uring_wait_cqe", err);
        io_uring_cqe_seen(&ring_, cqe);
        return cqe->res;
      }
      io_uring_cqe_seen(&ring_, cqe);
    }
  } else {
    err = io_uring_wait_cqe(&ring_, &cqe);
    if (err != 0) {
      LOG("uring_wait_cqe", err);
    }
    io_uring_cqe_seen(&ring_, cqe);
  }

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
  int maxBlocks = 64;
  uint32_t maxTfrbytes = maxBlocks * blocksize_;  // mdts :6 (2^6) blocks

  // zero-based offset(aligned)
  off_t zOffset = (offset / blocksize_) * blocksize_;
  off_t misOffset = offset - zOffset;
  off_t lastOffset = offset + size - 1;
  int32_t left = lastOffset - zOffset + 1;
  uint32_t nRead = 0;
  int loop = 0;

  //    INFO: 너무 긴 경우(>4MB, QD16) 예외처리
  if (size > maxTfrbytes * 16) {
    return -EINVAL;
  }

  void *tempBuf;
  if (posix_memalign((void **)&tempBuf, PAGE_SIZE, maxTfrbytes * 16)) {
    LOG("[ERROR]", "MEM Align");
    return -ENOMEM;
  }

  while (left > 0) {
    loop++;
    uint32_t nCurSize = ((uint32_t)left > maxTfrbytes) ? maxTfrbytes : left;
    nCurSize = (((nCurSize - 1) / blocksize_) + 1) * blocksize_;

    // LOG(zOffset, nCurSize);
    prepUringCmd(fd, ns, op_read, zOffset, nCurSize, (char *)tempBuf + nRead,
                 loop);

    /*
    submitCommand();
    ret = waitCompleted();
    if (ret < 0) {
      LOG("ERR", ret);
    }
    */
    left -= nCurSize;
    zOffset += nCurSize;
    nRead += nCurSize;
  }
  // TODO: Batch I/O 분석 필요
  submitCommand();
  ret = waitCompleted(loop);
  if (ret < 0) {
    LOG("ERR", ret);
  }
  memcpy((char *)buf, (char *)tempBuf + misOffset, size);
  free(tempBuf);

  // std::cout << "[READ] &ring " << &ring_ << " offset " << offset << " size "
  //           << size << std::endl;
  //   TODO: 실제 읽은 block size를 전달할 지, 요청한 size를 전달할지 고민됨.
  //   return nRead;
  return size;
}

int UringCmd::uringCmdWrite(int fd, int ns, off_t offset, size_t size,
                            void *buf, uint32_t dspec) {
  const uint32_t kPlacementMode = 2;
  int ret = 0;
  int maxBlocks = 64;
  uint32_t maxTfrbytes = maxBlocks * blocksize_;  // mdts :6 (2^6) blocks

  //  zero-based offset(aligned)
  off_t zOffset = (offset / blocksize_) * blocksize_;
  off_t misOffset = offset - zOffset;
  off_t lastOffset = offset + size - 1;
  int32_t left = lastOffset - zOffset + 1;
  uint32_t nWritten = 0;
  int loop = 0;

  //   INFO: 너무 긴 경우(>4MB) 예외처리
  if (size > maxTfrbytes * 16) {
    return -EINVAL;
  }

  while (left > 0) {
    loop++;
    uint32_t nCurSize = ((uint32_t)left > maxTfrbytes) ? maxTfrbytes : left;

    if (misOffset || nCurSize < blocksize_) {
      // TODO: Mis-aligned 발생 시, data compare 필요, 맨 아래 주석 코드 활용
      return -EIO;

    } else {
      prepUringCmd(fd, ns, op_write, zOffset, nCurSize, (char *)buf + nWritten,
                   loop, kPlacementMode, dspec);
      /*
      submitCommand();
      ret = waitCompleted();
      */
    }
    if (ret < 0) {
      LOG("ERROR", ret);
      return ret;
    }

    left -= nCurSize;
    zOffset += nCurSize;
    nWritten += nCurSize - misOffset;
    // INFO: misOffset은 처음 한번만 반영
    misOffset = 0;
  }
  // TODO: Batch I/O 분석 필요
  submitCommand();
  ret = waitCompleted(loop);
  if (ret < 0) {
    LOG("ERR", ret);
  }
  // std::cout << "[WRITE] &ring " << &ring_ << " offset " << offset << " size "
  //           << size << std::endl;
  return nWritten;
}
int UringCmd::isCqOverflow() { return io_uring_cq_has_overflow(&ring_); }

int UringCmd::uringFsync(int fd, int ns) {
  struct io_uring_sqe *sqe = io_uring_get_sqe(&ring_);
  struct io_uring_cqe *cqe;
  struct nvme_uring_cmd *cmd;
  int ret;
  /* FSYNC
  io_uring_prep_fsync(sqe, fd, 0);
  */

  /* NVMe Flush */
  sqe->fd = fd;
  sqe->cmd_op = NVME_URING_CMD_IO;
  sqe->opcode = IORING_OP_URING_CMD;
  sqe->user_data = 0;

  cmd = (struct nvme_uring_cmd *)sqe->cmd;
  // NVMe Flush 명령 설정
  memset(cmd, 0, sizeof(struct nvme_uring_cmd));
  cmd->opcode = 0x00;  // NVMe FLUSH 명령어 코드
  cmd->nsid = ns;      // Namespace ID
  /* NVMe Flush END*/

  ret = io_uring_submit(&ring_);
  if (ret < 0) {
    LOG("ERROR", "io_uring_submit");
    return ret;
  }

  ret = io_uring_wait_cqe(&ring_, &cqe);
  if (ret < 0) {
    LOG("ERROR", "io_uring_wait_cqe");
    return ret;
  }
  ret = cqe->res;
  if (ret < 0) {
    LOG("ERROR", "Fail fsync");
    return ret;
  }
  io_uring_cqe_seen(&ring_, cqe);
  LOG("COMPLETED", "NVMe Flush");

  return ret;
}
