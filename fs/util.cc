#include "util.h"

// roundup_pow2 함수 정의
int roundup_pow2(unsigned int depth) {
  if (depth == 0)
    return 1; // 0일 경우 1을 반환

  depth--; // depth보다 큰 가장 작은 2의 거듭제곱을 찾기 위해 depth를 하나 감소
  depth |= depth >> 1;
  depth |= depth >> 2;
  depth |= depth >> 4;
  depth |= depth >> 8;
  depth |= depth >> 16;
#if (UINT_MAX == 0xFFFFFFFFFFFFFFFF)
  depth |= depth >> 32; // 64비트 시스템에서 추가로 필요한 연산
#endif
  return (int)depth + 1;
}

uint64_t strToU64(const std::string &str) {
  try {
    unsigned long long ull = std::stoull(str);
    if (ull > std::numeric_limits<uint64_t>::max()) {
      throw std::out_of_range("Value out of range for uint64_t");
    }
    return static_cast<uint64_t>(ull);
  } catch (const std::invalid_argument &e) {
    std::cerr << "Invalid argument: " << e.what() << '\n';
    throw;
  } catch (const std::out_of_range &e) {
    std::cerr << "Out of range: " << e.what() << '\n';
    throw;
  }
}

uint16_t strToU16(const std::string &str) {
  try {
    unsigned long ul = std::stoul(str);
    if (ul > std::numeric_limits<uint16_t>::max()) {
      throw std::out_of_range("Value out of range for uint16_t");
    }
    return static_cast<uint16_t>(ul);
  } catch (const std::invalid_argument &e) {
    std::cerr << "Invalid argument: " << e.what() << '\n';
    throw;
  } catch (const std::out_of_range &e) {
    std::cerr << "Out of range: " << e.what() << '\n';
    throw;
  }
}

uint32_t strToU32(const std::string &str) {
  try {
    unsigned long ul = std::stoul(str);
    if (ul > std::numeric_limits<uint32_t>::max()) {
      throw std::out_of_range("Value out of range for uint32_t");
    }
    return static_cast<uint32_t>(ul);
  } catch (const std::invalid_argument &e) {
    std::cerr << "Invalid argument: " << e.what() << '\n';
    throw;
  } catch (const std::out_of_range &e) {
    std::cerr << "Out of range: " << e.what() << '\n';
    throw;
  }
}

int32_t strToI32(const std::string &str) {
  try {
    long val = std::stol(str);
    if (val < std::numeric_limits<int32_t>::min() ||
        val > std::numeric_limits<int32_t>::max()) {
      throw std::out_of_range("Value out of range for int32_t");
    }
    return static_cast<int32_t>(val);
  } catch (const std::invalid_argument &e) {
    std::cerr << "Invalid argument: " << e.what() << '\n';
    throw;
  } catch (const std::out_of_range &e) {
    std::cerr << "Out of range: " << e.what() << '\n';
    throw;
  }
}

void RangeLock::lock(size_t start, size_t end) {
  for (size_t i = start; i <= end; ++i) {
    locks[i].lock();
  }
}

void RangeLock::unlock(size_t start, size_t end) {
  for (size_t i = start; i <= end; ++i) {
    locks[i].unlock();
  }
}
