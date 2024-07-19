#pragma once

#include <fcntl.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <unistd.h>

#include <atomic>
#include <cstdint>  // for uint16_t, uint32_t
#include <cstring>
#include <iomanip>
#include <iostream>
#include <limits>
#include <mutex>
#include <random>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>
#define D_LOG
// #define D_DBG
#ifdef D_LOG
#define LOG(x, y)                                                             \
  std::cout << "[LOG] " << __FILE__ << "(" << __LINE__ << ") : " << x << "= " \
            << y << "\n"
#else
#define LOG(x, y)
#endif

#ifdef D_DBG
#define DBG(x, y)                                                             \
  std::cout << "[DBG] " << __FILE__ << "(" << __LINE__ << ") : " << x << "= " \
            << y << "\n"
#else
#define DBG(x, y)
#endif

int roundup_pow2(unsigned int depth);
uint16_t strToU16(const std::string &str);
uint32_t strToU32(const std::string &str);
uint64_t strToU64(const std::string &str);

int32_t strToI32(const std::string &str);

template <typename T>
constexpr T constexpr_log2_(T a, T e) {
  return e == T(1) ? a : constexpr_log2_(a + T(1), e / T(2));
}

template <typename T>
constexpr T constexpr_log2_ceil_(T l2, T t) {
  return l2 + T(T(1) << l2 < t ? 1 : 0);
}

template <typename T>
constexpr T constexpr_log2(T t) {
  return constexpr_log2_(T(0), t);
}

template <typename T>
constexpr T constexpr_log2_ceil(T t) {
  return constexpr_log2_ceil_(constexpr_log2(t), t);
}

class RangeLock {
 public:
  RangeLock(size_t size) : locks(size) {}

  void lock(size_t start, size_t end);
  void unlock(size_t start, size_t end);

 private:
  std::vector<std::mutex> locks;
};
