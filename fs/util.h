#pragma once

#include <fcntl.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cstdint>  // for uint16_t, uint32_t
#include <cstring>
#include <iostream>
#include <limits>
#include <random>
#include <sstream>
#include <stdexcept>
#include <string>

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
