#include "util.h"

// roundup_pow2 함수 정의
int roundup_pow2(unsigned int depth) {
  if (depth == 0) return 1;  // 0일 경우 1을 반환

  depth--;  // depth보다 큰 가장 작은 2의 거듭제곱을 찾기 위해 depth를 하나 감소
  depth |= depth >> 1;
  depth |= depth >> 2;
  depth |= depth >> 4;
  depth |= depth >> 8;
  depth |= depth >> 16;
#if (UINT_MAX == 0xFFFFFFFFFFFFFFFF)
  depth |= depth >> 32;  // 64비트 시스템에서 추가로 필요한 연산
#endif
  return (int)depth + 1;
}
