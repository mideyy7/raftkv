#include "common/crc32c.h"

#include <string>

#include "tinytest.h"

using namespace raftkv;

TEST(crc32c, known_vectors) {
  CHECK_EQ(crc32c(std::string_view("")), 0u);
  // The canonical CRC-32C check value for "123456789".
  CHECK_EQ(crc32c(std::string_view("123456789")), 0xE3069283u);
}

TEST(crc32c, incremental_equals_oneshot) {
  std::string data = "the quick brown fox jumps over the lazy dog";
  uint32_t one = crc32c(0, data.data(), data.size());

  uint32_t inc = 0;
  size_t mid = data.size() / 3;
  inc = crc32c(inc, data.data(), mid);
  inc = crc32c(inc, data.data() + mid, data.size() - mid);
  CHECK_EQ(one, inc);
}

TEST(crc32c, single_bit_flip_changes_crc) {
  std::string a(64, 'x');
  std::string b = a;
  b[30] ^= 0x01;
  CHECK_NE(crc32c(a), crc32c(b));
}

TINYTEST_MAIN()
