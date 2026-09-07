#include "common/crc32c.h"

#include <array>

namespace raftkv {
namespace {

std::array<uint32_t, 256> make_table() {
  constexpr uint32_t kPoly = 0x82F63B78u;  // reversed 0x1EDC6F41
  std::array<uint32_t, 256> t{};
  for (uint32_t i = 0; i < 256; ++i) {
    uint32_t c = i;
    for (int k = 0; k < 8; ++k) c = (c & 1u) ? (kPoly ^ (c >> 1)) : (c >> 1);
    t[i] = c;
  }
  return t;
}

const std::array<uint32_t, 256>& table() {
  static const std::array<uint32_t, 256> t = make_table();
  return t;
}

}  // namespace

uint32_t crc32c(uint32_t crc, const void* data, size_t len) {
  const auto& t = table();
  const auto* p = static_cast<const uint8_t*>(data);
  crc = ~crc;
  for (size_t i = 0; i < len; ++i) {
    crc = t[(crc ^ p[i]) & 0xffu] ^ (crc >> 8);
  }
  return ~crc;
}

}  // namespace raftkv
