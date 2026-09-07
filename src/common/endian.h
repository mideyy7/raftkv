// Little-endian fixed-width read/write helpers. All on-disk and on-wire
// integers in RaftKV are stored little-endian regardless of host byte order.
#pragma once
#include <cstdint>
#include <cstring>
#include <string>

namespace raftkv {

inline void put_u8(std::string& out, uint8_t v) {
  out.push_back(static_cast<char>(v));
}

inline void put_u16(std::string& out, uint16_t v) {
  char b[2] = {char(v & 0xff), char((v >> 8) & 0xff)};
  out.append(b, 2);
}

inline void put_u32(std::string& out, uint32_t v) {
  char b[4] = {char(v & 0xff), char((v >> 8) & 0xff), char((v >> 16) & 0xff),
               char((v >> 24) & 0xff)};
  out.append(b, 4);
}

inline void put_u64(std::string& out, uint64_t v) {
  char b[8];
  for (int i = 0; i < 8; ++i) b[i] = char((v >> (8 * i)) & 0xff);
  out.append(b, 8);
}

inline uint8_t get_u8(const char* p) { return static_cast<uint8_t>(p[0]); }

inline uint16_t get_u16(const char* p) {
  return uint16_t(uint8_t(p[0])) | (uint16_t(uint8_t(p[1])) << 8);
}

inline uint32_t get_u32(const char* p) {
  return uint32_t(uint8_t(p[0])) | (uint32_t(uint8_t(p[1])) << 8) |
         (uint32_t(uint8_t(p[2])) << 16) | (uint32_t(uint8_t(p[3])) << 24);
}

inline uint64_t get_u64(const char* p) {
  uint64_t v = 0;
  for (int i = 0; i < 8; ++i) v |= uint64_t(uint8_t(p[i])) << (8 * i);
  return v;
}

}  // namespace raftkv
