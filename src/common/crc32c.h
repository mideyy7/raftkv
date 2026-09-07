// CRC-32C (Castagnoli, polynomial 0x1EDC6F41 / reversed 0x82F63B78).
// Used to detect torn / corrupt WAL records on replay.
#pragma once
#include <cstddef>
#include <cstdint>
#include <string_view>

namespace raftkv {

// Incremental: pass the previous result back as `crc` (start with 0).
uint32_t crc32c(uint32_t crc, const void* data, size_t len);

inline uint32_t crc32c(std::string_view s) {
  return crc32c(0, s.data(), s.size());
}

}  // namespace raftkv
