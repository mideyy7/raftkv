// Write-ahead log: an append-only file of length+CRC framed records.
//
// Frame:  [u32 payload_len][u32 crc32c(payload)][payload bytes]
//
// On open(), records are replayed in order. Replay stops at the first
// malformed frame (short header, absurd length, short payload, or CRC
// mismatch) -- this is a torn tail write from a crash mid-append -- and the
// file is physically truncated back to the last intact frame. Everything
// before the torn frame is returned intact.
#pragma once
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "common/result.h"

namespace raftkv {

// Durability of sync():
//   kFull -> F_FULLFSYNC on macOS: flushes the drive's write cache. The only
//            mode that truly survives a power cut. Slow (ms per call).
//   kOs   -> fsync(): data leaves the OS page cache but may sit in the drive
//            cache. Survives a process crash / kill -9, not a power cut. Fast.
//   kNone -> no-op. Tests that only exercise replay/framing logic.
enum class SyncMode { kFull, kOs, kNone };

class Wal {
 public:
  // Hard cap on a single record; frames claiming more are treated as corrupt.
  static constexpr uint32_t kMaxRecord = 64u * 1024u * 1024u;

  ~Wal();
  Wal(const Wal&) = delete;
  Wal& operator=(const Wal&) = delete;

  // Opens (creating if absent), replays, truncates any torn tail.
  static Result<std::unique_ptr<Wal>> open(const std::string& path,
                                           SyncMode mode = SyncMode::kFull);

  // Intact payloads recovered at open(), in append order.
  const std::vector<std::string>& records() const { return records_; }

  // Appends one record. Not durable until sync() succeeds.
  Status append(std::string_view payload);

  // Flushes userspace + OS buffers to stable storage (F_FULLFSYNC on macOS).
  Status sync();

  uint64_t size_bytes() const { return size_; }
  const std::string& path() const { return path_; }

  // Atomically replace the file's contents with `recs` (used by compaction).
  static Status rewrite(const std::string& path,
                        const std::vector<std::string>& recs);

  static std::string frame(std::string_view payload);

 private:
  Wal() = default;
  int fd_ = -1;
  std::string path_;
  uint64_t size_ = 0;
  SyncMode mode_ = SyncMode::kFull;
  std::vector<std::string> records_;
};

}  // namespace raftkv
