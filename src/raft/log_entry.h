// POD types shared by the storage layer and the Raft core: a single log entry
// and the persistent "hard state". Header-only, no dependencies, so storage can
// include it without depending on the rest of raft/.
#pragma once
#include <cstdint>
#include <string>

namespace raftkv {

enum class EntryType : uint8_t {
  kNormal = 0,      // a client command (opaque bytes, interpreted by the KV SM)
  kNoOp = 1,        // leader's no-op appended on election (commit-safety)
  kConfChange = 2,  // membership change (stretch goal)
};

struct LogEntry {
  uint64_t term = 0;
  uint64_t index = 0;  // 1-based; 0 is the sentinel "before the log"
  EntryType type = EntryType::kNormal;
  std::string data;

  bool operator==(const LogEntry& o) const {
    return term == o.term && index == o.index && type == o.type &&
           data == o.data;
  }
};

// The subset of state Raft must fsync before responding to RPCs.
// voted_for == 0 means "no vote this term" (valid node ids are >= 1).
struct HardState {
  uint64_t current_term = 0;
  uint64_t voted_for = 0;
  uint64_t commit_index = 0;

  bool operator==(const HardState& o) const {
    return current_term == o.current_term && voted_for == o.voted_for &&
           commit_index == o.commit_index;
  }
};

}  // namespace raftkv
