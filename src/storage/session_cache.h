// SessionCache: per-client dedup for exactly-once client writes.
//
// Raft delivers a proposal at-least-once (clients retry on timeout / leader
// change). The state machine records the last applied seq per client and
// replays the cached result for any seq <= that, so a retried PUT never applies
// twice. Updated by the apply path as part of applying a mutating command.
#pragma once
#include <cstdint>
#include <optional>
#include <string>
#include <unordered_map>

namespace raftkv {

class SessionCache {
 public:
  struct Entry {
    uint64_t last_seq = 0;
    std::string last_value;
    bool last_found = false;
  };

  // If this (client_id, seq) was already applied, returns the cached result.
  std::optional<Entry> lookup(uint64_t client_id, uint64_t seq) const {
    auto it = map_.find(client_id);
    if (it == map_.end()) return std::nullopt;
    if (seq <= it->second.last_seq) return it->second;
    return std::nullopt;
  }

  // Record the result of applying (client_id, seq). seq must be > last_seq.
  void record(uint64_t client_id, uint64_t seq, std::string value, bool found) {
    auto& e = map_[client_id];
    if (seq <= e.last_seq) return;  // stale / duplicate apply -- keep newest
    e.last_seq = seq;
    e.last_value = std::move(value);
    e.last_found = found;
  }

  size_t clients() const { return map_.size(); }

 private:
  std::unordered_map<uint64_t, Entry> map_;
};

}  // namespace raftkv
