// LogStore: a typed, replayable Raft log on top of the WAL.
//
// The WAL holds three kinds of records (first payload byte = kind):
//   kEntry(1)     : term u64, index u64, type u8, data bytes
//   kHardState(2) : current_term u64, voted_for u64, commit_index u64
//   kTruncate(3)  : from_index u64      (logical suffix deletion)
//
// Physical rewrite of the file only happens on compaction (stretch). Suffix
// truncation is a marker record honoured on replay, so the log stays strictly
// append-only on disk -- mirroring Raft's "a leader only appends" discipline.
#pragma once
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "common/result.h"
#include "raft/log_entry.h"
#include "storage/wal.h"

namespace raftkv {

class LogStore {
 public:
  static Result<std::unique_ptr<LogStore>> open(
      const std::string& path, SyncMode mode = SyncMode::kFull);

  // --- reads ---
  uint64_t first_index() const { return first_index_; }
  uint64_t last_index() const {
    return first_index_ - 1 + static_cast<uint64_t>(entries_.size());
  }
  bool empty() const { return entries_.empty(); }

  // term of the entry at `index`; 0 if index is out of the live range.
  uint64_t term_at(uint64_t index) const;

  // entries in [lo, hi). Clamped to the live range.
  std::vector<LogEntry> entries(uint64_t lo, uint64_t hi) const;

  // precondition: first_index() <= index <= last_index()
  const LogEntry& at(uint64_t index) const;

  HardState hard_state() const { return hard_; }

  // --- writes (append a record; caller calls sync() to make durable) ---

  // `e.index` must equal last_index()+1.
  Status append(const LogEntry& e);
  Status append_batch(const std::vector<LogEntry>& es);

  // Drop every entry with index >= from_index. No-op if from_index > last.
  Status truncate_suffix(uint64_t from_index);

  Status set_hard_state(const HardState& hs);

  Status sync() { return wal_->sync(); }
  uint64_t size_bytes() const { return wal_->size_bytes(); }

 private:
  LogStore() = default;
  void apply_record(const std::string& payload);

  std::unique_ptr<Wal> wal_;
  std::vector<LogEntry> entries_;  // entries_[i].index == first_index_ + i
  HardState hard_;
  uint64_t first_index_ = 1;  // no snapshots in phases 1-4
};

}  // namespace raftkv
