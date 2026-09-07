// StoreEngine: the Phase-1 single-node durable store. Ties a LogStore (WAL on
// disk) to a KvTable (in-memory). A write is appended + fsync'd to the log
// *before* execute() returns -- so an acknowledged write survives an immediate
// crash. On open() the log is replayed into the table.
//
// No consensus here: every entry uses term 1 and is considered committed the
// moment it is durable. Phases 2-3 replace this glue with the Raft node driver.
#pragma once
#include <memory>
#include <optional>
#include <string>

#include "common/result.h"
#include "storage/kv_table.h"
#include "storage/log_store.h"

namespace raftkv {

class StoreEngine {
 public:
  static Result<std::unique_ptr<StoreEngine>> open(
      const std::string& wal_path, SyncMode mode = SyncMode::kFull);

  // Append+fsync to the log, then apply. Returns the value a caller should see.
  Result<std::string> execute(const Command& c);

  std::optional<std::string> get(const std::string& key) const {
    return table_.get(key);
  }
  const KvTable& table() const { return table_; }
  size_t key_count() const { return table_.size(); }
  uint64_t last_index() const { return log_->last_index(); }
  uint64_t log_bytes() const { return log_->size_bytes(); }

 private:
  StoreEngine() = default;
  std::unique_ptr<LogStore> log_;
  KvTable table_;
};

}  // namespace raftkv
