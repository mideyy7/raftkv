#include "storage/store_engine.h"

namespace raftkv {

Result<std::unique_ptr<StoreEngine>> StoreEngine::open(
    const std::string& wal_path, SyncMode mode) {
  auto lr = LogStore::open(wal_path, mode);
  if (!lr) return lr.status();

  std::unique_ptr<StoreEngine> e(new StoreEngine());
  e->log_ = std::move(lr.value());

  // Replay: every persisted entry is durable == committed for a single node.
  uint64_t lo = e->log_->first_index();
  uint64_t hi = e->log_->last_index() + 1;
  for (const auto& ent : e->log_->entries(lo, hi)) {
    if (ent.type == EntryType::kNormal)
      e->table_.apply(Command::decode(ent.data));
  }
  return Result<std::unique_ptr<StoreEngine>>(std::move(e));
}

Result<std::string> StoreEngine::execute(const Command& c) {
  LogEntry e;
  e.term = 1;
  e.index = log_->last_index() + 1;
  e.type = EntryType::kNormal;
  e.data = c.encode();

  if (Status s = log_->append(e); !s) return s;
  if (Status s = log_->sync(); !s) return s;  // durable before we ack

  return Result<std::string>(table_.apply(c));
}

}  // namespace raftkv
