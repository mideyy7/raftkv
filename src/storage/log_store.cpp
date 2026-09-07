#include "storage/log_store.h"

#include <cassert>

#include "common/codec.h"

namespace raftkv {
namespace {
constexpr uint8_t kEntry = 1;
constexpr uint8_t kHardState = 2;
constexpr uint8_t kTruncate = 3;

std::string encode_entry(const LogEntry& e) {
  Buffer b;
  b.u8(kEntry);
  b.u64(e.term);
  b.u64(e.index);
  b.u8(static_cast<uint8_t>(e.type));
  b.bytes(e.data);
  return b.take();
}

std::string encode_hard(const HardState& h) {
  Buffer b;
  b.u8(kHardState);
  b.u64(h.current_term);
  b.u64(h.voted_for);
  b.u64(h.commit_index);
  return b.take();
}

std::string encode_truncate(uint64_t from_index) {
  Buffer b;
  b.u8(kTruncate);
  b.u64(from_index);
  return b.take();
}
}  // namespace

void LogStore::apply_record(const std::string& payload) {
  Reader r(payload);
  uint8_t kind = r.u8();
  switch (kind) {
    case kEntry: {
      LogEntry e;
      e.term = r.u64();
      e.index = r.u64();
      e.type = static_cast<EntryType>(r.u8());
      e.data = r.bytes();
      uint64_t last = last_index();
      if (e.index == last + 1) {
        entries_.push_back(std::move(e));
      } else if (e.index >= first_index_ && e.index <= last) {
        // rewrite of an existing suffix (post-conflict re-append)
        entries_.resize(static_cast<size_t>(e.index - first_index_));
        entries_.push_back(std::move(e));
      }
      // else: gap -> ignore (should not occur with a well-formed WAL)
      break;
    }
    case kHardState: {
      hard_.current_term = r.u64();
      hard_.voted_for = r.u64();
      hard_.commit_index = r.u64();
      break;
    }
    case kTruncate: {
      uint64_t from = r.u64();
      if (from <= first_index_) {
        entries_.clear();
      } else if (from <= last_index()) {
        entries_.resize(static_cast<size_t>(from - first_index_));
      }
      break;
    }
    default:
      break;  // unknown record kind -> skip
  }
}

Result<std::unique_ptr<LogStore>> LogStore::open(const std::string& path,
                                                SyncMode mode) {
  auto wr = Wal::open(path, mode);
  if (!wr) return wr.status();

  std::unique_ptr<LogStore> ls(new LogStore());
  ls->wal_ = std::move(wr.value());
  for (const auto& rec : ls->wal_->records()) ls->apply_record(rec);
  return Result<std::unique_ptr<LogStore>>(std::move(ls));
}

uint64_t LogStore::term_at(uint64_t index) const {
  if (index < first_index_ || index > last_index()) return 0;
  return entries_[static_cast<size_t>(index - first_index_)].term;
}

std::vector<LogEntry> LogStore::entries(uint64_t lo, uint64_t hi) const {
  std::vector<LogEntry> out;
  if (hi <= lo) return out;
  lo = std::max(lo, first_index_);
  hi = std::min(hi, last_index() + 1);
  for (uint64_t i = lo; i < hi; ++i)
    out.push_back(entries_[static_cast<size_t>(i - first_index_)]);
  return out;
}

const LogEntry& LogStore::at(uint64_t index) const {
  assert(index >= first_index_ && index <= last_index());
  return entries_[static_cast<size_t>(index - first_index_)];
}

Status LogStore::append(const LogEntry& e) {
  if (e.index != last_index() + 1)
    return Status::error("LogStore::append: non-contiguous index");
  Status s = wal_->append(encode_entry(e));
  if (!s) return s;
  entries_.push_back(e);
  return Status::ok();
}

Status LogStore::append_batch(const std::vector<LogEntry>& es) {
  for (const auto& e : es) {
    Status s = append(e);
    if (!s) return s;
  }
  return Status::ok();
}

Status LogStore::truncate_suffix(uint64_t from_index) {
  if (from_index > last_index()) return Status::ok();
  Status s = wal_->append(encode_truncate(from_index));
  if (!s) return s;
  if (from_index <= first_index_)
    entries_.clear();
  else
    entries_.resize(static_cast<size_t>(from_index - first_index_));
  return Status::ok();
}

Status LogStore::set_hard_state(const HardState& hs) {
  Status s = wal_->append(encode_hard(hs));
  if (!s) return s;
  hard_ = hs;
  return Status::ok();
}

}  // namespace raftkv
