#include "common/wire.h"

#include "common/codec.h"

namespace raftkv {

static void put_entry(Buffer& b, const LogEntry& e) {
  b.u64(e.term);
  b.u64(e.index);
  b.u8(static_cast<uint8_t>(e.type));
  b.bytes(e.data);
}

static LogEntry get_entry(Reader& r) {
  LogEntry e;
  e.term = r.u64();
  e.index = r.u64();
  e.type = static_cast<EntryType>(r.u8());
  e.data = r.bytes();
  return e;
}

std::string encode_message(const Message& m) {
  Buffer b;
  b.u8(static_cast<uint8_t>(m.type));
  b.u64(m.from);
  b.u64(m.to);
  b.u64(m.term);
  b.u64(m.last_log_index);
  b.u64(m.last_log_term);
  b.boolean(m.vote_granted);
  b.u64(m.prev_log_index);
  b.u64(m.prev_log_term);
  b.u64(m.leader_commit);
  b.boolean(m.success);
  b.u64(m.conflict_term);
  b.u64(m.conflict_index);
  b.u64(m.match_index);
  b.u64(m.read_ctx);
  b.u64(m.hb_round);
  b.u32(static_cast<uint32_t>(m.entries.size()));
  for (const auto& e : m.entries) put_entry(b, e);
  return b.take();
}

Message decode_message(std::string_view payload) {
  Reader r(payload);
  Message m;
  m.type = static_cast<MsgType>(r.u8());
  m.from = r.u64();
  m.to = r.u64();
  m.term = r.u64();
  m.last_log_index = r.u64();
  m.last_log_term = r.u64();
  m.vote_granted = r.boolean();
  m.prev_log_index = r.u64();
  m.prev_log_term = r.u64();
  m.leader_commit = r.u64();
  m.success = r.boolean();
  m.conflict_term = r.u64();
  m.conflict_index = r.u64();
  m.match_index = r.u64();
  m.read_ctx = r.u64();
  m.hb_round = r.u64();
  uint32_t n = r.u32();
  m.entries.reserve(n);
  for (uint32_t i = 0; i < n; ++i) m.entries.push_back(get_entry(r));
  return m;
}

}  // namespace raftkv
