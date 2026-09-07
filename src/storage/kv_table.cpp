#include "storage/kv_table.h"

#include "common/codec.h"

namespace raftkv {

std::string Command::encode() const {
  Buffer b;
  b.u8(static_cast<uint8_t>(op));
  b.str(key);
  b.str(value);
  return b.take();
}

Command Command::decode(std::string_view bytes) {
  Reader r(bytes);
  Command c;
  c.op = static_cast<CmdOp>(r.u8());
  c.key = r.str();
  c.value = r.str();
  return c;
}

std::string KvTable::apply(const Command& c) {
  switch (c.op) {
    case CmdOp::kPut:
      map_[c.key] = c.value;
      return c.value;
    case CmdOp::kDelete: {
      auto it = map_.find(c.key);
      std::string prev = (it == map_.end()) ? std::string() : it->second;
      if (it != map_.end()) map_.erase(it);
      return prev;
    }
  }
  return {};
}

std::optional<std::string> KvTable::get(const std::string& key) const {
  auto it = map_.find(key);
  if (it == map_.end()) return std::nullopt;
  return it->second;
}

}  // namespace raftkv
