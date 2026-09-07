// In-memory key/value table -- the replicated state machine. Committed log
// entries carrying an encoded Command are apply()'d here in index order.
#pragma once
#include <cstdint>
#include <optional>
#include <string>
#include <unordered_map>

namespace raftkv {

enum class CmdOp : uint8_t { kPut = 0, kDelete = 1 };

struct Command {
  CmdOp op = CmdOp::kPut;
  std::string key;
  std::string value;  // unused for kDelete

  std::string encode() const;
  static Command decode(std::string_view bytes);
};

class KvTable {
 public:
  // Apply a command; returns the value string that a client waiter should see
  // (the stored value for a PUT, the previous value for a DELETE, "" if absent).
  std::string apply(const Command& c);

  std::optional<std::string> get(const std::string& key) const;
  bool contains(const std::string& key) const { return map_.count(key) > 0; }
  size_t size() const { return map_.size(); }
  const std::unordered_map<std::string, std::string>& snapshot_map() const {
    return map_;
  }

 private:
  std::unordered_map<std::string, std::string> map_;
};

}  // namespace raftkv
