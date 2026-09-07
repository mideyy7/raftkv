// checker.h -- a small brute-force linearizability checker for KV histories
// (Wing & Gong style). Exponential in the number of concurrent ops, so keep
// histories to a few dozen ops; we exploit that ops on different keys are
// independent and check each key's projection separately.
//
// A history entry is one completed client operation with its real-time
// invoke/response instants and (for reads) the value observed.
#pragma once
#include <algorithm>
#include <cstdint>
#include <functional>
#include <map>
#include <optional>
#include <string>
#include <vector>

namespace raftkv::lin {

enum class OpKind { kPut, kGet, kDel };

struct Event {
  OpKind kind;
  std::string key;
  std::string value;              // put: written value; get: value observed
  bool observed_present = true;   // get: was the key present?
  uint64_t invoke = 0;            // real-time, monotonic
  uint64_t response = 0;
};

namespace detail {

// Can this KV register history (single key) be linearized?
inline bool linearizable_key(std::vector<Event> h) {
  std::sort(h.begin(), h.end(),
            [](const Event& a, const Event& b) { return a.invoke < b.invoke; });
  const size_t n = h.size();
  std::vector<char> used(n, 0);

  struct State {
    std::optional<std::string> val;  // nullopt == absent
  };

  // recursive search; `now` = number linearized so far, `st` = model state
  std::function<bool(size_t, State)> go = [&](size_t done, State st) -> bool {
    if (done == n) return true;
    // earliest response among not-yet-linearized ops: any op whose invoke is
    // after that time cannot go next (would violate real-time order)
    uint64_t min_resp = UINT64_MAX;
    for (size_t i = 0; i < n; ++i)
      if (!used[i]) min_resp = std::min(min_resp, h[i].response);

    for (size_t i = 0; i < n; ++i) {
      if (used[i]) continue;
      if (h[i].invoke > min_resp) continue;  // not a legal next choice

      State ns = st;
      bool ok = true;
      switch (h[i].kind) {
        case OpKind::kPut:
          ns.val = h[i].value;
          break;
        case OpKind::kDel:
          ns.val = std::nullopt;
          break;
        case OpKind::kGet:
          if (h[i].observed_present)
            ok = st.val.has_value() && *st.val == h[i].value;
          else
            ok = !st.val.has_value();
          break;
      }
      if (!ok) continue;
      used[i] = 1;
      if (go(done + 1, ns)) return true;
      used[i] = 0;
    }
    return false;
  };

  return go(0, State{});
}

}  // namespace detail

// Returns "" if the whole history is linearizable, else a description.
inline std::string check(const std::vector<Event>& history) {
  std::map<std::string, std::vector<Event>> by_key;
  for (const auto& e : history) by_key[e.key].push_back(e);
  for (auto& [key, evs] : by_key) {
    if (!detail::linearizable_key(evs))
      return "history not linearizable on key '" + key + "' (" +
             std::to_string(evs.size()) + " ops)";
  }
  return "";
}

}  // namespace raftkv::lin
