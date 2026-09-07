// Reusable Raft safety-invariant checks, run after every step by the
// integration and fuzz harnesses. Each returns "" on success or a human
// description of the first violation found.
//
// These operate on a lightweight view of each node so the checker does not
// depend on RaftCore internals.
#pragma once
#include <cstdint>
#include <map>
#include <string>
#include <vector>

#include "raft/log_entry.h"
#include "raft/types.h"

namespace raftkv::invariants {

struct NodeView {
  NodeId id = 0;
  bool alive = false;
  Role role = Role::kFollower;
  uint64_t term = 0;
  uint64_t commit_index = 0;
  std::vector<LogEntry> log;   // index 1..N
  std::vector<LogEntry> applied;  // apply history in order
};

// 1. Election Safety: at most one leader per term across all nodes.
inline std::string election_safety(const std::vector<NodeView>& nodes) {
  std::map<uint64_t, NodeId> leader_of_term;
  for (const auto& n : nodes) {
    if (!n.alive || n.role != Role::kLeader) continue;
    auto it = leader_of_term.find(n.term);
    if (it != leader_of_term.end() && it->second != n.id)
      return "election safety: nodes " + std::to_string(it->second) + " and " +
             std::to_string(n.id) + " both leader in term " +
             std::to_string(n.term);
    leader_of_term[n.term] = n.id;
  }
  return "";
}

// 2. Log Matching: if two logs contain an entry with the same index and term,
//    the logs are identical in all entries up through that index.
inline std::string log_matching(const std::vector<NodeView>& nodes) {
  for (size_t a = 0; a < nodes.size(); ++a) {
    for (size_t b = a + 1; b < nodes.size(); ++b) {
      const auto& la = nodes[a].log;
      const auto& lb = nodes[b].log;
      size_t common = std::min(la.size(), lb.size());
      for (size_t i = 0; i < common; ++i) {
        if (la[i].index != lb[i].index)
          return "log matching: index mismatch at slot " + std::to_string(i);
        if (la[i].term == lb[i].term) {
          if (!(la[i] == lb[i]))
            return "log matching: entry " + std::to_string(la[i].index) +
                   " same term " + std::to_string(la[i].term) +
                   " but differing data on nodes " +
                   std::to_string(nodes[a].id) + "/" + std::to_string(nodes[b].id);
          // and everything before must match too
          for (size_t j = 0; j < i; ++j)
            if (!(la[j] == lb[j]))
              return "log matching: prefix diverges before matched entry " +
                     std::to_string(la[i].index);
        }
      }
    }
  }
  return "";
}

// 3. State Machine Safety: no two nodes apply a different command at the same
//    log index.
inline std::string state_machine_safety(const std::vector<NodeView>& nodes) {
  std::map<uint64_t, LogEntry> applied_at;
  for (const auto& n : nodes) {
    for (const auto& e : n.applied) {
      auto it = applied_at.find(e.index);
      if (it == applied_at.end()) {
        applied_at[e.index] = e;
      } else if (!(it->second == e)) {
        return "state machine safety: index " + std::to_string(e.index) +
               " applied with different contents across nodes";
      }
    }
  }
  return "";
}

// 4. Leader Completeness / commit durability: if two nodes both consider an
//    entry committed (index <= their commit_index), the entries are identical.
//    An *uncommitted* divergent tail on some node is legal -- it gets
//    truncated when that node next hears from the leader -- so we compare only
//    where BOTH nodes have committed the index.
inline std::string leader_completeness(const std::vector<NodeView>& nodes) {
  std::map<uint64_t, std::pair<NodeId, LogEntry>> committed;  // idx -> (who, e)
  for (const auto& n : nodes) {
    if (!n.alive) continue;
    for (const auto& e : n.log) {
      if (e.index > n.commit_index) break;
      auto it = committed.find(e.index);
      if (it == committed.end()) {
        committed[e.index] = {n.id, e};
      } else if (!(it->second.second == e)) {
        return "commit durability: nodes " + std::to_string(it->second.first) +
               " and " + std::to_string(n.id) +
               " committed different entries at index " + std::to_string(e.index);
      }
    }
  }
  return "";
}

// 5. Commit index never exceeds the log length on any node.
inline std::string commit_bounds(const std::vector<NodeView>& nodes) {
  for (const auto& n : nodes) {
    if (n.commit_index > n.log.size())
      return "commit bounds: node " + std::to_string(n.id) + " commit=" +
             std::to_string(n.commit_index) + " > log len " +
             std::to_string(n.log.size());
  }
  return "";
}

inline std::string check_all(const std::vector<NodeView>& nodes) {
  for (auto fn : {election_safety, log_matching, state_machine_safety,
                  leader_completeness, commit_bounds}) {
    std::string e = fn(nodes);
    if (!e.empty()) return e;
  }
  return "";
}

}  // namespace raftkv::invariants
