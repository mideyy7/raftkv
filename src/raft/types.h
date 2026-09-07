// Core Raft value types: roles, messages, the Ready struct, and the
// persistent-state bundle the core is constructed from.
#pragma once
#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include "raft/log_entry.h"

namespace raftkv {

using NodeId = uint64_t;

enum class Role : uint8_t { kFollower = 0, kCandidate = 1, kLeader = 2 };

const char* role_name(Role r);

enum class MsgType : uint8_t {
  kRequestVote = 1,
  kRequestVoteResp = 2,
  kAppendEntries = 3,
  kAppendEntriesResp = 4,
  kReadIndex = 5,       // phase 4: client-read leadership probe (piggybacked)
  kReadIndexResp = 6,
};

const char* msg_name(MsgType t);

// One flat message struct carries every RPC/response. Only the fields relevant
// to `type` are meaningful; the rest stay zero/empty. Flat (vs. variant) keeps
// serialization and test construction trivial.
struct Message {
  MsgType type{};
  NodeId from = 0;
  NodeId to = 0;
  uint64_t term = 0;

  // --- RequestVote ---
  uint64_t last_log_index = 0;
  uint64_t last_log_term = 0;
  // --- RequestVoteResp ---
  bool vote_granted = false;

  // --- AppendEntries ---
  uint64_t prev_log_index = 0;
  uint64_t prev_log_term = 0;
  uint64_t leader_commit = 0;
  std::vector<LogEntry> entries;
  // --- AppendEntriesResp ---
  bool success = false;
  uint64_t conflict_term = 0;   // phase 3: fast-backup hint
  uint64_t conflict_index = 0;  // phase 3
  uint64_t match_index = 0;     // phase 3: follower's last index on success

  // --- ReadIndex / ReadIndexResp (phase 4) ---
  uint64_t read_ctx = 0;
};

// What the driver must do after a step()/tick(), in this order:
//   1. if hard_state -> persist (fsync) it
//   2. if entries    -> if entries[0].index <= log.last_index(): truncate_suffix
//                        first; then append_batch; fsync
//   3. send every message in `messages`
//   4. apply every entry in `committed` to the state machine, in order
//   5. resolve every ctx in `read_states` (phase 4) -- reads now safe to serve
// then call advance(ready).
// A ReadIndex request that has been confirmed safe to serve: the read may
// proceed once the state machine has applied through `index`. index == 0 means
// leadership could not be confirmed -> the driver should tell the client RETRY.
struct ReadState {
  uint64_t ctx = 0;
  uint64_t index = 0;
};

struct Ready {
  std::optional<HardState> hard_state;
  std::vector<LogEntry> entries;
  std::vector<Message> messages;
  std::vector<LogEntry> committed;
  std::vector<ReadState> read_states;

  bool empty() const {
    return !hard_state && entries.empty() && messages.empty() &&
           committed.empty() && read_states.empty();
  }
};

// The state a node loads from disk at startup (or defaults for a fresh node).
struct PersistentState {
  HardState hard_state;
  std::vector<LogEntry> entries;  // contiguous, index 1..N
};

struct RaftConfig {
  NodeId id = 0;
  std::vector<NodeId> peers;      // ALL members, including self
  uint32_t election_timeout = 10; // base, in ticks; actual in [T, 2T)
  uint32_t heartbeat_timeout = 2; // in ticks; must be < election_timeout
  uint64_t seed = 0;              // 0 -> derive from id (deterministic)

  size_t quorum() const { return peers.size() / 2 + 1; }
};

}  // namespace raftkv
