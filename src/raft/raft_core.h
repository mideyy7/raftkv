// RaftCore -- the deterministic, single-threaded, clock-free consensus state
// machine. It owns the full in-memory log. It performs NO I/O and reads NO
// clock: the driver calls tick() on a timer and step() with inbound messages,
// then drains ready() (persist -> send -> apply) and calls advance().
//
// Phase 2 scope: leader election only (RequestVote + empty-AppendEntries
// heartbeats + term rules). Phase 3 adds real log replication.
#pragma once
#include <cstdint>
#include <random>
#include <vector>

#include "raft/types.h"

namespace raftkv {

class RaftCore {
 public:
  RaftCore(RaftConfig cfg, PersistentState init);

  // --- inputs ---
  void tick();                       // advance the logical clock by 1
  void step(const Message& m);       // feed one inbound message
  // Append a client command (leader only; no-op otherwise). Phase 3+.
  bool propose(EntryType type, std::string data);
  // Request a linearizable read; `ctx` echoes back via Ready.read_states once
  // leadership is confirmed for the current term. Phase 4+.
  void request_read(uint64_t ctx);

  // --- output ---
  Ready ready();
  void advance(const Ready& rd);

  // --- observation (for the driver + tests) ---
  Role role() const { return role_; }
  bool is_leader() const { return role_ == Role::kLeader; }
  uint64_t term() const { return hard_.current_term; }
  NodeId id() const { return cfg_.id; }
  NodeId leader_id() const { return leader_; }
  uint64_t commit_index() const { return hard_.commit_index; }
  uint64_t last_applied() const { return last_applied_; }
  uint64_t last_log_index() const;
  uint64_t last_log_term() const;
  size_t log_size() const { return log_.size(); }
  // full log, index 1..N (for invariant checkers and tests)
  const std::vector<LogEntry>& log_view() const { return log_; }
  uint64_t match_index_of(NodeId peer) const {
    return match_index_[peer_slot(peer)];
  }
  uint64_t term_at(uint64_t index) const;  // 0 if out of range
  // election timeout currently in effect (ticks); for test assertions
  uint32_t effective_election_timeout() const { return election_timeout_; }

 private:
  // role transitions
  void become_follower(uint64_t term, NodeId leader);
  void become_candidate();
  void become_leader();

  // message handlers
  void handle_pre_vote(const Message& m);
  void handle_pre_vote_resp(const Message& m);
  void handle_request_vote(const Message& m);
  void handle_request_vote_resp(const Message& m);
  void handle_append_entries(const Message& m);
  void handle_append_entries_resp(const Message& m);
  void handle_read_index(const Message& m);
  void handle_read_index_resp(const Message& m);

  // helpers
  void reset_election_timer();
  void fail_pending_reads();
  void start_prevote();
  void send(Message m);
  void broadcast_vote_request(MsgType type, uint64_t term);
  void broadcast_request_vote();
  void broadcast_append_entries(bool heartbeat);
  void send_append_to(NodeId peer, bool heartbeat);
  bool log_is_up_to_date(uint64_t cand_last_index, uint64_t cand_last_term) const;
  void maybe_advance_commit();               // leader: majority matchIndex rule
  void mark_hard_dirty() { hard_dirty_ = true; }
  const std::vector<NodeId>& peers() const { return cfg_.peers; }
  bool is_voter(NodeId n) const;

  RaftConfig cfg_;
  HardState hard_;                 // persistent: currentTerm, votedFor, commit
  std::vector<LogEntry> log_;      // index 1..N == log_[0..N-1]

  Role role_ = Role::kFollower;
  enum class Campaign { kNone, kPre, kReal };
  Campaign campaign_ = Campaign::kNone;
  NodeId leader_ = 0;
  uint64_t last_applied_ = 0;

  // election / heartbeat clocks (in ticks)
  uint32_t election_elapsed_ = 0;
  uint32_t heartbeat_elapsed_ = 0;
  uint32_t election_timeout_ = 0;  // randomized in [T, 2T)
  std::mt19937_64 rng_;

  // candidate vote tally
  std::vector<bool> votes_granted_;   // indexed parallel to cfg_.peers
  std::vector<bool> votes_responded_;

  // leader per-follower progress (Phase 3)
  std::vector<uint64_t> next_index_;
  std::vector<uint64_t> match_index_;

  // ReadIndex bookkeeping (Phase 4)
  uint64_t hb_round_ = 0;  // leader: bumped on every AppendEntries broadcast
  struct PendingRead {
    uint64_t ctx;
    uint64_t index;
    uint64_t round;  // acks with hb_round >= this confirm current leadership
    std::vector<bool> acks;
  };
  std::vector<PendingRead> pending_reads_;

  // Ready accumulators
  bool hard_dirty_ = false;
  uint64_t stable_index_ = 0;      // entries with index <= this are persisted
  std::vector<Message> out_msgs_;
  std::vector<ReadState> ready_reads_;

  size_t peer_slot(NodeId n) const;
};

}  // namespace raftkv
