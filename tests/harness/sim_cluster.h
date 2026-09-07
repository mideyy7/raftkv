// Deterministic in-process Raft cluster for unit/integration/fuzz tests.
//
// No threads, no sockets, no wall clock. The test drives everything:
//   cluster.tick_all();        // one logical clock tick on every live node
//   cluster.deliver_all();     // route all queued messages to quiescence
//   cluster.propose(id, cmd);  // client write via a specific node
//   cluster.crash(id); cluster.restart(id);   // lose volatile state, keep disk
//   cluster.isolate(id); cluster.partition({..},{..}); cluster.heal();
//
// Persistence is modelled in RAM by MemLog (entries + HardState) and survives
// crash()/restart(); a crash drops only the volatile RaftCore.
#pragma once
#include <algorithm>
#include <cstdint>
#include <deque>
#include <map>
#include <memory>
#include <random>
#include <set>
#include <string>
#include <vector>

#include "raft/raft_core.h"
#include "storage/kv_table.h"

namespace raftkv::sim {

// ---- RAM-backed persistence (stands in for LogStore on disk) ----
class MemLog {
 public:
  PersistentState state() const { return {hard_, entries_}; }

  void apply_ready_entries(const std::vector<LogEntry>& ents) {
    if (ents.empty()) return;
    const uint64_t from = ents.front().index;
    // truncate anything at/after `from`
    while (!entries_.empty() && entries_.back().index >= from) entries_.pop_back();
    for (const auto& e : ents) {
      // contiguity check mirrors LogStore::append
      if (!entries_.empty() && e.index != entries_.back().index + 1) continue;
      if (entries_.empty() && e.index != 1) continue;
      entries_.push_back(e);
    }
  }
  void set_hard(const HardState& h) { hard_ = h; }

  const std::vector<LogEntry>& entries() const { return entries_; }
  const HardState& hard() const { return hard_; }

 private:
  HardState hard_;
  std::vector<LogEntry> entries_;
};

struct SimNode {
  NodeId id = 0;
  bool alive = true;
  std::unique_ptr<RaftCore> core;
  MemLog disk;
  KvTable kv;
  std::vector<LogEntry> applied;        // full apply history, in order
  uint64_t applied_index = 0;
};

class SimCluster {
 public:
  SimCluster(std::vector<NodeId> ids, uint32_t election_timeout = 10,
             uint32_t heartbeat_timeout = 2, uint64_t seed = 1)
      : ids_(ids), et_(election_timeout), hb_(heartbeat_timeout), rng_(seed) {
    std::sort(ids_.begin(), ids_.end());
    for (NodeId id : ids_) {
      auto n = std::make_unique<SimNode>();
      n->id = id;
      n->core = make_core(*n);
      nodes_[id] = std::move(n);
    }
  }

  // ---- clock ----
  void tick(NodeId id) {
    auto& n = *nodes_.at(id);
    if (!n.alive) return;
    n.core->tick();
    pump_ready(n);
  }
  void tick_all() {
    for (NodeId id : ids_) tick(id);
  }
  // Run `rounds` of {tick_all; deliver_all}.
  void run(int rounds) {
    for (int i = 0; i < rounds; ++i) {
      tick_all();
      deliver_all();
    }
  }

  // ---- message routing ----
  size_t pending() const { return q_.size(); }

  bool deliver_one() {
    if (q_.empty()) return false;
    Message m = q_.front();
    q_.pop_front();
    route(m);
    return true;
  }
  // Deliver until no messages remain (bounded to catch livelock).
  size_t deliver_all(size_t cap = 200000) {
    size_t n = 0;
    while (!q_.empty() && n < cap) {
      n += deliver_one() ? 1 : 0;
    }
    return n;
  }

  // ---- faults ----
  void crash(NodeId id) {
    auto& n = *nodes_.at(id);
    n.alive = false;
    n.core.reset();  // volatile state gone; n.disk survives
    // drop in-flight messages to/from a crashed node
    std::deque<Message> keep;
    for (auto& m : q_)
      if (m.from != id && m.to != id) keep.push_back(m);
    q_.swap(keep);
  }
  void restart(NodeId id) {
    auto& n = *nodes_.at(id);
    n.alive = true;
    n.core = make_core(n);
    // replay committed entries from disk into the KV state machine
    n.kv = KvTable{};
    n.applied.clear();
    n.applied_index = 0;
    uint64_t commit = n.disk.hard().commit_index;
    for (const auto& e : n.disk.entries()) {
      if (e.index > commit) break;
      n.applied.push_back(e);
      n.applied_index = e.index;
      if (e.type == EntryType::kNormal) n.kv.apply(Command::decode(e.data));
    }
  }

  void isolate(NodeId id) {
    for (NodeId o : ids_)
      if (o != id) block_.insert(pair_key(id, o));
  }
  void partition(const std::vector<NodeId>& a, const std::vector<NodeId>& b) {
    for (NodeId x : a)
      for (NodeId y : b) block_.insert(pair_key(x, y));
  }
  void heal() { block_.clear(); }
  void set_drop_prob(double p) { drop_p_ = p; }

  // ---- client ops ----
  bool propose(NodeId id, const Command& c) {
    auto& n = *nodes_.at(id);
    if (!n.alive || !n.core) return false;
    bool ok = n.core->propose(EntryType::kNormal, c.encode());
    pump_ready(n);
    return ok;
  }
  void request_read(NodeId id, uint64_t ctx) {
    auto& n = *nodes_.at(id);
    if (!n.alive || !n.core) return;
    n.core->request_read(ctx);
    pump_ready(n);
  }

  // ---- observation ----
  SimNode& node(NodeId id) { return *nodes_.at(id); }
  const std::vector<NodeId>& ids() const { return ids_; }

  // The unique leader if exactly one live node claims leadership for the
  // highest term seen; 0 otherwise.
  NodeId leader() const {
    NodeId lead = 0;
    uint64_t best_term = 0;
    int leaders_at_best = 0;
    for (NodeId id : ids_) {
      const auto& n = *nodes_.at(id);
      if (!n.alive || !n.core) continue;
      if (n.core->is_leader()) {
        if (n.core->term() > best_term) {
          best_term = n.core->term();
          lead = id;
          leaders_at_best = 1;
        } else if (n.core->term() == best_term) {
          ++leaders_at_best;
        }
      }
    }
    return leaders_at_best == 1 ? lead : 0;
  }

  int leader_count_for_term(uint64_t term) const {
    int c = 0;
    for (NodeId id : ids_) {
      const auto& n = *nodes_.at(id);
      if (n.alive && n.core && n.core->is_leader() && n.core->term() == term) ++c;
    }
    return c;
  }

 private:
  std::unique_ptr<RaftCore> make_core(SimNode& n) {
    RaftConfig cfg;
    cfg.id = n.id;
    cfg.peers = ids_;
    cfg.election_timeout = et_;
    cfg.heartbeat_timeout = hb_;
    cfg.seed = 0x1000 + n.id;  // per-node deterministic election jitter
    return std::make_unique<RaftCore>(cfg, n.disk.state());
  }

  void pump_ready(SimNode& n) {
    if (!n.core) return;
    for (int guard = 0; guard < 1000; ++guard) {
      Ready rd = n.core->ready();
      if (rd.empty()) return;
      if (rd.hard_state) n.disk.set_hard(*rd.hard_state);
      if (!rd.entries.empty()) n.disk.apply_ready_entries(rd.entries);
      for (auto& m : rd.messages) q_.push_back(m);
      for (auto& e : rd.committed) {
        n.applied.push_back(e);
        n.applied_index = e.index;
        if (e.type == EntryType::kNormal) n.kv.apply(Command::decode(e.data));
      }
      n.core->advance(rd);
    }
  }

  void route(const Message& m) {
    auto dit = nodes_.find(m.to);
    if (dit == nodes_.end()) return;
    SimNode& dst = *dit->second;
    if (!dst.alive || !dst.core) return;
    if (block_.count(pair_key(m.from, m.to))) return;  // partitioned
    if (drop_p_ > 0.0) {
      std::uniform_real_distribution<double> u(0.0, 1.0);
      if (u(rng_) < drop_p_) return;
    }
    dst.core->step(m);
    pump_ready(dst);
  }

  static uint64_t pair_key(NodeId a, NodeId b) {
    if (a > b) std::swap(a, b);
    return (a << 32) ^ b;
  }

  std::vector<NodeId> ids_;
  uint32_t et_, hb_;
  std::mt19937_64 rng_;
  double drop_p_ = 0.0;
  std::map<NodeId, std::unique_ptr<SimNode>> nodes_;
  std::deque<Message> q_;
  std::set<uint64_t> block_;
};

}  // namespace raftkv::sim
