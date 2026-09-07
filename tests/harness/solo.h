// Solo -- a single RaftCore plus RAM persistence and message capture, for
// white-box unit tests that hand-craft inbound messages and inspect the
// core's replies / persisted state / role transitions.
#pragma once
#include <memory>
#include <optional>
#include <vector>

#include "raft/raft_core.h"
#include "storage/kv_table.h"
#include "harness/sim_cluster.h"

namespace raftkv::sim {

class Solo {
 public:
  Solo(NodeId id, std::vector<NodeId> peers, uint32_t et = 10, uint32_t hb = 2,
       uint64_t seed = 12345)
      : id_(id), peers_(std::move(peers)), et_(et), hb_(hb), seed_(seed) {
    build();
  }

  // Preload durable state (log + HardState) then (re)build the core -- for
  // white-box tests that need a node to start with a specific log, e.g. a
  // prior-term entry for the Figure-8 commit-safety scenario.
  void preload(std::vector<LogEntry> log, HardState hs) {
    for (auto& e : log) disk_.apply_ready_entries({e});
    disk_.set_hard(hs);
    build();
  }

  RaftCore& core() { return *core_; }
  MemLog& disk() { return disk_; }

  void tick(int n = 1) {
    for (int i = 0; i < n; ++i) {
      core_->tick();
      drain();
    }
  }
  void step(const Message& m) {
    core_->step(m);
    drain();
  }
  bool propose(const Command& c) {
    bool ok = core_->propose(EntryType::kNormal, c.encode());
    drain();
    return ok;
  }

  // Messages emitted since the last take_messages().
  std::vector<Message> take_messages() {
    std::vector<Message> out;
    out.swap(out_);
    return out;
  }
  std::optional<Message> last_of(MsgType t) {
    for (auto it = out_.rbegin(); it != out_.rend(); ++it)
      if (it->type == t) return *it;
    return std::nullopt;
  }

  // Simulate a crash+restart: rebuild the core from persisted state only.
  void restart() { build(); }

  const std::vector<LogEntry>& applied() const { return applied_; }
  uint64_t persisted_term() const { return disk_.hard().current_term; }
  uint64_t persisted_vote() const { return disk_.hard().voted_for; }

 private:
  void build() {
    RaftConfig cfg;
    cfg.id = id_;
    cfg.peers = peers_;
    cfg.election_timeout = et_;
    cfg.heartbeat_timeout = hb_;
    cfg.seed = seed_;
    core_ = std::make_unique<RaftCore>(cfg, disk_.state());
    drain();
  }
  void drain() {
    for (int g = 0; g < 1000; ++g) {
      Ready rd = core_->ready();
      if (rd.empty()) return;
      if (rd.hard_state) disk_.set_hard(*rd.hard_state);
      if (!rd.entries.empty()) disk_.apply_ready_entries(rd.entries);
      for (auto& m : rd.messages) out_.push_back(m);
      for (auto& e : rd.committed) {
        applied_.push_back(e);
        if (e.type == EntryType::kNormal) kv_.apply(Command::decode(e.data));
      }
      core_->advance(rd);
    }
  }

  NodeId id_;
  std::vector<NodeId> peers_;
  uint32_t et_, hb_;
  uint64_t seed_;
  std::unique_ptr<RaftCore> core_;
  MemLog disk_;
  KvTable kv_;
  std::vector<Message> out_;
  std::vector<LogEntry> applied_;
};

}  // namespace raftkv::sim
