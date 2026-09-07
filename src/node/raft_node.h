// RaftNode: the threaded driver that turns the pure RaftCore into a running
// node. Owns the raft thread (single-threaded consensus stepping), the
// LogStore (durable log), the KvTable (state machine), and wires them to a
// Transport.
//
// Consensus invariant: RaftCore is only ever touched while holding mu_, and
// only from the raft thread's process path or from client calls that take mu_.
#pragma once
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>

#include "common/config.h"
#include "raft/raft_core.h"
#include "storage/kv_table.h"
#include "storage/log_store.h"
#include "storage/session_cache.h"
#include "transport/transport.h"

namespace raftkv {

class RaftNode {
 public:
  RaftNode(NodeConfig cfg, Transport* transport);
  ~RaftNode();

  void start();
  void stop();

  struct NodeStatus {
    Role role = Role::kFollower;
    uint64_t term = 0;
    NodeId leader = 0;
    uint64_t commit_index = 0;
    uint64_t last_applied = 0;
    uint64_t last_log_index = 0;
  };
  NodeStatus status();

  enum class ClientStatus { kOk, kRedirect, kRetry };
  struct ClientResult {
    ClientStatus status = ClientStatus::kRetry;
    std::string value;
    bool found = false;
    NodeId leader_hint = 0;
  };

  // Phase 4 client API. client_id + seq give exactly-once semantics.
  ClientResult client_write(uint64_t client_id, uint64_t seq, const Command& cmd);
  ClientResult client_read(const std::string& key);

  Transport* transport() { return transport_; }
  NodeId id() const { return cfg_.id; }

 private:
  void raft_loop();
  void on_message(Message m);
  void process_ready_locked(std::unique_lock<std::mutex>& lk);
  void apply_entry_locked(const LogEntry& e);

  NodeConfig cfg_;
  Transport* transport_;
  std::unique_ptr<LogStore> log_;
  std::unique_ptr<RaftCore> core_;
  KvTable kv_;
  SessionCache sessions_;

  std::mutex mu_;
  std::condition_variable wake_;   // raft thread
  std::condition_variable applied_cv_;  // client waiters
  std::deque<Message> mailbox_;
  bool running_ = false;
  std::thread raft_thread_;

  uint64_t applied_index_ = 0;
  std::map<uint64_t, std::string> apply_results_;  // index -> value (for waiters)

  // pending ReadIndex requests: ctx -> confirmed commit index (0 == not yet)
  uint64_t next_read_ctx_ = 1;
  std::map<uint64_t, uint64_t> read_results_;
};

}  // namespace raftkv
