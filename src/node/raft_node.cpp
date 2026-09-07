#include "node/raft_node.h"

#include <sys/stat.h>

#include <algorithm>
#include <chrono>

#include "common/codec.h"
#include "common/logging.h"

namespace raftkv {
namespace {
uint32_t ms_to_ticks(uint32_t ms, uint32_t tick_ms) {
  return std::max<uint32_t>(1, ms / std::max<uint32_t>(1, tick_ms));
}
}  // namespace

RaftNode::RaftNode(NodeConfig cfg, Transport* transport)
    : cfg_(std::move(cfg)), transport_(transport) {
  ::mkdir(cfg_.data_dir.c_str(), 0755);
  std::string wal = cfg_.data_dir + "/raft.wal";
  auto lr = LogStore::open(wal);
  if (!lr) {
    LOG_ERROR("node", "open log %s: %s", wal.c_str(), lr.message().c_str());
    std::abort();
  }
  log_ = std::move(lr.value());

  RaftConfig rc;
  rc.id = cfg_.id;
  rc.peers = cfg_.peer_ids();
  rc.election_timeout = ms_to_ticks(cfg_.election_timeout_ms, cfg_.tick_ms);
  rc.heartbeat_timeout = ms_to_ticks(cfg_.heartbeat_ms, cfg_.tick_ms);
  rc.seed = cfg_.seed;

  PersistentState ps;
  ps.hard_state = log_->hard_state();
  ps.entries = log_->entries(log_->first_index(), log_->last_index() + 1);
  core_ = std::make_unique<RaftCore>(std::move(rc), std::move(ps));

  // Rebuild the state machine by replaying committed entries.
  for (uint64_t i = 1; i <= core_->commit_index(); ++i) {
    if (i < log_->first_index()) continue;
    apply_entry_locked(log_->at(i));
  }
}

RaftNode::~RaftNode() { stop(); }

void RaftNode::start() {
  {
    std::lock_guard<std::mutex> g(mu_);
    running_ = true;
  }
  transport_->set_receiver([this](Message m) { on_message(std::move(m)); });
  transport_->start();
  raft_thread_ = std::thread([this] { raft_loop(); });
}

void RaftNode::stop() {
  {
    std::lock_guard<std::mutex> g(mu_);
    if (!running_) return;
    running_ = false;
  }
  wake_.notify_all();
  applied_cv_.notify_all();
  if (raft_thread_.joinable()) raft_thread_.join();
  transport_->stop();
}

void RaftNode::on_message(Message m) {
  {
    std::lock_guard<std::mutex> g(mu_);
    if (!running_) return;
    mailbox_.push_back(std::move(m));
  }
  wake_.notify_one();
}

void RaftNode::raft_loop() {
  using clock = std::chrono::steady_clock;
  const auto tick_period = std::chrono::milliseconds(cfg_.tick_ms);
  auto last_tick = clock::now();

  std::unique_lock<std::mutex> lk(mu_);
  while (running_) {
    wake_.wait_until(lk, last_tick + tick_period, [&] {
      return !running_ || !mailbox_.empty() || pending_work_;
    });
    if (!running_) break;
    pending_work_ = false;

    // Drain every queued message in one pass so concurrent client proposals
    // and follower acks batch into a single Ready -> one fsync, one
    // AppendEntries round.
    while (!mailbox_.empty()) {
      Message m = std::move(mailbox_.front());
      mailbox_.pop_front();
      core_->step(m);
    }

    auto now = clock::now();
    while (now - last_tick >= tick_period) {
      core_->tick();
      last_tick += tick_period;
    }

    process_ready_locked(lk);
  }
}

void RaftNode::process_ready_locked(std::unique_lock<std::mutex>& lk) {
  bool applied_advanced = false;
  for (int guard = 0; guard < 10000; ++guard) {
    Ready rd = core_->ready();
    if (rd.empty()) break;

    if (rd.hard_state) {
      if (Status s = log_->set_hard_state(*rd.hard_state); !s)
        LOG_ERROR("node", "set_hard_state: %s", s.message().c_str());
    }
    if (!rd.entries.empty()) {
      uint64_t first = rd.entries.front().index;
      if (first <= log_->last_index()) log_->truncate_suffix(first);
      if (Status s = log_->append_batch(rd.entries); !s)
        LOG_ERROR("node", "append_batch: %s", s.message().c_str());
    }
    if (rd.hard_state || !rd.entries.empty()) {
      // fsync is the slow part (~ms). Only the raft thread ever writes log_, so
      // it is safe to drop mu_ here -- client threads can queue more proposals
      // into the core meanwhile, which batch into the next Ready.
      lk.unlock();
      log_->sync();
      lk.lock();
    }

    for (const auto& m : rd.messages) transport_->send(m.to, m);

    for (const auto& e : rd.committed) {
      apply_entry_locked(e);
      applied_advanced = true;
    }

    for (const auto& rs : rd.read_states) read_results_[rs.ctx] = rs.index;

    core_->advance(rd);
  }
  if (applied_advanced || !read_results_.empty()) {
    lk.unlock();
    applied_cv_.notify_all();
    lk.lock();
  }
}

void RaftNode::apply_entry_locked(const LogEntry& e) {
  if (e.index <= applied_index_) return;
  applied_index_ = e.index;
  if (e.type != EntryType::kNormal) return;

  // Command layout for client writes: [u64 client_id][u64 seq][Command bytes].
  // Phase 2/3 internal proposals use a plain Command with client_id 0.
  if (e.data.size() < 16) return;
  Reader r(e.data);
  uint64_t client_id = r.u64();
  uint64_t seq = r.u64();
  Command cmd = Command::decode(r.rest());

  std::string value;
  bool found = false;
  if (client_id != 0) {
    if (auto cached = sessions_.lookup(client_id, seq)) {
      apply_results_[e.index] = cached->last_value;
      return;  // already applied -- do NOT touch the state machine again
    }
  }
  const bool existed = kv_.contains(cmd.key);
  value = kv_.apply(cmd);
  found = (cmd.op == CmdOp::kPut) ? true : existed;
  if (client_id != 0) sessions_.record(client_id, seq, value, found);
  apply_results_[e.index] = value;
}

RaftNode::NodeStatus RaftNode::status() {
  std::lock_guard<std::mutex> g(mu_);
  NodeStatus s;
  s.role = core_->role();
  s.term = core_->term();
  s.leader = core_->leader_id();
  s.commit_index = core_->commit_index();
  s.last_applied = applied_index_;
  s.last_log_index = core_->last_log_index();
  return s;
}

RaftNode::ClientResult RaftNode::client_write(uint64_t client_id, uint64_t seq,
                                              const Command& cmd) {
  std::unique_lock<std::mutex> lk(mu_);
  if (!core_->is_leader()) {
    ClientResult r;
    r.status = core_->leader_id() ? ClientStatus::kRedirect : ClientStatus::kRetry;
    r.leader_hint = core_->leader_id();
    return r;
  }
  if (auto cached = sessions_.lookup(client_id, seq)) {
    ClientResult r;
    r.status = ClientStatus::kOk;
    r.value = cached->last_value;
    r.found = cached->last_found;
    return r;
  }

  Buffer b;
  b.u64(client_id);
  b.u64(seq);
  b.raw(cmd.encode());
  if (!core_->propose(EntryType::kNormal, b.take())) {
    ClientResult r;
    r.status = ClientStatus::kRetry;
    return r;
  }
  uint64_t idx = core_->last_log_index();
  uint64_t want_term = core_->term();
  // Hand off to the raft thread instead of persisting inline: concurrent
  // client_write calls then coalesce into one fsync + one replication round.
  pending_work_ = true;
  wake_.notify_one();

  bool ok = applied_cv_.wait_for(lk, std::chrono::seconds(3), [&] {
    return !running_ || applied_index_ >= idx || core_->term() != want_term;
  });
  ClientResult r;
  if (!running_ || !ok || core_->term() != want_term || applied_index_ < idx) {
    r.status = ClientStatus::kRetry;
    r.leader_hint = core_->leader_id();
    return r;
  }
  r.status = ClientStatus::kOk;
  auto it = apply_results_.find(idx);
  if (it != apply_results_.end()) {
    r.value = it->second;
    apply_results_.erase(it);
  }
  if (auto cached = sessions_.lookup(client_id, seq)) r.found = cached->last_found;
  return r;
}

RaftNode::ClientResult RaftNode::client_read(const std::string& key) {
  std::unique_lock<std::mutex> lk(mu_);
  if (!core_->is_leader()) {
    ClientResult r;
    r.status = core_->leader_id() ? ClientStatus::kRedirect : ClientStatus::kRetry;
    r.leader_hint = core_->leader_id();
    return r;
  }
  uint64_t ctx = next_read_ctx_++;
  core_->request_read(ctx);
  pending_work_ = true;
  wake_.notify_one();

  bool ok = applied_cv_.wait_for(lk, std::chrono::seconds(3), [&] {
    return !running_ || read_results_.count(ctx) > 0;
  });
  ClientResult r;
  auto it = read_results_.find(ctx);
  if (!running_ || !ok || it == read_results_.end() || it->second == 0) {
    if (it != read_results_.end()) read_results_.erase(it);
    r.status = ClientStatus::kRetry;
    r.leader_hint = core_->leader_id();
    return r;
  }
  uint64_t need = it->second;
  read_results_.erase(it);
  // wait for the state machine to catch up to the confirmed index
  applied_cv_.wait_for(lk, std::chrono::seconds(3), [&] {
    return !running_ || applied_index_ >= need || !core_->is_leader();
  });
  if (!core_->is_leader()) {
    r.status = ClientStatus::kRetry;
    return r;
  }
  auto v = kv_.get(key);
  r.status = ClientStatus::kOk;
  r.found = v.has_value();
  r.value = v.value_or("");
  return r;
}

}  // namespace raftkv
