#include "raft/raft_core.h"

#include <algorithm>
#include <cassert>

#include "common/logging.h"

namespace raftkv {

const char* role_name(Role r) {
  switch (r) {
    case Role::kFollower: return "follower";
    case Role::kCandidate: return "candidate";
    case Role::kLeader: return "leader";
  }
  return "?";
}

const char* msg_name(MsgType t) {
  switch (t) {
    case MsgType::kRequestVote: return "RequestVote";
    case MsgType::kRequestVoteResp: return "RequestVoteResp";
    case MsgType::kAppendEntries: return "AppendEntries";
    case MsgType::kAppendEntriesResp: return "AppendEntriesResp";
    case MsgType::kReadIndex: return "ReadIndex";
    case MsgType::kReadIndexResp: return "ReadIndexResp";
  }
  return "?";
}

RaftCore::RaftCore(RaftConfig cfg, PersistentState init)
    : cfg_(std::move(cfg)),
      hard_(init.hard_state),
      log_(std::move(init.entries)),
      rng_(cfg_.seed ? cfg_.seed : (0x9E3779B97F4A7C15ull ^ cfg_.id)) {
  assert(cfg_.heartbeat_timeout < cfg_.election_timeout);
  std::sort(cfg_.peers.begin(), cfg_.peers.end());
  const size_t n = cfg_.peers.size();
  votes_granted_.assign(n, false);
  votes_responded_.assign(n, false);
  next_index_.assign(n, last_log_index() + 1);
  match_index_.assign(n, 0);
  last_applied_ = 0;  // state machine is rebuilt by replaying committed entries
  stable_index_ = last_log_index();  // everything from disk is already stable
  reset_election_timer();
  become_follower(hard_.current_term, 0);
}

// ---------------------------------------------------------------------------
// small helpers
// ---------------------------------------------------------------------------
size_t RaftCore::peer_slot(NodeId n) const {
  auto it = std::lower_bound(cfg_.peers.begin(), cfg_.peers.end(), n);
  assert(it != cfg_.peers.end() && *it == n);
  return static_cast<size_t>(it - cfg_.peers.begin());
}

bool RaftCore::is_voter(NodeId n) const {
  return std::binary_search(cfg_.peers.begin(), cfg_.peers.end(), n);
}

uint64_t RaftCore::last_log_index() const {
  return log_.empty() ? 0 : log_.back().index;
}
uint64_t RaftCore::last_log_term() const {
  return log_.empty() ? 0 : log_.back().term;
}
uint64_t RaftCore::term_at(uint64_t index) const {
  if (index == 0) return 0;
  if (log_.empty()) return 0;
  uint64_t first = log_.front().index;
  if (index < first || index > log_.back().index) return 0;
  return log_[static_cast<size_t>(index - first)].term;
}

void RaftCore::reset_election_timer() {
  election_elapsed_ = 0;
  const uint32_t t = cfg_.election_timeout;
  election_timeout_ = t + static_cast<uint32_t>(rng_() % t);  // [T, 2T)
}

void RaftCore::send(Message m) {
  m.from = cfg_.id;
  m.term = (m.term == 0) ? hard_.current_term : m.term;
  out_msgs_.push_back(std::move(m));
}

// ---------------------------------------------------------------------------
// role transitions
// ---------------------------------------------------------------------------
void RaftCore::fail_pending_reads() {
  for (auto& pr : pending_reads_) ready_reads_.push_back(ReadState{pr.ctx, 0});
  pending_reads_.clear();
}

void RaftCore::become_follower(uint64_t term, NodeId leader) {
  if (term > hard_.current_term) {
    hard_.current_term = term;
    hard_.voted_for = 0;
    mark_hard_dirty();
  }
  role_ = Role::kFollower;
  leader_ = leader;
  heartbeat_elapsed_ = 0;
  fail_pending_reads();
  reset_election_timer();
}

void RaftCore::become_candidate() {
  role_ = Role::kCandidate;
  hard_.current_term += 1;
  hard_.voted_for = cfg_.id;  // vote for self
  mark_hard_dirty();
  leader_ = 0;
  reset_election_timer();
  std::fill(votes_granted_.begin(), votes_granted_.end(), false);
  std::fill(votes_responded_.begin(), votes_responded_.end(), false);
  votes_granted_[peer_slot(cfg_.id)] = true;
  votes_responded_[peer_slot(cfg_.id)] = true;
  LOG_DEBUG("raft", "node %llu -> candidate term %llu",
            (unsigned long long)cfg_.id, (unsigned long long)hard_.current_term);

  if (cfg_.quorum() <= 1) {  // single-node cluster
    become_leader();
    return;
  }
  broadcast_request_vote();
}

void RaftCore::become_leader() {
  role_ = Role::kLeader;
  leader_ = cfg_.id;
  heartbeat_elapsed_ = 0;
  const uint64_t li = last_log_index();
  for (size_t i = 0; i < cfg_.peers.size(); ++i) {
    next_index_[i] = li + 1;
    match_index_[i] = 0;
  }
  match_index_[peer_slot(cfg_.id)] = li;
  pending_reads_.clear();
  LOG_DEBUG("raft", "node %llu -> LEADER term %llu",
            (unsigned long long)cfg_.id, (unsigned long long)hard_.current_term);
  // Phase 3 will append a no-op here. Phase 2: just start heartbeating.
  broadcast_append_entries(/*heartbeat=*/true);
}

// ---------------------------------------------------------------------------
// clock
// ---------------------------------------------------------------------------
void RaftCore::tick() {
  if (role_ == Role::kLeader) {
    if (++heartbeat_elapsed_ >= cfg_.heartbeat_timeout) {
      heartbeat_elapsed_ = 0;
      broadcast_append_entries(/*heartbeat=*/true);
    }
    return;
  }
  if (++election_elapsed_ >= election_timeout_) {
    become_candidate();
  }
}

// ---------------------------------------------------------------------------
// step
// ---------------------------------------------------------------------------
void RaftCore::step(const Message& m) {
  if (!is_voter(m.from) && m.from != 0) return;  // ignore unknown peers

  // Rule: any RPC with a higher term -> step down and adopt it first.
  if (m.term > hard_.current_term) {
    NodeId lead = 0;
    if (m.type == MsgType::kAppendEntries || m.type == MsgType::kReadIndex)
      lead = m.from;
    become_follower(m.term, lead);
  }

  switch (m.type) {
    case MsgType::kRequestVote: handle_request_vote(m); break;
    case MsgType::kRequestVoteResp: handle_request_vote_resp(m); break;
    case MsgType::kAppendEntries: handle_append_entries(m); break;
    case MsgType::kAppendEntriesResp: handle_append_entries_resp(m); break;
    case MsgType::kReadIndex: handle_read_index(m); break;
    case MsgType::kReadIndexResp: handle_read_index_resp(m); break;
  }
}

bool RaftCore::log_is_up_to_date(uint64_t cand_last_index,
                                 uint64_t cand_last_term) const {
  const uint64_t my_term = last_log_term();
  const uint64_t my_index = last_log_index();
  if (cand_last_term != my_term) return cand_last_term > my_term;
  return cand_last_index >= my_index;
}

void RaftCore::handle_request_vote(const Message& m) {
  Message r;
  r.type = MsgType::kRequestVoteResp;
  r.to = m.from;
  r.term = hard_.current_term;
  r.vote_granted = false;

  if (m.term < hard_.current_term) {  // stale candidate
    send(std::move(r));
    return;
  }
  const bool can_vote =
      (hard_.voted_for == 0 || hard_.voted_for == m.from);
  if (can_vote && log_is_up_to_date(m.last_log_index, m.last_log_term)) {
    hard_.voted_for = m.from;
    mark_hard_dirty();
    reset_election_timer();  // granting a vote defers our own candidacy
    r.vote_granted = true;
  }
  send(std::move(r));
}

void RaftCore::handle_request_vote_resp(const Message& m) {
  if (role_ != Role::kCandidate || m.term != hard_.current_term) return;
  const size_t s = peer_slot(m.from);
  votes_responded_[s] = true;
  if (m.vote_granted) votes_granted_[s] = true;

  size_t granted = 0;
  for (bool g : votes_granted_) granted += g ? 1 : 0;
  if (granted >= cfg_.quorum()) become_leader();
}

void RaftCore::handle_append_entries(const Message& m) {
  Message r;
  r.type = MsgType::kAppendEntriesResp;
  r.to = m.from;
  r.term = hard_.current_term;
  r.success = false;

  if (m.term < hard_.current_term) {  // stale leader
    send(std::move(r));
    return;
  }
  // Valid leader for our term.
  role_ = Role::kFollower;
  leader_ = m.from;
  reset_election_timer();

  // Consistency check on prevLog{Index,Term}.
  if (m.prev_log_index > 0) {
    const uint64_t t = term_at(m.prev_log_index);
    if (m.prev_log_index > last_log_index() || t != m.prev_log_term) {
      // provide a fast-backup hint
      if (m.prev_log_index > last_log_index()) {
        r.conflict_index = last_log_index() + 1;
        r.conflict_term = 0;
      } else {
        r.conflict_term = t;
        // first index of that conflicting term
        uint64_t i = m.prev_log_index;
        while (i > 1 && term_at(i - 1) == t) --i;
        r.conflict_index = i;
      }
      send(std::move(r));
      return;
    }
  }

  // Append / overwrite. Find first divergence.
  uint64_t idx = m.prev_log_index + 1;
  size_t ei = 0;
  for (; ei < m.entries.size(); ++ei, ++idx) {
    if (idx <= last_log_index()) {
      if (term_at(idx) == m.entries[ei].term) continue;  // already matches
      // conflict: truncate our suffix from idx (only uncommitted entries can
      // differ -- committed ones are protected by the election rules)
      const uint64_t first = log_.front().index;
      log_.resize(static_cast<size_t>(idx - first));
      stable_index_ = std::min(stable_index_, idx - 1);
    }
    log_.push_back(m.entries[ei]);
  }

  if (m.leader_commit > hard_.commit_index) {
    hard_.commit_index = std::min(m.leader_commit, last_log_index());
    mark_hard_dirty();
  }

  r.success = true;
  r.match_index = m.prev_log_index + m.entries.size();
  send(std::move(r));
}

void RaftCore::handle_append_entries_resp(const Message& m) {
  if (role_ != Role::kLeader || m.term != hard_.current_term) return;
  const size_t s = peer_slot(m.from);

  if (m.success) {
    match_index_[s] = std::max(match_index_[s], m.match_index);
    next_index_[s] = match_index_[s] + 1;
    maybe_advance_commit();
    // ReadIndex acks piggyback on any successful AppendEntries round.
    for (auto& pr : pending_reads_) {
      if (s < pr.acks.size()) pr.acks[s] = true;
    }
    handle_read_index_resp(m);  // re-check pending reads for quorum
    return;
  }
  // failure -> back up next_index using the conflict hint
  uint64_t ni = next_index_[s];
  if (m.conflict_term != 0) {
    // find last index in our log with conflict_term
    uint64_t last_with_term = 0;
    for (uint64_t i = last_log_index(); i >= 1; --i) {
      if (term_at(i) == m.conflict_term) { last_with_term = i; break; }
      if (i == 1) break;
    }
    ni = last_with_term ? last_with_term + 1 : m.conflict_index;
  } else if (m.conflict_index != 0) {
    ni = m.conflict_index;
  } else if (ni > 1) {
    ni -= 1;
  }
  next_index_[s] = std::max<uint64_t>(1, ni);
  send_append_to(m.from, /*heartbeat=*/false);  // retry immediately
}

// ---------------------------------------------------------------------------
// leader replication senders
// ---------------------------------------------------------------------------
void RaftCore::broadcast_request_vote() {
  for (NodeId p : cfg_.peers) {
    if (p == cfg_.id) continue;
    Message m;
    m.type = MsgType::kRequestVote;
    m.to = p;
    m.term = hard_.current_term;
    m.last_log_index = last_log_index();
    m.last_log_term = last_log_term();
    send(std::move(m));
  }
}

void RaftCore::broadcast_append_entries(bool heartbeat) {
  for (NodeId p : cfg_.peers) {
    if (p == cfg_.id) continue;
    send_append_to(p, heartbeat);
  }
}

void RaftCore::send_append_to(NodeId peer, bool heartbeat) {
  const size_t s = peer_slot(peer);
  uint64_t ni = next_index_[s];
  if (ni < 1) ni = 1;
  const uint64_t prev_index = ni - 1;
  const uint64_t prev_term = term_at(prev_index);

  Message m;
  m.type = MsgType::kAppendEntries;
  m.to = peer;
  m.term = hard_.current_term;
  m.prev_log_index = prev_index;
  m.prev_log_term = prev_term;
  m.leader_commit = hard_.commit_index;
  if (!heartbeat || ni <= last_log_index()) {
    for (uint64_t i = ni; i <= last_log_index(); ++i) {
      const uint64_t first = log_.front().index;
      m.entries.push_back(log_[static_cast<size_t>(i - first)]);
    }
  }
  send(std::move(m));
}

void RaftCore::maybe_advance_commit() {
  // Largest N such that a majority has match_index >= N and log[N].term == cur.
  const uint64_t li = last_log_index();
  for (uint64_t n = li; n > hard_.commit_index; --n) {
    if (term_at(n) != hard_.current_term) continue;  // §5.4.2 safety rule
    size_t count = 0;
    for (size_t i = 0; i < cfg_.peers.size(); ++i)
      if (match_index_[i] >= n) ++count;
    if (count >= cfg_.quorum()) {
      hard_.commit_index = n;
      mark_hard_dirty();
      break;
    }
  }
}

// ---------------------------------------------------------------------------
// ReadIndex (Phase 4)
// ---------------------------------------------------------------------------
void RaftCore::request_read(uint64_t ctx) {
  if (role_ != Role::kLeader) {
    ready_reads_.push_back(ReadState{ctx, 0});  // 0 -> driver replies RETRY
    return;
  }
  PendingRead pr;
  pr.ctx = ctx;
  pr.index = hard_.commit_index;
  pr.acks.assign(cfg_.peers.size(), false);
  pr.acks[peer_slot(cfg_.id)] = true;
  pending_reads_.push_back(std::move(pr));
  // Nudge a heartbeat round so acks come back promptly.
  broadcast_append_entries(/*heartbeat=*/true);
}

void RaftCore::handle_read_index(const Message& m) {
  // followers just redirect; nothing to do in core (driver handles redirect)
  (void)m;
}

void RaftCore::handle_read_index_resp(const Message& m) {
  (void)m;
  if (pending_reads_.empty()) return;
  std::vector<PendingRead> still;
  for (auto& pr : pending_reads_) {
    size_t acks = 0;
    for (bool a : pr.acks) acks += a ? 1 : 0;
    if (acks >= cfg_.quorum())
      ready_reads_.push_back(ReadState{pr.ctx, pr.index});  // confirmed
    else
      still.push_back(std::move(pr));
  }
  pending_reads_.swap(still);
}

// ---------------------------------------------------------------------------
// propose (Phase 3)
// ---------------------------------------------------------------------------
bool RaftCore::propose(EntryType type, std::string data) {
  if (role_ != Role::kLeader) return false;
  LogEntry e;
  e.term = hard_.current_term;
  e.index = last_log_index() + 1;
  e.type = type;
  e.data = std::move(data);
  log_.push_back(std::move(e));
  match_index_[peer_slot(cfg_.id)] = last_log_index();
  broadcast_append_entries(/*heartbeat=*/false);
  return true;
}

// ---------------------------------------------------------------------------
// ready / advance
// ---------------------------------------------------------------------------
Ready RaftCore::ready() {
  Ready rd;
  if (hard_dirty_) rd.hard_state = hard_;

  // Entries the driver still needs to persist. After a plain append this is the
  // fresh tail (stable_index_, last]. After a follower-side conflict truncation
  // stable_index_ was lowered to the truncation point, so this same slice also
  // covers the re-appended divergent suffix -- the driver notices
  // entries[0].index <= its stored last_index and does truncate_suffix first.
  if (last_log_index() > stable_index_) {
    const uint64_t first = log_.front().index;
    for (uint64_t i = stable_index_ + 1; i <= last_log_index(); ++i)
      rd.entries.push_back(log_[static_cast<size_t>(i - first)]);
  }

  rd.messages.swap(out_msgs_);

  // committed-but-not-applied entries
  if (hard_.commit_index > last_applied_) {
    const uint64_t first = log_.empty() ? 1 : log_.front().index;
    for (uint64_t i = last_applied_ + 1; i <= hard_.commit_index; ++i) {
      if (i < first) continue;
      rd.committed.push_back(log_[static_cast<size_t>(i - first)]);
    }
  }

  rd.read_states.swap(ready_reads_);
  return rd;
}

void RaftCore::advance(const Ready& rd) {
  if (rd.hard_state) hard_dirty_ = false;
  if (!rd.entries.empty()) stable_index_ = rd.entries.back().index;
  if (!rd.committed.empty())
    last_applied_ = std::max(last_applied_, rd.committed.back().index);
}

}  // namespace raftkv
