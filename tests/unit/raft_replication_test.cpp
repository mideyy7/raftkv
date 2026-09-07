// Phase 3 white-box replication tests.
#include "harness/sim_cluster.h"
#include "harness/solo.h"
#include "tinytest.h"

using namespace raftkv;
using raftkv::sim::SimCluster;
using raftkv::sim::Solo;

static Command put(const std::string& k, const std::string& v) {
  return Command{CmdOp::kPut, k, v};
}
// valid encoded-command bytes for a hand-built LogEntry
static std::string cmd_bytes(const std::string& tag) {
  return Command{CmdOp::kPut, "x", tag}.encode();
}

// Drive the cluster until a stable leader exists; return its id.
static NodeId settle(SimCluster& c, int rounds = 200) {
  for (int i = 0; i < rounds; ++i) {
    c.tick_all();
    c.deliver_all();
    NodeId l = c.leader();
    if (l) {
      for (int k = 0; k < 6; ++k) { c.tick_all(); c.deliver_all(); }
      if (c.leader() == l) return l;
    }
  }
  return 0;
}

TEST(replication, leader_appends_noop_on_election) {
  SimCluster c({1, 2, 3});
  NodeId l = settle(c);
  REQUIRE(l != 0u);
  const auto& log = c.node(l).core->log_view();
  REQUIRE(!log.empty());
  CHECK_EQ(static_cast<int>(log[0].type), static_cast<int>(EntryType::kNoOp));
  CHECK_EQ(log[0].index, 1u);
  CHECK_EQ(log[0].term, c.node(l).core->term());
  // and it commits quickly (current-term entry, majority replicated)
  CHECK(c.node(l).core->commit_index() >= 1u);
}

TEST(replication, single_proposal_commits_and_applies_once) {
  SimCluster c({1, 2, 3});
  NodeId l = settle(c);
  REQUIRE(l != 0u);
  CHECK(c.propose(l, put("a", "1")));
  for (int i = 0; i < 20; ++i) { c.tick_all(); c.deliver_all(); }

  for (NodeId id : c.ids()) {
    auto v = c.node(id).kv.get("a");
    REQUIRE(v.has_value());
    CHECK_EQ(*v, std::string("1"));
    // applied exactly once: count kNormal entries with key "a"
    int n = 0;
    for (const auto& e : c.node(id).applied)
      if (e.type == EntryType::kNormal) ++n;
    CHECK_EQ(n, 1);
  }
  CHECK(c.check_invariants().empty());
}

TEST(replication, commits_with_bare_majority_laggards_catch_up) {
  SimCluster c({1, 2, 3, 4, 5});
  NodeId l = settle(c);
  REQUIRE(l != 0u);

  // isolate two followers, propose, they must not block commit
  std::vector<NodeId> lag;
  for (NodeId id : c.ids())
    if (id != l && lag.size() < 2) lag.push_back(id);
  for (NodeId id : lag) c.isolate(id);

  for (int i = 0; i < 30; ++i) {
    c.propose(l, put("k" + std::to_string(i), "v" + std::to_string(i)));
    c.tick_all();
    c.deliver_all();
  }
  CHECK(c.node(l).core->commit_index() >= 30u);

  // heal -> laggards converge
  c.heal();
  for (int i = 0; i < 60; ++i) { c.tick_all(); c.deliver_all(); }
  for (NodeId id : lag) {
    CHECK_EQ(c.node(id).kv.get("k29").value_or(""), std::string("v29"));
  }
  CHECK(c.check_invariants().empty());
}

TEST(replication, follower_divergent_tail_is_overwritten_committed_prefix_kept) {
  // Node 1 follower with a hand-built log that diverges from the leader in an
  // uncommitted suffix. After AppendEntries from a term-5 leader, the
  // uncommitted suffix is replaced; the (committed) prefix survives.
  Solo s(1, {1, 2, 3});
  Message seed;
  seed.type = MsgType::kAppendEntries;
  seed.from = 2;
  seed.term = 3;
  seed.prev_log_index = 0;
  seed.prev_log_term = 0;
  seed.leader_commit = 2;  // first two entries are committed
  seed.entries = {
      LogEntry{1, 1, EntryType::kNormal, cmd_bytes("A")},
      LogEntry{1, 2, EntryType::kNormal, cmd_bytes("B")},
      LogEntry{3, 3, EntryType::kNormal, cmd_bytes("C-old")},  // uncommitted
      LogEntry{3, 4, EntryType::kNormal, cmd_bytes("D-old")},  // uncommitted
  };
  s.step(seed);
  REQUIRE(s.core().last_log_index() == 4u);
  REQUIRE(s.core().commit_index() == 2u);

  // new leader term 5 replaces indices 3.. with different entries
  Message ae;
  ae.type = MsgType::kAppendEntries;
  ae.from = 3;
  ae.term = 5;
  ae.prev_log_index = 2;
  ae.prev_log_term = 1;
  ae.leader_commit = 4;
  ae.entries = {
      LogEntry{5, 3, EntryType::kNormal, cmd_bytes("C-new")},
      LogEntry{5, 4, EntryType::kNormal, cmd_bytes("D-new")},
  };
  s.step(ae);

  CHECK_EQ(s.core().term_at(1), 1u);  // committed prefix intact
  CHECK_EQ(s.core().term_at(2), 1u);
  CHECK_EQ(s.core().term_at(3), 5u);  // divergent suffix replaced
  CHECK_EQ(s.core().term_at(4), 5u);
  auto resp = s.last_of(MsgType::kAppendEntriesResp);
  REQUIRE(resp.has_value());
  CHECK(resp->success);
}

TEST(replication, conflict_hint_gives_fast_backup) {
  // Leader 1 with a long log; follower 2 far behind. Count the AppendEntries
  // round trips to converge -- should be O(1), not O(gap).
  SimCluster c({1, 2, 3});
  NodeId l = settle(c);
  REQUIRE(l != 0u);
  NodeId f = 0;
  for (NodeId id : c.ids()) if (id != l) { f = id; break; }

  c.isolate(f);
  for (int i = 0; i < 200; ++i) {
    c.propose(l, put("k" + std::to_string(i), "x"));
    c.tick_all(); c.deliver_all();
  }
  REQUIRE(c.node(l).core->commit_index() >= 200u);

  c.heal();
  int rounds = 0;
  for (; rounds < 40; ++rounds) {
    c.tick_all();
    c.deliver_all();
    if (c.node(f).core->last_log_index() >= c.node(l).core->commit_index()) break;
  }
  // With one-entry-at-a-time backup this would need ~200 rounds. The conflict
  // hint should get there in a handful of heartbeat rounds.
  CHECK(rounds < 15);
  CHECK(c.check_invariants().empty());
}

static Message prevote_resp(NodeId from, uint64_t term) {
  Message m; m.type = MsgType::kPreVoteResp; m.from = from; m.term = term;
  m.vote_granted = true; return m;
}
static Message vote_resp(NodeId from, uint64_t term) {
  Message m; m.type = MsgType::kRequestVoteResp; m.from = from; m.term = term;
  m.vote_granted = true; return m;
}
static Message ae_resp(NodeId from, uint64_t term, uint64_t match) {
  Message m; m.type = MsgType::kAppendEntriesResp; m.from = from; m.term = term;
  m.success = true; m.match_index = match; return m;
}

// Raft paper Figure 8: a leader must NOT treat an entry from an earlier term as
// committed just because it sits on a majority -- a future leader that lacks it
// could still overwrite it. Only once a *current-term* entry commits on top do
// the earlier entries commit with it (§5.4.2).
TEST(replication, figure8_prior_term_entry_not_committed_by_count) {
  Solo s(1, {1, 2, 3});
  s.preload(
      {
          LogEntry{1, 1, EntryType::kNoOp, ""},
          LogEntry{2, 2, EntryType::kNormal, cmd_bytes("X")},  // prior-term entry
      },
      HardState{/*term=*/2, /*vote=*/0, /*commit=*/1});

  // elect node 1 to term 3
  s.tick(19);
  s.step(prevote_resp(2, 2));
  s.step(vote_resp(2, 3));
  REQUIRE(s.core().is_leader());
  REQUIRE(s.core().term() == 3u);
  // leader appended a term-3 no-op at index 3
  REQUIRE(s.core().last_log_index() == 3u);
  REQUIRE(s.core().term_at(2) == 2u);
  REQUIRE(s.core().term_at(3) == 3u);
  CHECK_EQ(s.core().commit_index(), 1u);

  // follower 2 acks up to index 2 (has the prior-term entry) -> majority on
  // index 2, but index 2 is term 2 != current term 3 -> MUST NOT commit it
  s.step(ae_resp(2, 3, /*match=*/2));
  CHECK_EQ(s.core().commit_index(), 1u);

  // follower 2 now acks the term-3 no-op at index 3 -> current-term entry on a
  // majority -> commit advances to 3, dragging the term-2 entry in safely
  s.step(ae_resp(2, 3, /*match=*/3));
  CHECK_EQ(s.core().commit_index(), 3u);
}

TEST(replication, commit_index_monotonic_and_bounded) {
  SimCluster c({1, 2, 3});
  NodeId l = settle(c);
  REQUIRE(l != 0u);
  std::map<NodeId, uint64_t> last_commit;
  for (int i = 0; i < 100; ++i) {
    if (i % 3 == 0) c.propose(l, put("k" + std::to_string(i), "v"));
    c.tick_all();
    c.deliver_all();
    for (NodeId id : c.ids()) {
      uint64_t ci = c.node(id).core ? c.node(id).core->commit_index() : 0;
      CHECK(ci >= last_commit[id]);          // never decreases
      last_commit[id] = ci;
      if (c.node(id).core)
        CHECK(ci <= c.node(id).core->last_log_index());  // bounded
    }
  }
}

TEST(replication, restart_does_not_double_apply) {
  SimCluster c({1, 2, 3});
  NodeId l = settle(c);
  REQUIRE(l != 0u);
  for (int i = 0; i < 20; ++i) {
    c.propose(l, put("k" + std::to_string(i), "v" + std::to_string(i)));
    c.tick_all(); c.deliver_all();
  }
  NodeId f = 0;
  for (NodeId id : c.ids()) if (id != l) { f = id; break; }
  uint64_t applied_before = c.node(f).applied_index;
  REQUIRE(applied_before >= 20u);

  c.crash(f);
  c.restart(f);            // replays committed entries from disk once
  for (int i = 0; i < 30; ++i) { c.tick_all(); c.deliver_all(); }

  // applied history has each index exactly once
  std::set<uint64_t> seen;
  for (const auto& e : c.node(f).applied) {
    CHECK(seen.insert(e.index).second);  // insert fails if duplicate
  }
  CHECK(c.check_invariants().empty());
}

TINYTEST_MAIN()
