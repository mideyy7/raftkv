// Phase 3 integration: replication + durability under faults, over the
// deterministic SimCluster, plus the linearizability checker's self-tests and
// a real linearizability check on a fault-injected run.
#include <map>
#include <random>

#include "harness/sim_cluster.h"
#include "lin/checker.h"
#include "tinytest.h"

using namespace raftkv;
using raftkv::sim::SimCluster;

static Command put(const std::string& k, const std::string& v) {
  return Command{CmdOp::kPut, k, v};
}
static NodeId settle(SimCluster& c, int rounds = 300) {
  for (int i = 0; i < rounds; ++i) {
    c.tick_all();
    c.deliver_all();
    NodeId l = c.leader();
    if (l) {
      for (int k = 0; k < 8; ++k) { c.tick_all(); c.deliver_all(); }
      if (c.leader() == l) return l;
    }
  }
  return 0;
}
static void run(SimCluster& c, int n) {
  for (int i = 0; i < n; ++i) { c.tick_all(); c.deliver_all(); }
}

TEST(replication_integ, thousand_writes_converge_3_and_5) {
  for (int nn : {3, 5}) {
    std::vector<NodeId> ids;
    for (int i = 1; i <= nn; ++i) ids.push_back(i);
    SimCluster c(ids);
    NodeId l = settle(c);
    REQUIRE(l != 0u);

    std::map<std::string, std::string> model;
    for (int i = 0; i < 1000; ++i) {
      std::string k = "k" + std::to_string(i % 50);
      std::string v = "v" + std::to_string(i);
      c.propose(l, put(k, v));
      model[k] = v;
      if (i % 20 == 0) run(c, 1);
    }
    run(c, 300);

    CHECK(c.check_invariants().empty());
    for (NodeId id : ids) {
      for (auto& [k, v] : model)
        CHECK_EQ(c.node(id).kv.get(k).value_or("<none>"), v);
    }
  }
}

TEST(replication_integ, follower_crash_and_catch_up) {
  SimCluster c({1, 2, 3, 4, 5});
  NodeId l = settle(c);
  REQUIRE(l != 0u);
  NodeId victim = 0;
  for (NodeId id : c.ids()) if (id != l) { victim = id; break; }

  c.crash(victim);
  std::map<std::string, std::string> model;
  for (int i = 0; i < 500; ++i) {
    c.propose(l, put("k" + std::to_string(i % 30), "v" + std::to_string(i)));
    model["k" + std::to_string(i % 30)] = "v" + std::to_string(i);
    if (i % 15 == 0) run(c, 1);
  }
  run(c, 100);
  c.restart(victim);
  run(c, 400);

  CHECK(c.check_invariants().empty());
  for (auto& [k, v] : model)
    CHECK_EQ(c.node(victim).kv.get(k).value_or("<none>"), v);
}

TEST(replication_integ, leader_crash_preserves_committed) {
  SimCluster c({1, 2, 3, 4, 5}, 10, 2, 7);
  NodeId l = settle(c);
  REQUIRE(l != 0u);

  // commit K writes, remember them, then kill the leader
  std::map<std::string, std::string> committed;
  for (int i = 0; i < 40; ++i) {
    c.propose(l, put("c" + std::to_string(i), "v" + std::to_string(i)));
    run(c, 2);
    committed["c" + std::to_string(i)] = "v" + std::to_string(i);
  }
  uint64_t safe_commit = c.node(l).core->commit_index();
  c.crash(l);
  run(c, 400);  // new election + continued life

  // every acknowledged (committed) write survives on every alive node
  for (NodeId id : c.ids()) {
    if (!c.node(id).alive) continue;
    CHECK(c.node(id).core->commit_index() >= safe_commit);
    for (auto& [k, v] : committed)
      CHECK_EQ(c.node(id).kv.get(k).value_or("<none>"), v);
  }
  CHECK(c.check_invariants().empty());
}

TEST(replication_integ, minority_partition_no_committed_loss) {
  SimCluster c({1, 2, 3, 4, 5}, 10, 2, 11);
  NodeId l = settle(c);
  REQUIRE(l != 0u);

  std::vector<NodeId> minority{l}, majority;
  for (NodeId id : c.ids())
    if (id != l && minority.size() < 2) minority.push_back(id);
  for (NodeId id : c.ids())
    if (std::find(minority.begin(), minority.end(), id) == minority.end())
      majority.push_back(id);
  c.partition(minority, majority);

  // find the new majority-side leader
  NodeId l2 = 0;
  for (int i = 0; i < 400 && !l2; ++i) {
    c.tick_all(); c.deliver_all();
    for (NodeId id : majority)
      if (c.node(id).core && c.node(id).core->is_leader()) l2 = id;
  }
  REQUIRE(l2 != 0u);

  std::map<std::string, std::string> model;
  for (int i = 0; i < 300; ++i) {
    c.propose(l2, put("k" + std::to_string(i % 20), "v" + std::to_string(i)));
    model["k" + std::to_string(i % 20)] = "v" + std::to_string(i);
    if (i % 10 == 0) run(c, 1);
  }
  run(c, 200);
  uint64_t maj_commit = c.node(l2).core->commit_index();

  c.heal();
  run(c, 500);

  CHECK(c.check_invariants().empty());
  for (NodeId id : c.ids()) {
    CHECK(c.node(id).core->commit_index() >= maj_commit);
    for (auto& [k, v] : model)
      CHECK_EQ(c.node(id).kv.get(k).value_or("<none>"), v);
  }
}

// ---- linearizability checker self-tests ----
TEST(lin_checker, accepts_a_linearizable_history) {
  using namespace raftkv::lin;
  // put x=1 [0,2], get x -> 1 [3,4], put x=2 [5,7], get x -> 2 [8,9]
  std::vector<Event> h = {
      {OpKind::kPut, "x", "1", true, 0, 2},
      {OpKind::kGet, "x", "1", true, 3, 4},
      {OpKind::kPut, "x", "2", true, 5, 7},
      {OpKind::kGet, "x", "2", true, 8, 9},
  };
  CHECK(check(h).empty());
}

TEST(lin_checker, accepts_concurrent_reordering) {
  using namespace raftkv::lin;
  // put x=1 and put x=2 overlap; a later read sees 2 -> linearizable as (1,2)
  std::vector<Event> h = {
      {OpKind::kPut, "x", "1", true, 0, 10},
      {OpKind::kPut, "x", "2", true, 2, 12},
      {OpKind::kGet, "x", "2", true, 13, 14},
  };
  CHECK(check(h).empty());
}

TEST(lin_checker, rejects_a_stale_read) {
  using namespace raftkv::lin;
  // put x=1 fully before put x=2 fully before a read that returns 1 -> illegal
  std::vector<Event> h = {
      {OpKind::kPut, "x", "1", true, 0, 1},
      {OpKind::kPut, "x", "2", true, 2, 3},
      {OpKind::kGet, "x", "1", true, 4, 5},
  };
  CHECK(!check(h).empty());
}

TEST(lin_checker, rejects_lost_write) {
  using namespace raftkv::lin;
  std::vector<Event> h = {
      {OpKind::kPut, "x", "1", true, 0, 1},
      {OpKind::kGet, "x", "", false, 2, 3},  // key reported absent after a write
  };
  CHECK(!check(h).empty());
}

// ---- a real linearizable run with induced faults ----
TEST(replication_integ, history_is_linearizable_under_faults) {
  SimCluster c({1, 2, 3}, 10, 2, 99);
  NodeId l = settle(c);
  REQUIRE(l != 0u);

  std::vector<lin::Event> history;
  uint64_t clk = 1;
  std::mt19937_64 rng(2024);
  int next_val = 1;

  for (int i = 0; i < 120; ++i) {
    NodeId cur = c.leader();
    if (cur == 0) { run(c, 5); continue; }

    if (rng() % 3 == 0) {
      // a read: capture invoke, run a ReadIndex, capture the observed value
      uint64_t inv = clk++;
      // model the read as: whatever the leader's committed KV currently says,
      // after we let the cluster settle a beat
      run(c, 2);
      auto v = c.node(cur).kv.get("x");
      uint64_t resp = clk++;
      lin::Event e;
      e.kind = lin::OpKind::kGet;
      e.key = "x";
      e.observed_present = v.has_value();
      e.value = v.value_or("");
      e.invoke = inv;
      e.response = resp;
      history.push_back(e);
    } else {
      uint64_t inv = clk++;
      std::string val = std::to_string(next_val++);
      bool ok = c.propose(cur, put("x", val));
      run(c, 3);
      uint64_t resp = clk++;
      if (ok && c.node(cur).core &&
          c.node(cur).core->commit_index() >=
              c.node(cur).core->last_log_index()) {
        lin::Event e{lin::OpKind::kPut, "x", val, true, inv, resp};
        history.push_back(e);
      }
    }

    if (i % 25 == 12) {  // induce a fault
      NodeId x = 1 + (rng() % 3);
      if (c.node(x).alive) { c.crash(x); run(c, 20); c.restart(x); }
    }
    run(c, 2);
  }
  run(c, 200);

  CHECK(c.check_invariants().empty());
  std::string r = lin::check(history);
  CHECK(r.empty());
}

TINYTEST_MAIN()
