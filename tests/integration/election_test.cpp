// Phase 2 integration: full cluster over the deterministic SimCluster harness.
#include "harness/sim_cluster.h"
#include "tinytest.h"

using namespace raftkv;
using raftkv::sim::SimCluster;

// Run rounds of {tick_all; deliver_all} until a stable unique leader appears,
// or `max_rounds` is hit. Returns the leader id (0 on failure).
static NodeId elect(SimCluster& c, int max_rounds = 200) {
  for (int i = 0; i < max_rounds; ++i) {
    c.tick_all();
    c.deliver_all();
    NodeId l = c.leader();
    if (l != 0) {
      // confirm it stays put for a few more rounds
      bool stable = true;
      for (int k = 0; k < 5; ++k) {
        c.tick_all();
        c.deliver_all();
        if (c.leader() != l) { stable = false; break; }
      }
      if (stable) return l;
    }
  }
  return 0;
}

TEST(election_integ, converges_3) {
  SimCluster c({1, 2, 3});
  NodeId l = elect(c);
  CHECK_NE(l, 0u);
  // every live node agrees on term and (for followers) on the leader id
  uint64_t term = c.node(l).core->term();
  for (NodeId id : c.ids()) {
    CHECK_EQ(c.node(id).core->term(), term);
    if (id != l) {
      CHECK(!c.node(id).core->is_leader());
      CHECK_EQ(c.node(id).core->leader_id(), l);
    }
  }
  CHECK_EQ(c.leader_count_for_term(term), 1);
}

TEST(election_integ, converges_5) {
  SimCluster c({1, 2, 3, 4, 5});
  NodeId l = elect(c);
  CHECK_NE(l, 0u);
  CHECK_EQ(c.leader_count_for_term(c.node(l).core->term()), 1);
}

TEST(election_integ, reelect_after_leader_isolated) {
  SimCluster c({1, 2, 3, 4, 5});
  NodeId l1 = elect(c);
  REQUIRE(l1 != 0u);
  uint64_t t1 = c.node(l1).core->term();

  c.isolate(l1);  // network-partition the leader away from everyone
  NodeId l2 = 0;
  for (int i = 0; i < 300 && l2 == 0; ++i) {
    c.tick_all();
    c.deliver_all();
    NodeId cand = c.leader();
    if (cand != 0 && cand != l1) l2 = cand;
  }
  CHECK_NE(l2, 0u);
  CHECK(c.node(l2).core->term() > t1);

  // heal: the old leader must convert to follower at the new higher term
  c.heal();
  for (int i = 0; i < 50; ++i) { c.tick_all(); c.deliver_all(); }
  CHECK(!c.node(l1).core->is_leader());
  CHECK_EQ(c.node(l1).core->term(), c.node(l2).core->term());
  CHECK_EQ(c.leader_count_for_term(c.node(l2).core->term()), 1);
}

TEST(election_integ, minority_partition_cannot_elect) {
  SimCluster c({1, 2, 3, 4, 5});
  NodeId l1 = elect(c);
  REQUIRE(l1 != 0u);

  // Put the leader + one follower on the minority side (2 of 5).
  std::vector<NodeId> minority{l1}, majority;
  for (NodeId id : c.ids())
    if (id != l1 && minority.size() < 2) minority.push_back(id);
  for (NodeId id : c.ids())
    if (std::find(minority.begin(), minority.end(), id) == minority.end())
      majority.push_back(id);
  c.partition(minority, majority);

  // majority side elects a new leader
  NodeId l2 = 0;
  for (int i = 0; i < 300 && l2 == 0; ++i) {
    c.tick_all();
    c.deliver_all();
    for (NodeId id : majority)
      if (c.node(id).core && c.node(id).core->is_leader()) l2 = id;
  }
  CHECK_NE(l2, 0u);

  // minority side: nobody can be leader (no quorum)
  for (NodeId id : minority) {
    // even if l1 still thinks it's leader briefly, it cannot commit; but with
    // symmetric partition it will step down on its own election attempts? No --
    // an isolated leader keeps its term. The safety property is: it cannot be
    // leader for a term the majority also has a leader in.
    if (c.node(id).core && c.node(id).core->is_leader())
      CHECK(c.node(id).core->term() < c.node(l2).core->term());
  }
}

TEST(election_integ, no_spurious_elections_when_stable) {
  SimCluster c({1, 2, 3});
  NodeId l = elect(c);
  REQUIRE(l != 0u);
  uint64_t term = c.node(l).core->term();
  for (int i = 0; i < 3000; ++i) {
    c.tick_all();
    c.deliver_all();
  }
  CHECK_EQ(c.leader(), l);
  CHECK_EQ(c.node(l).core->term(), term);
}

TEST(election_integ, lossy_links_still_elect) {
  for (uint64_t seed = 1; seed <= 30; ++seed) {
    SimCluster c({1, 2, 3, 4, 5}, /*et=*/12, /*hb=*/2, seed);
    c.set_drop_prob(0.20);
    NodeId l = 0;
    for (int i = 0; i < 1500 && l == 0; ++i) {
      c.tick_all();
      c.deliver_all();
      l = c.leader();
    }
    CHECK_NE(l, 0u);  // termination under 20% loss
  }
}

TINYTEST_MAIN()
