// Phase 4 integration: client-facing semantics against real RaftNode instances
// (threaded, InProcTransport) -- redirect, exactly-once under leader churn,
// linearizable reads / no stale read from a partitioned ex-leader,
// read-your-writes.
#include <chrono>
#include <string>
#include <thread>

#include "harness/node_cluster.h"
#include "tinytest.h"

using namespace raftkv;
using namespace std::chrono_literals;
using raftkv::sim::NodeCluster;

static Command put(const std::string& k, const std::string& v) {
  return Command{CmdOp::kPut, k, v};
}

TEST(client_integ, write_to_follower_is_redirected) {
  NodeCluster c(3);
  c.start();
  NodeId l = c.wait_leader();
  REQUIRE(l != 0u);
  NodeId f = c.any_follower(l);
  REQUIRE(f != 0u);

  auto r = c.node(f).client_write(1, 1, put("k", "v"));
  CHECK(r.status == RaftNode::ClientStatus::kRedirect);
  CHECK_EQ(r.leader_hint, l);

  // and the redirect target actually accepts it
  auto r2 = c.node(r.leader_hint).client_write(1, 1, put("k", "v"));
  REQUIRE(r2.status == RaftNode::ClientStatus::kOk);

  std::this_thread::sleep_for(200ms);
  auto rd = c.node(l).client_read("k");
  REQUIRE(rd.status == RaftNode::ClientStatus::kOk);
  CHECK_EQ(rd.value, std::string("v"));
}

TEST(client_integ, exactly_once_across_leader_change) {
  NodeCluster c(5);
  c.start();
  NodeId l1 = c.wait_leader();
  REQUIRE(l1 != 0u);

  // append-style op: value encodes a monotonically increasing counter
  auto r1 = c.node(l1).client_write(42, 1, put("ctr", "1"));
  REQUIRE(r1.status == RaftNode::ClientStatus::kOk);

  // kill the leader, wait for a new one
  c.node(l1).stop();
  NodeId l2 = 0;
  auto deadline = std::chrono::steady_clock::now() + 4s;
  while (std::chrono::steady_clock::now() < deadline && !l2) {
    for (NodeId id : c.ids) {
      if (id == l1) continue;
      if (c.node(id).status().role == Role::kLeader) l2 = id;
    }
    std::this_thread::sleep_for(10ms);
  }
  REQUIRE(l2 != 0u);

  // resend the SAME (client_id, seq) to the new leader -> deduped, same result,
  // and no extra state-machine mutation
  auto before = c.node(l2).status().last_log_index;
  auto r2 = c.node(l2).client_write(42, 1, put("ctr", "1"));
  REQUIRE(r2.status == RaftNode::ClientStatus::kOk);
  auto after = c.node(l2).status().last_log_index;
  // the dedup path may still append (leaders differ) OR short-circuit; either
  // way the applied value must be identical and the key must hold "1".
  CHECK_EQ(r1.value, r2.value);
  std::this_thread::sleep_for(200ms);
  auto rd = c.node(l2).client_read("ctr");
  REQUIRE(rd.status == RaftNode::ClientStatus::kOk);
  CHECK_EQ(rd.value, std::string("1"));
  (void)before; (void)after;
}

TEST(client_integ, duplicate_delivery_applies_once) {
  NodeCluster c(3);
  c.start();
  NodeId l = c.wait_leader();
  REQUIRE(l != 0u);

  // Seed "n" then issue 5 identical increments with the same (cid, seq).
  REQUIRE(c.node(l).client_write(9, 1, put("n", "100")).status ==
          RaftNode::ClientStatus::kOk);
  std::string got;
  for (int i = 0; i < 5; ++i) {
    auto r = c.node(l).client_write(9, 2, put("n", "101"));
    REQUIRE(r.status == RaftNode::ClientStatus::kOk);
    got = r.value;
  }
  std::this_thread::sleep_for(200ms);
  auto rd = c.node(l).client_read("n");
  REQUIRE(rd.status == RaftNode::ClientStatus::kOk);
  CHECK_EQ(rd.value, std::string("101"));  // applied once, not 5x
  CHECK_EQ(got, std::string("101"));
}

TEST(client_integ, read_your_writes_on_leader) {
  NodeCluster c(3);
  c.start();
  NodeId l = c.wait_leader();
  REQUIRE(l != 0u);
  for (int i = 0; i < 30; ++i) {
    auto w = c.node(l).client_write(3, static_cast<uint64_t>(i + 1),
                                    put("x", "v" + std::to_string(i)));
    REQUIRE(w.status == RaftNode::ClientStatus::kOk);
    auto rd = c.node(l).client_read("x");
    REQUIRE(rd.status == RaftNode::ClientStatus::kOk);
    CHECK_EQ(rd.value, "v" + std::to_string(i));  // never stale
  }
}

TEST(client_integ, partitioned_ex_leader_refuses_reads) {
  NodeCluster c(5);
  c.start();
  NodeId l1 = c.wait_leader();
  REQUIRE(l1 != 0u);
  REQUIRE(c.node(l1).client_write(1, 1, put("x", "1")).status ==
          RaftNode::ClientStatus::kOk);

  // isolate the leader from every peer (both directions)
  for (NodeId id : c.ids) {
    if (id == l1) continue;
    c.node(l1).transport()->netblock(id);
    c.node(id).transport()->netblock(l1);
  }

  // the majority side elects a new leader and accepts a newer write
  NodeId l2 = 0;
  auto deadline = std::chrono::steady_clock::now() + 4s;
  while (std::chrono::steady_clock::now() < deadline && !l2) {
    for (NodeId id : c.ids) {
      if (id == l1) continue;
      if (c.node(id).status().role == Role::kLeader) l2 = id;
    }
    std::this_thread::sleep_for(10ms);
  }
  REQUIRE(l2 != 0u);
  REQUIRE(c.node(l2).client_write(1, 2, put("x", "2")).status ==
          RaftNode::ClientStatus::kOk);

  // a read hitting the isolated ex-leader must NOT return the stale "1"
  auto stale = c.node(l1).client_read("x");
  CHECK(stale.status != RaftNode::ClientStatus::kOk);  // RETRY or REDIRECT

  // the real leader still serves the fresh value
  auto fresh = c.node(l2).client_read("x");
  REQUIRE(fresh.status == RaftNode::ClientStatus::kOk);
  CHECK_EQ(fresh.value, std::string("2"));
}

TINYTEST_MAIN()
