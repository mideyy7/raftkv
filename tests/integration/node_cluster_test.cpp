// Phase 2/3 threaded integration: real RaftNode instances (raft thread +
// transport threads + fsync) wired by InProcTransport. This is the test that
// gives the TSan gate teeth -- the deterministic SimCluster tests are
// single-threaded.
#include <chrono>
#include <memory>
#include <thread>
#include <vector>

#include "node/raft_node.h"
#include "test_util.h"
#include "tinytest.h"
#include "transport/inproc.h"

using namespace raftkv;
using namespace std::chrono_literals;

namespace {

struct Cluster {
  raftkv::test::TempDir dir;
  std::shared_ptr<InProcBroker> broker = std::make_shared<InProcBroker>();
  std::vector<std::unique_ptr<InProcTransport>> transports;
  std::vector<std::unique_ptr<RaftNode>> nodes;
  std::vector<NodeId> ids;

  explicit Cluster(int n) {
    for (int i = 1; i <= n; ++i) ids.push_back(static_cast<NodeId>(i));
    for (NodeId id : ids) {
      NodeConfig cfg;
      cfg.id = id;
      cfg.data_dir = dir.file("n" + std::to_string(id));
      for (NodeId p : ids) cfg.peers.push_back(PeerAddr{p, "inproc", 0});
      cfg.election_timeout_ms = 150;
      cfg.heartbeat_ms = 40;
      cfg.tick_ms = 15;
      auto t = std::make_unique<InProcTransport>(id, broker);
      auto node = std::make_unique<RaftNode>(cfg, t.get());
      transports.push_back(std::move(t));
      nodes.push_back(std::move(node));
    }
  }
  void start() { for (auto& n : nodes) n->start(); }
  ~Cluster() { for (auto& n : nodes) n->stop(); }

  RaftNode& node(NodeId id) { return *nodes[id - 1]; }

  NodeId wait_leader(int timeout_ms = 4000) {
    auto deadline = std::chrono::steady_clock::now() +
                    std::chrono::milliseconds(timeout_ms);
    while (std::chrono::steady_clock::now() < deadline) {
      NodeId lead = 0;
      int count = 0;
      uint64_t lt = 0;
      for (NodeId id : ids) {
        auto s = node(id).status();
        if (s.role == Role::kLeader) { ++count; lead = id; lt = s.term; }
      }
      if (count == 1) {
        // confirm a follower agrees
        for (NodeId id : ids) {
          if (id == lead) continue;
          auto s = node(id).status();
          if (s.term == lt && s.leader == lead) return lead;
        }
      }
      std::this_thread::sleep_for(10ms);
    }
    return 0;
  }
};

}  // namespace

TEST(node_cluster, elects_single_leader_3) {
  Cluster c(3);
  c.start();
  NodeId l = c.wait_leader();
  CHECK_NE(l, 0u);
}

TEST(node_cluster, reelects_after_leader_stop_5) {
  Cluster c(5);
  c.start();
  NodeId l1 = c.wait_leader();
  REQUIRE(l1 != 0u);
  uint64_t t1 = c.node(l1).status().term;

  c.node(l1).stop();  // graceful stop stands in for a crash here

  // a new leader among the rest, at a higher term
  NodeId l2 = 0;
  auto deadline = std::chrono::steady_clock::now() + 4s;
  while (std::chrono::steady_clock::now() < deadline && l2 == 0) {
    for (NodeId id : c.ids) {
      if (id == l1) continue;
      auto s = c.node(id).status();
      if (s.role == Role::kLeader) l2 = id;
    }
    std::this_thread::sleep_for(10ms);
  }
  CHECK_NE(l2, 0u);
  CHECK(c.node(l2).status().term > t1);
}

TEST(node_cluster, replicates_client_writes_3) {
  Cluster c(3);
  c.start();
  NodeId l = c.wait_leader();
  REQUIRE(l != 0u);

  // drive 50 writes through the leader
  for (int i = 0; i < 50; ++i) {
    Command cmd{CmdOp::kPut, "k" + std::to_string(i), "v" + std::to_string(i)};
    auto r = c.node(l).client_write(/*cid=*/7, /*seq=*/static_cast<uint64_t>(i + 1),
                                    cmd);
    REQUIRE(r.status == RaftNode::ClientStatus::kOk);
  }
  // let followers catch up
  std::this_thread::sleep_for(300ms);

  // every node applied all 50 at the same commit prefix
  for (NodeId id : c.ids) {
    auto s = c.node(id).status();
    CHECK(s.last_applied >= 50u);
  }
  // read back via the leader (ReadIndex path)
  auto rd = c.node(l).client_read("k42");
  REQUIRE(rd.status == RaftNode::ClientStatus::kOk);
  CHECK_EQ(rd.value, std::string("v42"));
}

TEST(node_cluster, exactly_once_on_duplicate_seq) {
  Cluster c(3);
  c.start();
  NodeId l = c.wait_leader();
  REQUIRE(l != 0u);

  Command incr{CmdOp::kPut, "counter", "1"};
  auto r1 = c.node(l).client_write(99, 5, incr);
  REQUIRE(r1.status == RaftNode::ClientStatus::kOk);
  // resend the SAME (client_id, seq) -> must be deduped, same result, no new
  // log entry beyond the first
  auto before = c.node(l).status().last_log_index;
  auto r2 = c.node(l).client_write(99, 5, incr);
  REQUIRE(r2.status == RaftNode::ClientStatus::kOk);
  auto after = c.node(l).status().last_log_index;
  CHECK_EQ(r1.value, r2.value);
  CHECK_EQ(before, after);  // no second proposal
}

TINYTEST_MAIN()
