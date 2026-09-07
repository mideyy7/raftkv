// Threaded in-process cluster of real RaftNode instances wired by
// InProcTransport. Used by the Phase 2/3/4 threaded integration tests and gives
// the TSan gate real coverage (SimCluster is single-threaded).
#pragma once
#include <chrono>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "node/raft_node.h"
#include "test_util.h"
#include "transport/inproc.h"

namespace raftkv::sim {

struct NodeCluster {
  raftkv::test::TempDir dir;
  std::shared_ptr<InProcBroker> broker = std::make_shared<InProcBroker>();
  std::vector<std::unique_ptr<InProcTransport>> transports;
  std::vector<std::unique_ptr<RaftNode>> nodes;
  std::vector<NodeId> ids;

  NodeCluster(int n, uint32_t election_ms = 150, uint32_t hb_ms = 40,
              uint32_t tick_ms = 15) {
    for (int i = 1; i <= n; ++i) ids.push_back(static_cast<NodeId>(i));
    for (NodeId id : ids) {
      NodeConfig cfg;
      cfg.id = id;
      cfg.data_dir = dir.file("n" + std::to_string(id));
      for (NodeId p : ids) cfg.peers.push_back(PeerAddr{p, "inproc", 0});
      cfg.election_timeout_ms = election_ms;
      cfg.heartbeat_ms = hb_ms;
      cfg.tick_ms = tick_ms;
      auto t = std::make_unique<InProcTransport>(id, broker);
      auto node = std::make_unique<RaftNode>(cfg, t.get());
      transports.push_back(std::move(t));
      nodes.push_back(std::move(node));
    }
  }
  ~NodeCluster() { for (auto& n : nodes) n->stop(); }

  void start() { for (auto& n : nodes) n->start(); }
  RaftNode& node(NodeId id) { return *nodes[id - 1]; }

  NodeId leader() {
    NodeId lead = 0;
    int count = 0;
    uint64_t lt = 0;
    for (NodeId id : ids) {
      auto s = node(id).status();
      if (s.role == Role::kLeader) { ++count; lead = id; lt = s.term; }
    }
    if (count != 1) return 0;
    for (NodeId id : ids) {
      if (id == lead) continue;
      auto s = node(id).status();
      if (s.term == lt && s.leader == lead) return lead;
    }
    return 0;
  }
  NodeId any_follower(NodeId not_this) {
    for (NodeId id : ids)
      if (id != not_this && node(id).status().role == Role::kFollower) return id;
    return 0;
  }

  NodeId wait_leader(int timeout_ms = 4000) {
    auto deadline = std::chrono::steady_clock::now() +
                    std::chrono::milliseconds(timeout_ms);
    while (std::chrono::steady_clock::now() < deadline) {
      NodeId l = leader();
      if (l) return l;
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    return 0;
  }
};

}  // namespace raftkv::sim
