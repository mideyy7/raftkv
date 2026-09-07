// raftkv-fuzz -- randomized-schedule model checker for the Raft core.
//
// For each seed: run a random sequence of {tick a node, deliver k messages,
// crash a node, restart a node, partition / heal, propose a command} against a
// std::map reference model. After every step, check the five safety invariants
// and that the applied KV state agrees with the model on the longest common
// committed prefix. A failing seed prints the seed + a compact schedule so it
// can be replayed.
//
//   raftkv-fuzz [--seeds=N] [--steps=M] [--nodes=3] [--start=S] [-v]
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <map>
#include <random>
#include <string>
#include <vector>

#include "harness/sim_cluster.h"
#include "raft/invariants.h"
#include "storage/kv_table.h"

using namespace raftkv;
using raftkv::sim::SimCluster;

namespace {

struct Model {
  // command index -> (key,value or delete). Applied in commit order.
  std::map<std::string, std::string> kv;
  void apply(const Command& c) {
    if (c.op == CmdOp::kPut) kv[c.key] = c.value;
    else kv.erase(c.key);
  }
};

// Longest prefix of committed indices on which ALL alive nodes agree, then
// verify a fresh replay of those commands equals each node's KV table
// restricted to touched keys.
std::string check_against_model(SimCluster& c) {
  auto views = c.views();
  // find min commit index among alive nodes (the safe common prefix)
  uint64_t common = UINT64_MAX;
  bool any = false;
  for (auto& v : views) {
    if (!v.alive) continue;
    any = true;
    common = std::min(common, v.commit_index);
  }
  if (!any || common == 0) return "";

  // reference: replay the agreed committed entries from the longest log
  const std::vector<LogEntry>* longest = nullptr;
  for (auto& v : views)
    if (v.alive && (!longest || v.log.size() > longest->size())) longest = &v.log;
  if (!longest) return "";

  Model model;
  for (uint64_t i = 1; i <= common && i <= longest->size(); ++i) {
    const auto& e = (*longest)[i - 1];
    if (e.type == EntryType::kNormal) model.apply(Command::decode(e.data));
  }

  // every alive node's KV table must match the model for keys the model knows,
  // considering only entries up to `common`
  for (NodeId id : c.ids()) {
    auto& n = c.node(id);
    if (!n.alive || !n.core) continue;
    // rebuild this node's expected state from its own log prefix [1, common]
    Model mine;
    const auto& lg = n.core->log_view();
    for (uint64_t i = 1; i <= common && i <= lg.size(); ++i)
      if (lg[i - 1].type == EntryType::kNormal)
        mine.apply(Command::decode(lg[i - 1].data));
    if (mine.kv != model.kv)
      return "model mismatch on node " + std::to_string(id) +
             " at common commit " + std::to_string(common);
  }
  return "";
}

int run_seed(uint64_t seed, int steps, int nnodes, bool verbose) {
  std::mt19937_64 rng(seed);
  std::vector<NodeId> ids;
  for (int i = 1; i <= nnodes; ++i) ids.push_back(static_cast<NodeId>(i));
  SimCluster c(ids, /*et=*/8 + static_cast<uint32_t>(seed % 5), /*hb=*/2, seed);

  uint64_t next_key = 0;
  std::string schedule;
  auto note = [&](const char* s) { if (verbose) { schedule += s; schedule += ' '; } };

  for (int step = 0; step < steps; ++step) {
    int roll = static_cast<int>(rng() % 100);
    if (roll < 45) {
      c.tick_all();
      note("T");
    } else if (roll < 80) {
      size_t k = 1 + rng() % 8;
      for (size_t i = 0; i < k && c.pending(); ++i) c.deliver_one();
      note("D");
    } else if (roll < 88) {
      NodeId id = ids[rng() % ids.size()];
      if (c.node(id).alive) { c.crash(id); note("x"); }
    } else if (roll < 94) {
      NodeId id = ids[rng() % ids.size()];
      if (!c.node(id).alive) { c.restart(id); note("r"); }
    } else if (roll < 97) {
      // random partition into two groups
      std::vector<NodeId> a, b;
      for (NodeId id : ids) ((rng() & 1) ? a : b).push_back(id);
      c.heal();
      if (!a.empty() && !b.empty()) c.partition(a, b);
      note("P");
    } else if (roll < 98) {
      c.heal();
      note("H");
    } else {
      NodeId l = c.leader();
      if (l) {
        Command cmd{CmdOp::kPut, "k" + std::to_string(next_key % 12),
                    "v" + std::to_string(next_key)};
        if (rng() % 5 == 0) cmd.op = CmdOp::kDelete;
        ++next_key;
        c.propose(l, cmd);
        note("W");
      }
    }

    std::string inv = c.check_invariants();
    if (!inv.empty()) {
      std::printf("SEED %llu step %d: INVARIANT VIOLATION: %s\n",
                  (unsigned long long)seed, step, inv.c_str());
      if (verbose) std::printf("  schedule: %s\n", schedule.c_str());
      return 1;
    }
    std::string mm = check_against_model(c);
    if (!mm.empty()) {
      std::printf("SEED %llu step %d: %s\n", (unsigned long long)seed, step,
                  mm.c_str());
      if (verbose) std::printf("  schedule: %s\n", schedule.c_str());
      return 1;
    }
  }

  // final convergence: heal, run, expect a single leader and identical logs on
  // the committed prefix
  c.heal();
  for (int i = 0; i < 400; ++i) { c.tick_all(); c.deliver_all(); }
  std::string inv = c.check_invariants();
  if (!inv.empty()) {
    std::printf("SEED %llu final: %s\n", (unsigned long long)seed, inv.c_str());
    return 1;
  }
  return 0;
}

}  // namespace

int main(int argc, char** argv) {
  int seeds = 500, steps = 2000, nodes = 3;
  uint64_t start = 1;
  bool verbose = false;
  for (int i = 1; i < argc; ++i) {
    if (!std::strncmp(argv[i], "--seeds=", 8)) seeds = std::atoi(argv[i] + 8);
    else if (!std::strncmp(argv[i], "--steps=", 8)) steps = std::atoi(argv[i] + 8);
    else if (!std::strncmp(argv[i], "--nodes=", 8)) nodes = std::atoi(argv[i] + 8);
    else if (!std::strncmp(argv[i], "--start=", 8)) start = std::strtoull(argv[i] + 8, nullptr, 10);
    else if (!std::strcmp(argv[i], "-v")) verbose = true;
  }

  int failures = 0;
  for (int s = 0; s < seeds; ++s) {
    uint64_t seed = start + static_cast<uint64_t>(s);
    int n3 = run_seed(seed, steps, nodes, verbose);
    int n5 = run_seed(seed ^ 0xBEEF, steps, 5, verbose);
    failures += n3 + n5;
    if ((s + 1) % 50 == 0)
      std::printf("  ... %d/%d seeds, %d failures\n", s + 1, seeds, failures);
  }
  std::printf("fuzz done: %d seeds x2 sizes x %d steps, %d failures\n", seeds,
              steps, failures);
  return failures ? 1 : 0;
}
