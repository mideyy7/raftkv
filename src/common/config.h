// Cluster / node configuration. Parsed from CLI flags (--key=value) and/or a
// simple "key = value" text file. Used from Phase 2 on by raftkv-node.
#pragma once
#include <cstdint>
#include <string>
#include <vector>

namespace raftkv {

struct PeerAddr {
  uint64_t id = 0;
  std::string host;
  uint16_t port = 0;
};

struct NodeConfig {
  uint64_t id = 0;
  std::string listen_host = "127.0.0.1";
  uint16_t listen_port = 0;
  std::vector<PeerAddr> peers;  // includes self
  std::string data_dir = "./data";

  uint32_t election_timeout_ms = 300;  // base T; actual in [T, 2T]
  uint32_t heartbeat_ms = 75;          // <= T/4
  uint32_t tick_ms = 25;               // driver logical-tick period
  uint64_t snapshot_threshold = 0;     // 0 == disabled (stretch)
  uint64_t seed = 0;                    // election-timeout RNG seed (0 == random)

  std::vector<uint64_t> peer_ids() const;
  const PeerAddr* find_peer(uint64_t pid) const;
};

// Parse argv (flags of the form --key=value). `--config=path` loads a file
// first, then remaining flags override. Unknown flags are ignored (so the same
// argv can carry tool-specific flags).
NodeConfig parse_config(int argc, char** argv);
NodeConfig parse_config_file(const std::string& path);

}  // namespace raftkv
