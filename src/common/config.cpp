#include "common/config.h"

#include <cstdlib>
#include <fstream>
#include <sstream>
#include <stdexcept>

namespace raftkv {
namespace {

std::string trim(std::string s) {
  size_t a = s.find_first_not_of(" \t\r\n");
  size_t b = s.find_last_not_of(" \t\r\n");
  if (a == std::string::npos) return "";
  return s.substr(a, b - a + 1);
}

// "id@host:port,id@host:port,..."
std::vector<PeerAddr> parse_peers(const std::string& spec) {
  std::vector<PeerAddr> out;
  std::stringstream ss(spec);
  std::string item;
  while (std::getline(ss, item, ',')) {
    item = trim(item);
    if (item.empty()) continue;
    PeerAddr p;
    size_t at = item.find('@');
    size_t colon = item.rfind(':');
    if (at == std::string::npos || colon == std::string::npos || colon < at)
      throw std::runtime_error("bad peer spec: " + item);
    p.id = std::strtoull(item.substr(0, at).c_str(), nullptr, 10);
    p.host = item.substr(at + 1, colon - at - 1);
    p.port = static_cast<uint16_t>(std::atoi(item.substr(colon + 1).c_str()));
    out.push_back(p);
  }
  return out;
}

void apply_kv(NodeConfig& c, const std::string& key, const std::string& val) {
  if (key == "id") c.id = std::strtoull(val.c_str(), nullptr, 10);
  else if (key == "listen_host") c.listen_host = val;
  else if (key == "listen_port") c.listen_port = static_cast<uint16_t>(std::atoi(val.c_str()));
  else if (key == "peers") c.peers = parse_peers(val);
  else if (key == "data_dir") c.data_dir = val;
  else if (key == "election_timeout_ms") c.election_timeout_ms = static_cast<uint32_t>(std::atoi(val.c_str()));
  else if (key == "heartbeat_ms") c.heartbeat_ms = static_cast<uint32_t>(std::atoi(val.c_str()));
  else if (key == "tick_ms") c.tick_ms = static_cast<uint32_t>(std::atoi(val.c_str()));
  else if (key == "snapshot_threshold") c.snapshot_threshold = std::strtoull(val.c_str(), nullptr, 10);
  else if (key == "seed") c.seed = std::strtoull(val.c_str(), nullptr, 10);
  // unknown keys ignored on purpose
}

}  // namespace

std::vector<uint64_t> NodeConfig::peer_ids() const {
  std::vector<uint64_t> v;
  v.reserve(peers.size());
  for (const auto& p : peers) v.push_back(p.id);
  return v;
}

const PeerAddr* NodeConfig::find_peer(uint64_t pid) const {
  for (const auto& p : peers)
    if (p.id == pid) return &p;
  return nullptr;
}

NodeConfig parse_config_file(const std::string& path) {
  NodeConfig c;
  std::ifstream in(path);
  if (!in) throw std::runtime_error("cannot open config file: " + path);
  std::string line;
  while (std::getline(in, line)) {
    size_t hash = line.find('#');
    if (hash != std::string::npos) line = line.substr(0, hash);
    line = trim(line);
    if (line.empty()) continue;
    size_t eq = line.find('=');
    if (eq == std::string::npos) continue;
    apply_kv(c, trim(line.substr(0, eq)), trim(line.substr(eq + 1)));
  }
  return c;
}

NodeConfig parse_config(int argc, char** argv) {
  NodeConfig c;
  // first pass: --config
  for (int i = 1; i < argc; ++i) {
    std::string a = argv[i];
    if (a.rfind("--config=", 0) == 0) {
      c = parse_config_file(a.substr(9));
    }
  }
  // second pass: flag overrides
  for (int i = 1; i < argc; ++i) {
    std::string a = argv[i];
    if (a.rfind("--", 0) != 0) continue;
    size_t eq = a.find('=');
    if (eq == std::string::npos) continue;
    std::string key = a.substr(2, eq - 2);
    std::string val = a.substr(eq + 1);
    if (key == "config") continue;
    apply_kv(c, key, val);
  }
  return c;
}

}  // namespace raftkv
