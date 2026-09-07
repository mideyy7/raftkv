// RaftKvClient: blocking client for a raftkv cluster.
//
// - talks the line protocol on each node's client port
// - caches the leader; follows REDIRECT; retries RETRY / timeouts / connection
//   errors with capped exponential backoff + jitter across all endpoints
// - stamps every mutating op with (client_id, seq) so a retried write is
//   applied exactly once by the cluster's SessionCache
#pragma once
#include <cstdint>
#include <random>
#include <string>
#include <vector>

namespace raftkv {

struct ClientEndpoint {
  uint64_t id = 0;
  std::string host;
  uint16_t port = 0;  // the node's CLIENT port
};

struct ClientOptions {
  uint32_t connect_timeout_ms = 1000;
  uint32_t rpc_timeout_ms = 3000;
  uint32_t backoff_base_ms = 10;
  uint32_t backoff_cap_ms = 1000;
  uint32_t overall_deadline_ms = 15000;
  uint32_t max_redirects = 8;
  uint64_t seed = 0;  // 0 -> random (client_id + jitter)
};

class RaftKvClient {
 public:
  RaftKvClient(std::vector<ClientEndpoint> endpoints, ClientOptions opts = {});
  ~RaftKvClient();
  RaftKvClient(const RaftKvClient&) = delete;
  RaftKvClient& operator=(const RaftKvClient&) = delete;

  struct Reply {
    bool ok = false;
    bool found = false;       // for GET: was the key present
    std::string value;
    std::string error;        // set when ok == false
  };

  Reply put(const std::string& key, const std::string& value);
  Reply del(const std::string& key);
  Reply get(const std::string& key);

  uint64_t client_id() const { return client_id_; }

  // capped exponential backoff with full jitter in [d/2, d]; exposed for tests
  uint32_t backoff_delay(int attempt);

 private:
  // Sends one already-formatted request line, handling redirect/retry/backoff.
  Reply do_request(const std::string& line, bool mutating);
  int connect_to(const ClientEndpoint& ep);
  bool send_line(int fd, const std::string& line);
  bool recv_line(int fd, std::string& out);
  int endpoint_index_for_id(uint64_t id) const;

  std::vector<ClientEndpoint> eps_;
  ClientOptions opts_;
  uint64_t client_id_ = 0;
  uint64_t seq_ = 0;
  int leader_idx_ = 0;
  int leader_fd_ = -1;   // cached persistent connection to eps_[leader_idx_]
  int cached_idx_ = -1;  // which endpoint leader_fd_ points at
  std::mt19937_64 rng_;

  void drop_conn();
};

}  // namespace raftkv
