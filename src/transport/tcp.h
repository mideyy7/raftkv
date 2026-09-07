// TcpTransport: real network transport for raftkv-node processes.
//
// - one listening socket; an accept thread spawns a reader thread per inbound
//   connection that decodes length-prefixed frames into Messages
// - one sender thread per peer, each owning a bounded outbound queue and a
//   lazily (re)connected socket with backoff
// - netblock()/netunblock() drop traffic to/from a peer at the app layer,
//   standing in for iptables on macOS during chaos tests
#pragma once
#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <map>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "transport/transport.h"

namespace raftkv {

struct TcpPeer {
  NodeId id;
  std::string host;
  uint16_t port;
};

class TcpTransport : public Transport {
 public:
  TcpTransport(NodeId self, std::string bind_host, uint16_t bind_port,
               std::vector<TcpPeer> peers);
  ~TcpTransport() override;

  void set_receiver(std::function<void(Message)> on_msg) override;
  void send(NodeId to, const Message& m) override;
  void start() override;
  void stop() override;
  void netblock(NodeId peer) override;
  void netunblock(NodeId peer) override;

 private:
  struct PeerLink {
    TcpPeer info;
    std::mutex mu;
    std::condition_variable cv;
    std::deque<std::string> out;  // encoded payloads
    std::thread sender;
    int fd = -1;
  };

  void accept_loop();
  void reader_loop(int fd);
  void sender_loop(PeerLink* link);
  bool blocked(NodeId peer);

  NodeId self_;
  std::string bind_host_;
  uint16_t bind_port_;
  int listen_fd_ = -1;
  std::atomic<bool> running_{false};
  std::function<void(Message)> recv_;

  std::map<NodeId, std::unique_ptr<PeerLink>> peers_;
  std::thread accept_thread_;
  std::vector<std::thread> reader_threads_;
  std::mutex readers_mu_;

  std::mutex block_mu_;
  std::set<NodeId> blocked_;

  static constexpr size_t kMaxQueue = 4096;
  static constexpr uint32_t kMaxFrame = 16u * 1024u * 1024u;
};

}  // namespace raftkv
