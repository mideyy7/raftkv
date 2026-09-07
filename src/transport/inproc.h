// InProcTransport: a Transport implementation that delivers messages between
// RaftNode instances living in the same process, via a shared broker. Each
// transport has its own delivery thread + queue so message handoff is async
// (like the real TcpTransport) -- which is what makes it useful under TSan.
//
// Header-only; test/bench use only.
#pragma once
#include <atomic>
#include <condition_variable>
#include <deque>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <thread>

#include "transport/transport.h"

namespace raftkv {

class InProcTransport;

class InProcBroker {
 public:
  void register_node(NodeId id, InProcTransport* t) {
    std::lock_guard<std::mutex> g(mu_);
    nodes_[id] = t;
  }
  void unregister_node(NodeId id) {
    std::lock_guard<std::mutex> g(mu_);
    nodes_.erase(id);
  }
  InProcTransport* find(NodeId id) {
    std::lock_guard<std::mutex> g(mu_);
    auto it = nodes_.find(id);
    return it == nodes_.end() ? nullptr : it->second;
  }

 private:
  std::mutex mu_;
  std::map<NodeId, InProcTransport*> nodes_;
};

class InProcTransport : public Transport {
 public:
  InProcTransport(NodeId self, std::shared_ptr<InProcBroker> broker)
      : self_(self), broker_(std::move(broker)) {}
  ~InProcTransport() override { stop(); }

  void set_receiver(std::function<void(Message)> on_msg) override {
    recv_ = std::move(on_msg);
  }

  void start() override {
    running_ = true;
    broker_->register_node(self_, this);
    worker_ = std::thread([this] { run(); });
  }

  void stop() override {
    bool was = running_.exchange(false);
    if (!was) return;
    broker_->unregister_node(self_);
    { std::lock_guard<std::mutex> g(mu_); cv_.notify_all(); }
    if (worker_.joinable()) worker_.join();
  }

  void send(NodeId to, const Message& m) override {
    if (!running_) return;
    if (blocked(to)) return;
    InProcTransport* peer = broker_->find(to);
    if (!peer) return;
    peer->enqueue(m);
  }

  void netblock(NodeId peer) override {
    std::lock_guard<std::mutex> g(block_mu_);
    blocked_.insert(peer);
  }
  void netunblock(NodeId peer) override {
    std::lock_guard<std::mutex> g(block_mu_);
    blocked_.erase(peer);
  }

 private:
  void enqueue(const Message& m) {
    if (blocked(m.from)) return;  // simulated inbound partition
    std::lock_guard<std::mutex> g(mu_);
    q_.push_back(m);
    cv_.notify_one();
  }
  void run() {
    std::unique_lock<std::mutex> lk(mu_);
    while (running_) {
      cv_.wait(lk, [&] { return !running_ || !q_.empty(); });
      while (!q_.empty()) {
        Message m = std::move(q_.front());
        q_.pop_front();
        lk.unlock();
        if (recv_) recv_(std::move(m));
        lk.lock();
      }
    }
  }
  bool blocked(NodeId peer) {
    std::lock_guard<std::mutex> g(block_mu_);
    return blocked_.count(peer) > 0;
  }

  NodeId self_;
  std::shared_ptr<InProcBroker> broker_;
  std::function<void(Message)> recv_;
  std::atomic<bool> running_{false};
  std::thread worker_;
  std::mutex mu_;
  std::condition_variable cv_;
  std::deque<Message> q_;
  std::mutex block_mu_;
  std::set<NodeId> blocked_;
};

}  // namespace raftkv
