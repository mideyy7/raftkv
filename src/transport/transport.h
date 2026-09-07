// Transport abstraction used by the RaftNode driver. Raft core never sees this;
// the driver pulls Ready.messages and hands them to send(), and feeds inbound
// messages from the receiver callback back into core.step().
#pragma once
#include <functional>

#include "raft/types.h"

namespace raftkv {

class Transport {
 public:
  virtual ~Transport() = default;

  // Register the sink for inbound messages. Called before start().
  virtual void set_receiver(std::function<void(Message)> on_msg) = 0;

  // Best-effort, connectionless-looking send. May silently drop (peer down,
  // partitioned, buffer full). Ordering to a given peer is best-effort FIFO.
  virtual void send(NodeId to, const Message& m) = 0;

  virtual void start() {}
  virtual void stop() {}

  // Debug fault-injection hooks (used by chaos tests on macOS where there is no
  // iptables). Default no-ops.
  virtual void netblock(NodeId peer) { (void)peer; }
  virtual void netunblock(NodeId peer) { (void)peer; }
};

}  // namespace raftkv
