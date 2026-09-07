#include "transport/tcp.h"

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>

#include "common/codec.h"
#include "common/endian.h"
#include "common/logging.h"
#include "common/wire.h"

namespace raftkv {
namespace {

bool write_all(int fd, const char* p, size_t n) {
  size_t off = 0;
  while (off < n) {
    ssize_t w = ::send(fd, p + off, n - off, 0);
    if (w < 0) {
      if (errno == EINTR) continue;
      return false;
    }
    off += static_cast<size_t>(w);
  }
  return true;
}

bool read_all(int fd, char* p, size_t n) {
  size_t off = 0;
  while (off < n) {
    ssize_t r = ::recv(fd, p + off, n - off, 0);
    if (r < 0) {
      if (errno == EINTR) continue;
      return false;
    }
    if (r == 0) return false;  // peer closed
    off += static_cast<size_t>(r);
  }
  return true;
}

void set_nodelay(int fd) {
  int one = 1;
  ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
}

}  // namespace

TcpTransport::TcpTransport(NodeId self, std::string bind_host,
                           uint16_t bind_port, std::vector<TcpPeer> peers)
    : self_(self), bind_host_(std::move(bind_host)), bind_port_(bind_port) {
  for (const auto& p : peers) {
    if (p.id == self_) continue;
    auto link = std::make_unique<PeerLink>();
    link->info = p;
    peers_[p.id] = std::move(link);
  }
}

TcpTransport::~TcpTransport() { stop(); }

void TcpTransport::set_receiver(std::function<void(Message)> on_msg) {
  recv_ = std::move(on_msg);
}

void TcpTransport::start() {
  running_ = true;

  listen_fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
  int one = 1;
  ::setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_port = htons(bind_port_);
  addr.sin_addr.s_addr = inet_addr(bind_host_.c_str());
  if (::bind(listen_fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
    LOG_ERROR("tcp", "bind %s:%u failed: %s", bind_host_.c_str(), bind_port_,
              std::strerror(errno));
    running_ = false;
    return;
  }
  ::listen(listen_fd_, 64);
  accept_thread_ = std::thread([this] { accept_loop(); });

  for (auto& [id, link] : peers_) {
    PeerLink* lp = link.get();
    lp->sender = std::thread([this, lp] { sender_loop(lp); });
  }
}

void TcpTransport::stop() {
  bool was = running_.exchange(false);
  if (!was) return;

  if (listen_fd_ >= 0) { ::shutdown(listen_fd_, SHUT_RDWR); ::close(listen_fd_); listen_fd_ = -1; }
  if (accept_thread_.joinable()) accept_thread_.join();

  for (auto& [id, link] : peers_) {
    {
      std::lock_guard<std::mutex> g(link->mu);
      link->cv.notify_all();
    }
    if (link->sender.joinable()) link->sender.join();
    if (link->fd >= 0) { ::close(link->fd); link->fd = -1; }
  }
  {
    std::lock_guard<std::mutex> g(readers_mu_);
    for (auto& t : reader_threads_)
      if (t.joinable()) t.detach();  // readers exit on recv() error
    reader_threads_.clear();
  }
}

void TcpTransport::accept_loop() {
  while (running_) {
    sockaddr_in cli{};
    socklen_t len = sizeof(cli);
    int fd = ::accept(listen_fd_, reinterpret_cast<sockaddr*>(&cli), &len);
    if (fd < 0) {
      if (!running_) break;
      if (errno == EINTR) continue;
      break;
    }
    set_nodelay(fd);
    std::lock_guard<std::mutex> g(readers_mu_);
    reader_threads_.emplace_back([this, fd] { reader_loop(fd); });
  }
}

void TcpTransport::reader_loop(int fd) {
  for (;;) {
    char hdr[8];
    if (!read_all(fd, hdr, 8)) break;
    uint32_t magic = get_u32(hdr);
    uint32_t plen = get_u32(hdr + 4);
    if (magic != kWireMagic || plen > kMaxFrame) break;
    std::string payload;
    payload.resize(plen);
    if (plen && !read_all(fd, payload.data(), plen)) break;
    if (!running_) break;
    try {
      Message m = decode_message(payload);
      if (blocked(m.from)) continue;  // simulated inbound partition
      if (recv_) recv_(std::move(m));
    } catch (const std::exception& e) {
      LOG_WARN("tcp", "decode error: %s", e.what());
      break;
    }
  }
  ::close(fd);
}

void TcpTransport::sender_loop(PeerLink* link) {
  int backoff_ms = 50;
  while (running_) {
    // ensure connected
    if (link->fd < 0) {
      int fd = ::socket(AF_INET, SOCK_STREAM, 0);
      sockaddr_in addr{};
      addr.sin_family = AF_INET;
      addr.sin_port = htons(link->info.port);
      addr.sin_addr.s_addr = inet_addr(link->info.host.c_str());
      if (::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) == 0) {
        set_nodelay(fd);
        link->fd = fd;
        backoff_ms = 50;
      } else {
        ::close(fd);
        std::unique_lock<std::mutex> lk(link->mu);
        link->cv.wait_for(lk, std::chrono::milliseconds(backoff_ms),
                          [&] { return !running_; });
        backoff_ms = std::min(backoff_ms * 2, 1000);
        continue;
      }
    }

    std::string payload;
    {
      std::unique_lock<std::mutex> lk(link->mu);
      link->cv.wait(lk, [&] { return !running_ || !link->out.empty(); });
      if (!running_) break;
      payload = std::move(link->out.front());
      link->out.pop_front();
    }

    std::string frame;
    put_u32(frame, kWireMagic);
    put_u32(frame, static_cast<uint32_t>(payload.size()));
    frame += payload;
    if (!write_all(link->fd, frame.data(), frame.size())) {
      ::close(link->fd);
      link->fd = -1;  // reconnect next loop; this message is lost (best-effort)
    }
  }
  if (link->fd >= 0) { ::close(link->fd); link->fd = -1; }
}

void TcpTransport::send(NodeId to, const Message& m) {
  auto it = peers_.find(to);
  if (it == peers_.end()) return;
  if (blocked(to)) return;
  PeerLink* link = it->second.get();
  std::string payload = encode_message(m);
  std::lock_guard<std::mutex> g(link->mu);
  if (link->out.size() >= kMaxQueue) link->out.pop_front();  // shed oldest
  link->out.push_back(std::move(payload));
  link->cv.notify_one();
}

bool TcpTransport::blocked(NodeId peer) {
  std::lock_guard<std::mutex> g(block_mu_);
  return blocked_.count(peer) > 0;
}
void TcpTransport::netblock(NodeId peer) {
  std::lock_guard<std::mutex> g(block_mu_);
  blocked_.insert(peer);
}
void TcpTransport::netunblock(NodeId peer) {
  std::lock_guard<std::mutex> g(block_mu_);
  blocked_.erase(peer);
}

}  // namespace raftkv
