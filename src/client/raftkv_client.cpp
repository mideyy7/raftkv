#include "client/raftkv_client.h"

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <chrono>
#include <cstring>
#include <thread>

namespace raftkv {
namespace {
uint64_t now_ms() {
  return static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::steady_clock::now().time_since_epoch())
          .count());
}
}  // namespace

RaftKvClient::RaftKvClient(std::vector<ClientEndpoint> endpoints,
                           ClientOptions opts)
    : eps_(std::move(endpoints)), opts_(opts) {
  std::random_device rd;
  uint64_t s = opts_.seed ? opts_.seed : ((uint64_t(rd()) << 32) ^ rd());
  rng_.seed(s);
  client_id_ = rng_() | 1;  // never 0 (0 == "no session" on the server)
}

int RaftKvClient::endpoint_index_for_id(uint64_t id) const {
  for (size_t i = 0; i < eps_.size(); ++i)
    if (eps_[i].id == id) return static_cast<int>(i);
  return -1;
}

int RaftKvClient::connect_to(const ClientEndpoint& ep) {
  int fd = ::socket(AF_INET, SOCK_STREAM, 0);
  if (fd < 0) return -1;
  // non-blocking connect with timeout
  int fl = ::fcntl(fd, F_GETFL, 0);
  ::fcntl(fd, F_SETFL, fl | O_NONBLOCK);
  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_port = htons(ep.port);
  addr.sin_addr.s_addr = inet_addr(ep.host.c_str());
  int rc = ::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
  if (rc != 0 && errno != EINPROGRESS) { ::close(fd); return -1; }
  if (rc != 0) {
    fd_set wf;
    FD_ZERO(&wf);
    FD_SET(fd, &wf);
    timeval tv{static_cast<time_t>(opts_.connect_timeout_ms / 1000),
               static_cast<suseconds_t>((opts_.connect_timeout_ms % 1000) * 1000)};
    if (::select(fd + 1, nullptr, &wf, nullptr, &tv) <= 0) { ::close(fd); return -1; }
    int err = 0;
    socklen_t len = sizeof(err);
    ::getsockopt(fd, SOL_SOCKET, SO_ERROR, &err, &len);
    if (err != 0) { ::close(fd); return -1; }
  }
  ::fcntl(fd, F_SETFL, fl);  // back to blocking
  int one = 1;
  ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
  timeval rt{static_cast<time_t>(opts_.rpc_timeout_ms / 1000),
             static_cast<suseconds_t>((opts_.rpc_timeout_ms % 1000) * 1000)};
  ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &rt, sizeof(rt));
  ::setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &rt, sizeof(rt));
  return fd;
}

bool RaftKvClient::send_line(int fd, const std::string& line) {
  std::string buf = line;
  buf.push_back('\n');
  size_t off = 0;
  while (off < buf.size()) {
    ssize_t w = ::send(fd, buf.data() + off, buf.size() - off, 0);
    if (w <= 0) return false;
    off += static_cast<size_t>(w);
  }
  return true;
}

bool RaftKvClient::recv_line(int fd, std::string& out) {
  out.clear();
  char c;
  for (;;) {
    ssize_t r = ::recv(fd, &c, 1, 0);
    if (r <= 0) return false;
    if (c == '\n') return true;
    if (c != '\r') out.push_back(c);
    if (out.size() > 1 << 20) return false;
  }
}

uint32_t RaftKvClient::backoff_delay(int attempt) {
  uint64_t d = opts_.backoff_base_ms;
  for (int i = 0; i < attempt && d < opts_.backoff_cap_ms; ++i) d *= 2;
  if (d > opts_.backoff_cap_ms) d = opts_.backoff_cap_ms;
  // full jitter in [d/2, d]
  uint64_t half = d / 2;
  uint64_t j = half + (rng_() % (d - half + 1));
  return static_cast<uint32_t>(j);
}

RaftKvClient::Reply RaftKvClient::do_request(const std::string& line,
                                             bool mutating) {
  const uint64_t deadline = now_ms() + opts_.overall_deadline_ms;
  int redirects = 0;
  int attempt = 0;

  while (now_ms() < deadline) {
    if (eps_.empty()) return {false, false, "", "no endpoints"};
    ClientEndpoint& ep = eps_[static_cast<size_t>(leader_idx_ % eps_.size())];
    int fd = connect_to(ep);
    if (fd >= 0) {
      std::string resp;
      if (send_line(fd, line) && recv_line(fd, resp)) {
        ::close(fd);
        if (resp.rfind("OK", 0) == 0) {
          Reply r;
          r.ok = true;
          r.found = true;
          if (resp.size() > 3) r.value = resp.substr(3);
          return r;
        }
        if (resp == "NIL") return {true, false, "", ""};
        if (resp.rfind("VALUE ", 0) == 0)
          return {true, true, resp.substr(6), ""};
        if (resp.rfind("REDIRECT", 0) == 0) {
          if (++redirects > static_cast<int>(opts_.max_redirects))
            return {false, false, "", "too many redirects"};
          uint64_t hint = std::strtoull(resp.c_str() + 9, nullptr, 10);
          int idx = endpoint_index_for_id(hint);
          if (idx >= 0) { leader_idx_ = idx; continue; }  // no backoff on redirect
          // unknown hint -> fall through to rotate + backoff
        }
        // RETRY or ERR -> fall through to backoff
      } else {
        ::close(fd);
      }
    }
    // rotate to the next endpoint and back off
    leader_idx_ = (leader_idx_ + 1) % static_cast<int>(eps_.size());
    uint32_t d = backoff_delay(attempt++);
    if (now_ms() + d >= deadline) break;
    std::this_thread::sleep_for(std::chrono::milliseconds(d));
  }
  return {false, false, "", mutating ? "write timed out" : "read timed out"};
}

RaftKvClient::Reply RaftKvClient::put(const std::string& key,
                                      const std::string& value) {
  uint64_t s = ++seq_;
  return do_request("PUT " + key + " " + value + " " +
                        std::to_string(client_id_) + " " + std::to_string(s),
                    true);
}

RaftKvClient::Reply RaftKvClient::del(const std::string& key) {
  uint64_t s = ++seq_;
  return do_request("DEL " + key + "  " + std::to_string(client_id_) + " " +
                        std::to_string(s),
                    true);
}

RaftKvClient::Reply RaftKvClient::get(const std::string& key) {
  return do_request("GET " + key, false);
}

}  // namespace raftkv
