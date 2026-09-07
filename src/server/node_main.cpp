// raftkv-node -- one node of the Raft cluster.
//
// Peer traffic: TcpTransport on --listen host:port.
// Control/client traffic: a line protocol on --client-port (default listen+1):
//   STATUS                       -> "role=<r> term=<t> leader=<id> commit=<c>
//                                    applied=<a> last=<li>"
//   NETBLOCK <peerId>            -> "OK"     (drop traffic to/from peer)
//   NETUNBLOCK <peerId>         -> "OK"
//   PUT <key> <value> [cid seq] -> "OK <value>" | "REDIRECT <leader>" | "RETRY"
//   GET <key>                    -> "VALUE <v>" | "NIL" | "REDIRECT.." | "RETRY"
//   DEL <key> [cid seq]         -> "OK <prev>" | "REDIRECT.." | "RETRY"
//   QUIT                         -> closes the connection
//
// SIGINT/SIGTERM -> graceful stop. SIGKILL is the crash-test path.
#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <signal.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <cctype>
#include <cerrno>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include "common/config.h"
#include "node/raft_node.h"
#include "transport/tcp.h"

using namespace raftkv;

static std::atomic<bool> g_stop{false};
static void on_signal(int) { g_stop = true; }

static bool write_all_str(int fd, const std::string& s) {
  size_t off = 0;
  while (off < s.size()) {
    ssize_t w = ::send(fd, s.data() + off, s.size() - off, 0);
    if (w < 0) { if (errno == EINTR) continue; return false; }
    off += static_cast<size_t>(w);
  }
  return true;
}

static std::string handle_line(RaftNode& node, const std::string& line) {
  std::istringstream is(line);
  std::string op;
  is >> op;
  for (auto& ch : op) ch = static_cast<char>(toupper(ch));

  if (op == "STATUS") {
    auto s = node.status();
    char buf[256];
    std::snprintf(buf, sizeof(buf),
                  "role=%s term=%llu leader=%llu commit=%llu applied=%llu last=%llu",
                  role_name(s.role), (unsigned long long)s.term,
                  (unsigned long long)s.leader, (unsigned long long)s.commit_index,
                  (unsigned long long)s.last_applied,
                  (unsigned long long)s.last_log_index);
    return buf;
  }
  if (op == "NETBLOCK" || op == "NETUNBLOCK") {
    uint64_t pid = 0;
    is >> pid;
    if (op == "NETBLOCK") node.transport()->netblock(pid);
    else node.transport()->netunblock(pid);
    return "OK";
  }
  if (op == "PUT" || op == "DEL") {
    std::string key, value;
    is >> key;
    uint64_t cid = 0, seq = 0;
    if (op == "PUT") {
      is >> value;
    }
    is >> cid >> seq;
    Command cmd{op == "PUT" ? CmdOp::kPut : CmdOp::kDelete, key, value};
    auto r = node.client_write(cid, seq, cmd);
    switch (r.status) {
      case RaftNode::ClientStatus::kOk: return "OK " + r.value;
      case RaftNode::ClientStatus::kRedirect:
        return "REDIRECT " + std::to_string(r.leader_hint);
      case RaftNode::ClientStatus::kRetry: return "RETRY";
    }
  }
  if (op == "GET") {
    std::string key;
    is >> key;
    auto r = node.client_read(key);
    switch (r.status) {
      case RaftNode::ClientStatus::kOk: return r.found ? "VALUE " + r.value : "NIL";
      case RaftNode::ClientStatus::kRedirect:
        return "REDIRECT " + std::to_string(r.leader_hint);
      case RaftNode::ClientStatus::kRetry: return "RETRY";
    }
  }
  return "ERR unknown";
}

static void client_conn(RaftNode* node, int fd) {
  std::string buf;
  char tmp[1024];
  for (;;) {
    ssize_t n = ::recv(fd, tmp, sizeof(tmp), 0);
    if (n <= 0) break;
    buf.append(tmp, static_cast<size_t>(n));
    size_t nl;
    while ((nl = buf.find('\n')) != std::string::npos) {
      std::string line = buf.substr(0, nl);
      buf.erase(0, nl + 1);
      if (!line.empty() && line.back() == '\r') line.pop_back();
      std::string upper = line;
      for (auto& c : upper) c = static_cast<char>(toupper(c));
      if (upper == "QUIT") { ::close(fd); return; }
      std::string resp = handle_line(*node, line);
      resp.push_back('\n');
      if (!write_all_str(fd, resp)) { ::close(fd); return; }
    }
  }
  ::close(fd);
}

static void client_server(RaftNode* node, const std::string& host, uint16_t port) {
  int lfd = ::socket(AF_INET, SOCK_STREAM, 0);
  int one = 1;
  ::setsockopt(lfd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_port = htons(port);
  addr.sin_addr.s_addr = inet_addr(host.c_str());
  if (::bind(lfd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
    std::fprintf(stderr, "client bind %s:%u failed: %s\n", host.c_str(), port,
                 std::strerror(errno));
    return;
  }
  ::listen(lfd, 64);
  std::vector<std::thread> conns;
  while (!g_stop) {
    sockaddr_in cli{};
    socklen_t len = sizeof(cli);
    int cfd = ::accept(lfd, reinterpret_cast<sockaddr*>(&cli), &len);
    if (cfd < 0) { if (g_stop) break; continue; }
    int no = 1;
    ::setsockopt(cfd, IPPROTO_TCP, TCP_NODELAY, &no, sizeof(no));
    conns.emplace_back([node, cfd] { client_conn(node, cfd); });
    conns.back().detach();
  }
  ::close(lfd);
}

int main(int argc, char** argv) {
  NodeConfig cfg = parse_config(argc, argv);
  uint16_t client_port = cfg.listen_port + 1;
  for (int i = 1; i < argc; ++i) {
    std::string a = argv[i];
    if (a.rfind("--client-port=", 0) == 0)
      client_port = static_cast<uint16_t>(std::atoi(a.c_str() + 14));
  }
  if (cfg.id == 0 || cfg.listen_port == 0 || cfg.peers.empty()) {
    std::fprintf(stderr,
                 "usage: raftkv-node --id=N --listen_port=P "
                 "--peers=1@host:p,2@host:p,... --data_dir=DIR\n");
    return 2;
  }

  ::signal(SIGINT, on_signal);
  ::signal(SIGTERM, on_signal);
  ::signal(SIGPIPE, SIG_IGN);

  std::vector<TcpPeer> tpeers;
  for (const auto& p : cfg.peers)
    tpeers.push_back(TcpPeer{p.id, p.host, p.port});

  TcpTransport transport(cfg.id, cfg.listen_host, cfg.listen_port, tpeers);
  RaftNode node(cfg, &transport);
  node.start();
  std::fprintf(stderr, "raftkv-node %llu up (raft %s:%u, client :%u, dir %s)\n",
               (unsigned long long)cfg.id, cfg.listen_host.c_str(),
               cfg.listen_port, client_port, cfg.data_dir.c_str());

  std::thread csrv([&] { client_server(&node, cfg.listen_host, client_port); });

  while (!g_stop) std::this_thread::sleep_for(std::chrono::milliseconds(50));

  std::fprintf(stderr, "raftkv-node %llu stopping\n", (unsigned long long)cfg.id);
  node.stop();
  // nudge the client accept loop
  { int f = ::socket(AF_INET, SOCK_STREAM, 0);
    sockaddr_in a{}; a.sin_family = AF_INET; a.sin_port = htons(client_port);
    a.sin_addr.s_addr = inet_addr(cfg.listen_host.c_str());
    ::connect(f, reinterpret_cast<sockaddr*>(&a), sizeof(a)); ::close(f); }
  if (csrv.joinable()) csrv.join();
  return 0;
}
