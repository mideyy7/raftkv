// raftkv-cli -- command-line client for a raftkv cluster.
//
//   raftkv-cli --endpoints=1@127.0.0.1:7002,2@127.0.0.1:7004,... <cmd>
//
// commands:
//   put <key> <value>
//   get <key>
//   del <key>
//   bench <n>            fire n PUT then n GET, print throughput + a checksum
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <sstream>
#include <string>
#include <vector>

#include "client/raftkv_client.h"

using namespace raftkv;

static std::vector<ClientEndpoint> parse_endpoints(const std::string& spec) {
  std::vector<ClientEndpoint> out;
  std::stringstream ss(spec);
  std::string item;
  while (std::getline(ss, item, ',')) {
    if (item.empty()) continue;
    size_t at = item.find('@'), colon = item.rfind(':');
    ClientEndpoint e;
    e.id = std::strtoull(item.substr(0, at).c_str(), nullptr, 10);
    e.host = item.substr(at + 1, colon - at - 1);
    e.port = static_cast<uint16_t>(std::atoi(item.c_str() + colon + 1));
    out.push_back(e);
  }
  return out;
}

int main(int argc, char** argv) {
  std::string eps_spec;
  std::vector<std::string> pos;
  for (int i = 1; i < argc; ++i) {
    std::string a = argv[i];
    if (a.rfind("--endpoints=", 0) == 0) eps_spec = a.substr(12);
    else pos.push_back(a);
  }
  if (eps_spec.empty() || pos.empty()) {
    std::fprintf(stderr,
                 "usage: raftkv-cli --endpoints=1@h:p,2@h:p,... "
                 "<put k v | get k | del k | bench N>\n");
    return 2;
  }

  RaftKvClient cli(parse_endpoints(eps_spec));
  const std::string& cmd = pos[0];

  if (cmd == "put" && pos.size() >= 3) {
    auto r = cli.put(pos[1], pos[2]);
    std::printf("%s\n", r.ok ? "OK" : ("ERR " + r.error).c_str());
    return r.ok ? 0 : 1;
  }
  if (cmd == "del" && pos.size() >= 2) {
    auto r = cli.del(pos[1]);
    std::printf("%s\n", r.ok ? "OK" : ("ERR " + r.error).c_str());
    return r.ok ? 0 : 1;
  }
  if (cmd == "get" && pos.size() >= 2) {
    auto r = cli.get(pos[1]);
    if (!r.ok) { std::printf("ERR %s\n", r.error.c_str()); return 1; }
    std::printf("%s\n", r.found ? r.value.c_str() : "(nil)");
    return 0;
  }
  if (cmd == "bench" && pos.size() >= 2) {
    int n = std::atoi(pos[1].c_str());
    auto t0 = std::chrono::steady_clock::now();
    int wok = 0;
    for (int i = 0; i < n; ++i)
      if (cli.put("bk" + std::to_string(i), "v" + std::to_string(i)).ok) ++wok;
    auto t1 = std::chrono::steady_clock::now();
    int rok = 0;
    long long sum = 0;
    for (int i = 0; i < n; ++i) {
      auto r = cli.get("bk" + std::to_string(i));
      if (r.ok && r.found) { ++rok; sum += std::atoll(r.value.c_str() + 1); }
    }
    auto t2 = std::chrono::steady_clock::now();
    double ws = std::chrono::duration<double>(t1 - t0).count();
    double rs = std::chrono::duration<double>(t2 - t1).count();
    std::printf("writes: %d/%d ok in %.2fs (%.0f/s)\n", wok, n, ws, wok / ws);
    std::printf("reads : %d/%d ok in %.2fs (%.0f/s)  checksum=%lld\n", rok, n, rs,
                rok / rs, sum);
    return (wok == n && rok == n) ? 0 : 1;
  }

  std::fprintf(stderr, "bad command\n");
  return 2;
}
