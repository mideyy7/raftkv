// raftkv-loadgen -- concurrent PUT/GET load generator with per-request timing.
//
//   raftkv-loadgen --endpoints=1@h:p,... [options]
//     -c N              concurrent client connections/threads (default 16)
//     -d SECS           run duration (default 20)
//     -r RATE           aggregate target ops/sec (0 = closed loop, default 0)
//     --mix put=90,get=10   op mix percentages (default put=100)
//     --keyspace K      distinct keys (default 10000)
//     --warmup SECS     leading seconds excluded from the steady window (5)
//     --value-bytes N   payload size (default 16)
//     --out FILE        CSV: id,worker,op,t_send_ns,t_recv_ns,status
//     --seed S
//
// Latency is measured as the whole client-observed op time INCLUDING the
// client's own redirect/retry/backoff -- that is the user-visible number and
// is what makes the leader-failure window show up honestly.
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <mutex>
#include <random>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include "client/raftkv_client.h"

using namespace raftkv;
using clk = std::chrono::steady_clock;

static uint64_t ns_now() {
  return static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::nanoseconds>(
          clk::now().time_since_epoch())
          .count());
}

struct Rec {
  uint64_t id, t_send, t_recv;
  int worker;
  char op;
  bool ok;
};

static std::vector<ClientEndpoint> parse_eps(const std::string& s) {
  std::vector<ClientEndpoint> out;
  std::stringstream ss(s);
  std::string it;
  while (std::getline(ss, it, ',')) {
    if (it.empty()) continue;
    size_t at = it.find('@'), c = it.rfind(':');
    ClientEndpoint e;
    e.id = std::strtoull(it.substr(0, at).c_str(), nullptr, 10);
    e.host = it.substr(at + 1, c - at - 1);
    e.port = static_cast<uint16_t>(std::atoi(it.c_str() + c + 1));
    out.push_back(e);
  }
  return out;
}

int main(int argc, char** argv) {
  std::string eps, out = "loadgen.csv", mix = "put=100";
  int conns = 16, dur = 20, keyspace = 10000, warmup = 5, vbytes = 16;
  double rate = 0;
  uint64_t seed = 12345;
  for (int i = 1; i < argc; ++i) {
    std::string a = argv[i];
    auto val = [&](const char* p) { return a.substr(std::strlen(p)); };
    if (a.rfind("--endpoints=", 0) == 0) eps = val("--endpoints=");
    else if (a == "-c") conns = std::atoi(argv[++i]);
    else if (a == "-d") dur = std::atoi(argv[++i]);
    else if (a == "-r") rate = std::atof(argv[++i]);
    else if (a.rfind("--mix=", 0) == 0) mix = val("--mix=");
    else if (a.rfind("--keyspace=", 0) == 0) keyspace = std::atoi(val("--keyspace=").c_str());
    else if (a.rfind("--warmup=", 0) == 0) warmup = std::atoi(val("--warmup=").c_str());
    else if (a.rfind("--value-bytes=", 0) == 0) vbytes = std::atoi(val("--value-bytes=").c_str());
    else if (a.rfind("--out=", 0) == 0) out = val("--out=");
    else if (a.rfind("--seed=", 0) == 0) seed = std::strtoull(val("--seed=").c_str(), nullptr, 10);
  }
  if (eps.empty()) { std::fprintf(stderr, "need --endpoints=\n"); return 2; }

  int put_pct = 100;
  { size_t p = mix.find("put="); if (p != std::string::npos) put_pct = std::atoi(mix.c_str() + p + 4); }

  auto endpoints = parse_eps(eps);
  std::string payload(static_cast<size_t>(std::max(1, vbytes)), 'x');

  std::atomic<bool> stop{false};
  std::atomic<uint64_t> next_id{1};
  std::vector<std::vector<Rec>> per_worker(static_cast<size_t>(conns));
  const uint64_t t0 = ns_now();
  const double per_conn_rate = rate > 0 ? rate / conns : 0;

  auto worker = [&](int wid) {
    ClientOptions o;
    o.seed = seed + static_cast<uint64_t>(wid) * 0x9E3779B9u;
    o.overall_deadline_ms = 8000;
    RaftKvClient cli(endpoints, o);
    std::mt19937_64 rng(o.seed);
    auto& recs = per_worker[static_cast<size_t>(wid)];
    recs.reserve(1 << 16);
    uint64_t issued = 0;
    const uint64_t wstart = ns_now();

    while (!stop.load(std::memory_order_relaxed)) {
      if (per_conn_rate > 0) {
        double due_s = static_cast<double>(issued) / per_conn_rate;
        uint64_t due = wstart + static_cast<uint64_t>(due_s * 1e9);
        uint64_t now = ns_now();
        if (now < due) {
          std::this_thread::sleep_for(std::chrono::nanoseconds(due - now));
        }
      }
      bool is_put = (static_cast<int>(rng() % 100) < put_pct);
      std::string key = "k" + std::to_string(rng() % static_cast<uint64_t>(keyspace));
      uint64_t id = next_id.fetch_add(1, std::memory_order_relaxed);
      uint64_t ts = ns_now();
      RaftKvClient::Reply r =
          is_put ? cli.put(key, payload) : cli.get(key);
      uint64_t tr = ns_now();
      recs.push_back(Rec{id, ts, tr, wid, is_put ? 'P' : 'G', r.ok});
      ++issued;
    }
  };

  std::vector<std::thread> threads;
  for (int i = 0; i < conns; ++i) threads.emplace_back(worker, i);
  std::this_thread::sleep_for(std::chrono::seconds(dur));
  stop.store(true);
  for (auto& t : threads) t.join();

  // write CSV
  FILE* f = std::fopen(out.c_str(), "w");
  std::fprintf(f, "# t0_ns=%llu warmup_s=%d duration_s=%d conns=%d rate=%.0f "
                  "keyspace=%d put_pct=%d seed=%llu\n",
               (unsigned long long)t0, warmup, dur, conns, rate, keyspace,
               put_pct, (unsigned long long)seed);
  std::fprintf(f, "id,worker,op,t_send_ns,t_recv_ns,status\n");
  uint64_t n = 0, nok = 0;
  for (auto& v : per_worker)
    for (auto& r : v) {
      std::fprintf(f, "%llu,%d,%c,%llu,%llu,%s\n", (unsigned long long)r.id,
                   r.worker, r.op, (unsigned long long)r.t_send,
                   (unsigned long long)r.t_recv, r.ok ? "ok" : "fail");
      ++n; nok += r.ok ? 1 : 0;
    }
  std::fclose(f);

  double wall = (ns_now() - t0) / 1e9;
  std::printf("loadgen: %llu ops (%llu ok) in %.1fs -> %.0f ops/s overall "
              "(warmup %ds excluded by report.py)\n",
              (unsigned long long)n, (unsigned long long)nok, wall, n / wall,
              warmup);
  std::printf("wrote %s\n", out.c_str());
  return 0;
}
