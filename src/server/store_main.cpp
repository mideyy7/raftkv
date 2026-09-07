// raftkv-store -- the Phase 1 single-node durable KV CLI.
//
// Reads line commands from stdin:
//   put <key> <value>     -> OK <value>
//   get <key>             -> VALUE <value> | NIL
//   del <key>             -> OK <prev>
//   keys                  -> list of keys
//   size                  -> count + log bytes
//   exit                  -> quit
//
// Every put/del is fsync'd to the WAL before its OK is printed, so a `kill -9`
// after an OK loses nothing. Restart with the same --wal to recover.
#include <sys/stat.h>

#include <iostream>
#include <sstream>
#include <string>

#include "storage/store_engine.h"

using namespace raftkv;

static void ensure_parent_dir(const std::string& path) {
  size_t slash = path.find_last_of('/');
  if (slash == std::string::npos) return;
  std::string dir = path.substr(0, slash);
  if (!dir.empty()) ::mkdir(dir.c_str(), 0755);
}

int main(int argc, char** argv) {
  std::string wal = "./data/store.wal";
  for (int i = 1; i < argc; ++i) {
    std::string a = argv[i];
    if (a.rfind("--wal=", 0) == 0) wal = a.substr(6);
  }
  ensure_parent_dir(wal);

  auto er = StoreEngine::open(wal);
  if (!er) {
    std::cerr << "open failed: " << er.message() << "\n";
    return 1;
  }
  auto& engine = *er.value();
  std::cerr << "raftkv-store ready (wal=" << wal
            << ", last_index=" << engine.last_index() << ")\n";

  std::string line;
  while (std::getline(std::cin, line)) {
    std::istringstream is(line);
    std::string op;
    if (!(is >> op)) continue;

    if (op == "put") {
      std::string k, v;
      if (!(is >> k)) { std::cout << "ERR usage: put <k> <v>\n"; continue; }
      std::getline(is, v);
      if (!v.empty() && v[0] == ' ') v.erase(0, 1);
      auto r = engine.execute(Command{CmdOp::kPut, k, v});
      if (!r) { std::cout << "ERR " << r.message() << "\n"; continue; }
      std::cout << "OK " << r.value() << "\n";
    } else if (op == "del") {
      std::string k;
      if (!(is >> k)) { std::cout << "ERR usage: del <k>\n"; continue; }
      auto r = engine.execute(Command{CmdOp::kDelete, k, ""});
      if (!r) { std::cout << "ERR " << r.message() << "\n"; continue; }
      std::cout << "OK " << r.value() << "\n";
    } else if (op == "get") {
      std::string k;
      if (!(is >> k)) { std::cout << "ERR usage: get <k>\n"; continue; }
      auto v = engine.get(k);
      std::cout << (v ? "VALUE " + *v : std::string("NIL")) << "\n";
    } else if (op == "keys") {
      for (const auto& [k, _] : engine.table().snapshot_map())
        std::cout << k << "\n";
      std::cout << "END\n";
    } else if (op == "size") {
      std::cout << "SIZE " << engine.key_count() << " log_bytes "
                << engine.log_bytes() << "\n";
    } else if (op == "exit" || op == "quit") {
      break;
    } else {
      std::cout << "ERR unknown op: " << op << "\n";
    }
    std::cout.flush();
  }
  return 0;
}
