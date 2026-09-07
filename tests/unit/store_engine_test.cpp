#include "storage/store_engine.h"

#include <map>
#include <random>
#include <string>

#include "test_util.h"
#include "tinytest.h"

using namespace raftkv;
using raftkv::test::TempDir;

TEST(store_engine, basic_execute_and_get) {
  TempDir dir;
  auto e = StoreEngine::open(dir.file("s.wal"));
  REQUIRE(e.is_ok());
  auto& eng = *e.value();
  CHECK_EQ(eng.execute({CmdOp::kPut, "x", "1"}).value(), std::string("1"));
  CHECK_EQ(*eng.get("x"), std::string("1"));
  CHECK_EQ(eng.execute({CmdOp::kDelete, "x", ""}).value(), std::string("1"));
  CHECK(!eng.get("x").has_value());
}

TEST(store_engine, durability_no_graceful_close) {
  TempDir dir;
  std::string path = dir.file("s.wal");
  {
    auto e = StoreEngine::open(path);
    REQUIRE(e.is_ok());
    for (int i = 0; i < 1000; ++i) {
      auto r = e.value()->execute(
          {CmdOp::kPut, "k" + std::to_string(i), "v" + std::to_string(i)});
      REQUIRE(r.is_ok());
    }
    // NOTE: no explicit flush here beyond per-op fsync inside execute().
    // The engine object is destroyed at end of scope -- simulating a process
    // that exits right after the last ack.
  }
  {
    auto e = StoreEngine::open(path);
    REQUIRE(e.is_ok());
    CHECK_EQ(e.value()->key_count(), 1000u);
    for (int i = 0; i < 1000; ++i)
      CHECK_EQ(*e.value()->get("k" + std::to_string(i)),
               "v" + std::to_string(i));
  }
}

TEST(store_engine, model_check_with_periodic_reopen) {
  TempDir dir;
  std::string path = dir.file("s.wal");
  std::mt19937_64 rng(tinytest::seed);
  std::map<std::string, std::string> model;

  // kOs: this test checks replay/apply logic, not power-cut durability
  // (that is covered by durability_no_graceful_close). Avoids 20k F_FULLFSYNCs.
  auto e = StoreEngine::open(path, SyncMode::kOs);
  REQUIRE(e.is_ok());
  std::unique_ptr<StoreEngine> eng = std::move(e.value());

  const int kOps = 20000;
  const int kKeyspace = 200;
  for (int i = 0; i < kOps; ++i) {
    std::string key = "k" + std::to_string(rng() % kKeyspace);
    if (rng() % 4 == 0) {
      eng->execute({CmdOp::kDelete, key, ""});
      model.erase(key);
    } else {
      std::string val = "v" + std::to_string(rng());
      eng->execute({CmdOp::kPut, key, val});
      model[key] = val;
    }

    // Verify full agreement every op.
    for (int k = 0; k < kKeyspace; ++k) {
      std::string kk = "k" + std::to_string(k);
      auto got = eng->get(kk);
      auto mit = model.find(kk);
      if (mit == model.end()) {
        CHECK(!got.has_value());
      } else {
        REQUIRE(got.has_value());
        CHECK_EQ(*got, mit->second);
      }
    }

    if (i % 1000 == 999) {
      eng.reset();  // drop the object
      auto re = StoreEngine::open(path, SyncMode::kOs);
      REQUIRE(re.is_ok());
      eng = std::move(re.value());
    }
  }
  CHECK_EQ(eng->key_count(), model.size());
}

TINYTEST_MAIN()
