#include "storage/log_store.h"

#include <string>

#include "test_util.h"
#include "tinytest.h"

using namespace raftkv;
using raftkv::test::TempDir;

static LogEntry ent(uint64_t term, uint64_t index, std::string data = "") {
  LogEntry e;
  e.term = term;
  e.index = index;
  e.type = EntryType::kNormal;
  e.data = std::move(data);
  return e;
}

TEST(log_store, append_and_query) {
  TempDir dir;
  auto ls = LogStore::open(dir.file("l.wal"));
  REQUIRE(ls.is_ok());
  auto& log = *ls.value();

  CHECK_EQ(log.first_index(), 1u);
  CHECK_EQ(log.last_index(), 0u);

  for (uint64_t i = 1; i <= 100; ++i)
    CHECK(log.append(ent(i <= 50 ? 1 : 2, i, "d" + std::to_string(i))).is_ok());

  CHECK_EQ(log.last_index(), 100u);
  CHECK_EQ(log.term_at(1), 1u);
  CHECK_EQ(log.term_at(50), 1u);
  CHECK_EQ(log.term_at(51), 2u);
  CHECK_EQ(log.term_at(100), 2u);
  CHECK_EQ(log.term_at(101), 0u);  // out of range
  CHECK_EQ(log.term_at(0), 0u);

  auto slice = log.entries(10, 15);
  REQUIRE(slice.size() == 5u);
  CHECK_EQ(slice.front().index, 10u);
  CHECK_EQ(slice.back().index, 14u);
  CHECK_EQ(slice[2].data, std::string("d12"));

  // clamping
  CHECK_EQ(log.entries(95, 200).size(), 6u);
  CHECK_EQ(log.entries(5, 5).size(), 0u);
}

TEST(log_store, non_contiguous_append_rejected) {
  TempDir dir;
  auto ls = LogStore::open(dir.file("l.wal"));
  auto& log = *ls.value();
  CHECK(log.append(ent(1, 1)).is_ok());
  CHECK(log.append(ent(1, 3)).is_error());  // skipped index 2
  CHECK_EQ(log.last_index(), 1u);
}

TEST(log_store, truncate_suffix_then_reappend) {
  TempDir dir;
  std::string path = dir.file("l.wal");
  {
    auto ls = LogStore::open(path);
    auto& log = *ls.value();
    for (uint64_t i = 1; i <= 100; ++i) CHECK(log.append(ent(1, i)).is_ok());
    CHECK(log.truncate_suffix(50).is_ok());
    CHECK_EQ(log.last_index(), 49u);
    CHECK_EQ(log.term_at(50), 0u);
    // re-append a divergent suffix at a higher term
    for (uint64_t i = 50; i <= 60; ++i) CHECK(log.append(ent(7, i)).is_ok());
    CHECK_EQ(log.last_index(), 60u);
    CHECK_EQ(log.term_at(55), 7u);
    CHECK(log.sync().is_ok());
  }
  {
    // persists across reopen (truncate marker + re-appends replayed in order)
    auto ls = LogStore::open(path);
    auto& log = *ls.value();
    CHECK_EQ(log.last_index(), 60u);
    CHECK_EQ(log.term_at(49), 1u);
    CHECK_EQ(log.term_at(50), 7u);
    CHECK_EQ(log.term_at(60), 7u);
  }
}

TEST(log_store, truncate_entire_log) {
  TempDir dir;
  auto ls = LogStore::open(dir.file("l.wal"));
  auto& log = *ls.value();
  for (uint64_t i = 1; i <= 10; ++i) CHECK(log.append(ent(1, i)).is_ok());
  CHECK(log.truncate_suffix(1).is_ok());
  CHECK_EQ(log.last_index(), 0u);
  CHECK(log.empty());
  CHECK(log.append(ent(3, 1)).is_ok());
  CHECK_EQ(log.term_at(1), 3u);
}

TEST(log_store, hard_state_last_write_wins_across_reopen) {
  TempDir dir;
  std::string path = dir.file("l.wal");
  {
    auto ls = LogStore::open(path);
    auto& log = *ls.value();
    CHECK(log.set_hard_state({1, 0, 0}).is_ok());
    CHECK(log.set_hard_state({2, 3, 0}).is_ok());
    CHECK(log.set_hard_state({5, 4, 42}).is_ok());
    CHECK(log.sync().is_ok());
  }
  auto ls = LogStore::open(path);
  HardState hs = ls.value()->hard_state();
  CHECK_EQ(hs.current_term, 5u);
  CHECK_EQ(hs.voted_for, 4u);
  CHECK_EQ(hs.commit_index, 42u);
}

TEST(log_store, entries_and_hard_state_interleaved_replay) {
  TempDir dir;
  std::string path = dir.file("l.wal");
  {
    auto ls = LogStore::open(path);
    auto& log = *ls.value();
    CHECK(log.set_hard_state({1, 1, 0}).is_ok());
    CHECK(log.append(ent(1, 1, "a")).is_ok());
    CHECK(log.append(ent(1, 2, "b")).is_ok());
    CHECK(log.set_hard_state({1, 1, 2}).is_ok());
    CHECK(log.append(ent(2, 3, "c")).is_ok());
    CHECK(log.sync().is_ok());
  }
  auto ls = LogStore::open(path);
  auto& log = *ls.value();
  CHECK_EQ(log.last_index(), 3u);
  CHECK_EQ(log.at(3).data, std::string("c"));
  CHECK_EQ(log.hard_state().commit_index, 2u);
}

TINYTEST_MAIN()
