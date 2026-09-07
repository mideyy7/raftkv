#include "storage/wal.h"

#include <fcntl.h>
#include <unistd.h>

#include <random>
#include <string>

#include "test_util.h"
#include "tinytest.h"

using namespace raftkv;
using raftkv::test::TempDir;

static std::vector<std::string> make_records(std::mt19937_64& rng, int n) {
  std::vector<std::string> out;
  for (int i = 0; i < n; ++i) {
    size_t len = rng() % 300;
    std::string s(len, 0);
    for (auto& c : s) c = static_cast<char>(rng() & 0xff);
    out.push_back(std::move(s));
  }
  return out;
}

TEST(wal, write_then_reopen_roundtrip) {
  TempDir dir;
  std::string path = dir.file("a.wal");
  std::mt19937_64 rng(tinytest::seed);
  auto recs = make_records(rng, 200);

  {
    auto w = Wal::open(path);
    REQUIRE(w.is_ok());
    for (const auto& r : recs) CHECK(w.value()->append(r).is_ok());
    CHECK(w.value()->sync().is_ok());
  }
  {
    auto w = Wal::open(path);
    REQUIRE(w.is_ok());
    const auto& got = w.value()->records();
    REQUIRE(got.size() == recs.size());
    for (size_t i = 0; i < recs.size(); ++i) CHECK_EQ(got[i], recs[i]);
  }
}

TEST(wal, missing_file_opens_empty) {
  TempDir dir;
  auto w = Wal::open(dir.file("nope.wal"));
  REQUIRE(w.is_ok());
  CHECK_EQ(w.value()->records().size(), 0u);
  CHECK_EQ(w.value()->size_bytes(), 0u);
}

TEST(wal, zero_byte_file_opens_empty) {
  TempDir dir;
  std::string path = dir.file("z.wal");
  int fd = ::open(path.c_str(), O_CREAT | O_RDWR, 0644);
  ::close(fd);
  auto w = Wal::open(path);
  REQUIRE(w.is_ok());
  CHECK_EQ(w.value()->records().size(), 0u);
}

TEST(wal, torn_tail_garbage_is_dropped_and_truncated) {
  TempDir dir;
  std::string path = dir.file("t.wal");
  {
    auto w = Wal::open(path);
    REQUIRE(w.is_ok());
    for (int i = 0; i < 10; ++i)
      CHECK(w.value()->append("record-" + std::to_string(i)).is_ok());
    CHECK(w.value()->sync().is_ok());
  }
  uint64_t clean_size = 0;
  {
    auto w = Wal::open(path);
    clean_size = w.value()->size_bytes();
  }
  // Append a bogus partial frame (a header claiming 999 bytes + only 3 bytes).
  {
    int fd = ::open(path.c_str(), O_RDWR | O_APPEND);
    REQUIRE(fd >= 0);
    char junk[11] = {(char)0xE7, 0x03, 0, 0, 1, 2, 3, 4, 'x', 'y', 'z'};
    CHECK(::write(fd, junk, sizeof(junk)) == (ssize_t)sizeof(junk));
    ::close(fd);
  }
  {
    auto w = Wal::open(path);
    REQUIRE(w.is_ok());
    REQUIRE(w.value()->records().size() == 10u);
    for (int i = 0; i < 10; ++i)
      CHECK_EQ(w.value()->records()[i], "record-" + std::to_string(i));
    // Torn tail physically truncated back to the last good frame.
    CHECK_EQ(w.value()->size_bytes(), clean_size);
  }
  {
    // And the truncation persisted: reopening again is still 10 clean records
    // and a subsequent append lands contiguously.
    auto w = Wal::open(path);
    CHECK(w.value()->append("record-10").is_ok());
    CHECK(w.value()->sync().is_ok());
  }
  {
    auto w = Wal::open(path);
    REQUIRE(w.value()->records().size() == 11u);
    CHECK_EQ(w.value()->records()[10], "record-10");
  }
}

TEST(wal, bit_flip_in_payload_truncates_at_that_record) {
  TempDir dir;
  std::string path = dir.file("f.wal");
  {
    auto w = Wal::open(path);
    for (int i = 0; i < 6; ++i)
      CHECK(w.value()->append("aaaaaaaaaa").is_ok());  // 10 bytes each
    CHECK(w.value()->sync().is_ok());
  }
  // Corrupt one byte inside the 4th record's payload.
  // frame = 8 header + 10 payload = 18 bytes; 4th record payload starts at
  // offset 3*18 + 8.
  {
    int fd = ::open(path.c_str(), O_RDWR);
    off_t off = 3 * 18 + 8 + 2;
    ::lseek(fd, off, SEEK_SET);
    char c;
    ::read(fd, &c, 1);
    c ^= 0x20;
    ::lseek(fd, off, SEEK_SET);
    ::write(fd, &c, 1);
    ::close(fd);
  }
  auto w = Wal::open(path);
  REQUIRE(w.is_ok());
  // Records 0..2 survive; record 3 (corrupt) and everything after is dropped.
  CHECK_EQ(w.value()->records().size(), 3u);
}

TEST(wal, bulk_many_records_various_sizes) {
  TempDir dir;
  std::string path = dir.file("bulk.wal");
  std::mt19937_64 rng(tinytest::seed ^ 0x9e3779b9u);
  auto recs = make_records(rng, 20000);
  {
    auto w = Wal::open(path);
    for (const auto& r : recs) CHECK(w.value()->append(r).is_ok());
    CHECK(w.value()->sync().is_ok());
  }
  auto w = Wal::open(path);
  REQUIRE(w.value()->records().size() == recs.size());
  for (size_t i = 0; i < recs.size(); i += 137) CHECK_EQ(w.value()->records()[i], recs[i]);
}

TEST(wal, rewrite_replaces_contents_atomically) {
  TempDir dir;
  std::string path = dir.file("rw.wal");
  {
    auto w = Wal::open(path);
    for (int i = 0; i < 5; ++i) CHECK(w.value()->append("old").is_ok());
    CHECK(w.value()->sync().is_ok());
  }
  std::vector<std::string> fresh = {"n0", "n1", "n2"};
  CHECK(Wal::rewrite(path, fresh).is_ok());
  auto w = Wal::open(path);
  REQUIRE(w.value()->records().size() == 3u);
  CHECK_EQ(w.value()->records()[1], "n1");
}

TINYTEST_MAIN()
