#include "storage/session_cache.h"

#include "tinytest.h"

using namespace raftkv;

TEST(session_cache, dedup_returns_cached_result) {
  SessionCache sc;
  CHECK(!sc.lookup(7, 1).has_value());  // nothing recorded yet

  sc.record(7, 1, "v1", true);
  auto r = sc.lookup(7, 1);
  REQUIRE(r.has_value());
  CHECK_EQ(r->last_value, std::string("v1"));
  CHECK_EQ(r->last_seq, 1u);

  // a retry of seq 1 still hits the cache
  CHECK(sc.lookup(7, 1).has_value());
  // seq 2 is new -> not cached
  CHECK(!sc.lookup(7, 2).has_value());
}

TEST(session_cache, older_or_equal_seq_is_covered_by_newer_entry) {
  SessionCache sc;
  sc.record(7, 5, "v5", true);
  // any seq <= 5 is considered already applied
  CHECK(sc.lookup(7, 3).has_value());
  CHECK(sc.lookup(7, 5).has_value());
  CHECK(!sc.lookup(7, 6).has_value());
}

TEST(session_cache, record_ignores_stale_seq) {
  SessionCache sc;
  sc.record(7, 5, "v5", true);
  sc.record(7, 3, "v3", true);  // stale -> ignored
  CHECK_EQ(sc.lookup(7, 5)->last_value, std::string("v5"));
}

TEST(session_cache, independent_per_client) {
  SessionCache sc;
  sc.record(1, 1, "a", true);
  sc.record(2, 1, "b", true);
  CHECK_EQ(sc.lookup(1, 1)->last_value, std::string("a"));
  CHECK_EQ(sc.lookup(2, 1)->last_value, std::string("b"));
  CHECK_EQ(sc.clients(), 2u);
}

TINYTEST_MAIN()
