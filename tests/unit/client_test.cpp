#include "client/raftkv_client.h"

#include "tinytest.h"

using namespace raftkv;

TEST(client, backoff_is_capped_exponential_with_jitter) {
  ClientOptions o;
  o.backoff_base_ms = 10;
  o.backoff_cap_ms = 1000;
  o.seed = 123;
  RaftKvClient c({{1, "127.0.0.1", 1}}, o);

  uint32_t prev_ceiling = o.backoff_base_ms;
  for (int attempt = 0; attempt < 12; ++attempt) {
    uint32_t ceiling = o.backoff_base_ms;
    for (int i = 0; i < attempt && ceiling < o.backoff_cap_ms; ++i) ceiling *= 2;
    if (ceiling > o.backoff_cap_ms) ceiling = o.backoff_cap_ms;

    for (int rep = 0; rep < 50; ++rep) {
      uint32_t d = c.backoff_delay(attempt);
      CHECK(d >= ceiling / 2);       // full jitter lower bound
      CHECK(d <= ceiling);           // never exceeds the (capped) ceiling
    }
    CHECK(ceiling >= prev_ceiling);  // monotonically grows then plateaus
    prev_ceiling = ceiling;
  }
  // plateau at the cap
  CHECK_EQ(prev_ceiling, o.backoff_cap_ms);
}

TEST(client, distinct_client_ids_and_nonzero) {
  RaftKvClient a({{1, "h", 1}});
  RaftKvClient b({{1, "h", 1}});
  CHECK(a.client_id() != 0u);
  CHECK(b.client_id() != 0u);
  CHECK(a.client_id() != b.client_id());
}

TEST(client, seeded_client_id_is_deterministic) {
  ClientOptions o;
  o.seed = 999;
  RaftKvClient a({{1, "h", 1}}, o);
  RaftKvClient b({{1, "h", 1}}, o);
  CHECK_EQ(a.client_id(), b.client_id());
}

TINYTEST_MAIN()
