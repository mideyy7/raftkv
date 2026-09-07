#include "common/codec.h"

#include <random>
#include <string>

#include "tinytest.h"

using namespace raftkv;

TEST(codec, fixed_width_roundtrip) {
  Buffer b;
  b.u8(0x12);
  b.u16(0x3456);
  b.u32(0x789abcdeu);
  b.u64(0x0102030405060708ull);
  b.boolean(true);
  b.boolean(false);

  Reader r(b.data());
  CHECK_EQ(r.u8(), 0x12u);
  CHECK_EQ(r.u16(), 0x3456u);
  CHECK_EQ(r.u32(), 0x789abcdeu);
  CHECK_EQ(r.u64(), 0x0102030405060708ull);
  CHECK_EQ(r.boolean(), true);
  CHECK_EQ(r.boolean(), false);
  CHECK(r.empty());
}

TEST(codec, bytes_roundtrip_including_embedded_nul_and_empty) {
  std::string with_nul("ab\0cd", 5);
  Buffer b;
  b.bytes("");
  b.bytes(with_nul);
  b.str("hello");

  Reader r(b.data());
  CHECK_EQ(r.bytes(), std::string(""));
  CHECK_EQ(r.bytes(), with_nul);
  CHECK_EQ(r.str(), std::string("hello"));
  CHECK(r.empty());
}

TEST(codec, little_endian_byte_order_is_stable) {
  Buffer b;
  b.u32(0x01020304u);
  const std::string& s = b.data();
  REQUIRE(s.size() == 4);
  CHECK_EQ(static_cast<uint8_t>(s[0]), 0x04u);
  CHECK_EQ(static_cast<uint8_t>(s[1]), 0x03u);
  CHECK_EQ(static_cast<uint8_t>(s[2]), 0x02u);
  CHECK_EQ(static_cast<uint8_t>(s[3]), 0x01u);
}

TEST(codec, short_buffer_throws) {
  Buffer b;
  b.u16(1);
  Reader r(b.data());
  CHECK_THROWS(r.u32());  // only 2 bytes available
}

TEST(codec, fuzz_roundtrip) {
  std::mt19937_64 rng(tinytest::seed);
  for (int iter = 0; iter < 10000; ++iter) {
    Buffer b;
    struct Rec {
      int kind;
      uint64_t num;
      std::string blob;
    };
    std::vector<Rec> recs;
    int n = static_cast<int>(rng() % 12);
    for (int i = 0; i < n; ++i) {
      Rec rec;
      rec.kind = static_cast<int>(rng() % 5);
      rec.num = rng();
      size_t len = rng() % 40;
      rec.blob.resize(len);
      for (auto& ch : rec.blob) ch = static_cast<char>(rng() & 0xff);
      switch (rec.kind) {
        case 0: b.u8(static_cast<uint8_t>(rec.num)); break;
        case 1: b.u16(static_cast<uint16_t>(rec.num)); break;
        case 2: b.u32(static_cast<uint32_t>(rec.num)); break;
        case 3: b.u64(rec.num); break;
        case 4: b.bytes(rec.blob); break;
      }
      recs.push_back(rec);
    }
    Reader r(b.data());
    for (const auto& rec : recs) {
      switch (rec.kind) {
        case 0: CHECK_EQ(r.u8(), static_cast<uint8_t>(rec.num)); break;
        case 1: CHECK_EQ(r.u16(), static_cast<uint16_t>(rec.num)); break;
        case 2: CHECK_EQ(r.u32(), static_cast<uint32_t>(rec.num)); break;
        case 3: CHECK_EQ(r.u64(), rec.num); break;
        case 4: CHECK_EQ(r.bytes(), rec.blob); break;
      }
    }
    CHECK(r.empty());
  }
}

TINYTEST_MAIN()
