#include "storage/kv_table.h"

#include <string>

#include "tinytest.h"

using namespace raftkv;

TEST(kv_table, put_get_overwrite_delete) {
  KvTable t;
  CHECK(!t.get("a").has_value());

  CHECK_EQ(t.apply({CmdOp::kPut, "a", "1"}), std::string("1"));
  CHECK_EQ(*t.get("a"), std::string("1"));

  CHECK_EQ(t.apply({CmdOp::kPut, "a", "2"}), std::string("2"));
  CHECK_EQ(*t.get("a"), std::string("2"));
  CHECK_EQ(t.size(), 1u);

  CHECK_EQ(t.apply({CmdOp::kDelete, "a", ""}), std::string("2"));  // prev value
  CHECK(!t.get("a").has_value());
  CHECK_EQ(t.size(), 0u);

  // delete of a missing key is a no-op returning ""
  CHECK_EQ(t.apply({CmdOp::kDelete, "ghost", ""}), std::string(""));
}

TEST(kv_table, command_encode_decode_roundtrip) {
  Command a{CmdOp::kPut, "key with spaces", std::string("v\0v", 3)};
  Command b = Command::decode(a.encode());
  CHECK_EQ(static_cast<int>(b.op), static_cast<int>(a.op));
  CHECK_EQ(b.key, a.key);
  CHECK_EQ(b.value, a.value);

  Command d{CmdOp::kDelete, "k", ""};
  Command d2 = Command::decode(d.encode());
  CHECK_EQ(static_cast<int>(d2.op), static_cast<int>(CmdOp::kDelete));
  CHECK_EQ(d2.key, std::string("k"));
}

TEST(kv_table, empty_key_and_empty_value_are_valid) {
  KvTable t;
  CHECK_EQ(t.apply({CmdOp::kPut, "", ""}), std::string(""));
  CHECK(t.get("").has_value());
  CHECK_EQ(*t.get(""), std::string(""));
}

TINYTEST_MAIN()
