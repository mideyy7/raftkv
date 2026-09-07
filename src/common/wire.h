// Wire serialization for Raft peer messages. Framing (magic + length) is added
// by the transport; these functions handle just the payload.
#pragma once
#include <string>
#include <string_view>

#include "raft/types.h"

namespace raftkv {

constexpr uint32_t kWireMagic = 0x52414654;  // "RAFT"

std::string encode_message(const Message& m);
// Throws DecodeError on a malformed buffer.
Message decode_message(std::string_view payload);

}  // namespace raftkv
