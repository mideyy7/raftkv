// Buffer + Reader: little-endian, self-describing length-prefixed encoding
// used for both WAL payloads and (from Phase 2) wire messages.
#pragma once
#include <cstdint>
#include <stdexcept>
#include <string>
#include <string_view>

#include "common/endian.h"

namespace raftkv {

class Buffer {
 public:
  void u8(uint8_t v) { put_u8(s_, v); }
  void u16(uint16_t v) { put_u16(s_, v); }
  void u32(uint32_t v) { put_u32(s_, v); }
  void u64(uint64_t v) { put_u64(s_, v); }
  void boolean(bool v) { put_u8(s_, v ? 1u : 0u); }
  // length-prefixed (u32) blob
  void bytes(std::string_view b) {
    put_u32(s_, static_cast<uint32_t>(b.size()));
    s_.append(b.data(), b.size());
  }
  void str(std::string_view b) { bytes(b); }
  // raw append, no length prefix
  void raw(std::string_view b) { s_.append(b.data(), b.size()); }

  const std::string& data() const { return s_; }
  std::string take() { return std::move(s_); }
  size_t size() const { return s_.size(); }

 private:
  std::string s_;
};

class DecodeError : public std::runtime_error {
 public:
  explicit DecodeError(const std::string& m) : std::runtime_error(m) {}
};

class Reader {
 public:
  explicit Reader(std::string_view s) : s_(s) {}

  uint8_t u8() {
    need(1);
    uint8_t v = get_u8(s_.data() + pos_);
    pos_ += 1;
    return v;
  }
  uint16_t u16() {
    need(2);
    uint16_t v = get_u16(s_.data() + pos_);
    pos_ += 2;
    return v;
  }
  uint32_t u32() {
    need(4);
    uint32_t v = get_u32(s_.data() + pos_);
    pos_ += 4;
    return v;
  }
  uint64_t u64() {
    need(8);
    uint64_t v = get_u64(s_.data() + pos_);
    pos_ += 8;
    return v;
  }
  bool boolean() { return u8() != 0; }

  std::string bytes() {
    uint32_t n = u32();
    need(n);
    std::string out(s_.substr(pos_, n));
    pos_ += n;
    return out;
  }
  std::string str() { return bytes(); }

  std::string_view rest() const { return s_.substr(pos_); }
  size_t remaining() const { return s_.size() - pos_; }
  bool empty() const { return pos_ >= s_.size(); }

 private:
  void need(size_t n) {
    if (pos_ + n > s_.size()) throw DecodeError("Reader: short buffer");
  }
  std::string_view s_;
  size_t pos_ = 0;
};

}  // namespace raftkv
