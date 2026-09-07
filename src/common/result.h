// Minimal Status / Result<T> for fallible operations (I/O, parsing).
#pragma once
#include <optional>
#include <string>
#include <utility>

namespace raftkv {

class Status {
 public:
  Status() = default;  // ok
  static Status ok() { return Status(); }
  static Status error(std::string msg) { return Status(std::move(msg)); }

  bool is_ok() const { return !msg_.has_value(); }
  bool is_error() const { return msg_.has_value(); }
  const std::string& message() const {
    static const std::string kEmpty;
    return msg_ ? *msg_ : kEmpty;
  }
  explicit operator bool() const { return is_ok(); }

 private:
  explicit Status(std::string m) : msg_(std::move(m)) {}
  std::optional<std::string> msg_;
};

template <class T>
class Result {
 public:
  Result(T value) : value_(std::move(value)) {}          // NOLINT(implicit)
  Result(Status err) : status_(std::move(err)) {}        // NOLINT(implicit)

  bool is_ok() const { return status_.is_ok(); }
  bool is_error() const { return status_.is_error(); }
  explicit operator bool() const { return is_ok(); }

  T& value() { return *value_; }
  const T& value() const { return *value_; }
  const Status& status() const { return status_; }
  const std::string& message() const { return status_.message(); }

 private:
  Status status_;
  std::optional<T> value_;
};

}  // namespace raftkv
