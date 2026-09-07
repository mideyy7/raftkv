// Tiny leveled logger to stderr. Enable debug with RAFTKV_LOG=debug.
#pragma once
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <mutex>
#include <string>

namespace raftkv {

enum class LogLevel { kError = 0, kWarn = 1, kInfo = 2, kDebug = 3 };

inline LogLevel& log_level() {
  static LogLevel lvl = [] {
    const char* e = std::getenv("RAFTKV_LOG");
    if (!e) return LogLevel::kInfo;
    if (std::strcmp(e, "debug") == 0) return LogLevel::kDebug;
    if (std::strcmp(e, "warn") == 0) return LogLevel::kWarn;
    if (std::strcmp(e, "error") == 0) return LogLevel::kError;
    return LogLevel::kInfo;
  }();
  return lvl;
}

inline std::mutex& log_mutex() {
  static std::mutex m;
  return m;
}

inline void log_emit(LogLevel lvl, const char* tag, const std::string& msg) {
  if (static_cast<int>(lvl) > static_cast<int>(log_level())) return;
  const char* p = "I";
  switch (lvl) {
    case LogLevel::kError: p = "E"; break;
    case LogLevel::kWarn: p = "W"; break;
    case LogLevel::kInfo: p = "I"; break;
    case LogLevel::kDebug: p = "D"; break;
  }
  std::lock_guard<std::mutex> g(log_mutex());
  std::fprintf(stderr, "[%s] %s: %s\n", p, tag, msg.c_str());
}

#define RAFTKV_LOGF(lvl, tag, ...)                        \
  do {                                                   \
    char _b[512];                                        \
    std::snprintf(_b, sizeof(_b), __VA_ARGS__);          \
    ::raftkv::log_emit(lvl, tag, _b);                    \
  } while (0)

#define LOG_ERROR(tag, ...) RAFTKV_LOGF(::raftkv::LogLevel::kError, tag, __VA_ARGS__)
#define LOG_WARN(tag, ...) RAFTKV_LOGF(::raftkv::LogLevel::kWarn, tag, __VA_ARGS__)
#define LOG_INFO(tag, ...) RAFTKV_LOGF(::raftkv::LogLevel::kInfo, tag, __VA_ARGS__)
#define LOG_DEBUG(tag, ...) RAFTKV_LOGF(::raftkv::LogLevel::kDebug, tag, __VA_ARGS__)

}  // namespace raftkv
