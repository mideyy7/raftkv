// tinytest.h — a tiny, dependency-free C++ test framework for RaftKV.
//
// Usage:
//   #include "tinytest.h"
//   TEST(suite_name, test_name) {
//     CHECK(cond);
//     CHECK_EQ(a, b);
//     REQUIRE(ptr != nullptr);   // aborts this test on failure
//   }
//   TINYTEST_MAIN()              // in exactly one .cpp per test binary
//
// Command line:
//   ./bin --filter='suite.*'    glob filter on "suite.test"
//   ./bin --list                list tests and exit
//   ./bin --seed=12345          set tinytest::seed (tests may read it)
//
// Exit code is the number of failed tests (0 == all passed).

#pragma once
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <exception>
#include <functional>
#include <random>
#include <string>
#include <vector>

namespace tinytest {

inline uint64_t seed = 0;  // settable via --seed=, readable by tests

struct TestCase {
  std::string suite;
  std::string name;
  std::function<void()> fn;
};

struct FailAbort : std::exception {};

class Registry {
 public:
  static Registry& instance() {
    static Registry r;
    return r;
  }
  int add(TestCase tc) {
    cases_.push_back(std::move(tc));
    return static_cast<int>(cases_.size());
  }
  const std::vector<TestCase>& cases() const { return cases_; }

 private:
  std::vector<TestCase> cases_;
};

// Per-test failure state.
inline int g_current_failures = 0;
inline const char* g_current_full = "";

inline bool glob_match(const char* pat, const char* str) {
  // supports '*' and '?'
  if (*pat == '\0') return *str == '\0';
  if (*pat == '*') {
    for (; *str; ++str)
      if (glob_match(pat + 1, str)) return true;
    return glob_match(pat + 1, str);
  }
  if (*str == '\0') return false;
  if (*pat == '?' || *pat == *str) return glob_match(pat + 1, str + 1);
  return false;
}

inline void report_fail(const char* file, int line, const std::string& msg) {
  ++g_current_failures;
  std::fprintf(stderr, "  FAIL %s\n    at %s:%d\n    %s\n", g_current_full, file,
               line, msg.c_str());
}

template <class A, class B>
inline std::string eq_msg(const char* ae, const char* be, const A& a,
                          const B& b) {
  std::string s = "CHECK_EQ(";
  s += ae;
  s += ", ";
  s += be;
  s += ") failed: ";
  if constexpr (std::is_arithmetic_v<A>)
    s += std::to_string(a);
  else
    s += "lhs";
  s += " != ";
  if constexpr (std::is_arithmetic_v<B>)
    s += std::to_string(b);
  else
    s += "rhs";
  return s;
}

inline int run_all(int argc, char** argv) {
  const char* filter = nullptr;
  bool list_only = false;
  for (int i = 1; i < argc; ++i) {
    if (std::strncmp(argv[i], "--filter=", 9) == 0)
      filter = argv[i] + 9;
    else if (std::strcmp(argv[i], "--list") == 0)
      list_only = true;
    else if (std::strncmp(argv[i], "--seed=", 7) == 0)
      seed = std::strtoull(argv[i] + 7, nullptr, 10);
  }
  if (seed == 0) {
    seed = std::random_device{}();
  }

  const auto& cases = Registry::instance().cases();
  int total = 0, passed = 0, failed = 0;
  auto t_start = std::chrono::steady_clock::now();

  for (const auto& tc : cases) {
    std::string full = tc.suite + "." + tc.name;
    if (filter && !glob_match(filter, full.c_str())) continue;
    if (list_only) {
      std::printf("%s\n", full.c_str());
      continue;
    }
    ++total;
    g_current_failures = 0;
    g_current_full = full.c_str();
    auto t0 = std::chrono::steady_clock::now();
    try {
      tc.fn();
    } catch (const FailAbort&) {
      // REQUIRE already recorded the failure
    } catch (const std::exception& e) {
      report_fail(__FILE__, __LINE__,
                  std::string("unexpected exception: ") + e.what());
    } catch (...) {
      report_fail(__FILE__, __LINE__, "unexpected non-std exception");
    }
    auto t1 = std::chrono::steady_clock::now();
    double ms =
        std::chrono::duration<double, std::milli>(t1 - t0).count();
    if (g_current_failures == 0) {
      ++passed;
      std::printf("  ok   %-45s (%.1f ms)\n", full.c_str(), ms);
    } else {
      ++failed;
      std::printf("  BAD  %-45s (%.1f ms, %d checks failed)\n", full.c_str(),
                  ms, g_current_failures);
    }
  }

  if (list_only) return 0;
  auto t_end = std::chrono::steady_clock::now();
  double total_ms =
      std::chrono::duration<double, std::milli>(t_end - t_start).count();
  std::printf("\n%d run, %d passed, %d failed  (seed=%llu, %.1f ms)\n", total,
              passed, failed, (unsigned long long)seed, total_ms);
  return failed;
}

}  // namespace tinytest

#define TT_CAT_(a, b) a##b
#define TT_CAT(a, b) TT_CAT_(a, b)

#define TEST(suite, name)                                                     \
  static void TT_CAT(tt_fn_, __LINE__)();                                     \
  static const int TT_CAT(tt_reg_, __LINE__) =                               \
      ::tinytest::Registry::instance().add(                                   \
          {#suite, #name, &TT_CAT(tt_fn_, __LINE__)});                        \
  static void TT_CAT(tt_fn_, __LINE__)()

#define CHECK(cond)                                                           \
  do {                                                                       \
    if (!(cond))                                                             \
      ::tinytest::report_fail(__FILE__, __LINE__, "CHECK(" #cond ") failed"); \
  } while (0)

#define REQUIRE(cond)                                                         \
  do {                                                                       \
    if (!(cond)) {                                                           \
      ::tinytest::report_fail(__FILE__, __LINE__,                            \
                              "REQUIRE(" #cond ") failed");                  \
      throw ::tinytest::FailAbort{};                                         \
    }                                                                       \
  } while (0)

#define CHECK_EQ(a, b)                                                        \
  do {                                                                       \
    auto _a = (a);                                                           \
    auto _b = (b);                                                           \
    if (!(_a == _b))                                                         \
      ::tinytest::report_fail(__FILE__, __LINE__,                            \
                              ::tinytest::eq_msg(#a, #b, _a, _b));           \
  } while (0)

#define CHECK_NE(a, b)                                                        \
  do {                                                                       \
    if ((a) == (b))                                                          \
      ::tinytest::report_fail(__FILE__, __LINE__,                            \
                              "CHECK_NE(" #a ", " #b ") failed");            \
  } while (0)

#define CHECK_NEAR(a, b, eps)                                                 \
  do {                                                                       \
    if (std::fabs((double)(a) - (double)(b)) > (double)(eps))                \
      ::tinytest::report_fail(__FILE__, __LINE__,                            \
                              "CHECK_NEAR(" #a ", " #b ") failed");          \
  } while (0)

#define CHECK_THROWS(expr)                                                    \
  do {                                                                       \
    bool _threw = false;                                                     \
    try {                                                                    \
      (void)(expr);                                                          \
    } catch (...) {                                                          \
      _threw = true;                                                         \
    }                                                                       \
    if (!_threw)                                                             \
      ::tinytest::report_fail(__FILE__, __LINE__,                            \
                              "CHECK_THROWS(" #expr ") did not throw");      \
  } while (0)

#define TINYTEST_MAIN()                                                       \
  int main(int argc, char** argv) { return ::tinytest::run_all(argc, argv); }
