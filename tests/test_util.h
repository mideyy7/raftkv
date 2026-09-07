// Shared test helpers: a self-cleaning temp directory.
#pragma once
#include <dirent.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>

namespace raftkv::test {

inline void rm_rf(const std::string& path) {
  DIR* d = ::opendir(path.c_str());
  if (d) {
    struct dirent* e;
    while ((e = ::readdir(d)) != nullptr) {
      if (!std::strcmp(e->d_name, ".") || !std::strcmp(e->d_name, "..")) continue;
      std::string child = path + "/" + e->d_name;
      struct stat st;
      if (::lstat(child.c_str(), &st) == 0 && S_ISDIR(st.st_mode))
        rm_rf(child);
      else
        ::unlink(child.c_str());
    }
    ::closedir(d);
  }
  ::rmdir(path.c_str());
}

class TempDir {
 public:
  TempDir() {
    const char* base = std::getenv("TMPDIR");
    std::string tmpl = (base ? std::string(base) : std::string("/tmp/")) +
                       "raftkv_test_XXXXXX";
    std::vector<char> buf(tmpl.begin(), tmpl.end());
    buf.push_back('\0');
    char* p = ::mkdtemp(buf.data());
    path_ = p ? std::string(p) : std::string("/tmp/raftkv_test_fallback");
    if (!p) ::mkdir(path_.c_str(), 0755);
  }
  ~TempDir() { rm_rf(path_); }
  TempDir(const TempDir&) = delete;
  TempDir& operator=(const TempDir&) = delete;

  const std::string& path() const { return path_; }
  std::string file(const std::string& name) const { return path_ + "/" + name; }

 private:
  std::string path_;
};

}  // namespace raftkv::test
