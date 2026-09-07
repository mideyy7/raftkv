#include "storage/wal.h"

#include <fcntl.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>

#include "common/crc32c.h"
#include "common/endian.h"

namespace raftkv {
namespace {

Status errno_status(const char* what) {
  return Status::error(std::string(what) + ": " + std::strerror(errno));
}

bool full_read(int fd, void* buf, size_t n) {
  auto* p = static_cast<char*>(buf);
  size_t got = 0;
  while (got < n) {
    ssize_t r = ::read(fd, p + got, n - got);
    if (r < 0) {
      if (errno == EINTR) continue;
      return false;
    }
    if (r == 0) return false;  // EOF before n
    got += static_cast<size_t>(r);
  }
  return true;
}

bool full_write(int fd, const void* buf, size_t n) {
  const auto* p = static_cast<const char*>(buf);
  size_t put = 0;
  while (put < n) {
    ssize_t w = ::write(fd, p + put, n - put);
    if (w < 0) {
      if (errno == EINTR) continue;
      return false;
    }
    put += static_cast<size_t>(w);
  }
  return true;
}

Status fsync_fd(int fd, SyncMode mode) {
  if (mode == SyncMode::kNone) return Status::ok();
#ifdef F_FULLFSYNC
  if (mode == SyncMode::kFull) {
    if (::fcntl(fd, F_FULLFSYNC) == 0) return Status::ok();
    // fall through to fsync if F_FULLFSYNC unsupported on this fs
  }
#endif
  if (::fsync(fd) != 0) return errno_status("fsync");
  return Status::ok();
}

}  // namespace

Wal::~Wal() {
  if (fd_ >= 0) ::close(fd_);
}

std::string Wal::frame(std::string_view payload) {
  std::string out;
  out.reserve(payload.size() + 8);
  put_u32(out, static_cast<uint32_t>(payload.size()));
  put_u32(out, crc32c(0, payload.data(), payload.size()));
  out.append(payload.data(), payload.size());
  return out;
}

Result<std::unique_ptr<Wal>> Wal::open(const std::string& path, SyncMode mode) {
  int fd = ::open(path.c_str(), O_RDWR | O_CREAT, 0644);
  if (fd < 0) return errno_status("open wal");

  std::unique_ptr<Wal> w(new Wal());
  w->fd_ = fd;
  w->path_ = path;
  w->mode_ = mode;

  // Replay from the start.
  if (::lseek(fd, 0, SEEK_SET) < 0) return errno_status("lseek");
  uint64_t good_offset = 0;
  for (;;) {
    char hdr[8];
    if (!full_read(fd, hdr, 8)) break;  // no more full headers -> done/torn
    uint32_t len = get_u32(hdr);
    uint32_t want_crc = get_u32(hdr + 4);
    if (len > kMaxRecord) break;  // implausible -> corrupt tail
    std::string payload;
    payload.resize(len);
    if (len > 0 && !full_read(fd, payload.data(), len)) break;  // torn payload
    if (crc32c(0, payload.data(), len) != want_crc) break;      // corrupt
    w->records_.push_back(std::move(payload));
    good_offset += 8 + len;
  }

  // Truncate any torn tail so future appends start clean.
  if (::ftruncate(fd, static_cast<off_t>(good_offset)) != 0)
    return errno_status("ftruncate torn tail");
  if (::lseek(fd, static_cast<off_t>(good_offset), SEEK_SET) < 0)
    return errno_status("lseek end");
  w->size_ = good_offset;
  return Result<std::unique_ptr<Wal>>(std::move(w));
}

Status Wal::append(std::string_view payload) {
  std::string f = frame(payload);
  if (!full_write(fd_, f.data(), f.size())) return errno_status("write wal");
  size_ += f.size();
  return Status::ok();
}

Status Wal::sync() { return fsync_fd(fd_, mode_); }

Status Wal::rewrite(const std::string& path,
                    const std::vector<std::string>& recs) {
  std::string tmp = path + ".tmp";
  int fd = ::open(tmp.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0644);
  if (fd < 0) return errno_status("open tmp");
  for (const auto& r : recs) {
    std::string f = frame(r);
    if (!full_write(fd, f.data(), f.size())) {
      ::close(fd);
      return errno_status("write tmp");
    }
  }
  Status s = fsync_fd(fd, SyncMode::kFull);
  ::close(fd);
  if (!s) return s;
  if (::rename(tmp.c_str(), path.c_str()) != 0) return errno_status("rename");
  // best-effort: fsync the directory so the rename is durable
  std::string dir = path.substr(0, path.find_last_of('/'));
  if (dir.empty()) dir = ".";
  int dfd = ::open(dir.c_str(), O_RDONLY);
  if (dfd >= 0) {
    (void)fsync_fd(dfd, SyncMode::kFull);
    ::close(dfd);
  }
  return Status::ok();
}

}  // namespace raftkv
