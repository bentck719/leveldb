#ifndef STORAGE_LEVELDB_DB_PRE_L0_IO_CONTEXT_H_
#define STORAGE_LEVELDB_DB_PRE_L0_IO_CONTEXT_H_

namespace leveldb {

enum class IoClass { kOther, kFlush, kCompaction, kScan };

inline thread_local IoClass tls_io_class = IoClass::kOther;

inline IoClass CurrentIoClass() { return tls_io_class; }

class IoClassScope {
 public:
  explicit IoClassScope(IoClass c) : prev_(tls_io_class) { tls_io_class = c; }
  ~IoClassScope() { tls_io_class = prev_; }

  IoClassScope(const IoClassScope&) = delete;
  IoClassScope& operator=(const IoClassScope&) = delete;

 private:
  IoClass prev_;
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_PRE_L0_IO_CONTEXT_H_
