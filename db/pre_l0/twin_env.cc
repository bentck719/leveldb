#include "db/pre_l0/twin_env.h"

#include <string>

#include "db/io_stats.h"
#include "db/pre_l0/io_context.h"
#include "db/pre_l0/latency_injector.h"

namespace leveldb {

namespace {

inline bool DeviceInternal(bool enable_cxl_compaction) {
  switch (CurrentIoClass()) {
    case IoClass::kFlush:
      return true;
    case IoClass::kCompaction:
      return enable_cxl_compaction;
    case IoClass::kOther:
      return false;
  }
  return false;
}

class TwinRandomAccessFile : public RandomAccessFile {
 public:
  TwinRandomAccessFile(RandomAccessFile* real, LatencyInjector* inner,
                       LatencyInjector* host_read_stream,
                       LatencyInjector* host_read_random,
                       bool enable_cxl_compaction)
      : real_(real),
        inner_(inner),
        host_read_stream_injector_(host_read_stream),
        host_read_random_injector_(host_read_random),
        enable_cxl_compaction_(enable_cxl_compaction) {}
  ~TwinRandomAccessFile() override { delete real_; }

  Status Read(uint64_t offset, size_t n, Slice* result,
              char* scratch) const override {
    Status s = real_->Read(offset, n, result, scratch);
    if (s.ok()) {
      if (DeviceInternal(enable_cxl_compaction_)) {
        inner_->Inject(n);
        g_io_stats.cxl_read_bytes += n;
      } else {
        IoClass cls = CurrentIoClass();
        if (cls == IoClass::kCompaction || cls == IoClass::kScan) {
          if (host_read_stream_injector_) host_read_stream_injector_->Inject(n);
        } else {
          if (host_read_random_injector_) host_read_random_injector_->Inject(n);
        }
        g_io_stats.host_read_bytes += n;
      }
    }
    return s;
  }

 private:
  RandomAccessFile* real_;
  LatencyInjector* inner_;
  LatencyInjector* host_read_stream_injector_;
  LatencyInjector* host_read_random_injector_;
  bool enable_cxl_compaction_;
};

class TwinWritableFile : public WritableFile {
 public:
  TwinWritableFile(WritableFile* real, LatencyInjector* inner,
                   LatencyInjector* host_write, bool enable_cxl_compaction)
      : real_(real),
        inner_injector_(inner),
        host_write_injector_(host_write),
        enable_cxl_compaction_(enable_cxl_compaction) {}
  ~TwinWritableFile() override { delete real_; }

  Status Append(const Slice& data) override {
    Status s = real_->Append(data);
    if (s.ok()) {
      if (DeviceInternal(enable_cxl_compaction_)) {
        if (inner_injector_) inner_injector_->Inject(data.size());
        g_io_stats.cxl_write_bytes += data.size();
      } else {
        if (host_write_injector_) host_write_injector_->Inject(data.size());
        g_io_stats.host_write_bytes += data.size();
      }
    }
    return s;
  }

  Status Close() override { return real_->Close(); }
  Status Flush() override { return real_->Flush(); }
  Status Sync() override { return real_->Sync(); }

 private:
  WritableFile* real_;
  LatencyInjector* inner_injector_;
  LatencyInjector* host_write_injector_;
  bool enable_cxl_compaction_;
};

}  // namespace

TwinEnv::TwinEnv(Env* base, LatencyInjector* inner_injector,
                 LatencyInjector* host_read_stream_injector,
                 LatencyInjector* host_read_random_injector,
                 LatencyInjector* host_write_injector,
                 bool enable_cxl_compaction)
    : EnvWrapper(base),
      inner_injector_(inner_injector),
      host_read_stream_injector_(host_read_stream_injector),
      host_read_random_injector_(host_read_random_injector),
      host_write_injector_(host_write_injector),
      enable_cxl_compaction_(enable_cxl_compaction) {}

bool TwinEnv::IsSSTable(const std::string& fname) {
  size_t dot = fname.rfind('.');
  if (dot == std::string::npos) return false;
  std::string ext = fname.substr(dot);
  return ext == ".ldb" || ext == ".sst";
}

Status TwinEnv::NewRandomAccessFile(const std::string& fname,
                                    RandomAccessFile** result) {
  if (!IsSSTable(fname)) {
    return target()->NewRandomAccessFile(fname, result);
  }
  RandomAccessFile* real = nullptr;
  Status s = target()->NewRandomAccessFile(fname, &real);
  if (!s.ok()) return s;
  *result = new TwinRandomAccessFile(real, inner_injector_,
                                     host_read_stream_injector_,
                                     host_read_random_injector_,
                                     enable_cxl_compaction_);
  return Status::OK();
}

Status TwinEnv::NewWritableFile(const std::string& fname,
                                WritableFile** result) {
  if (!IsSSTable(fname)) {
    return target()->NewWritableFile(fname, result);
  }
  WritableFile* real = nullptr;
  Status s = target()->NewWritableFile(fname, &real);
  if (!s.ok()) return s;
  *result = new TwinWritableFile(real, inner_injector_, host_write_injector_,
                                 enable_cxl_compaction_);
  return Status::OK();
}

}  // namespace leveldb
