#ifndef STORAGE_LEVELDB_DB_PRE_L0_TWIN_ENV_H_
#define STORAGE_LEVELDB_DB_PRE_L0_TWIN_ENV_H_

#include <string>

#include "leveldb/env.h"

namespace leveldb {

class LatencyInjector;

class TwinEnv : public EnvWrapper {
 public:
  TwinEnv(Env* base, LatencyInjector* inner_injector,
          LatencyInjector* host_read_stream_injector,
          LatencyInjector* host_read_random_injector,
          LatencyInjector* host_write_injector,
          bool enable_cxl_compaction);

  Status NewRandomAccessFile(const std::string& fname,
                             RandomAccessFile** result) override;
  Status NewWritableFile(const std::string& fname,
                         WritableFile** result) override;

 private:
  static bool IsSSTable(const std::string& fname);

  LatencyInjector* inner_injector_;
  LatencyInjector* host_read_stream_injector_;
  LatencyInjector* host_read_random_injector_;
  LatencyInjector* host_write_injector_;
  bool enable_cxl_compaction_;
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_PRE_L0_TWIN_ENV_H_
