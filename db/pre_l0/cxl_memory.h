#ifndef STORAGE_LEVELDB_DB_PRE_L0_CXL_MEMORY_H_
#define STORAGE_LEVELDB_DB_PRE_L0_CXL_MEMORY_H_

#include <atomic>
#include <cstdint>
#include <string>
#include <vector>

#include "db/pre_l0/latency_injector.h"
#include "leveldb/status.h"
#include "port/port.h"
#include "util/mutexlock.h"

namespace leveldb {

class CxlMemory {
 public:
  ~CxlMemory();

  static Status Open(size_t size, double link_bps, double link_fixed_ns,
                     CxlMemory** out);

  uint32_t AllocateNode();
  void FreeNode(uint32_t offset);

  uint32_t AllocateKV(size_t size);
  void FreeKV(uint32_t offset, size_t size);

  void* ptr(uint32_t offset) const {
    return static_cast<char*>(base_) + offset;
  }

  void InjectLink(size_t bytes) const {
    link_injector_.Inject(bytes);
    link_value_bytes_.fetch_add(bytes, std::memory_order_relaxed);
  }

  void TouchNode() const {
    link_injector_.Inject(kNodeSize);
    node_accesses_.fetch_add(1, std::memory_order_relaxed);
  }

  uint64_t NodeAccesses() const {
    return node_accesses_.load(std::memory_order_relaxed);
  }
  void ResetNodeAccesses() const {
    node_accesses_.store(0, std::memory_order_relaxed);
  }

  uint64_t LinkValueBytes() const {
    return link_value_bytes_.load(std::memory_order_relaxed);
  }

  size_t size() const { return size_; }

  size_t NodesInUse() const;
  size_t KvBytesInUse() const;

  static constexpr uint32_t kInvalidOffset = UINT32_MAX;
  static constexpr size_t kNodeSize = 256;
  static constexpr size_t kNodeAlign = 64;

 private:
  CxlMemory() = default;

  void* base_ = nullptr;
  size_t size_ = 0;

  LatencyInjector link_injector_;

  mutable std::atomic<uint64_t> node_accesses_{0};
  mutable std::atomic<uint64_t> link_value_bytes_{0};

  mutable port::Mutex mutex_;

  uint32_t node_bump_ = 0;
  uint32_t kv_bump_ = 0;
  std::vector<uint32_t> node_free_list_;

  static constexpr size_t kMaxFreeListSize = 4096;
  std::vector<std::vector<uint32_t>> kv_free_lists_;
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_PRE_L0_CXL_MEMORY_H_
