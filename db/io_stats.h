#ifndef STORAGE_LEVELDB_DB_IO_STATS_H_
#define STORAGE_LEVELDB_DB_IO_STATS_H_

#include <atomic>
#include <cstdint>

namespace leveldb {

struct IOStats {
  std::atomic<uint64_t> host_write_bytes{0};

  std::atomic<uint64_t> cxl_write_bytes{0};

  std::atomic<uint64_t> host_read_bytes{0};

  std::atomic<uint64_t> cxl_read_bytes{0};

  std::atomic<uint64_t> prel0_write_bytes{0};

  void Reset() {
    host_write_bytes = 0;
    cxl_write_bytes = 0;
    host_read_bytes = 0;
    cxl_read_bytes = 0;
    prel0_write_bytes = 0;
  }

  uint64_t HostVisibleIO() const {
    return host_write_bytes.load() + host_read_bytes.load() +
           prel0_write_bytes.load();
  }
  uint64_t SSDInternalIO() const {
    return cxl_write_bytes.load() + cxl_read_bytes.load();
  }
};

extern IOStats g_io_stats;

struct DataBlockWorkingSet {
  std::atomic<uint64_t> live_bytes{0};
  std::atomic<uint64_t> live_count{0};
  std::atomic<uint64_t> peak_bytes{0};
  std::atomic<uint64_t> peak_count{0};
  std::atomic<uint64_t> total_bytes{0};
  std::atomic<uint64_t> total_count{0};

  void Acquire(uint64_t size) {
    const uint64_t lb =
        live_bytes.fetch_add(size, std::memory_order_relaxed) + size;
    const uint64_t lc = live_count.fetch_add(1, std::memory_order_relaxed) + 1;
    total_bytes.fetch_add(size, std::memory_order_relaxed);
    total_count.fetch_add(1, std::memory_order_relaxed);
    if (lb > peak_bytes.load(std::memory_order_relaxed)) {
      peak_bytes.store(lb, std::memory_order_relaxed);
    }
    if (lc > peak_count.load(std::memory_order_relaxed)) {
      peak_count.store(lc, std::memory_order_relaxed);
    }
  }

  void Release(uint64_t size) {
    live_bytes.fetch_sub(size, std::memory_order_relaxed);
    live_count.fetch_sub(1, std::memory_order_relaxed);
  }

  void ResetPeak() {
    live_bytes = 0;
    live_count = 0;
    peak_bytes = 0;
    peak_count = 0;
    total_bytes = 0;
    total_count = 0;
  }
};

extern DataBlockWorkingSet g_data_block_ws;

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_IO_STATS_H_
