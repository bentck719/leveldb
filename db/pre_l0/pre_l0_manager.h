#ifndef STORAGE_LEVELDB_DB_PRE_L0_PRE_L0_MANAGER_H_
#define STORAGE_LEVELDB_DB_PRE_L0_PRE_L0_MANAGER_H_

#include <atomic>
#include <cassert>
#include <list>
#include <map>
#include <string>
#include <unordered_map>
#include <vector>

#include "db/dbformat.h"
#include "db/memtable.h"
#include "db/pre_l0/bplus_tree.h"
#include "db/pre_l0/cxl_memory.h"
#include "db/version_edit.h"
#include "leveldb/options.h"
#include "leveldb/status.h"
#include "port/port.h"
#include "util/mutexlock.h"

namespace leveldb {

struct PseudoSST {
  uint32_t id;
  bool is_hot;
  BPlusTree* tree;
};

class Env;
class Iterator;
class TableCache;
class VersionSet;

class PreL0Manager {
 public:
  PreL0Manager(const Options& options, const std::string& dbname,
               Env* env, TableCache* table_cache, VersionSet* versions,
               port::Mutex* db_mutex);
  ~PreL0Manager();

  Status FlushMemTable(MemTable* mem, uint64_t mem_log_number,
                       VersionEdit* edit);

  bool Get(const LookupKey& lkey, std::string* value, Status* s);

  Iterator* NewIterator();

  Status Recover();

  uint64_t MinLogNumber() const;

  void RepointRetentionToSnapshot(uint64_t snapshot_num);

  size_t DistinctLiveLogs() const {
    MutexLock l(&mutex_);
    return log_refcount_.size();
  }

  uint64_t LinkValueBytes() const { return cxl_mem_->LinkValueBytes(); }

  void DumpTimers() const;

  void DumpStats() const;

  static constexpr int kFlushThreshold = 8;
  static constexpr int kDefaultHotThreshold = 2;
  static constexpr size_t kMaxTreeBytes = 4ULL * 1024 * 1024;

 private:
  bool InsertOrUpdate(const std::string& user_key, const Slice& value,
                      SequenceNumber seq, ValueType type,
                      bool is_hot, uint64_t mem_log_number);

  Status FlushOldestPst(VersionEdit* edit);

  Status EvictForCapacity(VersionEdit* edit);

  double UsageFraction() const {
    if (cxl_mem_->size() == 0) return 0.0;
    size_t used = cxl_mem_->NodesInUse() * CxlMemory::kNodeSize +
                  cxl_mem_->KvBytesInUse();
    return static_cast<double>(used) / cxl_mem_->size();
  }

#ifndef NDEBUG
  void DebugCheckRefcountInvariant() const {
    uint64_t total = 0;
    for (const auto& kv : log_refcount_) total += kv.second;
    assert(total == hashmap_.size() &&
           "log_refcount_ total must equal hashmap_ size");
  }
#endif

  void AssertDbMutexHeld() const {
    if (db_mutex_ != nullptr) db_mutex_->AssertHeld();
  }

  std::atomic<int> dbg_writers_{0};
  std::atomic<int> dbg_readers_{0};

  class DbgWriterScope {
   public:
    explicit DbgWriterScope(PreL0Manager* m) : m_(m) {
#ifndef NDEBUG
      assert(m_->dbg_readers_.load(std::memory_order_acquire) == 0 &&
             "pre-L0 mutation overlapped a NewIterator reader: db_mutex_ not "
             "held by the mutator (locking contract broken)");
      m_->dbg_writers_.fetch_add(1, std::memory_order_release);
#else
      (void)m_;
#endif
    }
    ~DbgWriterScope() {
#ifndef NDEBUG
      m_->dbg_writers_.fetch_sub(1, std::memory_order_release);
#endif
    }

   private:
    PreL0Manager* m_;
  };

  class DbgReaderScope {
   public:
    explicit DbgReaderScope(PreL0Manager* m) : m_(m) {
#ifndef NDEBUG
      assert(m_->dbg_writers_.load(std::memory_order_acquire) == 0 &&
             "NewIterator overlapped a pre-L0 mutation: the mutator did not "
             "hold db_mutex_ (locking contract broken)");
      m_->dbg_readers_.fetch_add(1, std::memory_order_release);
#else
      (void)m_;
#endif
    }
    ~DbgReaderScope() {
#ifndef NDEBUG
      m_->dbg_readers_.fetch_sub(1, std::memory_order_release);
#endif
    }

   private:
    PreL0Manager* m_;
  };

  PseudoSST* active_hot_;
  PseudoSST* active_cold_;
  CxlMemory* cxl_mem_;

  struct KeyLoc {
    PseudoSST* pst;
    uint64_t latest_log;
  };

  std::unordered_map<std::string, KeyLoc> hashmap_;
  std::map<uint64_t, uint64_t> log_refcount_;
  std::unordered_map<uint32_t, std::list<PseudoSST*>::iterator> pst_index_;
  std::list<PseudoSST*> lru_;

  uint32_t next_pst_id_;

  const Options options_;
  const std::string dbname_;
  Env* env_;
  TableCache* table_cache_;
  VersionSet* versions_;
  port::Mutex* db_mutex_;

  mutable port::Mutex mutex_;

  std::atomic<uint64_t> t_scan_{0};
  std::atomic<uint64_t> t_insert_{0};
  std::atomic<uint64_t> t_readback_{0};
  std::atomic<uint64_t> t_buildtable_{0};

  uint64_t insert_new_;
  uint64_t insert_update_;
  uint64_t hot_route_keys_;
  uint64_t cold_route_keys_;
  uint64_t bytes_absorbed_;
  uint64_t bytes_coalesced_;
  uint64_t bytes_flushed_to_l0_;
  uint64_t flush_to_l0_calls_;
  uint64_t l0_sst_emitted_;
  uint64_t peak_lru_size_;
  uint64_t fallback_keys_;
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_PRE_L0_PRE_L0_MANAGER_H_
