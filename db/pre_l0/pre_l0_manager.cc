#include "db/pre_l0/pre_l0_manager.h"

#include <algorithm>
#include <cassert>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <vector>

#include "db/builder.h"
#include "db/dbformat.h"
#include "db/filename.h"
#include "db/io_stats.h"
#include "db/pre_l0/io_context.h"
#include "db/table_cache.h"
#include "db/version_set.h"
#include "leveldb/env.h"
#include "leveldb/iterator.h"
#include "util/coding.h"

namespace leveldb {

namespace {

struct KVEntry {
  std::string ikey;
  Slice value;
};

class VectorIterator : public Iterator {
 public:
  explicit VectorIterator(std::vector<KVEntry> entries)
      : entries_(std::move(entries)), pos_(0) {}

  bool Valid() const override { return pos_ < entries_.size(); }
  void SeekToFirst() override { pos_ = 0; }
  void SeekToLast() override {
    pos_ = entries_.empty() ? 0 : entries_.size() - 1;
  }
  void Seek(const Slice&) override { SeekToFirst(); }
  void Next() override { ++pos_; }
  void Prev() override { if (pos_ > 0) --pos_; }
  Slice key() const override { return Slice(entries_[pos_].ikey); }
  Slice value() const override { return entries_[pos_].value; }
  Status status() const override { return Status::OK(); }

 private:
  std::vector<KVEntry> entries_;
  size_t pos_;
};

class OwningVectorIterator : public Iterator {
 public:
  OwningVectorIterator(std::vector<std::pair<std::string, std::string>> entries,
                       const InternalKeyComparator& icmp)
      : entries_(std::move(entries)), icmp_(icmp), pos_(0) {}

  bool Valid() const override { return pos_ < entries_.size(); }
  void SeekToFirst() override { pos_ = 0; }
  void SeekToLast() override {
    pos_ = entries_.empty() ? 0 : entries_.size() - 1;
  }
  void Seek(const Slice& target) override {
    pos_ = std::lower_bound(
               entries_.begin(), entries_.end(), target,
               [this](const std::pair<std::string, std::string>& e,
                      const Slice& t) {
                 return icmp_.Compare(Slice(e.first), t) < 0;
               }) -
           entries_.begin();
  }
  void Next() override { ++pos_; }
  void Prev() override {
    if (pos_ > 0) --pos_; else pos_ = entries_.size();
  }
  Slice key() const override { return Slice(entries_[pos_].first); }
  Slice value() const override { return Slice(entries_[pos_].second); }
  Status status() const override { return Status::OK(); }

 private:
  std::vector<std::pair<std::string, std::string>> entries_;
  InternalKeyComparator icmp_;
  size_t pos_;
};

std::string MakeInternalKey(const std::string& user_key,
                             SequenceNumber seq, ValueType type) {
  std::string ikey;
  ikey.resize(user_key.size() + 8);
  memcpy(&ikey[0], user_key.data(), user_key.size());
  uint64_t tag = (seq << 8) | static_cast<uint8_t>(type);
  for (int b = 0; b < 8; b++)
    ikey[user_key.size() + b] = static_cast<char>((tag >> (8 * b)) & 0xff);
  return ikey;
}

std::string HexEncode(const Slice& s) {
  static const char kHex[] = "0123456789abcdef";
  std::string out;
  out.reserve(s.size() * 2);
  for (size_t i = 0; i < s.size(); ++i) {
    unsigned char c = static_cast<unsigned char>(s[i]);
    out.push_back(kHex[c >> 4]);
    out.push_back(kHex[c & 0xf]);
  }
  return out;
}

uint64_t NowMicros() {
  return static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::microseconds>(
          std::chrono::system_clock::now().time_since_epoch())
          .count());
}

}  // namespace

PreL0Manager::PreL0Manager(const Options& options, const std::string& dbname,
                           Env* env, TableCache* table_cache,
                           VersionSet* versions, port::Mutex* db_mutex)
    : active_hot_(nullptr),
      active_cold_(nullptr),
      cxl_mem_(nullptr),
      next_pst_id_(0),
      options_(options),
      dbname_(dbname),
      env_(env),
      table_cache_(table_cache),
      versions_(versions),
      db_mutex_(db_mutex),
      insert_new_(0),
      insert_update_(0),
      hot_route_keys_(0),
      cold_route_keys_(0),
      bytes_absorbed_(0),
      bytes_coalesced_(0),
      bytes_flushed_to_l0_(0),
      flush_to_l0_calls_(0),
      l0_sst_emitted_(0),
      peak_lru_size_(0),
      fallback_keys_(0) {}

PreL0Manager::~PreL0Manager() {
  auto delete_pst = [](PseudoSST* pst) {
    if (pst != nullptr) {
      delete pst->tree;
      delete pst;
    }
  };
  delete_pst(active_hot_);
  delete_pst(active_cold_);
  for (PseudoSST* pst : lru_) delete_pst(pst);
  delete cxl_mem_;
}

Status PreL0Manager::Recover() {
  MutexLock l(&mutex_);

  Status s = CxlMemory::Open(options_.pre_l0_cxl_size,
                             options_.pre_l0_cxl_link_bps,
                             options_.pre_l0_cxl_link_fixed_ns, &cxl_mem_);
  if (!s.ok()) return s;

  return Status::OK();
}

bool PreL0Manager::InsertOrUpdate(const std::string& user_key, const Slice& value,
                                   SequenceNumber seq, ValueType type,
                                   bool is_hot, uint64_t mem_log_number) {
  AssertDbMutexHeld();
  DbgWriterScope writer(this);
  Slice key_slice(user_key);
  auto it = hashmap_.find(user_key);

  auto charge_move = [&](size_t move_bytes) {
    g_io_stats.prel0_write_bytes += move_bytes;
    cxl_mem_->InjectLink(move_bytes);
    bytes_absorbed_ += move_bytes;
  };

  if (it == hashmap_.end()) {
    charge_move(user_key.size() + value.size());
    ++insert_new_;
    if (is_hot) {
      ++hot_route_keys_;
    } else {
      ++cold_route_keys_;
    }
    PseudoSST*& active = is_hot ? active_hot_ : active_cold_;
    if (active == nullptr) {
      active = new PseudoSST();
      active->id = next_pst_id_++;
      active->is_hot = is_hot;
      active->tree = new BPlusTree(cxl_mem_);
    }
    if (!active->tree->Insert(key_slice, value, seq, type)) return false;
    hashmap_[user_key] = {active, mem_log_number};
    log_refcount_[mem_log_number]++;

    if (active->tree->ByteSize() >= kMaxTreeBytes) {
      lru_.push_front(active);
      pst_index_[active->id] = lru_.begin();
      if (lru_.size() > peak_lru_size_) peak_lru_size_ = lru_.size();
      active = nullptr;
    }
  } else {
    charge_move(value.size());
    ++insert_update_;
    bytes_coalesced_ += user_key.size() + value.size();
    PseudoSST* owner = it->second.pst;
    if (!owner->tree->Update(key_slice, value, seq, type)) return false;
    uint64_t old_log = it->second.latest_log;
    if (--log_refcount_[old_log] == 0) log_refcount_.erase(old_log);
    log_refcount_[mem_log_number]++;
    it->second.latest_log = mem_log_number;

    auto pit = pst_index_.find(owner->id);
    if (pit != pst_index_.end() && pit->second != lru_.begin()) {
      lru_.splice(lru_.begin(), lru_, pit->second);
      pst_index_[owner->id] = lru_.begin();
    }
  }
  return true;
}

Status PreL0Manager::FlushMemTable(MemTable* mem, uint64_t mem_log_number,
                                   VersionEdit* edit) {
  AssertDbMutexHeld();
  MutexLock l(&mutex_);
  using Clock = std::chrono::steady_clock;
  auto ns = [](Clock::duration d) {
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(d).count());
  };

  auto loop_start = Clock::now();
  uint64_t insert_ns = 0;
  Status flush_status;
  bool failed = false;

  bool fallback_mode = false;
  std::vector<KVEntry> fallback_entries;

  Iterator* iter = mem->NewIterator();
  for (iter->SeekToFirst(); iter->Valid(); ) {
    Slice ikey = iter->key();
    Slice user_key = ExtractUserKey(ikey);
    ParsedInternalKey parsed;
    ParseInternalKey(ikey, &parsed);
    Slice value = iter->value();

    int count = 1;
    iter->Next();
    while (iter->Valid() &&
           ExtractUserKey(iter->key()).compare(user_key) == 0) {
      count++;
      iter->Next();
    }
    bool is_hot = count >= options_.pre_l0_hot_threshold;
    std::string uk = user_key.ToString();
    bool in_prel0 = hashmap_.count(uk) > 0;

    if (fallback_mode) {
      if (in_prel0) {
        auto i0 = Clock::now();
        bool ok = InsertOrUpdate(uk, value, parsed.sequence, parsed.type, is_hot,
                                 mem_log_number);
        insert_ns += ns(Clock::now() - i0);
        if (!ok) {
          flush_status = Status::IOError(
              "pre-L0 CXL OOM: insufficient space even after flush");
          failed = true;
          break;
        }
      } else {
        fallback_entries.push_back(
            {MakeInternalKey(uk, parsed.sequence, parsed.type), value});
        ++fallback_keys_;
      }
      continue;
    }

    size_t before = lru_.size();
    auto i0 = Clock::now();
    bool ok = InsertOrUpdate(uk, value, parsed.sequence, parsed.type, is_hot,
                             mem_log_number);
    insert_ns += ns(Clock::now() - i0);

    if (ok && lru_.size() > before &&
        UsageFraction() >= options_.pre_l0_evict_high_watermark) {
      Status es = EvictForCapacity(edit);
      if (!es.ok()) {
        flush_status = es;
        failed = true;
        break;
      }
    }

    if (!ok) {
      Status fs = EvictForCapacity(edit);
      if (!fs.ok()) {
        flush_status = fs;
        failed = true;
        break;
      }
      auto r0 = Clock::now();
      ok = InsertOrUpdate(uk, value, parsed.sequence, parsed.type, is_hot,
                          mem_log_number);
      insert_ns += ns(Clock::now() - r0);
      if (!ok) {
        if (!options_.pre_l0_fallback_to_l0) {
          flush_status = Status::IOError(
              "pre-L0 CXL OOM: insufficient space even after flush");
          failed = true;
          break;
        }
        if (in_prel0) {
          flush_status = Status::IOError(
              "pre-L0 CXL OOM: insufficient space even after flush");
          failed = true;
          break;
        }
        fallback_entries.push_back(
            {MakeInternalKey(uk, parsed.sequence, parsed.type), value});
        ++fallback_keys_;
        fallback_mode = true;
      }
    }
  }

  uint64_t loop_ns = ns(Clock::now() - loop_start);
  t_insert_ += insert_ns;
  t_scan_ += loop_ns >= insert_ns ? loop_ns - insert_ns : 0;

  if (failed) {
    delete iter;
    return flush_status;
  }

  if (!fallback_entries.empty()) {
    FileMetaData meta;
    meta.number = versions_->NewFileNumber();
    Iterator* table_iter = new VectorIterator(std::move(fallback_entries));

    auto bt0 = Clock::now();
    db_mutex_->Unlock();
    Status bs;
    {
      IoClassScope io_scope(IoClass::kFlush);
      bs = BuildTable(dbname_, env_, options_, table_cache_, table_iter, &meta);
    }
    db_mutex_->Lock();
    t_buildtable_ += ns(Clock::now() - bt0);

    if (!bs.ok()) {
      delete iter;
      return bs;
    }
    if (meta.file_size > 0) {
      edit->AddFile(0, meta.number, meta.file_size, meta.smallest, meta.largest);
      ++l0_sst_emitted_;
    }
  }
  delete iter;

  Status s;
  if (UsageFraction() >= options_.pre_l0_evict_high_watermark) {
    s = EvictForCapacity(edit);
  }
#ifndef NDEBUG
  DebugCheckRefcountInvariant();
#endif
  return s;
}

Status PreL0Manager::FlushOldestPst(VersionEdit* edit) {
  AssertDbMutexHeld();
  if (lru_.empty()) return Status::OK();
  using Clock = std::chrono::steady_clock;
  auto ns = [](Clock::duration d) {
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(d).count());
  };
  ++flush_to_l0_calls_;

  auto pit = std::prev(lru_.end());
  PseudoSST* pst = *pit;
  BPlusTree* tree = pst->tree;

  std::vector<KVEntry> entries;
  std::vector<std::string> keys_to_erase;
  entries.reserve(tree->Count());
  keys_to_erase.reserve(tree->Count());

  auto rb0 = Clock::now();
  BPlusTree::Iterator* tree_iter = tree->NewIterator();
  for (tree_iter->SeekToFirst(); tree_iter->Valid(); tree_iter->Next()) {
    std::string key_str = tree_iter->key().ToString();
    BPlusTreeValue bv = tree_iter->value();
    Slice value;
    if (bv.value_len > 0) {
      value = Slice(static_cast<char*>(cxl_mem_->ptr(bv.value_offset)),
                    bv.value_len);
    }

    entries.push_back({MakeInternalKey(key_str, bv.seq, bv.type), value});
    keys_to_erase.push_back(std::move(key_str));
  }
  delete tree_iter;
  t_readback_ += ns(Clock::now() - rb0);

  if (!entries.empty()) {
    FileMetaData meta;
    meta.number = versions_->NewFileNumber();

    uint64_t entry_count = entries.size();
    Iterator* table_iter = new VectorIterator(std::move(entries));

    auto bt0 = Clock::now();
    db_mutex_->Unlock();
    Status s;
    {
      IoClassScope io_scope(IoClass::kFlush);
      s = BuildTable(dbname_, env_, options_, table_cache_, table_iter, &meta);
    }
    db_mutex_->Lock();
    t_buildtable_ += ns(Clock::now() - bt0);

    if (!s.ok()) return s;

    if (meta.file_size > 0) {
      edit->AddFile(0, meta.number, meta.file_size,
                    meta.smallest, meta.largest);
      ++l0_sst_emitted_;
      bytes_flushed_to_l0_ += meta.file_size;

      if (!options_.metrics_dir.empty()) {
        std::string path = options_.metrics_dir + "/pre_l0_flush.csv";
        FILE* f = std::fopen(path.c_str(), "a");
        if (f != nullptr) {
          std::fseek(f, 0, SEEK_END);
          if (std::ftell(f) == 0) {
            std::fprintf(f,
                         "timestamp_us,pst_id,is_hot,entry_count,"
                         "sst_size_bytes,smallest_key,largest_key\n");
          }
          std::fprintf(f, "%llu,%u,%d,%llu,%llu,%s,%s\n",
                       static_cast<unsigned long long>(NowMicros()),
                       pst->id, pst->is_hot ? 1 : 0,
                       static_cast<unsigned long long>(entry_count),
                       static_cast<unsigned long long>(meta.file_size),
                       HexEncode(meta.smallest.user_key()).c_str(),
                       HexEncode(meta.largest.user_key()).c_str());
          std::fclose(f);
        }
      }
    }
  }

  {
    DbgWriterScope writer(this);
    for (const auto& key_str : keys_to_erase) {
      auto it = hashmap_.find(key_str);
      uint64_t latest_log = it->second.latest_log;
      if (--log_refcount_[latest_log] == 0) log_refcount_.erase(latest_log);
      hashmap_.erase(it);
    }
    pst_index_.erase(pst->id);
    lru_.erase(pit);
    delete pst->tree;
    delete pst;
#ifndef NDEBUG
    DebugCheckRefcountInvariant();
#endif
  }

  return Status::OK();
}

Status PreL0Manager::EvictForCapacity(VersionEdit* edit) {
  AssertDbMutexHeld();
  Status s;
  while (!lru_.empty() &&
         UsageFraction() > options_.pre_l0_evict_low_watermark) {
    s = FlushOldestPst(edit);
    if (!s.ok()) return s;
  }
  return s;
}

bool PreL0Manager::Get(const LookupKey& lkey, std::string* value, Status* s) {
  MutexLock l(&mutex_);
  Slice user_key = lkey.user_key();
  static thread_local std::string lookup_key;
  lookup_key.assign(user_key.data(), user_key.size());
  auto it = hashmap_.find(lookup_key);
  if (it == hashmap_.end()) return false;

  BPlusTree* tree = it->second.pst->tree;
  BPlusTreeValue bv;
  if (!tree->Get(user_key, &bv)) return false;

  Slice ik = lkey.internal_key();
  SequenceNumber snapshot_seq =
      DecodeFixed64(ik.data() + ik.size() - 8) >> 8;
  if (bv.seq > snapshot_seq) return false;

  cxl_mem_->InjectLink(bv.value_len);

  if (bv.type == kTypeDeletion) {
    *s = Status::NotFound(Slice());
  } else {
    value->assign(static_cast<char*>(cxl_mem_->ptr(bv.value_offset)),
                  bv.value_len);
  }
  return true;
}

Iterator* PreL0Manager::NewIterator() {
  AssertDbMutexHeld();
  DbgReaderScope reader(this);

  std::vector<std::pair<std::string, std::string>> entries;
  auto collect = [&](PseudoSST* pst) {
    if (pst == nullptr) return;
    BPlusTree::Iterator* tit = pst->tree->NewIterator();
    for (tit->SeekToFirst(); tit->Valid(); tit->Next()) {
      std::string user_key = tit->key().ToString();
      BPlusTreeValue bv = tit->value();
      std::string val;
      if (bv.value_len > 0) {
        val.assign(static_cast<char*>(cxl_mem_->ptr(bv.value_offset)),
                   bv.value_len);
      }
      entries.emplace_back(MakeInternalKey(user_key, bv.seq, bv.type),
                           std::move(val));
    }
    delete tit;
  };
  collect(active_hot_);
  collect(active_cold_);
  for (PseudoSST* pst : lru_) collect(pst);

  InternalKeyComparator icmp(options_.comparator);
  std::sort(entries.begin(), entries.end(),
            [&icmp](const std::pair<std::string, std::string>& a,
                    const std::pair<std::string, std::string>& b) {
              return icmp.Compare(Slice(a.first), Slice(b.first)) < 0;
            });
  return new OwningVectorIterator(std::move(entries), icmp);
}

void PreL0Manager::RepointRetentionToSnapshot(uint64_t snapshot_num) {
  AssertDbMutexHeld();
  DbgWriterScope writer(this);
  MutexLock l(&mutex_);
  for (auto& kv : hashmap_) {
    kv.second.latest_log = snapshot_num;
  }
  log_refcount_.clear();
  if (!hashmap_.empty()) {
    log_refcount_[snapshot_num] = hashmap_.size();
  }
#ifndef NDEBUG
  DebugCheckRefcountInvariant();
#endif
}

uint64_t PreL0Manager::MinLogNumber() const {
  MutexLock l(&mutex_);
  return log_refcount_.empty() ? 0 : log_refcount_.begin()->first;
}

void PreL0Manager::DumpTimers() const {
  if (options_.metrics_dir.empty()) return;
  std::string path = options_.metrics_dir + "/phase_timing.csv";
  FILE* f = std::fopen(path.c_str(), "a");
  if (f == nullptr) return;
  std::fseek(f, 0, SEEK_END);
  if (std::ftell(f) == 0) {
    std::fprintf(f, "scan_ms,insert_ms,readback_ms,buildtable_ms\n");
  }
  std::fprintf(f, "%.3f,%.3f,%.3f,%.3f\n", t_scan_.load() / 1e6,
               t_insert_.load() / 1e6, t_readback_.load() / 1e6,
               t_buildtable_.load() / 1e6);
  std::fclose(f);
}

void PreL0Manager::DumpStats() const {
  if (options_.metrics_dir.empty()) return;
  std::string path = options_.metrics_dir + "/pre_l0_stats.csv";
  FILE* f = std::fopen(path.c_str(), "a");
  if (f == nullptr) return;
  std::fseek(f, 0, SEEK_END);
  if (std::ftell(f) == 0) {
    std::fprintf(f,
                 "insert_new,insert_update,coalesce_ratio,coalesce_byte_ratio,"
                 "hot_route_keys,cold_route_keys,bytes_absorbed,"
                 "bytes_coalesced,bytes_flushed_to_l0,pst_created,"
                 "flush_to_l0_calls,l0_sst_emitted,peak_lru_size,"
                 "fallback_keys\n");
  }
  uint64_t total_inserts = insert_new_ + insert_update_;
  double coalesce_ratio =
      total_inserts == 0 ? 0.0
                         : static_cast<double>(insert_update_) / total_inserts;
  double coalesce_byte_ratio =
      bytes_absorbed_ == 0
          ? 0.0
          : static_cast<double>(bytes_coalesced_) / bytes_absorbed_;
  std::fprintf(f,
               "%llu,%llu,%.6f,%.6f,%llu,%llu,%llu,%llu,%llu,%u,%llu,%llu,%llu,"
               "%llu\n",
               static_cast<unsigned long long>(insert_new_),
               static_cast<unsigned long long>(insert_update_), coalesce_ratio,
               coalesce_byte_ratio,
               static_cast<unsigned long long>(hot_route_keys_),
               static_cast<unsigned long long>(cold_route_keys_),
               static_cast<unsigned long long>(bytes_absorbed_),
               static_cast<unsigned long long>(bytes_coalesced_),
               static_cast<unsigned long long>(bytes_flushed_to_l0_),
               next_pst_id_,
               static_cast<unsigned long long>(flush_to_l0_calls_),
               static_cast<unsigned long long>(l0_sst_emitted_),
               static_cast<unsigned long long>(peak_lru_size_),
               static_cast<unsigned long long>(fallback_keys_));
  std::fclose(f);
}

}  // namespace leveldb
