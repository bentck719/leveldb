// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/db_impl.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <set>
#include <string>
#include <vector>

#include <sys/time.h>

#include "db/builder.h"
#include "db/db_iter.h"
#include "db/io_stats.h"
#include "db/pre_l0/latency_injector.h"
#include "db/pre_l0/io_context.h"
#include "db/pre_l0/pre_l0_manager.h"
#include "db/pre_l0/twin_env.h"
#include "db/dbformat.h"
#include "db/filename.h"
#include "db/log_reader.h"
#include "db/log_writer.h"
#include "db/memtable.h"
#include "db/table_cache.h"
#include "db/version_set.h"
#include "db/write_batch_internal.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "leveldb/status.h"
#include "leveldb/table.h"
#include "leveldb/table_builder.h"
#include "port/port.h"
#include "table/block.h"
#include "table/merger.h"
#include "table/two_level_iterator.h"
#include "util/coding.h"
#include "util/logging.h"
#include "util/mutexlock.h"

namespace leveldb {

const int kNumNonTableCacheFiles = 10;

// Information kept for every waiting writer
struct DBImpl::Writer {
  explicit Writer(port::Mutex* mu)
      : batch(nullptr), sync(false), done(false), cv(mu) {}

  Status status;
  WriteBatch* batch;
  bool sync;
  bool done;
  port::CondVar cv;
};

struct DBImpl::CompactionState {
  // Files produced by compaction
  struct Output {
    uint64_t number;
    uint64_t file_size;
    InternalKey smallest, largest;
  };

  Output* current_output() { return &outputs[outputs.size() - 1]; }

  explicit CompactionState(Compaction* c)
      : compaction(c),
        smallest_snapshot(0),
        outfile(nullptr),
        builder(nullptr),
        total_bytes(0) {}

  Compaction* const compaction;

  // Sequence numbers < smallest_snapshot are not significant since we
  // will never have to service a snapshot below smallest_snapshot.
  // Therefore if we have seen a sequence number S <= smallest_snapshot,
  // we can drop all entries for the same key with sequence numbers < S.
  SequenceNumber smallest_snapshot;

  std::vector<Output> outputs;

  // State kept for output being generated
  WritableFile* outfile;
  TableBuilder* builder;

  uint64_t total_bytes;
};

// Fix user-supplied options to be reasonable
template <class T, class V>
static void ClipToRange(T* ptr, V minvalue, V maxvalue) {
  if (static_cast<V>(*ptr) > maxvalue) *ptr = maxvalue;
  if (static_cast<V>(*ptr) < minvalue) *ptr = minvalue;
}
Options SanitizeOptions(const std::string& dbname,
                        const InternalKeyComparator* icmp,
                        const InternalFilterPolicy* ipolicy,
                        const Options& src) {
  Options result = src;
  result.comparator = icmp;
  result.filter_policy = (src.filter_policy != nullptr) ? ipolicy : nullptr;
  ClipToRange(&result.max_open_files, 64 + kNumNonTableCacheFiles, 50000);
  ClipToRange(&result.write_buffer_size, 64 << 10, 1 << 30);
  ClipToRange(&result.max_file_size, 1 << 20, 1 << 30);
  ClipToRange(&result.block_size, 1 << 10, 4 << 20);
  if (result.info_log == nullptr) {
    // Open a log file in the same directory as the db
    src.env->CreateDir(dbname);  // In case it does not exist
    src.env->RenameFile(InfoLogFileName(dbname), OldInfoLogFileName(dbname));
    Status s = src.env->NewLogger(InfoLogFileName(dbname), &result.info_log);
    if (!s.ok()) {
      // No place suitable for logging
      result.info_log = nullptr;
    }
  }
  if (result.block_cache == nullptr) {
    result.block_cache = NewLRUCache(8 << 20);
  }
  return result;
}

static int TableCacheSize(const Options& sanitized_options) {
  // Reserve ten files or so for other uses and give the rest to TableCache.
  return sanitized_options.max_open_files - kNumNonTableCacheFiles;
}

DBImpl::DBImpl(const Options& raw_options, const std::string& dbname)
    : env_(raw_options.env),
      internal_comparator_(raw_options.comparator),
      internal_filter_policy_(raw_options.filter_policy),
      options_(SanitizeOptions(dbname, &internal_comparator_,
                               &internal_filter_policy_, raw_options)),
      owns_info_log_(options_.info_log != raw_options.info_log),
      owns_cache_(options_.block_cache != raw_options.block_cache),
      dbname_(dbname),
      table_cache_(new TableCache(dbname_, options_, TableCacheSize(options_))),
      db_lock_(nullptr),
      shutting_down_(false),
      background_work_finished_signal_(&mutex_),
      mem_(nullptr),
      imm_(nullptr),
      has_imm_(false),
      logfile_(nullptr),
      logfile_number_(0),
      log_(nullptr),
      seed_(0),
      tmp_batch_(new WriteBatch),
      background_compaction_scheduled_(false),
      manual_compaction_(nullptr),
      versions_(new VersionSet(dbname_, &options_, table_cache_,
                               &internal_comparator_)),
      metrics_file_(nullptr),
      flush_hotness_file_(nullptr),
      flush_seq_(0),
      flush_metrics_file_(nullptr),
      wal_metrics_file_(nullptr),
      wal_footprint_file_(nullptr),
      live_wal_bytes_(0),
      wal_snapshot_trigger_(options_.pre_l0_wal_snapshot_threshold_bytes,
                            options_.pre_l0_wal_snapshot_release_ratio),
      wal_snapshot_requested_(false),
      wal_snapshot_count_(0),
      key_access_file_(nullptr),
      pre_l0_(nullptr),
      imm_log_number_(0),
      twin_env_(nullptr),
      inner_injector_(nullptr),
      host_read_stream_injector_(nullptr),
      host_read_random_injector_(nullptr),
      host_write_injector_(nullptr),
      user_write_bytes_(0) {
  if (!options_.metrics_dir.empty()) {
    const std::string& dir = options_.metrics_dir;
    metrics_file_ = std::fopen((dir + "/compaction_metrics.csv").c_str(), "a");
    if (metrics_file_ == nullptr) {
      std::fprintf(stderr,
                   "LevelDB warning: failed to open compaction_metrics.csv\n");
    } else {
      std::fseek(metrics_file_, 0, SEEK_END);
      if (std::ftell(metrics_file_) == 0) {
        std::fprintf(metrics_file_,
                     "timestamp_us,compaction_level,input_bytes,output_bytes,"
                     "l0_files_before,l0_files_after,l0_bytes,"
                     "l1_files_before,l1_files_after,l1_bytes,"
                     "peak_resident_data_block_bytes,"
                     "peak_resident_data_block_count,"
                     "total_data_block_bytes,total_data_block_count\n");
      }
    }
    flush_hotness_file_ =
        std::fopen((dir + "/flush_hotness.csv").c_str(), "a");
    if (flush_hotness_file_ == nullptr) {
      std::fprintf(stderr,
                   "LevelDB warning: failed to open flush_hotness.csv\n");
    } else {
      std::fseek(flush_hotness_file_, 0, SEEK_END);
      if (std::ftell(flush_hotness_file_) == 0) {
        std::fprintf(flush_hotness_file_,
                     "timestamp_us,flush_seq,total_entries,unique_keys,"
                     "hot_keys,hot_key_ratio,hot_byte_ratio,"
                     "max_version_count,p50_version_count,"
                     "p90_version_count\n");
      }
    }
    flush_metrics_file_ =
        std::fopen((dir + "/flush_metrics.csv").c_str(), "a");
    if (flush_metrics_file_ == nullptr) {
      std::fprintf(stderr,
                   "LevelDB warning: failed to open flush_metrics.csv\n");
    } else {
      std::fseek(flush_metrics_file_, 0, SEEK_END);
      if (std::ftell(flush_metrics_file_) == 0) {
        std::fprintf(flush_metrics_file_,
                     "timestamp_us,memtable_key_count,l0_file_count,"
                     "bloom_hit_count,bloom_hit_ratio,"
                     "new_sst_smallest,new_sst_largest,new_sst_size_bytes\n");
      }
    }
    wal_metrics_file_ = std::fopen((dir + "/wal_metrics.csv").c_str(), "a");
    if (wal_metrics_file_ == nullptr) {
      std::fprintf(stderr,
                   "LevelDB warning: failed to open wal_metrics.csv\n");
    } else {
      std::fseek(wal_metrics_file_, 0, SEEK_END);
      if (std::ftell(wal_metrics_file_) == 0) {
        std::fprintf(wal_metrics_file_,
                     "wal_number,create_time_us,delete_time_us,"
                     "lifetime_ms,wal_size_bytes\n");
      }
    }
    if (options_.enable_wal_footprint_tracking) {
      wal_footprint_file_ =
          std::fopen((dir + "/wal_footprint.csv").c_str(), "a");
      if (wal_footprint_file_ == nullptr) {
        std::fprintf(stderr,
                     "LevelDB warning: failed to open wal_footprint.csv\n");
      } else {
        std::fseek(wal_footprint_file_, 0, SEEK_END);
        if (std::ftell(wal_footprint_file_) == 0) {
          std::fprintf(wal_footprint_file_,
                       "timestamp_us,retained_wal_count,retained_wal_bytes,"
                       "oldest_retained_log,newest_log,log_gap\n");
        }
      }
    }
    key_access_file_ = std::fopen((dir + "/key_access.csv").c_str(), "w");
    if (key_access_file_ == nullptr) {
      std::fprintf(stderr,
                   "LevelDB warning: failed to open key_access.csv\n");
    } else {
      std::fprintf(key_access_file_,
                   "key,read_count,write_count,total_count\n");
    }
    user_write_bytes_path_ = dir + "/user_write_bytes.csv";
  }
}

DBImpl::~DBImpl() {
  // Wait for background work to finish.
  mutex_.Lock();
  shutting_down_.store(true, std::memory_order_release);
  while (background_compaction_scheduled_) {
    background_work_finished_signal_.Wait();
  }
  mutex_.Unlock();

  if (metrics_file_ != nullptr) {
    std::fclose(metrics_file_);
  }
  if (flush_hotness_file_ != nullptr) {
    std::fclose(flush_hotness_file_);
  }
  if (flush_metrics_file_ != nullptr) {
    std::fclose(flush_metrics_file_);
  }
  if (wal_metrics_file_ != nullptr) {
    std::fclose(wal_metrics_file_);
  }
  if (wal_footprint_file_ != nullptr) {
    std::fclose(wal_footprint_file_);
  }
  if (key_access_file_ != nullptr) {
    MutexLock l(&mutex_);
    DumpKeyAccessStats();
    std::fclose(key_access_file_);
    key_access_file_ = nullptr;
  }
  if (!user_write_bytes_path_.empty()) {
    FILE* f = std::fopen(user_write_bytes_path_.c_str(), "w");
    if (f == nullptr) {
      std::fprintf(stderr,
                   "LevelDB warning: failed to open user_write_bytes.csv\n");
    } else {
      std::fprintf(f, "cumulative_bytes\n");
      std::fprintf(f, "%llu\n",
                   static_cast<unsigned long long>(user_write_bytes_));
      std::fclose(f);
    }
  }

  if (db_lock_ != nullptr) {
    env_->UnlockFile(db_lock_);
  }

  if (pre_l0_ != nullptr) {
    pre_l0_->DumpTimers();
    pre_l0_->DumpStats();
  }
  delete pre_l0_;
  delete versions_;
  if (mem_ != nullptr) mem_->Unref();
  if (imm_ != nullptr) imm_->Unref();
  delete tmp_batch_;
  delete log_;
  delete logfile_;
  delete table_cache_;

  delete twin_env_;
  delete inner_injector_;
  delete host_read_stream_injector_;
  delete host_read_random_injector_;
  delete host_write_injector_;

  if (owns_info_log_) {
    delete options_.info_log;
  }
  if (owns_cache_) {
    delete options_.block_cache;
  }
}

Status DBImpl::NewDB() {
  VersionEdit new_db;
  new_db.SetComparatorName(user_comparator()->Name());
  new_db.SetLogNumber(0);
  new_db.SetNextFile(2);
  new_db.SetLastSequence(0);

  const std::string manifest = DescriptorFileName(dbname_, 1);
  WritableFile* file;
  Status s = env_->NewWritableFile(manifest, &file);
  if (!s.ok()) {
    return s;
  }
  {
    log::Writer log(file);
    std::string record;
    new_db.EncodeTo(&record);
    s = log.AddRecord(record);
    if (s.ok()) {
      s = file->Sync();
    }
    if (s.ok()) {
      s = file->Close();
    }
  }
  delete file;
  if (s.ok()) {
    // Make "CURRENT" file that points to the new manifest file.
    s = SetCurrentFile(env_, dbname_, 1);
  } else {
    env_->RemoveFile(manifest);
  }
  return s;
}

void DBImpl::MaybeIgnoreError(Status* s) const {
  if (s->ok() || options_.paranoid_checks) {
    // No change needed
  } else {
    Log(options_.info_log, "Ignoring error %s", s->ToString().c_str());
    *s = Status::OK();
  }
}

void DBImpl::RemoveObsoleteFiles() {
  mutex_.AssertHeld();

  if (!bg_error_.ok()) {
    // After a background error, we don't know whether a new version may
    // or may not have been committed, so we cannot safely garbage collect.
    return;
  }

  // Make a set of all of the live files
  std::set<uint64_t> live = pending_outputs_;
  versions_->AddLiveFiles(&live);

  std::vector<std::string> filenames;
  env_->GetChildren(dbname_, &filenames);  // Ignoring errors on purpose
  uint64_t number;
  FileType type;
  std::vector<std::string> files_to_delete;
  for (std::string& filename : filenames) {
    if (ParseFileName(filename, &number, &type)) {
      bool keep = true;
      switch (type) {
        case kLogFile:
          keep = ((number >= versions_->LogNumber()) ||
                  (number == versions_->PrevLogNumber()));
          break;
        case kDescriptorFile:
          // Keep my manifest file, and any newer incarnations'
          // (in case there is a race that allows other incarnations)
          keep = (number >= versions_->ManifestFileNumber());
          break;
        case kTableFile:
          keep = (live.find(number) != live.end());
          break;
        case kTempFile:
          // Any temp files that are currently being written to must
          // be recorded in pending_outputs_, which is inserted into "live"
          keep = (live.find(number) != live.end());
          break;
        case kCurrentFile:
        case kDBLockFile:
        case kInfoLogFile:
          keep = true;
          break;
      }

      if (!keep) {
        files_to_delete.push_back(std::move(filename));
        if (type == kTableFile) {
          table_cache_->Evict(number);
        }
        Log(options_.info_log, "Delete type=%d #%lld\n", static_cast<int>(type),
            static_cast<unsigned long long>(number));
      }
    }
  }

  struct WalDeleteInfo {
    uint64_t number;
    uint64_t create_time_us;
    std::string filename;
    uint64_t size_bytes;
  };
  std::vector<WalDeleteInfo> wal_deletes;
  for (const std::string& fname : files_to_delete) {
    uint64_t num;
    FileType t;
    if (ParseFileName(fname, &num, &t) && t == kLogFile) {
      auto it = wal_create_times_.find(num);
      uint64_t create_time = (it != wal_create_times_.end()) ? it->second : 0;
      wal_deletes.push_back({num, create_time, fname, 0});
    }
  }

  // While deleting all files unblock other threads. All files being deleted
  // have unique names which will not collide with newly created files and
  // are therefore safe to delete while allowing other threads to proceed.
  mutex_.Unlock();
  for (WalDeleteInfo& info : wal_deletes) {
    env_->GetFileSize(dbname_ + "/" + info.filename, &info.size_bytes);
  }
  for (const std::string& filename : files_to_delete) {
    env_->RemoveFile(dbname_ + "/" + filename);
  }
  mutex_.Lock();

  uint64_t delete_time_us = env_->NowMicros();
  for (const WalDeleteInfo& info : wal_deletes) {
    wal_create_times_.erase(info.number);
    auto bit = wal_bytes_.find(info.number);
    if (bit != wal_bytes_.end()) {
      live_wal_bytes_ =
          (live_wal_bytes_ >= bit->second) ? live_wal_bytes_ - bit->second : 0;
      wal_bytes_.erase(bit);
    }
    if (info.create_time_us > 0) {
      LogWalMetrics(info.number, info.create_time_us, delete_time_us,
                    info.size_bytes);
    }
  }

  SnapshotWalFootprint(filenames);
}

Status DBImpl::Recover(VersionEdit* edit, bool* save_manifest) {
  mutex_.AssertHeld();

  // Ignore error from CreateDir since the creation of the DB is
  // committed only when the descriptor is created, and this directory
  // may already exist from a previous failed creation attempt.
  env_->CreateDir(dbname_);
  assert(db_lock_ == nullptr);
  Status s = env_->LockFile(LockFileName(dbname_), &db_lock_);
  if (!s.ok()) {
    return s;
  }

  if (!env_->FileExists(CurrentFileName(dbname_))) {
    if (options_.create_if_missing) {
      Log(options_.info_log, "Creating DB %s since it was missing.",
          dbname_.c_str());
      s = NewDB();
      if (!s.ok()) {
        return s;
      }
    } else {
      return Status::InvalidArgument(
          dbname_, "does not exist (create_if_missing is false)");
    }
  } else {
    if (options_.error_if_exists) {
      return Status::InvalidArgument(dbname_,
                                     "exists (error_if_exists is true)");
    }
  }

  s = versions_->Recover(save_manifest);
  if (!s.ok()) {
    return s;
  }
  SequenceNumber max_sequence(0);

  // Recover from all newer log files than the ones named in the
  // descriptor (new log files may have been added by the previous
  // incarnation without registering them in the descriptor).
  //
  // Note that PrevLogNumber() is no longer used, but we pay
  // attention to it in case we are recovering a database
  // produced by an older version of leveldb.
  const uint64_t min_log = versions_->LogNumber();
  const uint64_t prev_log = versions_->PrevLogNumber();
  std::vector<std::string> filenames;
  s = env_->GetChildren(dbname_, &filenames);
  if (!s.ok()) {
    return s;
  }
  std::set<uint64_t> expected;
  versions_->AddLiveFiles(&expected);
  uint64_t number;
  FileType type;
  std::vector<uint64_t> logs;
  for (size_t i = 0; i < filenames.size(); i++) {
    if (ParseFileName(filenames[i], &number, &type)) {
      expected.erase(number);
      if (type == kLogFile && ((number >= min_log) || (number == prev_log)))
        logs.push_back(number);
    }
  }
  if (!expected.empty()) {
    char buf[50];
    std::snprintf(buf, sizeof(buf), "%d missing files; e.g.",
                  static_cast<int>(expected.size()));
    return Status::Corruption(buf, TableFileName(dbname_, *(expected.begin())));
  }

  // Recover in the order in which the logs were generated
  std::sort(logs.begin(), logs.end());

  uint64_t retained_wal_bytes = 0;
  for (size_t i = 0; i < logs.size(); i++) {
    uint64_t lsize = 0;
    if (env_->GetFileSize(LogFileName(dbname_, logs[i]), &lsize).ok()) {
      retained_wal_bytes += lsize;
    }
  }

  uint64_t records_replayed = 0;
  int l0_tables_written = 0;
  auto replay_start = std::chrono::steady_clock::now();
  for (size_t i = 0; i < logs.size(); i++) {
    s = RecoverLogFile(logs[i], (i == logs.size() - 1), save_manifest, edit,
                       &max_sequence, &records_replayed, &l0_tables_written);
    if (!s.ok()) {
      return s;
    }

    // The previous incarnation may not have written any MANIFEST
    // records after allocating this log number.  So we manually
    // update the file number allocation counter in VersionSet.
    versions_->MarkFileNumberUsed(logs[i]);
  }
  auto replay_end = std::chrono::steady_clock::now();
  double recovery_ms =
      std::chrono::duration<double, std::milli>(replay_end - replay_start)
          .count();

  if (!options_.metrics_dir.empty()) {
    std::string path = options_.metrics_dir + "/recovery.csv";
    FILE* f = std::fopen(path.c_str(), "a");
    if (f != nullptr) {
      std::fseek(f, 0, SEEK_END);
      if (std::ftell(f) == 0) {
        std::fprintf(f,
                     "num_logs_replayed,retained_wal_bytes,records_replayed,"
                     "l0_tables_written,recovery_ms\n");
      }
      std::fprintf(f, "%zu,%llu,%llu,%d,%.3f\n", logs.size(),
                   static_cast<unsigned long long>(retained_wal_bytes),
                   static_cast<unsigned long long>(records_replayed),
                   l0_tables_written, recovery_ms);
      std::fclose(f);
    }
  }

  if (versions_->LastSequence() < max_sequence) {
    versions_->SetLastSequence(max_sequence);
  }

  return Status::OK();
}

Status DBImpl::RecoverLogFile(uint64_t log_number, bool last_log,
                              bool* save_manifest, VersionEdit* edit,
                              SequenceNumber* max_sequence,
                              uint64_t* records_replayed,
                              int* l0_tables_written) {
  struct LogReporter : public log::Reader::Reporter {
    Env* env;
    Logger* info_log;
    const char* fname;
    Status* status;  // null if options_.paranoid_checks==false
    void Corruption(size_t bytes, const Status& s) override {
      Log(info_log, "%s%s: dropping %d bytes; %s",
          (this->status == nullptr ? "(ignoring error) " : ""), fname,
          static_cast<int>(bytes), s.ToString().c_str());
      if (this->status != nullptr && this->status->ok()) *this->status = s;
    }
  };

  mutex_.AssertHeld();

  // Open the log file
  std::string fname = LogFileName(dbname_, log_number);
  SequentialFile* file;
  Status status = env_->NewSequentialFile(fname, &file);
  if (!status.ok()) {
    MaybeIgnoreError(&status);
    return status;
  }

  // Create the log reader.
  LogReporter reporter;
  reporter.env = env_;
  reporter.info_log = options_.info_log;
  reporter.fname = fname.c_str();
  reporter.status = (options_.paranoid_checks ? &status : nullptr);
  // We intentionally make log::Reader do checksumming even if
  // paranoid_checks==false so that corruptions cause entire commits
  // to be skipped instead of propagating bad information (like overly
  // large sequence numbers).
  log::Reader reader(file, &reporter, true /*checksum*/, 0 /*initial_offset*/);
  Log(options_.info_log, "Recovering log #%llu",
      (unsigned long long)log_number);

  // Read all the records and add to a memtable
  std::string scratch;
  Slice record;
  WriteBatch batch;
  int compactions = 0;
  MemTable* mem = nullptr;
  while (reader.ReadRecord(&record, &scratch) && status.ok()) {
    (*records_replayed)++;
    if (record.size() < 12) {
      reporter.Corruption(record.size(),
                          Status::Corruption("log record too small"));
      continue;
    }
    WriteBatchInternal::SetContents(&batch, record);

    if (mem == nullptr) {
      mem = new MemTable(internal_comparator_);
      mem->Ref();
    }
    status = WriteBatchInternal::InsertInto(&batch, mem);
    MaybeIgnoreError(&status);
    if (!status.ok()) {
      break;
    }
    const SequenceNumber last_seq = WriteBatchInternal::Sequence(&batch) +
                                    WriteBatchInternal::Count(&batch) - 1;
    if (last_seq > *max_sequence) {
      *max_sequence = last_seq;
    }

    if (mem->ApproximateMemoryUsage() > options_.write_buffer_size) {
      compactions++;
      *save_manifest = true;
      status = WriteLevel0Table(mem, edit, nullptr);
      mem->Unref();
      mem = nullptr;
      if (!status.ok()) {
        // Reflect errors immediately so that conditions like full
        // file-systems cause the DB::Open() to fail.
        break;
      }
    }
  }

  delete file;

  // See if we should keep reusing the last log file.
  if (status.ok() && options_.reuse_logs && last_log && compactions == 0) {
    assert(logfile_ == nullptr);
    assert(log_ == nullptr);
    assert(mem_ == nullptr);
    uint64_t lfile_size;
    if (env_->GetFileSize(fname, &lfile_size).ok() &&
        env_->NewAppendableFile(fname, &logfile_).ok()) {
      Log(options_.info_log, "Reusing old log %s \n", fname.c_str());
      log_ = new log::Writer(logfile_, lfile_size);
      logfile_number_ = log_number;
      if (mem != nullptr) {
        mem_ = mem;
        mem = nullptr;
      } else {
        // mem can be nullptr if lognum exists but was empty.
        mem_ = new MemTable(internal_comparator_);
        mem_->Ref();
      }
    }
  }

  if (mem != nullptr) {
    // mem did not get reused; compact it.
    if (status.ok()) {
      *save_manifest = true;
      status = WriteLevel0Table(mem, edit, nullptr);
    }
    mem->Unref();
  }

  *l0_tables_written += compactions;
  return status;
}

Status DBImpl::WriteLevel0Table(MemTable* mem, VersionEdit* edit,
                                Version* base) {
  mutex_.AssertHeld();
  const uint64_t start_micros = env_->NowMicros();
  FileMetaData meta;
  meta.number = versions_->NewFileNumber();
  pending_outputs_.insert(meta.number);
  Iterator* iter = mem->NewIterator();
  Log(options_.info_log, "Level-0 table #%llu: started",
      (unsigned long long)meta.number);

  if (flush_hotness_file_ != nullptr) {
    FlushHotnessMetrics fhm;
    fhm.timestamp_us = env_->NowMicros();
    fhm.flush_seq = flush_seq_++;

    struct KeyStats { int count; uint64_t bytes; };
    std::vector<KeyStats> per_key;
    uint64_t total_entries = 0;
    uint64_t total_bytes = 0;
    std::string prev_ukey;
    int cur_count = 0;
    uint64_t cur_bytes = 0;

    Iterator* scan = mem->NewIterator();
    for (scan->SeekToFirst(); scan->Valid(); scan->Next()) {
      total_entries++;
      Slice ukey = ExtractUserKey(scan->key());
      uint64_t ebytes = scan->key().size() + scan->value().size();
      total_bytes += ebytes;
      if (ukey.compare(Slice(prev_ukey)) != 0) {
        if (cur_count > 0) per_key.push_back({cur_count, cur_bytes});
        prev_ukey = ukey.ToString();
        cur_count = 1;
        cur_bytes = ebytes;
      } else {
        cur_count++;
        cur_bytes += ebytes;
      }
    }
    if (cur_count > 0) per_key.push_back({cur_count, cur_bytes});
    delete scan;

    uint64_t unique_keys = per_key.size();
    uint64_t hot_keys = 0;
    uint64_t hot_bytes = 0;
    int max_vc = 0;
    std::vector<int> counts;
    counts.reserve(unique_keys);
    for (const auto& ks : per_key) {
      counts.push_back(ks.count);
      if (ks.count > max_vc) max_vc = ks.count;
      if (ks.count >= 2) { hot_keys++; hot_bytes += ks.bytes; }
    }
    int p50 = 1, p90 = 1;
    if (!counts.empty()) {
      std::sort(counts.begin(), counts.end());
      p50 = counts[(counts.size() - 1) * 50 / 100];
      p90 = counts[(counts.size() - 1) * 90 / 100];
    }
    fhm.total_entries = total_entries;
    fhm.unique_keys = unique_keys;
    fhm.hot_keys = hot_keys;
    fhm.hot_key_ratio = unique_keys > 0
        ? static_cast<double>(hot_keys) / unique_keys : 0.0;
    fhm.hot_byte_ratio = total_bytes > 0
        ? static_cast<double>(hot_bytes) / total_bytes : 0.0;
    fhm.max_version_count = max_vc;
    fhm.p50_version_count = p50;
    fhm.p90_version_count = p90;
    LogFlushHotnessMetrics(fhm);
  }

  FlushOverlapMetrics fom;
  fom.timestamp_us = env_->NowMicros();
  fom.memtable_key_count = 0;
  fom.l0_file_count = 0;
  fom.bloom_hit_count = 0;
  if (flush_metrics_file_ != nullptr) {
    const std::vector<FileMetaData*>& l0_files =
        versions_->current()->GetFiles(0);
    fom.l0_file_count = static_cast<int>(l0_files.size());
    if (!l0_files.empty()) {
      Iterator* mem_iter = mem->NewIterator();
      for (mem_iter->SeekToFirst(); mem_iter->Valid(); mem_iter->Next()) {
        fom.memtable_key_count++;
        Slice user_key = ExtractUserKey(mem_iter->key());
        for (const FileMetaData* f : l0_files) {
          if (table_cache_->KeyMayMatch(f->number, f->file_size, user_key)) {
            fom.bloom_hit_count++;
            break;
          }
        }
      }
      delete mem_iter;
    }
  }
  fom.bloom_hit_ratio = (fom.memtable_key_count > 0)
      ? static_cast<double>(fom.bloom_hit_count) / fom.memtable_key_count
      : 0.0;

  Status s;
  {
    mutex_.Unlock();
    s = BuildTable(dbname_, env_, options_, table_cache_, iter, &meta);
    mutex_.Lock();
  }

  Log(options_.info_log, "Level-0 table #%llu: %lld bytes %s",
      (unsigned long long)meta.number, (unsigned long long)meta.file_size,
      s.ToString().c_str());
  delete iter;
  pending_outputs_.erase(meta.number);

  // Note that if file_size is zero, the file has been deleted and
  // should not be added to the manifest.
  int level = 0;
  if (s.ok() && meta.file_size > 0) {
    const Slice min_user_key = meta.smallest.user_key();
    const Slice max_user_key = meta.largest.user_key();
    if (base != nullptr) {
      level = base->PickLevelForMemTableOutput(min_user_key, max_user_key);
    }
    edit->AddFile(level, meta.number, meta.file_size, meta.smallest,
                  meta.largest);
  }

  CompactionStats stats;
  stats.micros = env_->NowMicros() - start_micros;
  stats.bytes_written = meta.file_size;
  stats_[level].Add(stats);

  if (s.ok() && meta.file_size > 0 && twin_env_ == nullptr) {
    g_io_stats.host_write_bytes += meta.file_size;
  }

  if (s.ok() && meta.file_size > 0) {
    fom.new_sst_smallest = meta.smallest.user_key().ToString();
    fom.new_sst_largest = meta.largest.user_key().ToString();
    fom.new_sst_size_bytes = meta.file_size;
    LogFlushOverlapMetrics(fom);
  }

  struct timeval tv;
  gettimeofday(&tv, nullptr);
  CompactionMetrics cm;
  cm.timestamp_us = static_cast<int64_t>(tv.tv_sec) * 1000000 + tv.tv_usec;
  cm.compaction_level = -1;
  cm.input_bytes = 0;
  cm.output_bytes = meta.file_size;
  cm.l0_files_before = versions_->current()->NumFiles(0);
  cm.l0_files_after = versions_->current()->NumFiles(0);
  cm.l0_bytes = versions_->NumLevelBytes(0);
  cm.l1_files_before = versions_->current()->NumFiles(1);
  cm.l1_files_after = versions_->current()->NumFiles(1);
  cm.l1_bytes = versions_->NumLevelBytes(1);
  LogCompactionMetrics(cm);

  return s;
}

void DBImpl::CompactMemTable() {
  mutex_.AssertHeld();
  assert(imm_ != nullptr);

  VersionEdit edit;
  Status s;

  if (options_.enable_pre_l0 && pre_l0_ != nullptr) {
    s = pre_l0_->FlushMemTable(imm_, imm_log_number_, &edit);
    if (s.ok()) {
      edit.SetPrevLogNumber(0);
      uint64_t safe_log = pre_l0_->MinLogNumber();
      edit.SetLogNumber(safe_log > 0 ? safe_log : logfile_number_);

      if (!options_.metrics_dir.empty()) {
        std::string path = options_.metrics_dir + "/wal_gap.csv";
        FILE* f = std::fopen(path.c_str(), "a");
        if (f != nullptr) {
          std::fseek(f, 0, SEEK_END);
          if (std::ftell(f) == 0) {
            std::fprintf(f,
                         "timestamp_us,logfile_number,min_log_number,gap,"
                         "distinct_live_logs\n");
          }
          struct timeval tvg;
          gettimeofday(&tvg, nullptr);
          uint64_t now_us =
              static_cast<uint64_t>(tvg.tv_sec) * 1000000 + tvg.tv_usec;
          std::fprintf(f, "%llu,%llu,%llu,%llu,%zu\n",
                       static_cast<unsigned long long>(now_us),
                       static_cast<unsigned long long>(logfile_number_),
                       static_cast<unsigned long long>(safe_log),
                       static_cast<unsigned long long>(logfile_number_ -
                                                       safe_log),
                       pre_l0_->DistinctLiveLogs());
          std::fclose(f);
        }
      }

      s = versions_->LogAndApply(&edit, &mutex_);
    }
  } else {
    Version* base = versions_->current();
    base->Ref();
    s = WriteLevel0Table(imm_, &edit, base);
    base->Unref();
    if (s.ok()) {
      edit.SetPrevLogNumber(0);
      edit.SetLogNumber(logfile_number_);
      s = versions_->LogAndApply(&edit, &mutex_);
    }
  }

  if (s.ok() && shutting_down_.load(std::memory_order_acquire)) {
    s = Status::IOError("Deleting DB during memtable compaction");
  }

  if (s.ok()) {
    imm_->Unref();
    imm_ = nullptr;
    has_imm_.store(false, std::memory_order_release);
    RemoveObsoleteFiles();
  } else {
    RecordBackgroundError(s);
  }
}

void DBImpl::BackgroundWalSnapshot() {
  mutex_.AssertHeld();
  assert(imm_ == nullptr);
  if (!(options_.enable_pre_l0 && pre_l0_ != nullptr) ||
      options_.pre_l0_wal_snapshot_threshold_bytes == 0 ||
      shutting_down_.load(std::memory_order_acquire)) {
    return;
  }

  const uint64_t t_start_us = env_->NowMicros();
  const uint64_t pre_live_wal_bytes = live_wal_bytes_;

  const SequenceNumber snap_seq = versions_->LastSequence();
  const uint64_t snapshot_num = versions_->NewFileNumber();
  Status s = SwitchToNewWAL();
  if (!s.ok()) {
    RecordBackgroundError(s);
    return;
  }
  assert(logfile_number_ == snapshot_num + 1 &&
         "rotation WAL must sort immediately above snapshot_num");
  const uint64_t imm_log = imm_log_number_;

  Iterator* iter = pre_l0_->NewIterator();

  uint64_t entries_written = 0;
  mutex_.Unlock();
  Status ws = WriteWalSnapshotFile(snapshot_num, snap_seq, iter, &entries_written);
  delete iter;
  mutex_.Lock();

  if (shutting_down_.load(std::memory_order_acquire)) {
    return;
  }
  if (!ws.ok()) {
    RecordBackgroundError(ws);
    return;
  }

  pre_l0_->RepointRetentionToSnapshot(snapshot_num);
  const uint64_t new_floor = std::min(snapshot_num, imm_log);
  VersionEdit edit;
  edit.SetPrevLogNumber(0);
  edit.SetLogNumber(new_floor);
  s = versions_->LogAndApply(&edit, &mutex_);
  if (!s.ok()) {
    RecordBackgroundError(s);
    return;
  }
  RemoveObsoleteFiles();

  uint64_t snap_size = 0;
  env_->GetFileSize(LogFileName(dbname_, snapshot_num), &snap_size);
  ++wal_snapshot_count_;

  if (!options_.metrics_dir.empty()) {
    const uint64_t dur_us = env_->NowMicros() - t_start_us;
    std::string path = options_.metrics_dir + "/pre_l0_wal_snapshot.csv";
    FILE* f = std::fopen(path.c_str(), "a");
    if (f != nullptr) {
      std::fseek(f, 0, SEEK_END);
      if (std::ftell(f) == 0) {
        std::fprintf(f,
                     "timestamp_us,snapshot_num,snap_seq,new_floor,"
                     "pre_live_wal_bytes,post_live_wal_bytes,snapshot_file_bytes,"
                     "entries_written,duration_us\n");
      }
      std::fprintf(f, "%llu,%llu,%llu,%llu,%llu,%llu,%llu,%llu,%llu\n",
                   static_cast<unsigned long long>(t_start_us),
                   static_cast<unsigned long long>(snapshot_num),
                   static_cast<unsigned long long>(snap_seq),
                   static_cast<unsigned long long>(new_floor),
                   static_cast<unsigned long long>(pre_live_wal_bytes),
                   static_cast<unsigned long long>(live_wal_bytes_),
                   static_cast<unsigned long long>(snap_size),
                   static_cast<unsigned long long>(entries_written),
                   static_cast<unsigned long long>(dur_us));
      std::fclose(f);
    }
  }
}

Status DBImpl::WriteWalSnapshotFile(uint64_t snapshot_num,
                                    SequenceNumber snap_seq, Iterator* iter,
                                    uint64_t* entries_written) {
  *entries_written = 0;
  const std::string tmp = TempFileName(dbname_, snapshot_num);
  WritableFile* file = nullptr;
  Status s = env_->NewWritableFile(tmp, &file);
  if (!s.ok()) {
    return s;
  }

  log::Writer writer(file);
  uint64_t count = 0;
  for (iter->SeekToFirst(); iter->Valid() && s.ok(); iter->Next()) {
    ParsedInternalKey ikey;
    if (!ParseInternalKey(iter->key(), &ikey)) {
      s = Status::Corruption("pre-L0 WAL snapshot: bad internal key");
      break;
    }
    if (ikey.sequence > snap_seq) {
      continue;
    }
    WriteBatch batch;
    if (ikey.type == kTypeDeletion) {
      batch.Delete(ikey.user_key);
    } else {
      batch.Put(ikey.user_key, iter->value());
    }
    WriteBatchInternal::SetSequence(&batch, ikey.sequence);
    s = writer.AddRecord(WriteBatchInternal::Contents(&batch));
    ++count;
  }

  if (s.ok()) {
    s = file->Sync();
  }
  Status close_s = file->Close();
  if (s.ok()) {
    s = close_s;
  }
  delete file;

  if (!s.ok()) {
    env_->RemoveFile(tmp);
    return s;
  }
  s = env_->RenameFile(tmp, LogFileName(dbname_, snapshot_num));
  if (!s.ok()) {
    env_->RemoveFile(tmp);
    return s;
  }
  *entries_written = count;
  return s;
}

void DBImpl::CompactRange(const Slice* begin, const Slice* end) {
  int max_level_with_files = 1;
  {
    MutexLock l(&mutex_);
    Version* base = versions_->current();
    for (int level = 1; level < config::kNumLevels; level++) {
      if (base->OverlapInLevel(level, begin, end)) {
        max_level_with_files = level;
      }
    }
  }
  TEST_CompactMemTable();  // TODO(sanjay): Skip if memtable does not overlap
  for (int level = 0; level < max_level_with_files; level++) {
    TEST_CompactRange(level, begin, end);
  }
}

void DBImpl::TEST_CompactRange(int level, const Slice* begin,
                               const Slice* end) {
  assert(level >= 0);
  assert(level + 1 < config::kNumLevels);

  InternalKey begin_storage, end_storage;

  ManualCompaction manual;
  manual.level = level;
  manual.done = false;
  if (begin == nullptr) {
    manual.begin = nullptr;
  } else {
    begin_storage = InternalKey(*begin, kMaxSequenceNumber, kValueTypeForSeek);
    manual.begin = &begin_storage;
  }
  if (end == nullptr) {
    manual.end = nullptr;
  } else {
    end_storage = InternalKey(*end, 0, static_cast<ValueType>(0));
    manual.end = &end_storage;
  }

  MutexLock l(&mutex_);
  while (!manual.done && !shutting_down_.load(std::memory_order_acquire) &&
         bg_error_.ok()) {
    if (manual_compaction_ == nullptr) {  // Idle
      manual_compaction_ = &manual;
      MaybeScheduleCompaction();
    } else {  // Running either my compaction or another compaction.
      background_work_finished_signal_.Wait();
    }
  }
  // Finish current background compaction in the case where
  // `background_work_finished_signal_` was signalled due to an error.
  while (background_compaction_scheduled_) {
    background_work_finished_signal_.Wait();
  }
  if (manual_compaction_ == &manual) {
    // Cancel my manual compaction since we aborted early for some reason.
    manual_compaction_ = nullptr;
  }
}

Status DBImpl::TEST_CompactMemTable() {
  // nullptr batch means just wait for earlier writes to be done
  Status s = Write(WriteOptions(), nullptr);
  if (s.ok()) {
    // Wait until the compaction completes
    MutexLock l(&mutex_);
    while (imm_ != nullptr && bg_error_.ok() &&
           !shutting_down_.load(std::memory_order_acquire)) {
      background_work_finished_signal_.Wait();
    }
    if (imm_ != nullptr) {
      s = bg_error_;
    }
  }
  return s;
}

void DBImpl::RecordBackgroundError(const Status& s) {
  mutex_.AssertHeld();
  if (bg_error_.ok()) {
    bg_error_ = s;
    background_work_finished_signal_.SignalAll();
  }
}

void DBImpl::MaybeScheduleCompaction() {
  mutex_.AssertHeld();
  if (background_compaction_scheduled_) {
    // Already scheduled
  } else if (shutting_down_.load(std::memory_order_acquire)) {
    // DB is being deleted; no more background compactions
  } else if (!bg_error_.ok()) {
    // Already got an error; no more changes
  } else if (imm_ == nullptr && manual_compaction_ == nullptr &&
             !versions_->NeedsCompaction() && !wal_snapshot_requested_) {
    // No work to be done
  } else {
    background_compaction_scheduled_ = true;
    env_->Schedule(&DBImpl::BGWork, this);
  }
}

void DBImpl::BGWork(void* db) {
  reinterpret_cast<DBImpl*>(db)->BackgroundCall();
}

void DBImpl::BackgroundCall() {
  MutexLock l(&mutex_);
  assert(background_compaction_scheduled_);
  if (shutting_down_.load(std::memory_order_acquire)) {
    // No more background work when shutting down.
  } else if (!bg_error_.ok()) {
    // No more background work after a background error.
  } else {
    BackgroundCompaction();
  }

  background_compaction_scheduled_ = false;

  // Previous compaction may have produced too many files in a level,
  // so reschedule another compaction if needed.
  MaybeScheduleCompaction();
  background_work_finished_signal_.SignalAll();
}

void DBImpl::BackgroundCompaction() {
  mutex_.AssertHeld();

  if (imm_ != nullptr) {
    CompactMemTable();
    return;
  }

  if (wal_snapshot_requested_) {
    wal_snapshot_requested_ = false;
    BackgroundWalSnapshot();
    return;
  }

  Compaction* c;
  bool is_manual = (manual_compaction_ != nullptr);
  InternalKey manual_end;
  if (is_manual) {
    ManualCompaction* m = manual_compaction_;
    c = versions_->CompactRange(m->level, m->begin, m->end);
    m->done = (c == nullptr);
    if (c != nullptr) {
      manual_end = c->input(0, c->num_input_files(0) - 1)->largest;
    }
    Log(options_.info_log,
        "Manual compaction at level-%d from %s .. %s; will stop at %s\n",
        m->level, (m->begin ? m->begin->DebugString().c_str() : "(begin)"),
        (m->end ? m->end->DebugString().c_str() : "(end)"),
        (m->done ? "(end)" : manual_end.DebugString().c_str()));
  } else {
    c = versions_->PickCompaction();
  }

  Status status;
  if (c == nullptr) {
    // Nothing to do
  } else if (!is_manual && c->IsTrivialMove()) {
    // Move file to next level
    assert(c->num_input_files(0) == 1);
    FileMetaData* f = c->input(0, 0);
    c->edit()->RemoveFile(c->level(), f->number);
    c->edit()->AddFile(c->level() + 1, f->number, f->file_size, f->smallest,
                       f->largest);
    status = versions_->LogAndApply(c->edit(), &mutex_);
    if (!status.ok()) {
      RecordBackgroundError(status);
    }
    VersionSet::LevelSummaryStorage tmp;
    Log(options_.info_log, "Moved #%lld to level-%d %lld bytes %s: %s\n",
        static_cast<unsigned long long>(f->number), c->level() + 1,
        static_cast<unsigned long long>(f->file_size),
        status.ToString().c_str(), versions_->LevelSummary(&tmp));
  } else {
    CompactionState* compact = new CompactionState(c);
    status = DoCompactionWork(compact);
    if (!status.ok()) {
      RecordBackgroundError(status);
    }
    CleanupCompaction(compact);
    c->ReleaseInputs();
    RemoveObsoleteFiles();
  }
  delete c;

  if (status.ok()) {
    // Done
  } else if (shutting_down_.load(std::memory_order_acquire)) {
    // Ignore compaction errors found during shutting down
  } else {
    Log(options_.info_log, "Compaction error: %s", status.ToString().c_str());
  }

  if (is_manual) {
    ManualCompaction* m = manual_compaction_;
    if (!status.ok()) {
      m->done = true;
    }
    if (!m->done) {
      // We only compacted part of the requested range.  Update *m
      // to the range that is left to be compacted.
      m->tmp_storage = manual_end;
      m->begin = &m->tmp_storage;
    }
    manual_compaction_ = nullptr;
  }
}

void DBImpl::CleanupCompaction(CompactionState* compact) {
  mutex_.AssertHeld();
  if (compact->builder != nullptr) {
    // May happen if we get a shutdown call in the middle of compaction
    compact->builder->Abandon();
    delete compact->builder;
  } else {
    assert(compact->outfile == nullptr);
  }
  delete compact->outfile;
  for (size_t i = 0; i < compact->outputs.size(); i++) {
    const CompactionState::Output& out = compact->outputs[i];
    pending_outputs_.erase(out.number);
  }
  delete compact;
}

Status DBImpl::OpenCompactionOutputFile(CompactionState* compact) {
  assert(compact != nullptr);
  assert(compact->builder == nullptr);
  uint64_t file_number;
  {
    mutex_.Lock();
    file_number = versions_->NewFileNumber();
    pending_outputs_.insert(file_number);
    CompactionState::Output out;
    out.number = file_number;
    out.file_size = 0;
    out.smallest.Clear();
    out.largest.Clear();
    compact->outputs.push_back(out);
    mutex_.Unlock();
  }

  // Make the output file
  std::string fname = TableFileName(dbname_, file_number);
  Status s = env_->NewWritableFile(fname, &compact->outfile);
  if (s.ok()) {
    compact->builder = new TableBuilder(options_, compact->outfile);
  }
  return s;
}

Status DBImpl::FinishCompactionOutputFile(CompactionState* compact,
                                          Iterator* input) {
  assert(compact != nullptr);
  assert(compact->outfile != nullptr);
  assert(compact->builder != nullptr);

  const uint64_t output_number = compact->current_output()->number;
  assert(output_number != 0);

  // Check for iterator errors
  Status s = input->status();
  const uint64_t current_entries = compact->builder->NumEntries();
  if (s.ok()) {
    s = compact->builder->Finish();
  } else {
    compact->builder->Abandon();
  }
  const uint64_t current_bytes = compact->builder->FileSize();
  compact->current_output()->file_size = current_bytes;
  compact->total_bytes += current_bytes;
  delete compact->builder;
  compact->builder = nullptr;

  // Finish and check for file errors
  if (s.ok()) {
    s = compact->outfile->Sync();
  }
  if (s.ok()) {
    s = compact->outfile->Close();
  }
  delete compact->outfile;
  compact->outfile = nullptr;

  if (s.ok() && current_entries > 0) {
    // Verify that the table is usable
    Iterator* iter =
        table_cache_->NewIterator(ReadOptions(), output_number, current_bytes);
    s = iter->status();
    delete iter;
    if (s.ok()) {
      Log(options_.info_log, "Generated table #%llu@%d: %lld keys, %lld bytes",
          (unsigned long long)output_number, compact->compaction->level(),
          (unsigned long long)current_entries,
          (unsigned long long)current_bytes);
    }
  }
  return s;
}

Status DBImpl::InstallCompactionResults(CompactionState* compact) {
  mutex_.AssertHeld();
  Log(options_.info_log, "Compacted %d@%d + %d@%d files => %lld bytes",
      compact->compaction->num_input_files(0), compact->compaction->level(),
      compact->compaction->num_input_files(1), compact->compaction->level() + 1,
      static_cast<long long>(compact->total_bytes));

  // Add compaction outputs
  compact->compaction->AddInputDeletions(compact->compaction->edit());
  const int level = compact->compaction->level();
  for (size_t i = 0; i < compact->outputs.size(); i++) {
    const CompactionState::Output& out = compact->outputs[i];
    compact->compaction->edit()->AddFile(level + 1, out.number, out.file_size,
                                         out.smallest, out.largest);
  }
  return versions_->LogAndApply(compact->compaction->edit(), &mutex_);
}

void DBImpl::LogCompactionMetrics(const CompactionMetrics& m) {
  mutex_.AssertHeld();
  if (metrics_file_ == nullptr) return;
  std::fprintf(metrics_file_,
               "%lld,%d,%lld,%lld,%d,%d,%lld,%d,%d,%lld,%lld,%lld,%lld,%lld\n",
               static_cast<long long>(m.timestamp_us), m.compaction_level,
               static_cast<long long>(m.input_bytes),
               static_cast<long long>(m.output_bytes), m.l0_files_before,
               m.l0_files_after, static_cast<long long>(m.l0_bytes),
               m.l1_files_before, m.l1_files_after,
               static_cast<long long>(m.l1_bytes),
               static_cast<long long>(m.peak_resident_data_block_bytes),
               static_cast<long long>(m.peak_resident_data_block_count),
               static_cast<long long>(m.total_data_block_bytes),
               static_cast<long long>(m.total_data_block_count));
  std::fflush(metrics_file_);
}

void DBImpl::LogFlushHotnessMetrics(const FlushHotnessMetrics& m) {
  mutex_.AssertHeld();
  if (flush_hotness_file_ == nullptr) return;
  std::fprintf(flush_hotness_file_,
               "%llu,%llu,%llu,%llu,%llu,%.6f,%.6f,%d,%d,%d\n",
               static_cast<unsigned long long>(m.timestamp_us),
               static_cast<unsigned long long>(m.flush_seq),
               static_cast<unsigned long long>(m.total_entries),
               static_cast<unsigned long long>(m.unique_keys),
               static_cast<unsigned long long>(m.hot_keys),
               m.hot_key_ratio, m.hot_byte_ratio,
               m.max_version_count, m.p50_version_count, m.p90_version_count);
  std::fflush(flush_hotness_file_);
}

void DBImpl::LogFlushOverlapMetrics(const FlushOverlapMetrics& m) {
  mutex_.AssertHeld();
  if (flush_metrics_file_ == nullptr) return;
  std::fprintf(flush_metrics_file_, "%llu,%d,%d,%d,%.4f,%s,%s,%llu\n",
               static_cast<unsigned long long>(m.timestamp_us),
               m.memtable_key_count, m.l0_file_count, m.bloom_hit_count,
               m.bloom_hit_ratio, m.new_sst_smallest.c_str(),
               m.new_sst_largest.c_str(),
               static_cast<unsigned long long>(m.new_sst_size_bytes));
  std::fflush(flush_metrics_file_);
}

void DBImpl::LogWalMetrics(uint64_t wal_number, uint64_t create_time_us,
                            uint64_t delete_time_us, uint64_t wal_size_bytes) {
  mutex_.AssertHeld();
  if (wal_metrics_file_ == nullptr) return;
  uint64_t lifetime_ms =
      (delete_time_us >= create_time_us) ? (delete_time_us - create_time_us) / 1000 : 0;
  std::fprintf(wal_metrics_file_, "%llu,%llu,%llu,%llu,%llu\n",
               static_cast<unsigned long long>(wal_number),
               static_cast<unsigned long long>(create_time_us),
               static_cast<unsigned long long>(delete_time_us),
               static_cast<unsigned long long>(lifetime_ms),
               static_cast<unsigned long long>(wal_size_bytes));
  std::fflush(wal_metrics_file_);
}

void DBImpl::SnapshotWalFootprint(const std::vector<std::string>& filenames) {
  mutex_.AssertHeld();
  if (wal_footprint_file_ == nullptr) return;

  const uint64_t oldest_retained_log = versions_->LogNumber();
  const uint64_t prev_log_number = versions_->PrevLogNumber();
  uint64_t retained_wal_count = 0;
  uint64_t retained_wal_bytes = 0;
  uint64_t number;
  FileType type;
  for (const std::string& filename : filenames) {
    if (ParseFileName(filename, &number, &type) && type == kLogFile &&
        ((number >= oldest_retained_log) || (number == prev_log_number))) {
      uint64_t size_bytes = 0;
      env_->GetFileSize(dbname_ + "/" + filename, &size_bytes);
      retained_wal_count++;
      retained_wal_bytes += size_bytes;
    }
  }

  const uint64_t log_gap = (logfile_number_ >= oldest_retained_log)
                               ? (logfile_number_ - oldest_retained_log)
                               : 0;
  std::fprintf(wal_footprint_file_, "%llu,%llu,%llu,%llu,%llu,%llu\n",
               static_cast<unsigned long long>(env_->NowMicros()),
               static_cast<unsigned long long>(retained_wal_count),
               static_cast<unsigned long long>(retained_wal_bytes),
               static_cast<unsigned long long>(oldest_retained_log),
               static_cast<unsigned long long>(logfile_number_),
               static_cast<unsigned long long>(log_gap));
  std::fflush(wal_footprint_file_);
}

void DBImpl::LogUserWriteBytes(uint64_t batch_bytes) {
  mutex_.AssertHeld();
  user_write_bytes_ += batch_bytes;
}

void DBImpl::EnableKeyAccessTracking() {
  MutexLock l(&mutex_);
  key_access_map_.clear();
}

void DBImpl::DumpKeyAccessStats() {
  mutex_.AssertHeld();
  if (key_access_file_ == nullptr) return;
  for (const auto& [key, stats] : key_access_map_) {
    std::fprintf(key_access_file_, "%s,%llu,%llu,%llu\n", key.c_str(),
                 static_cast<unsigned long long>(stats.read_count),
                 static_cast<unsigned long long>(stats.write_count),
                 static_cast<unsigned long long>(stats.read_count +
                                                 stats.write_count));
  }
  std::fflush(key_access_file_);
}

Status DBImpl::DoCompactionWork(CompactionState* compact) {
  const uint64_t start_micros = env_->NowMicros();
  int64_t imm_micros = 0;  // Micros spent doing imm_ compactions

  Log(options_.info_log, "Compacting %d@%d + %d@%d files",
      compact->compaction->num_input_files(0), compact->compaction->level(),
      compact->compaction->num_input_files(1),
      compact->compaction->level() + 1);

  assert(versions_->NumLevelFiles(compact->compaction->level()) > 0);
  assert(compact->builder == nullptr);
  assert(compact->outfile == nullptr);
  if (snapshots_.empty()) {
    compact->smallest_snapshot = versions_->LastSequence();
  } else {
    compact->smallest_snapshot = snapshots_.oldest()->sequence_number();
  }

  Iterator* input = versions_->MakeInputIterator(compact->compaction);

  // Release mutex while we're actually doing the compaction work
  mutex_.Unlock();

  Status status;
  {
    IoClassScope io_scope(IoClass::kCompaction);

    g_data_block_ws.ResetPeak();

    input->SeekToFirst();
  ParsedInternalKey ikey;
  std::string current_user_key;
  bool has_current_user_key = false;
  SequenceNumber last_sequence_for_key = kMaxSequenceNumber;
  while (input->Valid() && !shutting_down_.load(std::memory_order_acquire)) {
    // Prioritize immutable compaction work
    if (has_imm_.load(std::memory_order_relaxed)) {
      const uint64_t imm_start = env_->NowMicros();
      mutex_.Lock();
      if (imm_ != nullptr) {
        CompactMemTable();
        // Wake up MakeRoomForWrite() if necessary.
        background_work_finished_signal_.SignalAll();
      }
      mutex_.Unlock();
      imm_micros += (env_->NowMicros() - imm_start);
    }

    Slice key = input->key();
    if (compact->compaction->ShouldStopBefore(key) &&
        compact->builder != nullptr) {
      status = FinishCompactionOutputFile(compact, input);
      if (!status.ok()) {
        break;
      }
    }

    // Handle key/value, add to state, etc.
    bool drop = false;
    if (!ParseInternalKey(key, &ikey)) {
      // Do not hide error keys
      current_user_key.clear();
      has_current_user_key = false;
      last_sequence_for_key = kMaxSequenceNumber;
    } else {
      if (!has_current_user_key ||
          user_comparator()->Compare(ikey.user_key, Slice(current_user_key)) !=
              0) {
        // First occurrence of this user key
        current_user_key.assign(ikey.user_key.data(), ikey.user_key.size());
        has_current_user_key = true;
        last_sequence_for_key = kMaxSequenceNumber;
      }

      if (last_sequence_for_key <= compact->smallest_snapshot) {
        // Hidden by an newer entry for same user key
        drop = true;  // (A)
      } else if (ikey.type == kTypeDeletion &&
                 ikey.sequence <= compact->smallest_snapshot &&
                 compact->compaction->IsBaseLevelForKey(ikey.user_key)) {
        // For this user key:
        // (1) there is no data in higher levels
        // (2) data in lower levels will have larger sequence numbers
        // (3) data in layers that are being compacted here and have
        //     smaller sequence numbers will be dropped in the next
        //     few iterations of this loop (by rule (A) above).
        // Therefore this deletion marker is obsolete and can be dropped.
        drop = true;
      }

      last_sequence_for_key = ikey.sequence;
    }
#if 0
    Log(options_.info_log,
        "  Compact: %s, seq %d, type: %d %d, drop: %d, is_base: %d, "
        "%d smallest_snapshot: %d",
        ikey.user_key.ToString().c_str(),
        (int)ikey.sequence, ikey.type, kTypeValue, drop,
        compact->compaction->IsBaseLevelForKey(ikey.user_key),
        (int)last_sequence_for_key, (int)compact->smallest_snapshot);
#endif

    if (!drop) {
      // Open output file if necessary
      if (compact->builder == nullptr) {
        status = OpenCompactionOutputFile(compact);
        if (!status.ok()) {
          break;
        }
      }
      if (compact->builder->NumEntries() == 0) {
        compact->current_output()->smallest.DecodeFrom(key);
      }
      compact->current_output()->largest.DecodeFrom(key);
      compact->builder->Add(key, input->value());

      // Close output file if it is big enough
      if (compact->builder->FileSize() >=
          compact->compaction->MaxOutputFileSize()) {
        status = FinishCompactionOutputFile(compact, input);
        if (!status.ok()) {
          break;
        }
      }
    }

    input->Next();
  }

  if (status.ok() && shutting_down_.load(std::memory_order_acquire)) {
    status = Status::IOError("Deleting DB during compaction");
  }
  if (status.ok() && compact->builder != nullptr) {
    status = FinishCompactionOutputFile(compact, input);
  }
  if (status.ok()) {
    status = input->status();
  }
  delete input;
  input = nullptr;
  }

  CompactionStats stats;
  stats.micros = env_->NowMicros() - start_micros - imm_micros;
  for (int which = 0; which < 2; which++) {
    for (int i = 0; i < compact->compaction->num_input_files(which); i++) {
      stats.bytes_read += compact->compaction->input(which, i)->file_size;
    }
  }
  for (size_t i = 0; i < compact->outputs.size(); i++) {
    stats.bytes_written += compact->outputs[i].file_size;
  }

  if (twin_env_ == nullptr) {
    g_io_stats.host_read_bytes += stats.bytes_read;
    g_io_stats.host_write_bytes += stats.bytes_written;
  }

  mutex_.Lock();
  stats_[compact->compaction->level() + 1].Add(stats);

  const int l0_before = versions_->current()->NumFiles(0);
  const int l1_before = versions_->current()->NumFiles(1);

  if (status.ok()) {
    status = InstallCompactionResults(compact);
  }
  if (!status.ok()) {
    RecordBackgroundError(status);
  }

  struct timeval tv;
  gettimeofday(&tv, nullptr);
  CompactionMetrics cm;
  cm.timestamp_us = static_cast<int64_t>(tv.tv_sec) * 1000000 + tv.tv_usec;
  cm.compaction_level = compact->compaction->level();
  cm.input_bytes = stats.bytes_read;
  cm.output_bytes = stats.bytes_written;
  cm.l0_files_before = l0_before;
  cm.l0_files_after = versions_->current()->NumFiles(0);
  cm.l0_bytes = versions_->NumLevelBytes(0);
  cm.l1_files_before = l1_before;
  cm.l1_files_after = versions_->current()->NumFiles(1);
  cm.l1_bytes = versions_->NumLevelBytes(1);
  cm.peak_resident_data_block_bytes =
      static_cast<int64_t>(g_data_block_ws.peak_bytes.load());
  cm.peak_resident_data_block_count =
      static_cast<int64_t>(g_data_block_ws.peak_count.load());
  cm.total_data_block_bytes =
      static_cast<int64_t>(g_data_block_ws.total_bytes.load());
  cm.total_data_block_count =
      static_cast<int64_t>(g_data_block_ws.total_count.load());
  LogCompactionMetrics(cm);

  VersionSet::LevelSummaryStorage tmp;
  Log(options_.info_log, "compacted to: %s", versions_->LevelSummary(&tmp));
  return status;
}

namespace {

struct IterState {
  port::Mutex* const mu;
  Version* const version GUARDED_BY(mu);
  MemTable* const mem GUARDED_BY(mu);
  MemTable* const imm GUARDED_BY(mu);

  IterState(port::Mutex* mutex, MemTable* mem, MemTable* imm, Version* version)
      : mu(mutex), version(version), mem(mem), imm(imm) {}
};

static void CleanupIteratorState(void* arg1, void* arg2) {
  IterState* state = reinterpret_cast<IterState*>(arg1);
  state->mu->Lock();
  state->mem->Unref();
  if (state->imm != nullptr) state->imm->Unref();
  state->version->Unref();
  state->mu->Unlock();
  delete state;
}

}  // anonymous namespace

Iterator* DBImpl::NewInternalIterator(const ReadOptions& options,
                                      SequenceNumber* latest_snapshot,
                                      uint32_t* seed) {
  mutex_.Lock();
  *latest_snapshot = versions_->LastSequence();

  // Collect together all needed child iterators
  std::vector<Iterator*> list;
  list.push_back(mem_->NewIterator());
  mem_->Ref();
  if (imm_ != nullptr) {
    list.push_back(imm_->NewIterator());
    imm_->Ref();
  }
  if (options_.enable_pre_l0 && pre_l0_ != nullptr) {
    list.push_back(pre_l0_->NewIterator());
  }
  versions_->current()->AddIterators(options, &list);
  Iterator* internal_iter =
      NewMergingIterator(&internal_comparator_, &list[0], list.size());
  versions_->current()->Ref();

  IterState* cleanup = new IterState(&mutex_, mem_, imm_, versions_->current());
  internal_iter->RegisterCleanup(CleanupIteratorState, cleanup, nullptr);

  *seed = ++seed_;
  mutex_.Unlock();
  return internal_iter;
}

Iterator* DBImpl::TEST_NewInternalIterator() {
  SequenceNumber ignored;
  uint32_t ignored_seed;
  return NewInternalIterator(ReadOptions(), &ignored, &ignored_seed);
}

int64_t DBImpl::TEST_MaxNextLevelOverlappingBytes() {
  MutexLock l(&mutex_);
  return versions_->MaxNextLevelOverlappingBytes();
}

uint64_t DBImpl::TEST_PreL0LinkValueBytes() {
  MutexLock l(&mutex_);
  return pre_l0_ != nullptr ? pre_l0_->LinkValueBytes() : 0;
}

uint64_t DBImpl::TEST_PreL0MinLogNumber() {
  MutexLock l(&mutex_);
  return pre_l0_ != nullptr ? pre_l0_->MinLogNumber() : 0;
}

Status DBImpl::TEST_TriggerWalSnapshot() {
  MutexLock l(&mutex_);
  const uint64_t before = wal_snapshot_count_;
  wal_snapshot_requested_ = true;
  MaybeScheduleCompaction();
  while (wal_snapshot_count_ == before && bg_error_.ok() &&
         !shutting_down_.load(std::memory_order_acquire)) {
    background_work_finished_signal_.Wait();
  }
  return bg_error_;
}

uint64_t DBImpl::TEST_LiveWalBytes() {
  MutexLock l(&mutex_);
  return live_wal_bytes_;
}

uint64_t DBImpl::TEST_WalSnapshotCount() {
  MutexLock l(&mutex_);
  return wal_snapshot_count_;
}

Status DBImpl::Get(const ReadOptions& options, const Slice& key,
                   std::string* value) {
  Status s;
  MutexLock l(&mutex_);
  SequenceNumber snapshot;
  if (options.snapshot != nullptr) {
    snapshot =
        static_cast<const SnapshotImpl*>(options.snapshot)->sequence_number();
  } else {
    snapshot = versions_->LastSequence();
  }

  MemTable* mem = mem_;
  MemTable* imm = imm_;
  Version* current = versions_->current();
  mem->Ref();
  if (imm != nullptr) imm->Ref();
  current->Ref();

  bool have_stat_update = false;
  Version::GetStats stats;

  // Unlock while reading from files and memtables
  {
    mutex_.Unlock();
    // First look in the memtable, then in the immutable memtable (if any).
    LookupKey lkey(key, snapshot);
    if (mem->Get(lkey, value, &s)) {
      // Done
    } else if (imm != nullptr && imm->Get(lkey, value, &s)) {
      // Done
    } else if (options_.enable_pre_l0 && pre_l0_ != nullptr &&
               pre_l0_->Get(lkey, value, &s)) {
      // Done
    } else {
      s = current->Get(options, lkey, value, &stats);
      have_stat_update = true;
    }
    mutex_.Lock();
  }

  if (have_stat_update && current->UpdateStats(stats)) {
    MaybeScheduleCompaction();
  }
  if (key_access_file_ != nullptr) {
    key_access_map_[key.ToString()].read_count++;
  }
  mem->Unref();
  if (imm != nullptr) imm->Unref();
  current->Unref();
  return s;
}

Iterator* DBImpl::NewIterator(const ReadOptions& options) {
  SequenceNumber latest_snapshot;
  uint32_t seed;
  Iterator* iter = NewInternalIterator(options, &latest_snapshot, &seed);
  return NewDBIterator(this, user_comparator(), iter,
                       (options.snapshot != nullptr
                            ? static_cast<const SnapshotImpl*>(options.snapshot)
                                  ->sequence_number()
                            : latest_snapshot),
                       seed);
}

void DBImpl::RecordReadSample(Slice key) {
  MutexLock l(&mutex_);
  if (versions_->current()->RecordReadSample(key)) {
    MaybeScheduleCompaction();
  }
}

const Snapshot* DBImpl::GetSnapshot() {
  MutexLock l(&mutex_);
  return snapshots_.New(versions_->LastSequence());
}

void DBImpl::ReleaseSnapshot(const Snapshot* snapshot) {
  MutexLock l(&mutex_);
  snapshots_.Delete(static_cast<const SnapshotImpl*>(snapshot));
}

// Convenience methods
Status DBImpl::Put(const WriteOptions& o, const Slice& key, const Slice& val) {
  return DB::Put(o, key, val);
}

Status DBImpl::Delete(const WriteOptions& options, const Slice& key) {
  return DB::Delete(options, key);
}

Status DBImpl::Write(const WriteOptions& options, WriteBatch* updates) {
  Writer w(&mutex_);
  w.batch = updates;
  w.sync = options.sync;
  w.done = false;

  MutexLock l(&mutex_);
  writers_.push_back(&w);
  while (!w.done && &w != writers_.front()) {
    w.cv.Wait();
  }
  if (w.done) {
    return w.status;
  }

  // May temporarily unlock and wait.
  Status status = MakeRoomForWrite(updates == nullptr);
  uint64_t last_sequence = versions_->LastSequence();
  Writer* last_writer = &w;
  if (status.ok() && updates != nullptr) {  // nullptr batch is for compactions
    WriteBatch* write_batch = BuildBatchGroup(&last_writer);
    WriteBatchInternal::SetSequence(write_batch, last_sequence + 1);
    last_sequence += WriteBatchInternal::Count(write_batch);

    // Add to log and apply to memtable.  We can release the lock
    // during this phase since &w is currently responsible for logging
    // and protects against concurrent loggers and concurrent writes
    // into mem_.
    const uint64_t wal_append_number = logfile_number_;
    const uint64_t wal_append_bytes = WriteBatchInternal::ByteSize(write_batch);
    {
      mutex_.Unlock();
      status = log_->AddRecord(WriteBatchInternal::Contents(write_batch));
      bool sync_error = false;
      if (status.ok() && options.sync) {
        status = logfile_->Sync();
        if (!status.ok()) {
          sync_error = true;
        }
      }
      if (status.ok()) {
        status = WriteBatchInternal::InsertInto(write_batch, mem_);
      }
      mutex_.Lock();
      if (status.ok()) {
        AddWalBytes(wal_append_number, wal_append_bytes);
      }
      if (sync_error) {
        // The state of the log file is indeterminate: the log record we
        // just added may or may not show up when the DB is re-opened.
        // So we force the DB into a mode where all future writes fail.
        RecordBackgroundError(status);
      }
    }
    if (status.ok()) {
      LogUserWriteBytes(WriteBatchInternal::ByteSize(updates));
    }
    if (status.ok() && key_access_file_ != nullptr) {
      struct KeyCounter : public WriteBatch::Handler {
        std::unordered_map<std::string, KeyAccessStats>* map;
        void Put(const Slice& key, const Slice&) override {
          (*map)[key.ToString()].write_count++;
        }
        void Delete(const Slice& key) override {
          (*map)[key.ToString()].write_count++;
        }
      };
      KeyCounter counter;
      counter.map = &key_access_map_;
      write_batch->Iterate(&counter);
    }
    if (write_batch == tmp_batch_) tmp_batch_->Clear();

    versions_->SetLastSequence(last_sequence);
  }

  while (true) {
    Writer* ready = writers_.front();
    writers_.pop_front();
    if (ready != &w) {
      ready->status = status;
      ready->done = true;
      ready->cv.Signal();
    }
    if (ready == last_writer) break;
  }

  // Notify new head of write queue
  if (!writers_.empty()) {
    writers_.front()->cv.Signal();
  }

  return status;
}

// REQUIRES: Writer list must be non-empty
// REQUIRES: First writer must have a non-null batch
WriteBatch* DBImpl::BuildBatchGroup(Writer** last_writer) {
  mutex_.AssertHeld();
  assert(!writers_.empty());
  Writer* first = writers_.front();
  WriteBatch* result = first->batch;
  assert(result != nullptr);

  size_t size = WriteBatchInternal::ByteSize(first->batch);

  // Allow the group to grow up to a maximum size, but if the
  // original write is small, limit the growth so we do not slow
  // down the small write too much.
  size_t max_size = 1 << 20;
  if (size <= (128 << 10)) {
    max_size = size + (128 << 10);
  }

  *last_writer = first;
  std::deque<Writer*>::iterator iter = writers_.begin();
  ++iter;  // Advance past "first"
  for (; iter != writers_.end(); ++iter) {
    Writer* w = *iter;
    if (w->sync && !first->sync) {
      // Do not include a sync write into a batch handled by a non-sync write.
      break;
    }

    if (w->batch != nullptr) {
      size += WriteBatchInternal::ByteSize(w->batch);
      if (size > max_size) {
        // Do not make batch too big
        break;
      }

      // Append to *result
      if (result == first->batch) {
        // Switch to temporary batch instead of disturbing caller's batch
        result = tmp_batch_;
        assert(WriteBatchInternal::Count(result) == 0);
        WriteBatchInternal::Append(result, first->batch);
      }
      WriteBatchInternal::Append(result, w->batch);
    }
    *last_writer = w;
  }
  return result;
}

// REQUIRES: mutex_ is held
// REQUIRES: this thread is currently at the front of the writer queue
Status DBImpl::SwitchToNewWAL() {
  mutex_.AssertHeld();
  assert(versions_->PrevLogNumber() == 0);
  assert(imm_ == nullptr);
  uint64_t new_log_number = versions_->NewFileNumber();
  WritableFile* lfile = nullptr;
  Status s = env_->NewWritableFile(LogFileName(dbname_, new_log_number), &lfile);
  if (!s.ok()) {
    // Avoid chewing through file number space in a tight loop.
    versions_->ReuseFileNumber(new_log_number);
    return s;
  }

  delete log_;

  Status close_status = logfile_->Close();
  if (!close_status.ok()) {
    // We may have lost some data written to the previous log file. Switch to
    // the new log file anyway, but record as a background error so we do not
    // attempt any more writes.
    //
    // We could perhaps attempt to save the memtable corresponding to log file
    // and suppress the error if that works, but that would add more complexity
    // in a critical code path.
    RecordBackgroundError(close_status);
  }
  delete logfile_;

  logfile_ = lfile;
  if (options_.enable_pre_l0) {
    imm_log_number_ = logfile_number_;
  }
  logfile_number_ = new_log_number;
  wal_create_times_[new_log_number] = env_->NowMicros();
  wal_bytes_[new_log_number] = 0;
  log_ = new log::Writer(lfile);
  imm_ = mem_;
  has_imm_.store(true, std::memory_order_release);
  mem_ = new MemTable(internal_comparator_);
  mem_->Ref();
  MaybeScheduleCompaction();
  return s;
}

Status DBImpl::MakeRoomForWrite(bool force) {
  mutex_.AssertHeld();
  assert(!writers_.empty());
  bool allow_delay = !force;
  Status s;
  while (true) {
    if (!bg_error_.ok()) {
      // Yield previous error
      s = bg_error_;
      break;
    } else if (allow_delay && versions_->NumLevelFiles(0) >=
                                  config::kL0_SlowdownWritesTrigger) {
      // We are getting close to hitting a hard limit on the number of
      // L0 files.  Rather than delaying a single write by several
      // seconds when we hit the hard limit, start delaying each
      // individual write by 1ms to reduce latency variance.  Also,
      // this delay hands over some CPU to the compaction thread in
      // case it is sharing the same core as the writer.
      mutex_.Unlock();
      env_->SleepForMicroseconds(1000);
      allow_delay = false;  // Do not delay a single write more than once
      mutex_.Lock();
    } else if (!force &&
               (mem_->ApproximateMemoryUsage() <= options_.write_buffer_size)) {
      // There is room in current memtable
      break;
    } else if (imm_ != nullptr) {
      // We have filled up the current memtable, but the previous
      // one is still being compacted, so we wait.
      Log(options_.info_log, "Current memtable full; waiting...\n");
      background_work_finished_signal_.Wait();
    } else if (versions_->NumLevelFiles(0) >= config::kL0_StopWritesTrigger) {
      // There are too many level-0 files.
      Log(options_.info_log, "Too many L0 files; waiting...\n");
      background_work_finished_signal_.Wait();
    } else {
      // Attempt to switch to a new memtable and trigger compaction of old
      s = SwitchToNewWAL();
      if (!s.ok()) {
        break;
      }
      force = false;  // Do not force another compaction if have room

      if (options_.enable_pre_l0 &&
          wal_snapshot_trigger_.ShouldTrigger(live_wal_bytes_)) {
        wal_snapshot_requested_ = true;
        MaybeScheduleCompaction();
      }
    }
  }
  return s;
}

bool DBImpl::GetProperty(const Slice& property, std::string* value) {
  value->clear();

  MutexLock l(&mutex_);
  Slice in = property;
  Slice prefix("leveldb.");
  if (!in.starts_with(prefix)) return false;
  in.remove_prefix(prefix.size());

  if (in.starts_with("num-files-at-level")) {
    in.remove_prefix(strlen("num-files-at-level"));
    uint64_t level;
    bool ok = ConsumeDecimalNumber(&in, &level) && in.empty();
    if (!ok || level >= config::kNumLevels) {
      return false;
    } else {
      char buf[100];
      std::snprintf(buf, sizeof(buf), "%d",
                    versions_->NumLevelFiles(static_cast<int>(level)));
      *value = buf;
      return true;
    }
  } else if (in == "stats") {
    char buf[200];
    std::snprintf(buf, sizeof(buf),
                  "                               Compactions\n"
                  "Level  Files Size(MB) Time(sec) Read(MB) Write(MB)\n"
                  "--------------------------------------------------\n");
    value->append(buf);
    for (int level = 0; level < config::kNumLevels; level++) {
      int files = versions_->NumLevelFiles(level);
      if (stats_[level].micros > 0 || files > 0) {
        std::snprintf(buf, sizeof(buf), "%3d %8d %8.0f %9.0f %8.0f %9.0f\n",
                      level, files, versions_->NumLevelBytes(level) / 1048576.0,
                      stats_[level].micros / 1e6,
                      stats_[level].bytes_read / 1048576.0,
                      stats_[level].bytes_written / 1048576.0);
        value->append(buf);
      }
    }
    return true;
  } else if (in == "sstables") {
    *value = versions_->current()->DebugString();
    return true;
  } else if (in == "approximate-memory-usage") {
    size_t total_usage = options_.block_cache->TotalCharge();
    if (mem_) {
      total_usage += mem_->ApproximateMemoryUsage();
    }
    if (imm_) {
      total_usage += imm_->ApproximateMemoryUsage();
    }
    char buf[50];
    std::snprintf(buf, sizeof(buf), "%llu",
                  static_cast<unsigned long long>(total_usage));
    value->append(buf);
    return true;
  }

  return false;
}

void DBImpl::GetApproximateSizes(const Range* range, int n, uint64_t* sizes) {
  // TODO(opt): better implementation
  MutexLock l(&mutex_);
  Version* v = versions_->current();
  v->Ref();

  for (int i = 0; i < n; i++) {
    // Convert user_key into a corresponding internal key.
    InternalKey k1(range[i].start, kMaxSequenceNumber, kValueTypeForSeek);
    InternalKey k2(range[i].limit, kMaxSequenceNumber, kValueTypeForSeek);
    uint64_t start = versions_->ApproximateOffsetOf(v, k1);
    uint64_t limit = versions_->ApproximateOffsetOf(v, k2);
    sizes[i] = (limit >= start ? limit - start : 0);
  }

  v->Unref();
}

// Default implementations of convenience methods that subclasses of DB
// can call if they wish
Status DB::Put(const WriteOptions& opt, const Slice& key, const Slice& value) {
  WriteBatch batch;
  batch.Put(key, value);
  return Write(opt, &batch);
}

Status DB::Delete(const WriteOptions& opt, const Slice& key) {
  WriteBatch batch;
  batch.Delete(key);
  return Write(opt, &batch);
}

DB::~DB() = default;

Status DB::Open(const Options& options, const std::string& dbname, DB** dbptr) {
  *dbptr = nullptr;

  Options open_options = options;
  LatencyInjector* inner_injector = nullptr;
  LatencyInjector* host_read_stream = nullptr;
  LatencyInjector* host_read_random = nullptr;
  LatencyInjector* host_write = nullptr;
  TwinEnv* twin_env = nullptr;
  if (options.enable_pre_l0 || options.enable_cxl_compaction ||
      options.enable_host_model) {
    inner_injector = new LatencyInjector();
    inner_injector->Init(options.pre_l0_inner_bps, options.pre_l0_inner_fixed_ns);

    host_read_stream = new LatencyInjector();
    host_read_stream->Init(options.pre_l0_host_read_bps, 0.0);
    host_read_random = new LatencyInjector();
    host_read_random->Init(options.pre_l0_host_read_bps, options.pre_l0_host_fixed_ns);
    host_write = new LatencyInjector();
    host_write->Init(options.pre_l0_host_write_bps, 0.0);

    twin_env = new TwinEnv(options.env, inner_injector,
                           host_read_stream, host_read_random, host_write,
                           options.enable_cxl_compaction);
    open_options.env = twin_env;
  }

  DBImpl* impl = new DBImpl(open_options, dbname);
  impl->twin_env_ = twin_env;
  impl->inner_injector_ = inner_injector;
  impl->host_read_stream_injector_ = host_read_stream;
  impl->host_read_random_injector_ = host_read_random;
  impl->host_write_injector_ = host_write;
  impl->mutex_.Lock();
  VersionEdit edit;
  // Recover handles create_if_missing, error_if_exists
  bool save_manifest = false;
  Status s = impl->Recover(&edit, &save_manifest);
  if (s.ok() && impl->mem_ == nullptr) {
    // Create new log and a corresponding memtable.
    uint64_t new_log_number = impl->versions_->NewFileNumber();
    WritableFile* lfile;
    s = options.env->NewWritableFile(LogFileName(dbname, new_log_number),
                                     &lfile);
    if (s.ok()) {
      edit.SetLogNumber(new_log_number);
      impl->logfile_ = lfile;
      impl->logfile_number_ = new_log_number;
      impl->wal_create_times_[new_log_number] = impl->env_->NowMicros();
      impl->wal_bytes_[new_log_number] = 0;
      impl->log_ = new log::Writer(lfile);
      impl->mem_ = new MemTable(impl->internal_comparator_);
      impl->mem_->Ref();
    }
  }
  if (s.ok() && save_manifest) {
    edit.SetPrevLogNumber(0);  // No older logs needed after recovery.
    edit.SetLogNumber(impl->logfile_number_);
    s = impl->versions_->LogAndApply(&edit, &impl->mutex_);
  }
  if (s.ok() && options.enable_pre_l0) {
    impl->pre_l0_ = new PreL0Manager(open_options, dbname, impl->env_,
                                     impl->table_cache_, impl->versions_,
                                     &impl->mutex_);
    s = impl->pre_l0_->Recover();
  }
  if (s.ok()) {
    impl->RemoveObsoleteFiles();
    impl->MaybeScheduleCompaction();
  }
  impl->mutex_.Unlock();
  if (s.ok()) {
    assert(impl->mem_ != nullptr);
    *dbptr = impl;
  } else {
    delete impl;
  }
  return s;
}

Snapshot::~Snapshot() = default;

Status DestroyDB(const std::string& dbname, const Options& options) {
  Env* env = options.env;
  std::vector<std::string> filenames;
  Status result = env->GetChildren(dbname, &filenames);
  if (!result.ok()) {
    // Ignore error in case directory does not exist
    return Status::OK();
  }

  FileLock* lock;
  const std::string lockname = LockFileName(dbname);
  result = env->LockFile(lockname, &lock);
  if (result.ok()) {
    uint64_t number;
    FileType type;
    for (size_t i = 0; i < filenames.size(); i++) {
      if (ParseFileName(filenames[i], &number, &type) &&
          type != kDBLockFile) {  // Lock file will be deleted at end
        Status del = env->RemoveFile(dbname + "/" + filenames[i]);
        if (result.ok() && !del.ok()) {
          result = del;
        }
      }
    }
    env->UnlockFile(lock);  // Ignore error since state is already gone
    env->RemoveFile(lockname);
    env->RemoveDir(dbname);  // Ignore error in case dir contains other files
  }
  return result;
}

}  // namespace leveldb
