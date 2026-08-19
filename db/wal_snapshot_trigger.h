// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_WAL_SNAPSHOT_TRIGGER_H_
#define STORAGE_LEVELDB_DB_WAL_SNAPSHOT_TRIGGER_H_

#include <cstdint>

namespace leveldb {

class WalSnapshotTrigger {
 public:
  WalSnapshotTrigger(uint64_t threshold_bytes, double release_ratio)
      : threshold_(threshold_bytes),
        release_(static_cast<uint64_t>(threshold_bytes * release_ratio)),
        armed_(true) {}

  bool ShouldTrigger(uint64_t live_wal_bytes) {
    if (threshold_ == 0) return false;
    if (armed_ && live_wal_bytes >= threshold_) {
      armed_ = false;
      return true;
    }
    if (!armed_ && live_wal_bytes < release_) {
      armed_ = true;
    }
    return false;
  }

 private:
  const uint64_t threshold_;
  const uint64_t release_;
  bool armed_;
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_WAL_SNAPSHOT_TRIGGER_H_
