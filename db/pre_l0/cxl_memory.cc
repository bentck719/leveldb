#include "db/pre_l0/cxl_memory.h"

#include <sys/mman.h>
#include <unistd.h>

#include <cassert>
#include <cerrno>
#include <cstring>

namespace leveldb {

CxlMemory::~CxlMemory() {
  if (base_ != nullptr && base_ != MAP_FAILED) {
    munmap(base_, size_);
  }
}

Status CxlMemory::Open(size_t size, double link_bps, double link_fixed_ns,
                       CxlMemory** out) {
  *out = nullptr;
  if (size == 0) return Status::InvalidArgument("size must be > 0");

  void* base = mmap(nullptr, size, PROT_READ | PROT_WRITE,
                    MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
  if (base == MAP_FAILED) {
    return Status::IOError("mmap failed", strerror(errno));
  }
  madvise(base, size, MADV_HUGEPAGE);
  if (mlock(base, size) != 0) {
  }
  memset(base, 0, size);

  auto* m = new CxlMemory();
  m->base_ = base;
  m->size_ = size;
  m->link_injector_.Init(link_bps, link_fixed_ns);

  m->node_bump_ = 0;
  m->kv_bump_ = static_cast<uint32_t>(size);
  m->node_free_list_.clear();
  m->kv_free_lists_.resize(kMaxFreeListSize + 1);

  *out = m;
  return Status::OK();
}

uint32_t CxlMemory::AllocateNode() {
  MutexLock l(&mutex_);
  if (!node_free_list_.empty()) {
    uint32_t off = node_free_list_.back();
    node_free_list_.pop_back();
    return off;
  }
  uint32_t off = node_bump_;
  if (off + kNodeSize > kv_bump_) return kInvalidOffset;
  node_bump_ += static_cast<uint32_t>(kNodeSize);
  assert(off % kNodeAlign == 0);
  return off;
}

void CxlMemory::FreeNode(uint32_t offset) {
  MutexLock l(&mutex_);
  node_free_list_.push_back(offset);
}

uint32_t CxlMemory::AllocateKV(size_t size) {
  if (size == 0) size = 1;
  MutexLock l(&mutex_);
  if (size <= kMaxFreeListSize && !kv_free_lists_[size].empty()) {
    uint32_t off = kv_free_lists_[size].back();
    kv_free_lists_[size].pop_back();
    return off;
  }
  if (static_cast<size_t>(kv_bump_ - node_bump_) < size) return kInvalidOffset;
  kv_bump_ -= static_cast<uint32_t>(size);
  return kv_bump_;
}

void CxlMemory::FreeKV(uint32_t offset, size_t size) {
  if (offset == kInvalidOffset) return;
  if (size == 0) size = 1;
  MutexLock l(&mutex_);
  if (size <= kMaxFreeListSize) {
    kv_free_lists_[size].push_back(offset);
  }
}

size_t CxlMemory::NodesInUse() const {
  MutexLock l(&mutex_);
  return node_bump_ / kNodeSize - node_free_list_.size();
}

size_t CxlMemory::KvBytesInUse() const {
  MutexLock l(&mutex_);
  size_t bumped = size_ - kv_bump_;
  size_t reclaimed = 0;
  for (size_t s = 0; s < kv_free_lists_.size(); ++s) {
    reclaimed += s * kv_free_lists_[s].size();
  }
  return bumped - reclaimed;
}

}  // namespace leveldb
