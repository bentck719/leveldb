#include <algorithm>
#include <atomic>
#include <cstdio>
#include <cstring>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include "gtest/gtest.h"

#include "db/db_impl.h"
#include "db/dbformat.h"
#include "db/wal_snapshot_trigger.h"
#include "db/io_stats.h"
#include "db/memtable.h"
#include "db/pre_l0/bplus_tree.h"
#include "db/pre_l0/cxl_memory.h"
#include "db/pre_l0/io_context.h"
#include "db/pre_l0/latency_injector.h"
#include "db/pre_l0/pre_l0_manager.h"
#include "db/pre_l0/twin_env.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "leveldb/options.h"
#include "util/testutil.h"

namespace leveldb {

static std::string TmpFile(const std::string& name) {
  return testing::TempDir() + name;
}

static CxlMemory* OpenMem(size_t size = 4 * 1024 * 1024) {
  CxlMemory* m = nullptr;
  Status s = CxlMemory::Open(size, LatencyInjector::kLinkBytesPerNs,
                             LatencyInjector::kLinkFixedNs, &m);
  EXPECT_TRUE(s.ok()) << s.ToString();
  return m;
}

static std::vector<uint32_t> DrainNodesTo(CxlMemory* mem, size_t keep_free) {
  std::vector<uint32_t> drained;
  uint32_t o;
  while ((o = mem->AllocateNode()) != CxlMemory::kInvalidOffset) {
    drained.push_back(o);
  }
  for (size_t i = 0; i < keep_free && !drained.empty(); i++) {
    mem->FreeNode(drained.back());
    drained.pop_back();
  }
  return drained;
}

static int CountKey(BPlusTree* tree, const std::string& key) {
  int n = 0;
  BPlusTree::Iterator* it = tree->NewIterator();
  for (it->SeekToFirst(); it->Valid(); it->Next()) {
    if (it->key().ToString() == key) n++;
  }
  delete it;
  return n;
}

class CxlMemoryTest : public ::testing::Test {
 protected:
  void SetUp() override { mem_ = OpenMem(); }
  void TearDown() override { delete mem_; }
  CxlMemory* mem_ = nullptr;
};

TEST_F(CxlMemoryTest, AllocFreeNode) {
  uint32_t off = mem_->AllocateNode();
  ASSERT_NE(off, CxlMemory::kInvalidOffset);
  char* p = static_cast<char*>(mem_->ptr(off));
  memset(p, 0xAB, CxlMemory::kNodeSize);
  EXPECT_EQ(static_cast<unsigned char>(p[0]), 0xAB);
  mem_->FreeNode(off);
  uint32_t off2 = mem_->AllocateNode();
  ASSERT_NE(off2, CxlMemory::kInvalidOffset);
  mem_->FreeNode(off2);
}

TEST_F(CxlMemoryTest, NodeAlignment) {
  for (int i = 0; i < 10; i++) {
    uint32_t off = mem_->AllocateNode();
    ASSERT_NE(off, CxlMemory::kInvalidOffset);
    EXPECT_EQ(off % CxlMemory::kNodeAlign, 0u) << "offset " << off << " not 64-byte aligned";
    mem_->FreeNode(off);
  }
}

TEST_F(CxlMemoryTest, AllocFreeKV) {
  uint32_t off = mem_->AllocateKV(100);
  ASSERT_NE(off, CxlMemory::kInvalidOffset);
  char* p = static_cast<char*>(mem_->ptr(off));
  memcpy(p, "hello", 5);
  EXPECT_EQ(memcmp(p, "hello", 5), 0);
  mem_->FreeKV(off, 100);
  uint32_t off2 = mem_->AllocateKV(100);
  ASSERT_NE(off2, CxlMemory::kInvalidOffset);
  mem_->FreeKV(off2, 100);
}

TEST_F(CxlMemoryTest, InjectLinkNocrash) {
  mem_->InjectLink(0);
  mem_->InjectLink(256);
}

TEST_F(CxlMemoryTest, ExhaustNodes) {
  std::vector<uint32_t> offsets;
  while (true) {
    uint32_t off = mem_->AllocateNode();
    if (off == CxlMemory::kInvalidOffset) break;
    offsets.push_back(off);
  }
  EXPECT_GT(offsets.size(), 0u);
  for (uint32_t o : offsets) mem_->FreeNode(o);
}

class BPlusTreeTest : public ::testing::Test {
 protected:
  void SetUp() override {
    mem_ = OpenMem();
    tree_ = new BPlusTree(mem_);
  }
  void TearDown() override {
    delete tree_;
    delete mem_;
  }
  CxlMemory* mem_ = nullptr;
  BPlusTree* tree_ = nullptr;
};

TEST_F(BPlusTreeTest, InsertAndGet) {
  tree_->Insert(Slice("apple"), Slice("red"), 1, kTypeValue);
  BPlusTreeValue bv;
  ASSERT_TRUE(tree_->Get(Slice("apple"), &bv));
  EXPECT_EQ(bv.seq, 1u);
  EXPECT_EQ(bv.type, kTypeValue);
  std::string val(static_cast<char*>(mem_->ptr(bv.value_offset)), bv.value_len);
  EXPECT_EQ(val, "red");
}

TEST_F(BPlusTreeTest, GetMiss) {
  BPlusTreeValue bv;
  EXPECT_FALSE(tree_->Get(Slice("nokey"), &bv));
}

TEST_F(BPlusTreeTest, Update) {
  tree_->Insert(Slice("k"), Slice("v1"), 1, kTypeValue);
  ASSERT_TRUE(tree_->Update(Slice("k"), Slice("v2"), 2, kTypeValue));
  BPlusTreeValue bv;
  ASSERT_TRUE(tree_->Get(Slice("k"), &bv));
  EXPECT_EQ(bv.seq, 2u);
  std::string val(static_cast<char*>(mem_->ptr(bv.value_offset)), bv.value_len);
  EXPECT_EQ(val, "v2");
}

TEST_F(BPlusTreeTest, UpdateMiss) {
  EXPECT_FALSE(tree_->Update(Slice("nokey"), Slice("v"), 1, kTypeValue));
}

TEST_F(BPlusTreeTest, Delete) {
  tree_->Insert(Slice("del"), Slice("val"), 1, kTypeValue);
  ASSERT_TRUE(tree_->Delete(Slice("del")));
  BPlusTreeValue bv;
  EXPECT_FALSE(tree_->Get(Slice("del"), &bv));
  EXPECT_EQ(tree_->Count(), 0u);
}

TEST_F(BPlusTreeTest, DeleteMiss) {
  EXPECT_FALSE(tree_->Delete(Slice("nokey")));
}

TEST_F(BPlusTreeTest, Count) {
  EXPECT_EQ(tree_->Count(), 0u);
  tree_->Insert(Slice("a"), Slice("1"), 1, kTypeValue);
  tree_->Insert(Slice("b"), Slice("2"), 2, kTypeValue);
  EXPECT_EQ(tree_->Count(), 2u);
  tree_->Delete(Slice("a"));
  EXPECT_EQ(tree_->Count(), 1u);
}

TEST_F(BPlusTreeTest, SortedIterator) {
  std::vector<std::string> keys = {"mango", "apple", "zebra", "banana", "kiwi"};
  for (size_t i = 0; i < keys.size(); i++) {
    tree_->Insert(Slice(keys[i]), Slice("v"), static_cast<SequenceNumber>(i + 1),
                  kTypeValue);
  }
  std::vector<std::string> sorted_keys = keys;
  std::sort(sorted_keys.begin(), sorted_keys.end());

  BPlusTree::Iterator* it = tree_->NewIterator();
  it->SeekToFirst();
  for (const auto& k : sorted_keys) {
    ASSERT_TRUE(it->Valid());
    EXPECT_EQ(it->key().ToString(), k);
    it->Next();
  }
  EXPECT_FALSE(it->Valid());
  delete it;
}

TEST_F(BPlusTreeTest, ManyKeysForcesSplit) {
  const int N = 50;
  for (int i = 0; i < N; i++) {
    std::string k = "key" + std::to_string(i);
    tree_->Insert(Slice(k), Slice("val"), static_cast<SequenceNumber>(i), kTypeValue);
  }
  EXPECT_EQ(tree_->Count(), static_cast<size_t>(N));

  for (int i = 0; i < N; i++) {
    std::string k = "key" + std::to_string(i);
    BPlusTreeValue bv;
    EXPECT_TRUE(tree_->Get(Slice(k), &bv)) << "missing key: " << k;
  }

  std::vector<std::string> expected;
  for (int i = 0; i < N; i++) expected.push_back("key" + std::to_string(i));
  std::sort(expected.begin(), expected.end());

  BPlusTree::Iterator* it = tree_->NewIterator();
  it->SeekToFirst();
  for (const auto& k : expected) {
    ASSERT_TRUE(it->Valid()) << "iterator ended early at key " << k;
    EXPECT_EQ(it->key().ToString(), k);
    it->Next();
  }
  EXPECT_FALSE(it->Valid());
  delete it;
}

TEST_F(BPlusTreeTest, StressManyKeysWithDeleteReinsert) {
  delete tree_;
  delete mem_;
  mem_ = OpenMem(256 * 1024 * 1024);
  tree_ = new BPlusTree(mem_);

  const int kRounds = 120;
  const int kBatch = 300;
  const int kKeySpace = 1000000;
  std::srand(777);
  std::vector<char> present(kKeySpace, 0);
  char buf[32];
  for (int r = 0; r < kRounds; r++) {
    std::vector<int> batch;
    for (int i = 0; i < kBatch; i++) batch.push_back(std::rand() % kKeySpace);
    std::sort(batch.begin(), batch.end());
    batch.erase(std::unique(batch.begin(), batch.end()), batch.end());
    for (int k : batch) {
      std::snprintf(buf, sizeof(buf), "%016d", k);
      SequenceNumber seq = static_cast<SequenceNumber>(r * kBatch + k);
      int vlen = 1 + (std::rand() % 64);
      std::string val(vlen, 'x');
      if (present[k]) {
        if (std::rand() % 5 == 0) {
          ASSERT_TRUE(tree_->Delete(Slice(buf)))
              << "delete failed at k=" << k << " round=" << r;
          ASSERT_TRUE(tree_->Insert(Slice(buf), Slice(val), seq, kTypeValue))
              << "reinsert failed at k=" << k << " round=" << r;
        } else {
          ASSERT_TRUE(tree_->Update(Slice(buf), Slice(val), seq, kTypeValue))
              << "update failed at k=" << k << " round=" << r;
        }
      } else {
        ASSERT_TRUE(tree_->Insert(Slice(buf), Slice(val), seq, kTypeValue))
            << "insert failed at k=" << k << " round=" << r;
        present[k] = 1;
      }
    }
    if (r % 15 == 14 || r == kRounds - 1) {
      int missing = 0;
      for (int k = 0; k < kKeySpace; k++) {
        if (!present[k]) continue;
        std::snprintf(buf, sizeof(buf), "%016d", k);
        BPlusTreeValue bv;
        if (!tree_->Get(Slice(buf), &bv)) {
          missing++;
          if (missing <= 3) ADD_FAILURE() << "missing key: " << buf << " after round " << r;
        }
      }
      ASSERT_EQ(missing, 0) << "round " << r;
    }
  }
}

TEST_F(BPlusTreeTest, NodeAccessChargedOncePerNodeOnGet) {
  std::vector<std::string> keys = {"k0", "k1", "k2", "k3", "k4", "k5", "k6"};
  for (size_t i = 0; i < keys.size(); i++) {
    ASSERT_TRUE(tree_->Insert(Slice(keys[i]), Slice("v"),
                              static_cast<SequenceNumber>(i + 1), kTypeValue));
  }
  BPlusTreeValue bv;
  mem_->ResetNodeAccesses();
  ASSERT_TRUE(tree_->Get(Slice("k3"), &bv));
  EXPECT_EQ(mem_->NodeAccesses(), 1u)
      << "depth-1 Get must charge exactly one node fetch (no per-slot inflation)";

  for (int i = 0; i < 13; i++) {
    char k[16];
    std::snprintf(k, sizeof(k), "key%04d", i);
    ASSERT_TRUE(tree_->Insert(Slice(k), Slice("v"), static_cast<SequenceNumber>(i),
                              kTypeValue));
  }
  mem_->ResetNodeAccesses();
  ASSERT_TRUE(tree_->Get(Slice("key0008"), &bv));
  EXPECT_EQ(mem_->NodeAccesses(), 2u)
      << "depth-2 Get must charge exactly two node fetches (root + leaf)";
}

TEST_F(BPlusTreeTest, DeletionTombstone) {
  tree_->Insert(Slice("tomb"), Slice(""), 5, kTypeDeletion);
  BPlusTreeValue bv;
  ASSERT_TRUE(tree_->Get(Slice("tomb"), &bv));
  EXPECT_EQ(bv.type, kTypeDeletion);
}

TEST(CxlReclaimTest, TreeTeardownReturnsPoolsToBaseline) {
  CxlMemory* mem = OpenMem(16 * 1024 * 1024);
  ASSERT_EQ(mem->NodesInUse(), 0u);
  ASSERT_EQ(mem->KvBytesInUse(), 0u);

  {
    BPlusTree tree(mem);
    for (int i = 0; i < 300; i++) {
      char k[32];
      std::snprintf(k, sizeof(k), "key%06d", i);
      if (i % 5 == 0) {
        ASSERT_TRUE(tree.Insert(Slice(k), Slice(""), i + 1, kTypeDeletion));
      } else {
        ASSERT_TRUE(tree.Insert(Slice(k), std::string(20, 'v'), i + 1,
                                kTypeValue));
      }
    }
    for (int i = 1; i < 300; i += 3) {
      char k[32];
      std::snprintf(k, sizeof(k), "key%06d", i);
      tree.Update(Slice(k), std::string(40, 'u'), 10000 + i, kTypeValue);
    }
    ASSERT_GT(mem->NodesInUse(), 1u);
    ASSERT_GT(mem->KvBytesInUse(), 0u);
  }

  EXPECT_EQ(mem->NodesInUse(), 0u);
  EXPECT_EQ(mem->KvBytesInUse(), 0u);
  delete mem;
}

TEST(CxlReclaimTest, NodePoolSurvivesManyTreeCycles) {
  CxlMemory* mem = OpenMem(512 * 1024);

  const int kCycles = 100;
  for (int c = 0; c < kCycles; c++) {
    {
      BPlusTree tree(mem);
      for (int i = 0; i < 100; i++) {
        char k[32];
        std::snprintf(k, sizeof(k), "k%05d", i);
        ASSERT_TRUE(tree.Insert(Slice(k), std::string(8, 'x'), i + 1,
                                kTypeValue))
            << "insert failed at cycle " << c << " i " << i
            << " — node pool exhausted (leak?)";
      }
    }
    ASSERT_EQ(mem->NodesInUse(), 0u) << "nodes leaked after cycle " << c;
  }
  delete mem;
}

TEST(BPlusTreeOomTest, LeafSplitOomLeavesTreeUnchanged) {
  CxlMemory* mem = OpenMem(256 * 1024);
  {
    BPlusTree tree(mem);
    for (int i = 0; i < 7; i++) {
      char k[8];
      std::snprintf(k, sizeof(k), "k%d", i);
      ASSERT_TRUE(tree.Insert(Slice(k), Slice("v"), i + 1, kTypeValue));
    }
    std::vector<uint32_t> drained = DrainNodesTo(mem, /*keep_free=*/1);

    size_t nodes_before = mem->NodesInUse();
    size_t kv_before = mem->KvBytesInUse();
    size_t count_before = tree.Count();

    EXPECT_FALSE(tree.Insert(Slice("k7"), Slice("v"), 8, kTypeValue));

    EXPECT_EQ(tree.Count(), count_before);
    EXPECT_EQ(mem->NodesInUse(), nodes_before);
    EXPECT_EQ(mem->KvBytesInUse(), kv_before);
    for (int i = 0; i < 7; i++) {
      char k[8];
      std::snprintf(k, sizeof(k), "k%d", i);
      BPlusTreeValue bv;
      EXPECT_TRUE(tree.Get(Slice(k), &bv)) << k;
    }
    BPlusTreeValue bv;
    EXPECT_FALSE(tree.Get(Slice("k7"), &bv));
    EXPECT_EQ(CountKey(&tree, "k7"), 0);

    for (uint32_t o : drained) mem->FreeNode(o);
  }
  delete mem;
}

TEST(BPlusTreeOomTest, CascadeSplitOomLeavesTreeUnchanged) {
  CxlMemory* mem = OpenMem(4 * 1024 * 1024);
  {
    BPlusTree tree(mem);
    int i = 0;
    while (mem->NodesInUse() < 16) {
      char k[16];
      std::snprintf(k, sizeof(k), "key%06d", i++);
      ASSERT_TRUE(tree.Insert(Slice(k), std::string(8, 'v'), i, kTypeValue));
      ASSERT_LT(i, 100000);
    }
    ASSERT_EQ(mem->NodesInUse(), 16u);

    std::vector<uint32_t> drained = DrainNodesTo(mem, /*keep_free=*/1);

    bool saw_failure = false;
    for (int j = 0; j < 5000 && !saw_failure; j++) {
      char k[16];
      std::snprintf(k, sizeof(k), "zzz%06d", j);
      size_t nodes_before = mem->NodesInUse();
      size_t kv_before = mem->KvBytesInUse();
      size_t count_before = tree.Count();
      if (!tree.Insert(Slice(k), std::string(8, 'v'), 100000 + j, kTypeValue)) {
        saw_failure = true;
        EXPECT_EQ(tree.Count(), count_before);
        EXPECT_EQ(mem->NodesInUse(), nodes_before);
        EXPECT_EQ(mem->KvBytesInUse(), kv_before);
        EXPECT_EQ(CountKey(&tree, k), 0);
      }
    }
    EXPECT_TRUE(saw_failure) << "expected a root-splitting insert to hit node OOM";
    for (uint32_t o : drained) mem->FreeNode(o);
  }
  delete mem;
}

TEST(BPlusTreeOomTest, FailedInsertThenRetryInsertsExactlyOnce) {
  CxlMemory* mem = OpenMem(256 * 1024);
  {
    BPlusTree tree(mem);
    for (int i = 0; i < 7; i++) {
      char k[8];
      std::snprintf(k, sizeof(k), "k%d", i);
      ASSERT_TRUE(tree.Insert(Slice(k), Slice("v"), i + 1, kTypeValue));
    }
    std::vector<uint32_t> drained = DrainNodesTo(mem, /*keep_free=*/1);

    EXPECT_FALSE(tree.Insert(Slice("k7"), Slice("zzz"), 8, kTypeValue));
    EXPECT_EQ(CountKey(&tree, "k7"), 0);

    for (uint32_t o : drained) mem->FreeNode(o);
    EXPECT_TRUE(tree.Insert(Slice("k7"), Slice("zzz"), 8, kTypeValue));
    EXPECT_EQ(CountKey(&tree, "k7"), 1);
    EXPECT_EQ(tree.Count(), 8u);
    BPlusTreeValue bv;
    ASSERT_TRUE(tree.Get(Slice("k7"), &bv));
    std::string val(static_cast<char*>(mem->ptr(bv.value_offset)), bv.value_len);
    EXPECT_EQ(val, "zzz");
  }
  delete mem;
}

TEST(BPlusTreeOomTest, FreeSubtreeAfterFailedInsertReturnsPoolToBaseline) {
  CxlMemory* mem = OpenMem(256 * 1024);
  ASSERT_EQ(mem->NodesInUse(), 0u);
  ASSERT_EQ(mem->KvBytesInUse(), 0u);
  {
    BPlusTree tree(mem);
    for (int i = 0; i < 7; i++) {
      char k[8];
      std::snprintf(k, sizeof(k), "k%d", i);
      ASSERT_TRUE(tree.Insert(Slice(k), std::string(16, 'v'), i + 1, kTypeValue));
    }
    std::vector<uint32_t> drained = DrainNodesTo(mem, /*keep_free=*/1);
    EXPECT_FALSE(tree.Insert(Slice("k7"), Slice("v"), 8, kTypeValue));
    for (uint32_t o : drained) mem->FreeNode(o);
  }

  EXPECT_EQ(mem->NodesInUse(), 0u);
  EXPECT_EQ(mem->KvBytesInUse(), 0u);
  delete mem;
}

class PreL0ManagerTest : public ::testing::Test {
 protected:
  void SetUp() override {
    wal_dir_ = testing::TempDir() + "manager_testdb/";
    Env::Default()->CreateDir(wal_dir_);

    options_.enable_pre_l0   = true;
    options_.pre_l0_cxl_size = 8 * 1024 * 1024;
    options_.comparator = BytewiseComparator();

    mgr_ = new PreL0Manager(options_, wal_dir_,
                            Env::Default(), /*table_cache=*/nullptr,
                            /*versions=*/nullptr, /*db_mutex=*/nullptr);
    ASSERT_TRUE(mgr_->Recover().ok());
  }

  void TearDown() override {
    delete mgr_;
    std::remove((wal_dir_ + "pre_l0_wal").c_str());
    Env::Default()->DeleteDir(wal_dir_);
  }

  MemTable* MakeMemTable(
      const std::vector<std::tuple<std::string, std::string, int>>& entries) {
    MemTable* mem = new MemTable(InternalKeyComparator(BytewiseComparator()));
    mem->Ref();
    SequenceNumber seq = 1;
    for (const auto& [key, value, count] : entries) {
      for (int i = 0; i < count; i++) {
        mem->Add(seq++, kTypeValue, Slice(key), Slice(value));
      }
    }
    return mem;
  }

  std::string wal_dir_;
  Options options_;
  PreL0Manager* mgr_ = nullptr;
};

TEST_F(PreL0ManagerTest, HotColdClassification) {
  MemTable* mem = MakeMemTable({{"hot_key", "hv", 2}, {"cold_key", "cv", 1}});
  VersionEdit edit;
  ASSERT_TRUE(mgr_->FlushMemTable(mem, /*mem_log_number=*/1, &edit).ok());

  std::string val;
  Status s;
  LookupKey lk_hot("hot_key", kMaxSequenceNumber);
  EXPECT_TRUE(mgr_->Get(lk_hot, &val, &s));
  EXPECT_EQ(val, "hv");

  LookupKey lk_cold("cold_key", kMaxSequenceNumber);
  EXPECT_TRUE(mgr_->Get(lk_cold, &val, &s));
  EXPECT_EQ(val, "cv");

  mem->Unref();
}

TEST_F(PreL0ManagerTest, GetKeyWithEmbeddedNul) {
  const std::string key("ab\0cd", 5);
  ASSERT_EQ(key.size(), 5u);
  MemTable* mem = MakeMemTable({{key, "nulval", 1}});
  VersionEdit edit;
  ASSERT_TRUE(mgr_->FlushMemTable(mem, /*mem_log_number=*/1, &edit).ok());
  mem->Unref();

  std::string val;
  Status s;
  LookupKey lk(Slice(key), kMaxSequenceNumber);
  EXPECT_TRUE(mgr_->Get(lk, &val, &s));
  EXPECT_EQ(val, "nulval");

  LookupKey lk_prefix(Slice("ab"), kMaxSequenceNumber);
  std::string val2;
  EXPECT_FALSE(mgr_->Get(lk_prefix, &val2, &s));
}

TEST_F(PreL0ManagerTest, NoDuplicates_InplaceUpdate) {
  MemTable* mem1 = MakeMemTable({{"key", "v1", 1}});
  VersionEdit edit1;
  ASSERT_TRUE(mgr_->FlushMemTable(mem1, 1, &edit1).ok());
  mem1->Unref();

  MemTable* mem2 = MakeMemTable({{"key", "v2", 1}});
  VersionEdit edit2;
  ASSERT_TRUE(mgr_->FlushMemTable(mem2, 2, &edit2).ok());
  mem2->Unref();

  std::string val;
  Status s;
  LookupKey lk("key", kMaxSequenceNumber);
  ASSERT_TRUE(mgr_->Get(lk, &val, &s));
  EXPECT_EQ(val, "v2");
}

TEST_F(PreL0ManagerTest, HotColdReclassification) {
  MemTable* mem1 = MakeMemTable({{"key", "v1", 1}});
  VersionEdit edit1;
  ASSERT_TRUE(mgr_->FlushMemTable(mem1, 1, &edit1).ok());
  mem1->Unref();

  MemTable* mem2 = MakeMemTable({{"key", "v2", 2}});
  VersionEdit edit2;
  ASSERT_TRUE(mgr_->FlushMemTable(mem2, 2, &edit2).ok());
  mem2->Unref();

  std::string val;
  Status s;
  LookupKey lk("key", kMaxSequenceNumber);
  ASSERT_TRUE(mgr_->Get(lk, &val, &s));
  EXPECT_EQ(val, "v2");
}

TEST_F(PreL0ManagerTest, PreL0WriteBytesValueOnlyOnUpdate) {
  g_io_stats.Reset();
  MemTable* m1 = MakeMemTable({{"akey", "value1", 1}});
  VersionEdit e1;
  ASSERT_TRUE(mgr_->FlushMemTable(m1, 1, &e1).ok());
  m1->Unref();
  EXPECT_EQ(g_io_stats.prel0_write_bytes.load(), 10u);

  g_io_stats.Reset();
  MemTable* m2 = MakeMemTable({{"akey", "vv", 1}});
  VersionEdit e2;
  ASSERT_TRUE(mgr_->FlushMemTable(m2, 2, &e2).ok());
  m2->Unref();
  EXPECT_EQ(g_io_stats.prel0_write_bytes.load(), 2u);
}

TEST_F(PreL0ManagerTest, GetMiss) {
  std::string val;
  Status s;
  LookupKey lk("nonexistent", kMaxSequenceNumber);
  EXPECT_FALSE(mgr_->Get(lk, &val, &s));
}

TEST_F(PreL0ManagerTest, MinLogNumber_EmptyIsZero) {
  EXPECT_EQ(mgr_->MinLogNumber(), 0u);
}

TEST_F(PreL0ManagerTest, MinLogNumber_ReflectsOldestPST) {
  MemTable* mem1 = MakeMemTable({{"k1", "v1", 1}});
  VersionEdit edit1;
  ASSERT_TRUE(mgr_->FlushMemTable(mem1, /*mem_log_number=*/42, &edit1).ok());
  mem1->Unref();

  MemTable* mem2 = MakeMemTable({{"k2", "v2", 1}});
  VersionEdit edit2;
  ASSERT_TRUE(mgr_->FlushMemTable(mem2, /*mem_log_number=*/100, &edit2).ok());
  mem2->Unref();

  EXPECT_EQ(mgr_->MinLogNumber(), 42u);
}

TEST_F(PreL0ManagerTest, MinLogNumber_HotKeyReleasesOldWal) {
  MemTable* m1 = MakeMemTable({{"K", "v1", 1}});
  VersionEdit e1;
  ASSERT_TRUE(mgr_->FlushMemTable(m1, /*mem_log_number=*/10, &e1).ok());
  m1->Unref();
  EXPECT_EQ(mgr_->MinLogNumber(), 10u);

  MemTable* m2 = MakeMemTable({{"K", "v2", 1}});
  VersionEdit e2;
  ASSERT_TRUE(mgr_->FlushMemTable(m2, /*mem_log_number=*/20, &e2).ok());
  m2->Unref();
  EXPECT_EQ(mgr_->MinLogNumber(), 20u);
}

TEST_F(PreL0ManagerTest, MinLogNumber_ColdKeyPinsOldWal) {
  MemTable* m1 = MakeMemTable({{"K", "v1", 1}, {"cold", "c", 1}});
  VersionEdit e1;
  ASSERT_TRUE(mgr_->FlushMemTable(m1, /*mem_log_number=*/10, &e1).ok());
  m1->Unref();
  EXPECT_EQ(mgr_->MinLogNumber(), 10u);

  MemTable* m2 = MakeMemTable({{"K", "v2", 1}});
  VersionEdit e2;
  ASSERT_TRUE(mgr_->FlushMemTable(m2, /*mem_log_number=*/20, &e2).ok());
  m2->Unref();
  EXPECT_EQ(mgr_->MinLogNumber(), 10u);
}

class TwinEnvRoutingTest : public ::testing::Test {
 protected:
  void SetUp() override {
    inner_.Init(LatencyInjector::kInnerBytesPerNs,
                LatencyInjector::kInnerFixedNs);
    path_ = TmpFile("twin_routing.ldb");
    std::remove(path_.c_str());
  }
  void TearDown() override { std::remove(path_.c_str()); }

  void PrepareFile(size_t n) {
    WritableFile* f = nullptr;
    ASSERT_TRUE(Env::Default()->NewWritableFile(path_, &f).ok());
    ASSERT_TRUE(f->Append(Slice(std::string(n, 'x'))).ok());
    ASSERT_TRUE(f->Close().ok());
    delete f;
  }
  void AppendThrough(TwinEnv* env, const std::string& data) {
    WritableFile* f = nullptr;
    ASSERT_TRUE(env->NewWritableFile(path_, &f).ok());
    ASSERT_TRUE(f->Append(Slice(data)).ok());
    ASSERT_TRUE(f->Close().ok());
    delete f;
  }
  void ReadThrough(TwinEnv* env, size_t n) {
    RandomAccessFile* f = nullptr;
    ASSERT_TRUE(env->NewRandomAccessFile(path_, &f).ok());
    std::string scratch(n, '\0');
    Slice result;
    ASSERT_TRUE(f->Read(0, n, &result, &scratch[0]).ok());
    delete f;
  }

  LatencyInjector inner_;
  std::string path_;
};

TEST_F(TwinEnvRoutingTest, FlushWriteCountsCxlWriteOnce) {
  TwinEnv env(Env::Default(), &inner_,
              /*host_read_stream=*/nullptr, /*host_read_random=*/nullptr,
              /*host_write=*/nullptr, /*enable_cxl_compaction=*/false);
  g_io_stats.Reset();
  {
    IoClassScope s(IoClass::kFlush);
    AppendThrough(&env, "abcde");
  }
  EXPECT_EQ(g_io_stats.cxl_write_bytes.load(), 5u);
  EXPECT_EQ(g_io_stats.host_write_bytes.load(), 0u);
}

TEST_F(TwinEnvRoutingTest, CompactionWriteHostWhenCxlOff) {
  TwinEnv env(Env::Default(), &inner_,
              /*host_read_stream=*/nullptr, /*host_read_random=*/nullptr,
              /*host_write=*/nullptr, /*enable_cxl_compaction=*/false);
  g_io_stats.Reset();
  {
    IoClassScope s(IoClass::kCompaction);
    AppendThrough(&env, "abcde");
  }
  EXPECT_EQ(g_io_stats.host_write_bytes.load(), 5u);
  EXPECT_EQ(g_io_stats.cxl_write_bytes.load(), 0u);
}

TEST_F(TwinEnvRoutingTest, CompactionWriteCxlWhenCxlOn) {
  TwinEnv env(Env::Default(), &inner_,
              /*host_read_stream=*/nullptr, /*host_read_random=*/nullptr,
              /*host_write=*/nullptr, /*enable_cxl_compaction=*/true);
  g_io_stats.Reset();
  {
    IoClassScope s(IoClass::kCompaction);
    AppendThrough(&env, "abcde");
  }
  EXPECT_EQ(g_io_stats.cxl_write_bytes.load(), 5u);
  EXPECT_EQ(g_io_stats.host_write_bytes.load(), 0u);
}

TEST_F(TwinEnvRoutingTest, OtherReadCountsHostReadOnly) {
  PrepareFile(64);
  TwinEnv env(Env::Default(), &inner_,
              /*host_read_stream=*/nullptr, /*host_read_random=*/nullptr,
              /*host_write=*/nullptr, /*enable_cxl_compaction=*/false);
  g_io_stats.Reset();
  ReadThrough(&env, 64);
  EXPECT_EQ(g_io_stats.host_read_bytes.load(), 64u);
  EXPECT_EQ(g_io_stats.cxl_read_bytes.load(), 0u);
}

TEST_F(TwinEnvRoutingTest, CompactionReadHostWhenCxlOff) {
  PrepareFile(64);
  TwinEnv env(Env::Default(), &inner_,
              /*host_read_stream=*/nullptr, /*host_read_random=*/nullptr,
              /*host_write=*/nullptr, /*enable_cxl_compaction=*/false);
  g_io_stats.Reset();
  {
    IoClassScope s(IoClass::kCompaction);
    ReadThrough(&env, 64);
  }
  EXPECT_EQ(g_io_stats.host_read_bytes.load(), 64u);
  EXPECT_EQ(g_io_stats.cxl_read_bytes.load(), 0u);
}

TEST_F(TwinEnvRoutingTest, CompactionReadCxlWhenCxlOn) {
  PrepareFile(64);
  TwinEnv env(Env::Default(), &inner_,
              /*host_read_stream=*/nullptr, /*host_read_random=*/nullptr,
              /*host_write=*/nullptr, /*enable_cxl_compaction=*/true);
  g_io_stats.Reset();
  {
    IoClassScope s(IoClass::kCompaction);
    ReadThrough(&env, 64);
  }
  EXPECT_EQ(g_io_stats.cxl_read_bytes.load(), 64u);
  EXPECT_EQ(g_io_stats.host_read_bytes.load(), 0u);
}

TEST(TwinEnvDBTest, UserGetCountsHostReadOnly) {
  std::string dbname = testing::TempDir() + "twin_get_db";
  DestroyDB(dbname, Options());

  Options options;
  options.create_if_missing = true;
  options.enable_pre_l0 = false;
  options.enable_cxl_compaction = true;
  options.write_buffer_size = 64 * 1024;
  DB* db = nullptr;
  ASSERT_TRUE(DB::Open(options, dbname, &db).ok());

  WriteOptions wo;
  std::string value(200, 'v');
  for (int i = 0; i < 20000; i++) {
    char key[32];
    std::snprintf(key, sizeof(key), "key%08d", i);
    ASSERT_TRUE(db->Put(wo, key, value).ok());
  }
  db->CompactRange(nullptr, nullptr);
  delete db;

  ASSERT_TRUE(DB::Open(options, dbname, &db).ok());
  g_io_stats.Reset();

  ReadOptions ro;
  std::string val;
  int found = 0;
  for (int i = 0; i < 200; i++) {
    char key[32];
    std::snprintf(key, sizeof(key), "key%08d", (i * 97) % 20000);
    if (db->Get(ro, key, &val).ok()) found++;
  }
  ASSERT_GT(found, 0);

  EXPECT_GT(g_io_stats.host_read_bytes.load(), 0u);
  EXPECT_EQ(g_io_stats.cxl_read_bytes.load(), 0u);
  EXPECT_EQ(g_io_stats.cxl_write_bytes.load(), 0u);

  delete db;
  DestroyDB(dbname, Options());
}

static uint64_t SumLdbBytes(const std::string& dbname) {
  std::vector<std::string> children;
  EXPECT_TRUE(Env::Default()->GetChildren(dbname, &children).ok());
  uint64_t total = 0;
  for (const auto& child : children) {
    if (child.size() > 4 && child.substr(child.size() - 4) == ".ldb") {
      uint64_t fsize = 0;
      EXPECT_TRUE(
          Env::Default()->GetFileSize(dbname + "/" + child, &fsize).ok());
      total += fsize;
    }
  }
  return total;
}

TEST(PreL0MinLogNumberEvictionTest, ColdEvictionAdvancesFloor) {
  std::string dbname = testing::TempDir() + "prel0_minlog_evict_db";
  DestroyDB(dbname, Options());
  Options options;
  options.create_if_missing = true;
  options.enable_pre_l0 = true;
  options.pre_l0_cxl_size = 512ULL * 1024 * 1024;
  options.pre_l0_hot_threshold = 2;
  options.pre_l0_evict_high_watermark = 0.05;
  options.pre_l0_evict_low_watermark = 0.0;
  DB* db = nullptr;
  ASSERT_TRUE(DB::Open(options, dbname, &db).ok());
  DBImpl* impl = reinterpret_cast<DBImpl*>(db);

  ASSERT_TRUE(db->Put(WriteOptions(), "K", "v1").ok());
  ASSERT_TRUE(db->Put(WriteOptions(), "K", "v1b").ok());
  ASSERT_TRUE(db->Put(WriteOptions(), "cold", "c").ok());
  ASSERT_TRUE(impl->TEST_CompactMemTable().ok());
  uint64_t floor_after_insert = impl->TEST_PreL0MinLogNumber();
  ASSERT_GT(floor_after_insert, 0u);

  ASSERT_TRUE(db->Put(WriteOptions(), "K", "v2").ok());
  ASSERT_TRUE(impl->TEST_CompactMemTable().ok());
  EXPECT_EQ(impl->TEST_PreL0MinLogNumber(), floor_after_insert)
      << "cold key must keep the old WAL pinned";

  std::string filler(1000, 'f');
  const int kSpillKeys = static_cast<int>(
      PreL0Manager::kMaxTreeBytes * (PreL0Manager::kFlushThreshold + 4) /
      filler.size());
  for (int i = 0; i < kSpillKeys; i++) {
    char k[32];
    std::snprintf(k, sizeof(k), "fill%08d", i);
    ASSERT_TRUE(db->Put(WriteOptions(), k, filler).ok());
    if (i % 2000 == 1999) ASSERT_TRUE(impl->TEST_CompactMemTable().ok());
  }
  ASSERT_TRUE(impl->TEST_CompactMemTable().ok());
  ASSERT_GT(SumLdbBytes(dbname), 0u) << "expected a pre-L0 spill to L0";

  EXPECT_GT(impl->TEST_PreL0MinLogNumber(), floor_after_insert)
      << "floor must advance once the cold key is evicted to L0";

  delete db;
  DestroyDB(dbname, Options());
}

TEST(TwinEnvDBTest, BaselineCompactionCountsHostRead) {
  std::string dbname = testing::TempDir() + "twin_baseline_db";
  DestroyDB(dbname, Options());

  Options options;
  options.create_if_missing = true;
  options.enable_pre_l0 = false;
  options.enable_cxl_compaction = false;
  DB* db = nullptr;
  ASSERT_TRUE(DB::Open(options, dbname, &db).ok());
  DBImpl* impl = reinterpret_cast<DBImpl*>(db);

  WriteOptions wo;
  std::string value(200, 'v');
  for (int batch = 0; batch < 2; batch++) {
    for (int i = 0; i < 1000; i++) {
      char key[32];
      std::snprintf(key, sizeof(key), "key%08d", i);
      ASSERT_TRUE(db->Put(wo, key, value).ok());
    }
    ASSERT_TRUE(impl->TEST_CompactMemTable().ok());
  }

  g_io_stats.Reset();
  db->CompactRange(nullptr, nullptr);

  EXPECT_GT(g_io_stats.host_read_bytes.load(), 0u);
  EXPECT_EQ(g_io_stats.cxl_read_bytes.load(), 0u);

  delete db;
  DestroyDB(dbname, Options());
}

TEST(TwinEnvDBTest, FlushCountedOnceWhenTwinActive) {
  std::string dbname = testing::TempDir() + "twin_flush_once_db";
  DestroyDB(dbname, Options());

  Options options;
  options.create_if_missing = true;
  options.enable_pre_l0 = false;
  options.enable_cxl_compaction = true;
  DB* db = nullptr;
  ASSERT_TRUE(DB::Open(options, dbname, &db).ok());
  DBImpl* impl = reinterpret_cast<DBImpl*>(db);

  g_io_stats.Reset();

  WriteOptions wo;
  std::string value(200, 'v');
  for (int i = 0; i < 1000; i++) {
    char key[32];
    std::snprintf(key, sizeof(key), "key%08d", i);
    ASSERT_TRUE(db->Put(wo, key, value).ok());
  }
  ASSERT_TRUE(impl->TEST_CompactMemTable().ok());

  uint64_t ldb_bytes = SumLdbBytes(dbname);
  ASSERT_GT(ldb_bytes, 0u);
  EXPECT_EQ(g_io_stats.host_write_bytes.load(), ldb_bytes);
  EXPECT_EQ(g_io_stats.cxl_write_bytes.load(), 0u);

  delete db;
  DestroyDB(dbname, Options());
}

class PreL0ReadPathTest : public ::testing::Test {
 protected:
  void SetUp() override {
    dbname_ = testing::TempDir() + "prel0_readpath_db";
    DestroyDB(dbname_, Options());
    options_.create_if_missing = true;
    options_.enable_pre_l0 = true;
    options_.pre_l0_cxl_size = 512ULL * 1024 * 1024;
    options_.pre_l0_evict_high_watermark = 0.05;
    options_.pre_l0_evict_low_watermark = 0.0;
    ASSERT_TRUE(DB::Open(options_, dbname_, &db_).ok());
    impl_ = reinterpret_cast<DBImpl*>(db_);
  }
  void TearDown() override {
    delete db_;
    DestroyDB(dbname_, Options());
  }

  void SpillTargetToL0(const std::string& v1) {
    ASSERT_TRUE(db_->Put(WriteOptions(), "target", v1).ok());
    std::string filler(1000, 'f');
    const int kSpillKeys = static_cast<int>(
        PreL0Manager::kMaxTreeBytes * (PreL0Manager::kFlushThreshold + 4) /
        filler.size());
    for (int i = 0; i < kSpillKeys; i++) {
      char k[32];
      std::snprintf(k, sizeof(k), "fill%08d", i);
      ASSERT_TRUE(db_->Put(WriteOptions(), k, filler).ok());
      if (i % 2000 == 1999) ASSERT_TRUE(impl_->TEST_CompactMemTable().ok());
    }
    ASSERT_TRUE(impl_->TEST_CompactMemTable().ok());
    ASSERT_GT(SumLdbBytes(dbname_), 0u) << "expected a pre-L0 spill to L0";
  }

  std::string ScanTarget(const ReadOptions& ro) {
    Iterator* it = db_->NewIterator(ro);
    std::string out = "<none>";
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
      if (it->key().ToString() == "target") {
        out = it->value().ToString();
        break;
      }
    }
    EXPECT_TRUE(it->status().ok());
    delete it;
    return out;
  }

  std::string dbname_;
  Options options_;
  DB* db_ = nullptr;
  DBImpl* impl_ = nullptr;
};

TEST_F(PreL0ReadPathTest, BufferedKeyVisibleToScan) {
  ASSERT_TRUE(db_->Put(WriteOptions(), "target", "only").ok());
  ASSERT_TRUE(impl_->TEST_CompactMemTable().ok());
  EXPECT_EQ(ScanTarget(ReadOptions()), "only");
}

TEST_F(PreL0ReadPathTest, ScanReturnsPreL0NotStaleL0) {
  SpillTargetToL0("v1");
  std::string val;
  ASSERT_TRUE(db_->Get(ReadOptions(), "target", &val).ok());
  ASSERT_EQ(val, "v1");
  ASSERT_EQ(ScanTarget(ReadOptions()), "v1");

  ASSERT_TRUE(db_->Put(WriteOptions(), "target", "v2").ok());
  ASSERT_TRUE(impl_->TEST_CompactMemTable().ok());

  EXPECT_TRUE(db_->Get(ReadOptions(), "target", &val).ok());
  EXPECT_EQ(val, "v2");
  EXPECT_EQ(ScanTarget(ReadOptions()), "v2");
}

TEST_F(PreL0ReadPathTest, ScanHidesPreL0Tombstone) {
  SpillTargetToL0("v1");
  ASSERT_TRUE(db_->Delete(WriteOptions(), "target").ok());
  ASSERT_TRUE(impl_->TEST_CompactMemTable().ok());

  std::string val;
  EXPECT_TRUE(db_->Get(ReadOptions(), "target", &val).IsNotFound());
  EXPECT_EQ(ScanTarget(ReadOptions()), "<none>");
}

TEST_F(PreL0ReadPathTest, SnapshotIgnoresNewerPreL0) {
  SpillTargetToL0("v1");
  const Snapshot* snap = db_->GetSnapshot();

  ASSERT_TRUE(db_->Put(WriteOptions(), "target", "v2").ok());
  ASSERT_TRUE(impl_->TEST_CompactMemTable().ok());

  ReadOptions ro;
  ro.snapshot = snap;
  std::string val;
  EXPECT_TRUE(db_->Get(ro, "target", &val).ok());
  EXPECT_EQ(val, "v1");
  EXPECT_EQ(ScanTarget(ro), "v1");

  EXPECT_TRUE(db_->Get(ReadOptions(), "target", &val).ok());
  EXPECT_EQ(val, "v2");
  EXPECT_EQ(ScanTarget(ReadOptions()), "v2");

  db_->ReleaseSnapshot(snap);
}

TEST_F(PreL0ReadPathTest, MultiPstGlobalOrdering) {
  const std::string filler(1000, 'x');
  const int N = static_cast<int>(
      PreL0Manager::kMaxTreeBytes * (PreL0Manager::kFlushThreshold / 2) /
      filler.size());

  std::vector<int> order(N);
  for (int i = 0; i < N; i++) order[i] = i;
  std::shuffle(order.begin(), order.end(), std::mt19937(12345));

  for (int j = 0; j < N; j++) {
    char k[32];
    std::snprintf(k, sizeof(k), "key%06d", order[j]);
    ASSERT_TRUE(db_->Put(WriteOptions(), k, std::string(k) + filler).ok());
    if (j % 1500 == 1499) ASSERT_TRUE(impl_->TEST_CompactMemTable().ok());
  }
  ASSERT_TRUE(impl_->TEST_CompactMemTable().ok());
  ASSERT_EQ(SumLdbBytes(dbname_), 0u);

  Iterator* it = db_->NewIterator(ReadOptions());
  int count = 0;
  std::string prev;
  for (it->SeekToFirst(); it->Valid(); it->Next()) {
    std::string key = it->key().ToString();
    if (count > 0) ASSERT_LT(prev, key) << "forward scan not strictly ascending";
    char ek[32];
    std::snprintf(ek, sizeof(ek), "key%06d", count);
    ASSERT_EQ(key, ek);
    ASSERT_EQ(it->value().ToString(), key + filler);
    prev = key;
    count++;
  }
  ASSERT_TRUE(it->status().ok());
  ASSERT_EQ(count, N);
  delete it;

  it = db_->NewIterator(ReadOptions());
  count = 0;
  for (it->SeekToLast(); it->Valid(); it->Prev()) {
    std::string key = it->key().ToString();
    if (count > 0) ASSERT_GT(prev, key) << "reverse scan not strictly descending";
    char ek[32];
    std::snprintf(ek, sizeof(ek), "key%06d", N - 1 - count);
    ASSERT_EQ(key, ek);
    prev = key;
    count++;
  }
  ASSERT_TRUE(it->status().ok());
  ASSERT_EQ(count, N);
  delete it;
}

TEST_F(PreL0ReadPathTest, TombstoneHonorsSnapshotInGet) {
  SpillTargetToL0("v1");
  const Snapshot* before = db_->GetSnapshot();

  ASSERT_TRUE(db_->Delete(WriteOptions(), "target").ok());
  ASSERT_TRUE(impl_->TEST_CompactMemTable().ok());
  const Snapshot* after = db_->GetSnapshot();

  std::string val;
  ReadOptions ro_before;
  ro_before.snapshot = before;
  EXPECT_TRUE(db_->Get(ro_before, "target", &val).ok());
  EXPECT_EQ(val, "v1");

  ReadOptions ro_after;
  ro_after.snapshot = after;
  EXPECT_TRUE(db_->Get(ro_after, "target", &val).IsNotFound());

  db_->ReleaseSnapshot(before);
  db_->ReleaseSnapshot(after);
}

TEST(PreL0FlushInjectorTest, ReadbackDoesNotDoubleInjectLink) {
  std::string dbname = testing::TempDir() + "prel0_c3_db";
  DestroyDB(dbname, Options());

  Options options;
  options.create_if_missing = true;
  options.enable_pre_l0 = true;
  options.enable_cxl_compaction = true;
  options.pre_l0_cxl_size = 512ULL * 1024 * 1024;
  options.pre_l0_evict_high_watermark = 0.05;
  options.pre_l0_evict_low_watermark = 0.0;
  DB* db = nullptr;
  ASSERT_TRUE(DB::Open(options, dbname, &db).ok());
  DBImpl* impl = reinterpret_cast<DBImpl*>(db);

  g_io_stats.Reset();

  WriteOptions wo;
  std::string filler(1000, 'f');
  const int kSpillKeys = static_cast<int>(
      PreL0Manager::kMaxTreeBytes * (PreL0Manager::kFlushThreshold + 4) /
      filler.size());
  for (int i = 0; i < kSpillKeys; i++) {
    char k[32];
    std::snprintf(k, sizeof(k), "key%08d", i);
    ASSERT_TRUE(db->Put(wo, k, filler).ok());
    if (i % 2000 == 1999) ASSERT_TRUE(impl->TEST_CompactMemTable().ok());
  }
  ASSERT_TRUE(impl->TEST_CompactMemTable().ok());

  ASSERT_GT(SumLdbBytes(dbname), 0u) << "expected a pre-L0 spill to L0";
  EXPECT_GT(g_io_stats.cxl_write_bytes.load(), 0u);

  EXPECT_EQ(impl->TEST_PreL0LinkValueBytes(),
            g_io_stats.prel0_write_bytes.load());

  delete db;
  DestroyDB(dbname, Options());
}

TEST(PreL0FallbackTest, ActiveTreeFillsRegionFallsBackToL0) {
  std::string dbname = testing::TempDir() + "prel0_fallback_db";
  DestroyDB(dbname, Options());

  Options options;
  options.create_if_missing = true;
  options.enable_pre_l0 = true;
  options.pre_l0_fallback_to_l0 = true;
  options.pre_l0_cxl_size = 4ULL * 1024 * 1024;
  DB* db = nullptr;
  ASSERT_TRUE(DB::Open(options, dbname, &db).ok());
  DBImpl* impl = reinterpret_cast<DBImpl*>(db);

  WriteOptions wo;
  std::string filler(1000, 'v');
  const int kKeys = 8000;
  for (int i = 0; i < kKeys; i++) {
    char k[32];
    std::snprintf(k, sizeof(k), "key%08d", i);
    ASSERT_TRUE(db->Put(wo, k, filler).ok());
    if (i % 1000 == 999) ASSERT_TRUE(impl->TEST_CompactMemTable().ok());
  }
  ASSERT_TRUE(impl->TEST_CompactMemTable().ok());

  ASSERT_GT(SumLdbBytes(dbname), 0u) << "expected an OOM → L0 fallback spill";

  ReadOptions ro;
  std::string val;
  for (int i = 0; i < kKeys; i++) {
    char k[32];
    std::snprintf(k, sizeof(k), "key%08d", i);
    ASSERT_TRUE(db->Get(ro, k, &val).ok()) << "missing key " << k;
    ASSERT_EQ(val, filler) << "wrong value for key " << k;
  }

  delete db;
  DestroyDB(dbname, Options());
}

TEST(PreL0WalSnapshotOptionsTest, DefaultsAndBounds) {
  Options options;
  EXPECT_EQ(options.pre_l0_wal_snapshot_threshold_bytes,
            2ULL * 1024 * 1024 * 1024);
  EXPECT_DOUBLE_EQ(options.pre_l0_wal_snapshot_release_ratio, 0.5);

  EXPECT_GT(options.pre_l0_wal_snapshot_release_ratio, 0.0);
  EXPECT_LE(options.pre_l0_wal_snapshot_release_ratio, 1.0);
}

TEST(WalSnapshotTriggerTest, FiresOnceWithHysteresis) {
  WalSnapshotTrigger t(/*threshold=*/1000, /*release_ratio=*/0.5);
  EXPECT_FALSE(t.ShouldTrigger(0));
  EXPECT_FALSE(t.ShouldTrigger(999));
  EXPECT_TRUE(t.ShouldTrigger(1000));
  EXPECT_FALSE(t.ShouldTrigger(1500));
  EXPECT_FALSE(t.ShouldTrigger(5000));
  EXPECT_FALSE(t.ShouldTrigger(600));
  EXPECT_FALSE(t.ShouldTrigger(499));
  EXPECT_FALSE(t.ShouldTrigger(999));
  EXPECT_TRUE(t.ShouldTrigger(2000));
}

TEST(WalSnapshotTriggerTest, DisabledNeverFires) {
  WalSnapshotTrigger t(/*threshold=*/0, /*release_ratio=*/0.5);
  EXPECT_FALSE(t.ShouldTrigger(1ULL << 40));
  EXPECT_FALSE(t.ShouldTrigger(0));
}

namespace {

Options WalSnapshotOptions() {
  Options o;
  o.create_if_missing = true;
  o.enable_pre_l0 = true;
  o.metrics_dir = "";
  o.pre_l0_cxl_size = 256ULL * 1024 * 1024;
  o.write_buffer_size = 32 * 1024;
  o.pre_l0_wal_snapshot_threshold_bytes = 64 * 1024;
  return o;
}

void DriveUpdates(DB* db, DBImpl* impl, int nkeys, int updates,
                  const std::string& value) {
  WriteOptions wo;
  for (int i = 0; i < updates; i++) {
    char k[32];
    std::snprintf(k, sizeof(k), "key%06d", i % nkeys);
    ASSERT_TRUE(db->Put(wo, k, value).ok());
    if (i % 256 == 255) ASSERT_TRUE(impl->TEST_CompactMemTable().ok());
  }
  ASSERT_TRUE(impl->TEST_CompactMemTable().ok());
}

}  // namespace

TEST(PreL0WalSnapshotTest, ReclaimsWalAndKeepsReadsCorrect) {
  std::string dbname = testing::TempDir() + "prel0_wal_snap_reclaim";
  DestroyDB(dbname, Options());
  Options options = WalSnapshotOptions();
  DB* db = nullptr;
  ASSERT_TRUE(DB::Open(options, dbname, &db).ok());
  DBImpl* impl = reinterpret_cast<DBImpl*>(db);

  const int kKeys = 200;
  const std::string v1(200, 'a');
  DriveUpdates(db, impl, kKeys, 4000, v1);

  EXPECT_GE(impl->TEST_WalSnapshotCount(), 1u)
      << "crossing the threshold must auto-trigger at least one snapshot";
  const uint64_t threshold = options.pre_l0_wal_snapshot_threshold_bytes;

  const uint64_t count_before = impl->TEST_WalSnapshotCount();
  ASSERT_TRUE(impl->TEST_TriggerWalSnapshot().ok());
  EXPECT_EQ(impl->TEST_WalSnapshotCount(), count_before + 1);

  const uint64_t live_after = impl->TEST_LiveWalBytes();
  EXPECT_LT(live_after, 16 * threshold)
      << "live WAL bytes must stay bounded (working-set sized), not grow with "
         "the total write volume";

  ReadOptions ro;
  std::string got;
  for (int i = 0; i < kKeys; i++) {
    char k[32];
    std::snprintf(k, sizeof(k), "key%06d", i);
    ASSERT_TRUE(db->Get(ro, k, &got).ok()) << "missing " << k;
    EXPECT_EQ(got, v1);
  }

  delete db;
  db = nullptr;
  ASSERT_TRUE(DB::Open(options, dbname, &db).ok());
  for (int i = 0; i < kKeys; i++) {
    char k[32];
    std::snprintf(k, sizeof(k), "key%06d", i);
    ASSERT_TRUE(db->Get(ro, k, &got).ok()) << "missing after recovery " << k;
    EXPECT_EQ(got, v1);
  }
  delete db;
  DestroyDB(dbname, Options());
}

TEST(PreL0WalSnapshotTest, TombstoneSurvivesAndLatestWins) {
  std::string dbname = testing::TempDir() + "prel0_wal_snap_tomb";
  DestroyDB(dbname, Options());
  Options options = WalSnapshotOptions();
  DB* db = nullptr;
  ASSERT_TRUE(DB::Open(options, dbname, &db).ok());
  DBImpl* impl = reinterpret_cast<DBImpl*>(db);

  WriteOptions wo;
  ASSERT_TRUE(db->Put(wo, "keep", "old").ok());
  ASSERT_TRUE(db->Put(wo, "gone", "x").ok());
  ASSERT_TRUE(impl->TEST_CompactMemTable().ok());
  ASSERT_TRUE(db->Put(wo, "keep", "new").ok());
  ASSERT_TRUE(db->Delete(wo, "gone").ok());
  ASSERT_TRUE(impl->TEST_CompactMemTable().ok());

  DriveUpdates(db, impl, 100, 2000, std::string(200, 'p'));

  const uint64_t count_before = impl->TEST_WalSnapshotCount();
  ASSERT_TRUE(impl->TEST_TriggerWalSnapshot().ok());
  EXPECT_EQ(impl->TEST_WalSnapshotCount(), count_before + 1);

  delete db;
  db = nullptr;
  ASSERT_TRUE(DB::Open(options, dbname, &db).ok());
  ReadOptions ro;
  std::string got;
  EXPECT_TRUE(db->Get(ro, "keep", &got).ok());
  EXPECT_EQ(got, "new") << "latest version must win after snapshot+recovery";
  EXPECT_TRUE(db->Get(ro, "gone", &got).IsNotFound())
      << "tombstone must survive the snapshot (no resurrection)";
  delete db;
  DestroyDB(dbname, Options());
}

TEST(PreL0WalSnapshotTest, ConcurrentWritesDuringSnapshot) {
  std::string dbname = testing::TempDir() + "prel0_wal_snap_concurrent";
  DestroyDB(dbname, Options());
  Options options = WalSnapshotOptions();
  DB* db = nullptr;
  ASSERT_TRUE(DB::Open(options, dbname, &db).ok());
  DBImpl* impl = reinterpret_cast<DBImpl*>(db);

  DriveUpdates(db, impl, 150, 3000, std::string(200, 'c'));

  std::atomic<bool> stop{false};
  std::thread writer([&] {
    WriteOptions wo;
    int i = 0;
    while (!stop.load(std::memory_order_relaxed)) {
      char k[32];
      std::snprintf(k, sizeof(k), "cc%06d", i++);
      db->Put(wo, k, "z");
    }
  });

  const uint64_t count_before = impl->TEST_WalSnapshotCount();
  ASSERT_TRUE(impl->TEST_TriggerWalSnapshot().ok());
  stop.store(true, std::memory_order_relaxed);
  writer.join();
  EXPECT_EQ(impl->TEST_WalSnapshotCount(), count_before + 1);

  ReadOptions ro;
  std::string got;
  for (int i = 0; i < 100; i++) {
    char k[32];
    std::snprintf(k, sizeof(k), "cc%06d", i);
    ASSERT_TRUE(db->Get(ro, k, &got).ok()) << "lost concurrent write " << k;
  }
  ASSERT_TRUE(db->Get(ro, "key000000", &got).ok());
  delete db;
  DestroyDB(dbname, Options());
}

}  // namespace leveldb
