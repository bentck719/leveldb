#ifndef STORAGE_LEVELDB_DB_PRE_L0_BPLUS_TREE_H_
#define STORAGE_LEVELDB_DB_PRE_L0_BPLUS_TREE_H_

#include <cassert>
#include <cstdint>
#include <vector>

#include "db/dbformat.h"
#include "db/pre_l0/cxl_memory.h"
#include "leveldb/slice.h"

namespace leveldb {

struct BPlusTreeValue {
  uint32_t value_offset;
  uint32_t value_len;
  SequenceNumber seq;
  ValueType type;
};

class BPlusTree {
 public:
  explicit BPlusTree(CxlMemory* mem);

  ~BPlusTree();

  bool Insert(const Slice& user_key, const Slice& value,
              SequenceNumber seq, ValueType type);

  bool Update(const Slice& user_key, const Slice& value,
              SequenceNumber seq, ValueType type);

  bool Get(const Slice& user_key, BPlusTreeValue* out) const;

  bool Delete(const Slice& user_key);

  size_t Count() const { return count_; }

  size_t ByteSize() const { return byte_size_; }

  class Iterator {
   public:
    explicit Iterator(const BPlusTree* tree);
    void SeekToFirst();
    bool Valid() const;
    Slice key() const;
    BPlusTreeValue value() const;
    void Next();

   private:
    const BPlusTree* tree_;
    uint32_t leaf_off_;
    int pos_;
  };

  Iterator* NewIterator() const;

 private:
  static constexpr int kMaxLeafKeys     = 7;
  static constexpr int kMaxInternalKeys = 14;

  struct NodeHeader {
    uint8_t  is_leaf;
    uint8_t  num_keys;
    uint8_t  pad[6];
    uint32_t parent_off;
    uint32_t next_leaf;
  };
  static_assert(sizeof(NodeHeader) == 16, "NodeHeader must be 16 bytes");

  struct LeafSlot {
    uint32_t key_offset;
    uint32_t key_len;
    uint32_t value_offset;
    uint32_t value_len;
    uint64_t seq;
    uint8_t  type;
    uint8_t  pad[7];
  };
  static_assert(sizeof(LeafSlot) == 32, "LeafSlot must be 32 bytes");

  struct InternalSlot {
    uint32_t key_offset;
    uint32_t key_len;
    uint32_t child_offset;
    uint32_t pad;
  };
  static_assert(sizeof(InternalSlot) == 16, "InternalSlot must be 16 bytes");

  struct NodeReservation {
    std::vector<uint32_t> nodes;
    size_t next = 0;
    uint32_t Take() {
      assert(next < nodes.size());
      return nodes[next++];
    }
  };

  NodeHeader* header(uint32_t off) const {
    return static_cast<NodeHeader*>(mem_->ptr(off));
  }
  LeafSlot* leaf_slots(uint32_t off) const {
    return reinterpret_cast<LeafSlot*>(
        static_cast<char*>(mem_->ptr(off)) + sizeof(NodeHeader));
  }
  uint32_t* leftmost_child(uint32_t off) const {
    return reinterpret_cast<uint32_t*>(
        static_cast<char*>(mem_->ptr(off)) + sizeof(NodeHeader));
  }
  InternalSlot* internal_slots(uint32_t off) const {
    return reinterpret_cast<InternalSlot*>(
        static_cast<char*>(mem_->ptr(off)) + sizeof(NodeHeader) + sizeof(uint32_t));
  }

  uint32_t FindLeaf(const Slice& user_key) const;

  int LowerBound(uint32_t leaf_off, const Slice& user_key) const;
  int LowerBoundInternal(uint32_t node_off, const Slice& user_key) const;

  Slice KeyAt(uint32_t node_off, int slot, bool is_leaf) const;

  void InitNode(uint32_t off, bool is_leaf);

  uint32_t AllocNode(bool is_leaf);

  int Height() const;

  uint32_t SplitLeaf(uint32_t leaf_off, uint32_t* split_key_off,
                     uint32_t* split_key_len, NodeReservation* res);

  void InsertIntoParent(uint32_t left_off, uint32_t right_off,
                        uint32_t split_key_off, uint32_t split_key_len,
                        NodeReservation* res);

  uint32_t FirstLeaf() const;

#ifndef NDEBUG
  void DebugCheckSeparators(uint32_t node_off) const;
#endif

  void FreeSubtree(uint32_t node_off);

  CxlMemory* mem_;
  uint32_t root_off_;
  size_t count_;
  size_t byte_size_;

#ifndef NDEBUG
  bool dbg_had_delete_ = false;
#endif
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_PRE_L0_BPLUS_TREE_H_
