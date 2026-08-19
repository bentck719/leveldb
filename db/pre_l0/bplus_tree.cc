#include "db/pre_l0/bplus_tree.h"

#include <algorithm>
#include <cassert>
#include <cstring>

namespace leveldb {

BPlusTree::BPlusTree(CxlMemory* mem) : mem_(mem), count_(0), byte_size_(0) {
  root_off_ = AllocNode(/*is_leaf=*/true);
  assert(root_off_ != CxlMemory::kInvalidOffset);
}

BPlusTree::~BPlusTree() {
  if (root_off_ != CxlMemory::kInvalidOffset) {
    FreeSubtree(root_off_);
    root_off_ = CxlMemory::kInvalidOffset;
  }
}

void BPlusTree::FreeSubtree(uint32_t node_off) {
  NodeHeader* h = header(node_off);
  if (h->is_leaf) {
    LeafSlot* slots = leaf_slots(node_off);
    for (int i = 0; i < h->num_keys; i++) {
      mem_->FreeKV(slots[i].key_offset, slots[i].key_len);
      mem_->FreeKV(slots[i].value_offset, slots[i].value_len);
    }
  } else {
#ifndef NDEBUG
    DebugCheckSeparators(node_off);
#endif
    FreeSubtree(*leftmost_child(node_off));
    InternalSlot* slots = internal_slots(node_off);
    for (int i = 0; i < h->num_keys; i++) {
      FreeSubtree(slots[i].child_offset);
    }
  }
  mem_->FreeNode(node_off);
}

void BPlusTree::InitNode(uint32_t off, bool is_leaf) {
  memset(mem_->ptr(off), 0, CxlMemory::kNodeSize);
  NodeHeader* h = header(off);
  h->is_leaf = is_leaf ? 1 : 0;
  h->num_keys = 0;
  h->parent_off = CxlMemory::kInvalidOffset;
  h->next_leaf = CxlMemory::kInvalidOffset;
}

uint32_t BPlusTree::AllocNode(bool is_leaf) {
  uint32_t off = mem_->AllocateNode();
  if (off == CxlMemory::kInvalidOffset) return off;
  InitNode(off, is_leaf);
  return off;
}

int BPlusTree::Height() const {
  int h = 1;
  uint32_t cur = root_off_;
  while (!header(cur)->is_leaf) {
    cur = *leftmost_child(cur);
    h++;
  }
  return h;
}

#ifndef NDEBUG
void BPlusTree::DebugCheckSeparators(uint32_t node_off) const {
  NodeHeader* h = header(node_off);
  assert(!h->is_leaf);
  InternalSlot* slots = internal_slots(node_off);
  for (int i = 0; i < h->num_keys; i++) {
    uint32_t cur = slots[i].child_offset;
    while (!header(cur)->is_leaf) cur = *leftmost_child(cur);
    NodeHeader* lh = header(cur);
    assert(lh->num_keys > 0 && "leftmost leaf of a subtree must be non-empty");
    LeafSlot* ls = leaf_slots(cur);
    Slice sep(static_cast<char*>(mem_->ptr(slots[i].key_offset)),
              slots[i].key_len);
    Slice first(static_cast<char*>(mem_->ptr(ls->key_offset)), ls->key_len);
    assert(sep.compare(first) == 0 &&
           "FreeSubtree: separator must equal its subtree's smallest key");
    if (!dbg_had_delete_) {
      assert(slots[i].key_offset == ls->key_offset &&
             "FreeSubtree separator-aliasing bijection broken (leak/double-free)");
    }
  }
}
#endif

Slice BPlusTree::KeyAt(uint32_t node_off, int slot, bool is_leaf) const {
  if (is_leaf) {
    LeafSlot* s = leaf_slots(node_off) + slot;
    return Slice(static_cast<char*>(mem_->ptr(s->key_offset)), s->key_len);
  } else {
    InternalSlot* s = internal_slots(node_off) + slot;
    return Slice(static_cast<char*>(mem_->ptr(s->key_offset)), s->key_len);
  }
}

int BPlusTree::LowerBound(uint32_t leaf_off, const Slice& user_key) const {
  mem_->TouchNode();
  NodeHeader* h = header(leaf_off);
  int lo = 0, hi = h->num_keys;
  while (lo < hi) {
    int mid = (lo + hi) / 2;
    Slice k = KeyAt(leaf_off, mid, /*is_leaf=*/true);
    if (k.compare(user_key) < 0) lo = mid + 1;
    else hi = mid;
  }
  return lo;
}

int BPlusTree::LowerBoundInternal(uint32_t node_off,
                                   const Slice& user_key) const {
  mem_->TouchNode();
  NodeHeader* h = header(node_off);
  int lo = 0, hi = h->num_keys;
  while (lo < hi) {
    int mid = (lo + hi) / 2;
    Slice k = KeyAt(node_off, mid, /*is_leaf=*/false);
    if (k.compare(user_key) < 0) lo = mid + 1;
    else hi = mid;
  }
  return lo;
}

uint32_t BPlusTree::FindLeaf(const Slice& user_key) const {
  uint32_t cur = root_off_;
  while (true) {
    NodeHeader* h = header(cur);
    if (h->is_leaf) return cur;
    mem_->TouchNode();
    NodeHeader* nh = header(cur);
    int lo = 0, hi = nh->num_keys;
    while (lo < hi) {
      int mid = (lo + hi) / 2;
      if (KeyAt(cur, mid, /*is_leaf=*/false).compare(user_key) <= 0)
        lo = mid + 1;
      else
        hi = mid;
    }
    uint32_t child;
    if (lo == 0) {
      child = *leftmost_child(cur);
    } else {
      child = internal_slots(cur)[lo - 1].child_offset;
    }
    cur = child;
  }
}

uint32_t BPlusTree::FirstLeaf() const {
  uint32_t cur = root_off_;
  while (true) {
    NodeHeader* h = header(cur);
    if (h->is_leaf) return cur;
    cur = *leftmost_child(cur);
  }
}

bool BPlusTree::Insert(const Slice& user_key, const Slice& value,
                       SequenceNumber seq, ValueType type) {
  uint32_t leaf = FindLeaf(user_key);
  NodeHeader* h = header(leaf);

  uint32_t koff = mem_->AllocateKV(user_key.size());
  if (koff == CxlMemory::kInvalidOffset) return false;
  memcpy(mem_->ptr(koff), user_key.data(), user_key.size());

  uint32_t voff = mem_->AllocateKV(value.size() ? value.size() : 1);
  if (voff == CxlMemory::kInvalidOffset) {
    mem_->FreeKV(koff, user_key.size());
    return false;
  }
  if (value.size()) memcpy(mem_->ptr(voff), value.data(), value.size());

  int pos = LowerBound(leaf, user_key);

  if (h->num_keys < kMaxLeafKeys) {
    LeafSlot* slots = leaf_slots(leaf);
    memmove(slots + pos + 1, slots + pos,
            sizeof(LeafSlot) * (h->num_keys - pos));
    slots[pos] = {koff, static_cast<uint32_t>(user_key.size()),
                  voff, static_cast<uint32_t>(value.size()),
                  seq, static_cast<uint8_t>(type), {0, 0, 0}};
    h->num_keys++;
    count_++;
    byte_size_ += user_key.size() + value.size();
    return true;
  }

  NodeReservation res;
  int need = Height() + 1;
  res.nodes.reserve(need);
  for (int i = 0; i < need; i++) {
    uint32_t n = mem_->AllocateNode();
    if (n == CxlMemory::kInvalidOffset) {
      for (uint32_t r : res.nodes) mem_->FreeNode(r);
      mem_->FreeKV(koff, user_key.size());
      mem_->FreeKV(voff, value.size() ? value.size() : 1);
      return false;
    }
    res.nodes.push_back(n);
  }

  uint32_t split_key_off, split_key_len;
  uint32_t right = SplitLeaf(leaf, &split_key_off, &split_key_len, &res);

  Slice split_key(static_cast<char*>(mem_->ptr(split_key_off)), split_key_len);
  uint32_t target = (user_key.compare(split_key) < 0) ? leaf : right;

  NodeHeader* th = header(target);
  int tpos = LowerBound(target, user_key);
  LeafSlot* tslots = leaf_slots(target);
  memmove(tslots + tpos + 1, tslots + tpos,
          sizeof(LeafSlot) * (th->num_keys - tpos));
  tslots[tpos] = {koff, static_cast<uint32_t>(user_key.size()),
                  voff, static_cast<uint32_t>(value.size()),
                  seq, static_cast<uint8_t>(type), {0, 0, 0}};
  th->num_keys++;
  count_++;
  byte_size_ += user_key.size() + value.size();

  InsertIntoParent(leaf, right, split_key_off, split_key_len, &res);

  for (size_t i = res.next; i < res.nodes.size(); i++) {
    mem_->FreeNode(res.nodes[i]);
  }
  return true;
}

uint32_t BPlusTree::SplitLeaf(uint32_t leaf_off, uint32_t* split_key_off,
                               uint32_t* split_key_len, NodeReservation* res) {
  NodeHeader* lh = header(leaf_off);
  uint32_t right = res->Take();
  InitNode(right, /*is_leaf=*/true);
  NodeHeader* rh = header(right);
  rh->parent_off = lh->parent_off;
  rh->next_leaf = lh->next_leaf;
  lh->next_leaf = right;

  int mid = kMaxLeafKeys / 2;
  LeafSlot* lslots = leaf_slots(leaf_off);
  LeafSlot* rslots = leaf_slots(right);
  int rcount = lh->num_keys - mid;
  memcpy(rslots, lslots + mid, sizeof(LeafSlot) * rcount);
  rh->num_keys = static_cast<uint8_t>(rcount);
  lh->num_keys = static_cast<uint8_t>(mid);

  *split_key_off = rslots[0].key_offset;
  *split_key_len = rslots[0].key_len;
  return right;
}

void BPlusTree::InsertIntoParent(uint32_t left_off, uint32_t right_off,
                                  uint32_t split_key_off,
                                  uint32_t split_key_len,
                                  NodeReservation* res) {
  NodeHeader* lh = header(left_off);
  uint32_t parent = lh->parent_off;

  if (parent == CxlMemory::kInvalidOffset) {
    uint32_t new_root = res->Take();
    InitNode(new_root, /*is_leaf=*/false);
    NodeHeader* rth = header(new_root);
    *leftmost_child(new_root) = left_off;
    InternalSlot* slots = internal_slots(new_root);
    slots[0] = {split_key_off, split_key_len, right_off, 0};
    rth->num_keys = 1;
    lh->parent_off = new_root;
    header(right_off)->parent_off = new_root;
    root_off_ = new_root;
    return;
  }

  NodeHeader* ph = header(parent);
  if (ph->num_keys < kMaxInternalKeys) {
    int idx = LowerBoundInternal(parent, Slice(static_cast<char*>(
        mem_->ptr(split_key_off)), split_key_len));
    InternalSlot* pslots = internal_slots(parent);
    memmove(pslots + idx + 1, pslots + idx,
            sizeof(InternalSlot) * (ph->num_keys - idx));
    pslots[idx] = {split_key_off, split_key_len, right_off, 0};
    ph->num_keys++;
    header(right_off)->parent_off = parent;
    return;
  }

  int cur = ph->num_keys;
  std::vector<uint32_t> children(cur + 1);
  std::vector<uint32_t> koffs(cur), klens(cur);

  children[0] = *leftmost_child(parent);
  InternalSlot* pslots = internal_slots(parent);
  for (int i = 0; i < cur; i++) {
    koffs[i] = pslots[i].key_offset;
    klens[i] = pslots[i].key_len;
    children[i + 1] = pslots[i].child_offset;
  }

  Slice sk(static_cast<char*>(mem_->ptr(split_key_off)), split_key_len);
  int ins = 0;
  while (ins < cur &&
         Slice(static_cast<char*>(mem_->ptr(koffs[ins])), klens[ins]).compare(sk) < 0) {
    ins++;
  }
  koffs.insert(koffs.begin() + ins, split_key_off);
  klens.insert(klens.begin() + ins, split_key_len);
  children.insert(children.begin() + ins + 1, right_off);
  int total = cur + 1;

  int mid = total / 2;
  uint32_t promote_koff = koffs[mid];
  uint32_t promote_klen = klens[mid];

  *leftmost_child(parent) = children[0];
  for (int i = 0; i < mid; i++) {
    pslots[i] = {koffs[i], klens[i], children[i + 1], 0};
  }
  ph->num_keys = static_cast<uint8_t>(mid);

  uint32_t new_internal = res->Take();
  InitNode(new_internal, /*is_leaf=*/false);
  NodeHeader* nih = header(new_internal);
  *leftmost_child(new_internal) = children[mid + 1];
  InternalSlot* nslots = internal_slots(new_internal);
  int rcount = total - mid - 1;
  for (int i = 0; i < rcount; i++) {
    nslots[i] = {koffs[mid + 1 + i], klens[mid + 1 + i], children[mid + 2 + i], 0};
  }
  nih->num_keys = static_cast<uint8_t>(rcount);
  nih->parent_off = ph->parent_off;

  auto update_parent = [&](uint32_t child, uint32_t p) {
    header(child)->parent_off = p;
  };
  update_parent(*leftmost_child(new_internal), new_internal);
  for (int i = 0; i < rcount; i++) {
    update_parent(nslots[i].child_offset, new_internal);
  }

  InsertIntoParent(parent, new_internal, promote_koff, promote_klen, res);
}

bool BPlusTree::Update(const Slice& user_key, const Slice& value,
                       SequenceNumber seq, ValueType type) {
  uint32_t leaf = FindLeaf(user_key);
  NodeHeader* h = header(leaf);
  int pos = LowerBound(leaf, user_key);
  if (pos >= h->num_keys) return false;
  Slice k = KeyAt(leaf, pos, /*is_leaf=*/true);
  if (k.compare(user_key) != 0) return false;

  LeafSlot* s = leaf_slots(leaf) + pos;
  uint32_t old_value_len = s->value_len;
  mem_->FreeKV(s->value_offset, s->value_len);
  uint32_t voff = mem_->AllocateKV(value.size() ? value.size() : 1);
  if (voff == CxlMemory::kInvalidOffset) return false;
  if (value.size()) memcpy(mem_->ptr(voff), value.data(), value.size());
  s->value_offset = voff;
  s->value_len = static_cast<uint32_t>(value.size());
  s->seq = seq;
  s->type = static_cast<uint8_t>(type);
  byte_size_ += value.size();
  byte_size_ -= old_value_len;
  return true;
}

bool BPlusTree::Get(const Slice& user_key, BPlusTreeValue* out) const {
  uint32_t leaf = FindLeaf(user_key);
  NodeHeader* h = header(leaf);
  int pos = LowerBound(leaf, user_key);
  if (pos >= h->num_keys) return false;
  Slice k = KeyAt(leaf, pos, /*is_leaf=*/true);
  if (k.compare(user_key) != 0) return false;
  LeafSlot* s = leaf_slots(leaf) + pos;
  out->value_offset = s->value_offset;
  out->value_len    = s->value_len;
  out->seq          = s->seq;
  out->type         = static_cast<ValueType>(s->type);
  return true;
}

bool BPlusTree::Delete(const Slice& user_key) {
  uint32_t leaf = FindLeaf(user_key);
  NodeHeader* h = header(leaf);
  int pos = LowerBound(leaf, user_key);
  if (pos >= h->num_keys) return false;
  Slice k = KeyAt(leaf, pos, /*is_leaf=*/true);
  if (k.compare(user_key) != 0) return false;

  LeafSlot* s = leaf_slots(leaf) + pos;
  if (pos != 0) {
    mem_->FreeKV(s->key_offset, s->key_len);
  }
  mem_->FreeKV(s->value_offset, s->value_len);
  byte_size_ -= (s->key_len + s->value_len);

  LeafSlot* slots = leaf_slots(leaf);
  memmove(slots + pos, slots + pos + 1,
          sizeof(LeafSlot) * (h->num_keys - pos - 1));
  h->num_keys--;
  count_--;
#ifndef NDEBUG
  dbg_had_delete_ = true;
#endif
  return true;
}

BPlusTree::Iterator::Iterator(const BPlusTree* tree)
    : tree_(tree), leaf_off_(CxlMemory::kInvalidOffset), pos_(0) {}

void BPlusTree::Iterator::SeekToFirst() {
  leaf_off_ = tree_->FirstLeaf();
  pos_ = 0;
  while (leaf_off_ != CxlMemory::kInvalidOffset) {
    if (tree_->header(leaf_off_)->num_keys > 0) break;
    leaf_off_ = tree_->header(leaf_off_)->next_leaf;
  }
}

bool BPlusTree::Iterator::Valid() const {
  if (leaf_off_ == CxlMemory::kInvalidOffset) return false;
  return pos_ < tree_->header(leaf_off_)->num_keys;
}

Slice BPlusTree::Iterator::key() const {
  LeafSlot* s = tree_->leaf_slots(leaf_off_) + pos_;
  return Slice(static_cast<char*>(tree_->mem_->ptr(s->key_offset)), s->key_len);
}

BPlusTreeValue BPlusTree::Iterator::value() const {
  LeafSlot* s = tree_->leaf_slots(leaf_off_) + pos_;
  return {s->value_offset, s->value_len, s->seq,
          static_cast<ValueType>(s->type)};
}

void BPlusTree::Iterator::Next() {
  pos_++;
  while (leaf_off_ != CxlMemory::kInvalidOffset &&
         pos_ >= tree_->header(leaf_off_)->num_keys) {
    leaf_off_ = tree_->header(leaf_off_)->next_leaf;
    pos_ = 0;
    if (leaf_off_ != CxlMemory::kInvalidOffset) {
      tree_->mem_->TouchNode();
    }
  }
}

BPlusTree::Iterator* BPlusTree::NewIterator() const {
  return new Iterator(this);
}

}  // namespace leveldb
