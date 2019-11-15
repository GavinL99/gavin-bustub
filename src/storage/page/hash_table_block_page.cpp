//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_table_block_page.cpp
//
// Identification: src/storage/page/hash_table_block_page.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/hash_table_block_page.h"
#include "cassert"
#include "storage/index/generic_key.h"
#include <include/storage/index/hash_comparator.h>

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
KeyType HASH_TABLE_BLOCK_TYPE::KeyAt(slot_offset_t bucket_ind) const {
  return array_[bucket_ind].first;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
ValueType HASH_TABLE_BLOCK_TYPE::ValueAt(slot_offset_t bucket_ind) const {
  return array_[bucket_ind].second;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BLOCK_TYPE::Insert(slot_offset_t bucket_ind, const KeyType &key, const ValueType &value) {
  size_t char_idx = bucket_ind / 8;
  size_t bit_idx = bucket_ind % 8;
  // can still insert if tombstone
  if ((occupied_[char_idx] >> bit_idx) & 0x01 && (readable_[char_idx] >> bit_idx) & 0x01) {
    return false;
  }
  // set bit for readable, occupied to 1
  occupied_[char_idx] |= (0x01 << bit_idx);
  readable_[char_idx] |= (0x01 << bit_idx);
  array_[bucket_ind].first = key;
  array_[bucket_ind].second = value;
  return true;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BLOCK_TYPE::Remove(slot_offset_t bucket_ind) {
  // only set readable to 0
  unsigned int one = 0x01;
  size_t char_idx = bucket_ind / 8;
  size_t bit_idx = bucket_ind % 8;
  // if occupied
  if ((occupied_[char_idx] >> bit_idx) & one) {
    readable_[char_idx] ^= (one << bit_idx);
  }
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BLOCK_TYPE::IsOccupied(slot_offset_t bucket_ind) const {
  unsigned int one = 0x01;
  size_t char_idx = bucket_ind / 8;
  size_t bit_idx = bucket_ind % 8;
  assert(char_idx < ((BLOCK_ARRAY_SIZE - 1) / 8 + 1));
  return (occupied_[char_idx] >> bit_idx) & one;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BLOCK_TYPE::IsReadable(slot_offset_t bucket_ind) const {
  unsigned int one = 0x01;
  size_t char_idx = bucket_ind / 8;
  size_t bit_idx = bucket_ind % 8;
  assert(char_idx < ((BLOCK_ARRAY_SIZE - 1) / 8 + 1));
  return (readable_[char_idx] >> bit_idx) & one;
}

// DO NOT REMOVE ANYTHING BELOW THIS LINE
template class HashTableBlockPage<int, int, IntComparator>;
template class HashTableBlockPage<hash_t, TmpTuple, HashComparator>;
template class HashTableBlockPage<GenericKey<4>, RID, GenericComparator<4>>;
template class HashTableBlockPage<GenericKey<8>, RID, GenericComparator<8>>;
template class HashTableBlockPage<GenericKey<16>, RID, GenericComparator<16>>;
template class HashTableBlockPage<GenericKey<32>, RID, GenericComparator<32>>;
template class HashTableBlockPage<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
