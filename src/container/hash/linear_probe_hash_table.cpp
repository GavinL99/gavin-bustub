//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// linear_probe_hash_table.cpp
//
// Identification: src/container/hash/linear_probe_hash_table.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "container/hash/linear_probe_hash_table.h"
#include <iostream>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>
#include <include/common/util/hash_util.h>
#include "cassert"
#include "common/logger.h"
#include "common/rid.h"
//#include "storage/index/hash_comparator.h"
#include "storage/table/tmp_tuple.h"

namespace bustub {
#define MAX_NUM_BLOCK_PAGES 1020

class HashComparator {
public:
  inline int operator()(const hash_t lhs, const hash_t rhs) { return lhs < rhs ? -1 : (lhs > rhs ? 1 : 0); }
};

//void lock_with_set(std::unordered_set<Page *> page_set, Page *ptr, bool if_lock, bool if_write) {
//  if (if_lock) {
//    if (page_set.find(ptr) == page_set.end()) {
//      page_set.insert(ptr);
//      if (if_write) {
//        ptr->WLatch();
//      } else {
//        ptr->RLatch();
//      }
//    }
//  } else {
//    if (page_set.find(ptr) != page_set.end()) {
//      page_set.erase(ptr);
//      if (if_write) {
//        ptr->WUnlatch();
//      } else {
//        ptr->RUnlatch();
//      }
//    }
//  }
//}

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_TYPE::LinearProbeHashTable(const std::string &name, BufferPoolManager *buffer_pool_manager,
                                      const KeyComparator &comparator, size_t num_buckets,
                                      HashFunction<KeyType> hash_fn)
    : buffer_pool_manager_(buffer_pool_manager), comparator_(comparator), hash_fn_(std::move(hash_fn)) {
  // allocate memory for header / block pages based on num_buckets, assume always success
  page_id_t temp_p = INVALID_PAGE_ID;
  num_block_pages_ = (num_buckets + BLOCK_ARRAY_SIZE - 1) / BLOCK_ARRAY_SIZE;
  num_buckets_ = num_block_pages_ * BLOCK_ARRAY_SIZE;

  // allocate header page
  header_page_id_ = INVALID_PAGE_ID;
  auto header_page =
      reinterpret_cast<HashTableHeaderPage *>(buffer_pool_manager_->NewPage(&header_page_id_)->GetData());
  header_page->SetSize(num_buckets_);
  header_page->SetPageId(header_page_id_);
  // block pages, need to add all buckets
  for (size_t i = 0; i < num_block_pages_; ++i) {
    buffer_pool_manager_->NewPage(&temp_p, nullptr);
    header_page->AddBlockPageId(temp_p);
    // need to unpin after allocation (flush here!)
//    buffer_pool_manager_->FlushPage(temp_p);
    buffer_pool_manager_->UnpinPage(temp_p, true);
  }
//  buffer_pool_manager_->FlushPage(header_page_id_);
  buffer_pool_manager_->UnpinPage(header_page_id_, true);
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::GetValue(Transaction *transaction, const KeyType &key, std::vector<ValueType> *result) {
  // hash f -> page_id -> block page -> linear probe -> stop till not occupied
  // get the actual data
  std::unordered_set<Page *> page_latch_set;
  // LOG_DEBUG("Lock\n");
  table_latch_.RLock();
  // LOG_DEBUG("Locked\n");
  Page *header_page_p = buffer_pool_manager_->FetchPage(header_page_id_);
  assert(header_page_p);
  // LOG_DEBUG("Lock\n");
  header_page_p->RLatch();
  // LOG_DEBUG("Locked\n");
  auto header_page = reinterpret_cast<HashTableHeaderPage *>(header_page_p->GetData());
  uint64_t bucket_id = hash_fn_.GetHash(key) % num_buckets_;
  uint64_t start_id = bucket_id;
  // where to start linear probing
  page_id_t page_id = header_page->GetBlockPageId(bucket_id / BLOCK_ARRAY_SIZE);
  // if need to read a new page or check the next page (or wrap around)
  bool switch_page = true;

  slot_offset_t offset;
  BLOCK_PAGE_TYPE *block_page(nullptr);
  // crab latch and unlatch
  Page *temp_page(nullptr);
  Page *next_latch_page(nullptr);

  while (true) {
    // need to fetch a new block page
    if (switch_page) {
      // if need to latch the next page
      if (temp_page != nullptr) {
        next_latch_page = buffer_pool_manager_->FetchPage(page_id);
        assert(next_latch_page);
        // LOG_DEBUG("Lock\n");
        temp_page->RUnlatch();
        next_latch_page->RLatch();
        // LOG_DEBUG("Locked\n");
//        lock_with_set(page_latch_set, next_latch_page, true, false);
//        lock_with_set(page_latch_set, temp_page, false, false);
        temp_page = next_latch_page;
      } else {
        temp_page = buffer_pool_manager_->FetchPage(page_id);
        assert(temp_page);
        temp_page->RLatch();
//        lock_with_set(page_latch_set, temp_page, true, false);
      }
      block_page = reinterpret_cast<BLOCK_PAGE_TYPE *>(temp_page->GetData());
      switch_page = false;
    }
    // block_page slot
    offset = bucket_id % BLOCK_ARRAY_SIZE;
    // if not found
    if (!block_page->IsOccupied(offset)) {
      break;
    }
    // if match
    if (block_page->IsReadable(offset) && comparator_(block_page->KeyAt(offset), key) == 0) {
      result->push_back(block_page->ValueAt(offset));
    }
    // linear probe
    bucket_id = (bucket_id + 1) % num_buckets_;
    // if wrapped around
    if (bucket_id == start_id) {
      break;
    }
    // only switch if have more than 1 page
    if (bucket_id % BLOCK_ARRAY_SIZE == 0 && num_block_pages_ > 1) {
      switch_page = true;
      // need to unpin page
      // will unlatch at the start of loop
      assert(buffer_pool_manager_->UnpinPage(page_id, false));
      page_id = header_page->GetBlockPageId(bucket_id / BLOCK_ARRAY_SIZE);
    }
  }
  assert(buffer_pool_manager_->UnpinPage(page_id, false));
  assert(buffer_pool_manager_->UnpinPage(header_page_id_, false));
  temp_page->RUnlatch();
  header_page_p->RUnlatch();
  //  lock_with_set(page_latch_set, temp_page, false, false);
  //    lock_with_set(page_latch_set, header_page_p, false, false);
//  assert(page_latch_set.empty());
  table_latch_.RUnlock();
  // LOG_DEBUG("Finished Get..\n");
  return !result->empty();
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Insert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  // need to traverse until the first vacant slot in case of duplicates
  // but insertion can be tombstones in the middle
  std::unordered_set<Page *> page_latch_set;
  // LOG_DEBUG("Acquire read lock...\n");
  table_latch_.RLock();
  // LOG_DEBUG("Locked\n");
  Page *header_page_p = buffer_pool_manager_->FetchPage(header_page_id_);
  assert(header_page_p);
  // LOG_DEBUG("Acquire header page lock...\n");
  header_page_p->RLatch();
  // LOG_DEBUG("Locked\n");
//  lock_with_set(page_latch_set, header_page_p, true, false);
  auto header_page = reinterpret_cast<HashTableHeaderPage *>(header_page_p->GetData());
  uint64_t bucket_id = hash_fn_.GetHash(key) % num_buckets_;
  uint64_t start_id = bucket_id;
  // where to start linear probing
  page_id_t page_id = header_page->GetBlockPageId(bucket_id / BLOCK_ARRAY_SIZE);

  // if need to read a new page or check the next page (or wrap around)
  bool switch_page = true;
  // if need to insert at some tombstones
  bool insert_flag = false;
  // for tombstones insertion
  page_id_t insert_page_id = INVALID_PAGE_ID;
  slot_offset_t offset(0);
  slot_offset_t insert_offset(0);
  BLOCK_PAGE_TYPE *block_page(nullptr);
  BLOCK_PAGE_TYPE *insert_page(nullptr);
  Page *temp_page(nullptr);
  Page *next_latch_page(nullptr);
  Page *insert_latch_page(nullptr);

  while (true) {
    // fetch block page
    if (switch_page) {
      // crab latch
      if (temp_page != nullptr) {
        next_latch_page = buffer_pool_manager_->FetchPage(page_id);
        assert(next_latch_page);
        // LOG_DEBUG("Lock\n");
        temp_page->WUnlatch();
        next_latch_page->WLatch();
        // LOG_DEBUG("Locked\n");
//        lock_with_set(page_latch_set, next_latch_page, true, true);
//        lock_with_set(page_latch_set, temp_page, false, true);
        temp_page = next_latch_page;
      } else {
//        // LOG_DEBUG("Fetch first block page... %d\n", (int)page_id);
        temp_page = buffer_pool_manager_->FetchPage(page_id);
        assert(temp_page);
//         LOG_DEBUG("Lock\n");
        temp_page->WLatch();
//         LOG_DEBUG("Locked\n");
//        lock_with_set(page_latch_set, temp_page, true, true);
      }
      block_page = reinterpret_cast<BLOCK_PAGE_TYPE *>(temp_page->GetData());
      switch_page = false;
    }
    // block_page slot
    offset = bucket_id % BLOCK_ARRAY_SIZE;
    // Two cases of success:
    // if vacant or have checked all but no duplicates
    // and there's tombstone to insert
    if (!block_page->IsOccupied(offset) || (bucket_id == start_id && insert_page_id != INVALID_PAGE_ID)) {
      // if insert into tombstones
      if (insert_page_id != INVALID_PAGE_ID) {
        //          // LOG_DEBUG("Insert tombstones...\n");
        assert(insert_page->Insert(insert_offset, key, value));
        // unpin current page on hold
        if (insert_page_id != page_id) {
          assert(buffer_pool_manager_->UnpinPage(insert_page_id, true));
          insert_latch_page->WUnlatch();
//          lock_with_set(page_latch_set, insert_latch_page, false, true);
          assert(buffer_pool_manager_->UnpinPage(page_id, false));
        }
      } else {
        //          // LOG_DEBUG("Insert vacant...\n");
        //  if insert here
        assert(block_page->Insert(offset, key, value));
        assert(buffer_pool_manager_->UnpinPage(page_id, true));
      }
        temp_page->WUnlatch();
//      lock_with_set(page_latch_set, temp_page, false, true);
      insert_flag = true;
      break;
    }
    // possible place to insert; if encounter the first tombstone
    if (!block_page->IsReadable(offset) && insert_page_id == INVALID_PAGE_ID) {
      //        // LOG_DEBUG("Found tombstones...\n");
      insert_latch_page = temp_page;
      insert_page_id = page_id;
      insert_page = block_page;
      insert_offset = offset;
    }
    // if duplicates detected, abort
    if (block_page->IsReadable(offset) && comparator_(block_page->KeyAt(offset), key) == 0 &&
        block_page->ValueAt(offset) == value) {
      assert(buffer_pool_manager_->UnpinPage(page_id, false));
      temp_page->WUnlatch();
      if (insert_page_id != INVALID_PAGE_ID && insert_page_id != page_id) {
        assert(buffer_pool_manager_->UnpinPage(insert_page_id, false));
        insert_latch_page->WUnlatch();
      }
      // LOG_DEBUG("Duplicated!\n");
      break;
    }
    // linear probe
    bucket_id = (bucket_id + 1) % num_buckets_;
    // if wrapped around, need to resize and reset local variables
    // edge case was handled above: wrap around but tombstones found
    if (bucket_id == start_id && insert_page_id == INVALID_PAGE_ID) {
      // need to unpin things here before resize to precent mem leak
      // unlock all latches
//      // LOG_DEBUG("Resize Unlocking...\n");
      assert(buffer_pool_manager_->UnpinPage(header_page_id_, false));
      assert(buffer_pool_manager_->UnpinPage(page_id, false));
      header_page_p->RUnlatch();
      temp_page->WUnlatch();
//      lock_with_set(page_latch_set, header_page_p, false, false);
//      lock_with_set(page_latch_set, temp_page, false, true);
      table_latch_.RUnlock();

//      // LOG_DEBUG("Resize locking...\n");
//       LOG_DEBUG("Lock\n");
      table_latch_.WLock();
//       LOG_DEBUG("Locked\n");
      LOG_DEBUG("Start resizing: %d\n", (int)num_buckets_);
      Resize(num_buckets_);
      LOG_DEBUG("Finished resizing: %d\n", (int)num_buckets_);
      table_latch_.WUnlock();
      // LOG_DEBUG("Lock\n");
      table_latch_.RLock();
      // LOG_DEBUG("Locked\n");
      bucket_id = hash_fn_.GetHash(key) % num_buckets_;
      start_id = bucket_id;
      header_page_p = buffer_pool_manager_->FetchPage(header_page_id_);
      assert(header_page_p);
      header_page_p->RLatch();
//      lock_with_set(page_latch_set, header_page_p, true, false);
      header_page = reinterpret_cast<HashTableHeaderPage *>(header_page_p->GetData());
      page_id = header_page->GetBlockPageId(bucket_id / BLOCK_ARRAY_SIZE);
      switch_page = true;
      temp_page = nullptr;
    } else if (bucket_id % BLOCK_ARRAY_SIZE == 0 && num_block_pages_ > 1) {
      // no resize need, if need to check a new page
      switch_page = true;
      // unpin page if no possible insertion
      // ulatch is handled above
      if (page_id != insert_page_id) {
        assert(buffer_pool_manager_->UnpinPage(page_id, false));
      }
      assert((size_t)bucket_id / BLOCK_ARRAY_SIZE < header_page->NumBlocks());
      page_id = header_page->GetBlockPageId(bucket_id / BLOCK_ARRAY_SIZE);
    }
  }
  // unpin page_id is handled above
  assert(buffer_pool_manager_->UnpinPage(header_page_id_, false));
  header_page_p->RUnlatch();
//  lock_with_set(page_latch_set, header_page_p, false, false);
  table_latch_.RUnlock();
  // LOG_DEBUG("Finished Insert.. \n");
  return insert_flag;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.RLock();
  Page *header_page_p = buffer_pool_manager_->FetchPage(header_page_id_);
  assert(header_page_p);
  header_page_p->RLatch();
  auto header_page = reinterpret_cast<HashTableHeaderPage *>(header_page_p->GetData());

  uint64_t bucket_id = hash_fn_.GetHash(key) % num_buckets_;
  uint64_t start_id = bucket_id;
  // where to start linear probing
  page_id_t page_id = header_page->GetBlockPageId(bucket_id / BLOCK_ARRAY_SIZE);
  // if need to read a new page or check the next page (or wrap around)
  bool switch_page = true;

  slot_offset_t offset(0);
  BLOCK_PAGE_TYPE *block_page(nullptr);
  bool remove_flag = false;
  bool page_dirty_flag = false;
  Page *temp_page(nullptr);
  Page *next_latch_page(nullptr);

  while (true) {
    // fetch block page
    if (switch_page) {
      //        // LOG_DEBUG("Page Switched!\n");
      if (temp_page != nullptr) {
        next_latch_page = buffer_pool_manager_->FetchPage(page_id);
        assert(next_latch_page);
//        LOG_DEBUG("Lock\n");
        temp_page->WUnlatch();
        next_latch_page->WLatch();
//        LOG_DEBUG("Locked\n");
        temp_page = next_latch_page;
      } else {
        temp_page = buffer_pool_manager_->FetchPage(page_id);
        assert(temp_page);
//        LOG_DEBUG("Lock\n");
        temp_page->WLatch();
//        LOG_DEBUG("Locked\n");
      }
      block_page = reinterpret_cast<BLOCK_PAGE_TYPE *>(temp_page->GetData());
      switch_page = false;
    }
    // block_page slot
    offset = bucket_id % BLOCK_ARRAY_SIZE;
    //      // LOG_DEBUG("Probed: %d!\n", (int) offset);

    if (!block_page->IsOccupied(offset)) {
      break;
    }
    // if match, remove and mark page dirty
    if (block_page->IsReadable(offset) && comparator_(block_page->KeyAt(offset), key) == 0 &&
        block_page->ValueAt(offset) == value) {
      remove_flag = true;
      block_page->Remove(offset);
      page_dirty_flag = true;
    }
    // linear probe
    bucket_id = (bucket_id + 1) % num_buckets_;
    // if wrapped around
    if (bucket_id == start_id) {
      break;
    }
    if (bucket_id % BLOCK_ARRAY_SIZE == 0 && num_block_pages_ > 1) {
      switch_page = true;
      // need to unpin page based on whether page is dirty
      assert(buffer_pool_manager_->UnpinPage(page_id, page_dirty_flag));
      page_dirty_flag = false;
      assert((size_t)bucket_id / BLOCK_ARRAY_SIZE < header_page->NumBlocks());
      page_id = header_page->GetBlockPageId(bucket_id / BLOCK_ARRAY_SIZE);
    }
  }
  assert(buffer_pool_manager_->UnpinPage(page_id, page_dirty_flag));
  assert(buffer_pool_manager_->UnpinPage(header_page_id_, false));
  temp_page->WUnlatch();
  header_page_p->RUnlatch();
  table_latch_.RUnlock();
  return remove_flag;
}

/*****************************************************************************
 * RESIZE
 *****************************************************************************/
// need to allocate new header and block pages and move contents to new pages
// update header meta data at the very end
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::Resize(size_t initial_size) {
  page_id_t allocate_temp_p;
  page_id_t new_header_page;
  page_id_t old_page_id;
  uint64_t bucket_id(0);
  // for moving contents
  BLOCK_PAGE_TYPE *block_page(nullptr);
  BLOCK_PAGE_TYPE *new_block_page(nullptr);

  size_t new_num_blocks = (2 * initial_size + BLOCK_ARRAY_SIZE - 1) / BLOCK_ARRAY_SIZE;
  if (new_num_blocks > MAX_NUM_BLOCK_PAGES) {
    LOG_ERROR("Exceed Limit of Number of Blocks!\n");
    return;
  }
  size_t new_size = new_num_blocks * BLOCK_ARRAY_SIZE;

  new_header_page = allocate_temp_p = INVALID_PAGE_ID;
  auto prev_header_page =
      reinterpret_cast<HashTableHeaderPage *>(buffer_pool_manager_->FetchPage(header_page_id_)->GetData());
  assert(prev_header_page && "get header!");
  // allocate new pages
  auto header_page =
      reinterpret_cast<HashTableHeaderPage *>(buffer_pool_manager_->NewPage(&new_header_page)->GetData());
//  buffer_pool_manager_->FlushPage(new_header_page);
  header_page->SetSize(new_size);
  header_page->SetPageId(new_header_page);

//  auto block_pages = new BLOCK_PAGE_TYPE *[new_num_blocks];
  std::vector<page_id_t> block_pages;
  block_pages.reserve(new_num_blocks);

  for (size_t i = 0; i < new_num_blocks; ++i) {
    auto tmp_block_page = reinterpret_cast<BLOCK_PAGE_TYPE *>(buffer_pool_manager_->NewPage(&allocate_temp_p, nullptr));
    assert(tmp_block_page && "new page!");
    block_pages.push_back(allocate_temp_p);
//    assert(buffer_pool_manager_->FlushPage(allocate_temp_p));
    assert(buffer_pool_manager_->UnpinPage(allocate_temp_p, true));
    header_page->AddBlockPageId(allocate_temp_p);
  }

  // move content: need to do linear probing...
  slot_offset_t offset(0);
  for (size_t i = 0; i < num_block_pages_; ++i) {
    old_page_id = prev_header_page->GetBlockPageId(i);
    block_page = reinterpret_cast<BLOCK_PAGE_TYPE *>(buffer_pool_manager_->FetchPage(old_page_id));
    assert(block_page && "Block fetched!");
    // linear probing again
    //      // LOG_DEBUG("Block: %d\n", (int) sizeof(*block_page));
    for (size_t j = 0; j < BLOCK_ARRAY_SIZE; ++j) {
      if (!block_page->IsReadable(j)) {
        continue;
      }
      auto k_t = block_page->KeyAt(j);
      auto v_t = block_page->ValueAt(j);
      // where it should be in the new table
      bucket_id = hash_fn_.GetHash(k_t) % new_size;

      page_id_t tmp_page_id(INVALID_PAGE_ID);
      while (true) {
        if (tmp_page_id != INVALID_PAGE_ID && tmp_page_id != block_pages[bucket_id / BLOCK_ARRAY_SIZE]) {
          assert(buffer_pool_manager_->UnpinPage(tmp_page_id, false));
        }
        tmp_page_id = block_pages[bucket_id / BLOCK_ARRAY_SIZE];
        new_block_page = reinterpret_cast<BLOCK_PAGE_TYPE *>(buffer_pool_manager_->FetchPage
            (tmp_page_id)->GetData());
        assert(new_block_page && "Fetch new blocks");
        offset = bucket_id % BLOCK_ARRAY_SIZE;
        //          // LOG_DEBUG("Bucket: %d\n", (int) bucket_id);
        if (!new_block_page->IsOccupied(offset)) {
          assert(new_block_page->Insert(offset, k_t, v_t) && "Inserted!");
          assert(buffer_pool_manager_->UnpinPage(tmp_page_id, true));
          break;
        }
        bucket_id = (bucket_id + 1) % new_size;
      }
    }
    // delete block page
    assert(buffer_pool_manager_->UnpinPage(old_page_id, false));
//    assert(buffer_pool_manager_->DeletePage(old_page_id) && "delete page!");
    // this is evil....
    while (!buffer_pool_manager_->DeletePage(old_page_id)) {
      buffer_pool_manager_->UnpinPage(old_page_id, false);
    }
  }
  // cleanup: delete old header and reset
  header_page_id_ = new_header_page;
  num_buckets_ = new_size;
  num_block_pages_ = new_num_blocks;
  assert(buffer_pool_manager_->UnpinPage(prev_header_page->GetPageId(), false));
  assert(buffer_pool_manager_->DeletePage(prev_header_page->GetPageId()) && "delete header!");
  assert(buffer_pool_manager_->UnpinPage(header_page->GetPageId(), true));
}

/*****************************************************************************
 * GETSIZE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
size_t HASH_TABLE_TYPE::GetSize() {
  return num_buckets_;
}

// Explicit Template Instantiation
template class LinearProbeHashTable<hash_t, TmpTuple, HashComparator>;

template class LinearProbeHashTable<int, int, IntComparator>;

template class LinearProbeHashTable<GenericKey<4>, RID, GenericComparator<4>>;

template class LinearProbeHashTable<GenericKey<8>, RID, GenericComparator<8>>;

template class LinearProbeHashTable<GenericKey<16>, RID, GenericComparator<16>>;

template class LinearProbeHashTable<GenericKey<32>, RID, GenericComparator<32>>;

template class LinearProbeHashTable<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
