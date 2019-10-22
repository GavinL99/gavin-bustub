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

#include <iostream>
#include <string>
#include <utility>
#include <vector>
#include "common/logger.h"
#include "common/rid.h"
#include "container/hash/linear_probe_hash_table.h"
#include "cassert"

namespace bustub {
#define MAX_NUM_BLOCK_PAGES 1020

  template<typename KeyType, typename ValueType, typename KeyComparator>
  HASH_TABLE_TYPE::LinearProbeHashTable(const std::string &name, BufferPoolManager *buffer_pool_manager,
                                        const KeyComparator &comparator, size_t num_buckets,
                                        HashFunction<KeyType> hash_fn)
      : buffer_pool_manager_(buffer_pool_manager), comparator_(comparator), hash_fn_(std::move(hash_fn)) {
    // allocate memory for header / block pages based on num_buckets, assume always success
    page_id_t temp_p = INVALID_PAGE_ID;
    num_block_pages_ =  (num_buckets + BLOCK_ARRAY_SIZE - 1) / BLOCK_ARRAY_SIZE;
    num_buckets_ = num_block_pages_ * BLOCK_ARRAY_SIZE;

    // allocate header page
    header_page_id_ = INVALID_PAGE_ID;
    auto header_page = reinterpret_cast<HashTableHeaderPage *>(buffer_pool_manager_->NewPage(
        &header_page_id_)->GetData());
    header_page->SetSize(num_buckets_);
    header_page->SetPageId(header_page_id_);
    // block pages, need to add all buckets
    for (size_t i = 0; i < num_block_pages_; ++i) {
      buffer_pool_manager_->NewPage(&temp_p, nullptr);
      header_page->AddBlockPageId(temp_p);
      // need to unpin after allocation
      buffer_pool_manager_->UnpinPage(temp_p, false);
    }
    buffer_pool_manager_->UnpinPage(header_page_id_, true);
  }

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
  template<typename KeyType, typename ValueType, typename KeyComparator>
  bool HASH_TABLE_TYPE::GetValue(Transaction *transaction, const KeyType &key, std::vector<ValueType> *result) {
    // hash f -> page_id -> block page -> linear probe -> stop till not occupied
    // get the actual data
    table_latch_.RLock();
    Page *temp_page = buffer_pool_manager_->FetchPage(header_page_id_);
    if (temp_page == nullptr) {
      return false;
    }
    temp_page->RLatch();
    auto header_page = reinterpret_cast<HashTableHeaderPage *>(temp_page->GetData());
    uint64_t bucket_id = hash_fn_.GetHash(key) % num_buckets_;
    uint64_t start_id = bucket_id;
    // where to start linear probing
    page_id_t page_id = header_page->GetBlockPageId(bucket_id / BLOCK_ARRAY_SIZE);
    // if need to read a new page or check the next page (or wrap around)
    bool switch_page = true;

    slot_offset_t offset;
    BLOCK_PAGE_TYPE *block_page(nullptr);

    while (true) {
      // fetch block page
      if (switch_page) {
        temp_page = buffer_pool_manager_->FetchPage(page_id);
        if (temp_page == nullptr) {
          break;
        }
        temp_page->RLatch();
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
      if (block_page->IsReadable(offset) &&
          comparator_(block_page->KeyAt(offset), key) == 0) {
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
        buffer_pool_manager_->UnpinPage(page_id, false);
        temp_page->RUnlatch();
        page_id = header_page->GetBlockPageId(bucket_id / BLOCK_ARRAY_SIZE);
      }
    }
    buffer_pool_manager_->UnpinPage(page_id, false);
    buffer_pool_manager_->UnpinPage(header_page_id_, false);
    temp_page->RUnlatch();
    table_latch_.RUnlock();
    return !result->empty();
  }

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
  template<typename KeyType, typename ValueType, typename KeyComparator>
  bool HASH_TABLE_TYPE::Insert(Transaction *transaction, const KeyType &key, const ValueType &value) {
    // need to traverse until the first vacant slot in case of duplicates
    // but insertion can be tombstones in the middle
    Page *temp_page = buffer_pool_manager_->FetchPage(header_page_id_);
    if (temp_page == nullptr) {
      return false;
    }
    auto header_page = reinterpret_cast<HashTableHeaderPage *>(temp_page->GetData());
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

    while (true) {
      // fetch block page
      if (switch_page) {
        temp_page = buffer_pool_manager_->FetchPage(page_id);
        if (temp_page == nullptr) {
          break;
        }
        block_page = reinterpret_cast<BLOCK_PAGE_TYPE *>(temp_page->GetData());
        switch_page = false;
      }
      // block_page slot
      offset = bucket_id % BLOCK_ARRAY_SIZE;

      // Two cases of success:
      // if vacant or have checked all but no duplicates
      // and there's tombstone to insert
      if (!block_page->IsOccupied(offset) ||
          (bucket_id == start_id && insert_page_id != INVALID_PAGE_ID)) {
        // if insert into tombstones
        if (insert_page_id != INVALID_PAGE_ID) {
          insert_page->Insert(insert_offset, key, value);
          buffer_pool_manager_->UnpinPage(insert_page_id, true);
          // unpin current page on hold
          if (insert_page_id != page_id) {
            buffer_pool_manager_->UnpinPage(page_id, false);
          }
        } else {
          //  if insert here
          block_page->Insert(offset, key, value);
          buffer_pool_manager_->UnpinPage(page_id, true);
        }
        insert_flag = true;
        break;
      }
      // possible place to insert; if encounter the first tombstone
      if (!block_page->IsReadable(offset) && insert_page_id == INVALID_PAGE_ID) {
        insert_page_id = page_id;
        insert_page = block_page;
        insert_offset = offset;
      }
      // if duplicates detected, abort
      if (block_page->IsReadable(offset) &&
          comparator_(block_page->KeyAt(offset), key) == 0 &&
          block_page->ValueAt(offset) == value) {
        buffer_pool_manager_->UnpinPage(page_id, false);
        if (insert_page_id != INVALID_PAGE_ID) {
          buffer_pool_manager_->UnpinPage(insert_page_id, false);
        }
//        LOG_DEBUG("Duplicated!\n");
        break;
      }
      // linear probe
      bucket_id = (bucket_id + 1) % num_buckets_;
      // if wrapped around, need to resize and reset local variables
      // edge case was handled above: wrap around but tombstones found
      if (bucket_id == start_id && insert_page_id == INVALID_PAGE_ID) {
//        LOG_DEBUG("Insert Resize: %d\n", (int) num_buckets_);
        // need to unpin things here before resize to precent mem leak
        buffer_pool_manager_->UnpinPage(header_page_id_, false);
        buffer_pool_manager_->UnpinPage(page_id, false);
        Resize(num_buckets_);
        bucket_id = hash_fn_.GetHash(key) % num_buckets_;
        start_id = bucket_id;
        header_page = reinterpret_cast<HashTableHeaderPage *>(
            buffer_pool_manager_->FetchPage(header_page_id_)->GetData());
        page_id = header_page->GetBlockPageId(bucket_id / BLOCK_ARRAY_SIZE);
        switch_page = true;
      } else if (bucket_id % BLOCK_ARRAY_SIZE == 0 && num_block_pages_ > 1) {
        // if need to check a new page
        switch_page = true;
        // unpin page if no possible insertion
        if (page_id != insert_page_id) {
          buffer_pool_manager_->UnpinPage(page_id, false);
        }
        assert((size_t) bucket_id / BLOCK_ARRAY_SIZE < header_page->NumBlocks());
        page_id = header_page->GetBlockPageId(bucket_id / BLOCK_ARRAY_SIZE);
      }
    }
    // unpin page_id is handled above
    buffer_pool_manager_->UnpinPage(header_page_id_, false);
//    LOG_DEBUG("Unpin page..\n");
    return insert_flag;
  }

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
  template<typename KeyType, typename ValueType, typename KeyComparator>
  bool HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) {
    Page *temp_page = buffer_pool_manager_->FetchPage(header_page_id_);
    if (temp_page == nullptr) {
      return false;
    }
    auto header_page = reinterpret_cast<HashTableHeaderPage *>(temp_page->GetData());
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

    while (true) {
      // fetch block page
      if (switch_page) {
//        LOG_DEBUG("Page Switched!\n");
        temp_page = buffer_pool_manager_->FetchPage(page_id);
        if (temp_page == nullptr) {
          break;
        }
        block_page = reinterpret_cast<BLOCK_PAGE_TYPE *>(temp_page->GetData());
        switch_page = false;
      }
      // block_page slot
      offset = bucket_id % BLOCK_ARRAY_SIZE;
//      LOG_DEBUG("Probed: %d!\n", (int) offset);

      if (!block_page->IsOccupied(offset)) {
        break;
      }
      // if match, remove and mark page dirty
      if (block_page->IsReadable(offset) &&
          comparator_(block_page->KeyAt(offset), key) == 0 &&
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
        buffer_pool_manager_->UnpinPage(page_id, page_dirty_flag);
        page_dirty_flag = false;
        assert((size_t) bucket_id / BLOCK_ARRAY_SIZE < header_page->NumBlocks());
        page_id = header_page->GetBlockPageId(bucket_id / BLOCK_ARRAY_SIZE);
      }
    }
    buffer_pool_manager_->UnpinPage(page_id, page_dirty_flag);
    buffer_pool_manager_->UnpinPage(header_page_id_, false);
    return remove_flag;
  }

/*****************************************************************************
 * RESIZE
 *****************************************************************************/
// need to allocate new header and block pages and move contents to new pages
// update header meta data at the very end
  template<typename KeyType, typename ValueType, typename KeyComparator>
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
    auto prev_header_page = reinterpret_cast<HashTableHeaderPage *>(
        buffer_pool_manager_->FetchPage(header_page_id_)->GetData());
    // allocate new pages
    auto header_page = reinterpret_cast<HashTableHeaderPage *>(
        buffer_pool_manager_->NewPage(&new_header_page)->GetData());
    header_page->SetSize(new_size);
    header_page->SetPageId(new_header_page);

    auto block_pages = new BLOCK_PAGE_TYPE *[new_num_blocks];

    for (size_t i = 0; i < new_num_blocks; ++i) {
      block_pages[i] = reinterpret_cast<BLOCK_PAGE_TYPE *>
      (buffer_pool_manager_->NewPage(&allocate_temp_p,
                                     nullptr));
      assert(block_pages[i] != nullptr);
      header_page->AddBlockPageId(allocate_temp_p);
    }
    KeyType k_t;
    ValueType v_t;

    // move content: need to do linear probing...
    slot_offset_t offset(0);
    for (size_t i = 0; i < num_block_pages_; ++i) {
      old_page_id = prev_header_page->GetBlockPageId(i);
      block_page = reinterpret_cast<BLOCK_PAGE_TYPE *>(
          buffer_pool_manager_->FetchPage(old_page_id));
//      LOG_DEBUG("Start Block: %d\n", (int) i);
      // linear probing again
      for (size_t j = 0; j < BLOCK_ARRAY_SIZE; ++j) {
        if (!block_page->IsReadable(j)) {
          continue;
        }
        k_t = block_page->KeyAt(j);
        v_t = block_page->ValueAt(j);
//        LOG_DEBUG("Start Block: %d, %d\n", i, j);
        // where it should be in the new table
        bucket_id = hash_fn_.GetHash(k_t) % new_size;

        while (true) {
          new_block_page = block_pages[bucket_id / BLOCK_ARRAY_SIZE];
          offset = bucket_id % BLOCK_ARRAY_SIZE;
//          LOG_DEBUG("Bucket: %d\n", (int) bucket_id);
          if (!new_block_page->IsOccupied(offset)) {
            if (new_block_page->Insert(
                offset, k_t, v_t)) {
//                  LOG_DEBUG("Block processed: %d, %d\n", i, j);
            }
            break;
          }
          bucket_id = (bucket_id + 1) % new_size;
        }
      }
      // if need to fetch a new content page
      // delete block page
      buffer_pool_manager_->UnpinPage(old_page_id, false);
      if (buffer_pool_manager_->DeletePage(old_page_id)) {
//        LOG_DEBUG("Delete page: %d\n", (int) old_page_id);
      }
//      LOG_DEBUG("Finished block: %d\n", i);
    }
    // cleanup: delete old header and reset
//    LOG_DEBUG("Reset headers...\n");
    header_page_id_ = new_header_page;
    num_buckets_ = new_size;
    num_block_pages_ = new_num_blocks;
    buffer_pool_manager_->UnpinPage(prev_header_page->GetPageId(), false);
    if (buffer_pool_manager_->DeletePage(prev_header_page->GetPageId())) {
//      LOG_DEBUG("Delete old header: %d\n", (int) prev_header_page->GetPageId());
    }
    for (size_t j = 0; j < new_num_blocks; ++j) {
      buffer_pool_manager_->UnpinPage(header_page->GetBlockPageId(j), true);
    }
    buffer_pool_manager_->UnpinPage(header_page->GetPageId(), true);
    delete[] block_pages;
  }

/*****************************************************************************
 * GETSIZE
 *****************************************************************************/
  template<typename KeyType, typename ValueType, typename KeyComparator>
  size_t HASH_TABLE_TYPE::GetSize() {
    return num_buckets_;
  }

  template
  class LinearProbeHashTable<int, int, IntComparator>;

  template
  class LinearProbeHashTable<GenericKey<4>, RID, GenericComparator<4>>;

  template
  class LinearProbeHashTable<GenericKey<8>, RID, GenericComparator<8>>;

  template
  class LinearProbeHashTable<GenericKey<16>, RID, GenericComparator<16>>;

  template
  class LinearProbeHashTable<GenericKey<32>, RID, GenericComparator<32>>;

  template
  class LinearProbeHashTable<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
