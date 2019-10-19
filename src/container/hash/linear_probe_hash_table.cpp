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

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "container/hash/linear_probe_hash_table.h"
#include "common/logger.h"

namespace bustub {

  template<typename KeyType, typename ValueType, typename KeyComparator>
  HASH_TABLE_TYPE::LinearProbeHashTable(const std::string &name, BufferPoolManager *buffer_pool_manager,
                                        const KeyComparator &comparator, size_t num_buckets,
                                        HashFunction<KeyType> hash_fn)
      : buffer_pool_manager_(buffer_pool_manager), comparator_(comparator), hash_fn_(std::move(hash_fn)) {
    // allocate memory for header / block pages based on num_buckets, assume always success
    page_id_t temp_p = INVALID_PAGE_ID;
    num_block_pages_ = (size_t) (num_buckets + BLOCK_ARRAY_SIZE - 1) / BLOCK_ARRAY_SIZE;
    num_buckets_ = num_block_pages_ * BLOCK_ARRAY_SIZE;

    // allocate header page
    header_page_id_ = INVALID_PAGE_ID;
    auto header_page = reinterpret_cast<HashTableHeaderPage *>(buffer_pool_manager_->NewPage(
        &header_page_id_)->GetData());
    header_page->SetSize(num_buckets_);
    header_page->SetPageId(header_page_id_);
    // block pages, need to add all buckets
    for (int i = 0; i < (int) num_block_pages_; ++i) {
      buffer_pool_manager_->NewPage(&temp_p, nullptr);
      for (int j = 0; j < (int) BLOCK_ARRAY_SIZE; j++)
        header_page->AddBlockPageId(temp_p);
    }

  }

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
  template<typename KeyType, typename ValueType, typename KeyComparator>
  bool HASH_TABLE_TYPE::GetValue(Transaction *transaction, const KeyType &key, std::vector<ValueType> *result) {
    // hash f -> page_id -> block page -> linear probe -> stop till not occupied
    // get the actual data
    Page *temp_page = buffer_pool_manager_->FetchPage(header_page_id_);
    if (!temp_page) {
      return false;
    }
    auto header_page = reinterpret_cast<HashTableHeaderPage *>(temp_page->GetData());
    uint64_t block_id = hash_fn_.GetHash(key) % num_buckets_;
    int start_id = block_id;
    // where to start linear probing
    page_id_t page_id = header_page->GetBlockPageId(block_id);
    // if need to read a new page or check the next page (or wrap around)
    bool switch_page = true;

    slot_offset_t offset;
    BLOCK_PAGE_TYPE *block_page(nullptr);

    while (true) {
      // if wrapped around
      if (block_id == start_id + num_buckets_) {
        break;
      }
      // fetch block page
      if (switch_page) {
        temp_page = buffer_pool_manager_->FetchPage(page_id);
        if (!temp_page) {
          break;
        }
        block_page = reinterpret_cast<BLOCK_PAGE_TYPE *>(temp_page->GetData());
        switch_page = false;
      }
      // block_page slot
      offset = block_id % BLOCK_ARRAY_SIZE;
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
      block_id++;
      if (block_id % BLOCK_ARRAY_SIZE == 0) {
        switch_page = true;
        // need to unpin page
        buffer_pool_manager_->UnpinPage(page_id, false);
        // page_id = header_page->GetBlockPageId(block_id);
        // assume just try next page
        page_id++;
      }
    }
    buffer_pool_manager_->UnpinPage(page_id, false);
    buffer_pool_manager_->UnpinPage(header_page_id_, false);
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
    if (!temp_page) {
      return false;
    }
    auto header_page = reinterpret_cast<HashTableHeaderPage *>(temp_page->GetData());
    uint64_t block_id = hash_fn_.GetHash(key) % num_buckets_;
    uint64_t start_id = block_id;
    // where to start linear probing
    page_id_t page_id = header_page->GetBlockPageId(block_id);
    // if need to read a new page or check the next page (or wrap around)
    bool switch_page = true;
    // if need to insert at some tombstones
    bool insert_flag = false;

    // for tombstones insertion
    page_id_t insert_page_id = INVALID_PAGE_ID;
    slot_offset_t offset(0), insert_offset(0);
    BLOCK_PAGE_TYPE *block_page(nullptr), *insert_page(nullptr);

    while (true) {
      // if wrapped around
      if (block_id == start_id + num_buckets_) {
        break;
      }
      // fetch block page
      if (switch_page) {
        temp_page = buffer_pool_manager_->FetchPage(page_id);
        if (!temp_page) {
          break;
        }
        block_page = reinterpret_cast<BLOCK_PAGE_TYPE *>(temp_page->GetData());
        switch_page = false;
      }
      // block_page slot
      offset = block_id % BLOCK_ARRAY_SIZE;

      // if vacant
      if (!block_page->IsOccupied(offset)) {
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
        break;
      }
      // linear probe
      block_id++;
      if (block_id % BLOCK_ARRAY_SIZE == 0) {
        switch_page = true;
        // unpin page if no possible insertion
        if (page_id != insert_page_id) {
          buffer_pool_manager_->UnpinPage(page_id, false);
        }
        // page_id = header_page->GetBlockPageId(block_id);
        page_id++;
      }
    }
    // unpin page_id is handled above
    buffer_pool_manager_->UnpinPage(header_page_id_, false);
    return insert_flag;
  }

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
  template<typename KeyType, typename ValueType, typename KeyComparator>
  bool HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) {
    Page *temp_page = buffer_pool_manager_->FetchPage(header_page_id_);
    if (!temp_page) {
      return false;
    }
    auto header_page = reinterpret_cast<HashTableHeaderPage *>(temp_page->GetData());
    uint64_t block_id = hash_fn_.GetHash(key) % num_buckets_;
    uint64_t start_id = block_id;
    // where to start linear probing
    page_id_t page_id = header_page->GetBlockPageId(block_id);
    // if need to read a new page or check the next page (or wrap around)
    bool switch_page = true;

    slot_offset_t offset(0);
    BLOCK_PAGE_TYPE *block_page(nullptr);
    bool remove_flag = false;
    bool page_dirty_flag = false;

    while (true) {
      // if wrapped around
      if (block_id == start_id + num_buckets_) {
        break;
      }
      // fetch block page
      if (switch_page) {
//        LOG_DEBUG("Page Switched!\n");
        temp_page = buffer_pool_manager_->FetchPage(page_id);
        if (!temp_page) {
          break;
        }
        block_page = reinterpret_cast<BLOCK_PAGE_TYPE *>(temp_page->GetData());
        switch_page = false;
      }
      // block_page slot
      offset = block_id % BLOCK_ARRAY_SIZE;
//      LOG_DEBUG("Probed: %d!\n", (int) offset);

      if (!block_page->IsOccupied(offset)) {
        break;
      }
      // if match, remove and mark page dirty
      if (block_page->IsReadable(offset) && comparator_(block_page->KeyAt(offset), key) == 0 && block_page->ValueAt
          (offset) == value) {
        remove_flag = true;
        block_page->Remove(offset);
        page_dirty_flag = true;
      }
      // linear probe
      block_id++;
      if (block_id % BLOCK_ARRAY_SIZE == 0) {
        switch_page = true;
        // need to unpin page based on whether page is dirty
        buffer_pool_manager_->UnpinPage(page_id, page_dirty_flag);
        page_dirty_flag = false;
        // page_id = header_page->GetBlockPageId(block_id);
        page_id++;
      }
    }
    buffer_pool_manager_->UnpinPage(page_id, page_dirty_flag);
    buffer_pool_manager_->UnpinPage(header_page_id_, false);
    return remove_flag;
  }

/*****************************************************************************
 * RESIZE
 *****************************************************************************/
  template<typename KeyType, typename ValueType, typename KeyComparator>
  void HASH_TABLE_TYPE::Resize(size_t initial_size) {


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
