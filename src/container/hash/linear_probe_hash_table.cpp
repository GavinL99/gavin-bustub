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
#define MAX_NUM_BLOCK_PAGES 1020

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
    Page *temp_page = buffer_pool_manager_->FetchPage(header_page_id_);
    if (!temp_page) {
      return false;
    }
    auto header_page = reinterpret_cast<HashTableHeaderPage *>(temp_page->GetData());
    uint64_t bucket_id = hash_fn_.GetHash(key) % num_buckets_;
    int start_id = bucket_id;
    // where to start linear probing
    page_id_t page_id = header_page->GetBlockPageId(bucket_id / BLOCK_ARRAY_SIZE);
    // if need to read a new page or check the next page (or wrap around)
    bool switch_page = true;

    slot_offset_t offset;
    BLOCK_PAGE_TYPE *block_page(nullptr);

    while (true) {
      // if wrapped around
      if (bucket_id == start_id + num_buckets_) {
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
      bucket_id++;
      if (bucket_id % BLOCK_ARRAY_SIZE == 0) {
        switch_page = true;
        // need to unpin page
        buffer_pool_manager_->UnpinPage(page_id, false);
        page_id = header_page->GetBlockPageId(bucket_id / BLOCK_ARRAY_SIZE);
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
    slot_offset_t offset(0), insert_offset(0);
    BLOCK_PAGE_TYPE *block_page(nullptr), *insert_page(nullptr);

    while (true) {
      // if wrapped around, need to resize
      if (bucket_id == start_id + num_buckets_) {
        LOG_DEBUG("Insert debug...\n");
        Resize(num_buckets_);
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
      offset = bucket_id % BLOCK_ARRAY_SIZE;

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
        LOG_DEBUG("Duplicated!\n");
        break;
      }
      // linear probe
      bucket_id++;
      if (bucket_id % BLOCK_ARRAY_SIZE == 0) {
        switch_page = true;
        // unpin page if no possible insertion
        if (page_id != insert_page_id) {
          buffer_pool_manager_->UnpinPage(page_id, false);
        }
        page_id = header_page->GetBlockPageId(bucket_id / BLOCK_ARRAY_SIZE);
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
      // if wrapped around
//      LOG_DEBUG("Remove Id: %d\n", (int) bucket_id);
      if (bucket_id == start_id + num_buckets_) {
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
      bucket_id++;
      if (bucket_id % BLOCK_ARRAY_SIZE == 0) {
        switch_page = true;
        // need to unpin page based on whether page is dirty
        buffer_pool_manager_->UnpinPage(page_id, page_dirty_flag);
        page_dirty_flag = false;
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
    page_id_t temp_p, new_header_page, old_page_id;
    uint64_t bucket_id(0);
    // for moving contents
    BLOCK_PAGE_TYPE *block_page(nullptr), *new_block_page(nullptr);

    int new_num_blocks = (size_t) (2 * initial_size + BLOCK_ARRAY_SIZE - 1) / BLOCK_ARRAY_SIZE;
    if (new_num_blocks > MAX_NUM_BLOCK_PAGES) {
      LOG_ERROR("Exceed Limit of Number of Blocks!\n");
      return;
    }
    size_t new_size = new_num_blocks * BLOCK_ARRAY_SIZE;

    new_header_page = temp_p = INVALID_PAGE_ID;
    auto prev_header_page = reinterpret_cast<HashTableHeaderPage *>(
        buffer_pool_manager_->FetchPage(header_page_id_)->GetData());
    // allocate new pages
    auto header_page = reinterpret_cast<HashTableHeaderPage *>(buffer_pool_manager_->NewPage(
        &new_header_page)->GetData());
    header_page->SetSize(new_size);
    header_page->SetPageId(new_header_page);
    for (int i = 0; i < (int) new_num_blocks; ++i) {
      buffer_pool_manager_->NewPage(&temp_p, nullptr);
      header_page->AddBlockPageId(temp_p);
    }

    // move content
    for (int i = 0; i < (int) num_block_pages_; ++i) {
      old_page_id = prev_header_page->GetBlockPageId(i);
      block_page = reinterpret_cast<BLOCK_PAGE_TYPE *>(buffer_pool_manager_->FetchPage
          (old_page_id));
      for (int j = 0; j < (int) BLOCK_ARRAY_SIZE; ++j) {
        // where it should be in the new table
        bucket_id = hash_fn_.GetHash(block_page->KeyAt(j)) % new_size;
        // if need to fetch a new content page
        if (temp_p == INVALID_PAGE_ID || bucket_id / BLOCK_ARRAY_SIZE != (uint64_t) temp_p) {
          temp_p = bucket_id / BLOCK_ARRAY_SIZE;
          new_block_page = reinterpret_cast<BLOCK_PAGE_TYPE *>(
              buffer_pool_manager_->FetchPage(header_page->GetBlockPageId(temp_p))
          );
        }
        if (block_page->IsReadable(j)) {
          new_block_page->Insert(bucket_id % BLOCK_ARRAY_SIZE,
                                 block_page->KeyAt(j), block_page->ValueAt(j));
        }
      }
      // delete block page
      buffer_pool_manager_->UnpinPage(old_page_id, false);
      buffer_pool_manager_->DeletePage(old_page_id);
    }
    // cleanup: delete old header and reset
    header_page_id_ = new_header_page;
    num_buckets_ = new_size;
    num_block_pages_ = new_num_blocks;
    buffer_pool_manager_->UnpinPage(prev_header_page->GetPageId(), false);
    buffer_pool_manager_->DeletePage(prev_header_page->GetPageId());
    for (int j = 0; j < new_num_blocks; ++j) {
      buffer_pool_manager_->UnpinPage(header_page->GetBlockPageId(j), true);
    }
    buffer_pool_manager_->UnpinPage(header_page->GetPageId(), true);
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
