//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include <list>
#include <unordered_map>

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new ClockReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() {
  delete[] pages_;
  delete replacer_;
}

Page *BufferPoolManager::FetchPageImpl(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  frame_id_t frame_idx = -1;
  Page *temp_page = nullptr;

  // for modifying global data structure
  latch_.lock();
  if (page_table_.find(page_id) != page_table_.end()) {
    frame_idx = page_table_[page_id];
    // pin first and then modify ref count
    // s.t. this page will not be victimized
    temp_page = pages_ + frame_idx;
    replacer_->Pin(frame_idx);
    // get write lock to update pin
    temp_page->WLatch();
    latch_.unlock();
    IncremPin(temp_page);
    temp_page->WUnlatch();

  } else if (free_list_.size() > 0) {
    frame_idx = free_list_.front();
    free_list_.pop_front();
    page_table_[page_id] = frame_idx;
    temp_page = pages_ + frame_idx;
    replacer_->Pin(frame_idx);
    temp_page->WLatch();
    latch_.unlock();
    // reset memory and update meta data
    ResetPage(temp_page, page_id);
    disk_manager_->ReadPage(page_id, temp_page->data_);
    temp_page->WUnlatch();

  } else {
    // if nothing to evict
    if (!replacer_->Victim(&frame_idx)) {
      latch_.unlock();
      return nullptr;
    }
    frame_id_t old_page_id = temp_page->GetPageId();
    temp_page = pages_ + frame_idx;
    page_table_.erase(old_page_id);
    page_table_[page_id] = frame_idx;
    temp_page->WLatch();
    latch_.unlock();
    if (temp_page->IsDirty()) {
      disk_manager_->WritePage(old_page_id, temp_page->data_);
    }
    ResetPage(temp_page, page_id);
    disk_manager_->ReadPage(page_id, temp_page->data_);
    temp_page->WUnlatch();
  }
  return temp_page;
}


void BufferPoolManager::ResetPage(Page *page, page_id_t new_page_id) {
  page->ResetMemory();
  page->page_id_ = new_page_id;
  page->is_dirty_ = false;
  page->pin_count_ = (new_page_id == INVALID_PAGE_ID)? 0: 1;
}

bool BufferPoolManager::UnpinPageImpl(page_id_t page_id, bool is_dirty) {
  frame_id_t frame_idx = -1;
  Page *temp_page = nullptr;

  latch_.lock();
  if (page_table_.find(page_id) != page_table_.end()) {
    frame_idx = page_table_[page_id];
    temp_page = pages_ + frame_idx;
    temp_page->WLatch();
    if (temp_page->GetPinCount() == 0) {
      latch_.unlock();
      return false;
    }
    DecremPin(temp_page);
    if (temp_page->GetPinCount() == 0)
      replacer_->Unpin(frame_idx);
    latch_.unlock();
    temp_page->is_dirty_ &= is_dirty;
  } else {
    latch_.unlock();
  }
  return true;
}

bool BufferPoolManager::FlushPageImpl(page_id_t page_id) {
  // Make sure you call DiskManager::WritePage!
  frame_id_t frame_idx = -1;
  Page *temp_page = nullptr;
  latch_.lock();
  if (page_table_.find(page_id) != page_table_.end()) {
    // update page_table, free_list
    frame_idx = page_table_[page_id];
    temp_page = pages_ + frame_idx;
    free_list_.push_back(frame_idx);
    page_table_.erase(page_id);
    replacer_->Pin(frame_idx);
    temp_page->WLatch();
    latch_.unlock();
    if (temp_page->IsDirty()) {
      disk_manager_->WritePage(page_id, temp_page->data_);
    }
    ResetPage(temp_page, INVALID_PAGE_ID);
    return true;
  } else {
    latch_.unlock();
    return false;
  }
  
}

Page *BufferPoolManager::NewPageImpl(page_id_t *page_id) {
  // 0.   Make sure you call DiskManager::AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
  return nullptr;
}

bool BufferPoolManager::DeletePageImpl(page_id_t page_id) {
  // 0.   Make sure you call DiskManager::DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  return false;
}

void BufferPoolManager::FlushAllPagesImpl() {
  // You can do it!
}

}  // namespace bustub
