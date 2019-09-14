//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// clock_replacer.cpp
//
// Identification: src/buffer/clock_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/clock_replacer.h"

namespace bustub {

ClockReplacer::ClockReplacer(size_t num_pages) : capacity_((frame_id_t) num_pages), clock_map_(num_pages, -1) {
  clock_hand_ = 0;
  evict_size_ = 0;
}

ClockReplacer::~ClockReplacer() = default;

bool ClockReplacer::Victim(frame_id_t *frame_id) { 
  latch_.lock();
  frame_id_t prev_hand = clock_hand_;
  // possible when have evict_size_ = 0 but can evict
  // when there are only 1 and -1
  bool any_to_evict = false;

  while (clock_map_[clock_hand_] != 0) {
    if (clock_map_[clock_hand_] == 1) {
      any_to_evict = true;
      evict_size_--;
      clock_map_[clock_hand_] = 0;
    }
    clock_hand_ = (clock_hand_ + 1) % capacity_;
    // traverse the clock but nothing to evict
    if (clock_hand_ == prev_hand && !any_to_evict) {
      frame_id = nullptr;
      latch_.unlock();
      return false;
    }
  }
  *frame_id = clock_hand_;
  clock_map_[clock_hand_] = -1;
  latch_.unlock();
  return true;
}

// call pin when the page is pinned by threads
// if exist, need to mark it as "non exist"
// but not really, only page table knows
void ClockReplacer::Pin(frame_id_t frame_id) {
  if (frame_id < 0 || frame_id >= capacity_) 
    return;
  latch_.lock();
  if (clock_map_[frame_id] >= 0) {
    if (clock_map_[frame_id] == 1) 
      evict_size_--;
    clock_map_[frame_id] = -1;
  }
  latch_.unlock();
}

// if not exist or ref_bit = 0, increment size and ref bit
void ClockReplacer::Unpin(frame_id_t frame_id) {
  if (frame_id < 0 || frame_id >= capacity_) 
    return;
  latch_.lock();
  if (clock_map_[frame_id] <= 0) {
    evict_size_++;
    clock_map_[frame_id] = 1;
  }
  latch_.unlock();
}

size_t ClockReplacer::Size() { return evict_size_; }

}  // namespace bustub
