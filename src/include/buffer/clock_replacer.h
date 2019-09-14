//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// clock_replacer.h
//
// Identification: src/include/buffer/clock_replacer.h
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <list>
#include <mutex>  // NOLINT
#include <vector>

#include "buffer/replacer.h"
#include "common/config.h"

namespace bustub {

/**
 * ClockReplacer implements the clock replacement policy, which approximates the Least Recently Used policy.
 */
class ClockReplacer : public Replacer {
 public:
  /**
   * Create a new ClockReplacer.
   * @param num_pages the maximum number of pages the ClockReplacer will be required to store
   */
  explicit ClockReplacer(size_t num_pages);

  /**
   * Destroys the ClockReplacer.
   */
  ~ClockReplacer() override;

  // need to modify frame_id by policy
  bool Victim(frame_id_t *frame_id) override;

  // decrement size as have fewer slots
  // set replace ref_bit to 0
  // but not really remove (can be unpinned again)
  void Pin(frame_id_t frame_id) override;

  // when unpin, increment size as have more available slots (if exist)
  // set replace ref_bit to 1
  // essentially add a frame to clock replacer
  void Unpin(frame_id_t frame_id) override;

  size_t Size() override;

 private:
  // TODO(student): implement me!
  frame_id_t clock_hand_;
  const frame_id_t capacity_;
  size_t evict_size_;
  // -1: not exist
  // 0: exist with ref bit 0; 1: exist with ref bit 1
  std::vector<int> clock_map_;
  std::mutex latch_;

};

}  // namespace bustub
