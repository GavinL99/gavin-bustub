//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// log_manager.h
//
// Identification: src/include/recovery/log_manager.h
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <algorithm>
#include <condition_variable>  // NOLINT
#include <future>              // NOLINT
#include <mutex>               // NOLINT

#include "recovery/log_record.h"
#include "storage/disk/disk_manager.h"

namespace bustub {

/**
 * LogManager maintains a separate thread that is awakened whenever the log buffer is full or whenever a timeout
 * happens. When the thread is awakened, the log buffer's content is written into the disk log file.
 */
class LogManager {
 public:
  explicit LogManager(DiskManager *disk_manager)
      : next_lsn_(0), persistent_lsn_(INVALID_LSN), disk_manager_(disk_manager), buffer_used_(0), total_log_sz(0) {
    log_buffer_ = new char[LOG_BUFFER_SIZE];
    flush_buffer_ = new char[LOG_BUFFER_SIZE];
  }

  ~LogManager() {
    delete[] log_buffer_;
    delete[] flush_buffer_;
    log_buffer_ = nullptr;
    flush_buffer_ = nullptr;
  }


  void RunFlushThread();
  void StopFlushThread();
  // this is called by bpm if need to evict a dirty page
  void TriggerFlush(page_id_t);

  lsn_t AppendLogRecord(LogRecord *log_record);

  inline lsn_t GetNextLSN() { return next_lsn_; }
  inline lsn_t GetPersistentLSN() { return persistent_lsn_; }
  inline void SetPersistentLSN(lsn_t lsn) { persistent_lsn_ = lsn; }
  inline char *GetLogBuffer() { return log_buffer_; }

  inline int GetTotalSize() { return total_log_sz; }

  static void SerializeLog(char*, LogRecord*);

 private:
  using uniq_lock = std::unique_lock<std::mutex>;

  // TODO(students): you may add your own member variables

  // irrelavant to timeout; only set to ready if:
  // buffer is full or dirty page is evicted
//  volatile bool flush_trigger_;

  // helper function for async flush, will wait on flush_cv_
  // if time out or if woke up by other threads
  void flush_helper();

  /** The atomic counter which records the next log sequence number. */
  std::atomic<lsn_t> next_lsn_;
  /** The log records before and including the persistent lsn have been written to disk. */
  std::atomic<lsn_t> persistent_lsn_;

  // need to swap when flushing
  char *log_buffer_;
  char *flush_buffer_;

  // global latch for log manager
  std::mutex latch_;

  std::thread *flush_thread_ __attribute__((__unused__));

  std::condition_variable flush_thread_cv_;

  bool just_swapped = false;

  DiskManager *disk_manager_ __attribute__((__unused__));

  // size used
  std::atomic<int> buffer_used_;
  int flush_sz_;
  // total log size
  std::atomic<int> total_log_sz;

  };

}  // namespace bustub
