//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// log_manager.cpp
//
// Identification: src/recovery/log_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "recovery/log_manager.h"
#include "common/logger.h"
#include "recovery/log_recovery.h"


namespace bustub {
/*
 * set enable_logging = true
 * Start a separate thread to execute flush to disk operation periodically
 * The flush can be triggered when the log buffer is full or buffer pool
 * manager wants to force flush (it only happens when the flushed page has a
 * larger LSN than persistent LSN)
 */
  void LogManager::RunFlushThread() {
    enable_logging = true;
    buffer_used_ = 0;
    flush_thread_ = new std::thread([=] { flush_helper(); });
  }

/**
 * timeout or async call when full (need to swap buffer in append_log)
 */
  void LogManager::flush_helper() {
    while (enable_logging) {
      uniq_lock lock(latch_);
      // auto call unlock: deal with spurious wakeup
      LOG_INFO("Flush helper waiting...\n");
      // if spurious wakeup, block again
      flush_thread_cv_.wait_for(lock, log_timeout, [=] { return !enable_logging; });
      LOG_DEBUG("Flush helper wake up...\n");
      // no need to swap and update buffer_used
      if (just_swapped) {
        LOG_DEBUG("Flush from append async flush\n");
        // async no need to block
        lock.unlock();
        just_swapped = false;
        // no need to set persistent lsn
        assert(flush_sz_ > 0);
        disk_manager_->WriteLog(flush_buffer_, flush_sz_);
        flush_sz_ = 0;
      } else {
        if (buffer_used_ > 0) {
          char *temp = log_buffer_;
          log_buffer_ = flush_buffer_;
          flush_buffer_ = temp;
          persistent_lsn_ = next_lsn_ - 1;
          lock.unlock();
          LOG_DEBUG("Timeout flush..\n");
          disk_manager_->WriteLog(flush_buffer_, buffer_used_);
        } else {
          LOG_INFO("No log to flush...\n");
        }
      }

    }
  }

/*
 * called by bpm when need to evict a dirty page
 * force flush, so can flush first and then swap buffers
 */
  void LogManager::TriggerFlush(page_id_t page_lsn) {
    if (page_lsn <= persistent_lsn_) {
      return;
    }
    // block the whole log manager....
    uniq_lock lock(latch_);
//    LOG_DEBUG("Trigger force flush: %d, %d!\n", (int) page_lsn, (int) persistent_lsn_);
    // if no need to swap
    if (just_swapped) {
//      LOG_INFO("Flush no swap...\n");
      just_swapped = false;
      // no need to set persistent lsn
      assert(flush_sz_ > 0);
      disk_manager_->WriteLog(flush_buffer_, flush_sz_);
      flush_sz_ = 0;
    }
    // if still not fulfilled, swap buffers here and flush again
    if (page_lsn > persistent_lsn_) {
//      LOG_INFO("Flush swap...%d, %d!\n", (int) page_lsn, (int) persistent_lsn_);
      char *temp = log_buffer_;
      log_buffer_ = flush_buffer_;
      flush_buffer_ = temp;
      persistent_lsn_ = next_lsn_ - 1;
      disk_manager_->WriteLog(flush_buffer_, buffer_used_);
//      LOG_INFO("Finish writing...%d, %d!\n", (int) page_lsn, (int) persistent_lsn_);
      buffer_used_ = 0;
      assert(page_lsn <= persistent_lsn_);
    }
  }



/*
 * Stop and join the flush thread, set enable_logging = false
 */
  void LogManager::StopFlushThread() {
    LOG_INFO("Stop flush thread...\n");
    uniq_lock lock(latch_);
    if (enable_logging) {
      enable_logging = false;
      lock.unlock();
      flush_thread_cv_.notify_one();
      flush_thread_->join();
      delete flush_thread_;
    }
  }

/*
 * This is NON blocking
 * append a log record into log buffer
 * swap buffer, if full or evict dirty, notify flushing thread;
 * you MUST set the log record's lsn within this method
 * @return: lsn that is assigned to this log record
 */
  lsn_t LogManager::AppendLogRecord(LogRecord *log_record) {
    //  make sure serializeation
    uniq_lock lock(latch_);
    log_record->lsn_ = next_lsn_++;
    //  if need to swap and async flush
    if (buffer_used_ + log_record->size_ > LOG_BUFFER_SIZE) {
      persistent_lsn_ = next_lsn_ - 2;
      char *temp = log_buffer_;
      log_buffer_ = flush_buffer_;
      flush_buffer_ = temp;
      // make sure not swap twice
      just_swapped = true;
      flush_sz_ = buffer_used_;
      buffer_used_ = 0;
    }
    SerializeLog(log_buffer_ + buffer_used_, log_record);
    assert(log_record->size_ > 0 && log_record->lsn_ != INVALID_LSN);
    buffer_used_ += log_record->size_;
    total_log_sz += log_record->size_;
    if (just_swapped) {
      lock.unlock();
      flush_thread_cv_.notify_one();
    }
    return log_record->lsn_;
  }


  void LogManager::SerializeLog(char *data, LogRecord *log_record) {
//  memcpy(data, &log_record, LogRecord::HEADER_SIZE);
    memcpy(data, &log_record->size_, sizeof(int32_t));
    data += sizeof(int32_t);
    memcpy(data, &log_record->lsn_, sizeof(lsn_t));
    data += sizeof(lsn_t);
    memcpy(data, &log_record->txn_id_, sizeof(txn_id_t));
    data += sizeof(txn_id_t);
    memcpy(data, &log_record->prev_lsn_, sizeof(lsn_t));
    data += sizeof(lsn_t);
    memcpy(data, &log_record->log_record_type_, sizeof(LogRecordType));
    data += sizeof(LogRecordType);
//  data += LogRecord::HEADER_SIZE;
    assert(log_record->log_record_type_ != LogRecordType::INVALID);
    assert(log_record->lsn_ != INVALID_LSN);

    switch (log_record->log_record_type_) {
      case LogRecordType::INSERT:
        memcpy(data, &log_record->insert_rid_, sizeof(RID));
        data += sizeof(RID);
        log_record->insert_tuple_.SerializeTo(data);
        break;
      case LogRecordType::APPLYDELETE:
      case LogRecordType::MARKDELETE:
      case LogRecordType::ROLLBACKDELETE:
        memcpy(data, &log_record->delete_rid_, sizeof(RID));
        data += sizeof(RID);
        log_record->delete_tuple_.SerializeTo(data);
        break;
      case LogRecordType::UPDATE:
        memcpy(data, &log_record->update_rid_, sizeof(RID));
        data += sizeof(RID);
        log_record->old_tuple_.SerializeTo(data);
        data += sizeof(log_record->old_tuple_.GetLength()) + sizeof(int32_t);
        log_record->new_tuple_.SerializeTo(data);
        break;
      case LogRecordType::NEWPAGE:
        memcpy(data, &log_record->prev_page_id_, sizeof(page_id_t));
        break;
      default:
        break;
    }
  }


}  // namespace bustub
