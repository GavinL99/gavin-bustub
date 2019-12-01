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
  flush_thread_ = new std::thread([=] {flush_helper();});
}

/**
 * timeout or async call when full (need to swap buffer in append_log)
 */
void LogManager::flush_helper() {
  while (enable_logging) {
    uniq_lock lock(latch_);
    // auto call unlock: deal with spurious wakeup
    LOG_INFO("Flush helper waiting...\n");
    flush_cv_.wait_for(lock, log_timeout,
        [=] {return !enable_logging || disk_manager_->HasFlushLogFuture();});    //
    // if timeout, need to swap and set persistent_lsn_
    if (!disk_manager_->HasFlushLogFuture()) {
      LOG_INFO("Flush helper timeout...\n");
      if (buffer_used_ > 0) {
        char *temp = log_buffer_;
        log_buffer_ = flush_buffer_;
        flush_buffer_ = temp;
        persistent_lsn_ = next_lsn_ - 1;
      } else {
        LOG_INFO("No log to flush...\n");
        continue;
      }
    }
    int gg = buffer_used_;
    LOG_INFO("Flush helper wrote to disk: %d\n", gg);
    disk_manager_->WriteLog(flush_buffer_, buffer_used_);
    buffer_used_ = 0;
  }
}

/*
 * called by bpm when need to evict a dirty page
 * force flush, so can flush first and then swap buffers
 */
void LogManager::TriggerFlush() {
  uniq_lock lock(latch_);
  // flush log buffer and then swap!
  disk_manager_->WriteLog(log_buffer_, buffer_used_);
  char *temp = log_buffer_;
  log_buffer_ = flush_buffer_;
  flush_buffer_ = temp;
  persistent_lsn_ = next_lsn_ - 1;
  buffer_used_ = 0;
}


/*
 * Stop and join the flush thread, set enable_logging = false
 */
void LogManager::StopFlushThread() {
  LOG_INFO("Stop flush thread...\n");
  enable_logging = false;
  flush_thread_->join();
}

/*
 * This is NON blocking
 * append a log record into log buffer
 * swap buffer, if full or evict dirty, notify flushing thread;
 * you MUST set the log record's lsn within this method
 * @return: lsn that is assigned to this log record
 *
 *
 * example below
 * // First, serialize the header fields(20 bytes in total)
 * log_record.lsn_ = next_lsn_++;
 * memcpy(log_buffer_ + offset_, &log_record, 20);
 * int pos = offset_ + 20;
 *
 * if (log_record.log_record_type_ == LogRecordType::INSERT) {
 *    memcpy(log_buffer_ + pos, &log_record.insert_rid_, sizeof(RID));
 *    pos += sizeof(RID);
 *    // we have provided serialize function for tuple class
 *    log_record.insert_tuple_.SerializeTo(log_buffer_ + pos);
 *  }
 *
 */
lsn_t LogManager::AppendLogRecord(LogRecord *log_record) {
//  make sure serializeation
  uniq_lock lock(latch_);
  log_record->lsn_ = next_lsn_++;
//  if need to swap and flush
  if (buffer_used_ + log_record->size_ > LOG_BUFFER_SIZE) {
    persistent_lsn_ = next_lsn_ - 2;
    char *temp = log_buffer_;
    log_buffer_ = flush_buffer_;
    flush_buffer_ = temp;

    // need to null future pointer first and write
    int temp_sz = buffer_used_;
    // no need to swap page here
    std::future<void> fut = std::async(std::launch::async, [=] {
      bustub::LOG_DEBUG("Flush size: %d\n", temp_sz);
      disk_manager_->SetFlushLogFuture(nullptr);
      disk_manager_->WriteLog(flush_buffer_, buffer_used_);
      char *temp = log_buffer_;
      log_buffer_ = flush_buffer_;
      flush_buffer_ = temp;
      persistent_lsn_ = next_lsn_ - 1;
    });
    disk_manager_->SetFlushLogFuture(&fut);
    flush_cv_.notify_one();
    buffer_used_ = 0;
  }
  // includes commit/abort/begin
  memcpy(log_buffer_ + buffer_used_, &log_record, LogRecord::HEADER_SIZE);
  int pos = buffer_used_ + LogRecord::HEADER_SIZE;
  assert(log_record->log_record_type_ != LogRecordType::INVALID);

  switch (log_record->log_record_type_) {
    case LogRecordType::INSERT:
      memcpy(log_buffer_ + pos, &log_record->insert_rid_, sizeof(RID));
      pos += sizeof(RID);
      log_record->insert_tuple_.SerializeTo(log_buffer_ + pos);
      break;
    case LogRecordType::APPLYDELETE: case LogRecordType::MARKDELETE: case LogRecordType::ROLLBACKDELETE:
      memcpy(log_buffer_ + pos, &log_record->delete_rid_, sizeof(RID));
      pos += sizeof(RID);
      log_record->delete_tuple_.SerializeTo(log_buffer_ + pos);
      break;
    case LogRecordType::UPDATE:
      memcpy(log_buffer_ + pos, &log_record->update_rid_, sizeof(RID));
      pos += sizeof(RID);
      log_record->old_tuple_.SerializeTo(log_buffer_ + pos);
      pos += sizeof(log_record->old_tuple_.GetLength()) + sizeof(int32_t);
      log_record->new_tuple_.SerializeTo(log_buffer_ + pos);
      break;
    case LogRecordType::NEWPAGE:
      memcpy(log_buffer_ + pos, &log_record->prev_page_id_, sizeof(page_id_t));
      break;
    default:
      break;
  }
  return log_record->lsn_;
}

}  // namespace bustub
