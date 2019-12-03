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
        [=] {return !enable_logging;});    //
    // if timeout, need to swap and set persistent_lsn_
    LOG_DEBUG("Flush helper timeout...\n");
    if (buffer_used_ > 0) {
      char *temp = log_buffer_;
      log_buffer_ = flush_buffer_;
      flush_buffer_ = temp;
      persistent_lsn_ = next_lsn_ - 1;
    } else {
      LOG_INFO("No log to flush...\n");
      continue;
    }
    LOG_INFO("Flush helper wrote to disk: %d\n", (int) buffer_used_);
//    LOG_DEBUG("Thread: %d\n", (int) std::hash<std::thread::id>{}(std::this_thread::get_id()));
    disk_manager_->WriteLog(flush_buffer_, buffer_used_);
    buffer_used_ = 0;
  }
}

/*
 * called by bpm when need to evict a dirty page
 * force flush, so can flush first and then swap buffers
 */
void LogManager::TriggerFlush() {
  LOG_DEBUG("Trigger force flush!\n");
  uniq_lock lock(latch_);
  // flush log buffer and then swap!
  char *temp = log_buffer_;
  log_buffer_ = flush_buffer_;
  flush_buffer_ = temp;
  persistent_lsn_ = next_lsn_ - 1;
  disk_manager_->WriteLog(flush_buffer_, buffer_used_);
  LOG_DEBUG("Finish Trigger force flush: %d!\n", (int) buffer_used_);
  buffer_used_ = 0;
}


/*
 * Stop and join the flush thread, set enable_logging = false
 */
void LogManager::StopFlushThread() {
  LOG_INFO("Stop flush thread...\n");
  enable_logging = false;
  flush_cv_.notify_one();
  flush_thread_->join();
  delete flush_thread_;
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
//  if need to swap and flush
  if (buffer_used_ + log_record->size_ > LOG_BUFFER_SIZE) {
    persistent_lsn_ = next_lsn_ - 2;
    char *temp = log_buffer_;
    log_buffer_ = flush_buffer_;
    flush_buffer_ = temp;

    // need to null future pointer first and write
    // no need to swap page here
    LOG_DEBUG("Set Async Flush Futures\n");
    std::future<void> fut = std::async(std::launch::async, [=] {
      bustub::LOG_DEBUG("Async Flush size: %d, Thread: %d\n", (int) buffer_used_,
                        (int) std::hash<std::thread::id>{}(std::this_thread::get_id()));
      disk_manager_->SetFlushLogFuture(nullptr);
      disk_manager_->WriteLog(flush_buffer_, buffer_used_);
      bustub::LOG_DEBUG("Finish Async flush\n");
      persistent_lsn_ = next_lsn_ - 1;
    });
    disk_manager_->SetFlushLogFuture(&fut);
    flush_cv_.notify_one();
    buffer_used_ = 0;
  }
  SerializeLog(log_buffer_ + buffer_used_, log_record);
  assert(log_record->size_ > 0 && log_record->lsn_ != INVALID_LSN);
  buffer_used_ += log_record->size_;
  total_log_sz += log_record->size_;
  return log_record->lsn_;
}


void LogManager::SerializeLog(char* data, LogRecord* log_record) {
  char* begin_ptr = data;
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
    case LogRecordType::APPLYDELETE: case LogRecordType::MARKDELETE: case LogRecordType::ROLLBACKDELETE:
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
  LogRecord dummy_log;
  LogRecovery::DeserialHelper(begin_ptr, &dummy_log);
  LOG_DEBUG("Actual Serialize: %s\n", dummy_log.ToString().c_str());
}


}  // namespace bustub
