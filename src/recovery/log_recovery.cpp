//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// log_recovery.cpp
//
// Identification: src/recovery/log_recovery.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "recovery/log_recovery.h"

#include "storage/page/table_page.h"


namespace bustub {
/*
 * deserialize a log record from log buffer
 * @return: true means deserialize succeed, otherwise can't deserialize cause
 * incomplete log record
 */
  bool LogRecovery::DeserializeLogRecord(const char *data, LogRecord *log_record) {
    int32_t log_sz = *reinterpret_cast<const uint32_t *>(data);
    LogRecordType log_type = *reinterpret_cast<const LogRecordType *>(data + sizeof(uint32_t));
    if (data + log_sz > log_buffer_ + LOG_BUFFER_SIZE || log_type == LogRecordType::INVALID) {
      return false;
    }
    // read header
    memcpy((void *) log_record, data, sizeof(LogRecord::HEADER_SIZE));
    const char *tuple_data = data + LogRecord::HEADER_SIZE;

    switch (log_record->log_record_type_) {
      case LogRecordType::INSERT:
        log_record->insert_rid_ = *reinterpret_cast<const RID *>(tuple_data);
        log_record->insert_tuple_.DeserializeFrom(tuple_data + sizeof(RID));
        break;
      case LogRecordType::APPLYDELETE:
      case LogRecordType::MARKDELETE:
      case LogRecordType::ROLLBACKDELETE:
        log_record->delete_rid_ = *reinterpret_cast<const RID *>(tuple_data);
        log_record->delete_tuple_.DeserializeFrom(tuple_data + sizeof(RID));
        break;
      case LogRecordType::UPDATE:
        log_record->update_rid_ = *reinterpret_cast<const RID *>(tuple_data);
        log_record->old_tuple_.DeserializeFrom(tuple_data + sizeof(RID));
        log_record->new_tuple_.DeserializeFrom(tuple_data + sizeof(RID) +
                                               sizeof(int32_t) + log_record->old_tuple_.GetLength());
        break;
      case LogRecordType::NEWPAGE:
        log_record->prev_page_id_ = *reinterpret_cast<const page_id_t *>(tuple_data);
        break;
      default:
        break;
    }
    return true;
  }

/*
 *redo phase on TABLE PAGE level(table/table_page.h)
 *read log file from the beginning to end (you must prefetch log records into
 *log buffer to reduce unnecessary I/O operations), remember to compare page's
 *LSN with log_record's sequence number, and also build active_txn_ table &
 *lsn_mapping_ table
 */
  void LogRecovery::Redo() {
    offset_ = 0;
    // stop when hit invalid log or no more log to read
    while (true) {
      if (!disk_manager_->ReadLog(log_buffer_, LOG_BUFFER_SIZE, offset_)) {
        break;
      }
      // loop over records on one buffer
      int cursor = 0;

      while (true) {
        LogRecord temp_log;
        if (!DeserializeLogRecord(log_buffer_ + cursor, &temp_log)) {
          break;
        }
        lsn_t temp_lsn = temp_log.lsn_;
        txn_id_t temp_txn = temp_log.txn_id_;
        LogRecordType temp_type = temp_log.log_record_type_;
        LOG_DEBUG("Replay: %d\n", temp_log.log_record_type_);

        if (temp_type == LogRecordType::COMMIT || temp_type == LogRecordType::ABORT) {
          active_txn_.erase(temp_txn);
        } else if (temp_log.log_record_type_ == LogRecordType::BEGIN) {
          assert(active_txn_.find(temp_txn) == active_txn_.end());
          active_txn_[temp_txn] = temp_lsn;
        } else {
          // update active txn table
          active_txn_[temp_txn] = temp_lsn;
          lsn_mapping_[temp_lsn] = offset_ + cursor;

          if (temp_type == LogRecordType::INSERT) {
            auto temp_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(
                temp_log.insert_rid_.GetPageId()));
            Tuple temp_old_t;
            temp_page->UpdateTuple(temp_log.insert_tuple_, &temp_old_t, temp_log.insert_rid_, nullptr, nullptr,
                                   nullptr);
            buffer_pool_manager_->UnpinPage(temp_log.insert_rid_.GetPageId(), true);
          } else if (temp_type == LogRecordType::MARKDELETE) {
            auto temp_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(
                temp_log.delete_rid_.GetPageId()));
            temp_page->MarkDelete(temp_log.delete_rid_, nullptr, nullptr, nullptr);
            buffer_pool_manager_->UnpinPage(temp_log.delete_rid_.GetPageId(), true);
          } else if (temp_type == LogRecordType::APPLYDELETE) {
            auto temp_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(
                temp_log.delete_rid_.GetPageId()));
            temp_page->ApplyDelete(temp_log.delete_rid_, nullptr, nullptr);
            buffer_pool_manager_->UnpinPage(temp_log.delete_rid_.GetPageId(), true);
          } else if (temp_type == LogRecordType::ROLLBACKDELETE) {
            auto temp_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(
                temp_log.delete_rid_.GetPageId()));
            temp_page->RollbackDelete(temp_log.delete_rid_, nullptr, nullptr);
            buffer_pool_manager_->UnpinPage(temp_log.delete_rid_.GetPageId(), true);
          } else if (temp_type == LogRecordType::UPDATE) {
            Tuple temp_old_t;
            auto temp_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(
                temp_log.update_rid_.GetPageId()));
            temp_page->UpdateTuple(temp_log.new_tuple_, &temp_old_t, temp_log.update_rid_, nullptr, nullptr, nullptr);
            buffer_pool_manager_->UnpinPage(temp_log.update_rid_.GetPageId(), true);
          }
        }
        cursor += temp_log.size_;
      }
      offset_ += LOG_BUFFER_SIZE;
    }
  }

/*
 *undo phase on TABLE PAGE level(table/table_page.h)
 *iterate through active txn map and undo each operation
 */
  void LogRecovery::Undo() {
    for (auto iter: active_txn_) {
      // walk backward and reverse operations
      lsn_t temp_lsn = iter.second;
      while (lsn_mapping_.find(temp_lsn) != lsn_mapping_.end()) {
        disk_manager_->ReadLog(log_buffer_, LOG_BUFFER_SIZE,
                               lsn_mapping_[temp_lsn]);
        LogRecord temp_log;
        if (!DeserializeLogRecord(log_buffer_, &temp_log)) {
          LOG_ERROR("Fail to deserialize log...\n");
          break;
        }
        assert(temp_log.lsn_ == temp_lsn);
        LogRecordType temp_type = temp_log.log_record_type_;

        if (temp_type == LogRecordType::INSERT) {
          auto temp_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(
              temp_log.insert_rid_.GetPageId()));
          Tuple temp_old_t;
          temp_page->ApplyDelete(temp_log.insert_rid_, nullptr, nullptr);
          buffer_pool_manager_->UnpinPage(temp_log.insert_rid_.GetPageId(), true);
        } else if (temp_type == LogRecordType::MARKDELETE) {
          auto temp_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(
              temp_log.delete_rid_.GetPageId()));
          temp_page->RollbackDelete(temp_log.delete_rid_, nullptr, nullptr);
          buffer_pool_manager_->UnpinPage(temp_log.delete_rid_.GetPageId(), true);
        } else if (temp_type == LogRecordType::APPLYDELETE) {
          auto temp_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(
              temp_log.delete_rid_.GetPageId()));
          Tuple temp_old_t;
          temp_page->UpdateTuple(temp_log.delete_tuple_, &temp_old_t, temp_log.delete_rid_, nullptr, nullptr, nullptr);
          buffer_pool_manager_->UnpinPage(temp_log.delete_rid_.GetPageId(), true);
        } else if (temp_type == LogRecordType::ROLLBACKDELETE) {
          LOG_ERROR("Shoul not undo rollback!\n");
        } else if (temp_type == LogRecordType::UPDATE) {
          Tuple temp_old_t;
          auto temp_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(
              temp_log.update_rid_.GetPageId()));
          // recover to old value
          temp_page->UpdateTuple(temp_log.old_tuple_, &temp_old_t, temp_log.update_rid_, nullptr, nullptr, nullptr);
          buffer_pool_manager_->UnpinPage(temp_log.update_rid_.GetPageId(), true);
        }
        // walk reversely
        temp_lsn = temp_log.prev_lsn_;
      }
    }
    active_txn_.clear();
    lsn_mapping_.clear();
  }

}  // namespace bustub
