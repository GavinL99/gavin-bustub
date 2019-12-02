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
    int32_t log_sz = *reinterpret_cast<const int32_t *>(data);
    if (data + log_sz > log_buffer_ + LOG_BUFFER_SIZE) {
      LOG_DEBUG("Deserial Out of Bound!\n");
      return false;
    }
    // read header
    DeserialHelper(data, log_record);
    return true;
  }

  /*
   * Assume valid buffer to deserialize
   */
  void LogRecovery::DeserialHelper(const char *data, LogRecord *log_record) {
//    memcpy((void *) log_record, data, sizeof(LogRecord::HEADER_SIZE));
    memcpy(&log_record->size_, data, sizeof(int32_t));
    data += sizeof(int32_t);
    memcpy(&log_record->lsn_, data, sizeof(lsn_t));
    data += sizeof(lsn_t);
    memcpy(&log_record->txn_id_, data, sizeof(txn_id_t));
    data += sizeof(txn_id_t);
    memcpy(&log_record->prev_lsn_, data, sizeof(lsn_t));
    data += sizeof(lsn_t);
    memcpy(&log_record->log_record_type_, data, sizeof(LogRecordType));
    data += sizeof(LogRecordType);

    assert(log_record->lsn_ != INVALID_LSN);
    assert(log_record->size_ > 0);
//    LOG_DEBUG("Deserialize: %s\n", log_record->ToString().c_str());
    switch (log_record->log_record_type_) {
      case LogRecordType::INSERT:
        log_record->insert_rid_ = *reinterpret_cast<const RID *>(data);
        log_record->insert_tuple_.DeserializeFrom(data + sizeof(RID));
        break;
      case LogRecordType::APPLYDELETE:
      case LogRecordType::MARKDELETE:
      case LogRecordType::ROLLBACKDELETE:
        log_record->delete_rid_ = *reinterpret_cast<const RID *>(data);
        log_record->delete_tuple_.DeserializeFrom(data + sizeof(RID));
        break;
      case LogRecordType::UPDATE:
        log_record->update_rid_ = *reinterpret_cast<const RID *>(data);
        log_record->old_tuple_.DeserializeFrom(data + sizeof(RID));
        log_record->new_tuple_.DeserializeFrom(data + sizeof(RID) +
                                               sizeof(int32_t) + log_record->old_tuple_.GetLength());
        break;
      case LogRecordType::NEWPAGE:
        log_record->prev_page_id_ = *reinterpret_cast<const page_id_t *>(data);
        break;
      default:
        break;
    }
  }

  void LogRecovery::TestDeserial() {
    assert(disk_manager_->ReadLog(log_buffer_, LOG_BUFFER_SIZE, 0));
    int cursor = 0;
    while (true) {
      LogRecord temp_log;
      LOG_DEBUG("Cursor: %d\n", cursor);
      if (*(log_buffer_ + cursor) == '\0' || !DeserializeLogRecord(log_buffer_ + cursor, &temp_log)) {
        break;
      }
      LOG_DEBUG("Deserial: %s\n", temp_log.ToString().c_str());
      cursor += temp_log.GetSize();
    }
  }

/*
 *redo phase on TABLE PAGE level(table/table_page.h)
 *read log file from the beginning to end (you must prefetch log records into
 *log buffer to reduce unnecessary I/O operations), remember to compare page's
 *LSN with log_record's sequence number, and also build active_txn_ table &
 *lsn_mapping_ table
 */
  void LogRecovery::Redo() {
    assert(active_txn_.empty() && lsn_mapping_.empty());
    // scan all log
    assert(disk_manager_->ReadLog(log_buffer_, LOG_BUFFER_SIZE, offset_));
    offset_ = 0;
    int cursor = 0;
    while (true) {
      // loop over records on one buffer
      if (*(log_buffer_ + cursor) == '\0') {
        break;
      }
      LogRecord temp_log;
      if (cursor == LOG_BUFFER_SIZE || !DeserializeLogRecord(log_buffer_ + cursor, &temp_log)) {
        LOG_DEBUG("Hit the boundary, readjust cursor\n");
        offset_ += cursor;
        disk_manager_->ReadLog(log_buffer_, LOG_BUFFER_SIZE, offset_);
        cursor = 0;
      }
      RedoHelper(temp_log, cursor);
      cursor += temp_log.size_;
      offset_ += LOG_BUFFER_SIZE;
    }
  }

  void LogRecovery::RedoHelper(const LogRecord &temp_log, int cursor) {
    lsn_t temp_lsn = temp_log.lsn_;
    txn_id_t temp_txn = temp_log.txn_id_;
    LogRecordType temp_type = temp_log.log_record_type_;
    LOG_DEBUG("Replay: %s\n", temp_log.ToString().c_str());

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
        if (temp_page->GetLSN() < temp_log.lsn_) {
          Tuple temp_old_t;
          assert(temp_page->UpdateTuple(temp_log.insert_tuple_, &temp_old_t,
                                        temp_log.insert_rid_,
                                        nullptr, nullptr, nullptr));
          temp_page->SetLSN(temp_log.lsn_);
        } else {
          LOG_DEBUG("No Insert! Page: %d, Log: %d\n", temp_page->GetLSN(), temp_log.lsn_);
        }
        buffer_pool_manager_->UnpinPage(temp_log.insert_rid_.GetPageId(), true);
      } else if (temp_type == LogRecordType::MARKDELETE) {
        auto temp_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(
            temp_log.delete_rid_.GetPageId()));
        if (temp_page->GetLSN() < temp_log.lsn_) {
          assert(temp_page->MarkDelete(temp_log.delete_rid_, nullptr, nullptr, nullptr));
          temp_page->SetLSN(temp_log.lsn_);
        } else {
          LOG_DEBUG("No delete! Page: %d, Log: %d\n", temp_page->GetLSN(), temp_log.lsn_);
        }
        buffer_pool_manager_->UnpinPage(temp_log.delete_rid_.GetPageId(), true);
      } else if (temp_type == LogRecordType::APPLYDELETE) {
        auto temp_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(
            temp_log.delete_rid_.GetPageId()));
        if (temp_page->GetLSN() < temp_log.lsn_) {
          temp_page->ApplyDelete(temp_log.delete_rid_, nullptr, nullptr);
          temp_page->SetLSN(temp_log.lsn_);
        } else {
          LOG_DEBUG("No delete! Page: %d, Log: %d\n", temp_page->GetLSN(), temp_log.lsn_);
        }
        buffer_pool_manager_->UnpinPage(temp_log.delete_rid_.GetPageId(), true);
      } else if (temp_type == LogRecordType::ROLLBACKDELETE) {
        auto temp_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(
            temp_log.delete_rid_.GetPageId()));
        if (temp_page->GetLSN() < temp_log.lsn_) {
          temp_page->RollbackDelete(temp_log.delete_rid_, nullptr, nullptr);
          temp_page->SetLSN(temp_log.lsn_);
        } else {
          LOG_DEBUG("No delete! Page: %d, Log: %d\n", temp_page->GetLSN(), temp_log.lsn_);
        }
        buffer_pool_manager_->UnpinPage(temp_log.delete_rid_.GetPageId(), true);
      } else if (temp_type == LogRecordType::UPDATE) {
        auto temp_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(
            temp_log.update_rid_.GetPageId()));
        if (temp_page->GetLSN() < temp_log.lsn_) {
          Tuple temp_old_t;
          assert(temp_page->UpdateTuple(temp_log.new_tuple_, &temp_old_t, temp_log.update_rid_, nullptr, nullptr,
                                        nullptr));
          temp_page->SetLSN(temp_log.lsn_);
        } else {
          LOG_DEBUG("No update! Page: %d, Log: %d\n", temp_page->GetLSN(), temp_log.lsn_);
        }
        buffer_pool_manager_->UnpinPage(temp_log.update_rid_.GetPageId(), true);
      }
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
