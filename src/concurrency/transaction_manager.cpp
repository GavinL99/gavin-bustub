//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// transaction_manager.cpp
//
// Identification: src/concurrency/transaction_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/transaction_manager.h"

#include <unordered_map>
#include <unordered_set>

#include "storage/table/table_heap.h"

namespace bustub {

std::unordered_map<txn_id_t, Transaction *> TransactionManager::txn_map = {};

Transaction *TransactionManager::Begin(Transaction *txn) {
  // Acquire the global transaction latch in shared mode.
  global_txn_latch_.RLock();

  if (txn == nullptr) {
    txn = new Transaction(next_txn_id_++);
  }

  if (enable_logging) {
    LogRecord newLog(txn->GetTransactionId(), INVALID_LSN, LogRecordType::BEGIN);
    txn->SetPrevLSN(log_manager_->AppendLogRecord(&newLog));
  }

  txn_map[txn->GetTransactionId()] = txn;
  return txn;
}

void TransactionManager::Commit(Transaction *txn) {
  txn->SetState(TransactionState::COMMITTED);

  // Perform all deletes before we commit.
  auto write_set = txn->GetWriteSet();
  while (!write_set->empty()) {
    auto &item = write_set->back();
    auto table = item.table_;
    if (item.wtype_ == WType::DELETE) {
      // Note that this also releases the lock when holding the page latch.
      // the operation will handle logging itself
      table->ApplyDelete(item.rid_, txn);
    }
    write_set->pop_back();
  }
  write_set->clear();

  if (enable_logging) {
    if (txn->GetPrevLSN() > log_manager_->GetPersistentLSN()) {
      log_manager_->TriggerFlush();
      assert(txn->GetPrevLSN() <= log_manager_->GetPersistentLSN());
    }
    LogRecord newLog(txn->GetTransactionId(), txn->GetPrevLSN(), LogRecordType::COMMIT);
    txn->SetPrevLSN(log_manager_->AppendLogRecord(&newLog));
  }

  // Release all the locks.
  ReleaseLocks(txn);
  // Release the global transaction latch.
  global_txn_latch_.RUnlock();
}

void TransactionManager::Abort(Transaction *txn) {
  txn->SetState(TransactionState::ABORTED);

  // Rollback before releasing the lock.
  auto write_set = txn->GetWriteSet();
  while (!write_set->empty()) {
    auto &item = write_set->back();
    auto table = item.table_;
    if (item.wtype_ == WType::DELETE) {
      table->RollbackDelete(item.rid_, txn);
    } else if (item.wtype_ == WType::INSERT) {
      // Note that this also releases the lock when holding the page latch.
      table->ApplyDelete(item.rid_, txn);
    } else if (item.wtype_ == WType::UPDATE) {
      table->UpdateTuple(item.tuple_, item.rid_, txn);
    }
    write_set->pop_back();
  }
  write_set->clear();

  if (enable_logging) {
    if (txn->GetPrevLSN() > log_manager_->GetPersistentLSN()) {
      log_manager_->TriggerFlush();
      assert(txn->GetPrevLSN() <= log_manager_->GetPersistentLSN());
    }
    LogRecord newLog(txn->GetTransactionId(), txn->GetPrevLSN(), LogRecordType::ABORT);
    txn->SetPrevLSN(log_manager_->AppendLogRecord(&newLog));
  }

  // Release all the locks.
  ReleaseLocks(txn);
  // Release the global transaction latch.
  global_txn_latch_.RUnlock();
}

void TransactionManager::BlockAllTransactions() { global_txn_latch_.WLock(); }

void TransactionManager::ResumeTransactions() { global_txn_latch_.WUnlock(); }

}  // namespace bustub
