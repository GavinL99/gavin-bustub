//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.h
//
// Identification: src/include/execution/executors/seq_scan_executor.h
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <vector>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/seq_scan_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * SeqScanExecutor executes a sequential scan over a table.
 */
class SeqScanExecutor : public AbstractExecutor {
 public:
  /**
   * Creates a new sequential scan executor.
   * @param exec_ctx the executor context
   * @param plan the sequential scan plan to be executed
   */
  SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan) : AbstractExecutor(exec_ctx), plan_(plan) {}

  void Init() override {
    SimpleCatalog *catalog_;
    table_oid_t t_id_;
    TableMetadata* table_ptr_;

    catalog_ = exec_ctx_->GetCatalog();
    t_id_ = plan_->GetTableOid();
    schema_ = plan_->OutputSchema();
    table_ptr_ = catalog_->GetTable(t_id_);
    iter_ = table_ptr_->table_->Begin(exec_ctx_->GetTransaction());
    iter_end_ = table_ptr_->table_->End();
    predicate_ = plan_->GetPredicate();
  }

  bool Next(Tuple *tuple) override {
    while (iter_ != iter_end_ &&
      predicate_->Evaluate(iter_, schema_).GetAs<bool>()) {
      *tuple = *(iter_++);
      return true;
    }
    return false;
  }

  const Schema *GetOutputSchema() override { return schema_; }

 private:
  /** The sequential scan plan node to be executed. */
  const SeqScanPlanNode *plan_;
  TableIterator iter_;
  TableIterator iter_end_;
  AbstractExpression *predicate_;
  Schema* schema_;


};
}  // namespace bustub
