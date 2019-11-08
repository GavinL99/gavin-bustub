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
  SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan) : AbstractExecutor(exec_ctx), plan_(plan),
  table_ptr_(exec_ctx->GetCatalog()->GetTable(plan_->GetTableOid())),
  iter_(table_ptr_->table_->Begin(exec_ctx->GetTransaction())),
  iter_end_(table_ptr_->table_->End()) {
  }

  void Init() override {
    schema_ = plan_->OutputSchema();
    predicate_ = plan_->GetPredicate();
  }

  bool Next(Tuple *tuple) override {
    while (iter_ != iter_end_) {
      // have to use assignment operator of the dummy Tuple!
//      *tuple = *(iter_++);
      *tuple = *iter_;
      ++iter_;
      if (predicate_ && predicate_->Evaluate(tuple, schema_).GetAs<bool>()) {
        return true;
      }
    }
    return false;
  }

  const Schema *GetOutputSchema() override { return schema_; }

 private:
  /** The sequential scan plan node to be executed. */
  const SeqScanPlanNode *plan_;
  TableMetadata *table_ptr_;
  TableIterator iter_;
  TableIterator iter_end_;
  const AbstractExpression *predicate_;
  const Schema *schema_;

};
}  // namespace bustub
