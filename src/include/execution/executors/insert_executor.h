//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.h
//
// Identification: src/include/execution/executors/insert_executor.h
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <utility>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/insert_plan.h"
#include "storage/table/tuple.h"

namespace bustub {
/**
 * InsertExecutor executes an insert into a table.
 * Inserted values can either be embedded in the plan itself ("raw insert") or come from a child executor.
 */
class InsertExecutor : public AbstractExecutor {
 public:
  /**
   * Creates a new insert executor.
   * @param exec_ctx the executor context
   * @param plan the insert plan to be executed
   * @param child_executor the child executor to obtain insert values from, can be nullptr
   */
  InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                 std::unique_ptr<AbstractExecutor> &&child_executor)
      : AbstractExecutor(exec_ctx), plan_(plan), child_exec_(std::move(child_executor)) {
  }

  const Schema *GetOutputSchema() override { return plan_->OutputSchema(); }

  void Init() override {
    is_raw_ = plan_->IsRawInsert();
    if (is_raw_) {
      max_len_ = plan_->RawValues().size();
      iter_idx = 0;
    }
    table_ptr_ = exec_ctx_->GetCatalog()->GetTable(
        plan_->TableOid()
        );
  }

  // Note that Insert does not make use of the tuple pointer being passed in.
  // We return false if the insert failed for any reason, and return true if all inserts succeeded.
  bool Next([[maybe_unused]] Tuple *tuple) override {
    // rid is a useless variable
    RID *rid = &RID();
    bool output = false;
    // use a temp pointer instead of tuple in case that tuple == nullptr
    Tuple *temp_tuple;

    if (!is_raw_) {
      if (child_exec_->Next(temp_tuple)) {
        // possible that the table is full
        if (table_ptr_->table_->InsertTuple(*temp_tuple, rid, exec_ctx_->GetTransaction())) {
          output = true;
        }
      }
    } else {
      if (iter_idx_ < max_len_) {
        if (table_ptr_->table_->InsertTuple(
            Tuple(plan_->RawValuesAt(iter_idx_),
                plan_->OutputSchema()), rid, exec_ctx_->GetTransaction())) {
          output = true;
          iter_idx_++;
        }
      }
    }
    return output;
  }

 private:
  /** The insert plan node to be executed. */
  const InsertPlanNode *plan_;

  bool is_raw_;
  uint32_t max_len_;
  uint32_t iter_idx_;
  std::unique_ptr<AbstractExecutor> child_exec_;
  TableMetadata *table_ptr_;

};
}  // namespace bustub
