//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.h
//
// Identification: src/include/execution/executors/hash_join_executor.h
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/logger.h"
#include "common/util/hash_util.h"
#include "container/hash/hash_function.h"
#include "container/hash/linear_probe_hash_table.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/expressions/abstract_expression.h"
#include "execution/plans/hash_join_plan.h"
#include "storage/index/hash_comparator.h"
#include "storage/table/tmp_tuple.h"
#include "storage/table/tuple.h"
#include "storage/page/tmp_tuple_page.h"

namespace bustub {
/**
 * IdentityHashFunction hashes everything to itself, i.e. h(x) = x.
 */
class IdentityHashFunction : public HashFunction<hash_t> {
 public:
  /**
   * Hashes the key.
   * @param key the key to be hashed
   * @return the hashed value
   */
  uint64_t GetHash(size_t key) override { return key; }
};

/**
 * A simple hash table that supports hash joins.
 */
class SimpleHashJoinHashTable {
 public:
  SimpleHashJoinHashTable() = default;

  /** Creates a new simple hash join hash table. */
  SimpleHashJoinHashTable(const std::string &name, BufferPoolManager *bpm, HashComparator cmp, uint32_t buckets,
                          const IdentityHashFunction &hash_fn) {}

  /**
   * Inserts a (hash key, tuple) pair into the hash table.
   * @param txn the transaction that we execute in
   * @param h the hash key
   * @param t the tuple to associate with the key
   * @return true if the insert succeeded
   */
  bool Insert(Transaction *txn, hash_t h, const Tuple &t) {
    hash_table_[h].emplace_back(t);
    return true;
  }

  /**
   * Gets the values in the hash table that match the given hash key.
   * @param txn the transaction that we execute in
   * @param h the hash key
   * @param[out] t the list of tuples that matched the key
   */
  void GetValue(Transaction *txn, hash_t h, std::vector<Tuple> *t) { *t = hash_table_[h]; }

  bool CheckKey(Transaction *txn, hash_t h) { return (hash_table_.find(h) != hash_table_.end()); }

 private:
  std::unordered_map<hash_t, std::vector<Tuple>> hash_table_;
};

// TODO(student): when you are ready to attempt task 3, replace the using declaration!
//using HT = SimpleHashJoinHashTable;
using HashJoinKeyType = hash_t;
using HashJoinValType = TmpTuple;
using HT = LinearProbeHashTable<HashJoinKeyType, HashJoinValType, HashComparator>;

// IF use Linear Hash table:
// 1. Construct table: Insert tuple in left child into tmp_page, get tmp_page_tuple, and insert (left_hash,
// tmp_page_tuple) into linear table (so no duplicate!)
// 2. Next: for each right tuple, get right hash and find the tmp_page_tuple, retrieve the real tuple and proceed

/**
 * HashJoinExecutor executes hash join operations.
 */
class HashJoinExecutor : public AbstractExecutor {
 public:
  /**
   * Creates a new hash join executor.
   * @param exec_ctx the context that the hash join should be performed in
   * @param plan the hash join plan node
   * @param left the left child, used by convention to build the hash table
   * @param right the right child, used by convention to probe the hash table
   */
  HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan, std::unique_ptr<AbstractExecutor> &&left,
                   std::unique_ptr<AbstractExecutor> &&right)
      : AbstractExecutor(exec_ctx), plan_(plan), bm_(exec_ctx_->GetBufferPoolManager()), left_(std::move(left)),
      right_(std::move(right)), jht_("",
          bm_, HashComparator(), jht_num_buckets_, HashFunction<hash_t >()){}
//  jht_("",
//  exec_ctx_->GetBufferPoolManager(), HashComparator(), )
  /** @return the JHT in use. Do not modify this function, otherwise you will get a zero. */
  const HT *GetJHT() const { return &jht_; }

  const Schema *GetOutputSchema() override { return plan_->OutputSchema(); }

  void Init() override {
    left_->Init();
    right_->Init();
    predicate_ = plan_->Predicate();
    l_schema_ = left_->GetOutputSchema();
    r_schema_ = right_->GetOutputSchema();
    page_id_t tmp_tuple_page(INVALID_PAGE_ID);
    TmpTuplePage *tmp_page_ptr(nullptr);
    // build hash table using left table
    while (true) {
      Tuple temp_t;
      Tuple *l_tuple = &temp_t;
      if (!left_->Next(l_tuple)) {
        break;
      }
      hash_t l_hash_v = HashValues(l_tuple, l_schema_, plan_->GetLeftKeys());
      TmpTuple tmp_tuple(INVALID_PAGE_ID, 0);

      // may need to allocate a new table
      if (tmp_tuple_page == INVALID_PAGE_ID || !tmp_page_ptr->Insert(*l_tuple, &tmp_tuple)) {
        if (tmp_tuple_page != INVALID_PAGE_ID) {
          assert(bm_->UnpinPage(tmp_tuple_page, true));
        }
        tmp_page_ptr = reinterpret_cast<TmpTuplePage *>(bm_->NewPage(&tmp_tuple_page, nullptr));
        assert(tmp_page_ptr && "new tmp tuple page!");
        assert(bm_->FlushPage(tmp_tuple_page, nullptr));
        tmp_page_ptr->Init(tmp_tuple_page, PAGE_SIZE);
      }
      LOG_DEBUG("Insert: %d\n", (int) l_tuple->GetRid().Get());
      assert(jht_.Insert(exec_ctx_->GetTransaction(), l_hash_v, tmp_tuple));
    }
    LOG_DEBUG("Finish building HT!\n");
  }

  // pass in an emptry tuple to modify, not nullptr
  bool Next(Tuple *tuple) override {
    // static variable that contains (one right tuple, all matched left tuples)
    static std::vector<Tuple> merged_tuple_vec_;
    static int merged_idx_ = -1;
    Tuple t_tuple;
    Tuple *r_tuple = &t_tuple;

    // if need to update merged_tuple_vec_
    if (merged_tuple_vec_.empty()) {
      while (right_->Next(r_tuple)) {
        hash_t r_hash_v = HashValues(r_tuple, r_schema_, plan_->GetRightKeys());
        // need to check all left tuples hashed to the same bucket
        std::vector<TmpTuple> temp_v;
        // get all left tuples
        if (jht_.GetValue(exec_ctx_->GetTransaction(), r_hash_v, &temp_v)) {
          // need to further check predicate
          // merge tuples for two sides, assume concat right to left
          for (const TmpTuple &tmp_tuple : temp_v) {
            // get tmp_page and read the real tuple
            auto tmp_page_ptr = bm_->FetchPage(tmp_tuple.GetPageId());
            assert(tmp_page_ptr);
            Tuple t;
            t.DeserializeFrom(tmp_page_ptr->GetData() + tmp_tuple.GetOffset());
            if (predicate_ == nullptr || predicate_->EvaluateJoin(&t, l_schema_, r_tuple, r_schema_).GetAs<bool>()) {
              //              LOG_DEBUG("Start merging...\n");
              std::vector<Value> temp_merged_v;
              // add by left schema
              for (uint32_t i = 0; i < l_schema_->GetColumnCount(); ++i) {
                temp_merged_v.push_back(t.GetValue(l_schema_, i));
              }
              for (uint32_t i = 0; i < r_schema_->GetColumnCount(); ++i) {
                temp_merged_v.push_back(r_tuple->GetValue(r_schema_, i));
              }
              merged_tuple_vec_.emplace_back(Tuple(temp_merged_v, plan_->OutputSchema()));
              //              LOG_DEBUG("Finished merging...\n");
            }
            bm_->UnpinPage(tmp_tuple.GetPageId(), false);
          }
          // if have matched something
          if (!merged_tuple_vec_.empty()) {
            break;
          }
        }
      }
    }

    if (!merged_tuple_vec_.empty()) {
      // which tuple to output
      merged_idx_ = merged_idx_ == -1 ? 0 : merged_idx_ + 1;
      // have to use assignment operator of the dummy Tuple!
      *tuple = merged_tuple_vec_[merged_idx_];
      // if use out all tuples retrieved last time, reset
      if (merged_idx_ == static_cast<int>(merged_tuple_vec_.size()) - 1) {
        merged_tuple_vec_.clear();
        merged_idx_ = -1;
      }
      return true;
    }
    return false;
  }

  /**
   * Hashes a tuple by evaluating it against every expression on the given schema, combining all non-null hashes.
   * @param tuple tuple to be hashed
   * @param schema schema to evaluate the tuple on
   * @param exprs expressions to evaluate the tuple with
   * @return the hashed tuple
   */
  hash_t HashValues(const Tuple *tuple, const Schema *schema, const std::vector<const AbstractExpression *> &exprs) {
    hash_t curr_hash = 0;
    // For every expression,
    for (const auto &expr : exprs) {
      // We evaluate the tuple on the expression and schema.
      Value val = expr->Evaluate(tuple, schema);
      // If this produces a value,
      if (!val.IsNull()) {
        // We combine the hash of that value into our current hash.
        curr_hash = HashUtil::CombineHashes(curr_hash, HashUtil::HashValue(&val));
      }
    }
    return curr_hash;
  }

 private:
  /** The hash join plan node. */
  const HashJoinPlanNode *plan_;
  BufferPoolManager *bm_;
  /** The comparator is used to compare hashes. */
  [[maybe_unused]] HashComparator jht_comp_{};
  /** The identity hash function. */
  IdentityHashFunction jht_hash_fn_{};

  /** The hash table that we are using. */
  /** The number of buckets in the hash table. */
  static constexpr uint32_t jht_num_buckets_ = 2;
  std::unique_ptr<AbstractExecutor> left_, right_;
  HT jht_;
  const Schema *l_schema_, *r_schema_;
  const AbstractExpression *predicate_;

};

}  // namespace bustub

