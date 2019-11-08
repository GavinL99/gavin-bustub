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
#include "common/logger.h"

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

  bool CheckKey(Transaction *txn, hash_t h) {
    return (hash_table_.find(h) != hash_table_.end());
  }


 private:
  std::unordered_map<hash_t, std::vector<Tuple>> hash_table_;
};

// TODO(student): when you are ready to attempt task 3, replace the using declaration!
using HT = SimpleHashJoinHashTable;

// using HashJoinKeyType = ???;
// using HashJoinValType = ???;
// using HT = LinearProbeHashTable<HashJoinKeyType, HashJoinValType, HashComparator>;

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
      : AbstractExecutor(exec_ctx), plan_(plan), left_(std::move(left)), right_(std::move(right)) {
      }

  /** @return the JHT in use. Do not modify this function, otherwise you will get a zero. */
  // Uncomment me! const HT *GetJHT() const { return &jht_; }

  const Schema *GetOutputSchema() override { return plan_->OutputSchema(); }

  void Init() override {
    left_->Init();
    right_->Init();
    predicate_ = plan_->Predicate();
    l_schema_ = left_->GetOutputSchema();
    r_schema_ = right_->GetOutputSchema();
    // build hash table using left table
    while (true) {
      Tuple temp_t;
      Tuple *l_tuple = &temp_t;
      if (!left_->Next(l_tuple)) {
        break;
      }
      hash_t l_hash_v = HashValues(l_tuple, l_schema_, plan_->GetLeftKeys());
      jht_.Insert(exec_ctx_->GetTransaction(), l_hash_v, *l_tuple);
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
        if (jht_.CheckKey(exec_ctx_->GetTransaction(), r_hash_v)) {
          std::vector<Tuple> temp_v;
          // get all left tuples
          jht_.GetValue(exec_ctx_->GetTransaction(), r_hash_v, &temp_v);
          // need to further check predicate
          // merge tuples for two sides, assume concat right to left
          for (const Tuple& t: temp_v) {
            if (!predicate_ || predicate_->EvaluateJoin(&t, l_schema_, r_tuple, r_schema_).GetAs<bool>()) {
              LOG_DEBUG("Start merging...\n");
              std::vector<Value> temp_merged_v;
              for (uint32_t i = 0; i < t.GetLength(); ++i) {
                temp_merged_v.push_back(t.GetValue(l_schema_, i));
              }
              for (uint32_t i = 0; i < r_tuple->GetLength(); ++i) {
                temp_merged_v.push_back(r_tuple->GetValue(r_schema_, i));
              }
              merged_tuple_vec_.emplace_back(
                  Tuple(temp_merged_v, plan_->OutputSchema())
                  );
            }
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
      merged_idx_ = merged_idx_ == -1? 0: merged_idx_ + 1;
      // have to use assignment operator of the dummy Tuple!
      *tuple = merged_tuple_vec_[merged_idx_];
      merged_idx_ += 1;
      // if use out all tuples retrieved last time, reset
      if (merged_idx_ == static_cast<int>(merged_tuple_vec_.size())) {
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
  /** The comparator is used to compare hashes. */
  [[maybe_unused]] HashComparator jht_comp_{};
  /** The identity hash function. */
  IdentityHashFunction jht_hash_fn_{};

  /** The hash table that we are using. */
  HT jht_;
  /** The number of buckets in the hash table. */
  static constexpr uint32_t jht_num_buckets_ = 2;
  std::unique_ptr<AbstractExecutor> left_, right_;
  const Schema *l_schema_, *r_schema_;
  const AbstractExpression *predicate_;


};
}  // namespace bustub
