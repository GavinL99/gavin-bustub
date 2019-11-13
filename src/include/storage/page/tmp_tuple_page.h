#pragma once

#include "storage/page/page.h"
#include "storage/table/tmp_tuple.h"
#include "storage/table/tuple.h"
#include "common/logger.h"

namespace bustub {

// To pass the test cases for this class, you must follow the existing TmpTuplePage format and implement the
// existing functions exactly as they are! It may be helpful to look at TablePage.
// Remember that this task is optional, you get full credit if you finish the next task.

/**
 * TmpTuplePage format:
 *
 * Sizes are in bytes.
 * | PageId (4) | LSN (4) | FreeSpace (4) | (free space) | TupleSize2 | TupleData2 | TupleSize1 | TupleData1 |
 *
 * We choose this format because DeserializeExpression expects to read Size followed by Data.
 */
class TmpTuplePage : public Page {
 public:
  void Init(page_id_t page_id, uint32_t page_size) {
    LOG_DEBUG("Init: %d\n", page_size);
    memcpy(GetData(), &page_id, sizeof(page_id));
    memcpy(GetData() + OFFSET_FREE_SPACE, &page_size, sizeof(uint32_t));
  }

  page_id_t GetTablePageId() {
    return *reinterpret_cast<page_id_t*>(GetData());
  }

  bool Insert(const Tuple &tuple, TmpTuple *out) {
    auto ptr_to_sz = reinterpret_cast<uint32_t *>(GetData() + OFFSET_FREE_SPACE);
    uint32_t tuple_sz = tuple.GetLength();
    uint32_t prev_free_sz = *ptr_to_sz;
    if (prev_free_sz - OFFSET_FREE_SPACE >= tuple_sz + sizeof(uint32_t)) {
      memcpy(GetData() + prev_free_sz - tuple_sz, tuple.GetData(), tuple_sz);
      memcpy(GetData() + prev_free_sz - tuple_sz - sizeof(uint32_t), &tuple_sz, sizeof(uint32_t));
      out->setPageId(GetTablePageId());
      out->setOffset(prev_free_sz);
      *ptr_to_sz -= (tuple_sz + sizeof(uint32_t));

      LOG_DEBUG("Insert: %d, %d, %d\n", *ptr_to_sz, tuple_sz, prev_free_sz);
      return true;
    }
    return false;
  }

 private:
  static_assert(sizeof(page_id_t) == 4);
  static constexpr size_t OFFSET_FREE_SPACE = 8;
};

}  // namespace bustub
