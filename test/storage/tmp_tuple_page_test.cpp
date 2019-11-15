//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// tmp_tuple_page_test.cpp
//
// Identification: test/storage/tmp_tuple_page_test.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <vector>

#include "gtest/gtest.h"
#include "storage/page/tmp_tuple_page.h"
#include "type/value_factory.h"

namespace bustub {

// NOLINTNEXTLINE
TEST(TmpTuplePageTest, BasicTest) {
  // There are many ways to do this assignment, and this is only one of them.
  // If you don't like the TmpTuplePage idea, please feel free to delete this test case entirely.
  // You will get full credit as long as you are correctly using a linear probe hash table.

  TmpTuplePage page{};
  page_id_t page_id = 15445;
  page.Init(page_id, PAGE_SIZE);

  char *data = page.GetData();
  ASSERT_EQ(*reinterpret_cast<page_id_t *>(data), page_id);
  ASSERT_EQ(*reinterpret_cast<uint32_t *>(data + sizeof(page_id_t) + sizeof(lsn_t)), PAGE_SIZE);

  std::vector<Column> columns;
  columns.emplace_back("A", TypeId::INTEGER);
  Schema schema(columns);

  std::vector<Value> values;
  values.emplace_back(ValueFactory::GetIntegerValue(123));

  Tuple tuple(values, &schema);
  TmpTuple tmp_tuple(INVALID_PAGE_ID, 0);
  page.Insert(tuple, &tmp_tuple);

  ASSERT_EQ(*reinterpret_cast<uint32_t *>(data + sizeof(page_id_t) + sizeof(lsn_t)), PAGE_SIZE - 8);
  ASSERT_EQ(*reinterpret_cast<uint32_t *>(data + PAGE_SIZE - 8), 4);
  ASSERT_EQ(*reinterpret_cast<uint32_t *>(data + PAGE_SIZE - 4), 123);
  ASSERT_EQ(tmp_tuple.GetPageId(), page_id);
  ASSERT_EQ(tmp_tuple.GetOffset(), PAGE_SIZE - 8);

  columns.emplace_back("B", TypeId::INTEGER);
  Schema schema1(columns);
  values.emplace_back(ValueFactory::GetIntegerValue(456));
  Tuple tuple1(values, &schema1);
  page.Insert(tuple, &tmp_tuple);
  ASSERT_EQ(*reinterpret_cast<uint32_t *>(data + sizeof(page_id_t) + sizeof(lsn_t)), PAGE_SIZE - 16);
  ASSERT_EQ(*reinterpret_cast<uint32_t *>(data + PAGE_SIZE - 16), 4);
  ASSERT_EQ(*reinterpret_cast<uint32_t *>(data + PAGE_SIZE - 12), 123);
  ASSERT_EQ(tmp_tuple.GetPageId(), page_id);
  ASSERT_EQ(tmp_tuple.GetOffset(), PAGE_SIZE - 16);

  Tuple tuple2;
  tuple2.DeserializeFrom(page.GetData() + tmp_tuple.GetOffset());
  ASSERT_EQ(tuple2.GetLength(), 4);
  ASSERT_EQ(tuple2.GetValue(&schema1, 0).GetAs<int>(), 123);
  ASSERT_EQ(tuple2.GetValue(&schema1, 1).GetAs<int>(), 456);
}

}  // namespace bustub
