//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// catalog_test.cpp
//
// Identification: test/catalog/catalog_test.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <string>
#include <unordered_set>
#include <vector>

#include "buffer/buffer_pool_manager.h"
#include "catalog/simple_catalog.h"
#include "gtest/gtest.h"
#include "type/value_factory.h"

namespace bustub {

// NOLINTNEXTLINE
TEST(CatalogTest, CreateTableTest) {
  auto disk_manager = new DiskManager("catalog_test.db");
  auto bpm = new BufferPoolManager(32, disk_manager);
  auto catalog = new SimpleCatalog(bpm, nullptr, nullptr);
  std::string table_name = "potato";
  std::string table_name_1 = "tomato";
  TableMetadata *t1, *t2;

  // The table shouldn't exist in the catalog yet.
  //  EXPECT_THROW(catalog->GetTable(table_name), std::out_of_range);

  // Put the table into the catalog.
  std::vector<Column> columns;
  columns.emplace_back("A", TypeId::INTEGER);
  columns.emplace_back("B", TypeId::BOOLEAN);
  Schema schema(columns);
  t1 = catalog->CreateTable(nullptr, table_name, schema);

  columns.emplace_back("AA", TypeId::INTEGER);
  columns.emplace_back("BB", TypeId::BOOLEAN);
  Schema schema1(columns);
  t2 = catalog->CreateTable(nullptr, table_name_1, schema1);

  // Notice that this test case doesn't check anything! :(
  // It is up to you to extend it
  EXPECT_EQ(t1, catalog->GetTable(0));
  EXPECT_EQ(t1, catalog->GetTable(table_name));
  EXPECT_EQ(t2, catalog->GetTable(1));
  EXPECT_EQ(t2, catalog->GetTable(table_name_1));

  delete catalog;
  delete bpm;
  delete disk_manager;
}

}  // namespace bustub
