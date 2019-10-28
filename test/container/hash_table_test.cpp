//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_table_test.cpp
//
// Identification: test/container/hash_table_test.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <thread>  // NOLINT
#include <vector>
#include "common/logger.h"
#include "container/hash/linear_probe_hash_table.h"
#include "gtest/gtest.h"
#include "murmur3/MurmurHash3.h"

namespace bustub {

// NOLINTNEXTLINE
  TEST(HashTableTest, SampleTest) {
    auto *disk_manager = new DiskManager("test.db");
    auto *bpm = new BufferPoolManager(70, disk_manager);

    LinearProbeHashTable<int, int, IntComparator> ht("blah", bpm, IntComparator(), 100, HashFunction<int>());

    // insert a few values
    for (int i = 0; i < 10000; i++) {
      ht.Insert(nullptr, i, i);
      std::vector<int> res;
      ht.GetValue(nullptr, i, &res);
      EXPECT_EQ(1, res.size()) << "Failed to insert " << i << std::endl;
      EXPECT_EQ(i, res[0]);
    }

    // check if the inserted values are all there
    for (int i = 0; i < 10000; i++) {
      std::vector<int> res;
      ht.GetValue(nullptr, i, &res);
      EXPECT_EQ(1, res.size()) << "Failed to keep " << i << std::endl;
      EXPECT_EQ(i, res[0]);
    }

    // insert one more value for each key
    for (int i = 0; i < 10000; i++) {
//      LOG_DEBUG("Insert: %d\n", i);
      if (i == 0) {
        // duplicate values for the same key are not allowed
        EXPECT_FALSE(ht.Insert(nullptr, i, 2 * i));
      } else {
        EXPECT_TRUE(ht.Insert(nullptr, i, 2 * i));
      }
      // this should all be duplicates
      ht.Insert(nullptr, i, 2 * i);
      std::vector<int> res;
      ht.GetValue(nullptr, i, &res);
      if (i == 0) {
        // duplicate values for the same key are not allowed
        EXPECT_EQ(1, res.size());
        EXPECT_EQ(i, res[0]);
      } else {
        EXPECT_EQ(2, res.size());
        if (res[0] == i) {
          EXPECT_EQ(2 * i, res[1]);
        } else {
          EXPECT_EQ(2 * i, res[0]);
          EXPECT_EQ(i, res[1]);
        }
      }
    }

    // delete some values
    for (int i = 0; i < 10000; i++) {
      std::vector<int> res;
      ht.GetValue(nullptr, i, &res);
      if (i == 0) {
        EXPECT_EQ(1, res.size());
      } else {
        EXPECT_EQ(2, res.size());
      }
      EXPECT_TRUE(ht.Remove(nullptr, i, i));
      res.clear();
      ht.GetValue(nullptr, i, &res);
      if (i == 0) {
        // (0, 0) is the only pair with key 0
        EXPECT_EQ(0, res.size());
      } else {
        EXPECT_EQ(1, res.size());
        EXPECT_EQ(2 * i, res[0]);
      }
    }
    // delete all values
    for (int i = 0; i < 10000; i++) {
//      LOG_DEBUG("Delete: %d\n", i);
      if (i == 0) {
        // (0, 0) has been deleted
        EXPECT_FALSE(ht.Remove(nullptr, i, 2 * i));
      } else {
        EXPECT_TRUE(ht.Remove(nullptr, i, 2 * i));
      }
    }
    LOG_DEBUG("SHUT DOWN\n");
    disk_manager->ShutDown();
    remove("test.db");
    delete disk_manager;
    delete bpm;
  }

  TEST(HashTableTest, ResizeTest) {
    auto *disk_manager = new DiskManager("test.db");
    auto *bpm = new BufferPoolManager(50, disk_manager);

    LinearProbeHashTable<int, int, IntComparator> ht("blah", bpm, IntComparator(), 5, HashFunction<int>());

    // insert a few values
    for (int i = 0; i < 10000; i++) {
      ht.Insert(nullptr, i, i);
      std::vector<int> res;
      ht.GetValue(nullptr, i, &res);
      EXPECT_EQ(1, res.size()) << "Failed to insert " << i << std::endl;
      EXPECT_EQ(i, res[0]);
//      LOG_DEBUG("Inserted %d\n", i);
    }
    LOG_DEBUG("SHUT DOWN\n");
    disk_manager->ShutDown();
    remove("test.db");
    delete disk_manager;
    delete bpm;
  }

  void insert_f(LinearProbeHashTable<int, int, IntComparator>* ht, int start_i) {
    for (int i = start_i; i < 1000 + start_i; i++) {
      ht->Insert(nullptr, i, i);
      std::vector<int> res;
      ht->GetValue(nullptr, i, &res);
      EXPECT_EQ(1, res.size());
      EXPECT_EQ(i, res[0]);
    }
  }

  TEST(HashTableTest, ConcurrentInsertTest) {
    auto *disk_manager = new DiskManager("test.db");
    auto *bpm = new BufferPoolManager(50, disk_manager);

    LinearProbeHashTable<int, int, IntComparator> ht("blah", bpm, IntComparator(), 5, HashFunction<int>());
    std::thread t1(insert_f, &ht, 0);
    std::thread t2(insert_f, &ht, 5000);
    std::thread t3(insert_f, &ht, 10000);
    // insert a few values
    t1.join();
    t2.join();
    t3.join();

    LOG_DEBUG("SHUT DOWN\n");
    disk_manager->ShutDown();
    remove("test.db");
    delete disk_manager;
    delete bpm;
  }


}  // namespace bustub
