file(GLOB_RECURSE bustub_sources ${PROJECT_SOURCE_DIR}/src/*/*.cpp)

if(EXISTS ${PROJECT_SOURCE_DIR}/build_support/libp1_static.a)
list(REMOVE_ITEM bustub_sources "${PROJECT_SOURCE_DIR}/src/buffer/buffer_pool_manager.cpp" "${PROJECT_SOURCE_DIR}/src/buffer/clock_replacer.cpp")
endif()

add_library(bustub_shared SHARED ${bustub_sources})

if(EXISTS ${PROJECT_SOURCE_DIR}/build_support/libp1_static.a)
target_link_libraries(bustub_shared "${PROJECT_SOURCE_DIR}/build_support/libp1_static.a")
endif()

######################################################################################################################
# THIRD-PARTY SOURCES
######################################################################################################################

# murmur3
file(GLOB_RECURSE murmur3_sources
        ${PROJECT_SOURCE_DIR}/third_party/murmur3/*.cpp ${PROJECT_SOURCE_DIR}/third_party/murmur3/*.h)
add_library(thirdparty_murmur3 SHARED ${murmur3_sources})
target_link_libraries(bustub_shared thirdparty_murmur3)
