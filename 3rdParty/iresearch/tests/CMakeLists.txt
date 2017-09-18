set(iresearch_tests_sources
  ./analysis/analyzer_test.cpp
  ./analysis/token_stream_tests.cpp
  ./formats/formats_tests.cpp
  ./formats/skip_list_test.cpp
  ./store/directory_test_case.cpp
  ./store/directory_cleaner_tests.cpp
  ./store/fs_directory_tests.cpp
  ./store/memory_directory_tests.cpp
  ./store/memory_index_output_tests.cpp
  ./store/store_utils_tests.cpp
  ./index/doc_generator.cpp
  ./index/assert_format.cpp
  ./index/index_meta_tests.cpp
  ./index/index_tests.cpp
  ./index/field_meta_test.cpp
  ./index/merge_writer_tests.cpp
  ./index/postings_tests.cpp
  ./search/empty_filter_tests.cpp
  ./search/granular_range_filter_tests.cpp
  ./search/index_reader_test.cpp
  ./search/scorers_tests.cpp
  ./search/sort_tests.cpp
  ./search/tfidf_test.cpp
  ./search/bm25_test.cpp
  ./search/cost_attribute_test.cpp
  ./search/boost_attribute_test.cpp
  ./search/filter_test_case_base.cpp
  ./search/boolean_filter_tests.cpp
  ./search/all_filter_tests.cpp
  ./search/term_filter_tests.cpp
  ./search/prefix_filter_test.cpp
  ./search/range_filter_test.cpp
  ./search/phrase_filter_tests.cpp
  ./search/column_existence_filter_test.cpp
  ./search/same_position_filter_tests.cpp
  ./iql/parser_common_test.cpp
  ./iql/query_builder_test.cpp
  ./utils/async_utils_tests.cpp
  ./utils/container_utils_tests.cpp
  ./utils/map_utils_tests.cpp
  ./utils/object_pool_tests.cpp
  ./utils/numeric_utils_test.cpp
  ./utils/attributes_tests.cpp
  ./utils/directory_utils_tests.cpp
  ./utils/bit_packing_tests.cpp
  ./utils/bit_utils_tests.cpp
  ./utils/block_pool_test.cpp
  ./utils/locale_utils_tests.cpp
  ./utils/ref_counter_tests.cpp
  ./utils/memory_tests.cpp
  ./utils/string_tests.cpp
  ./utils/bitset_tests.cpp
  ./utils/ebo_tests.cpp
  ./utils/math_utils_test.cpp
  ./utils/std_test.cpp
  ./utils/type_utils_tests.cpp
  ./utils/utf8_path_tests.cpp
  ./tests_main.cpp
)

if(NOT BUILD_SHARED_LIB)
    list(APPEND iresearch_tests_sources
        ./analysis/text_analyzer_tests.cpp
        ./formats/formats_10_tests.cpp
        ./iql/parser_test.cpp
    )
endif()

if (MSVC)
  add_definitions(-D_SCL_SECURE_NO_WARNINGS)
endif()

# set test resource directory
set(IResearch_test_resource_dir
  ${CMAKE_CURRENT_SOURCE_DIR}/resources
)

configure_file(
  "${CMAKE_CURRENT_SOURCE_DIR}/tests_config.hpp.in"
  "${CMAKE_CURRENT_BINARY_DIR}/tests_config.hpp"
)

add_executable(iresearch-test ${iresearch_tests_sources})
target_include_directories(iresearch-test PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_CURRENT_BINARY_DIR}
)

target_link_libraries(iresearch-test
  ${GCOV_LIBRARY}
  iresearch
  rapidjson
  ${GTEST_BOTH_LIBRARIES}
  ${PTHREAD_LIBRARY}
  gtest
  gtest_main
)
