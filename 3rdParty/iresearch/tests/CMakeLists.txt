# set test resource directory
set(IResearch_test_resource_dir
  ${CMAKE_CURRENT_SOURCE_DIR}/resources
  CACHE INTERNAL "iresearch test resources"
)

configure_file(
  "${CMAKE_CURRENT_SOURCE_DIR}/tests_config.hpp.in"
  "${CMAKE_CURRENT_BINARY_DIR}/tests_config.hpp"
)

#used in arangodb tests
if(IRESEARCH_TESTS)
    set(iresearch_tests_sources
      ${CMAKE_CURRENT_SOURCE_DIR}/analysis/analyzer_test.cpp
      ${CMAKE_CURRENT_SOURCE_DIR}/analysis/token_stream_tests.cpp
      ${CMAKE_CURRENT_SOURCE_DIR}/formats/formats_tests.cpp
      ${CMAKE_CURRENT_SOURCE_DIR}/formats/skip_list_test.cpp
      ${CMAKE_CURRENT_SOURCE_DIR}/store/directory_test_case.cpp
      ${CMAKE_CURRENT_SOURCE_DIR}/store/directory_cleaner_tests.cpp
      ${CMAKE_CURRENT_SOURCE_DIR}/store/fs_directory_tests.cpp
      ${CMAKE_CURRENT_SOURCE_DIR}/store/memory_directory_tests.cpp
      ${CMAKE_CURRENT_SOURCE_DIR}/store/memory_index_output_tests.cpp
      ${CMAKE_CURRENT_SOURCE_DIR}/store/store_utils_tests.cpp
      ${CMAKE_CURRENT_SOURCE_DIR}/index/doc_generator.cpp
      ${CMAKE_CURRENT_SOURCE_DIR}/index/assert_format.cpp
      ${CMAKE_CURRENT_SOURCE_DIR}/index/index_meta_tests.cpp
      ${CMAKE_CURRENT_SOURCE_DIR}/index/index_tests.cpp
      ${CMAKE_CURRENT_SOURCE_DIR}/index/field_meta_test.cpp
      ${CMAKE_CURRENT_SOURCE_DIR}/index/merge_writer_tests.cpp
      ${CMAKE_CURRENT_SOURCE_DIR}/index/postings_tests.cpp
      ${CMAKE_CURRENT_SOURCE_DIR}/search/empty_filter_tests.cpp
      ${CMAKE_CURRENT_SOURCE_DIR}/search/granular_range_filter_tests.cpp
      ${CMAKE_CURRENT_SOURCE_DIR}/search/index_reader_test.cpp
      ${CMAKE_CURRENT_SOURCE_DIR}/search/scorers_tests.cpp
      ${CMAKE_CURRENT_SOURCE_DIR}/search/sort_tests.cpp
      ${CMAKE_CURRENT_SOURCE_DIR}/search/tfidf_test.cpp
      ${CMAKE_CURRENT_SOURCE_DIR}/search/bm25_test.cpp
      ${CMAKE_CURRENT_SOURCE_DIR}/search/cost_attribute_test.cpp
      ${CMAKE_CURRENT_SOURCE_DIR}/search/boost_attribute_test.cpp
      ${CMAKE_CURRENT_SOURCE_DIR}/search/filter_test_case_base.cpp
      ${CMAKE_CURRENT_SOURCE_DIR}/search/boolean_filter_tests.cpp
      ${CMAKE_CURRENT_SOURCE_DIR}/search/all_filter_tests.cpp
      ${CMAKE_CURRENT_SOURCE_DIR}/search/term_filter_tests.cpp
      ${CMAKE_CURRENT_SOURCE_DIR}/search/prefix_filter_test.cpp
      ${CMAKE_CURRENT_SOURCE_DIR}/search/range_filter_test.cpp
      ${CMAKE_CURRENT_SOURCE_DIR}/search/phrase_filter_tests.cpp
      ${CMAKE_CURRENT_SOURCE_DIR}/search/column_existence_filter_test.cpp
      ${CMAKE_CURRENT_SOURCE_DIR}/search/same_position_filter_tests.cpp
      ${CMAKE_CURRENT_SOURCE_DIR}/iql/parser_common_test.cpp
      ${CMAKE_CURRENT_SOURCE_DIR}/iql/query_builder_test.cpp
      ${CMAKE_CURRENT_SOURCE_DIR}/utils/async_utils_tests.cpp
      ${CMAKE_CURRENT_SOURCE_DIR}/utils/container_utils_tests.cpp
      ${CMAKE_CURRENT_SOURCE_DIR}/utils/map_utils_tests.cpp
      ${CMAKE_CURRENT_SOURCE_DIR}/utils/object_pool_tests.cpp
      ${CMAKE_CURRENT_SOURCE_DIR}/utils/numeric_utils_test.cpp
      ${CMAKE_CURRENT_SOURCE_DIR}/utils/attributes_tests.cpp
      ${CMAKE_CURRENT_SOURCE_DIR}/utils/directory_utils_tests.cpp
      ${CMAKE_CURRENT_SOURCE_DIR}/utils/bit_packing_tests.cpp
      ${CMAKE_CURRENT_SOURCE_DIR}/utils/bit_utils_tests.cpp
      ${CMAKE_CURRENT_SOURCE_DIR}/utils/block_pool_test.cpp
      ${CMAKE_CURRENT_SOURCE_DIR}/utils/locale_utils_tests.cpp
      ${CMAKE_CURRENT_SOURCE_DIR}/utils/ref_counter_tests.cpp
      ${CMAKE_CURRENT_SOURCE_DIR}/utils/memory_tests.cpp
      ${CMAKE_CURRENT_SOURCE_DIR}/utils/string_tests.cpp
      ${CMAKE_CURRENT_SOURCE_DIR}/utils/bitset_tests.cpp
      ${CMAKE_CURRENT_SOURCE_DIR}/utils/ebo_tests.cpp
      ${CMAKE_CURRENT_SOURCE_DIR}/utils/math_utils_test.cpp
      ${CMAKE_CURRENT_SOURCE_DIR}/utils/std_test.cpp
      ${CMAKE_CURRENT_SOURCE_DIR}/utils/type_utils_tests.cpp
      ${CMAKE_CURRENT_SOURCE_DIR}/utils/utf8_path_tests.cpp
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


    add_executable(iresearch-test
        ${iresearch_tests_sources}
        ./tests_main.cpp
    )

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
endif()
