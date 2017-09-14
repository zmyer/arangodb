cmake_minimum_required(VERSION 3.2 FATAL_ERROR)
project(iresearch)

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")
message(STATUS "module path: ${CMAKE_MODULE_PATH}")
set(CMAKE_CXX_STANDARD 11)

# set version components
if (NOT IResearch_version_major)
  set(IResearch_version_major "1")
endif()
if (NOT IResearch_version_minor)
  set(IResearch_version_minor "0")
endif()
if (NOT IResearch_version_revision)
  set(IResearch_version_revision "0")
endif()
if (NOT IResearch_version_patch)
  set(IResearch_version_patch "0")
endif()

# calculate version
math(EXPR IResearch_int_version "(${IResearch_version_major} * 1000000) + (${IResearch_version_minor} * 10000) + (${IResearch_version_revision} * 100) + (${IResearch_version_patch} * 1)" )
set(IResearch_version "${IResearch_version_major}.${IResearch_version_minor}.${IResearch_version_revision}.${IResearch_version_patch}")

# conains openfst murmurhash openfst commandline
add_subdirectory(external)

# generate version_defines.hpp
configure_file(
  "${CMAKE_CURRENT_SOURCE_DIR}/core/utils/version_defines.template.hpp"
  "${CMAKE_CURRENT_BINARY_DIR}/core/utils/version_defines.hpp"
)
# Find includes in corresponding build directories
set(CMAKE_INCLUDE_CURRENT_DIR ON)

# compiler version: ${CMAKE_CXX_COMPILER_VERSION}

# boost provided by arangodb
find_package(Boost COMPONENTS filesystem system locale thread REQUIRED)
# icu must be provided by arangodb

set(PTHREAD_LIBRARY pthread)
find_package(Threads REQUIRED)
#set(icu_roots "/usr" "/usr/local" "/opt" "/usr/lib/x86_64-linux-gnu/")
#set(ICU_CXX_LIBRARY  "/usr/lib/x86_64-linux-gnu/")
find_package(ICU COMPONENTS data i18n io le lx test tu uc REQUIRED)

find_package(BFD)
if (BFD_FOUND)
  add_definitions(-DUSE_LIBBFD)
endif()

find_package(Snowball REQUIRED)
find_package(Lz4 REQUIRED)
 
set(IResearch_extra_libs lz4_static snowball_static)

find_package(Unwind)
if (Unwind_FOUND)
  add_definitions(-DUSE_LIBUNWIND)
  list(APPEND IResearch_extra_libs unwind_static)
endif()


if(WIN32)
    add_definitions(-DUNICODE -D_UNICODE)
endif()

set(iresearch_sources
  ./core/utils/string.cpp
  ./core/analysis/analyzer.cpp
  ./core/analysis/analyzers.cpp
  ./core/analysis/token_attributes.cpp
  ./core/analysis/token_stream.cpp
  ./core/analysis/token_streams.cpp
  ./core/document/field.cpp
  ./core/error/error.cpp
  ./core/formats/formats.cpp
  ./core/formats/format_utils.cpp
  ./core/formats/skip_list.cpp
  ./core/index/directory_reader.cpp
  ./core/index/field_data.cpp
  ./core/index/field_meta.cpp
  ./core/index/file_names.cpp
  ./core/index/index_meta.cpp
  ./core/index/index_writer.cpp
  ./core/index/index_reader.cpp
  ./core/index/iterators.cpp
  ./core/index/merge_writer.cpp
  ./core/index/postings.cpp
  ./core/index/segment_reader.cpp
  ./core/index/segment_writer.cpp
  ./core/iql/parser.cc
  ./core/iql/parser_common.cpp
  ./core/iql/parser_context.cpp
  ./core/iql/query_builder.cpp
  ./core/search/all_filter.cpp
  ./core/search/granular_range_filter.cpp
  ./core/search/scorers.cpp
  ./core/search/sort.cpp
  ./core/search/cost.cpp
  ./core/search/score_doc_iterators.cpp
  ./core/search/filter.cpp
  ./core/search/term_filter.cpp
  ./core/search/prefix_filter.cpp
  ./core/search/range_filter.cpp
  ./core/search/phrase_filter.cpp
  ./core/search/column_existence_filter.cpp
  ./core/search/same_position_filter.cpp
  ./core/search/range_query.cpp
  ./core/search/term_query.cpp
  ./core/search/boolean_filter.cpp
  ./core/store/data_input.cpp
  ./core/store/data_output.cpp
  ./core/store/directory.cpp
  ./core/store/directory_attributes.cpp
  ./core/store/directory_cleaner.cpp
  ./core/store/fs_directory.cpp
  ./core/store/memory_directory.cpp
  ./core/store/store_utils.cpp
  ./core/utils/async_utils.cpp
  ./core/utils/attributes.cpp
  ./core/utils/bit_packing.cpp
  ./core/utils/compression.cpp
  ./core/utils/directory_utils.cpp
  ./core/utils/file_utils.cpp
  ./core/utils/hash_utils.cpp
  ./core/utils/index_utils.cpp
  ./core/utils/math_utils.cpp
  ./core/utils/memory.cpp
  ./core/utils/version_utils.cpp
  ./core/utils/utf8_path.cpp
  ./core/utils/locale_utils.cpp
  ./core/utils/log.cpp
  ./core/utils/unicode_utils.cpp
  ./core/utils/so_utils.cpp
  ./core/utils/process_utils.cpp
  ./core/utils/network_utils.cpp
  ./core/utils/cpuinfo.cpp
  ./core/utils/numeric_utils.cpp
)

# Find includes in corresponding build directories
# generate version_defines.hpp
configure_file(
  "${CMAKE_CURRENT_SOURCE_DIR}/core/utils/version_defines.template.hpp" 
  "${CMAKE_CURRENT_BINARY_DIR}/core/utils/version_defines.hpp"
)

add_custom_command(
  OUTPUT core/iql/parser.cc
  MAIN_DEPENDENCY ${CMAKE_CURRENT_SOURCE_DIR}/core/iql/parser.yy
  DEPENDS core/iql ${CMAKE_CURRENT_SOURCE_DIR}/core/iql/parser.yy
  COMMAND bison --graph --report=all -o parser.cc ${CMAKE_CURRENT_SOURCE_DIR}/core/iql/parser.yy
  WORKING_DIRECTORY core/iql
)

add_custom_command(
  OUTPUT core/iql
  COMMAND ${CMAKE_COMMAND} -E make_directory core/iql
)

add_library(iresearch STATIC ${iresearch_sources})
target_include_directories(iresearch PUBLIC
    "${CMAKE_CURRENT_SOURCE_DIR}"
    "${CMAKE_CURRENT_SOURCE_DIR}/core"
    "${CMAKE_CURRENT_BINARY_DIR}/core"
)
target_link_libraries(iresearch PUBLIC
    openfst
    rapidjson
    murmurhash
    cmdline
    ${IResearch_extra_libs}
    Boost::filesystem
    Boost::system
    Boost::locale
    Boost::thread
    icu_static
)

#add_subdirectory(core)
#add_subdirectory(tests)
#add_subdirectory(utils)
