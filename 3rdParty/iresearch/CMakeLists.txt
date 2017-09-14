cmake_minimum_required(VERSION 3.2 FATAL_ERROR)
project(IResearch CXX)
set(CMAKE_CXX_STANDARD 11)

## TODO
#  - select if libs and depends should be STATIC / SHARED (configurable)
#  - runtime should be handled by the project that includes iresearch
#  - add defines for debugging

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")
message(STATUS "module path: ${CMAKE_MODULE_PATH}")

if(NOT BUILD_SHARED_LIB)
    set(IRESEARCH_BUILD_LIB STATIC)
else()
    set(IRESEARCH_BUILD_LIB SHARED)
endif()

include(IResearchDefineSourceFiles)
include(IResearchGenerateFiles)

## libraries provided by arangodb in final version
if(IRESEARCH_IN_ARANGO)
    set(Boost_LIBRARIES boost_boost boost_filesystem boost_system boost_locale boost_thread)
    set(ICU_INCLUDES ${ICU_INCLUDE_DIR})
    set(ICU_LIBRARIES ${ICU_LIBS})
else()
    find_package(Boost COMPONENTS filesystem system locale thread REQUIRED)
    find_package(ICU COMPONENTS data i18n io le lx test tu uc REQUIRED)
endif()
message(STATUS "Using ICU tagets: ${ICU_LIBRARIES}")

## mandatroy libraries
set(PTHREAD_LIBRARY pthread)
find_package(Threads REQUIRED)
find_package(Snowball REQUIRED)
find_package(Lz4 REQUIRED)

## optional libraries
find_package(BFD)
if (BFD_FOUND)
  add_definitions(-DUSE_LIBBFD)
  list(APPEND IResearch_extra_libs ${bfd=something=something})
endif()

find_package(Unwind)
if (Unwind_FOUND)
  add_definitions(-DUSE_LIBUNWIND)
  list(APPEND IResearch_extra_libs ${Unwind_STATIC})
  message(STATUS "Using UNWIND tagets: ${Unwind_STATIC}")
endif()

## extra defines
if(WIN32)
    add_definitions(-DUNICODE -D_UNICODE)
endif()

## exteranl libraries requiered by iresearch
add_subdirectory(external)

## iresearch main library
add_library(iresearch ${IRESEARCH_BUILD_LIB} ${iresearch_sources})
add_dependencies(iresearch
  iresearch-build_identifier
  iresearch-build_version
)

if(IRESEARCH_IN_ARANGO)
    add_dependencies(iresearch v8_build)
endif()

target_include_directories(iresearch PUBLIC
    "${CMAKE_CURRENT_SOURCE_DIR}"
    "${CMAKE_CURRENT_SOURCE_DIR}/core"
    "${CMAKE_CURRENT_BINARY_DIR}/core"
    "${ICU_INCLUDES}"
)

target_link_libraries(iresearch PUBLIC
    openfst
    murmurhash
    cmdline
    lz4_static
    snowball_static
    ${IResearch_extra_libs}
    ${Boost_LIBRARIES}
    ${ICU_LIBRARIES}
)
################################################################################
### iresearch plugins
################################################################################

### analysis plugins
add_library(analyzer-text STATIC ./core/analysis/text_token_stream.cpp)
target_include_directories(analyzer-text PRIVATE "./core/analysis")
target_link_libraries(analyzer-text iresearch rapidjson)


### format plugins
add_library(format-1_0 STATIC
  ./core/formats/formats_10.cpp
  ./core/formats/formats_10_attributes.cpp
  ./core/formats/formats_burst_trie.cpp
)
target_include_directories(format-1_0 PUBLIC "./core/formats")
target_link_libraries(format-1_0 iresearch rapidjson)

### scorer plugin : TF-IDF
add_library(scorer-tfidf STATIC ./core/search/tfidf.cpp)
target_include_directories(format-1_0 PUBLIC "./core/search")
target_link_libraries(scorer-tfidf iresearch)

### scorer plugin : BM25
add_library(scorer-bm25 STATIC ./core/search/bm25.cpp)
target_include_directories(scorer-bm25 PUBLIC "./core/search")
target_link_libraries(scorer-bm25 iresearch)

## iresearch utils
#add_subdirectory(utils)
## iresearch tests
#add_subdirectory(tests)
## iresearch coverage
