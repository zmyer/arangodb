cmake_minimum_required(VERSION 3.2 FATAL_ERROR)
project(IResearch CXX)
set(CMAKE_CXX_STANDARD 11)

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")
message(STATUS "module path: ${CMAKE_MODULE_PATH}")

include(IResearchDefineSourceFiles)
include(IResearchGenerateFiles)

## libraries provided by arangodb in final version
find_package(Boost COMPONENTS filesystem system locale thread REQUIRED)
find_package(ICU COMPONENTS data i18n io le lx test tu uc REQUIRED)
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
add_library(iresearch STATIC ${iresearch_sources})
add_dependencies(iresearch
  iresearch-build_identifier
  iresearch-build_version
)

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
    lz4_static
    snowball_static
    ${IResearch_extra_libs}
    Boost::filesystem
    Boost::system
    Boost::locale
    Boost::thread
    ${ICU_LIBRARIES}
)
################################################################################
### iresearch plugins
################################################################################

### analysis plugins
add_library(analyzer-text STATIC ./core/analysis/text_token_stream.cpp)
target_include_directories(analyzer-text PRIVATE "./core/analysis")
target_link_libraries(analyzer-text iresearch)


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
