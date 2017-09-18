cmake_minimum_required(VERSION 3.2 FATAL_ERROR)
project(IResearch CXX)
set(CMAKE_CXX_STANDARD 11)

## TODO
#  - select if libs and depends should be STATIC / SHARED (configurable)
#  - runtime should be handled by the project that includes iresearch
#  - add defines for debugging
#  - add tests
#  - add coverage
#  - provide new findboost
#  - update build containers

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")
message(STATUS "module path: ${CMAKE_MODULE_PATH}")

include(IResearchDefineSourceFiles)
include(IResearchGenerateFiles)

if(NOT BUILD_SHARED_LIB)
    set(IRESEARCH_BUILD_LIB STATIC)
    list(APPEND iresearch_sources ${iresearch_plugin_sources})
else()
    set(IRESEARCH_BUILD_LIB SHARED)
endif()

###############################################################################
### extra defines
################################################################################
if(WIN32)
    add_definitions(-DUNICODE -D_UNICODE)
endif()

set(CMAKE_POSITION_INDEPENDENT_CODE TRUE)

################################################################################
### mandatory libraries
################################################################################
## exteranl libraries requiered by iresearch
add_subdirectory(external)

if(IRESEARCH_IN_ARANGO)
    set(Boost_LIBRARIES boost_boost boost_filesystem boost_system boost_locale boost_thread)
    set(ICU_INCLUDES ${ICU_INCLUDE_DIR})
    set(ICU_LIBRARIES ${ICU_LIBS})
    set(DO_NOT_SYSTEM_LIBS ON)
else()
    find_package(Boost COMPONENTS filesystem system locale thread REQUIRED)
    find_package(ICU COMPONENTS data i18n io le lx test tu uc REQUIRED)
endif()
message(STATUS "Using ICU tagets: ${ICU_LIBRARIES}")

set(PTHREAD_LIBRARY pthread)
find_package(Threads REQUIRED)

################################################################################
### optional libraries
################################################################################
# it is possible that the libs are not build PIC what will keep us form linking
# static code into shared objects
if(NOT DO_NOT_SYSTEM_LIBS)
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
endif()

################################################################################
### iresearch main library
################################################################################
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
    #lz4_static
    lz4
    #snowball_static
    stemmer
    rapidjson
    ${IResearch_extra_libs}
    ${Boost_LIBRARIES}
    ${ICU_LIBRARIES}
)
################################################################################
### iresearch plugins
################################################################################

# TODO - plugins need to become shared as soon as they libs they depend on are PIC

### analysis plugins
add_library(analyzer-text STATIC ${analyzer-text_sources})
target_include_directories(analyzer-text PRIVATE "./core/analysis")
target_link_libraries(analyzer-text iresearch rapidjson)


### format plugins
add_library(format-1_0 STATIC ${formats_10_sources})
target_include_directories(format-1_0 PUBLIC "./core/formats")
target_link_libraries(format-1_0 iresearch rapidjson)

### scorer plugin : TF-IDF
add_library(scorer-tfidf STATIC ${scorer-tfidf_sources})
target_include_directories(format-1_0 PUBLIC "./core/search")
target_link_libraries(scorer-tfidf iresearch)

### scorer plugin : BM25
add_library(scorer-bm25 STATIC ${scorer-bm25_sources})
target_include_directories(scorer-bm25 PUBLIC "./core/search")
target_link_libraries(scorer-bm25 iresearch)

## iresearch utils
#add_subdirectory(utils)

## iresearch tests
#add_subdirectory(tests)

## iresearch coverage
