cmake_minimum_required(VERSION 3.2 FATAL_ERROR)
project(IResearch CXX)
set(CMAKE_CXX_STANDARD 11)

option(IRESEARCH_TESTS ON)
option(IRESEARCH_COVERAGE ON)
option(IRESEARCH_UTILS ON)
option(DO_NOT_USE_SYSTEM_LIBS OFF)

## TODO
#  - select if libs and depends should be STATIC / SHARED (configurable)
#  - runtime should be handled by the project that includes iresearch
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
    add_definitions(-D_SCL_SECURE_NO_WARNINGS)
endif()

set(CMAKE_POSITION_INDEPENDENT_CODE TRUE)

################################################################################
### mandatory libraries
################################################################################
if(IRESEARCH_IN_ARANGO)
    message(STATUS "-- building iresearch as part of ArangoDB")
    message(STATUS "-- Using Arango boost and icu")
    set(Boost_LIBRARIES boost_boost boost_filesystem boost_system boost_locale boost_thread)
    set(ICU_INCLUDES ${ICU_INCLUDE_DIR})
    set(ICU_LIBRARIES ${ICU_LIBS})
    set(DO_NOT_USE_SYSTEM_LIBS ON)
elseif(NOT DO_NOT_USE_SYSTEM_LIBS)
    message(STATUS "-- building iresearch standalone")
    message(STATUS "-- Using SYSTEM boost and icu")
    find_package(Boost COMPONENTS filesystem system locale thread REQUIRED)
    find_package(ICU COMPONENTS data i18n io le lx test tu uc REQUIRED)
    set(BUILD_SHARED_LIB ON)
    message(STATUS "-- forcing shared build as system libs are not compiled pic")
else()
    # add configuration for self PIC-build icu and boost 
    message(FATAL_ERROR "no boost and icu lib specified")
endif()
message(STATUS "Using ICU tagets: ${ICU_LIBRARIES}")

set(PTHREAD_LIBRARY pthread)
find_package(Threads REQUIRED)

## exteranl libraries requiered by iresearch
add_subdirectory(external)

################################################################################
### optional libraries
################################################################################
# it is possible that the libs are not build PIC what will keep us form linking
# static code into shared objects
if(NOT DO_NOT_USE_SYSTEM_LIBS)
    find_package(BFD)
    if (BFD_FOUND)
      add_definitions(-DUSE_LIBBFD)
      list(APPEND IResearch_extra_libs ${BFD_STATIC})
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
    message(status "-- adding dependency to v8_build")
    add_dependencies(iresearch v8_build)
endif()

target_include_directories(iresearch PRIVATE SYSTEM
    "${ICU_INCLUDES}"
) 
target_include_directories(iresearch PUBLIC
    "${CMAKE_CURRENT_SOURCE_DIR}"
    "${CMAKE_CURRENT_SOURCE_DIR}/core"
    "${CMAKE_CURRENT_BINARY_DIR}/core"
)

target_compile_definitions(iresearch PUBLIC
    $<$<CONFIG:Debug>:IRESEARCH_DEBUG>
    $<$<STREQUAL:$<TARGET_PROPERTY:TYPE>,SHARED_LIBRARY>:IRESEARCH_DLL;IRESEARCH_DLL_EXPORTS>
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
    ${CMAKE_DL_LIBS}
)
################################################################################
### iresearch plugins
################################################################################

# TODO - plugins need to become shared as soon as they libs they depend on are PIC

### analysis plugins
# we need to build icu PIC
add_library(analyzer-text
    EXCLUDE_FROM_ALL
    SHARED
    ${analyzer-text_sources}
)
target_include_directories(analyzer-text PRIVATE "./core/analysis")
target_link_libraries(analyzer-text PRIVATE iresearch PUBLIC rapidjson)

### format plugins
add_library(format-1_0
    EXCLUDE_FROM_ALL
    SHARED
    ${format-1_0_sources}
)
target_include_directories(format-1_0 PRIVATE "./core/formats")
target_link_libraries(format-1_0 PRIVATE iresearch PUBLIC rapidjson)

### scorer plugin : TF-IDF
add_library(scorer-tfidf
    EXCLUDE_FROM_ALL
    SHARED
    ${scorer-tfidf_sources}
)
target_include_directories(scorer-tfidf PRIVATE "./core/search")
target_link_libraries(scorer-tfidf PRIVATE iresearch)

### scorer plugin : BM25
add_library(scorer-bm25
    EXCLUDE_FROM_ALL
    SHARED
    ${scorer-bm25_sources}
)
target_include_directories(scorer-bm25 PRIVATE "./core/search")
target_link_libraries(scorer-bm25 PRIVATE iresearch)

################################################################################
### utils
################################################################################
if(IRESEARCH_UTILS)
    add_subdirectory(utils)
endif()

################################################################################
### tests
################################################################################
add_subdirectory(tests)

################################################################################
### coverage
################################################################################
if (IRESEARCH_TESTS AND IRESEARCH_COVERAGE AND NOT MSVC
    AND (CMAKE_BUILD_TYPE STREQUAL "Coverage")
    AND (TARGET iresearch-test)
   )
  include(CodeCoverage REQUIRED)

  # exclude directories from coverage report
  SET(LCOV_EXCLUDE "external/*;boost/*")

  setup_target_for_coverage(
    iresearch-coverage
    $<TARGET_FILE:iresearch-test>
    coverage
    "--ires_output"
  )

  add_dependencies(iresearch-coverage iresearch-test)

  if (PYTHON_EXECUTABLE AND GCOVR_PATH)
    # exclude directories from coverage report
    SET(GCOVR_EXTRA_ARGS "-e;${PROJECT_SOURCE_DIR}/external/")

    setup_target_for_coverage_cobertura(
      iresearch-coverage-cobertura
      $<TARGET_FILE:iresearch-test>
      coverage
      "--ires_output"
    )
    add_dependencies(iresearch-coverage-cobertura iresearch-test)
  endif()
endif()
