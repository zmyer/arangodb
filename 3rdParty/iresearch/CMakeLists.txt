cmake_minimum_required(VERSION 3.2 FATAL_ERROR)
project(iresearch)

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

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")
message(STATUS "module path: ${CMAKE_MODULE_PATH}")
include(IResearchDefineSourceFiles)
include(IResearchGenerateFiles)
# compiler version: ${CMAKE_CXX_COMPILER_VERSION}

## libraries provided by arangodb in final version
# boost provided by arangodb
find_package(Boost COMPONENTS filesystem system locale thread REQUIRED)
# icu must be provided by arangodb
find_package(ICU COMPONENTS data i18n io le lx test tu uc REQUIRED)
message(STATUS "Using ICU tagets: ${ICU_LIBRARIES}")

## mandatroy libraries
set(PTHREAD_LIBRARY pthread)
find_package(Threads REQUIRED)
find_package(Snowball REQUIRED)
find_package(Lz4 REQUIRED)
set(IResearch_extra_libs lz4_static snowball_static)

## optional libraries
find_package(BFD)
if (BFD_FOUND)
  add_definitions(-DUSE_LIBBFD)
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
    ${IResearch_extra_libs}
    Boost::filesystem
    Boost::system
    Boost::locale
    Boost::thread
    ${ICU_LIBRARIES}
)
## iresearch plugins

## iresearch utils
#add_subdirectory(utils)
## iresearch tests
#add_subdirectory(tests)
## iresearch coverage
