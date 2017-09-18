# set openfst directories
project(OpenFST CXX)

add_library(openfst STATIC
  ./compat.cc
  ./flags.cc
  ./fst.cc
  ./mapped-file.cc
  ./properties.cc
  ./symbol-table.cc
  ./symbol-table-ops.cc
  ./util.cc
)

target_include_directories(openfst PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
