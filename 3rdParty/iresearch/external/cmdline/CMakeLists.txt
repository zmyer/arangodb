# set openfst directories
project(CmdLine CXX)
add_library(cmdline INTERFACE)
target_include_directories(cmdline INTERFACE ${CMAKE_CURRENT_SOURCE_DIR})
