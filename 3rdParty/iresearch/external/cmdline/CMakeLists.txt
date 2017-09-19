# set openfst directories
project(CmdLine CXX)
add_library(cmdline INTERFACE)
target_include_directories(cmdline SYSTEM INTERFACE ${CMAKE_CURRENT_SOURCE_DIR})
