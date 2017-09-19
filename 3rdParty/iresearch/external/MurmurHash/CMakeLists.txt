# set openfst directories
project(MurMurHash CXX)

add_library(murmurhash STATIC
  ./MurmurHash3.cpp
)
target_include_directories(murmurhash SYSTEM PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
