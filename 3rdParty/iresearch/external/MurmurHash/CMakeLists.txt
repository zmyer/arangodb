# set openfst directories
project(MurMurHash CXX)

add_library(murmurhash
  ./MurmurHash3.cpp
)
target_include_directories(murmurhash PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
