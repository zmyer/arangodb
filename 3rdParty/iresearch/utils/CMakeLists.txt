if(NOT BUILD_SHARED_LIB)
    set(iresarch_uitls_extra_static 
        analyzer-text format-1_0 scorer-tfidf scorer-bm25
       )
endif()


################################################################################
### iresearch-util
################################################################################

add_executable(
    iresearch-index-util
  ./index-dump.cpp
  ./index-util.cpp
  ./main.cpp
)

target_link_libraries( iresearch-index-util iresearch
    ${iresarch_uitls_extra_static}
)


################################################################################
### iresearch-benchmarks
################################################################################


add_executable(
  iresearch-benchmarks
  ./index-put.cpp
  ./index-search.cpp
  ./index-benchmarks.cpp
  ./main.cpp
)

target_link_libraries(iresearch-benchmarks iresearch
    ${iresarch_uitls_extra_static}
)
