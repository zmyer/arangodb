project(lz4)
add_library(lz4 STATIC
    lib/lz4.c
    lib/lz4frame.c
    lib/lz4hc.c
    lib/xxhash.c
)
target_include_directories(lz4 PUBLIC lib)
