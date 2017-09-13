project(RapidJson CXX)
add_library(rapidsjon INTERFACE)
target_include_directories(rapidjson INTERFACE ${CMAKE_CURRENT_SOURCE_DIR}/rapidsjon)
