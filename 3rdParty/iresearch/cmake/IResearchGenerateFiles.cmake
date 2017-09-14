# generate version_defines.hpp ################################################
math(EXPR IResearch_int_version "(${IResearch_version_major} * 1000000) + (${IResearch_version_minor} * 10000) + (${IResearch_version_revision} * 100) + (${IResearch_version_patch} * 1)" )
set(IResearch_version "${IResearch_version_major}.${IResearch_version_minor}.${IResearch_version_revision}.${IResearch_version_patch}")
configure_file(
  "${CMAKE_CURRENT_SOURCE_DIR}/core/utils/version_defines.template.hpp" 
  "${CMAKE_CURRENT_BINARY_DIR}/core/utils/version_defines.hpp"
)

## generate parser ############################################################
add_custom_command(
  OUTPUT core/iql/parser.cc
  MAIN_DEPENDENCY ${CMAKE_CURRENT_SOURCE_DIR}/core/iql/parser.yy
  DEPENDS core/iql ${CMAKE_CURRENT_SOURCE_DIR}/core/iql/parser.yy
  COMMAND bison --graph --report=all -o parser.cc ${CMAKE_CURRENT_SOURCE_DIR}/core/iql/parser.yy
  WORKING_DIRECTORY core/iql
)

add_custom_command(
  OUTPUT core/iql
  COMMAND ${CMAKE_COMMAND} -E make_directory core/iql
)

## trigger generation of BUILD_IDENTIFIER if needed ###########################
add_custom_command(
  OUTPUT ${CMAKE_BINARY_DIR}/BUILD_IDENTIFIER 
  COMMAND ${CMAKE_COMMAND} -E touch ${CMAKE_BINARY_DIR}/BUILD_IDENTIFIER
  DEPENDS utils
  WORKING_DIRECTORY utils
)

# trigger regeneration of utils/version_utils.cpp with build_id from file: BUILD_IDENTIFIER
add_custom_command(
  OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/utils/build_identifier.csx
  COMMAND ${CMAKE_COMMAND} -DSRC="${CMAKE_BINARY_DIR}/BUILD_IDENTIFIER" -DDST="${CMAKE_CURRENT_BINARY_DIR}/utils/build_identifier.csx" -P "${PROJECT_SOURCE_DIR}/cmake/HexEncodeFile.cmake"
  COMMAND ${CMAKE_COMMAND} -E touch_nocreate ${CMAKE_CURRENT_SOURCE_DIR}/version_utils.cpp
  DEPENDS utils ${CMAKE_BINARY_DIR}/BUILD_IDENTIFIER
  WORKING_DIRECTORY utils
)

add_custom_target(
  ${IResearch_TARGET_NAME}-build_identifier
  DEPENDS ${CMAKE_CURRENT_BINARY_DIR}/utils/build_identifier.csx
)

add_custom_command(
  OUTPUT ${CMAKE_BINARY_DIR}/BUILD_VERSION
  COMMAND ${CMAKE_COMMAND} -E echo_append "${IResearch_version}" > ${CMAKE_BINARY_DIR}/BUILD_VERSION
  COMMAND ${CMAKE_COMMAND} -DSRC="${CMAKE_BINARY_DIR}/BUILD_VERSION" -DDST="${CMAKE_CURRENT_BINARY_DIR}/utils/build_version.csx" -P "${PROJECT_SOURCE_DIR}/cmake/HexEncodeFile.cmake"
  COMMAND ${CMAKE_COMMAND} -E touch_nocreate ${CMAKE_CURRENT_SOURCE_DIR}/version_utils.cpp
  DEPENDS utils
  WORKING_DIRECTORY utils
)

add_custom_target(
  ${IResearch_TARGET_NAME}-build_version
  DEPENDS ${CMAKE_BINARY_DIR}/BUILD_VERSION
)
##
