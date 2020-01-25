cmake_minimum_required(VERSION 3.0)

project(Reventis)
include(ExternalProject)

set(MODULE_SRCS rbtree_redis.cpp spfc.cpp)
set(CMAKE_INSTALL_PREFIX /var/local)

set(CMAKE_BUILD_TYPE Debug)

add_library(reventis MODULE ${MODULE_SRCS})
set_target_properties(reventis PROPERTIES PREFIX "")
target_link_options(reventis PRIVATE "LINKER:-shared,-Bsymbolic")

set(HIREDIS_DIR ${CMAKE_CURRENT_SOURCE_DIR}/hiredis)
set(HIREDIS_INCLUDE_DIRS ${HIREDIS_DIR})
ExternalProject_Add(hiredis
  PREFIX ${CMAKE_CURRENT_SOURCE_DIR}/hiredis
  SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/hiredis
  BUILD_IN_SOURCE 1
  CONFIGURE_COMMAND echo configure
  BUILD_COMMAND make static
  INSTALL_COMMAND echo install)

add_executable(loadevents loaddata.cpp)
target_link_libraries(loadevents ${HIREDIS_DIR}/libhiredis.a)
set_target_properties(loadevents PROPERTIES INCLUDE_DIRECTORIES ${HIREDIS_INCLUDE_DIRS})

add_executable(gendata generatedata.cpp)
target_link_libraries(gendata ${HIREDIS_DIR}/libhiredis.a)
set_target_properties(gendata PROPERTIES INCLUDE_DIRECTORIES ${HIREDIS_INCLUDE_DIRS})

add_executable(testquery test_query.cpp)
target_link_libraries(testquery ${HIREDIS_DIR}/libhiredis.a)
set_target_properties(testquery PROPERTIES INCLUDE_DIRECTORIES ${HIREDIS_INCLUDE_DIRS})

install(TARGETS reventis DESTINATION lib)


set(CPACK_PACKAGE_DESCRIPTION_SUMMARY "Event Indexing System")
set(CPACK_PACKAGE_VENDOR "Aetilius")
set(CPACK_PACKAGE_VERSION_MAJOR "0")
set(CPACK_PACKAGE_VERSION_MINOR "1")
set(CPACK_PACKAGE_VERSION_PATCH "0")
set(CPACK_GENERATOR "TGZ")
include(CPack)


