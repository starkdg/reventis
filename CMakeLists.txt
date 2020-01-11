cmake_minimum_required(VERSION 3.0)

project(reventis-redis)
include(ExternalProject)

set(MODULE_SRCS rbtree_redis_ndt.cpp spfc.cpp)
set(CMAKE_INSTALL_PREFIX /var/local)

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


install(TARGETS reventis DESTINATION lib)
