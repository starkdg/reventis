cmake_minimum_required(VERSION 3.0)

project(Reventis VERSION 0.3.0 DESCRIPTION "spatio-temporal event index")
include(ExternalProject)

set(MODULE_SRCS module.cpp spfc.cpp rbtree.cpp haversine.cpp)
set(CMAKE_INSTALL_PREFIX /var/local)

set(CMAKE_BUILD_TYPE DEBUG)

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

add_executable(loadevents loadevents.cpp)
target_link_libraries(loadevents ${HIREDIS_DIR}/libhiredis.a)
set_target_properties(loadevents PROPERTIES INCLUDE_DIRECTORIES ${HIREDIS_INCLUDE_DIRS})

add_executable(queryevents queryevents.cpp)
target_link_libraries(queryevents ${HIREDIS_DIR}/libhiredis.a)
set_target_properties(queryevents PROPERTIES INCLUDE_DIRECTORIES ${HIREDIS_INCLUDE_DIRS})

add_executable(loadgdelt loadgdelt.cpp)
target_link_libraries(loadgdelt ${HIREDIS_DIR}/libhiredis.a)
set_target_properties(loadgdelt PROPERTIES INCLUDE_DIRECTORIES ${HIREDIS_INCLUDE_DIRS})

add_executable(testreventis test_reventis.cpp reventisclient_util.cpp)
target_link_libraries(testreventis ${HIREDIS_DIR}/libhiredis.a)
set_target_properties(testreventis PROPERTIES INCLUDE_DIRECTORIES ${HIREDIS_INCLUDE_DIRS})

add_executable(testspfc test_spfc.cpp spfc.cpp)
add_executable(testhaversine test_haversine.cpp haversine.cpp)
target_link_libraries(testhaversine m)

install(TARGETS reventis DESTINATION lib)

enable_testing()
add_test(NAME TestSpfc COMMAND testspfc)
add_test(NAME TestReventis COMMAND testreventis)
add_test(NAME TestHaversine COMMAND testhaversine)

set(CPACK_PACKAGE_DESCRIPTION_SUMMARY "Event Indexing System")
set(CPACK_PACKAGE_VENDOR "Aetilius")
set(CPACK_PACKAGE_VERSION_MAJOR "0")
set(CPACK_PACKAGE_VERSION_MINOR "3")
set(CPACK_PACKAGE_VERSION_PATCH "0")
set(CPACK_GENERATOR "TGZ")
include(CPack)


