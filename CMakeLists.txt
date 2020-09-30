cmake_minimum_required(VERSION 3.0)

project(Reventis VERSION 0.4.0 DESCRIPTION "spatio-temporal event index")
include(ExternalProject)

set(MODULE_SRCS module.cpp spfc.cpp rbtree.cpp haversine.cpp)
set(CMAKE_INSTALL_PREFIX /var/local)

set(CMAKE_BUILD_TYPE RelWithDebInfo)

add_library(reventis MODULE ${MODULE_SRCS})
set_target_properties(reventis PROPERTIES PREFIX "")
target_link_options(reventis PRIVATE "LINKER:-shared,-Bsymbolic")

find_package(Boost 1.67 COMPONENTS program_options)

if (Boost_FOUND)

  ExternalProject_Add(hiredis
	PREFIX ${CMAKE_CURRENT_BINARY_DIR}/hiredis
	URL https://github.com/redis/hiredis/archive/v1.0.0.tar.gz
	CONFIGURE_COMMAND ""
	BUILD_COMMAND echo make static
	BUILD_IN_SOURCE 1
	INSTALL_COMMAND make install PREFIX=<INSTALL_DIR>)

  set(HIREDIS_INCLUDE_DIR ${CMAKE_CURRENT_BINARY_DIR}/hiredis/include/hiredis)
  set(HIREDIS_LIB ${CMAKE_CURRENT_BINARY_DIR}/hiredis/lib/libhiredis.a)

  message("HIREDIS_INCLUDE_DIR = ${HIREDIS_INCLUDE_DIR}")
  message("HIREDIS_LIB = ${HIREDIS_LIB}")
  
  add_executable(loadevents loadevents.cpp)
  add_dependencies(loadevents hiredis)
  target_include_directories(loadevents PRIVATE ${Boost_INCLUDE_DIRS})
  target_link_libraries(loadevents ${HIREDIS_LIB}  ${Boost_LIBRARIES})
  set_target_properties(loadevents PROPERTIES INCLUDE_DIRECTORIES ${HIREDIS_INCLUDE_DIR})

  add_executable(queryevents queryevents.cpp)
  add_dependencies(queryevents hiredis)
  target_link_libraries(queryevents ${HIREDIS_LIB})
  set_target_properties(queryevents PROPERTIES INCLUDE_DIRECTORIES ${HIREDIS_INCLUDE_DIR})

  add_executable(loadgdelt loadgdelt.cpp)
  add_dependencies(loadgdelt hiredis)
  target_include_directories(loadgdelt PRIVATE ${Boost_INCLUDE_DIRS})
  target_link_libraries(loadgdelt ${HIREDIS_LIB} ${Boost_LIBRARIES})
  set_target_properties(loadgdelt PROPERTIES INCLUDE_DIRECTORIES ${HIREDIS_INCLUDE_DIR})
  
  add_executable(testreventis test_reventis.cpp reventisclient_util.cpp)
  add_dependencies(testreventis hiredis)
  target_link_libraries(testreventis ${HIREDIS_LIB})
  set_target_properties(testreventis PROPERTIES INCLUDE_DIRECTORIES ${HIREDIS_INCLUDE_DIR})

endif()
  
add_executable(testspfc test_spfc.cpp spfc.cpp)
add_executable(testhaversine test_haversine.cpp haversine.cpp)
target_link_libraries(testhaversine m)

install(TARGETS reventis DESTINATION lib)

enable_testing()
add_test(NAME TestSpfc COMMAND testspfc)
add_test(NAME TestReventis COMMAND testreventis)
add_test(NAME TestHaversine COMMAND testhaversine)

set(CPACK_PACKAGE_DESCRIPTION_SUMMARY "Event GeoSpatial Temporal Indexing System")
set(CPACK_PACKAGE_VENDOR "Aetilius")
set(CPACK_PACKAGE_VERSION_MAJOR "0")
set(CPACK_PACKAGE_VERSION_MINOR "4")
set(CPACK_PACKAGE_VERSION_PATCH "0")
set(CPACK_GENERATOR "TGZ")
include(CPack)


