cmake_minimum_required(VERSION 3.16)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

project(sensekv)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_BUILD_TYPE "Debug")

set(LIBRARY_OUTPUT_PATH ${CMAKE_SOURCE_DIR}/lib)
set(EXECUTABLE_OUTPUT_PATH ${CMAKE_SOURCE_DIR}/bin)

find_package(glog 0.6.0 REQUIRED)

include_directories(${CMAKE_SOURCE_DIR}/sensekv/memory)
include_directories(${CMAKE_SOURCE_DIR}/sensekv/db)
include_directories(${CMAKE_SOURCE_DIR}/sensekv/utils)

link_directories(${CMAKE_SOURCE_DIR}/lib)

add_subdirectory(sensekv)
add_subdirectory(examples)
add_subdirectory(tests)