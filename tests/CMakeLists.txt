set(SRC_LIST skiplistTest.cc)
add_executable(skiplistTest ${SRC_LIST})
target_link_libraries(skiplistTest sensekv)