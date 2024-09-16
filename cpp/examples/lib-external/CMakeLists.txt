
cmake_minimum_required(VERSION 3.16)

add_library(stm SHARED)

option(EXSTM_READ "Reader" OFF)
option(EXSTM_ENCODE "Reader" OFF)
option(EXSTM_RE "Reader" OFF)


if(EXSTM_READ)
add_subdirectory(encode)
endif()


if(EXSTM_ENCODE)
add_subdirectory(read)
endif()

if(EXSTM_RE)
add_subdirectory(read)
add_subdirectory(encode)
add_subdirectory(re)
endif()