# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

cmake_minimum_required(VERSION 3.16)

# Use Ninja generator if available
# if(NOT CMAKE_GENERATOR)
#   find_program(NINJA_EXECUTABLE ninja)
#   if(NINJA_EXECUTABLE)
#     set(CMAKE_GENERATOR "Ninja")
#   endif()
# endif()

# set(CMAKE_GENERATOR "Unix Makefiles")
# 
set(CMAKE_GENERATOR "Ninja")

project(ArrowMinimalExample)

option(ARROW_LINK_SHARED "Link to the Arrow shared library" OFF)

find_package(Arrow REQUIRED)
find_package(Parquet REQUIRED)
find_package(ArrowDataset REQUIRED)
find_package(ArrowAcero REQUIRED)

include_directories(lib-required)


set(EXSTM_RE ON)

add_subdirectory(lib-external)



set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
# if(NOT DEFINED CMAKE_CXX_STANDARD)
# endif()

# We require a C++17 compliant compiler
set(CMAKE_CXX_STANDARD_REQUIRED ON)

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)


if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release)
endif()

message(STATUS "Arrow version: ${ARROW_VERSION}")
message(STATUS "Arrow SO version: ${ARROW_FULL_SO_VERSION}")

add_executable(arrow-example 
  example.cxx 
  lib-required/stm.cpp

) #row_wise_conversion_example.cpp
# add_executable(arrow-example exec-plan-acero.cpp lib-required/stm.cpp)


# Enable colored output
if ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU")
  add_compile_options(-fdiagnostics-color=always)
elseif ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang")
  add_compile_options(-fcolor-diagnostics)
elseif ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "MSVC")
  add_compile_options(/EHsc /W3 /WX /FC /Zc:__cplusplus /diagnostics:caret)
endif()


if(ARROW_LINK_SHARED)
  target_link_libraries(arrow-example PRIVATE Arrow::arrow_shared)
else()
  target_link_libraries(arrow-example PRIVATE 
    Arrow::arrow_static 
    Parquet::parquet_static 
    ArrowDataset::arrow_dataset_static 
    ArrowAcero::arrow_acero_static
    stm )
endif()
