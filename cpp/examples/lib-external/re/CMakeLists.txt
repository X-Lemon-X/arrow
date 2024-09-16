

target_include_directories(stm PUBLIC 
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/inc>
  $<INSTALL_INTERFACE:include/stm>
)
target_sources(stm PRIVATE
  src/re.cpp
)
