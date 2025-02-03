cmake_minimum_required(VERSION 3.10)
project(DuckDBExample)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Set directories
set(SRC_DIR src)
set(INCLUDE_DIR include)
set(BUILD_DIR build)

# Include directories
include_directories(${INCLUDE_DIR})

# Create build directory
file(MAKE_DIRECTORY ${CMAKE_BINARY_DIR}/${BUILD_DIR})

# Add executable
add_executable(main ${SRC_DIR}/main.cpp)

# Link with DuckDB
target_link_libraries(main PRIVATE ${INCLUDE_DIR}/libduckdb.dylib)

# Set the runtime search path
set_target_properties(main PROPERTIES RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/${BUILD_DIR})
set_target_properties(main PROPERTIES INSTALL_RPATH "${CMAKE_SOURCE_DIR}/${INCLUDE_DIR}")
