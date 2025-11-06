cmake_minimum_required(VERSION 3.20)
project(Main VERSION 1.0.0 LANGUAGES CXX C)

# Force Clang compiler
if(NOT CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
    message(FATAL_ERROR "This project requires Clang compiler. Current compiler: ${CMAKE_CXX_COMPILER_ID}")
endif()

# Set C++ standard
set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON) # for clangd syntax highlighting

# PROFILE mode: disable aggressive optimizations for better profiling visibility
option(PROFILE_MODE "Build with profiling-friendly flags (no LTO, less inlining)" OFF)

# Compiler options
if(PROFILE_MODE)
    message(STATUS "PROFILE_MODE: sampling profiling with comprehensive optimization disabling")
    add_compile_definitions(PROFILE_MODE)  # Enable PROFILE_MODE macro in source code
    
    # Base flags for profiling
    set(CMAKE_CXX_FLAGS "-O0 -g -Wall -Wextra -march=native -fno-omit-frame-pointer")
    # Disable all forms of inlining
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-inline")                       # No inline at all
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-inline-functions")             # No automatic inlining
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -mllvm -inline-threshold=0")        # Force inline threshold to 0
    # Disable call optimizations
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-optimize-sibling-calls")       # No tail call optimization
    # Disable loop optimizations
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-unroll-loops")                 # No loop unrolling
    # Disable vectorization
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-vectorize")                    # No auto-vectorization
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-slp-vectorize")                # No SLP vectorization
    # Disable builtin optimizations
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-builtin")                      # No builtin function optimizations    
else()
    message(STATUS "Production build: Optimized for performance")
    
    # Base optimization flags
    set(CMAKE_CXX_FLAGS "-O3 -Wall -Wextra") # -DNDEBUG 决定是否实现断言(assert)
    # CPU-specific optimizations
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -march=native")
    # LTO for cross-module optimizations
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -flto=thin")
    # Fast math (already includes many aggressive FP optimizations)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -ffast-math")
    # Link-time optimizations
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fuse-ld=lld")
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -Wl,--icf=all")
endif()

# Output directories
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)

# Include directories
include_directories(../../include)
include_directories(../../package/zstd-1.5.7)

# Zstandard library path
set(ZSTD_LIB_PATH ${CMAKE_CURRENT_SOURCE_DIR}/../../package/zstd-1.5.7/libzstd.a)

# Source files
set(SOURCES
    ../../src/codec/json_config.cpp
    ../../src/codec/binary_decoder_L2.cpp
    ../../src/codec/binary_encoder_L2.cpp
    ../../src/misc/logging.cpp
    ../../src/misc/file_convert.cpp
    ../../src/main.cpp
)

# code specific settings ==========================================================================

# =================================================================================================

set(HEADERS
    ../../include/misc/timer.hpp
    ../../include/misc/print.hpp
    ../../include/misc/progress_single.hpp
    ../../include/misc/progress_parallel.hpp
    ../../include/misc/affinity.hpp
    ../../include/misc/logging.hpp
    ../../include/misc/file_convert.hpp
    ../../include/define/CBuffer.hpp
    ../../include/codec/json_config.hpp
    ../../include/codec/binary_decoder_L2.hpp
    ../../include/codec/binary_encoder_L2.hpp
    ../../include/codec/L2_DataType.hpp
    # ../../include/AnalysisTechnical.hpp
    ../../include/lob/LimitOrderBook.hpp
)

# Disable fast-math specifically for JSON library to avoid infinity issues
set_source_files_properties(../../src/codec/json_config.cpp PROPERTIES
    COMPILE_OPTIONS "-fno-fast-math"
)

# Create executable
add_executable(app_main ${SOURCES} ${HEADERS})

# Include directories
target_include_directories(app_main PRIVATE
    ../../include
    ../../package/zstd-1.5.7
)

# Link Zstandard library
target_link_libraries(app_main PRIVATE ${ZSTD_LIB_PATH})

# Link gperftools profiler in PROFILE_MODE
if(PROFILE_MODE)
    target_link_libraries(app_main PRIVATE profiler)
endif()

# Installation
install(TARGETS app_main RUNTIME DESTINATION bin)

# Custom target for running
add_custom_target(run_main
    COMMAND app_main
    DEPENDS app_main
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
    COMMENT "Running app_main"
)

# Print configuration info
message(STATUS "Build type: ${CMAKE_BUILD_TYPE}")
message(STATUS "C++ compiler: ${CMAKE_CXX_COMPILER_ID}")
message(STATUS "C++ standard: ${CMAKE_CXX_STANDARD}")
message(STATUS "Output directory: ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}")
message(STATUS "app_main sources: ${SOURCES}")
