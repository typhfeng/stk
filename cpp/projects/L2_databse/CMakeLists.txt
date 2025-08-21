cmake_minimum_required(VERSION 3.20)
project(L2_database VERSION 1.0.0 LANGUAGES CXX C)

# Set C++ standard
set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON) # for clangd syntax highlighting

# MSVC (Visual Studio) Optimizations:
# /GL - Whole program optimization
# /Gy - Function-level linking
# /Gw - Optimize global data
# /arch:AVX2 - Use AVX2 instructions
# /fp:fast - Fast floating-point model
# /LTCG - Link time code generation
# /OPT:REF - Remove unreferenced functions
# /OPT:ICF - Identical COMDAT folding

# GCC/Clang Optimizations:
# -flto - Link time optimization
# -ffast-math - Fast math operations

# Compiler-specific options
if(MSVC)
    # MSVC specific flags - Maximum optimization
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /O2 /W4 /EHsc")
    set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} /O2 /Ob2 /DNDEBUG /GL /Gy /Gw /arch:AVX2 /fp:fast")
    set(CMAKE_EXE_LINKER_FLAGS_RELEASE "${CMAKE_EXE_LINKER_FLAGS_RELEASE} /LTCG /OPT:REF /OPT:ICF")
elseif(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    # GCC flags with LTO
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O3 -Wall -Wextra -march=native -flto")
    set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -O3 -DNDEBUG -ffast-math")
elseif(CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
    # Clang flags with modern LTO using lld linker
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O3 -Wall -Wextra -march=native -flto")
    set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -O3 -DNDEBUG -ffinite-math-only")
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fuse-ld=lld")
endif()

# Output directories
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)

# Source files
set(SOURCES
    ../../src/codec/json_config.cpp
    ../../src/package/miniz/miniz.c
    ../../src/codec/binary_decoder_L1.cpp
    ../../src/technical_analysis.cpp
    ../../src/main.cpp
)

# code specific settings ==========================================================================
# Set C language for miniz.c and suppress external library warnings
set_source_files_properties(../../src/package/miniz/miniz.c PROPERTIES LANGUAGE C)
if(MSVC)
    set_source_files_properties(../../src/package/miniz/miniz.c PROPERTIES COMPILE_FLAGS "/wd4132 /wd4127")
elseif(CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
    set_source_files_properties(../../src/package/miniz/miniz.c PROPERTIES COMPILE_FLAGS "-Wno-#pragma-messages")
elseif(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    set_source_files_properties(../../src/package/miniz/miniz.c PROPERTIES COMPILE_FLAGS "-Wno-pragma-messages")
endif()

# Disable -ffinite-math-only for JSON-related files to avoid nlohmann/json warnings
if(CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
    set_source_files_properties(../../src/codec/json_config.cpp PROPERTIES COMPILE_FLAGS "-fno-finite-math-only")
endif()
# =================================================================================================

set(HEADERS
    ../../include/misc/misc.hpp
    ../../include/misc/print.hpp
    ../../include/misc/affinity.hpp
    ../../include/define/CBuffer.hpp
    ../../include/codec/json_config.hpp
    ../../include/package/miniz/miniz.h
    ../../include/codec/binary_decoder_L1.hpp
    ../../include/technical_analysis.hpp
)

# Create executable
add_executable(l2_database ${SOURCES} ${HEADERS})

# Include directories
target_include_directories(l2_database PRIVATE
    ../../src
    ../../include
    ../../include/package/miniz
    ../../include/package/nlohmann
)

# Windows-specific settings
if(WIN32)
    # For Windows, we might need additional libraries
    target_link_libraries(l2_database PRIVATE ws2_32)
    
    # Set subsystem to console
    set_target_properties(l2_database PROPERTIES
        WIN32_EXECUTABLE FALSE
    )
endif()

# Installation
install(TARGETS l2_database
    RUNTIME DESTINATION bin
)

# Custom target for running with default parameters
add_custom_target(run
    COMMAND l2_database
    DEPENDS l2_database
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
    COMMENT "Running L2_database with default parameters"
)

# Custom target for running with custom parameters
add_custom_target(run_custom
    COMMAND l2_database "D:/data/A_stock/A_L1_binary" "parsed_output.csv"
    DEPENDS l2_database
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
    COMMENT "Running L2_database with custom parameters"
)

# Print configuration info
message(STATUS "Build type: ${CMAKE_BUILD_TYPE}")
message(STATUS "C++ compiler: ${CMAKE_CXX_COMPILER_ID}")
message(STATUS "C++ standard: ${CMAKE_CXX_STANDARD}")
message(STATUS "Output directory: ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}")
