cmake_minimum_required(VERSION 3.20)
project(Main VERSION 1.0.0 LANGUAGES CXX C)

# Set C++ standard
set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON) # for clangd syntax highlighting

# Compiler-specific options
if(CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
    # Clang flags with modern LTO using lld linker
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O3 -Wall -Wextra -march=native -flto")
    set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -O3 -DNDEBUG -ffast-math")
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fuse-ld=lld")
elseif(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    # GCC flags with LTO
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O3 -Wall -Wextra -march=native -flto")
    set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -O3 -DNDEBUG -ffast-math")
elseif(MSVC)
    # MSVC specific flags - Maximum optimization
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /O2 /W4 /EHsc")
    set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} /O2 /Ob2 /DNDEBUG /GL /Gy /Gw /arch:AVX2 /fp:fast")
    set(CMAKE_EXE_LINKER_FLAGS_RELEASE "${CMAKE_EXE_LINKER_FLAGS_RELEASE} /LTCG /OPT:REF /OPT:ICF")
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
    ../../src/codec/binary_decoder_L1.cpp
    ../../src/codec/binary_decoder_L2.cpp
    ../../src/main.cpp
)

# code specific settings ==========================================================================

# =================================================================================================

set(HEADERS
    ../../include/misc/misc.hpp
    ../../include/misc/print.hpp
    ../../include/misc/affinity.hpp
    ../../include/define/CBuffer.hpp
    ../../include/codec/json_config.hpp
    ../../include/codec/binary_decoder_L1.hpp
    ../../include/codec/binary_decoder_L2.hpp
    ../../include/codec/delta_encoding.hpp
    ../../include/codec/L2_DataType.hpp
    # ../../include/AnalysisTechnical.hpp
    ../../include/AnalysisHighFrequency.hpp
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
