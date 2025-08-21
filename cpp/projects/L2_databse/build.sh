#!/bin/bash

echo "========================================"
echo "    L2_database Build Script (Linux)"
echo "========================================"

# Set compiler environment variables for clang
export CC=clang
export CXX=clang++

# Configure with CMake using Ninja generator
echo "Configuring project with CMake using Ninja generator..."
cmake -S . -B build -G "Ninja" -DCMAKE_BUILD_TYPE=Release -DCMAKE_EXPORT_COMPILE_COMMANDS=ON

# Check if configuration was successful
if [ $? -ne 0 ]; then
    echo "CMake configuration failed!"
    exit 1
fi

# Build the project
echo "Building project..."
cmake --build build --parallel

# Check if build was successful
if [ $? -ne 0 ]; then
    echo "Build failed!"
    exit 1
fi

echo "Build completed successfully!"

# Copy compile_commands.json to root directory for IDE access
if [ -f "build/compile_commands.json" ]; then
    echo "Copying compile_commands.json to root directory..."
    cp "build/compile_commands.json" "../../compile_commands.json"
    echo "compile_commands.json copied to root directory"
else
    echo "Warning: compile_commands.json not found"
fi

# Move executable to root directory
if [ -f "build/bin/l2_database" ]; then
    echo "Moving executable to root directory..."
    mv "build/bin/l2_database" "../../l2_database"
    echo "Executable moved to: ../../l2_database"
else
    echo "Warning: Executable not found at expected location"
fi

# Run the executable automatically
if [ -f "../../l2_database" ]; then
    echo "Running l2_database..."
    ../../l2_database
else
    echo "Error: l2_database not found in root directory"
fi

echo
echo "========================================"
echo "    L2_database build finished successfully!"
echo "    compile_commands.json is available for IDE"
echo "========================================"
