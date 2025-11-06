#!/usr/bin/env python3
"""
Convert disguised 7z files to non-solid RAR format.

Usage:
    python fix_7z_to_rar.py <archive_base_dir> <date1> [<date2> ...]
    
Example:
    python fix_7z_to_rar.py /path/to/archives 20241119 20240926
"""

import sys
import subprocess
import shutil
from pathlib import Path
from typing import Tuple

from repair_utils import (
    setup_output_dirs, cleanup_dirs,
    process_with_pool, resolve_archive_path, log_message
)


def detect_7z_format(archive_path: Path) -> bool:
    """Check if file is 7z format by magic bytes."""
    try:
        with open(archive_path, 'rb') as f:
            magic = f.read(6)
            return (len(magic) == 6 and 
                    magic[0] == ord('7') and magic[1] == ord('z') and
                    magic[2] == 0xbc and magic[3] == 0xaf and
                    magic[4] == 0x27 and magic[5] == 0x1c)
    except:
        return False


def convert_single_file(args: Tuple[Path, Path, Path], worker_idx: int) -> Tuple[str, bool, str]:
    """Convert a single 7z file to RAR format."""
    source_path, output_dir, temp_dir = args
    filename = source_path.name
    stem = source_path.stem
    
    log_message(worker_idx, f"=== Processing {filename} ===")
    
    temp_extract_dir = temp_dir / f"extract_{stem}_{worker_idx}"
    output_rar_path = output_dir / filename
    
    try:
        # Step 1: Extract 7z archive
        temp_extract_dir.mkdir(parents=True, exist_ok=True)
        log_message(worker_idx, f"Extracting {filename}...")
        result = subprocess.run(
            ['7z', 'x', str(source_path), f'-o{temp_extract_dir}/', '-y'],
            capture_output=True, text=True
        )
        if result.returncode != 0:
            log_message(worker_idx, f"Extract failed: {result.stderr}")
            return (filename, False, f"Extract failed")
        
        # Step 2: Create non-solid RAR archive
        log_message(worker_idx, f"Creating RAR archive...")
        result = subprocess.run(
            ['rar', 'a', '-m3', '-ma5', '-r', str(output_rar_path), '.'],
            cwd=temp_extract_dir,
            capture_output=True, text=True
        )
        if result.returncode != 0:
            log_message(worker_idx, f"RAR creation failed: {result.stderr}")
            return (filename, False, f"RAR creation failed")
        
        # Step 3: Cleanup
        shutil.rmtree(temp_extract_dir)
        log_message(worker_idx, f"Success: {filename}")
        
        return (filename, True, "Success")
        
    except Exception as e:
        log_message(worker_idx, f"Exception: {str(e)}")
        return (filename, False, f"Exception: {str(e)}")
    finally:
        shutil.rmtree(temp_extract_dir, ignore_errors=True)


def main():
    if len(sys.argv) < 3:
        print("Usage: python fix_7z_to_rar.py <archive_base_dir> <date1> [<date2> ...]")
        print("Example: python fix_7z_to_rar.py /path/to/archives 20241119 20240926")
        sys.exit(1)
    
    archive_base_dir = sys.argv[1]
    dates = sys.argv[2:]
    
    # Resolve archive paths and check if they're 7z format
    to_convert = []
    for date in dates:
        archive_path = resolve_archive_path(archive_base_dir, date)
        if not archive_path.exists():
            print(f"✗ Archive not found: {archive_path}")
            continue
        if not detect_7z_format(archive_path):
            print(f"✗ Not 7z format: {archive_path.name}")
            continue
        to_convert.append(archive_path)
    
    if not to_convert:
        print("No files to convert.")
        return
    
    print(f"\nFound {len(to_convert)} disguised 7z file(s):")
    for path in to_convert:
        print(f"  {path}")
    
    print("\nFixed RAR files will be saved to: output/fix/")
    print("Logs will be saved to: output/fix/logs/")
    print("Original files will NOT be modified.")
    print()
    
    response = input("Continue? (yes/no): ")
    if response.lower() != 'yes':
        print("Cancelled.")
        return
    
    # Setup directories
    output_dir, temp_dir = setup_output_dirs()
    
    # Process files
    print(f"\nConverting {len(to_convert)} file(s)...\n")
    args_list = [(path, output_dir, temp_dir) for path in to_convert]
    success_count, failed = process_with_pool(args_list, convert_single_file, num_workers=4)
    
    # Cleanup
    cleanup_dirs(output_dir, temp_dir)
    
    print(f"\nConversion summary:")
    print(f"  Success: {success_count}/{len(to_convert)}")
    if failed:
        print(f"  Failed: {len(failed)}")
        for name, message in failed:
            print(f"    {name}: {message}")
    
    print(f"\nFixed files saved to: {output_dir}/")
    print(f"Logs saved to: {output_dir}/logs/")
    print("Please manually replace original files after verification.")


if __name__ == '__main__':
    main()
