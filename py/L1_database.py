
import os
import sys
import subprocess
from typing import List, Dict

# the path include need to be earlier than relative library
TOP = "../"
sys.path.append(os.path.join(os.path.dirname(__file__), "."))
sys.path.append(os.path.join(os.path.dirname(__file__), TOP))
sys.path.append(os.path.join(os.path.dirname(__file__), "app"))


def run_L1_database():
    cpp_dir = os.path.join(os.path.dirname(__file__), TOP, "cpp/projects/L1_database")

    print("Triggering C++ build script...")
    result = subprocess.run(
        "./build.sh",
        cwd=cpp_dir,
        shell=True
    )

    if result.returncode != 0:
        print(f"Build script exited with code: {result.returncode}")
        return False
    return True


if __name__ == '__main__':
    run_L1_database()
