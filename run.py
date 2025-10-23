import os
import subprocess
import sys
import time

# ============================================================================
# Build & Run Pipeline
# ============================================================================
# Calling chain: run.py -> py/{APP_NAME}.py -> cpp/projects/{APP_NAME}/build.sh
#
# - run.py: orchestrates build -> run -> profiling
# - py/{APP_NAME}.py: project-specific preparation + triggers build.sh
# - build.sh: compiles C++ + copies compile_commands.json for clangd
# ============================================================================

# ============================================================================
# Configuration
# ============================================================================
ENABLE_PROFILE = False
APP_NAME = "main"                              # C++ project name
CPUPROFILE_FREQUENCY = 1000000                 # Profiler sampling rate (Hz)
PROFILER_LIB = '/usr/lib/x86_64-linux-gnu/libprofiler.so.0'

# Profiler report settings
TARGET_NAMESPACE = "AssetProcessor"            # Focus namespace
PPROF_PORT = 8080                              # Web GUI port
PPROF_IGNORE = "std::|__gnu_cxx::"             # Filter standard library


def _cleanup_background_processes():
    """Kill background processes that may cause memory bloat."""
    processes_to_kill = [
        f"pprof.*{PPROF_PORT}",      # Old pprof web servers
        f"app_{APP_NAME}",            # Old app instances
    ]

    for pattern in processes_to_kill:
        subprocess.run(["pkill", "-9", "-f", pattern],
                       capture_output=True, check=False)

    time.sleep(0.3)


def _cleanup_old_profiler():
    """Kill old pprof web server."""
    subprocess.run(["pkill", "-f", f"pprof.*{PPROF_PORT}"],
                   check=False, capture_output=True)
    time.sleep(0.2)


def _find_pprof_command():
    """Find pprof executable."""
    for cmd in ["pprof", "google-pprof", os.path.expanduser("~/go/bin/pprof")]:
        result = subprocess.run(["which", cmd], capture_output=True, check=False)
        if result.returncode == 0:
            return cmd
    return None


def _show_profile_report(binary_path, profile_file):
    """Generate and display profiling report."""
    pprof_cmd = _find_pprof_command()
    if not pprof_cmd:
        print("pprof not found. Profile data saved to:", profile_file)
        return

    print(f"\n{'='*80}")
    print(f"SAMPLING PROFILE - Top Functions ({CPUPROFILE_FREQUENCY} Hz)")
    print(f"{'='*80}\n")

    pprof_top = subprocess.run(
        [pprof_cmd, "--top", "--cum", "--nodecount=20",
         f"--focus={TARGET_NAMESPACE}", f"--hide={PPROF_IGNORE}",
         binary_path, profile_file],
        capture_output=True, text=True, check=False
    )

    if pprof_top.returncode == 0:
        print(pprof_top.stdout)

    # Launch web server
    subprocess.Popen(
        [pprof_cmd, f"-http=:{PPROF_PORT}", binary_path, profile_file],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
    )

    time.sleep(1.5)
    subprocess.run(["cmd.exe", "/c", "start", f"http://localhost:{PPROF_PORT}"],
                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=False)

    print(f"\n✓ Web GUI: http://localhost:{PPROF_PORT}")
    print(f"  To stop: pkill -f 'pprof.*{PPROF_PORT}'\n")


def run_with_profiling(binary_path, working_dir):
    """Run C++ binary with gperftools profiler."""
    profile_file = os.path.join(working_dir, "profile.out")

    _cleanup_old_profiler()
    if os.path.exists(profile_file):
        os.remove(profile_file)

    print(f"Running with gperftools profiler ({CPUPROFILE_FREQUENCY} Hz)...")

    env = os.environ.copy()
    env['CPUPROFILE'] = profile_file
    env['CPUPROFILE_FREQUENCY'] = str(CPUPROFILE_FREQUENCY)
    if os.path.exists(PROFILER_LIB):
        env['LD_PRELOAD'] = PROFILER_LIB

    start_time = time.time()
    subprocess.run([binary_path], cwd=working_dir, env=env, check=True)
    elapsed_time = time.time() - start_time

    print(f"\nProfiling complete (Time: {elapsed_time:.2f}s)")

    if os.path.exists(profile_file):
        _show_profile_report(binary_path, profile_file)
    else:
        print("\nProfile not generated. Install: sudo apt-get install libgoogle-perftools-dev")


def build_project(app_name, enable_profile_mode):
    """Trigger build via py/{app_name}.py -> build.sh."""
    py_script = f"./py/{app_name}.py"

    if not os.path.exists(py_script):
        print(f"Error: Build script not found: {py_script}")
        sys.exit(1)

    env = os.environ.copy()
    env['PROFILE_MODE'] = 'ON' if enable_profile_mode else 'OFF'

    result = subprocess.run(["python3", py_script], env=env, check=False)

    if result.returncode != 0:
        print(f"\nBuild failed with exit code {result.returncode}")
        sys.exit(1)


def run_binary(binary_path, working_dir, use_profiler):
    """Run binary with optional profiling."""
    if not os.path.exists(binary_path):
        print(f"Error: Binary not found: {binary_path}")
        sys.exit(1)

    if use_profiler:
        run_with_profiling(binary_path, working_dir)
    else:
        start_time = time.time()
        subprocess.run([binary_path], cwd=working_dir, check=True)
        elapsed = time.time() - start_time
        print(f"\nExecution time: {elapsed:.2f}s ({elapsed/60:.2f}min)")


def main():
    # Change to script directory
    script_dir = os.path.dirname(os.path.abspath(__file__))
    os.chdir(script_dir)

    # Cleanup background processes first
    print("Cleaning up background processes...")
    _cleanup_background_processes()

    # Build project
    build_project(APP_NAME, ENABLE_PROFILE)

    # Run binary from build directory (binary expects to run from build/ for relative paths)
    build_dir = os.path.abspath(f"cpp/projects/{APP_NAME}/build")
    binary_path = os.path.join(build_dir, f"bin/app_{APP_NAME}")
    run_binary(binary_path, build_dir, ENABLE_PROFILE)


if __name__ == "__main__":
    main()
