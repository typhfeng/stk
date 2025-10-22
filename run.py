import os
import subprocess
import time
from config.cfg_stk import cfg_stk


def run_cpp_with_sampling_profile(binary_path):
    """Run C++ binary with gperftools sampling profiler at maximum frequency."""
    
    profile_file = "profile.out"
    
    # Clean up old pprof web server (port 8080)
    subprocess.run(["pkill", "-f", "pprof.*8080"], check=False, capture_output=True)
    
    # Clean up old profile
    if os.path.exists(profile_file):
        os.remove(profile_file)
    
    print("Using gperftools CPU profiler (maximum sampling frequency for high-frequency code)...")
    start_time = time.time()
    
    # Run with gperftools at maximum sampling frequency
    env = os.environ.copy()
    env['CPUPROFILE'] = profile_file
    env['CPUPROFILE_FREQUENCY'] = '4000'  # 4000 Hz sampling (maximum practical limit)
    
    # Use LD_PRELOAD to inject profiler
    profiler_lib = '/usr/lib/x86_64-linux-gnu/libprofiler.so.0'
    if os.path.exists(profiler_lib):
        env['LD_PRELOAD'] = profiler_lib
    
    subprocess.run([binary_path], env=env, check=True)
    
    elapsed_time = time.time() - start_time
    print(f"\nProfiling complete (Time: {elapsed_time:.2f}s)")
    
    # Generate report and open web GUI
    if os.path.exists(profile_file):
        print("\n" + "="*80)
        print("SAMPLING PROFILE - Function-Level Statistics (4000 Hz)")
        print("="*80 + "\n")
        
        # Find pprof command
        pprof_cmd = None
        for cmd in ["pprof", "google-pprof", os.path.expanduser("~/go/bin/pprof")]:
            result = subprocess.run(["which", cmd], capture_output=True, check=False)
            if result.returncode == 0:
                pprof_cmd = cmd
                break
        
        if not pprof_cmd:
            print("pprof not found. Profile data saved to:", profile_file)
            return
        
        # Configuration
        TARGET_NAMESPACE = "AnalysisHighFrequency"
        DEPTH = 50  # Max nodes to display (controls effective depth)
        
        # Show call hierarchy tree
        print("\n" + "="*80)
        print(f"Call Tree: {TARGET_NAMESPACE} (depth={DEPTH})")
        print("="*80 + "\n")
        
        pprof_tree = subprocess.run(
            [pprof_cmd, "--tree", f"--nodecount={DEPTH}",
             f"--show={TARGET_NAMESPACE}",
             binary_path, profile_file],
            capture_output=True,
            text=True,
            check=False
        )
        
        if pprof_tree.returncode == 0:
            print(pprof_tree.stdout)
        
        print("\n" + "="*80)
        print("Opening pprof web GUI on http://localhost:8080")
        print("Press Ctrl+C to stop the web server")
        print("="*80 + "\n")
        
        # Launch pprof web GUI (blocking)
        subprocess.run(
            [pprof_cmd, "-http=:8080", binary_path, profile_file],
            check=False
        )
    else:
        print("\nNo profile generated. Install: sudo apt-get install libgoogle-perftools-dev")


def build_cpp_project(app_name, enable_profile_mode=False):
    """Build C++ project with optional profile mode."""
    cpp_project_dir = f"./cpp/projects/{app_name}"
    
    if enable_profile_mode:
        subprocess.run(
            ["cmake", "-S", ".", "-B", "build", "-DPROFILE_MODE=ON"],
            cwd=cpp_project_dir,
            check=True
        )
    
    subprocess.run(
        ["cmake", "--build", "build", "--parallel"],
        cwd=cpp_project_dir,
        check=True
    )


def run_cpp_binary(app_name, use_profiler=False):
    """Run C++ binary with optional profiling."""
    binary_path = f"./bin/app_{app_name}"
    
    if use_profiler:
        run_cpp_with_sampling_profile(binary_path)
    else:
        start_time = time.time()
        subprocess.run([binary_path], check=True)
        elapsed_time = time.time() - start_time
        print(f"\nExecution time: {elapsed_time:.2f}s ({elapsed_time/60:.2f}min)")


def main():
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    original_dir = os.getcwd()

    app_name = "main_csv"
    
    build_cpp_project(app_name, enable_profile_mode=cfg_stk.profile)
    
    cpp_build_dir = f"./cpp/projects/{app_name}/build"
    os.chdir(cpp_build_dir)
    
    try:
        run_cpp_binary(app_name, use_profiler=cfg_stk.profile)
    finally:
        os.chdir(original_dir)


if __name__ == "__main__":
    main()
