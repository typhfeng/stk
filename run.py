import os
import sys
import glob
import subprocess
from config.cfg_stk import cfg_stk


def kill_python_processes_windows(current_pid):
    """Kill all running Python processes in Windows, except this script."""
    print("Terminating Windows Python processes...")
    try:
        tasklist = subprocess.check_output(
            "tasklist", shell=True, encoding='cp1252')
        pids = [line.split()[1] for line in tasklist.splitlines(
        ) if 'python' in line.lower() and line.split()[1] != str(current_pid)]
        for pid in pids:
            subprocess.run(["taskkill", "/F", "/PID", pid], check=True)
    except subprocess.CalledProcessError as e:
        print(f"Error while killing processes: {e}")
    except Exception as ex:
        print(f"An unexpected error occurred: {ex}")


def kill_python_processes_unix(current_pid):
    """Kill all running Python processes in Unix-like systems, except this script."""
    print("Terminating Unix-like Python processes...")
    try:
        ps_output = subprocess.check_output("ps aux", shell=True).decode()
        pids = [line.split()[1] for line in ps_output.splitlines(
        ) if 'python' in line and 'grep' not in line and line.split()[1] != str(current_pid)]
        for pid in pids:
            subprocess.run(["kill", "-9", pid], check=False)  # Ignore permission errors
    except subprocess.CalledProcessError as e:
        print(f"Error while killing processes: {e}")
    except Exception as ex:
        print(f"An unexpected error occurred: {ex}")


def remove_old_files():
    """Remove old result files."""
    print("Removing old results...")
    for ext in ['*.xlsx', '*.html', '*.prof', '*.json']:
        for file in glob.glob(ext):
            os.remove(file)
            print(f"Removed {file}")


def run_cpp_with_profile(binary_path, profile_output_dir):
    """Run C++ binary with gperftools profiling."""
    os.makedirs(profile_output_dir, exist_ok=True)
    
    profile_data = os.path.join(profile_output_dir, "hfa_profile.prof")
    flamegraph = os.path.join(profile_output_dir, "hfa_flamegraph.svg")
    callgraph = os.path.join(profile_output_dir, "hfa_callgraph.pdf")
    
    # Clean old profiles
    for f in [profile_data, flamegraph, callgraph]:
        if os.path.exists(f):
            os.remove(f)
    
    print(f"🚀 Running with profiling enabled...")
    print(f"   Profile output: {profile_data}")
    
    env = os.environ.copy()
    env['CPUPROFILE'] = profile_data
    env['CPUPROFILE_FREQUENCY'] = '1000'
    env['LD_PRELOAD'] = '/lib/x86_64-linux-gnu/libprofiler.so'
    
    try:
        subprocess.run([binary_path], env=env, check=True)
    except subprocess.CalledProcessError as e:
        print(f"Error running binary: {e}")
        return
    
    if not os.path.exists(profile_data):
        print("❌ Profile data not generated")
        return
    
    print(f"✅ Profiling complete!")
    
    # Analyze with pprof
    pprof = os.path.expanduser("~/go/bin/pprof")
    if not os.path.exists(pprof):
        print("⚠️  pprof not found, skipping analysis")
        return
    
    print(f"\n📊 Top 30 Functions by CPU Time:")
    print("=" * 80)
    subprocess.run([pprof, "--text", "--nodecount=30", binary_path, profile_data])
    
    print(f"\n🔥 Generating flamegraph...")
    subprocess.run([pprof, "--svg", binary_path, profile_data], 
                   stdout=open(flamegraph, 'w'), stderr=subprocess.DEVNULL)
    if os.path.exists(flamegraph):
        print(f"✅ Flamegraph: {flamegraph}")
    
    print(f"\n📈 Generating call graph...")
    subprocess.run([pprof, "--pdf", binary_path, profile_data],
                   stdout=open(callgraph, 'w'), stderr=subprocess.DEVNULL)
    if os.path.exists(callgraph):
        print(f"✅ Call graph: {callgraph}")
    
    print(f"\n🎯 HFA Function Analysis:")
    print("=" * 80)
    subprocess.run([pprof, "--text", "--focus=AnalysisHighFrequency", 
                    "--nodecount=20", binary_path, profile_data])
    
    print(f"\n📋 Summary:")
    print(f"   Profile data: {profile_data}")
    print(f"   Flamegraph:   {flamegraph}")
    print(f"   Call graph:   {callgraph}")
    print(f"\n💡 To start interactive web UI:")
    print(f"   {pprof} -http=:8080 {binary_path} {profile_data}")


def main():
    # Change to the directory where this script is located
    os.chdir(os.path.dirname(os.path.abspath(__file__)))

    current_pid = os.getpid()  # Store the current process ID

    # Kill Python processes
    if os.name == 'nt':  # Windows
        kill_python_processes_windows(current_pid)
    else:  # Unix-like
        kill_python_processes_unix(current_pid)

    # Remove old files
    remove_old_files()

    # app_name = "main"
    app_name = "main_csv"
    # app_name = "L1_database"
    # app_name = "L2_database"
    
    
    print(f"Starting C++ project: {app_name}...")
    
    # Build the project first using cmake directly (avoid build.sh auto-run)
    cpp_project_dir = f"./cpp/projects/{app_name}"
    print(f"Building project in {cpp_project_dir}...")
    
    # Build using cmake (show output in real-time)
    build_result = subprocess.run(
        ["cmake", "--build", "build", "--parallel"],
        cwd=cpp_project_dir
    )
    
    if build_result.returncode != 0:
        print("❌ Build failed!")
        return
    
    print("✅ Build successful!")
    
    # Change to project build directory for correct relative paths
    cpp_build_dir = f"./cpp/projects/{app_name}/build"
    original_dir = os.getcwd()
    os.chdir(cpp_build_dir)
    
    try:
        if cfg_stk.profile:
            profile_dir = os.path.join(original_dir, "output/profile")
            run_cpp_with_profile("./bin/app_" + app_name, profile_dir)
        else:
            subprocess.run(["./bin/app_" + app_name], check=True)
    finally:
        os.chdir(original_dir)
    
    print("C++ app finished")


if __name__ == "__main__":
    main()
