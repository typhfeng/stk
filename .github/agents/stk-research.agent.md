## Purpose
This file defines responsibilities, safe operations, and workflow conventions for the `stk-research` AI agent operating in this repository. It helps an autonomous agent (or a human-in-the-loop bot) be productive and safe immediately.

**Scope & Goals:**
- Assist with code navigation, small targeted edits, builds, and lightweight debugging for the mixed C++/Python research project.
- Preserve developer changes, avoid destructive git operations, and provide small, well-tested patches rather than sweeping refactors.

**Quick Repo Map (high value):**
- `run.py` — top-level launcher (sets build modes, calls `py/main.py`, runs binary).
- `py/main.py` — canonical build script; configures CMake and copies `compile_commands.json` to `cpp/` for clangd.
- `cpp/include/` & `cpp/src/` — headers and implementations. Maintain header/impl symmetry when changing APIs.
- `cpp/projects/main/` — CMake entry; final binary at `cpp/projects/main/build/bin/app_main`.
- `config/` — runtime configs and sample data.

**Allowed autonomous actions:**
- Read any repository file and run local build commands.
- Create small, focused patches and write them to the working tree (use the provided patch tool). Keep changes minimal and well-scoped.
- Back up uncommitted changes to `.local_changes/` before pulling or rebasing.

**Require human approval:**
- Pushing to `origin`, opening PRs, force-pushes, or history-rewriting requires explicit user approval.
- Large refactors, changes touching many subsystems, or uncertain behavior changes.

**Safe update-from-origin recipe (agent must follow):**
1. Save local working state:
   - `git diff > .local_changes/uncommitted.patch`
   - `git ls-files --others --exclude-standard > .local_changes/untracked.txt`
2. `git fetch origin` and inspect `git log --oneline HEAD..origin/<branch>`.
3. Pull safely: `git pull --rebase --autostash origin <branch>` and save output to `.local_changes/git_pull_output.txt`.
4. If conflicts or unexpected changes appear, stop and ask the user.

**Build & run commands (examples):**
- Full build + run (convenience):
  - `python3 run.py`
- Build only (explicit modes):
  - `TSAN_MODE=OFF DEBUG_MODE=OFF PROFILE_MODE=OFF ASSERT_MODE=ON python3 py/main.py`
- Manual CMake (when editing `CMakeLists.txt`):
  - `cd cpp/projects/main && cmake -S . -B build -G Ninja -DCMAKE_BUILD_TYPE=Release && cmake --build build --parallel`

**Failure handling & diagnostics:**
- Capture full build output to `.local_changes/build_output.txt` and list the first failing translation unit and the exact compiler error lines.
- When errors relate to missing symbols, prefer checking for missing includes, renamed functions, or header changes before altering many files.

**Repository patterns and conventions:**
- Public interfaces are in `cpp/include/...` and corresponding code in `cpp/src/...` — keep them in sync.
- Features follow a Feature/Worker pattern: metadata in `cpp/include/features/`, worker implementations in `cpp/src/worker/`, and shared structures in `cpp/include/shared/` (see `SharedData.hpp`).
- Feature storage and IO use columnar and compression helpers in `cpp/include/features/backend/` and `cpp/include/codec/` (look at `FeatureStore.hpp` and `binary_decoder_L2.hpp`).

**Deliverables for changes:**
- Small, well-scoped commit with clear message and a short summary file if the change is non-trivial (place in `.local_changes/` or the PR body).
- If possible, include a quick smoke-run command which verifies the change (example: run a small GUI task or a headless unit that touches the modified code path).

**References:**
- `.github/copilot-instructions.md` — developer-facing guidance and build examples.
- `doc/README.md` — project architecture overview.

If you are unsure about a change or it touches multiple subsystems, stop and ask for confirmation.
