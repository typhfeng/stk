#!/usr/bin/env python3
"""Updater: follow `origin/main`, discard local changes, push to `fork/Linux`.

Usage:
    python3 py/update.py [--build]

This script saves uncommitted work to `.local_changes/`, then sets the
local branch to follow `origin/main`, discards local working-tree changes,
rebases the current branch onto `origin/main`, and force-pushes the result
to `fork/Linux`. All command output is captured in `.local_changes/`.
With `--build` it will run the project's build (`python3 py/main.py`) after
updating and save the build output.
"""
import argparse
import os
import subprocess
import sys
from datetime import datetime


ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
LC = os.path.join(ROOT, '.local_changes')


def run(cmd, cwd=ROOT, capture_file=None, env=None):
    proc = subprocess.run(
        cmd,
        cwd=cwd,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    out = proc.stdout
    if capture_file:
        with open(os.path.join(LC, capture_file), 'w') as f:
            f.write(out)
    return proc.returncode, out


def ensure_local_changes():
    os.makedirs(LC, exist_ok=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--build', action='store_true', help='Run build after updating'
    )
    args = parser.parse_args()

    ensure_local_changes()

    # metadata
    branch = subprocess.check_output(
        ['git', 'rev-parse', '--abbrev-ref', 'HEAD'], cwd=ROOT, text=True
    ).strip()

    with open(os.path.join(LC, 'LOCAL_CHANGES.md'), 'w') as f:
        f.write('Repository: ')
        f.write(ROOT + '\n')
        f.write('Branch: ')
        f.write(branch + '\n')
        f.write('Timestamp: ')
        f.write(datetime.utcnow().isoformat() + 'Z\n')

    # save working state
    run(
        ['git', 'status', '--porcelain=v1'],
        capture_file='git_status_before.txt',
    )
    run(['git', 'diff'], capture_file='uncommitted.patch')
    run(
        ['git', 'ls-files', '--others', '--exclude-standard'],
        capture_file='untracked.txt',
    )

    # record remotes and fetch
    run(['git', 'remote', '-v'], capture_file='remotes.txt')
    run(['git', 'fetch', 'origin'], capture_file='git_fetch_output.txt')
    run(['git', 'fetch', 'fork'], capture_file='git_fetch_fork_output.txt')

    # Ensure the local branch follows origin/main (best-effort)
    run(
        ['git', 'branch', '--set-upstream-to=origin/main', branch],
        capture_file='set_upstream.txt',
    )

    # Save incoming commits relative to origin/main
    run(
        ['git', 'log', '--oneline', 'HEAD..origin/main'],
        capture_file='incoming_commits_before_pull.txt',
    )

    # Discard local working-tree changes (we already saved them above)
    run(['git', 'reset', '--hard'], capture_file='git_reset_output.txt')
    run(['git', 'clean', '-fd'], capture_file='git_clean_output.txt')

    # Rebase current branch onto origin/main
    rc, out = run(
        ['git', 'rebase', 'origin/main'],
        capture_file='git_rebase_output.txt',
    )
    if rc != 0:
        print('Rebase failed. See .local_changes/git_rebase_output.txt')
        run(
            ['git', 'status', '--porcelain=v1'],
            capture_file='git_status_after.txt',
        )
        sys.exit(1)

    # Force-push the rebased branch to fork/Linux using --force-with-lease
    rpc, rout = run(
        ['git', 'push', '--force-with-lease', 'fork', 'HEAD:Linux'],
        capture_file='git_push_output.txt',
    )
    if rpc != 0:
        print('Push to fork failed. See .local_changes/git_push_output.txt')
        sys.exit(1)

    # record new commits (what changed in this update)
    rc2, _ = run(['git', 'rev-parse', '--verify', 'HEAD@{1}'])
    if rc2 == 0:
        run(
            ['git', 'log', '--oneline', 'HEAD@{1}..HEAD'],
            capture_file='new_commits_after_pull.txt',
        )
    else:
        with open(os.path.join(LC, 'new_commits_after_pull.txt'), 'w') as f:
            f.write('No reflog previous HEAD or no new commits\n')

    run(
        ['git', 'status', '--porcelain=v1'],
        capture_file='git_status_after.txt',
    )

    # optional build
    if args.build:
        env = os.environ.copy()
        env.setdefault('TSAN_MODE', 'OFF')
        env.setdefault('DEBUG_MODE', 'OFF')
        env.setdefault('PROFILE_MODE', 'OFF')
        env.setdefault('ASSERT_MODE', 'ON')
        print('Running build (this may take a while)...')
        rcb, bout = run(
            [sys.executable, 'py/main.py'],
            capture_file='build_output.txt',
            env=env,
        )
        if rcb != 0:
            print('Build failed; see .local_changes/build_output.txt')
        else:
            print('Build completed successfully')

    # top commits
    run(
        ['git', 'log', '--oneline', '-n', '20'],
        capture_file='top_20_commits.txt',
    )

    # final summary
    print('Update completed. Summary files are in .local_changes/')
    for name in sorted(os.listdir(LC)):
        print(' -', name)


if __name__ == '__main__':
    main()
