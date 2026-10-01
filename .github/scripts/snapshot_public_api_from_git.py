#!/usr/bin/env python3
"""Explicit maintainer operation: compile an exact Git revision, then print its API floor.

Example (classpath contains the core/JUnit compile dependencies, not project output):
  python3 .github/scripts/snapshot_public_api_from_git.py --revision <commit> --classpath "$dependency_classpath"

This never reads Java source from the working tree and never writes a tracked baseline itself.
Review and version the printed JSON deliberately. Ordinary compatibility checks do not run this.
"""

import argparse
import json
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

import check_public_api as api


SOURCE_ROOTS = ("query-audit-core/src/main/java", "query-audit-junit5/src/main/java")


def git(*args):
    return subprocess.check_output(["git", *args])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--revision", required=True)
    parser.add_argument("--classpath", required=True)
    parser.add_argument("--javac", default=shutil.which("javac"))
    args = parser.parse_args()
    if not args.javac:
        parser.error("A JDK compiler is required")
    revision = git("rev-parse", "--verify", args.revision + "^{commit}").decode().strip()
    tree_hashes = {root: git("rev-parse", revision + ":" + root).decode().strip() for root in SOURCE_ROOTS}
    paths = git("ls-tree", "-r", "--name-only", revision, "--", *SOURCE_ROOTS).decode().splitlines()
    with tempfile.TemporaryDirectory(prefix="query-audit-api-baseline-") as directory:
        directory = Path(directory)
        sources = []
        for relative in paths:
            if not relative.endswith(".java"):
                continue
            target = directory / relative
            if not target.resolve().is_relative_to(directory.resolve()):
                raise ValueError("Source path escapes the temporary baseline directory")
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(git("show", revision + ":" + relative))
            sources.append(str(target))
        classes = directory / "classes"
        classes.mkdir()
        command = [args.javac, "--release", "17", "-encoding", "UTF-8", "-proc:none",
            "-classpath", args.classpath, "-d", str(classes), *sources]
        subprocess.run(command, check=True)
        baseline = {
            "schema_version": api.SCHEMA_VERSION,
            "source_revision": revision,
            "source_tree_hashes": tree_hashes,
            "compiler_release": 17,
            "scope": {"prefixes": api.PREFIXES, "entry_points": api.ENTRY_POINTS,
                "excluded": ["io/queryaudit/core/extension/internal/"]},
            "classes": api.snapshot([classes]),
        }
        print(json.dumps(baseline, indent=2, sort_keys=True))


if __name__ == "__main__":
    try:
        main()
    except (OSError, ValueError, subprocess.CalledProcessError) as failure:
        print(f"Could not compile the requested API baseline: {failure}", file=sys.stderr)
        sys.exit(2)
