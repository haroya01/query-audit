#!/usr/bin/env python3
"""Resolve a published release tag and validate the source used for publication."""

import argparse
import json
import os
from pathlib import Path
import re
import subprocess
import sys
from urllib.parse import quote


class ReleaseError(Exception):
    pass


TAG_PATTERN = re.compile(r"v[0-9]+\.[0-9]+\.[0-9]+(?:-[0-9A-Za-z.-]+)?(?:\+[0-9A-Za-z.-]+)?")
SHA_PATTERN = re.compile(r"[0-9a-f]{40}")
REPOSITORY_PATTERN = re.compile(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+")


def version_from_tag(tag):
    if not TAG_PATTERN.fullmatch(tag):
        raise ReleaseError("Release tag must be a version tag such as v0.7.0 or v0.7.0-rc.1")
    return tag[1:]


def select_tag(event, retry_tag, created_tag, created_version):
    """The dispatch ref never chooses which source is published."""
    if event == "workflow_dispatch":
        tag = retry_tag
    elif event == "push":
        tag = created_tag
        if not created_version:
            raise ReleaseError("An automatic publication requires the release-please version")
    else:
        raise ReleaseError(f"Publication is not supported for event {event!r}")

    version = version_from_tag(tag)
    if event == "push" and version != created_version:
        raise ReleaseError("The release-please tag and version do not match")
    return tag, version


def github_api(endpoint):
    try:
        result = subprocess.run(
            ["gh", "api", "--method", "GET", endpoint],
            check=True, capture_output=True, text=True,
        )
        return json.loads(result.stdout)
    except (subprocess.CalledProcessError, json.JSONDecodeError) as error:
        raise ReleaseError(f"Could not resolve release metadata from {endpoint}") from error


def resolve_release(repository, tag):
    version_from_tag(tag)
    if not REPOSITORY_PATTERN.fullmatch(repository):
        raise ReleaseError("Repository must have the form owner/name")
    prefix = f"repos/{repository}"
    encoded_tag = quote(tag, safe="")
    release = github_api(f"{prefix}/releases/tags/{encoded_tag}")
    if (
        not isinstance(release, dict)
        or release.get("tag_name") != tag
        or release.get("draft") is not False
    ):
        raise ReleaseError("The tag must identify an existing, published GitHub release")

    reference = github_api(f"{prefix}/git/ref/tags/{encoded_tag}")
    if not isinstance(reference, dict) or reference.get("ref") != f"refs/tags/{tag}":
        raise ReleaseError("GitHub did not return the requested tag reference")
    target = reference.get("object", {})
    seen = set()
    while True:
        if not isinstance(target, dict):
            raise ReleaseError("The release tag has an invalid target object")
        sha = target.get("sha", "")
        if not isinstance(sha, str) or not SHA_PATTERN.fullmatch(sha):
            raise ReleaseError("The release tag has an invalid object SHA")
        if target.get("type") == "commit":
            return sha
        if target.get("type") != "tag" or sha in seen or len(seen) >= 10:
            raise ReleaseError("The release tag does not resolve to a commit")
        seen.add(sha)
        annotated_tag = github_api(f"{prefix}/git/tags/{sha}")
        target = annotated_tag.get("object") if isinstance(annotated_tag, dict) else None


def git_output(source, *arguments):
    try:
        return subprocess.run(
            ["git", "-C", str(source), *arguments],
            check=True, capture_output=True, text=True,
        ).stdout
    except subprocess.CalledProcessError as error:
        raise ReleaseError("Could not read the checked-out release commit") from error


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ReleaseError(f"Duplicate manifest key: {key}")
        result[key] = value
    return result


def validate_source(source, tag, commit):
    version = version_from_tag(tag)
    if not SHA_PATTERN.fullmatch(commit):
        raise ReleaseError("The resolved release commit is invalid")
    source = Path(source)
    if git_output(source, "rev-parse", "--verify", "HEAD").strip() != commit:
        raise ReleaseError("The checkout does not match the resolved release tag commit")

    contents = {}
    for name in ("gradle.properties", ".release-please-manifest.json"):
        try:
            contents[name] = (source / name).read_text(encoding="utf-8")
        except OSError as error:
            raise ReleaseError(f"Could not read {name} from the release checkout") from error
        if contents[name] != git_output(source, "show", f"{commit}:{name}"):
            raise ReleaseError(f"{name} changed after the release checkout")

    gradle_versions = []
    for line in contents["gradle.properties"].splitlines():
        key, separator, value = line.partition("=")
        if separator and key.strip() == "version":
            gradle_versions.append(value.strip())
    if gradle_versions != [version]:
        raise ReleaseError(f"gradle.properties must declare version={version} exactly once")

    try:
        manifest = json.loads(
            contents[".release-please-manifest.json"], object_pairs_hook=unique_object
        )
    except json.JSONDecodeError as error:
        raise ReleaseError("The release manifest is not valid JSON") from error
    if not isinstance(manifest, dict) or manifest.get(".") != version:
        raise ReleaseError(f"The release manifest must declare the root version as {version}")
    return version


def write_outputs(values):
    output_path = os.environ.get("GITHUB_OUTPUT")
    if output_path:
        with open(output_path, "a", encoding="utf-8") as output:
            for name, value in values.items():
                output.write(f"{name}={value}\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    resolve = commands.add_parser("resolve")
    resolve.add_argument("--repository", required=True)
    resolve.add_argument("--event", required=True)
    resolve.add_argument("--retry-tag", default="")
    resolve.add_argument("--created-tag", default="")
    resolve.add_argument("--created-version", default="")
    validate = commands.add_parser("validate")
    validate.add_argument("--source", required=True)
    validate.add_argument("--tag", required=True)
    validate.add_argument("--commit", required=True)
    arguments = parser.parse_args()
    try:
        if arguments.command == "resolve":
            tag, version = select_tag(
                arguments.event, arguments.retry_tag,
                arguments.created_tag, arguments.created_version,
            )
            commit = resolve_release(arguments.repository, tag)
            write_outputs({"tag": tag, "version": version, "commit": commit})
            print(f"Resolved published release {tag} to {commit}")
        else:
            version = validate_source(arguments.source, arguments.tag, arguments.commit)
            write_outputs({"version": version})
            print(f"Validated release {arguments.tag} at {arguments.commit}")
    except (ReleaseError, OSError) as error:
        print(f"Release validation failed: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
