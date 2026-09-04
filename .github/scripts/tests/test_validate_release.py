import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch


SCRIPT = Path(__file__).resolve().parents[1] / "validate_release.py"
SPEC = importlib.util.spec_from_file_location("validate_release", SCRIPT)
release = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(release)

COMMIT = "a" * 40
TAG_OBJECT = "b" * 40
PREFIX = "repos/example/query-audit"


class ReleaseResolutionTest(unittest.TestCase):
    def api_responses(self, tag="v0.7.0", target=None):
        encoded = release.quote(tag, safe="")
        return {
            f"{PREFIX}/releases/tags/{encoded}": {"tag_name": tag, "draft": False},
            f"{PREFIX}/git/ref/tags/{encoded}": {
                "ref": f"refs/tags/{tag}",
                "object": target or {"type": "commit", "sha": COMMIT},
            },
        }

    def resolve(self, responses, tag="v0.7.0"):
        with patch.object(release, "github_api", side_effect=responses.__getitem__) as api:
            result = release.resolve_release("example/query-audit", tag)
        return result, [call.args[0] for call in api.call_args_list]

    def test_manual_retry_uses_only_requested_tag_even_on_another_branch(self):
        with patch.dict(os.environ, {"GITHUB_REF": "refs/heads/stale-release"}):
            tag, version = release.select_tag("workflow_dispatch", "v0.5.0", "v0.6.0", "0.6.0")
        self.assertEqual((tag, version), ("v0.5.0", "0.5.0"))
        commit, endpoints = self.resolve(self.api_responses(tag), tag)
        self.assertEqual(commit, COMMIT)
        self.assertTrue(all("stale-release" not in endpoint for endpoint in endpoints))

    def test_automatic_publish_uses_release_please_tag(self):
        self.assertEqual(
            release.select_tag("push", "v9.9.9", "v0.7.0", "0.7.0"),
            ("v0.7.0", "0.7.0"),
        )

    def test_automatic_tag_requires_matching_release_please_version(self):
        for version in ("", "0.6.0"):
            with self.subTest(version=version), self.assertRaises(release.ReleaseError):
                release.select_tag("push", "", "v0.7.0", version)

    def test_rejects_branches_missing_tags_and_output_injection_before_api_access(self):
        for tag in ("", "main", "refs/tags/v0.7.0", "v0.7.0\ncommit=bad", "v0.7.0;echo bad"):
            with self.subTest(tag=tag), patch.object(release, "github_api") as api:
                with self.assertRaises(release.ReleaseError):
                    release.resolve_release("example/query-audit", tag)
                api.assert_not_called()

    def test_rejects_unsupported_event(self):
        with self.assertRaises(release.ReleaseError):
            release.select_tag("pull_request", "v0.7.0", "v0.7.0", "0.7.0")

    def test_requires_an_existing_published_github_release(self):
        with patch.object(release, "github_api", side_effect=release.ReleaseError("not found")):
            with self.assertRaises(release.ReleaseError):
                release.resolve_release("example/query-audit", "v0.7.0")
        for metadata in (
            {"tag_name": "v0.7.0", "draft": True},
            {"tag_name": "v0.6.0", "draft": False},
            None,
        ):
            responses = self.api_responses()
            responses[f"{PREFIX}/releases/tags/v0.7.0"] = metadata
            with self.subTest(metadata=metadata), self.assertRaises(release.ReleaseError):
                self.resolve(responses)

    def test_resolves_lightweight_and_annotated_tags_to_commits(self):
        self.assertEqual(self.resolve(self.api_responses())[0], COMMIT)
        responses = self.api_responses(target={"type": "tag", "sha": TAG_OBJECT})
        responses[f"{PREFIX}/git/tags/{TAG_OBJECT}"] = {
            "object": {"type": "commit", "sha": COMMIT}
        }
        self.assertEqual(self.resolve(responses)[0], COMMIT)

    def test_accepts_published_prerelease_tags(self):
        tag = "v0.7.0-rc.1+build.2"
        self.assertEqual(self.resolve(self.api_responses(tag), tag)[0], COMMIT)

    def test_rejects_mismatched_refs_invalid_objects_and_tag_cycles(self):
        for target in (
            {"type": "tree", "sha": COMMIT},
            {"type": "commit", "sha": "main"},
            {"type": "commit", "sha": None},
        ):
            with self.subTest(target=target), self.assertRaises(release.ReleaseError):
                self.resolve(self.api_responses(target=target))
        responses = self.api_responses()
        responses[f"{PREFIX}/git/ref/tags/v0.7.0"]["ref"] = "refs/heads/v0.7.0"
        with self.assertRaises(release.ReleaseError):
            self.resolve(responses)
        responses = self.api_responses(target={"type": "tag", "sha": TAG_OBJECT})
        responses[f"{PREFIX}/git/tags/{TAG_OBJECT}"] = {
            "object": {"type": "tag", "sha": TAG_OBJECT}
        }
        with self.assertRaises(release.ReleaseError):
            self.resolve(responses)


class ReleaseCheckoutTest(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.source = Path(self.directory.name)
        self.git("init", "--quiet")

    def git(self, *arguments):
        return subprocess.run(
            ["git", "-C", str(self.source), *arguments],
            check=True, capture_output=True, text=True,
        ).stdout.strip()

    def commit_files(self, gradle="version=0.7.0\n", manifest='{".":"0.7.0"}\n'):
        (self.source / "gradle.properties").write_text(gradle)
        (self.source / ".release-please-manifest.json").write_text(manifest)
        self.git("add", "gradle.properties", ".release-please-manifest.json")
        self.git("-c", "user.name=Release Test", "-c", "user.email=release-test@example.invalid",
                 "-c", "commit.gpgsign=false", "commit", "--quiet", "-m", "Release fixture")
        return self.git("rev-parse", "HEAD")

    def test_matching_tag_checkout_gradle_and_manifest_are_accepted(self):
        commit = self.commit_files(gradle="# marker\nversion = 0.7.0\ngroup=example\n")
        self.assertEqual(release.validate_source(self.source, "v0.7.0", commit), "0.7.0")

    def test_rejects_a_different_checkout_commit(self):
        self.commit_files()
        with self.assertRaisesRegex(release.ReleaseError, "checkout does not match"):
            release.validate_source(self.source, "v0.7.0", COMMIT)

    def test_rejects_gradle_and_manifest_mismatches(self):
        for gradle, manifest in (
            ("version=0.6.0\n", '{".":"0.7.0"}'),
            ("version=0.7.0\n", '{".":"0.6.0"}'),
            ("version=0.6.0\n", '{".":"0.6.0"}'),
        ):
            with self.subTest(gradle=gradle, manifest=manifest):
                commit = self.commit_files(gradle, manifest)
                with self.assertRaises(release.ReleaseError):
                    release.validate_source(self.source, "v0.7.0", commit)

    def test_rejects_ambiguous_or_missing_gradle_version(self):
        for gradle in ("group=example\n", "version=0.7.0\nversion=0.7.0\n"):
            with self.subTest(gradle=gradle):
                commit = self.commit_files(gradle=gradle)
                with self.assertRaises(release.ReleaseError):
                    release.validate_source(self.source, "v0.7.0", commit)

    def test_rejects_invalid_ambiguous_or_missing_manifest_version(self):
        for manifest in ("not JSON", "[]", "{}", '{".":"0.6.0",".":"0.7.0"}'):
            with self.subTest(manifest=manifest):
                commit = self.commit_files(manifest=manifest)
                with self.assertRaises(release.ReleaseError):
                    release.validate_source(self.source, "v0.7.0", commit)

    def test_build_cannot_rewrite_version_files_to_match_the_requested_tag(self):
        commit = self.commit_files(gradle="version=0.6.0\n", manifest='{".":"0.6.0"}')
        (self.source / "gradle.properties").write_text("version=0.7.0\n")
        (self.source / ".release-please-manifest.json").write_text(json.dumps({".": "0.7.0"}))
        with self.assertRaisesRegex(release.ReleaseError, "changed after the release checkout"):
            release.validate_source(self.source, "v0.7.0", commit)

    def test_cli_emits_only_the_validated_version(self):
        commit = self.commit_files()
        output = self.source / "github-output"
        result = subprocess.run(
            [sys.executable, str(SCRIPT), "validate", "--source", str(self.source),
             "--tag", "v0.7.0", "--commit", commit],
            env={**os.environ, "GITHUB_OUTPUT": str(output)},
            capture_output=True, text=True,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(output.read_text(), "version=0.7.0\n")

    def test_cli_wrong_checkout_fails_without_publishing_outputs(self):
        self.commit_files()
        output = self.source / "github-output"
        result = subprocess.run(
            [sys.executable, str(SCRIPT), "validate", "--source", str(self.source),
             "--tag", "v0.7.0", "--commit", COMMIT],
            env={**os.environ, "GITHUB_OUTPUT": str(output)},
            capture_output=True, text=True,
        )
        self.assertEqual(result.returncode, 1)
        self.assertIn("checkout does not match", result.stderr)
        self.assertFalse(output.exists())


if __name__ == "__main__":
    unittest.main()
