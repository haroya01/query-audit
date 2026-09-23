"""Executable negative controls: compile real before/after API mutations without dependencies."""

import importlib.util
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path


SCRIPT = Path(__file__).resolve().parents[1] / "check_public_api.py"
SPEC = importlib.util.spec_from_file_location("check_public_api", SCRIPT)
api = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(api)


class PublicApiCompatibilityTest(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(prefix="query-audit-api-control-")
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.counter = 0
        self.javac = shutil.which("javac")
        if self.javac is None:
            self.fail("The API compatibility negative controls require a JDK (javac)")

    def compile(self, source):
        self.counter += 1
        directory = self.root / str(self.counter)
        directory.mkdir()
        java = directory / "Api.java"
        java.write_text(source, encoding="utf-8")
        result = subprocess.run([self.javac, "--release", "17", "-d", str(directory), str(java)],
            capture_output=True, text=True, check=False)
        self.assertEqual(0, result.returncode, result.stderr)
        return api.snapshot([directory], restrict_scope=False)

    def compare(self, before, after):
        return api.differences(self.compile(before), self.compile(after))

    def test_additive_methods_fields_and_default_interface_methods_are_allowed(self):
        self.assertEqual([], self.compare(
            "public interface Api { String find(int id); }",
            "public interface Api { String find(int id); int LIMIT = 5; default boolean ready() { return true; } static Api empty() { return null; } }"))

    def test_removal_and_descriptor_changes_fail(self):
        before = "public class Api { public String find(int id) { return null; } }"
        baseline = self.compile(before)
        for source in (
            "public class Api {}",
            "public class Api { public String find(String id) { return null; } }",
            "public class Api { public Object find(int id) { return null; } }",
            "public class Api { private String find(int id) { return null; } }",
        ):
            with self.subTest(source=source):
                self.assertTrue(any("descriptor changed" in item for item in api.differences(baseline, self.compile(source))))

    def test_generic_signature_change_fails_even_when_the_jvm_descriptor_is_unchanged(self):
        failures = self.compare(
            "public interface Api { java.util.List<String> values(); }",
            "public interface Api { java.util.List<Long> values(); }")
        self.assertTrue(any("signature changed" in item for item in failures), failures)

    def test_static_final_and_visibility_method_changes_fail(self):
        baseline = self.compile("public class Api { public String find() { return null; } }")
        for modifier in ("public static", "public final", "protected"):
            with self.subTest(modifier=modifier):
                failures = api.differences(baseline, self.compile(f"public class Api {{ {modifier} String find() {{ return null; }} }}"))
                self.assertTrue(any("flags changed" in item for item in failures), failures)

    def test_class_finality_and_superclass_changes_fail(self):
        self.assertTrue(any("class flags changed" in item for item in self.compare("public class Api {}", "public final class Api {}")))
        self.assertTrue(any("superclass changed" in item for item in self.compare(
            "class Before {} class After {} public class Api extends Before {}",
            "class Before {} class After {} public class Api extends After {}")))

    def test_adding_an_abstract_method_breaks_existing_implementers(self):
        failures = self.compare("public interface Api { void run(); }", "public interface Api { void run(); void added(); }")
        self.assertTrue(any("new abstract method" in item for item in failures), failures)

    def test_default_annotation_addition_is_allowed_but_mandatory_addition_or_default_removal_is_not(self):
        baseline = self.compile("public @interface Api { String value() default \"one\"; }")
        self.assertEqual([], api.differences(baseline, self.compile("public @interface Api { String value() default \"one\"; String[] added() default {}; }")))
        failures = api.differences(baseline, self.compile("public @interface Api { String value(); String added(); }"))
        self.assertTrue(any("default removed" in item for item in failures), failures)
        self.assertTrue(any("new abstract method" in item for item in failures), failures)

    def test_public_field_type_and_staticness_changes_fail(self):
        baseline = self.compile("public class Api { public String value; }")
        for source in ("public class Api { public Object value; }", "public class Api { public static String value; }"):
            with self.subTest(source=source):
                self.assertTrue(api.differences(baseline, self.compile(source)))

    def test_checked_exception_contract_changes_fail(self):
        failures = self.compare("public interface Api { void run(); }", "public interface Api { void run() throws java.io.IOException; }")
        self.assertTrue(any("exceptions changed" in item for item in failures), failures)

    def test_nested_staticness_is_read_from_innerclasses_and_inaccessible_helpers_are_ignored(self):
        before = "public class Api { public static class Nested {} private class Hidden { public class Unreachable {} } }"
        after = "public class Api { public class Nested {} }"
        baseline = self.compile(before)
        self.assertNotIn("Api$Hidden", baseline)
        self.assertNotIn("Api$Hidden$Unreachable", baseline)
        failures = api.differences(baseline, self.compile(after))
        self.assertTrue(any("Api$Nested: class flags changed" in item for item in failures), failures)

    def test_new_public_types_are_allowed_and_missing_old_types_are_rejected(self):
        before = self.compile("public class Api {}")
        after = self.compile("public class Api { public static class Added {} }")
        self.assertEqual([], api.differences(before, after))
        self.assertTrue(any("type removed" in item for item in api.differences(after, before)))

    def test_empty_and_truncated_inputs_fail_closed(self):
        with self.assertRaisesRegex(ValueError, "empty compatibility snapshot"):
            api.snapshot([self.root], restrict_scope=False)
        bad = self.root / "Api.class"
        bad.write_bytes(b"\xca\xfe\xba\xbe")
        with self.assertRaisesRegex(ValueError, "Truncated"):
            api.read_class(bad)


if __name__ == "__main__":
    unittest.main()
