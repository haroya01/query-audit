#!/usr/bin/env python3
"""Verify the published example rejects an added write or SELECT, then passes.

Both fresh JUnit XML and the audit JSON must agree with the Gradle exit status.
A compilation, dependency-resolution, or test-startup failure is never an expected pass.
This standalone example consumes Maven Central artifacts, not the repository's source build.
"""

import json
from collections import Counter
from pathlib import Path
import shutil
import subprocess
import sys
import xml.etree.ElementTree as ET


PUBLISHED_VERSION = "0.6.0"
TEST_CLASS = "example.audit.FirstAuditTest"
TEST_METHOD = "readsOnce()"
TEST_ID = f"[engine:junit-jupiter]/[class:{TEST_CLASS}]/[method:{TEST_METHOD}]"


class VerificationError(Exception):
    pass


def require(condition, message):
    if not condition:
        raise VerificationError(message)


def verify_junit(path, extra_query, extra_write=False):
    require(path.is_file(), "Fresh JUnit XML is missing; the example may not have run")
    suite = ET.parse(path).getroot()
    expected_failures = "1" if extra_query or extra_write else "0"
    require(
        suite.tag == "testsuite"
        and suite.get("name") == TEST_CLASS
        and suite.get("tests") == "1"
        and suite.get("failures") == expected_failures
        and suite.get("errors") == "0"
        and suite.get("skipped") == "0",
        "JUnit must report exactly one executed example test with the expected result",
    )
    cases = suite.findall("testcase")
    require(
        len(cases) == 1
        and cases[0].get("classname") == TEST_CLASS
        and cases[0].get("name") == TEST_METHOD,
        "JUnit did not execute FirstAuditTest.readsOnce()",
    )
    case = cases[0]
    require(
        not case.findall("error") and not case.findall("skipped"),
        "The example test errored or was skipped",
    )
    failures = case.findall("failure")
    require(len(failures) == int(expected_failures), "Unexpected JUnit failure count")
    if extra_query or extra_write:
        message = failures[0].get("message", "")
        budget = "UPDATE: executed 1, expected at most 0." if extra_write else "SELECT: executed 2, expected at most 1."
        sql = "UPDATE orders SET status = 'NEW' WHERE id = 1" if extra_write else "SELECT status FROM orders WHERE id = 1 LIMIT 1"
        require(
            "QueryAudit: readsOnce() exceeded its query budget." in message
            and budget in message
            and sql in message,
            "The failing test must report the intended budget violation and its SQL",
        )


def verify_report(path, extra_query, extra_write=False):
    require(path.is_file(), "Fresh QueryAudit report.json is missing")
    envelope = json.loads(path.read_text(encoding="utf-8"))
    require(isinstance(envelope, dict), "Expected a QueryAudit JSON report envelope")
    expected_outcome = "FAIL" if extra_query or extra_write else "PASS"
    expected_queries = 1 + int(extra_query) + int(extra_write)
    require(envelope.get("outcome") == expected_outcome, f"Expected audit outcome {expected_outcome}")
    require(envelope.get("incompleteReasons") == [], "The audit must be complete")
    reports = envelope.get("reports")
    require(isinstance(reports, list) and len(reports) == 1, "Expected exactly one audited test")
    report = reports[0]
    require(isinstance(report, dict) and report.get("testId") == TEST_ID, "Wrong audited test identity")
    summary = report.get("summary")
    require(
        isinstance(summary, dict) and summary.get("totalQueries") == expected_queries,
        f"Expected {expected_queries} captured queries in the audit summary",
    )
    queries = report.get("queries")
    require(
        isinstance(queries, list) and len(queries) == expected_queries,
        f"Expected evidence for all {expected_queries} captured queries",
    )
    expected_counts = {"SELECT": 1 + int(extra_query)}
    if extra_write:
        expected_counts["UPDATE"] = 1
    actual_counts = Counter(query.get("sql", "").split(" ", 1)[0].upper() for query in queries)
    require(actual_counts == expected_counts, f"Expected statement counts {expected_counts}, got {actual_counts}")
    for query in queries:
        method = "writeOnReadPath" if query["sql"].startswith("UPDATE") else "readOne"
        require(
            f"{TEST_CLASS}.{method}:" in query.get("stackTrace", ""),
            f"Expected the application call site for {method} in query evidence",
        )
    evidence = report.get("queryEvidence")
    require(
        isinstance(evidence, dict) and evidence.get("status") == "COMPLETE",
        "Query evidence must be complete",
    )
    inputs = envelope.get("comparisonInputs")
    require(isinstance(inputs, dict), "Published artifact identity is missing")
    test_inputs = inputs.get(TEST_ID)
    require(
        isinstance(test_inputs, dict) and test_inputs.get("queryAuditVersion") == PUBLISHED_VERSION,
        f"The example must consume the pinned published QueryAudit {PUBLISHED_VERSION}",
    )


def run_case(repository, extra_query=False, extra_write=False):
    sample = repository / "examples" / "first-audit"
    report = sample / "build" / "reports" / "query-audit" / "report.json"
    junit = sample / "build" / "test-results" / "test" / f"TEST-{TEST_CLASS}.xml"
    case_name = "unexpected-write" if extra_write else "budget-failure" if extra_query else "within-budget"
    archive = sample / "build" / "reports" / "first-audit-verification" / case_name
    archive.mkdir(parents=True, exist_ok=True)
    for path in (report, junit, archive / "report.json", archive / "junit.xml", archive / "gradle.log"):
        path.unlink(missing_ok=True)

    command = [
        str(repository / "gradlew"), "-p", str(sample), "test",
        "--tests", TEST_CLASS, "--rerun-tasks", "--no-build-cache", "--console=plain",
    ]
    if extra_query:
        command.append("-PextraQuery=true")
    if extra_write:
        command.append("-PextraWrite=true")
    print(f"Verifying first-audit: {case_name}", flush=True)
    with (archive / "gradle.log").open("w", encoding="utf-8") as log:
        result = subprocess.run(
            command, cwd=repository, stdout=log, stderr=subprocess.STDOUT, timeout=300,
        )

    # Preserve every run, including intentional failures, before the next run overwrites it.
    for source, destination in ((report, "report.json"), (junit, "junit.xml")):
        if source.is_file():
            shutil.copy2(source, archive / destination)

    expected_exit = 1 if extra_query or extra_write else 0
    try:
        require(result.returncode == expected_exit, f"Expected Gradle exit {expected_exit}, got {result.returncode}")
        verify_junit(junit, extra_query, extra_write)
        verify_report(report, extra_query, extra_write)
    except (VerificationError, OSError, ValueError, ET.ParseError):
        print(f"Gradle output: {archive / 'gradle.log'}", file=sys.stderr)
        print((archive / "gradle.log").read_text(encoding="utf-8")[-12000:], file=sys.stderr)
        raise
    outcome = "FAIL" if extra_query or extra_write else "PASS"
    counts = f"{1 + int(extra_query)} SELECT(s), {int(extra_write)} UPDATE(s)"
    print(f"Verified {case_name}: Gradle exit {expected_exit}, JUnit result, audit {outcome}, {counts}, application call sites")


def main():
    repository = Path(__file__).resolve().parents[2]
    try:
        run_case(repository, extra_write=True)
        run_case(repository, extra_query=True)
        run_case(repository, extra_query=False)
    except (VerificationError, OSError, ValueError, ET.ParseError, subprocess.TimeoutExpired) as error:
        print(f"First-audit verification failed: {error}", file=sys.stderr)
        return 1
    print(f"Published QueryAudit {PUBLISHED_VERSION}: unexpected-write failure, extra-query failure, and passing example verified")
    return 0


if __name__ == "__main__":
    sys.exit(main())
