# API compatibility and deprecation policy

This page states what QueryAudit promises about compatibility, how a deprecation happens, and how the
promise is enforced in CI. See [Supported public API](supported-api.md) for *which* APIs are covered.

## The promise

For every release:

| Change | Allowed in a patch or minor release? |
| --- | --- |
| Add a method, class, or annotation attribute **with a default** | Yes |
| Add a field to a report schema and bump the schema version | Yes |
| Add a new configuration key | Yes |
| Change a Javadoc comment or an implementation detail | Yes |
| Remove or rename a supported type or member | No |
| Change a supported signature, return type, generic bound, or checked exception | No |
| Remove or change an annotation default | No |
| Add an abstract method to a supported interface or SPI | No |
| Change a documented configuration default | No |
| Remove or rename a `query-audit.*` key | No |
| Change a CLI exit code or its meaning | No |
| Remove a field from a 1.x report schema | No |

Additions are always safe for a consumer; removals and changes are not. The rule the project applies
is therefore: **add freely on the supported surface, remove only at a major release.**

Pre-1.0 the project is still finding its shape, so a breaking change may still ship in a minor
release. Each one is recorded with a `BREAKING CHANGE:` footer and a Conventional Commits `feat` or
`fix`, which Release Please turns into a version bump and a changelog entry. After 1.0, a breaking
change to the supported surface requires a major version.

The floor does not protect behavior, only signatures. A bug fix may change what a supported method
does. Report schema semantics are protected separately, by the schema version.

## How the floor is enforced

`config/public-api/<revision>.json` is a versioned snapshot of the supported surface, recorded from
an exact commit. It is a floor: normal builds read it and never rewrite it.

The check compares the compiled `query-audit-core`, `query-audit-junit5`, and
`query-audit-spring-boot-starter` classes against that snapshot and fails on a removed type, a
removed or retyped member, a changed class flag, superclass, generic signature, or declared checked
exception, a removed interface, a removed annotation default, and a new abstract method on a
supported interface. Additions pass.

```bash
./gradlew checkPublicApiCompatibility
```

It runs in CI as its own **Public API compatibility** step, and also as part of `build` through
`:query-audit-core:check`. The checker's own negative controls run first, so a checker that has
stopped detecting breakage fails the build before it can pass a real break.

The scope lives in one place, `PREFIXES`, `ENTRY_POINTS`, and `EXCLUDED_PREFIXES` in
`.github/scripts/check_public_api.py`, and matches the surface in
[Supported public API](supported-api.md).

### Changing the floor

The floor is a deliberate maintainer operation, never a side effect of a normal build:

```bash
python3 .github/scripts/snapshot_public_api_from_git.py \
    --revision <commit> --classpath "<dependency classpath>" > config/public-api/<short-sha>.json
```

Two rules when re-recording it:

1. **Keep every type the previous floor covered.** The floor may grow. Dropping a type withdraws a
   guarantee silently, so it needs its own commit and a reason in the message.
2. **Never re-record to make a break pass.** If the check fails, the fix is to restore the
   signature or to ship a major version. Re-recording over a real break converts a visible failure
   into an invisible promise you did not keep.

The `AuditRunResult.contractViolations` change is a worked example of the second rule: adding a
record component removes the previous canonical constructor descriptor, so the check fails until a
compatibility constructor is added or the break is accepted deliberately.

### Why the floor is not the whole policy

The floor sees JVM descriptors. It does not see behavior, inherited-member equivalence, generic
type-inference edge cases, annotation values, or resource and schema files. It also cannot see a
supported API in a module the check does not compile, which is why the Spring Boot starter is
compiled explicitly. Treat a green check as a floor, not as proof.

## Deprecation policy

A supported API is deprecated before it is removed, and never removed in the same release it is
deprecated in.

| Stage | Requirement |
| --- | --- |
| Announce | `@Deprecated(since = "<version>")` plus a Javadoc line naming the replacement. |
| Compile warnings | Deprecation warnings are enabled for QueryAudit's own build; a deprecated member used internally needs a reason. |
| Report | The deprecation appears in the changelog with the issue that announced it. |
| Migrate | The replacement is documented before, or in the same release as, the deprecation. |
| Remove | Only at a major release, and only after at least one **minor** release has carried the deprecation. |

Minimum lifetime between `@Deprecated` and removal is one minor release, which given the project's
cadence is a minimum of several weeks and normally longer.

The project already deprecates this way. `@DetectNPlusOne`, `@ExpectMaxQueryCount`, and query-count
baselines carry `@Deprecated(since = "0.7.0")` and are still supported, not removed.

Deprecated APIs keep working exactly as before. A deprecation is never a behavior change.

### What is not deprecated

| Type | Reason |
| --- | --- |
| Internal types | Never deprecated, because they were never supported. Use the supported replacement instead. |
| Report schema fields | Deprecated by the schema version that stops writing them, not by a Java annotation. |
| Configuration keys | An earlier name is deprecated through the "earlier names, still accepted" column in [Setting names](../guide/configuration.md#setting-names). The old name keeps working. |

## Migration policy

Every release that requires user action says so in three places, and all three are required:

1. **Changelog.** A `BREAKING CHANGE:` footer, surfaced by Release Please in `CHANGELOG.md`.
2. **Migration notes.** A page under `docs/getting-started/`, one per release line that needs it,
   listing each change and the exact edit required.
3. **Version page.** A row in [Versions and compatibility](../getting-started/versions.md) pointing
   at the notes.

A migration note is a table with a **Change** column and a **What to do** column. "What to do" must
be actionable: a setting to change, an annotation to swap, or explicitly "None". "Review the
changes" is not sufficient on its own.

The same notes are mirrored in the [compatibility changelog](api-changelog.md), which is organised
by API rather than by release, for consumers who need to know when a specific type changed.

## Adding a new API

1. Confirm it is on the [supported surface](supported-api.md). If not, do not add it to a supported
   package.
2. Add it. Additions need no floor change; the check permits them.
3. Re-record the floor only if the addition should be protected immediately. It is protected from the
   next re-recording either way, so prefer leaving the floor alone until a release needs one.
4. Document it, and update the floor in its own commit with a reason.

## See also

* [Supported public API](supported-api.md)
* [Compatibility changelog since 0.5](api-changelog.md)
* [Maintaining the Codebase](maintaining.md)
