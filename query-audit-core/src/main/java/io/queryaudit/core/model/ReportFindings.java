package io.queryaudit.core.model;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Compatibility boundary for the nullable, enum-only report API predating open finding kinds. */
final class ReportFindings {
  private final Bucket confirmed;
  private final Bucket informational;
  private final Bucket acknowledged;

  private ReportFindings(Bucket confirmed, Bucket informational, Bucket acknowledged) {
    this.confirmed = confirmed;
    this.informational = informational;
    this.acknowledged = acknowledged;
  }

  static ReportFindings legacy(List<Issue> confirmed, List<Issue> info, List<Issue> acknowledged) {
    return new ReportFindings(
        Bucket.legacy(confirmed), Bucket.legacy(info), Bucket.legacy(acknowledged));
  }

  static ReportFindings from(AuditFindings findings) {
    return new ReportFindings(
        Bucket.from(findings.confirmed()),
        Bucket.from(findings.informational()),
        Bucket.from(findings.acknowledged()));
  }

  ReportFindings withCustom(
      List<Finding> confirmed, List<Finding> info, List<Finding> acknowledged) {
    return new ReportFindings(
        this.confirmed.withCustom(confirmed),
        informational.withCustom(info),
        this.acknowledged.withCustom(acknowledged));
  }

  ReportFindings withInformational(List<Finding> findings) {
    return new ReportFindings(confirmed, Bucket.from(findings), acknowledged);
  }

  AuditFindings view() {
    return new AuditFindings(confirmed.all(), informational.all(), acknowledged.all());
  }

  List<Finding> confirmed() {
    return confirmed.all();
  }

  List<Finding> informational() {
    return informational.all();
  }

  List<Finding> acknowledged() {
    return acknowledged.all();
  }

  boolean hasConfirmed() {
    return (confirmed.legacy != null && !confirmed.legacy.isEmpty()) || !confirmed.custom.isEmpty();
  }

  List<Issue> legacyConfirmed() {
    return confirmed.legacy;
  }

  List<Issue> legacyInfo() {
    return informational.legacy;
  }

  List<Issue> legacyAcknowledged() {
    return acknowledged.legacy == null ? List.of() : acknowledged.legacy;
  }

  List<Finding> customConfirmed() {
    return confirmed.custom;
  }

  List<Finding> customInfo() {
    return informational.custom;
  }

  List<Finding> customAcknowledged() {
    return acknowledged.custom;
  }

  private static final class Bucket {
    private final List<Issue> legacy;
    private final List<Finding> custom;
    private final List<Finding> canonical;

    private Bucket(List<Issue> legacy, List<Finding> custom, List<Finding> canonical) {
      this.legacy = legacy;
      this.custom = custom;
      this.canonical = canonical;
    }

    static Bucket legacy(List<Issue> issues) {
      // Legacy constructors accepted null lists/elements. Preserve that boundary, and defer
      // validation until a caller explicitly requests the non-null open finding view.
      List<Issue> snapshot =
          issues == null ? null : Collections.unmodifiableList(new ArrayList<>(issues));
      return new Bucket(snapshot, List.of(), null);
    }

    static Bucket from(List<Finding> findings) {
      List<Finding> snapshot = List.copyOf(findings);
      List<Issue> legacy = new ArrayList<>();
      List<Finding> custom = new ArrayList<>();
      for (Finding finding : snapshot) {
        finding.toIssue().ifPresentOrElse(legacy::add, () -> custom.add(finding));
      }
      return new Bucket(List.copyOf(legacy), List.copyOf(custom), snapshot);
    }

    Bucket withCustom(List<Finding> findings) {
      return new Bucket(legacy, List.copyOf(findings), null);
    }

    List<Finding> all() {
      if (canonical != null) return canonical;
      List<Finding> all = new ArrayList<>();
      if (legacy != null) legacy.forEach(issue -> all.add(Finding.fromIssue(issue)));
      all.addAll(custom);
      return List.copyOf(all);
    }
  }
}
