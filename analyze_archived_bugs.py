#!/usr/bin/env python3
"""
Analyze archived Jira bugs to categorize and extract insights.
"""

import json
import re
from datetime import datetime
from collections import Counter
from typing import Dict, List, Any, Optional
import statistics


# Category keywords mapping
CATEGORY_KEYWORDS = {
    "Expected Behavior": [
        "expected behavior",
        "expected behaviour",
        "working as expected",
        "by design",
        "not a bug",
        "this is not a bug",
        "it's a expected",
        "it's an expected",
        "this is expected",
    ],
    "Improvement Needed": [
        "improvement",
        "enhancement",
        "feature request",
        "improvement created",
        "improvement should be",
        "created an improvement",
    ],
    "User Error / Misuse": [
        "user error",
        "misuse",
        "incorrect usage",
        "user mistake",
        "misconfiguration",
        "wrong configuration",
    ],
    "Integration Issue": [
        "integration",
        "third-party",
        "external service",
        "aws outage",
        "dependency",
        "external issue",
    ],
    "Unreproducible / Invalid": [
        "unreproducible",
        "cannot reproduce",
        "can't reproduce",
        "unable to reproduce",
        "not reproducible",
        "invalid",
        "cannot replicate",
    ],
    "Tech Limitation / Edge Case": [
        "limitation",
        "edge case",
        "technical limitation",
        "not supported",
        "constraint",
        "technical constraint",
    ],
    "Resolved Elsewhere": [
        "duplicate",
        "duplicated",
        "resolved elsewhere",
        "fixed in",
        "already fixed",
        "resolved in another",
        "migration completed",
    ],
    "Unclear / Missing Context": [
        "unclear",
        "missing context",
        "insufficient information",
        "need more information",
        "more details needed",
        "cannot determine",
    ],
}


def parse_date(date_str: str) -> datetime | None:
    """Parse date string to datetime object."""
    if not date_str:
        return None
    try:
        # Handle ISO format with timezone
        return datetime.fromisoformat(date_str.replace("Z", "+00:00"))
    except Exception:
        return None


def calculate_resolution_days(created: str, resolved: str) -> float | None:
    """Calculate days between creation and resolution."""
    created_dt = parse_date(created)
    resolved_dt = parse_date(resolved)

    if not created_dt or not resolved_dt:
        return None

    delta = resolved_dt - created_dt
    return delta.total_seconds() / 86400  # Convert to days


def extract_keywords(text: str, min_length: int = 3) -> List[str]:
    """Extract meaningful keywords from text."""
    if not text:
        return []

    # Remove special characters and convert to lowercase
    cleaned = re.sub(r"[^a-zA-Z0-9\s]", " ", text.lower())

    # Split into words and filter
    words = [w for w in cleaned.split() if len(w) >= min_length]

    # Filter out common words
    stop_words = {
        "the",
        "is",
        "at",
        "which",
        "on",
        "and",
        "or",
        "but",
        "in",
        "with",
        "a",
        "an",
        "as",
        "are",
        "was",
        "were",
        "been",
        "be",
        "have",
        "has",
        "had",
        "do",
        "does",
        "did",
        "will",
        "would",
        "should",
        "could",
        "may",
        "might",
        "must",
        "can",
        "for",
        "of",
        "to",
        "from",
        "by",
        "about",
        "into",
        "through",
        "during",
        "before",
        "after",
        "above",
        "below",
        "between",
        "under",
        "again",
        "further",
        "then",
        "once",
        "here",
        "there",
        "when",
        "where",
        "why",
        "how",
        "all",
        "both",
        "each",
        "few",
        "more",
        "most",
        "other",
        "some",
        "such",
        "than",
        "too",
        "very",
        "this",
        "that",
        "these",
        "those",
    }

    return [w for w in words if w not in stop_words]


def categorize_issue(issue: Dict[str, Any]) -> tuple:
    """
    Categorize an issue based on summary, description, and comments.
    Returns (categories, archival_reason)
    """
    categories = []
    archival_reasons = []

    # Combine all text for analysis
    all_text = []
    all_text.append(issue.get("summary", ""))
    all_text.append(issue.get("description", ""))

    # Add comments
    for comment in issue.get("comments", []):
        all_text.append(comment.get("body", ""))

    combined_text = " ".join(all_text).lower()

    # Check each category
    for category, keywords in CATEGORY_KEYWORDS.items():
        for keyword in keywords:
            if keyword in combined_text:
                if category not in categories:
                    categories.append(category)
                archival_reasons.append(keyword)
                break

    # Default category if none found
    if not categories:
        categories.append("Unclear / Missing Context")

    return categories, "; ".join(set(archival_reasons)) or "no explicit reason found"


def analyze_bugs(data: Dict[str, Any]) -> Dict[str, Any]:
    """Main analysis function."""

    issues = data.get("issues", [])

    # Initialize counters
    category_counter: Counter[str] = Counter()
    component_counter: Counter[str] = Counter()
    assignee_counter: Counter[str] = Counter()
    resolution_times: list[float] = []
    all_keywords: list[str] = []

    # Analyzed issues list
    analyzed_issues = []

    for issue in issues:
        # Categorize
        categories, archival_reason = categorize_issue(issue)

        # Count categories
        for cat in categories:
            category_counter[cat] += 1

        # Count components
        for comp in issue.get("components", []):
            component_counter[comp] += 1

        # Count assignees
        assignee = issue.get("assignee") or "Unassigned"
        assignee_counter[assignee] += 1

        # Calculate resolution time
        resolution_days = calculate_resolution_days(issue.get("created_date"), issue.get("resolution_date"))
        if resolution_days is not None:
            resolution_times.append(resolution_days)

        # Extract keywords
        text_for_keywords = f"{issue.get('summary', '')} {issue.get('description', '')}"
        keywords = extract_keywords(text_for_keywords)
        all_keywords.extend(keywords)

        # Build analyzed issue
        analyzed_issue = {
            "issue_key": issue.get("issue_key"),
            "summary": issue.get("summary"),
            "categories": categories,
            "archival_reason": archival_reason,
            "components": issue.get("components", []),
            "assignee": assignee,
            "resolution_time_days": round(resolution_days, 2) if resolution_days else None,
            "team": issue.get("team"),
            "created_date": issue.get("created_date"),
            "resolution_date": issue.get("resolution_date"),
            "labels": issue.get("labels", []),
            "status": issue.get("status"),
        }

        analyzed_issues.append(analyzed_issue)

    # Calculate statistics
    avg_resolution = statistics.mean(resolution_times) if resolution_times else 0
    median_resolution = statistics.median(resolution_times) if resolution_times else 0

    # Top keywords
    keyword_counter = Counter(all_keywords)
    top_keywords = keyword_counter.most_common(20)

    # Build summary
    summary = {
        "total_issues": len(issues),
        "category_distribution": dict(category_counter.most_common()),
        "average_resolution_days": round(avg_resolution, 2),
        "median_resolution_days": round(median_resolution, 2),
        "top_components": [{"component": comp, "count": count} for comp, count in component_counter.most_common(3)],
        "top_assignees": [
            {"assignee": assignee, "count": count} for assignee, count in assignee_counter.most_common(5)
        ],
        "resolution_time_stats": {
            "min_days": round(min(resolution_times), 2) if resolution_times else 0,
            "max_days": round(max(resolution_times), 2) if resolution_times else 0,
            "avg_days": round(avg_resolution, 2),
            "median_days": round(median_resolution, 2),
            "std_dev_days": round(statistics.stdev(resolution_times), 2) if len(resolution_times) > 1 else 0,
        },
        "top_keywords": [{"keyword": kw, "count": count} for kw, count in top_keywords],
        "team_distribution": dict(Counter([i.get("team") for i in issues]).most_common()),
        "time_window": data.get("time_window", {}),
    }

    # Build final report
    report = {
        "analysis_metadata": {
            "analysis_timestamp": datetime.now().isoformat(),
            "source_file": "snapshot_CWS_bug_20251027_165717.json",
            "project_key": data.get("project_key"),
            "issue_type": data.get("issue_type"),
        },
        "summary": summary,
        "issues": analyzed_issues,
        "insights": generate_insights(summary, analyzed_issues),
    }

    return report


def generate_insights(summary: Dict[str, Any], issues: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Generate actionable insights from the analysis."""

    insights: Dict[str, List[str]] = {"key_findings": [], "recommendations": [], "patterns": []}

    # Category analysis
    cat_dist = summary["category_distribution"]
    total = summary["total_issues"]

    for category, count in cat_dist.items():
        percentage = (count / total) * 100
        if percentage >= 20:
            insights["key_findings"].append(f"{category}: {count} issues ({percentage:.1f}%) - significant volume")

    # Expected behavior analysis
    if cat_dist.get("Expected Behavior", 0) > 5:
        insights["recommendations"].append(
            "High volume of 'Expected Behavior' bugs suggests need for better documentation "
            "or user training to prevent false bug reports"
        )

    # Improvement analysis
    if cat_dist.get("Improvement Needed", 0) > 3:
        insights["recommendations"].append(
            "Consider creating a separate improvement/enhancement ticket type to reduce bug backlog pollution"
        )

    # Unassigned issues
    unassigned_count = sum(1 for i in issues if i["assignee"] == "Unassigned")
    if unassigned_count > 5:
        insights["key_findings"].append(f"{unassigned_count} issues were unassigned - may indicate triage issues")

    # Resolution time
    avg_days = summary["average_resolution_days"]
    if avg_days > 7:
        insights["patterns"].append(
            f"Average resolution time of {avg_days:.1f} days suggests archived bugs "
            "take longer than typical fixes to categorize"
        )

    # Component concentration
    top_component = summary["top_components"][0] if summary["top_components"] else None
    if top_component and top_component["count"] > total * 0.3:
        insights["patterns"].append(
            f"Component '{top_component['component']}' has {top_component['count']} bugs "
            f"({(top_component['count'] / total) * 100:.1f}%) - may need focused attention"
        )

    return insights


def main():
    """Main execution function."""

    # Load data
    input_file = "output/issue-snapshot_20251027/snapshot_CWS_bug_20251027_165717.json"

    print(f"Loading data from {input_file}...")
    with open(input_file, "r") as f:
        data = json.load(f)

    print(f"Analyzing {data.get('total_issues', 0)} archived bugs...")

    # Analyze
    report = analyze_bugs(data)

    # Save report
    output_file = "output/issue-snapshot_20251027/archived_bugs_analysis_report.json"

    with open(output_file, "w") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)

    print(f"\n✅ Analysis complete! Report saved to: {output_file}")

    # Print summary
    print("\n" + "=" * 80)
    print("📊 ANALYSIS SUMMARY")
    print("=" * 80)

    summary = report["summary"]

    print(f"\n📋 Total Issues Analyzed: {summary['total_issues']}")
    print(f"⏱️  Average Resolution Time: {summary['average_resolution_days']:.1f} days")
    print(f"⏱️  Median Resolution Time: {summary['median_resolution_days']:.1f} days")

    print("\n📊 Category Distribution:")
    for category, count in summary["category_distribution"].items():
        percentage = (count / summary["total_issues"]) * 100
        bar = "█" * int(percentage / 2)
        print(f"  {category:30s}: {count:3d} ({percentage:5.1f}%) {bar}")

    print("\n🏆 Top 3 Components:")
    for i, comp in enumerate(summary["top_components"], 1):
        print(f"  {i}. {comp['component']:30s}: {comp['count']} issues")

    print("\n👥 Top 5 Assignees:")
    for i, assignee in enumerate(summary["top_assignees"], 1):
        print(f"  {i}. {assignee['assignee']:30s}: {assignee['count']} issues")

    print("\n🔑 Top 10 Keywords:")
    for i, kw in enumerate(summary["top_keywords"][:10], 1):
        print(f"  {i:2d}. {kw['keyword']:20s}: {kw['count']} occurrences")

    print("\n💡 KEY INSIGHTS:")
    for i, finding in enumerate(report["insights"]["key_findings"], 1):
        print(f"  {i}. {finding}")

    print("\n🎯 RECOMMENDATIONS:")
    for i, rec in enumerate(report["insights"]["recommendations"], 1):
        print(f"  {i}. {rec}")

    print("\n🔍 PATTERNS DETECTED:")
    for i, pattern in enumerate(report["insights"]["patterns"], 1):
        print(f"  {i}. {pattern}")

    print("\n" + "=" * 80)
    print(f"Full report available at: {output_file}")
    print("=" * 80)


if __name__ == "__main__":
    main()
