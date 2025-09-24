from __future__ import annotations

import os
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests

from utils.cache_manager.cache_manager import CacheManager
from utils.logging.logging_manager import LogManager

from domains.syngenta.datadog.events_analyzer import DatadogEventsAnalyzer


class DatadogEventsServiceError(RuntimeError):
    """Generic Datadog Events service error."""


class DatadogEventsAuthError(DatadogEventsServiceError):
    """Raised when Datadog authentication fails."""


class DatadogEventsService:
    """
    Service responsible for interacting with Datadog Events Search API.

    Implements caching, pagination handling, and helper methods to normalize
    events payloads for downstream command usage.
    """

    def __init__(
        self,
        *,
        site: Optional[str],
        api_key: Optional[str],
        app_key: Optional[str],
        use_cache: bool = False,
        cache_ttl_minutes: int = 30,
        timezone_name: str = "UTC",
    ) -> None:
        self.logger = LogManager.get_instance().get_logger("DatadogEventsService")
        self.site = (site or os.getenv("DD_SITE") or "datadoghq.com").strip()
        if self.site.startswith("app."):
            original = self.site
            self.site = self.site.split("app.", 1)[1]
            self.logger.warning(
                "Sanitized DD_SITE from '%s' to '%s'. Use values like 'datadoghq.com'.",
                original,
                self.site,
            )
        self.api_key = (api_key or os.getenv("DD_API_KEY") or "").strip()
        self.app_key = (app_key or os.getenv("DD_APP_KEY") or "").strip()
        self.use_cache = bool(use_cache)
        self.cache_ttl_minutes = cache_ttl_minutes
        self.timezone_name = timezone_name or "UTC"
        self.cache = CacheManager.get_instance()

        if not self.api_key or not self.app_key:
            raise DatadogEventsServiceError(
                "Missing Datadog credentials. Ensure DD_API_KEY and DD_APP_KEY are available."
            )

    # ---------------------------------------------------------------------
    # Public API
    # ---------------------------------------------------------------------
    def fetch_events_for_teams(self, *, teams: List[str], env: str, days: int) -> Dict[str, Any]:
        """
        Retrieve Datadog monitor alert events for the provided teams.

        Args:
            teams: Datadog team handles to query.
            env: Environment filter (e.g., "prod").
            days: Lookback window in days.

        Returns:
            Dictionary containing per-team events and metadata for reporting.
        """
        per_team: Dict[str, Dict[str, Any]] = {}
        errors: Dict[str, str] = {}

        for team in teams:
            try:
                raw_events = self._fetch_team_events(team=team, env=env, days=days)
                parsed_events = self.parse_event_data(raw_events, default_team=team, default_env=env)
                per_team[team] = {
                    "events": parsed_events,
                    "event_count": len(parsed_events),
                    "last_event_timestamp": self._extract_latest_timestamp(parsed_events),
                }
            except DatadogEventsAuthError:
                raise
            except DatadogEventsServiceError as exc:  # pragma: no cover - defensive path
                self.logger.error("Failed to fetch events for team '%s': %s", team, exc)
                errors[team] = str(exc)
                per_team[team] = {
                    "events": [],
                    "event_count": 0,
                    "last_event_timestamp": None,
                    "error": str(exc),
                }

        generated_at = datetime.now(timezone.utc)
        metadata = {
            "site": self.site,
            "env": env,
            "requested_teams": teams,
            "lookback_days": days,
            "generated_at": generated_at.isoformat(),
            "errors": errors,
        }
        return {
            "teams": per_team,
            "metadata": metadata,
        }

    def parse_event_data(
        self,
        events: List[Dict[str, Any]],
        *,
        default_team: str,
        default_env: str,
    ) -> List[Dict[str, Any]]:
        """
        Normalize raw Datadog events into a simplified structure.

        Args:
            events: Raw events returned by Datadog Events API.
            default_team: Team associated to the query (used as fallback).
            default_env: Environment associated to the query (fallback when tag absent).

        Returns:
            List of normalized event dictionaries.
        """
        normalized: List[Dict[str, Any]] = []
        for event in events or []:
            attributes = event.get("attributes") or {}
            inner_attributes = attributes.get("attributes")
            event_attributes = inner_attributes if isinstance(inner_attributes, dict) else {}

            tags = attributes.get("tags") or event_attributes.get("tags") or []
            tag_team = self._extract_team_from_tags(tags)
            tag_env = self._extract_env_from_tags(tags)

            monitor_source = attributes.get("monitor") or event_attributes.get("monitor")
            monitor_payload = self._safe_monitor_payload(monitor_source)
            lifecycle = self._extract_lifecycle(attributes, monitor_payload)

            normalized.append(
                {
                    "id": event.get("id"),
                    "title": attributes.get("title")
                    or attributes.get("text")
                    or event_attributes.get("title")
                    or "",
                    "message": attributes.get("text") or event_attributes.get("message") or "",
                    "timestamp": self._ensure_iso_timestamp(attributes.get("timestamp")),
                    "timestamp_epoch": self._ensure_epoch_timestamp(attributes.get("timestamp")),
                    "alert_type": attributes.get("alert_type") or event_attributes.get("alert_type"),
                    "source": attributes.get("source") or event_attributes.get("source"),
                    "status": attributes.get("status") or event_attributes.get("status"),
                    "url": attributes.get("url") or event_attributes.get("url"),
                    "monitor": monitor_payload,
                    "tags": tags,
                    "team": tag_team or default_team,
                    "env": tag_env or default_env,
                    "lifecycle": lifecycle,
                    "transition_type": lifecycle.get("transition_type"),
                    "state": lifecycle.get("destination_state"),
                    "duration_seconds": self._nanoseconds_to_seconds(
                        event_attributes.get("duration")
                    ),
                    "priority": event_attributes.get("priority") or monitor_payload.get("priority"),
                }
            )
        return normalized

    def generate_summary(
        self,
        per_team: Dict[str, Dict[str, Any]],
        *,
        requested_teams: List[str],
        days: int,
        env: str,
    ) -> Dict[str, Any]:
        """
        Produce aggregate metrics for executive summary rendering.

        Args:
            per_team: Mapping of team handle to normalized events payload.
            requested_teams: Original list of teams provided by the user.
            days: Lookback window in days.
            env: Environment filter applied in the query.

        Returns:
            Dictionary with aggregate metrics and formatted period labels.
        """
        total_events = 0
        active_team_count = 0
        most_active: Optional[Tuple[str, int]] = None

        for team in requested_teams:
            team_payload = per_team.get(team) or {}
            team_events = int(team_payload.get("event_count") or 0)
            total_events += team_events
            if team_events > 0:
                active_team_count += 1
            if team_events > 0 and (most_active is None or team_events > most_active[1]):
                most_active = (team, team_events)

        period = self._compute_period(days=days)
        summary = {
            "totals": {
                "events": total_events,
                "teams_with_alerts": active_team_count,
                "total_teams": len(requested_teams),
            },
            "most_active_team": {
                "team": most_active[0] if most_active else None,
                "event_count": most_active[1] if most_active else 0,
            },
            "time_period": {
                "start": period["start_iso"],
                "end": period["end_iso"],
                "label": period["label"],
                "lookback_days": days,
                "relative_label": period["relative"],
            },
            "env": env,
        }
        return summary

    def analyze_events_advanced(
        self,
        per_team: Dict[str, Dict[str, Any]],
        *,
        analysis_period_days: int = 30,
        min_confidence: float = 0.8,
        include_detailed_stats: bool = False,
        existing_monitors: List[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Generate advanced alert quality metrics and removal recommendations.

        Args:
            per_team: Mapping of team handle to normalized events payload.
            analysis_period_days: Analysis period in days for calculating metrics.
            min_confidence: Minimum confidence score for removal recommendations.
            include_detailed_stats: Whether to include detailed monitor statistics.
            existing_monitors: List of currently existing monitors from _fetch_all_monitors.

        Returns:
            Dictionary with advanced analysis results including quality metrics,
            removal candidates, temporal metrics, behavioral patterns, and deleted monitor info.
        """
        # Collect all events from all teams
        all_events: List[Dict[str, Any]] = []
        for team_data in per_team.values():
            if isinstance(team_data, dict) and "events" in team_data:
                events = team_data["events"]
                if isinstance(events, list):
                    all_events.extend(events)

        if not all_events:
            return self._empty_advanced_analysis()

        # Detect deleted monitors by comparing event monitors with existing monitors
        deleted_monitors = self._detect_deleted_monitors(all_events, existing_monitors or [])

        # Initialize analyzer with all events
        analyzer = DatadogEventsAnalyzer(
            events_data=all_events,
            analysis_period_days=analysis_period_days,
            deleted_monitors=deleted_monitors
        )

        # Generate comprehensive analysis
        alert_quality = analyzer.analyze_alert_quality()
        removal_candidates = analyzer.find_removal_candidates(min_confidence=min_confidence)
        temporal_metrics = analyzer.calculate_temporal_metrics()
        behavioral_patterns = analyzer.detect_behavioral_patterns()
        actionability_scores = analyzer.generate_actionability_scores()

        result = {
            "alert_quality": alert_quality,
            "removal_candidates": removal_candidates,
            "temporal_metrics": temporal_metrics,
            "behavioral_patterns": behavioral_patterns,
            "actionability_scores": actionability_scores,
            "recommendations": self._generate_recommendations(removal_candidates, alert_quality),
        }

        # Add detailed monitor statistics if requested
        if include_detailed_stats:
            detailed_stats = analyzer.generate_detailed_monitor_statistics()
            result["detailed_monitor_statistics"] = detailed_stats

        return result

    def _empty_advanced_analysis(self) -> Dict[str, Any]:
        """Return empty analysis structure when no events are available."""
        return {
            "alert_quality": {
                "overall": {
                    "overall_noise_score": 0.0,
                    "self_healing_rate": 0.0,
                    "total_monitors": 0,
                    "average_flapping_incidents": 0.0,
                    "actionable_alerts_percentage": 0.0,
                },
                "per_monitor": {}
            },
            "removal_candidates": {
                "items": [],
                "count": 0,
                "min_confidence": 0.8,
                "estimated_noise_reduction": 0.0,
            },
            "temporal_metrics": {
                "avg_time_to_resolution_minutes": None,
                "avg_warning_duration_minutes": None,
                "avg_alert_duration_minutes": None,
                "mtbf_hours": None,
                "cycle_duration_minutes": {"average": None, "p95": None},
                "samples": {"ttr_samples": 0, "cycle_samples": 0},
            },
            "behavioral_patterns": {
                "overall": {name: 0 for name in ["healthy", "escalated", "direct_critical", "stuck_warning", "chronic", "unknown"]},
                "per_monitor": {},
                "examples": {},
            },
            "actionability_scores": {
                "overall_score": None,
                "per_monitor": {},
            },
            "recommendations": {
                "immediate_actions": [],
                "threshold_adjustments": [],
            },
        }

    def _generate_recommendations(
        self,
        removal_candidates: Dict[str, Any],
        alert_quality: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Generate actionable recommendations based on analysis results."""
        immediate_actions = []
        threshold_adjustments = []

        # Generate removal recommendations
        candidates = removal_candidates.get("items", [])
        for candidate in candidates[:5]:  # Top 5 candidates
            if candidate.get("confidence_score", 0) >= 0.85:
                immediate_actions.append({
                    "action": "remove",
                    "monitor_id": candidate.get("monitor_id"),
                    "monitor_name": candidate.get("monitor_name"),
                    "priority": "high" if candidate.get("confidence_score", 0) >= 0.9 else "medium",
                    "estimated_noise_reduction": candidate.get("noise_score", 0) / 100.0,
                    "reason": f"High confidence removal candidate ({candidate.get('confidence_score', 0):.2f})"
                })

        # Generate threshold adjustment recommendations
        per_monitor = alert_quality.get("per_monitor", {})
        for monitor_id, metrics in per_monitor.items():
            if isinstance(metrics, dict):
                noise_score = metrics.get("noise_score", 0)
                flapping = metrics.get("flapping", {})
                if noise_score > 60 and flapping.get("flapping_incidents", 0) > 3:
                    threshold_adjustments.append({
                        "action": "adjust_thresholds",
                        "monitor_id": monitor_id,
                        "monitor_name": metrics.get("monitor_name"),
                        "current_noise_score": noise_score,
                        "reason": f"High noise with {flapping.get('flapping_incidents', 0)} flapping incidents"
                    })

        return {
            "immediate_actions": immediate_actions,
            "threshold_adjustments": threshold_adjustments,
        }

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _fetch_team_events(self, *, team: str, env: str, days: int) -> List[Dict[str, Any]]:
        cache_key = self.cache.generate_cache_key(
            prefix="datadog_events",
            site=self.site,
            team=team,
            env=env,
            days=days,
        )
        if self.use_cache:
            cached = self.cache.load(cache_key, expiration_minutes=self.cache_ttl_minutes)
            if cached is not None:
                self.logger.info(f"Using cached events for team '{team}' (cache hit): {len(cached)} events")
                return cached

        self.logger.info(f"Fetching events from Datadog API for team '{team}' (last {days} days, env: {env})")

        events: List[Dict[str, Any]] = []
        cursor: Optional[str] = None
        page_limit = 100
        url = f"{self._api_base()}/api/v2/events/search"
        page_count = 0

        self.logger.info(f"Starting paginated events search for team '{team}'")

        while True:
            page_count += 1
            payload: Dict[str, Any] = {
                "filter": {
                    "from": f"now-{days}d",
                    "to": "now",
                    "query": (f"team:{team} AND env:{env} AND source:alert "),
                },
                "page": {"limit": page_limit},
                "sort": "-timestamp",
                "options": {"timezone": self.timezone_name},
            }
            if cursor:
                payload["page"]["cursor"] = cursor

            self.logger.debug(f"Fetching events page {page_count} for team '{team}'" + (f" (cursor: {cursor[:20]}...)" if cursor else " (first page)"))

            try:
                response = requests.post(
                    url,
                    headers=self._headers(),
                    json=payload,
                    timeout=45,
                )
                response.raise_for_status()
            except requests.HTTPError as exc:
                status_code = exc.response.status_code if exc.response is not None else None
                if status_code in (401, 403):
                    raise DatadogEventsAuthError(
                        "Datadog authentication failed. Check DD_API_KEY/DD_APP_KEY permissions."
                    ) from exc
                error_msg = self._extract_error_message(exc.response)
                raise DatadogEventsServiceError(f"Datadog Events API returned HTTP {status_code}: {error_msg}") from exc
            except requests.RequestException as exc:
                raise DatadogEventsServiceError(f"Network error while contacting Datadog Events API: {exc}") from exc

            data = response.json() if response.content else {}
            chunk = data.get("data") or []
            events.extend(chunk)

            self.logger.debug(f"Events page {page_count} complete: got {len(chunk)} events (total so far: {len(events)})")

            cursor = self._next_cursor(data)
            if not cursor:
                self.logger.info(f"Events fetch complete for team '{team}': {len(events)} events fetched across {page_count} pages")
                break

        if self.use_cache:
            self.logger.info(f"Caching {len(events)} events for team '{team}'")
            self.cache.save(cache_key, events)
        return events

    def _headers(self) -> Dict[str, str]:
        return {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "DD-API-KEY": self.api_key,
            "DD-APPLICATION-KEY": self.app_key,
        }

    def _api_base(self) -> str:
        host = self.site
        if not host.startswith("api."):
            host = f"api.{host}"
        return f"https://{host}"

    def _compute_period(self, *, days: int) -> Dict[str, str]:
        end_ts = datetime.now(timezone.utc)
        start_ts = end_ts - timedelta(days=max(days, 0))
        if start_ts.month == end_ts.month and start_ts.year == end_ts.year:
            label = f"{start_ts.strftime('%b %d')}-{end_ts.strftime('%d, %Y')}"
        elif start_ts.year == end_ts.year:
            label = f"{start_ts.strftime('%b %d')} - {end_ts.strftime('%b %d, %Y')}"
        else:
            label = f"{start_ts.strftime('%b %d, %Y')} - {end_ts.strftime('%b %d, %Y')}"
        relative = "Last 1 day" if days == 1 else f"Last {days} days"
        return {
            "start_iso": start_ts.isoformat(),
            "end_iso": end_ts.isoformat(),
            "label": label,
            "relative": relative,
        }

    @staticmethod
    def _next_cursor(response_json: Dict[str, Any]) -> Optional[str]:
        meta = response_json.get("meta") or {}
        page = meta.get("page") if isinstance(meta, dict) else None
        if isinstance(page, dict):
            return page.get("after")
        links = response_json.get("links") or {}
        return links.get("next")

    @staticmethod
    def _extract_error_message(response: Optional[requests.Response]) -> str:
        if response is None:
            return "Unknown error"
        try:
            payload = response.json()  # type: ignore[no-untyped-call]
        except ValueError:
            return response.text[:200]
        errors = payload.get("errors") if isinstance(payload, dict) else None
        if isinstance(errors, list) and errors:
            return "; ".join(str(e) for e in errors)
        if isinstance(errors, dict):
            return str(errors)
        return payload.get("error", "Unknown error") if isinstance(payload, dict) else "Unknown error"

    @staticmethod
    def _ensure_iso_timestamp(value: Any) -> Optional[str]:
        if not value:
            return None
        if isinstance(value, str):
            return value
        if isinstance(value, (int, float)):
            try:
                return datetime.fromtimestamp(float(value), tz=timezone.utc).isoformat()
            except (ValueError, OSError):
                return None
        return None

    @staticmethod
    def _safe_monitor_payload(monitor: Any) -> Dict[str, Any]:
        if not isinstance(monitor, dict):
            return {}

        transition = monitor.get("transition") if isinstance(monitor.get("transition"), dict) else {}
        options = monitor.get("options") if isinstance(monitor.get("options"), dict) else {}
        thresholds = options.get("thresholds") if isinstance(options.get("thresholds"), dict) else {}
        result_payload = monitor.get("result") if isinstance(monitor.get("result"), dict) else {}

        alert_cycle_key = (
            monitor.get("alert_cycle_key")
            or monitor.get("alert_cycle_key_txt")
            or monitor.get("alert_cycle")
        )

        return {
            "id": monitor.get("id"),
            "name": monitor.get("name"),
            "status": monitor.get("status"),
            "type": monitor.get("type"),
            "url": monitor.get("url") or result_payload.get("alert_url"),
            "alert_cycle_key": alert_cycle_key,
            "alert_cycle_key_txt": monitor.get("alert_cycle_key_txt"),
            "priority": monitor.get("priority"),
            "groups": monitor.get("groups"),
            "transition": {
                "source_state": transition.get("source_state"),
                "destination_state": transition.get("destination_state"),
                "transition_type": transition.get("transition_type"),
            },
            "options": {
                "thresholds": {
                    "critical": thresholds.get("critical"),
                    "warning": thresholds.get("warning"),
                },
                "notify_no_data": options.get("notify_no_data"),
                "require_full_window": options.get("require_full_window"),
            },
            "result": {
                "alert_url": result_payload.get("alert_url"),
                "logs_url": result_payload.get("logs_url"),
                "snap_url": result_payload.get("snap_url"),
            },
        }

    @staticmethod
    def _extract_lifecycle(attributes: Dict[str, Any], monitor_payload: Dict[str, Any]) -> Dict[str, Any]:
        lifecycle = monitor_payload.get("transition") or {}
        if lifecycle:
            return {
                "source_state": lifecycle.get("source_state"),
                "destination_state": lifecycle.get("destination_state"),
                "transition_type": lifecycle.get("transition_type"),
            }

        # Fall back to attributes-level transition data if available
        attr_transition = attributes.get("transition")
        if isinstance(attr_transition, dict):
            return {
                "source_state": attr_transition.get("source_state"),
                "destination_state": attr_transition.get("destination_state"),
                "transition_type": attr_transition.get("transition_type"),
            }
        return {}

    @staticmethod
    def _nanoseconds_to_seconds(value: Any) -> Optional[float]:
        if value is None:
            return None
        try:
            return float(value) / 1_000_000_000
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _ensure_epoch_timestamp(value: Any) -> Optional[float]:
        if value is None:
            return None
        if isinstance(value, (int, float)):
            try:
                return float(value) / 1000 if float(value) > 9_999_999_999 else float(value)
            except (TypeError, ValueError):
                return None
        if isinstance(value, str):
            try:
                ts = value.replace("Z", "+00:00")
                return datetime.fromisoformat(ts).timestamp()
            except ValueError:
                return None
        return None

    @staticmethod
    def _extract_team_from_tags(tags: Any) -> Optional[str]:
        if not isinstance(tags, list):
            return None
        for tag in tags:
            if isinstance(tag, str) and tag.startswith("team:"):
                return tag.split("team:", 1)[1]
            if isinstance(tag, str) and tag.startswith("dd_team:"):
                return tag.split("dd_team:", 1)[1]
        return None

    @staticmethod
    def _extract_env_from_tags(tags: Any) -> Optional[str]:
        if not isinstance(tags, list):
            return None
        for tag in tags:
            if isinstance(tag, str) and tag.startswith("env:"):
                return tag.split("env:", 1)[1]
        return None

    @staticmethod
    def _extract_latest_timestamp(events: List[Dict[str, Any]]) -> Optional[str]:
        latest: Optional[str] = None
        for event in events:
            ts = event.get("timestamp")
            if not ts:
                continue
            if latest is None or ts > latest:
                latest = ts
        return latest

    def find_unused_monitors(
        self,
        *,
        teams: List[str],
        days: int = 30,
        env: str = "prod",
        include_all: bool = False,
        detailed: bool = False
    ) -> Dict[str, Any]:
        """
        Find monitors that haven't triggered events in the specified time period.

        Args:
            teams: List of team handles to analyze
            days: Number of days to look back for monitor activity
            env: Environment filter (e.g., 'prod', 'staging')
            include_all: Include both used and unused monitors in results
            detailed: Include detailed monitor information

        Returns:
            Dictionary containing unused monitors analysis
        """
        start_time = time.time()
        self.logger.info(f"Finding unused monitors for teams: {teams}, lookback: {days} days, env: {env}")

        # Get all monitors for the teams
        self.logger.info("Starting monitor discovery across teams...")
        monitor_start = time.time()
        all_monitors = self._fetch_all_monitors(teams=teams, env=env)
        monitor_duration = time.time() - monitor_start
        self.logger.info(f"Monitor discovery complete: found {len(all_monitors)} unique monitors across {len(teams)} teams ({monitor_duration:.2f}s)")

        # Get events for the same period to check monitor activity
        self.logger.info(f"Fetching events for {len(teams)} teams to determine monitor activity...")
        events_start = time.time()
        events_by_team = {}
        for i, team in enumerate(teams, 1):
            team_start = time.time()
            self.logger.info(f"Fetching events for team {i}/{len(teams)}: {team}")
            events = self._fetch_team_events(team=team, env=env, days=days)
            events_by_team[team] = events
            team_duration = time.time() - team_start
            self.logger.info(f"Team {team}: fetched {len(events)} events ({team_duration:.2f}s)")

        events_duration = time.time() - events_start

        # Extract monitor IDs that have triggered events
        analysis_start = time.time()
        self.logger.info("Analyzing events to identify active monitors...")
        active_monitor_ids = set()
        total_events = sum(len(team_events) for team_events in events_by_team.values())

        for team_events in events_by_team.values():
            for event in team_events:
                monitor_data = event.get("attributes", {}).get("monitor") or {}
                monitor_id = monitor_data.get("id")
                if monitor_id:
                    active_monitor_ids.add(str(monitor_id))

        self.logger.info(f"Events analysis complete: found {len(active_monitor_ids)} unique monitors with events from {total_events} total events ({events_duration:.2f}s)")

        # Classify monitors as used or unused
        self.logger.info("Classifying monitors as active/unused based on event data and metadata...")
        unused_monitors = []
        active_monitors = []
        muted_count = 0

        cutoff_timestamp = datetime.now(timezone.utc) - timedelta(days=days)

        for monitor in all_monitors:
            monitor_id = str(monitor.get("id", ""))
            is_muted = monitor.get("muted_until_ts") is not None and monitor.get("muted_until_ts") != 0
            if is_muted:
                muted_count += 1

            # Check if monitor has triggered in the period
            is_active = monitor_id in active_monitor_ids

            # Additionally check last_triggered_ts from monitor metadata
            last_triggered_ts = monitor.get("last_triggered_ts")
            if last_triggered_ts and not is_active:
                try:
                    last_triggered = datetime.fromtimestamp(last_triggered_ts, tz=timezone.utc)
                    if last_triggered >= cutoff_timestamp:
                        is_active = True
                        active_monitor_ids.add(monitor_id)
                except (ValueError, OSError):
                    pass

            # Calculate days since last triggered
            last_triggered_days_ago = None
            if last_triggered_ts:
                try:
                    last_triggered = datetime.fromtimestamp(last_triggered_ts, tz=timezone.utc)
                    days_diff = (datetime.now(timezone.utc) - last_triggered).days
                    last_triggered_days_ago = days_diff
                except (ValueError, OSError):
                    pass

            monitor_summary = {
                "id": monitor_id,
                "name": monitor.get("name", "Unknown"),
                "type": monitor.get("type", "unknown"),
                "classification": monitor.get("classification", "unknown"),
                "status": monitor.get("status", "unknown"),
                "muted": is_muted,
                "last_triggered_ts": last_triggered_ts,
                "last_triggered_days_ago": last_triggered_days_ago,
                "priority": monitor.get("priority"),
                "created": monitor.get("created"),
                "modified": monitor.get("modified"),
                "tags": monitor.get("tags", [])
            }

            if detailed:
                monitor_summary.update({
                    "creator": monitor.get("creator", {}),
                    "notifications": monitor.get("notifications", []),
                    "query": monitor.get("query", ""),
                    "scopes": monitor.get("scopes", []),
                    "metrics": monitor.get("metrics", [])
                })

            if is_active:
                active_monitors.append(monitor_summary)
            else:
                unused_monitors.append(monitor_summary)

        # Sort monitors by last triggered (newest first for active, oldest first for unused)
        active_monitors.sort(key=lambda x: x.get("last_triggered_days_ago") or 999)
        unused_monitors.sort(key=lambda x: x.get("last_triggered_days_ago") or 999, reverse=True)

        classification_duration = time.time() - analysis_start
        self.logger.info(f"Monitor classification complete: {len(active_monitors)} active, {len(unused_monitors)} unused, {muted_count} muted ({classification_duration:.2f}s)")

        # Prepare summary
        summary = {
            "analysis_period_days": days,
            "teams": teams,
            "env": env,
            "total_monitors": len(all_monitors),
            "unused_monitors": len(unused_monitors),
            "active_monitors": len(active_monitors),
            "muted_monitors": muted_count,
            "analysis_timestamp": datetime.now(timezone.utc).isoformat()
        }

        total_duration = time.time() - start_time
        self.logger.info(f"Unused monitors analysis complete for {len(teams)} teams: {len(unused_monitors)} unused monitors identified (total time: {total_duration:.2f}s)")

        result = {
            "summary": summary,
            "unused_monitors": unused_monitors,
            "include_all": include_all,
            "all_monitors": all_monitors  # Include raw monitors list for deleted monitor detection
        }

        if include_all:
            result["active_monitors"] = active_monitors

        return result

    def _fetch_all_monitors(self, *, teams: List[str], env: str) -> List[Dict[str, Any]]:
        """
        Fetch all monitors for the specified teams using the monitor search API.
        """
        cache_key = self.cache.generate_cache_key(
            prefix="datadog_monitors",
            site=self.site,
            teams="_".join(sorted(teams)),
            env=env,
        )

        if self.use_cache:
            cached = self.cache.load(cache_key, expiration_minutes=self.cache_ttl_minutes)
            if cached is not None:
                self.logger.info(f"Using cached monitor data for {len(teams)} teams (cache hit)")
                return cached

        self.logger.info(f"No cache available, fetching monitors from Datadog API for {len(teams)} teams...")
        all_monitors = []

        for i, team in enumerate(teams, 1):
            self.logger.info(f"Fetching monitors for team {i}/{len(teams)}: {team}")
            monitors = self._fetch_team_monitors(team=team, env=env)
            all_monitors.extend(monitors)
            self.logger.info(f"Team {team}: found {len(monitors)} monitors")

        self.logger.info(f"Raw monitor fetch complete: {len(all_monitors)} total monitors (before deduplication)")

        # Deduplicate monitors by ID (monitors can appear in multiple team searches)
        seen_ids = set()
        unique_monitors = []
        for monitor in all_monitors:
            monitor_id = monitor.get("id")
            if monitor_id and monitor_id not in seen_ids:
                seen_ids.add(monitor_id)
                unique_monitors.append(monitor)

        duplicates_removed = len(all_monitors) - len(unique_monitors)
        if duplicates_removed > 0:
            self.logger.info(f"Deduplication complete: removed {duplicates_removed} duplicate monitors, {len(unique_monitors)} unique monitors remain")
        else:
            self.logger.info(f"No duplicate monitors found, {len(unique_monitors)} unique monitors")

        if self.use_cache:
            self.logger.info(f"Caching {len(unique_monitors)} monitors for future requests")
            self.cache.save(cache_key, unique_monitors)

        return unique_monitors

    def _fetch_team_monitors(self, *, team: str, env: str) -> List[Dict[str, Any]]:
        """
        Fetch monitors for a specific team using the monitor search API.
        """
        monitors = []
        page = 0
        per_page = 100

        self.logger.info(f"Starting paginated monitor search for team '{team}' in env '{env}'")

        while True:
            url = f"{self._api_base()}/api/v1/monitor/search"
            params = {
                "query": f"team:{team} AND env:{env}",
                "page": page,
                "per_page": per_page,
                "sort": "name,asc"
            }

            self.logger.debug(f"Fetching page {page} for team '{team}' (up to {per_page} monitors per page)")

            try:
                response = requests.get(
                    url,
                    headers=self._headers(),
                    params=params,
                    timeout=30,
                )
                response.raise_for_status()
            except requests.HTTPError as exc:
                status_code = exc.response.status_code if exc.response is not None else None
                if status_code in (401, 403):
                    raise DatadogEventsAuthError(
                        "Datadog authentication failed. Check DD_API_KEY/DD_APP_KEY permissions."
                    ) from exc
                error_msg = self._extract_error_message(exc.response)
                raise DatadogEventsServiceError(f"Datadog Monitor Search API returned HTTP {status_code}: {error_msg}") from exc
            except requests.RequestException as exc:
                raise DatadogEventsServiceError(f"Network error while contacting Datadog Monitor Search API: {exc}") from exc

            data = response.json() if response.content else {}
            page_monitors = data.get("monitors", [])
            monitors.extend(page_monitors)

            # Check if we have more pages
            metadata = data.get("metadata", {})
            total_count = metadata.get("total_count", 0)
            current_count = (page + 1) * per_page

            self.logger.debug(f"Page {page} complete: got {len(page_monitors)} monitors (total so far: {len(monitors)})")

            if current_count >= total_count or len(page_monitors) < per_page:
                if total_count > 0:
                    self.logger.info(f"Monitor search complete for team '{team}': {len(monitors)}/{total_count} monitors fetched")
                else:
                    self.logger.info(f"Monitor search complete for team '{team}': {len(monitors)} monitors fetched")
                break

            page += 1

        return monitors

    def _detect_deleted_monitors(self, events: List[Dict[str, Any]], existing_monitors: List[Dict[str, Any]]) -> List[str]:
        """
        Detect monitors that appear in events but no longer exist in Datadog.

        Args:
            events: List of alert events from the analysis period
            existing_monitors: List of monitors currently existing in Datadog

        Returns:
            List of monitor IDs that appear in events but are not in existing_monitors
        """
        # Extract unique monitor IDs from events
        event_monitor_ids = set()
        for event in events:
            if isinstance(event, dict) and "monitor" in event:
                monitor = event["monitor"]
                if isinstance(monitor, dict) and "id" in monitor:
                    monitor_id = str(monitor["id"])
                    event_monitor_ids.add(monitor_id)

        # Extract monitor IDs from existing monitors
        existing_monitor_ids = set()
        for monitor in existing_monitors:
            if isinstance(monitor, dict) and "id" in monitor:
                monitor_id = str(monitor["id"])
                existing_monitor_ids.add(monitor_id)

        # Find monitors that appear in events but don't exist anymore
        deleted_monitor_ids = event_monitor_ids - existing_monitor_ids

        # Debug logging
        self.logger.info(f"Monitor ID comparison - Events: {len(event_monitor_ids)}, Existing: {len(existing_monitor_ids)}, Deleted: {len(deleted_monitor_ids)}")
        if event_monitor_ids and existing_monitor_ids:
            self.logger.debug(f"Sample event monitor IDs: {list(sorted(event_monitor_ids))[:5]}")
            self.logger.debug(f"Sample existing monitor IDs: {list(sorted(existing_monitor_ids))[:5]}")

        if deleted_monitor_ids:
            self.logger.info(f"Detected {len(deleted_monitor_ids)} deleted monitors: {sorted(list(deleted_monitor_ids))}")

        return list(deleted_monitor_ids)
