from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from statistics import mean
from typing import Any, Dict, Iterable, List, Optional, Tuple

from utils.logging.logging_manager import LogManager

ALERT_PATTERNS: Dict[str, List[str]] = {
    "healthy": ["OK", "Warn", "OK"],
    "escalated": ["OK", "Warn", "Alert", "OK"],
    "direct_critical": ["OK", "Alert", "OK"],
    "stuck_warning": ["OK", "Warn", "Warn"],
    "chronic": ["OK", "Alert", "Alert"],
}

FLAPPING_THRESHOLD = 5


@dataclass(frozen=True)
class LifecycleEvent:
    raw: Dict[str, Any]
    timestamp: Optional[datetime]
    timestamp_epoch: Optional[float]
    monitor_id: str
    monitor_name: Optional[str]
    alert_cycle_key: str
    source_state: Optional[str]
    destination_state: Optional[str]
    transition_type: Optional[str]
    status: Optional[str]
    team: Optional[str]
    env: Optional[str]
    duration_seconds: Optional[float]
    priority: Optional[int]

    def state_label(self) -> Optional[str]:
        return self.destination_state or self.status


@dataclass
class AlertCycle:
    key: str
    monitor_id: str
    monitor_name: Optional[str]
    events: List[LifecycleEvent] = field(default_factory=list)

    def __post_init__(self) -> None:
        self.events.sort(key=lambda evt: evt.timestamp or datetime.min.replace(tzinfo=timezone.utc))

    @property
    def start(self) -> Optional[datetime]:
        return self.events[0].timestamp if self.events else None

    @property
    def end(self) -> Optional[datetime]:
        return self.events[-1].timestamp if self.events else None

    @property
    def duration_seconds(self) -> Optional[float]:
        if self.start and self.end:
            return max((self.end - self.start).total_seconds(), 0.0)
        if self.events and self.events[0].duration_seconds is not None:
            return self.events[0].duration_seconds
        return None

    def state_sequence(self) -> List[str]:
        sequence: List[str] = []
        for event in self.events:
            if event.source_state and (not sequence or sequence[-1] != event.source_state):
                sequence.append(event.source_state)
            destination = event.destination_state or event.status
            if destination and (not sequence or sequence[-1] != destination):
                sequence.append(destination)
        return sequence

    def has_recovery(self) -> bool:
        for event in self.events:
            transition = (event.transition_type or "").lower()
            if "recovery" in transition:
                return True
            if (event.destination_state or "").upper() == "OK":
                return True
        return False

    def has_alert(self) -> bool:
        for event in self.events:
            if (event.destination_state or "").upper() == "ALERT":
                return True
        return False

    def teams(self) -> List[str]:
        return sorted({event.team for event in self.events if event.team})

    def environments(self) -> List[str]:
        return sorted({event.env for event in self.events if event.env})

    def duration_by_state_seconds(self) -> Dict[str, float]:
        durations: Dict[str, float] = defaultdict(float)
        for index, event in enumerate(self.events):
            start_time = event.timestamp
            if start_time is None:
                continue
            end_time: Optional[datetime]
            if index + 1 < len(self.events):
                end_time = self.events[index + 1].timestamp
            else:
                end_time = None

            if end_time is None and event.duration_seconds is not None:
                delta = max(event.duration_seconds, 0.0)
            elif end_time is not None:
                delta = max((end_time - start_time).total_seconds(), 0.0)
            else:
                delta = 0.0

            state = event.destination_state or event.status
            if state and delta:
                durations[state.upper()] += delta
        return dict(durations)

    def time_to_resolution_seconds(self) -> Optional[float]:
        if not self.start:
            return None
        for event in self.events:
            transition = (event.transition_type or "").lower()
            is_recovery_transition = "recovery" in transition
            destination_ok = (event.destination_state or "").upper() == "OK"
            if is_recovery_transition or destination_ok:
                if event.timestamp is None:
                    continue
                return max((event.timestamp - self.start).total_seconds(), 0.0)
        return None


class DatadogEventsAnalyzer:
    """Advanced analysis engine for Datadog events lifecycle and quality metrics."""

    def __init__(self, events_data: List[Dict[str, Any]], *, analysis_period_days: int = 30, deleted_monitors: List[str] = None) -> None:
        self.logger = LogManager.get_instance().get_logger("DatadogEventsAnalyzer")
        self.analysis_period_days = max(analysis_period_days, 1)
        self.deleted_monitors = set(deleted_monitors or [])
        self.events: List[LifecycleEvent] = [
            self._build_event(event)
            for event in events_data or []
            if self._build_event(event) is not None
        ]
        self.alert_cycles: Dict[str, AlertCycle] = self._group_by_alert_cycles()
        self.monitor_groups: Dict[str, List[LifecycleEvent]] = self._group_by_monitor()
        self.monitor_cycles: Dict[str, List[AlertCycle]] = self._map_cycles_to_monitors()
        self._monitor_metrics: Dict[str, Dict[str, Any]] = {}

    def analyze_alert_quality(self) -> Dict[str, Any]:
        per_monitor: Dict[str, Dict[str, Any]] = {}
        for monitor_id, cycles in self.monitor_cycles.items():
            metrics = self._compute_monitor_quality(monitor_id, cycles)
            per_monitor[monitor_id] = metrics

        self._monitor_metrics = per_monitor
        overall = self._aggregate_quality_metrics(per_monitor.values())

        return {
            "overall": overall,
            "per_monitor": per_monitor,
        }

    def find_removal_candidates(self, *, min_confidence: float = 0.8) -> Dict[str, Any]:
        self._ensure_quality_cached()

        min_confidence = min(max(min_confidence, 0.0), 1.0)
        candidates: List[Dict[str, Any]] = []
        for monitor_id, metrics in self._monitor_metrics.items():
            monitor_noise = float(metrics.get("noise_score") or 0.0)
            self_healing = float(metrics.get("self_healing_rate") or 0.0)
            manual_rate = float(metrics.get("manual_rate") or 0.0)
            cycle_count = int(metrics.get("cycle_count") or 0)
            if cycle_count < 2:
                continue
            if monitor_noise < 60 or self_healing < 0.6 or manual_rate > 0.4:
                continue

            confidence = min(
                1.0,
                (self_healing + (1.0 - manual_rate) + (monitor_noise / 100)) / 3,
            )
            if confidence < min_confidence:
                continue

            monitor_events = self.monitor_groups.get(monitor_id, [])
            cycles = self.monitor_cycles.get(monitor_id, [])
            first_event = monitor_events[0] if monitor_events else None

            reasons: List[str] = []
            if self_healing >= 0.8:
                reasons.append(f"{int(self_healing * 100)}% cycles self-heal")
            if manual_rate <= 0.2:
                reasons.append("No manual intervention observed")
            if monitor_noise >= 80:
                reasons.append("High noise score")

            evidence = {
                "cycle_count": cycle_count,
                "avg_cycle_duration_minutes": metrics.get("avg_cycle_duration_minutes"),
                "avg_alert_duration_minutes": metrics.get("avg_alert_duration_minutes"),
                "teams": metrics.get("teams", []),
                "environments": metrics.get("environments", []),
            }

            candidate = {
                "monitor_id": monitor_id,
                "monitor_name": metrics.get("monitor_name") or (first_event.monitor_name if first_event else None),
                "noise_score": round(monitor_noise, 2),
                "self_healing_rate": round(self_healing, 2),
                "confidence_score": round(confidence, 2),
                "reasons": reasons,
                "evidence": evidence,
            }
            candidates.append(candidate)

        candidates.sort(key=lambda item: item["confidence_score"], reverse=True)

        return {
            "items": candidates,
            "count": len(candidates),
            "min_confidence": min_confidence,
            "estimated_noise_reduction": self._estimate_noise_reduction(candidates),
        }

    def calculate_temporal_metrics(self) -> Dict[str, Any]:
        ttr_minutes: List[float] = []
        warning_minutes: List[float] = []
        alert_minutes: List[float] = []
        cycle_minutes: List[float] = []
        mtbf_hours: List[float] = []

        for monitor_cycles in self.monitor_cycles.values():
            ordered_cycles = sorted(
                monitor_cycles,
                key=lambda cycle: cycle.start or datetime.min.replace(tzinfo=timezone.utc),
            )
            for index, cycle in enumerate(ordered_cycles):
                ttr = cycle.time_to_resolution_seconds()
                if ttr is not None:
                    ttr_minutes.append(ttr / 60)

                durations = cycle.duration_by_state_seconds()
                if "WARN" in durations:
                    warning_minutes.append(durations["WARN"] / 60)
                if "ALERT" in durations:
                    alert_minutes.append(durations["ALERT"] / 60)

                if cycle.duration_seconds is not None:
                    cycle_minutes.append(cycle.duration_seconds / 60)

                if index + 1 < len(ordered_cycles):
                    next_cycle = ordered_cycles[index + 1]
                    if cycle.end and next_cycle.start:
                        delta = (next_cycle.start - cycle.end).total_seconds()
                        if delta > 0:
                            mtbf_hours.append(delta / 3600)

        return {
            "avg_time_to_resolution_minutes": self._round_safe_mean(ttr_minutes),
            "avg_warning_duration_minutes": self._round_safe_mean(warning_minutes),
            "avg_alert_duration_minutes": self._round_safe_mean(alert_minutes),
            "mtbf_hours": self._round_safe_mean(mtbf_hours),
            "cycle_duration_minutes": {
                "average": self._round_safe_mean(cycle_minutes),
                "p95": self._round_percentile(cycle_minutes, 95),
            },
            "samples": {
                "ttr_samples": len(ttr_minutes),
                "cycle_samples": len(cycle_minutes),
            },
        }

    def detect_behavioral_patterns(self) -> Dict[str, Any]:
        pattern_counts: Dict[str, int] = {name: 0 for name in ALERT_PATTERNS}
        pattern_counts["unknown"] = 0
        per_monitor: Dict[str, Dict[str, int]] = defaultdict(lambda: {name: 0 for name in ALERT_PATTERNS})

        examples: Dict[str, Dict[str, Any]] = {name: {} for name in ALERT_PATTERNS}

        for cycle in self.alert_cycles.values():
            sequence = self._normalize_sequence(cycle.state_sequence())
            matched = False
            for name, pattern in ALERT_PATTERNS.items():
                if self._sequence_matches(sequence, pattern):
                    pattern_counts[name] += 1
                    per_monitor[cycle.monitor_id][name] += 1
                    if not examples[name]:
                        examples[name] = {
                            "cycle": cycle.key,
                            "monitor_id": cycle.monitor_id,
                            "sequence": sequence,
                        }
                    matched = True
                    break
            if not matched:
                pattern_counts["unknown"] += 1

        return {
            "overall": pattern_counts,
            "per_monitor": per_monitor,
            "examples": examples,
        }

    def generate_actionability_scores(self) -> Dict[str, Any]:
        self._ensure_quality_cached()

        scores: Dict[str, Dict[str, Any]] = {}
        overall_scores: List[float] = []

        for monitor_id, metrics in self._monitor_metrics.items():
            manual_rate = float(metrics.get("manual_rate") or 0.0)
            alert_duration = float(metrics.get("avg_alert_duration_minutes") or 0.0)
            alert_ratio = float(metrics.get("alert_event_ratio") or 0.0)

            score, confidence = self._calculate_actionability_score(
                manual_rate=manual_rate,
                alert_duration_minutes=alert_duration,
                alert_ratio=alert_ratio,
            )
            overall_scores.append(score)
            scores[monitor_id] = {
                "monitor_name": metrics.get("monitor_name"),
                "score": score,
                "confidence": confidence,
                "factors": {
                    "manual_intervention_rate": manual_rate,
                    "avg_alert_duration_minutes": alert_duration,
                    "alert_event_ratio": alert_ratio,
                },
            }

        return {
            "overall_score": self._round_safe_mean(overall_scores),
            "per_monitor": scores,
        }

    def generate_detailed_monitor_statistics(self) -> Dict[str, Any]:
        """Generate comprehensive per-monitor statistics for decision making."""
        detailed_stats: Dict[str, Dict[str, Any]] = {}

        for monitor_id, cycles in self.monitor_cycles.items():
            if not cycles:
                continue

            monitor_events = self.monitor_groups.get(monitor_id, [])
            if not monitor_events:
                continue

            stats = self._calculate_comprehensive_monitor_stats(monitor_id, cycles, monitor_events)
            detailed_stats[monitor_id] = stats

        # Calculate overall insights
        overall_insights = self._calculate_overall_monitor_insights(detailed_stats)

        return {
            "per_monitor": detailed_stats,
            "overall_insights": overall_insights,
            "recommendations": self._generate_enhanced_recommendations(detailed_stats)
        }

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _build_event(self, event: Dict[str, Any]) -> Optional[LifecycleEvent]:
        monitor = event.get("monitor") if isinstance(event.get("monitor"), dict) else {}
        lifecycle = event.get("lifecycle") if isinstance(event.get("lifecycle"), dict) else {}
        timestamp = self._parse_timestamp(event)
        timestamp_epoch = self._extract_timestamp_epoch(event)
        monitor_id = str(monitor.get("id") or monitor.get("name") or event.get("monitor_id") or "unknown")
        cycle_key = (
            monitor.get("alert_cycle_key")
            or monitor.get("alert_cycle_key_txt")
            or lifecycle.get("alert_cycle_key")
            or event.get("alert_cycle_key")
            or f"{monitor_id}-{event.get('id') or timestamp_epoch or len(self.events)}"
        )

        return LifecycleEvent(
            raw=event,
            timestamp=timestamp,
            timestamp_epoch=timestamp_epoch,
            monitor_id=monitor_id,
            monitor_name=monitor.get("name"),
            alert_cycle_key=str(cycle_key),
            source_state=lifecycle.get("source_state"),
            destination_state=lifecycle.get("destination_state"),
            transition_type=lifecycle.get("transition_type"),
            status=event.get("status"),
            team=event.get("team"),
            env=event.get("env"),
            duration_seconds=event.get("duration_seconds"),
            priority=self._safe_int(event.get("priority")),
        )

    def _group_by_alert_cycles(self) -> Dict[str, AlertCycle]:
        cycles: Dict[str, List[LifecycleEvent]] = defaultdict(list)
        for event in self.events:
            cycles[event.alert_cycle_key].append(event)

        grouped: Dict[str, AlertCycle] = {}
        for key, items in cycles.items():
            monitor_id = items[0].monitor_id if items else "unknown"
            monitor_name = items[0].monitor_name if items else None
            grouped[key] = AlertCycle(key=key, monitor_id=monitor_id, monitor_name=monitor_name, events=list(items))
        return grouped

    def _group_by_monitor(self) -> Dict[str, List[LifecycleEvent]]:
        monitors: Dict[str, List[LifecycleEvent]] = defaultdict(list)
        for event in self.events:
            monitors[event.monitor_id].append(event)
        for monitor_events in monitors.values():
            monitor_events.sort(key=lambda evt: evt.timestamp or datetime.min.replace(tzinfo=timezone.utc))
        return monitors

    def _map_cycles_to_monitors(self) -> Dict[str, List[AlertCycle]]:
        mapping: Dict[str, List[AlertCycle]] = defaultdict(list)
        for cycle in self.alert_cycles.values():
            mapping[cycle.monitor_id].append(cycle)
        return mapping

    def _compute_monitor_quality(self, monitor_id: str, cycles: List[AlertCycle]) -> Dict[str, Any]:
        monitor_events = self.monitor_groups.get(monitor_id, [])
        cycle_count = len(cycles)
        if cycle_count == 0:
            return {
                "monitor_name": monitor_events[0].monitor_name if monitor_events else None,
                "cycle_count": 0,
                "event_count": len(monitor_events),
                "self_healing_rate": 0.0,
                "manual_rate": 0.0,
                "noise_score": 0.0,
                "avg_cycle_duration_minutes": None,
                "avg_alert_duration_minutes": None,
                "avg_warning_duration_minutes": None,
                "flapping": self._detect_flapping(monitor_events),
                "teams": sorted({evt.team for evt in monitor_events if evt.team}),
                "environments": sorted({evt.env for evt in monitor_events if evt.env}),
                "alert_event_ratio": 0.0,
            }

        recovered_cycles = sum(1 for cycle in cycles if cycle.has_recovery())
        self_healing_rate = recovered_cycles / cycle_count if cycle_count else 0.0
        manual_rate = 1.0 - self_healing_rate

        warning_durations: List[float] = []
        alert_durations: List[float] = []
        cycle_durations: List[float] = []
        alert_events = 0
        for cycle in cycles:
            durations = cycle.duration_by_state_seconds()
            if "WARN" in durations:
                warning_durations.append(durations["WARN"] / 60)
            if "ALERT" in durations:
                alert_durations.append(durations["ALERT"] / 60)
            if cycle.duration_seconds is not None:
                cycle_durations.append(cycle.duration_seconds / 60)
            alert_events += sum(
                1
                for event in cycle.events
                if (event.destination_state or "").upper() == "ALERT"
            )

        flapping = self._detect_flapping(monitor_events)
        frequency_score = self._calculate_frequency_score(cycle_count)
        noise_score = self._calculate_noise_score(
            self_healing_rate=self_healing_rate,
            frequency_score=frequency_score,
            flapping=flapping,
            manual_rate=manual_rate,
        )

        monitor_name = monitor_events[0].monitor_name if monitor_events else None
        teams = sorted({evt.team for evt in monitor_events if evt.team})
        environments = sorted({evt.env for evt in monitor_events if evt.env})
        total_events = len(monitor_events) or 1
        alert_event_ratio = alert_events / total_events

        return {
            "monitor_name": monitor_name,
            "cycle_count": cycle_count,
            "event_count": len(monitor_events),
            "self_healing_rate": round(self_healing_rate, 4),
            "manual_rate": round(manual_rate, 4),
            "noise_score": round(noise_score, 2),
            "flapping": flapping,
            "frequency_score": frequency_score,
            "avg_cycle_duration_minutes": self._round_safe_mean(cycle_durations),
            "avg_alert_duration_minutes": self._round_safe_mean(alert_durations),
            "avg_warning_duration_minutes": self._round_safe_mean(warning_durations),
            "teams": teams,
            "environments": environments,
            "alert_event_ratio": round(alert_event_ratio, 4),
        }

    def _aggregate_quality_metrics(self, monitor_metrics: Iterable[Dict[str, Any]]) -> Dict[str, Any]:
        monitor_metrics = list(monitor_metrics)
        if not monitor_metrics:
            return {
                "overall_noise_score": 0.0,
                "self_healing_rate": 0.0,
                "total_monitors": 0,
                "average_flapping_incidents": 0.0,
                "actionable_alerts_percentage": 0.0,
            }

        noise_scores = [metric.get("noise_score") or 0.0 for metric in monitor_metrics]
        self_healings = [metric.get("self_healing_rate") or 0.0 for metric in monitor_metrics]
        manual_rates = [metric.get("manual_rate") or 0.0 for metric in monitor_metrics]
        flapping_incidents = [
            metric.get("flapping", {}).get("flapping_incidents", 0) for metric in monitor_metrics
        ]

        actionable_percentage = 1.0 - (sum(manual_rates) / len(manual_rates) if manual_rates else 0.0)

        return {
            "overall_noise_score": round(sum(noise_scores) / len(noise_scores), 2),
            "self_healing_rate": round(sum(self_healings) / len(self_healings), 2),
            "total_monitors": len(monitor_metrics),
            "average_flapping_incidents": self._round_safe_mean(flapping_incidents),
            "actionable_alerts_percentage": round(actionable_percentage, 2),
        }

    def _detect_flapping(self, events: List[LifecycleEvent], time_window_hours: int = 1) -> Dict[str, Any]:
        timestamps = [evt.timestamp for evt in events if evt.timestamp]
        timestamps.sort()
        if not timestamps:
            return {
                "max_transitions_per_hour": 0,
                "avg_transitions_per_hour": 0.0,
                "flapping_incidents": 0,
            }

        counts: List[int] = []
        window_delta = timedelta(hours=time_window_hours)
        start_index = 0
        for end_index, current_time in enumerate(timestamps):
            while start_index <= end_index and current_time - timestamps[start_index] > window_delta:
                start_index += 1
            counts.append(end_index - start_index + 1)

        max_transitions = max(counts) if counts else 0
        avg_transitions = self._round_safe_mean([float(count) for count in counts])
        flapping_incidents = sum(1 for count in counts if count > FLAPPING_THRESHOLD)

        return {
            "max_transitions_per_hour": max_transitions,
            "avg_transitions_per_hour": avg_transitions,
            "flapping_incidents": flapping_incidents,
        }

    def _calculate_frequency_score(self, cycle_count: int) -> float:
        frequency = cycle_count / max(self.analysis_period_days, 1)
        return min(1.0, frequency)

    def _calculate_noise_score(
        self,
        *,
        self_healing_rate: float,
        frequency_score: float,
        flapping: Dict[str, Any],
        manual_rate: float,
    ) -> float:
        factors = {
            "self_healing": self_healing_rate * 40,
            "frequency": frequency_score * 25,
            "flapping": min(1.0, (flapping.get("max_transitions_per_hour", 0) or 0) / 10) * 20,
            "manual_intervention": max(0.0, 1.0 - manual_rate) * 15,
        }
        return min(100.0, sum(factors.values()))

    def _calculate_actionability_score(
        self,
        *,
        manual_rate: float,
        alert_duration_minutes: float,
        alert_ratio: float,
    ) -> Tuple[float, float]:
        manual_factor = manual_rate * 50
        duration_factor = min(1.0, alert_duration_minutes / 30.0) * 20
        alert_factor = min(1.0, alert_ratio) * 30
        score = min(100.0, manual_factor + duration_factor + alert_factor)
        confidence = min(1.0, (manual_rate + min(1.0, alert_duration_minutes / 60.0) + alert_ratio) / 3)
        return round(score, 2), round(confidence, 2)

    def _sequence_matches(self, sequence: List[str], pattern: List[str]) -> bool:
        if not sequence:
            return False
        if len(sequence) < len(pattern):
            return False
        return sequence[: len(pattern)] == pattern

    def _normalize_sequence(self, sequence: List[str]) -> List[str]:
        normalized: List[str] = []
        for state in sequence:
            state_upper = state.upper()
            if state_upper not in {"OK", "WARN", "ALERT"}:
                continue
            if not normalized or normalized[-1] != state_upper:
                normalized.append(state_upper)
        return normalized

    def _estimate_noise_reduction(self, candidates: List[Dict[str, Any]]) -> float:
        if not candidates:
            return 0.0
        return round(sum(item.get("noise_score", 0.0) for item in candidates) / len(candidates) / 100, 3)

    def _ensure_quality_cached(self) -> None:
        if not self._monitor_metrics:
            self.analyze_alert_quality()

    @staticmethod
    def _parse_timestamp(event: Dict[str, Any]) -> Optional[datetime]:
        value = event.get("timestamp")
        if value is None:
            epoch = event.get("timestamp_epoch")
            if isinstance(epoch, (int, float)):
                try:
                    return datetime.fromtimestamp(float(epoch), tz=timezone.utc)
                except (ValueError, OSError):
                    return None
            return None
        if isinstance(value, datetime):
            return value
        if isinstance(value, (int, float)):
            try:
                return datetime.fromtimestamp(float(value), tz=timezone.utc)
            except (ValueError, OSError):
                return None
        if isinstance(value, str):
            try:
                return datetime.fromisoformat(value.replace("Z", "+00:00"))
            except ValueError:
                return None
        return None

    @staticmethod
    def _extract_timestamp_epoch(event: Dict[str, Any]) -> Optional[float]:
        epoch = event.get("timestamp_epoch")
        if isinstance(epoch, (int, float)):
            return float(epoch)
        return None

    @staticmethod
    def _round_safe_mean(values: Iterable[float]) -> Optional[float]:
        values_list = [value for value in values if value is not None]
        if not values_list:
            return None
        return round(mean(values_list), 2)

    @staticmethod
    def _round_percentile(values: Iterable[float], percentile: int) -> Optional[float]:
        sorted_values = sorted(value for value in values if value is not None)
        if not sorted_values:
            return None
        k = (len(sorted_values) - 1) * (percentile / 100)
        lower_index = int(k)
        upper_index = min(lower_index + 1, len(sorted_values) - 1)
        weight = k - lower_index
        percentile_value = sorted_values[lower_index] * (1 - weight) + sorted_values[upper_index] * weight
        return round(percentile_value, 2)

    def _calculate_comprehensive_monitor_stats(
        self,
        monitor_id: str,
        cycles: List[AlertCycle],
        events: List[LifecycleEvent]
    ) -> Dict[str, Any]:
        """Calculate comprehensive statistics for a single monitor."""

        # Volume and Frequency Stats
        total_events = len(events)
        total_cycles = len(cycles)

        # Time-based analysis
        if events:
            first_event = min(events, key=lambda e: e.timestamp or datetime.min.replace(tzinfo=timezone.utc))
            last_event = max(events, key=lambda e: e.timestamp or datetime.min.replace(tzinfo=timezone.utc))

            if first_event.timestamp and last_event.timestamp:
                monitor_age_days = (last_event.timestamp - first_event.timestamp).days + 1
                events_per_day = total_events / max(monitor_age_days, 1)
                cycles_per_week = (total_cycles * 7) / max(monitor_age_days, 1)
            else:
                monitor_age_days = 1
                events_per_day = total_events
                cycles_per_week = total_cycles * 7
        else:
            monitor_age_days = 1
            events_per_day = 0
            cycles_per_week = 0

        # Status distribution
        status_counts = defaultdict(int)
        for event in events:
            status = (event.destination_state or event.status or "unknown").upper()
            status_counts[status] += 1

        # Alert patterns
        complete_cycles = sum(1 for cycle in cycles if cycle.has_recovery())
        orphaned_alerts = total_cycles - complete_cycles
        quick_recoveries = sum(
            1 for cycle in cycles
            if cycle.duration_seconds and cycle.duration_seconds < 300  # < 5 minutes
        )

        # Business impact analysis (adjusted for Brazilian timezone)
        business_hours_events = sum(
            1 for event in events
            if event.timestamp and self._is_business_hours_brazil(event.timestamp)
        )

        weekend_events = sum(
            1 for event in events
            if event.timestamp and self._is_weekend_brazil(event.timestamp)
        )

        # Response patterns (estimated)
        alert_events = status_counts.get("ALERT", 0)
        estimated_manual_responses = max(0, alert_events - quick_recoveries)
        response_rate = estimated_manual_responses / max(alert_events, 1)

        # Health scoring
        health_score = self._calculate_monitor_health_score(
            cycles=cycles,
            total_events=total_events,
            quick_recoveries=quick_recoveries,
            orphaned_alerts=orphaned_alerts,
            response_rate=response_rate
        )

        # Priority analysis
        high_priority_events = sum(
            1 for event in events
            if event.priority and event.priority >= 3
        )

        return {
            # Basic info
            "monitor_name": events[0].monitor_name if events else None,
            "teams": sorted({e.team for e in events if e.team}),
            "environments": sorted({e.env for e in events if e.env}),

            # Volume metrics
            "total_events": total_events,
            "total_alert_cycles": total_cycles,
            "monitor_age_days": monitor_age_days,
            "events_per_day": round(events_per_day, 2),
            "cycles_per_week": round(cycles_per_week, 2),

            # Status distribution
            "status_distribution": dict(status_counts),
            "status_percentages": {
                status: round((count / max(total_events, 1)) * 100, 1)
                for status, count in status_counts.items()
            },

            # Alert patterns
            "complete_cycles": complete_cycles,
            "complete_cycle_rate": round(complete_cycles / max(total_cycles, 1), 3),
            "orphaned_alerts": orphaned_alerts,
            "quick_recoveries": quick_recoveries,
            "quick_recovery_rate": round(quick_recoveries / max(total_cycles, 1), 3),

            # Business impact
            "business_hours_events": business_hours_events,
            "business_hours_percentage": round((business_hours_events / max(total_events, 1)) * 100, 1),
            "weekend_events": weekend_events,
            "weekend_percentage": round((weekend_events / max(total_events, 1)) * 100, 1),

            # Response analysis
            "estimated_manual_responses": estimated_manual_responses,
            "estimated_response_rate": round(response_rate, 3),
            "high_priority_events": high_priority_events,
            "high_priority_percentage": round((high_priority_events / max(total_events, 1)) * 100, 1),

            # Health and decision metrics
            "health_score": health_score,
            "removal_recommendation": self._get_removal_recommendation(health_score, cycles, total_events),

            # Additional temporal metrics per monitor
            "avg_time_to_resolution_minutes": self._round_safe_mean([
                cycle.time_to_resolution_seconds() / 60
                for cycle in cycles
                if cycle.time_to_resolution_seconds() is not None
            ]),
            "median_cycle_duration_minutes": self._calculate_median([
                cycle.duration_seconds / 60
                for cycle in cycles
                if cycle.duration_seconds is not None
            ]),

            # Time patterns
            "hourly_distribution": self._calculate_hourly_distribution(events),
            "daily_distribution": self._calculate_daily_distribution(events),

            # Monitor status
            "is_deleted": monitor_id in self.deleted_monitors,
        }

    def _calculate_monitor_health_score(
        self,
        cycles: List[AlertCycle],
        total_events: int,
        quick_recoveries: int,
        orphaned_alerts: int,
        response_rate: float
    ) -> Dict[str, Any]:
        """Calculate a comprehensive business value score for the monitor (0-100)."""

        if not cycles:
            return {"score": 0, "factors": {}, "grade": "F"}

        # Calculate average duration for quick recovery analysis
        valid_durations = [cycle.duration_seconds for cycle in cycles if cycle.duration_seconds is not None]
        avg_duration_minutes = (sum(valid_durations) / len(valid_durations)) / 60 if valid_durations else 0

        # Factor 1: Alert Relevance (35% weight) - "Este alerta importa?"
        alert_relevance_score = self._calculate_alert_relevance(
            cycles, quick_recoveries, avg_duration_minutes, orphaned_alerts
        )

        # Factor 2: Response Necessity (30% weight) - "Precisa de ação humana?"
        response_necessity_score = self._calculate_response_necessity(
            response_rate, quick_recoveries, len(cycles)
        )

        # Factor 3: Business Impact (20% weight) - "Afeta o negócio?"
        business_impact_score = self._calculate_business_impact(
            cycles, total_events  # We'll need business hours data here
        )

        # Factor 4: Timing Quality (15% weight) - "Alerta na hora certa?"
        timing_quality_score = self._calculate_timing_quality(
            cycles, avg_duration_minutes, orphaned_alerts
        )

        total_score = (
            alert_relevance_score +
            response_necessity_score +
            business_impact_score +
            timing_quality_score
        )

        # Grade assignment
        if total_score >= 80:
            grade = "A"
        elif total_score >= 70:
            grade = "B"
        elif total_score >= 60:
            grade = "C"
        elif total_score >= 50:
            grade = "D"
        else:
            grade = "F"

        return {
            "score": round(total_score, 1),
            "grade": grade,
            "factors": {
                "alert_relevance": round(alert_relevance_score, 1),
                "response_necessity": round(response_necessity_score, 1),
                "business_impact": round(business_impact_score, 1),
                "timing_quality": round(timing_quality_score, 1)
            }
        }

    def _calculate_alert_relevance(
        self,
        cycles: List[AlertCycle],
        quick_recoveries: int,
        avg_duration_minutes: float,
        orphaned_alerts: int
    ) -> float:
        """Calculate alert relevance score (35% weight)."""

        quick_recovery_rate = quick_recoveries / len(cycles) if cycles else 0

        # Penalize alerts that resolve too quickly (likely noise)
        if quick_recovery_rate > 0.7 and avg_duration_minutes < 5:
            # High frequency of very quick recoveries = noise
            relevance_base = 10
        elif quick_recovery_rate > 0.5 and avg_duration_minutes < 2:
            # Too many instant recoveries = definite noise
            relevance_base = 5
        elif quick_recovery_rate > 0.3 and avg_duration_minutes > 30:
            # Some quick recoveries but gives time to investigate = good
            relevance_base = 25
        elif quick_recovery_rate < 0.2:
            # Most alerts require action = very relevant
            relevance_base = 35
        else:
            # Moderate self-healing with reasonable duration
            relevance_base = 20

        # Bonus for completeness (full lifecycle)
        completion_rate = max(0, 1 - (orphaned_alerts / len(cycles)))
        completeness_bonus = completion_rate * 5

        return min(35, relevance_base + completeness_bonus)

    def _calculate_response_necessity(
        self,
        response_rate: float,
        quick_recoveries: int,
        total_cycles: int
    ) -> float:
        """Calculate response necessity score (30% weight)."""

        # High response rate = high necessity
        if response_rate > 0.8:
            necessity_base = 30  # Always needs action
        elif response_rate > 0.6:
            necessity_base = 25  # Frequently needs action
        elif response_rate > 0.4:
            necessity_base = 18  # Sometimes needs action
        elif response_rate > 0.2:
            necessity_base = 12  # Rarely needs action
        else:
            necessity_base = 5   # Almost never needs action

        # Adjust for quick recoveries (if too many, reduces necessity)
        quick_recovery_rate = quick_recoveries / total_cycles if total_cycles > 0 else 0
        if quick_recovery_rate > 0.8:
            necessity_base *= 0.5  # Heavy penalty for too much self-healing

        return min(30, necessity_base)

    def _calculate_business_impact(self, cycles: List[AlertCycle], total_events: int) -> float:
        """Calculate business impact score (20% weight)."""

        # For now, use a base score - in real implementation, you'd factor in:
        # - Business hours correlation
        # - Service criticality
        # - User impact correlation

        if total_events > 100:
            # High volume = high potential impact
            impact_base = 15
        elif total_events > 50:
            # Medium volume = medium impact
            impact_base = 12
        elif total_events > 10:
            # Low volume but consistent = some impact
            impact_base = 8
        else:
            # Very low volume = minimal impact
            impact_base = 5

        # Could add business hours analysis here when we have that data
        return min(20, impact_base)

    def _calculate_timing_quality(
        self,
        cycles: List[AlertCycle],
        avg_duration_minutes: float,
        orphaned_alerts: int
    ) -> float:
        """Calculate timing quality score (15% weight)."""

        # Good timing = alerts that give appropriate lead time
        if 5 <= avg_duration_minutes <= 60:
            # Good duration - gives time to investigate but not too long
            timing_base = 12
        elif 2 <= avg_duration_minutes < 5:
            # Short but reasonable
            timing_base = 8
        elif avg_duration_minutes > 60:
            # Long alerts might indicate real issues
            timing_base = 10
        else:
            # Too quick = poor timing
            timing_base = 3

        # Penalty for orphaned alerts (incomplete timing)
        orphan_ratio = orphaned_alerts / len(cycles) if cycles else 0
        orphan_penalty = orphan_ratio * 5

        return max(0, min(15, timing_base - orphan_penalty))

    def _get_removal_recommendation(
        self,
        health_score: Dict[str, Any],
        cycles: List[AlertCycle],
        total_events: int
    ) -> Dict[str, Any]:
        """Generate specific removal/action recommendation for the monitor."""

        score = health_score.get("score", 0)

        if score < 30 and total_events > 10:
            return {
                "action": "remove",
                "priority": "high",
                "reason": f"Poor health score ({score:.1f}) with high event volume",
                "confidence": 0.9
            }
        elif score < 50 and len(cycles) > 20:
            return {
                "action": "review_thresholds",
                "priority": "medium",
                "reason": f"Low health score ({score:.1f}) suggests threshold issues",
                "confidence": 0.7
            }
        elif score < 60:
            return {
                "action": "monitor",
                "priority": "low",
                "reason": f"Moderate health score ({score:.1f}) - monitor for improvement",
                "confidence": 0.5
            }
        else:
            return {
                "action": "keep",
                "priority": "none",
                "reason": f"Good health score ({score:.1f}) - monitor is valuable",
                "confidence": 0.8
            }

    def _calculate_hourly_distribution(self, events: List[LifecycleEvent]) -> Dict[int, int]:
        """Calculate distribution of events by hour of day (Brazilian timezone)."""
        hourly_counts = defaultdict(int)
        for event in events:
            if event.timestamp:
                try:
                    # Try using pytz for proper timezone conversion
                    try:
                        import pytz
                        utc_tz = pytz.UTC
                        brazil_tz = pytz.timezone('America/Sao_Paulo')

                        # Ensure timestamp is timezone-aware
                        if event.timestamp.tzinfo is None:
                            utc_time = utc_tz.localize(event.timestamp)
                        else:
                            utc_time = event.timestamp.astimezone(utc_tz)

                        # Convert to Brazilian timezone
                        brazil_time = utc_time.astimezone(brazil_tz)

                    except ImportError:
                        # Fallback to manual timezone conversion
                        brazil_time = event.timestamp.replace(tzinfo=timezone.utc).astimezone(timezone(timedelta(hours=-3)))

                    hourly_counts[brazil_time.hour] += 1
                except Exception:
                    # Final fallback to UTC if conversion fails
                    hourly_counts[event.timestamp.hour] += 1
        return dict(hourly_counts)

    def _calculate_daily_distribution(self, events: List[LifecycleEvent]) -> Dict[str, int]:
        """Calculate distribution of events by day of week (Brazilian timezone)."""
        day_names = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
        daily_counts = defaultdict(int)
        for event in events:
            if event.timestamp:
                try:
                    # Try using pytz for proper timezone conversion
                    try:
                        import pytz
                        utc_tz = pytz.UTC
                        brazil_tz = pytz.timezone('America/Sao_Paulo')

                        # Ensure timestamp is timezone-aware
                        if event.timestamp.tzinfo is None:
                            utc_time = utc_tz.localize(event.timestamp)
                        else:
                            utc_time = event.timestamp.astimezone(utc_tz)

                        # Convert to Brazilian timezone
                        brazil_time = utc_time.astimezone(brazil_tz)

                    except ImportError:
                        # Fallback to manual timezone conversion
                        brazil_time = event.timestamp.replace(tzinfo=timezone.utc).astimezone(timezone(timedelta(hours=-3)))

                    day_name = day_names[brazil_time.weekday()]
                    daily_counts[day_name] += 1
                except Exception:
                    # Final fallback to UTC if conversion fails
                    day_name = day_names[event.timestamp.weekday()]
                    daily_counts[day_name] += 1
        return dict(daily_counts)

    def _calculate_overall_monitor_insights(self, detailed_stats: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
        """Calculate insights across all monitors."""
        if not detailed_stats:
            return {}

        health_scores = [stats.get("health_score", {}).get("score", 0) for stats in detailed_stats.values()]

        grade_distribution = defaultdict(int)
        for stats in detailed_stats.values():
            grade = stats.get("health_score", {}).get("grade", "F")
            grade_distribution[grade] += 1

        return {
            "total_monitors_analyzed": len(detailed_stats),
            "average_health_score": self._round_safe_mean(health_scores),
            "grade_distribution": dict(grade_distribution),
            "monitors_needing_attention": len([s for s in detailed_stats.values()
                                            if s.get("health_score", {}).get("score", 0) < 60]),
            "high_volume_monitors": len([s for s in detailed_stats.values()
                                       if s.get("events_per_day", 0) > 5]),
            "weekend_heavy_monitors": len([s for s in detailed_stats.values()
                                         if s.get("weekend_percentage", 0) > 30])
        }

    def _generate_enhanced_recommendations(self, detailed_stats: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
        """Generate enhanced recommendations based on detailed statistics."""

        high_priority_removals = []
        threshold_adjustments = []
        automation_candidates = []

        for monitor_id, stats in detailed_stats.items():
            recommendation = stats.get("removal_recommendation", {})

            if recommendation.get("action") == "remove" and recommendation.get("priority") == "high":
                high_priority_removals.append({
                    "monitor_id": monitor_id,
                    "monitor_name": stats.get("monitor_name"),
                    "health_score": stats.get("health_score", {}).get("score"),
                    "total_events": stats.get("total_events"),
                    "reason": recommendation.get("reason")
                })

            if stats.get("quick_recovery_rate", 0) > 0.7:
                automation_candidates.append({
                    "monitor_id": monitor_id,
                    "monitor_name": stats.get("monitor_name"),
                    "quick_recovery_rate": stats.get("quick_recovery_rate"),
                    "suggestion": "Consider automating resolution - high rate of quick recoveries"
                })

            if recommendation.get("action") == "review_thresholds":
                threshold_adjustments.append({
                    "monitor_id": monitor_id,
                    "monitor_name": stats.get("monitor_name"),
                    "health_score": stats.get("health_score", {}).get("score"),
                    "cycles_per_week": stats.get("cycles_per_week"),
                    "suggestion": recommendation.get("reason")
                })

        return {
            "high_priority_removals": high_priority_removals,
            "automation_candidates": automation_candidates,
            "threshold_adjustments": threshold_adjustments
        }

    def _is_business_hours_brazil(self, timestamp: datetime) -> bool:
        """Check if timestamp falls within Brazilian business hours (9 AM - 5 PM BRT/BRST)."""
        try:
            # Try using pytz for proper timezone conversion with DST support
            try:
                import pytz
                utc_tz = pytz.UTC
                brazil_tz = pytz.timezone('America/Sao_Paulo')

                # Ensure timestamp is timezone-aware
                if timestamp.tzinfo is None:
                    utc_time = utc_tz.localize(timestamp)
                else:
                    utc_time = timestamp.astimezone(utc_tz)

                # Convert to Brazilian timezone
                brazil_time = utc_time.astimezone(brazil_tz)

            except ImportError:
                # Fallback to manual timezone conversion (UTC-3)
                brazil_time = timestamp.replace(tzinfo=timezone.utc).astimezone(timezone(timedelta(hours=-3)))

            # Check if it's between 9 AM and 5 PM (17:00) Brazilian time
            return 9 <= brazil_time.hour <= 17
        except Exception:
            # Final fallback to UTC-based calculation if conversion fails
            return 9 <= timestamp.hour <= 17

    def _is_weekend_brazil(self, timestamp: datetime) -> bool:
        """Check if timestamp falls on a weekend in Brazilian timezone."""
        try:
            # Try using pytz for proper timezone conversion with DST support
            try:
                import pytz
                utc_tz = pytz.UTC
                brazil_tz = pytz.timezone('America/Sao_Paulo')

                # Ensure timestamp is timezone-aware
                if timestamp.tzinfo is None:
                    utc_time = utc_tz.localize(timestamp)
                else:
                    utc_time = timestamp.astimezone(utc_tz)

                # Convert to Brazilian timezone
                brazil_time = utc_time.astimezone(brazil_tz)

            except ImportError:
                # Fallback to manual timezone conversion (UTC-3)
                brazil_time = timestamp.replace(tzinfo=timezone.utc).astimezone(timezone(timedelta(hours=-3)))

            # Saturday = 5, Sunday = 6 in Python's weekday()
            return brazil_time.weekday() >= 5
        except Exception:
            # Final fallback to UTC-based calculation if conversion fails
            return timestamp.weekday() >= 5

    @staticmethod
    def _calculate_median(values: Iterable[float]) -> Optional[float]:
        """Calculate median of a list of values."""
        values_list = sorted([value for value in values if value is not None])
        if not values_list:
            return None
        n = len(values_list)
        if n % 2 == 0:
            return round((values_list[n // 2 - 1] + values_list[n // 2]) / 2, 2)
        else:
            return round(values_list[n // 2], 2)

    @staticmethod
    def _safe_int(value: Any) -> Optional[int]:
        try:
            return int(value)
        except (TypeError, ValueError):
            return None
