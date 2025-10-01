from __future__ import annotations

import os
from datetime import datetime
from typing import Any, Dict, List, Optional

import requests

from utils.cache_manager.cache_manager import CacheManager
from utils.logging.logging_manager import LogManager


class HitsByEndpointService:
    """
    Service for querying Datadog trace.express.request.hits metrics grouped by endpoint.
    """

    def __init__(
        self,
        site: str,
        api_key: Optional[str] = None,
        app_key: Optional[str] = None,
        use_cache: bool = False,
        cache_ttl_minutes: int = 60,
    ):
        self.logger = LogManager.get_instance().get_logger("HitsByEndpointService")
        self.site = site or os.getenv("DD_SITE", "datadoghq.eu")
        if self.site.startswith("app."):
            self.site = self.site.split("app.", 1)[1]
        self.api_key = api_key or os.getenv("DD_API_KEY")
        self.app_key = app_key or os.getenv("DD_APP_KEY")
        self.use_cache = use_cache
        self.cache_ttl_minutes = cache_ttl_minutes
        self.cache = CacheManager.get_instance()

    def execute(
        self,
        service: str,
        env: str,
        from_ts: int,
        to_ts: int,
        granularity: str = "12h",
        tags: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """
        Query Datadog metrics API for trace.express.request.hits grouped by resource_name.

        Args:
            service: Service name (e.g., 'cropwise-catalog-products-api')
            env: Environment tag (e.g., 'prod')
            from_ts: Start timestamp (epoch seconds)
            to_ts: End timestamp (epoch seconds)
            granularity: Time granularity (1h, 6h, 12h, 24h)
            tags: Additional tag filters (optional)

        Returns:
            Dict with 'series', 'aggregations', 'metadata'
        """
        self.logger.info(
            f"Fetching hits by endpoint for service={service}, env={env}, "
            f"from={from_ts}, to={to_ts}, granularity={granularity}"
        )

        cache_key = self.cache.generate_cache_key(
            prefix="datadog_hits_by_endpoint",
            service=service,
            env=env,
            from_ts=from_ts,
            to_ts=to_ts,
            granularity=granularity,
            tags=",".join(tags or []),
        )

        if self.use_cache:
            cached = self.cache.load(
                cache_key, expiration_minutes=self.cache_ttl_minutes
            )
            if cached is not None:
                self.logger.info("Using cached data")
                return cached

        # Build metric queries (hits and latency)
        hits_query = self._build_query(service, env, tags, metric_type="hits")
        latency_query = self._build_query(service, env, tags, metric_type="latency")

        # Call Datadog timeseries API for both metrics
        raw_hits_series = self._fetch_timeseries(hits_query, from_ts, to_ts)
        raw_latency_series = self._fetch_timeseries(latency_query, from_ts, to_ts)

        if not raw_hits_series:
            self.logger.warning("No hits data returned from Datadog")
            return {
                "series": [],
                "aggregations": {},
                "metadata": {
                    "service": service,
                    "env": env,
                    "from_ts": from_ts,
                    "to_ts": to_ts,
                    "granularity": granularity,
                    "hits_query": hits_query,
                    "latency_query": latency_query,
                    "generated_at": datetime.utcnow().isoformat() + "Z",
                },
            }

        # Normalize series
        normalized_hits = self._normalize_series(raw_hits_series, metric_type="hits")
        normalized_latency = (
            self._normalize_series(raw_latency_series, metric_type="latency")
            if raw_latency_series
            else []
        )

        # Compute aggregations (including latency stats)
        aggregations = self._compute_aggregations(
            normalized_hits, normalized_latency, from_ts, to_ts
        )

        result = {
            "series": normalized_hits,
            "aggregations": aggregations,
            "metadata": {
                "service": service,
                "env": env,
                "from_ts": from_ts,
                "to_ts": to_ts,
                "granularity": granularity,
                "hits_query": hits_query,
                "latency_query": latency_query,
                "generated_at": datetime.utcnow().isoformat() + "Z",
            },
        }

        if self.use_cache:
            self.cache.save(cache_key, result)

        return result

    def _build_query(
        self,
        service: str,
        env: str,
        tags: Optional[List[str]] = None,
        metric_type: str = "hits",
    ) -> str:
        """
        Build Datadog metric query string for hits or latency.
        """
        tag_filter = f"env:{env},service:{service}"
        if tags:
            tag_filter += "," + ",".join(tags)

        if metric_type == "hits":
            query = f"sum:trace.express.request.hits{{{tag_filter}}} by {{resource_name}}.as_count()"
        else:  # latency
            query = f"avg:trace.express.request{{{tag_filter}}} by {{resource_name}}"

        return query

    def _fetch_timeseries(
        self, query: str, from_ts: int, to_ts: int
    ) -> List[Dict[str, Any]]:
        """
        Call Datadog Query Timeseries API.
        """
        url = f"https://api.{self.site}/api/v1/query"
        headers = {
            "DD-API-KEY": self.api_key or "",
            "DD-APPLICATION-KEY": self.app_key or "",
            "Accept": "application/json",
        }
        params = {"from": from_ts, "to": to_ts, "query": query}

        self.logger.debug(f"Fetching timeseries: {url} with query={query}")

        try:
            resp = requests.get(url, headers=headers, params=params, timeout=60)
            resp.raise_for_status()
            data = resp.json()
            series = data.get("series", [])
            self.logger.info(f"Received {len(series)} series from Datadog")
            return series
        except Exception as e:
            self.logger.error(f"Failed to fetch timeseries: {e}")
            raise

    def _normalize_series(
        self, raw_series: List[Dict[str, Any]], metric_type: str = "hits"
    ) -> List[Dict[str, Any]]:
        """
        Convert raw series to normalized format with resource_name, timestamp, hits/latency.
        """
        normalized = []
        for series in raw_series:
            resource_name = series.get("scope", "unknown")
            # Extract resource_name from scope (e.g., "resource_name:GET /api/v1/products")
            if "resource_name:" in resource_name:
                resource_name = resource_name.split("resource_name:", 1)[1].strip()

            # Remove service name from resource_name (e.g., ",service:cropwise-...")
            if ",service:" in resource_name:
                resource_name = resource_name.split(",service:", 1)[0].strip()

            # Uppercase HTTP methods (GET, POST, PUT, DELETE, PATCH)
            for method in ["get_", "post_", "put_", "delete_", "patch_"]:
                if resource_name.lower().startswith(method):
                    resource_name = (
                        method.upper().replace("_", " ") + resource_name[len(method) :]
                    )
                    break

            points = series.get("pointlist", [])
            for point in points:
                if len(point) >= 2:
                    timestamp_ms, value = point[0], point[1]
                    if value is None:
                        continue  # Skip null values
                    timestamp = (
                        datetime.utcfromtimestamp(timestamp_ms / 1000.0).isoformat()
                        + "Z"
                    )
                    record = {
                        "resource_name": resource_name,
                        "timestamp": timestamp,
                        "timestamp_ms": int(timestamp_ms),
                    }
                    if metric_type == "hits":
                        record["hits"] = float(value)
                    else:  # latency
                        record["latency_seconds"] = float(value)
                    normalized.append(record)

        return normalized

    def _compute_aggregations(
        self,
        normalized_hits: List[Dict[str, Any]],
        normalized_latency: List[Dict[str, Any]],
        from_ts: int,
        to_ts: int,
    ) -> Dict[str, Any]:
        """
        Compute total hits, average hits per interval, peak hits, and percentage share per endpoint.
        Also computes latency statistics (p50, p90, p95, p99, etc.)
        """
        from collections import defaultdict

        endpoint_data = defaultdict(lambda: {"hits": [], "total": 0.0, "latencies": []})

        # Collect hits data
        for record in normalized_hits:
            endpoint = record["resource_name"]
            hits = record["hits"]
            endpoint_data[endpoint]["hits"].append(hits)
            endpoint_data[endpoint]["total"] += hits

        # Collect latency data
        for record in normalized_latency:
            endpoint = record["resource_name"]
            latency = record["latency_seconds"]
            endpoint_data[endpoint]["latencies"].append(latency)

        self.logger.info(
            f"Found {len(endpoint_data)} unique endpoints with hits in the period"
        )

        aggregations = {}
        total_hits_all = sum(ep["total"] for ep in endpoint_data.values())
        period_days = (to_ts - from_ts) / 86400.0
        period_months = period_days / 30.0 if period_days > 0 else 1.0

        for endpoint, data in endpoint_data.items():
            hits_list = data["hits"]
            latencies = data["latencies"]
            total = data["total"]
            avg_per_interval = sum(hits_list) / len(hits_list) if hits_list else 0.0
            peak = max(hits_list) if hits_list else 0.0
            share = (total / total_hits_all * 100) if total_hits_all > 0 else 0.0
            monthly_avg = total / period_months if period_months > 0 else 0.0

            # Calculate latency statistics
            latency_stats = self._calculate_latency_stats(latencies)

            aggregations[endpoint] = {
                "total_hits": total,
                "avg_per_interval": avg_per_interval,
                "peak_interval": peak,
                "percentage_share": share,
                "monthly_avg": monthly_avg,
                "latency": latency_stats,
            }

        return aggregations

    def _calculate_latency_stats(
        self, latencies: List[float]
    ) -> Dict[str, float]:
        """
        Calculate comprehensive latency statistics for benchmark.
        Returns percentiles, mean, stddev, CV, etc.
        """
        import numpy as np

        if not latencies:
            return {
                "p50": 0.0,
                "p90": 0.0,
                "p95": 0.0,
                "p99": 0.0,
                "p99_9": 0.0,
                "mean": 0.0,
                "mean_trimmed": 0.0,
                "stddev": 0.0,
                "cv": 0.0,
                "max": 0.0,
                "min": 0.0,
                "count": 0,
            }

        latencies_array = np.array(latencies)

        # Percentiles
        p50 = np.percentile(latencies_array, 50)
        p90 = np.percentile(latencies_array, 90)
        p95 = np.percentile(latencies_array, 95)
        p99 = np.percentile(latencies_array, 99)
        p99_9 = np.percentile(latencies_array, 99.9)

        # Mean and stddev
        mean = np.mean(latencies_array)
        stddev = np.std(latencies_array)

        # Trimmed mean (remove 1% outliers on each side - between p1 and p99)
        p1 = np.percentile(latencies_array, 1)
        trimmed = latencies_array[
            (latencies_array >= p1) & (latencies_array <= p99)
        ]
        mean_trimmed = np.mean(trimmed) if len(trimmed) > 0 else mean

        # Coefficient of variation
        cv = (stddev / mean) if mean > 0 else 0.0

        return {
            "p50": float(p50),
            "p90": float(p90),
            "p95": float(p95),
            "p99": float(p99),
            "p99_9": float(p99_9),
            "mean": float(mean),
            "mean_trimmed": float(mean_trimmed),
            "stddev": float(stddev),
            "cv": float(cv),
            "max": float(np.max(latencies_array)),
            "min": float(np.min(latencies_array)),
            "count": len(latencies),
        }
