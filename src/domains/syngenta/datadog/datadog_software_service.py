from __future__ import annotations

import os
from typing import Any, Dict, List, Optional, Tuple

import json
import requests

from utils.cache_manager.cache_manager import CacheManager
from utils.logging.logging_manager import LogManager

from .datadog_service import DatadogService


class DatadogSoftwareService:
    """
    Wrapper for Datadog Software Catalog (v2) and Teams APIs.

    Responsibilities:
    - Validate/resolve team handles (via Teams v2 using existing DatadogService)
    - List software entities of kind=service for a given owner/team handle
      • First try server-side `filter[owner]=<team_handle>`
      • Fallback to client-side filtering over all `kind=service` entities
    - Handle pagination with `page[size]` and `page[number]`
    - Provide normalized records including: id, name, owner, links, env_facets
    - Optional caching (TTL minutes) using CacheManager
    """

    def __init__(
        self,
        *,
        site: str,
        api_key: Optional[str] = None,
        app_key: Optional[str] = None,
        use_cache: bool = False,
        cache_ttl_minutes: int = 30,
    ) -> None:
        self.logger = LogManager.get_instance().get_logger("DatadogSoftwareService")
        self.site = (site or os.getenv("DD_SITE") or "datadoghq.com").strip()
        if self.site.startswith("app."):
            original = self.site
            self.site = self.site.split("app.", 1)[1]
            self.logger.warning(
                f"Sanitized DD_SITE from '{original}' to '{self.site}'. Use values like 'datadoghq.com'."
            )
        self.api_key = api_key or os.getenv("DD_API_KEY") or ""
        self.app_key = app_key or os.getenv("DD_APP_KEY") or ""
        self.use_cache = bool(use_cache)
        self.cache_ttl_minutes = cache_ttl_minutes
        self.cache = CacheManager.get_instance()

        # Reuse Teams API helpers from existing service, but tolerate missing SDK
        try:
            self._teams: Optional[DatadogService] = DatadogService(
                site=self.site,
                api_key=self.api_key,
                app_key=self.app_key,
                use_cache=use_cache,
                cache_ttl_minutes=cache_ttl_minutes,
            )
        except RuntimeError:
            # SDK not installed. We'll allow proceeding without team validation.
            self.logger.info(
                "datadog-api-client not installed; team validation disabled. Proceeding without validation."
            )
            self._teams = None

    # -------- Public API --------
    def get_team(self, handle: str) -> Optional[Dict[str, Any]]:
        """Resolve a team by handle via Teams v2 (SDK path reused)."""
        if self._teams is None:
            return None
        try:
            return self._teams.get_team(handle)
        except Exception as e:
            self.logger.warning(f"Team validation failed for '{handle}': {e}")
            return None

    def list_services_for_team(self, handle: str) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
        """
        Return a tuple: (services, meta) where services are normalized service entities
        and meta captures details like fallbacks used.
        """
        cache_key = self.cache.generate_cache_key(
            prefix="dd_software_services_for_team", site=self.site, team=handle
        )
        if self.use_cache:
            cached = self.cache.load(cache_key, expiration_minutes=self.cache_ttl_minutes)
            if cached is not None:
                return cached, {"cache": True}

        meta: Dict[str, Any] = {"fallback": None, "pages": 0}

        # 1) Try server-side owner filter
        try:
            entities, pages = self._list_entities(kind="service", owner=handle)
            meta["pages"] = pages
            filtered = [self._normalize_entity(e) for e in entities]
            if filtered:
                if self.use_cache:
                    self.cache.save(cache_key, filtered)
                return filtered, meta
            # If empty, try fallback path (tenant may not support owner filter fully)
            meta["fallback"] = "client_filtering"
        except requests.HTTPError as e:
            status = e.response.status_code if e.response is not None else None
            # Owner filter not allowed/available for the tenant
            if status in (400, 403, 404):
                self.logger.info(
                    f"Owner filter path unavailable (HTTP {status}); falling back to client-side filtering."
                )
                meta["fallback"] = f"client_filtering_http_{status}"
            else:
                self.logger.warning(f"Owner filter request failed: {e}. Falling back to client-side filtering.")
                meta["fallback"] = "client_filtering_error"
        except Exception as e:
            self.logger.warning(f"Owner filter path errored: {e}. Falling back to client-side filtering.")
            meta["fallback"] = "client_filtering_error"

        # 2) Fallback: fetch all service entities and filter by owner attributes client-side
        all_entities, pages = self._list_entities(kind="service", owner=None)
        meta["pages"] = pages
        filtered = []
        for ent in all_entities:
            norm = self._normalize_entity(ent)
            # Accept match if owner equals handle or tags/annotations contain dd_team/team
            owner = (norm.get("owner") or "").strip().lower()
            if owner == handle.lower():
                filtered.append(norm)
                continue

            # Additional deep checks for owner in raw payload
            attrs = (ent or {}).get("attributes") or {}
            candidate = (
                (attrs.get("catalog_definition") or {}).get("team")
                or attrs.get("team")
                or (attrs.get("annotations") or {}).get("owner")
            )
            if isinstance(candidate, str) and candidate.strip().lower() == handle.lower():
                filtered.append(norm)
                continue

            tags = (attrs.get("tags") or []) if isinstance(attrs.get("tags"), list) else []
            if any(
                isinstance(t, str)
                and (t.lower() == f"dd_team:{handle}".lower() or t.lower() == f"team:{handle}".lower())
                for t in tags
            ):
                filtered.append(norm)

        if self.use_cache:
            self.cache.save(cache_key, filtered)
        return filtered, meta

    # -------- HTTP helpers --------
    def _api_base(self) -> str:
        host = self.site
        if not host.startswith("api."):
            host = f"api.{host}"
        return f"https://{host}"

    def _headers(self) -> Dict[str, str]:
        return {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "DD-API-KEY": self.api_key,
            "DD-APPLICATION-KEY": self.app_key,
        }

    def _list_entities(self, *, kind: str, owner: Optional[str]) -> Tuple[List[Dict[str, Any]], int]:
        """
        Page through /api/v2/catalog/entity with optional server-side filters.
        Returns (entities, total_pages_visited).
        """
        all_items: List[Dict[str, Any]] = []
        page_number = 0
        page_size = 100
        total_pages = 0

        while True:
            params = {
                "page[size]": str(page_size),
                "page[number]": str(page_number),
                "filter[kind]": kind,
            }
            if owner:
                params["filter[owner]"] = owner

            url = f"{self._api_base()}/api/v2/catalog/entity"
            resp = requests.get(url, headers=self._headers(), params=params, timeout=40)
            if resp.status_code >= 400:
                # Raise for caller to decide if fallback is needed
                try:
                    resp.raise_for_status()
                except Exception:
                    # Attach context for better error logs
                    self.logger.debug(
                        f"/catalog/entity error {resp.status_code}, body={resp.text[:500]} params={json.dumps(params)}"
                    )
                    raise

            data = resp.json() or {}
            items = (data.get("data") or []) if isinstance(data.get("data"), list) else []
            total_pages += 1
            if not items:
                break
            # Ensure dicts
            all_items.extend(items)
            if len(items) < page_size:
                break
            page_number += 1

            # Safety cap to avoid infinite loops
            if page_number > 1000:
                self.logger.warning("Pagination safety cap reached (1000 pages) for /catalog/entity")
                break

        return all_items, total_pages

    # -------- Normalization --------
    def _normalize_entity(self, entity: Dict[str, Any]) -> Dict[str, Any]:
        attrs = entity.get("attributes") or {}
        # Id and name
        eid = entity.get("id") or attrs.get("id") or ""
        name = attrs.get("name") or entity.get("id") or ""

        # Resolve owner using several known locations
        owner = (
            attrs.get("owner")
            or (attrs.get("catalog_definition") or {}).get("team")
            or attrs.get("team")
            or (attrs.get("annotations") or {}).get("owner")
            or ""
        )

        # Links: best-effort extraction (dict or list)
        links = self._extract_links(attrs.get("links"))

        # Env facets: try explicit field; fallback to tags with env:*
        env_facets = self._extract_env_facets(attrs)

        return {
            "id": eid,
            "name": name,
            "owner": owner if isinstance(owner, str) else str(owner),
            "links": links,
            "env_facets": env_facets,
        }

    @staticmethod
    def _extract_links(raw: Any) -> Dict[str, str]:
        out: Dict[str, str] = {}
        if not raw:
            return out
        # If dict with common keys
        if isinstance(raw, dict):
            for key in ("docs", "documentation", "repo", "repository", "runbook"):
                val = raw.get(key)
                if isinstance(val, str) and val:
                    canonical = {
                        "documentation": "docs",
                        "repository": "repo",
                    }.get(key, key)
                    out[canonical] = val
            return out
        # If list of {name|title|type, url}
        if isinstance(raw, list):
            for item in raw:
                if not isinstance(item, dict):
                    continue
                title = item.get("name") or item.get("title") or item.get("type") or "link"
                url = item.get("url") or item.get("href")
                if isinstance(url, str) and url:
                    key = title.lower().strip()
                    canonical = {
                        "documentation": "docs",
                        "docs": "docs",
                        "repo": "repo",
                        "repository": "repo",
                        "runbook": "runbook",
                    }.get(key, key)
                    out[canonical] = url
        return out

    @staticmethod
    def _extract_env_facets(attrs: Dict[str, Any]) -> List[str]:
        # Known fields
        envs = []
        for key in ("environments", "env", "envs"):
            val = attrs.get(key)
            if isinstance(val, list):
                envs.extend([str(v) for v in val if isinstance(v, (str, int))])
            elif isinstance(val, str) and val:
                envs.append(val)

        # From tags of form env:<name>
        tags = attrs.get("tags")
        if isinstance(tags, list):
            for t in tags:
                if isinstance(t, str) and t.lower().startswith("env:"):
                    envs.append(t.split(":", 1)[1])

        # Deduplicate while preserving order
        seen = set()
        deduped: List[str] = []
        for v in envs:
            if v not in seen:
                seen.add(v)
                deduped.append(v)
        return deduped
