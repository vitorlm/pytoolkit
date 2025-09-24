from __future__ import annotations

import os
from typing import Any, Dict, List, Optional

from utils.cache_manager.cache_manager import CacheManager
from utils.logging.logging_manager import LogManager


class DatadogService:
    """
    Lightweight wrapper around Datadog API client (v2) for Teams and Service Catalog.

    - Resolves team handles to ensure they exist.
    - Lists service definitions and filters them by team association.
    - Encapsulates pagination and returns plain Python dicts.
    """

    def __init__(
        self,
        site: str,
        api_key: Optional[str] = None,
        app_key: Optional[str] = None,
        use_cache: bool = False,
        cache_ttl_minutes: int = 30,
    ) -> None:
        self._logger = LogManager.get_instance().get_logger("DatadogService")
        self.site = site or os.getenv("DD_SITE", "datadoghq.com")
        # Sanitize common misconfig: 'app.datadoghq.eu' → 'datadoghq.eu'
        if isinstance(self.site, str) and self.site.startswith("app."):
            original = self.site
            self.site = self.site.split("app.", 1)[1]
            self._logger.warning(
                f"Sanitized DD_SITE from '{original}' to '{self.site}'. Use values like 'datadoghq.eu'."
            )
        self.api_key = api_key or os.getenv("DD_API_KEY")
        self.app_key = app_key or os.getenv("DD_APP_KEY")
        self.use_cache = use_cache
        self.cache_ttl_minutes = cache_ttl_minutes
        self.cache = CacheManager.get_instance()

        try:
            from datadog_api_client import Configuration
            from datadog_api_client.v2 import ApiClient

            configuration = Configuration()
            configuration.server_variables["site"] = self.site

            # Set keys via env or directly on config.headers (env is preferred in library)
            if self.api_key:
                os.environ.setdefault("DD_API_KEY", self.api_key)
            if self.app_key:
                os.environ.setdefault("DD_APP_KEY", self.app_key)

            self._api_client = ApiClient(configuration)
        except ImportError as e:
            # Provide a descriptive error; the command will handle and exit non-zero
            raise RuntimeError(
                "datadog-api-client not installed. Please add 'datadog-api-client' to requirements and pip install."
            ) from e

    # -------- Teams --------
    def get_team(self, handle: str) -> Optional[Dict[str, Any]]:
        """
        Resolve a team by handle using Teams v2 API. Returns None if not found.
        """
        try:
            from datadog_api_client.v2.api.teams_api import TeamsApi

            with self._api_client as api:
                teams_api = TeamsApi(api)

                # Try server-side filtering if available
                page_size = 100
                for page_number in range(0, 50):  # hard cap to avoid infinite loops
                    try:
                        # Newer clients often expose filter_query for text search
                        resp = teams_api.list_teams(page_size=page_size, page_number=page_number, filter_query=handle)
                    except TypeError:
                        # Some versions use filter_keyword instead
                        resp = teams_api.list_teams(page_size=page_size, page_number=page_number, filter_keyword=handle)

                    items = self._extract_items(resp)
                    if not items:
                        break

                    for t in items:
                        t_dict = self._normalize_model(t)
                        # Match by attributes.handle or top-level handle (case-insensitive)
                        attr = t_dict.get("attributes", {}) if isinstance(t_dict, dict) else {}
                        attr_handle = (attr.get("handle") if isinstance(attr, dict) else None) or t_dict.get("handle")
                        if attr_handle and str(attr_handle).lower() == handle.lower():
                            return {
                                "id": t_dict.get("id") or t_dict.get("data", {}).get("id"),
                                "name": t_dict.get("name") or attr.get("name"),
                                "handle": attr_handle,
                            }

                    # If using filter, a non-empty page means we already searched; break if less than full page
                    if len(items) < page_size:
                        break

                # Fallback to full listing if filter didn't find it
                page_number = 0
                while True:
                    resp = teams_api.list_teams(page_size=page_size, page_number=page_number)
                    items = self._extract_items(resp)
                    if not items:
                        break
                    for t in items:
                        t_dict = self._normalize_model(t)
                        attr = t_dict.get("attributes", {}) if isinstance(t_dict, dict) else {}
                        attr_handle = (attr.get("handle") if isinstance(attr, dict) else None) or t_dict.get("handle")
                        if attr_handle and str(attr_handle).lower() == handle.lower():
                            return {
                                "id": t_dict.get("id") or t_dict.get("data", {}).get("id"),
                                "name": t_dict.get("name") or attr.get("name"),
                                "handle": attr_handle,
                            }
                    if len(items) < page_size:
                        break
                    page_number += 1
        except Exception as e:
            self._logger.error(f"Failed to resolve team '{handle}': {e}")
            return None
        return None

    # -------- Service Catalog --------
    def list_services_for_team(self, handle: str) -> List[Dict[str, Any]]:
        """
        List service definitions and return only those associated to the given team handle.
        Also flags services missing proper team linkage.
        """
        cache_key = self.cache.generate_cache_key(prefix="datadog_services_for_team", site=self.site, team=handle)
        if self.use_cache:
            cached = self.cache.load(cache_key, expiration_minutes=self.cache_ttl_minutes)
            if cached is not None:
                return cached

        try:
            all_services = self._list_all_service_definitions()

            out: List[Dict[str, Any]] = []
            for svc in all_services:
                svc_dict = self._normalize_model(svc)

                # Extract core fields robustly
                name = (
                    svc_dict.get("name")
                    or svc_dict.get("attributes", {}).get("name")
                    or svc_dict.get("attributes", {}).get("title")
                )
                sid = svc_dict.get("id") or svc_dict.get("data", {}).get("id")

                # The team association may exist directly or within attributes
                svc_team = (svc_dict.get("team") or svc_dict.get("attributes", {}).get("team")) or None

                # Fallback: check tags for team:<handle>
                tags = svc_dict.get("tags") or svc_dict.get("attributes", {}).get("tags") or []
                tags = tags or []
                found_by_tag = any(
                    isinstance(tag, str) and tag.lower() == f"team:{handle}".lower() for tag in tags
                )

                # Only consider services that match by explicit team OR by fallback tag
                matched = (
                    bool(svc_team) and str(svc_team).lower() == handle.lower()
                ) or found_by_tag
                if not matched:
                    continue

                # team_link_ok is true only if the service.team matches the handle
                team_link_ok = bool(svc_team) and str(svc_team).lower() == handle.lower()

                # Extract contacts and links if present (best-effort)
                contacts = svc_dict.get("contacts") or svc_dict.get("attributes", {}).get("contacts") or []
                links = svc_dict.get("links") or svc_dict.get("attributes", {}).get("links") or {}

                record = {
                    "name": name,
                    "id": sid,
                    "team": svc_team,
                    "team_link_ok": bool(team_link_ok),
                    "contacts": contacts if isinstance(contacts, list) else [],
                    "links": links if isinstance(links, dict) else {},
                }

                # Include only services matched by team or tag
                out.append(record)

            if self.use_cache:
                self.cache.save(cache_key, out)
            return out
        except Exception as e:
            self._logger.error(f"Failed listing services for team '{handle}': {e}")
            return []

    # -------- Internal helpers --------
    def _list_all_service_definitions(self) -> List[Any]:
        """
        Retrieve all service definitions with pagination.
        """
        from datadog_api_client.v2.api.service_definition_api import ServiceDefinitionApi

        all_items: List[Any] = []
        with self._api_client as api:
            svc_api = ServiceDefinitionApi(api)
            page_number = 0
            page_size = 100
            while True:
                resp = svc_api.list_service_definitions(page_size=page_size, page_number=page_number)
                items = self._extract_items(resp)
                if not items:
                    break
                all_items.extend(items)
                if len(items) < page_size:
                    break
                page_number += 1
        return all_items

    # ---- Debug helpers ----
    def list_team_handles(self, query: Optional[str] = None, limit: int = 200) -> List[Dict[str, str]]:
        """Return a list of {handle, name} for quick discovery/debugging."""
        from datadog_api_client.v2.api.teams_api import TeamsApi

        out: List[Dict[str, str]] = []
        with self._api_client as api:
            teams_api = TeamsApi(api)
            page_size = min(100, max(1, limit))
            page_number = 0
            while len(out) < limit:
                try:
                    resp = (
                        teams_api.list_teams(page_size=page_size, page_number=page_number, filter_query=query)
                        if query is not None
                        else teams_api.list_teams(page_size=page_size, page_number=page_number)
                    )
                except TypeError:
                    resp = (
                        teams_api.list_teams(page_size=page_size, page_number=page_number, filter_keyword=query)
                        if query is not None
                        else teams_api.list_teams(page_size=page_size, page_number=page_number)
                    )
                items = self._extract_items(resp)
                if not items:
                    break
                for t in items:
                    t_dict = self._normalize_model(t)
                    attr = t_dict.get("attributes", {}) if isinstance(t_dict, dict) else {}
                    handle = (attr.get("handle") if isinstance(attr, dict) else None) or t_dict.get("handle")
                    name = (attr.get("name") if isinstance(attr, dict) else None) or t_dict.get("name")
                    if handle:
                        out.append({"handle": str(handle), "name": str(name) if name else ""})
                        if len(out) >= limit:
                            break
                if len(items) < page_size:
                    break
                page_number += 1
        return out

    def list_teams(self, query: Optional[str] = None, limit: int = 500) -> List[Dict[str, str]]:
        """
        List teams with id, handle, and name. Supports optional text query and limit.
        """
        from datadog_api_client.v2.api.teams_api import TeamsApi

        out: List[Dict[str, str]] = []
        with self._api_client as api:
            teams_api = TeamsApi(api)
            page_size = 100
            page_number = 0
            fetched = 0
            while fetched < limit:
                try:
                    resp = (
                        teams_api.list_teams(page_size=page_size, page_number=page_number, filter_query=query)
                        if query is not None
                        else teams_api.list_teams(page_size=page_size, page_number=page_number)
                    )
                except TypeError:
                    resp = (
                        teams_api.list_teams(page_size=page_size, page_number=page_number, filter_keyword=query)
                        if query is not None
                        else teams_api.list_teams(page_size=page_size, page_number=page_number)
                    )
                items = self._extract_items(resp)
                if not items:
                    break
                for t in items:
                    t_dict = self._normalize_model(t)
                    attr = t_dict.get("attributes", {}) if isinstance(t_dict, dict) else {}
                    handle = (attr.get("handle") if isinstance(attr, dict) else None) or t_dict.get("handle")
                    name = (attr.get("name") if isinstance(attr, dict) else None) or t_dict.get("name")
                    tid = t_dict.get("id") or (t_dict.get("data", {}) if isinstance(t_dict.get("data", {}), dict) else {}).get("id")
                    if handle:
                        out.append({
                            "id": str(tid) if tid else "",
                            "handle": str(handle),
                            "name": str(name) if name else "",
                        })
                        fetched += 1
                        if fetched >= limit:
                            break
                if len(items) < page_size:
                    break
                page_number += 1
        return out

    # -------- Raw HTTP (fallback / explicit) --------
    def _build_api_url(self, host_kind: str, path: str) -> str:
        host = self.site
        if not host.startswith("app.") and not host.startswith("api."):
            host = f"{host_kind}.{host}"
        return f"https://{host}{path}"

    def list_teams_raw(
        self,
        *,
        me: Optional[bool] = None,
        limit: int = 100,
        fields: Optional[List[str]] = None,
        use_app_host: bool = True,
    ) -> List[Dict[str, str]]:
        import requests

        fields = fields or [
            "id",
            "handle",
            "name",
            "summary",
            "description",
            "avatar",
            "banner",
            "link_count",
            "user_count",
            "user_team_permissions",
            "team_links",
            "is_managed",
        ]

        url = self._build_api_url("app" if use_app_host else "api", "/api/v2/team")
        headers = {
            "Accept": "application/json",
            "DD-API-KEY": self.api_key or "",
            "DD-APPLICATION-KEY": self.app_key or "",
        }

        teams: List[Dict[str, str]] = []
        offset = 0
        page_limit = min(100, max(1, limit))

        while len(teams) < limit:
            params = {
                "page[limit]": page_limit,
                "page[offset]": offset,
                "fields[team]": ",".join(fields),
            }
            if me is not None:
                params["filter[me]"] = "true" if me else "false"

            resp = requests.get(url, headers=headers, params=params, timeout=30)
            resp.raise_for_status()
            data = resp.json() or {}

            for item in (data.get("data") or []):
                t_attr = item.get("attributes") or {}
                teams.append(
                    {
                        "id": item.get("id", ""),
                        "handle": t_attr.get("handle", ""),
                        "name": t_attr.get("name", ""),
                        "user_count": t_attr.get("user_count", 0),
                        "link_count": t_attr.get("link_count", 0),
                    }
                )
                if len(teams) >= limit:
                    break

            meta = (data.get("meta") or {}).get("pagination") or {}
            total = int(meta.get("total", 0) or 0)
            if len(teams) >= total or len(teams) >= limit:
                break
            next_offset = meta.get("next_offset")
            if next_offset is None:
                break
            offset = int(next_offset)

        return teams

    @staticmethod
    def _extract_items(response: Any) -> List[Any]:
        """Best-effort extraction of array-like items from API responses."""
        if response is None:
            return []
        if isinstance(response, list):
            return response
        # datadog models often expose `.data` or are iterable
        data = getattr(response, "data", None)
        if isinstance(data, list):
            return data
        if hasattr(response, "to_dict"):
            rd = response.to_dict()  # type: ignore[attr-defined]
            if isinstance(rd, dict):
                for key in ("data", "items", "results"):
                    val = rd.get(key)
                    if isinstance(val, list):
                        return val
        return []

    @staticmethod
    def _normalize_model(obj: Any) -> Dict[str, Any]:
        """
        Convert datadog model instances into plain dicts safely.
        """
        if obj is None:
            return {}
        if isinstance(obj, dict):
            return obj
        if hasattr(obj, "to_dict"):
            try:
                return obj.to_dict()  # type: ignore[attr-defined]
            except Exception:
                pass
        # Fallback: introspect common attributes
        out: Dict[str, Any] = {}
        for attr in ("id", "name", "attributes", "data", "links", "contacts", "team", "tags"):
            if hasattr(obj, attr):
                out[attr] = getattr(obj, attr)
        return out
