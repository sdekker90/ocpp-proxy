"""Configuration and shared state for upstream OCPP services (CSMS such as Monta or Zeres)."""

import base64
import json
import logging
import os
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any
from urllib.parse import quote

import aiohttp

if TYPE_CHECKING:
    from .relay import ChargerRelay

_LOGGER = logging.getLogger(__name__)

CHARGE_POINT_ID_PLACEHOLDER = "{charge_point_id}"


@dataclass(frozen=True)
class ServiceConfig:
    """One upstream OCPP service the charger is relayed to."""

    service_id: str
    url: str
    primary: bool = False
    read_only: bool = True
    auth_type: str = "none"
    username: str | None = None
    password: str | None = None
    token: str | None = None

    def url_for(self, cp_id: str) -> str:
        """Build the service URL for a charger, like the charger itself would.

        A charger appends its own ID to the configured OCPP URL; the proxy does the same.
        Use ``{charge_point_id}`` in the URL to place the ID somewhere else.
        """
        quoted = quote(cp_id, safe="")
        if CHARGE_POINT_ID_PLACEHOLDER in self.url:
            return self.url.replace(CHARGE_POINT_ID_PLACEHOLDER, quoted)
        return f"{self.url.rstrip('/')}/{quoted}"

    def headers_for(self, cp_id: str) -> dict[str, str]:
        """Return authentication headers for the connection."""
        if self.auth_type == "basic" and self.password:
            # OCPP security profile 1: the username is the charge point ID by default.
            username = self.username or cp_id
            credentials = base64.b64encode(f"{username}:{self.password}".encode()).decode()
            return {"Authorization": f"Basic {credentials}"}
        if self.auth_type == "token" and self.token:
            return {"Authorization": f"Bearer {self.token}"}
        return {}


def parse_services(raw_services: list[dict[str, Any]]) -> list[ServiceConfig]:
    """Turn add-on options into service configs, choosing exactly one primary."""
    enabled = []
    for raw in raw_services:
        if not raw.get("enabled", True):
            continue
        if not raw.get("id") or not raw.get("url"):
            _LOGGER.error("Ignoring OCPP service without id or url: %s", raw.get("id"))
            continue
        enabled.append(raw)

    flagged = [raw["id"] for raw in enabled if raw.get("primary")]
    if len(flagged) > 1:
        _LOGGER.warning("Multiple primary OCPP services %s; using %s", flagged, flagged[0])
    primary_id = flagged[0] if flagged else (enabled[0]["id"] if enabled else None)

    services = []
    for raw in enabled:
        is_primary = raw["id"] == primary_id
        services.append(
            ServiceConfig(
                service_id=str(raw["id"]),
                url=str(raw["url"]),
                primary=is_primary,
                read_only=bool(raw.get("read_only", not is_primary)),
                auth_type=str(raw.get("auth_type") or "none"),
                username=raw.get("username") or None,
                password=raw.get("password") or None,
                token=raw.get("token") or None,
            )
        )
    return services


class TransactionStore:
    """Maps the transaction IDs the charger knows to the IDs each service issued.

    Persisted to disk so an add-on restart during a charging session does not lose the
    mapping needed to deliver StopTransaction to each service.
    """

    def __init__(self, path: str | None = None) -> None:
        self._path = path
        self._map: dict[str, dict[str, int]] = {}
        if path and os.path.exists(path):
            try:
                with open(path) as f:
                    loaded = json.load(f)
                if isinstance(loaded, dict):
                    self._map = loaded
            except (OSError, ValueError):
                _LOGGER.exception("Could not read transaction map %s", path)

    def get(self, service_id: str, charger_tx: int) -> int | None:
        """Return the service's transaction ID for a charger transaction ID."""
        return self._map.get(service_id, {}).get(str(charger_tx))

    def reverse(self, service_id: str, service_tx: int) -> int | None:
        """Return the charger transaction ID for a service's transaction ID."""
        for charger_tx, mapped in self._map.get(service_id, {}).items():
            if mapped == service_tx:
                return int(charger_tx)
        return None

    def set(self, service_id: str, charger_tx: int, service_tx: int) -> None:
        """Record a mapping."""
        self._map.setdefault(service_id, {})[str(charger_tx)] = service_tx
        self._save()

    def remove(self, service_id: str, charger_tx: int) -> None:
        """Forget a mapping once the transaction is finished."""
        if self._map.get(service_id, {}).pop(str(charger_tx), None) is not None:
            self._save()

    def _save(self) -> None:
        if not self._path:
            return
        tmp_path = f"{self._path}.tmp"
        try:
            with open(tmp_path, "w") as f:
                json.dump(self._map, f)
            os.replace(tmp_path, self._path)
        except OSError:
            _LOGGER.exception("Could not write transaction map %s", self._path)


class OCPPServiceManager:
    """Holds upstream service configuration and the state shared across charger connections.

    Connections to the services are opened per charger connection by ``ChargerRelay``,
    because the URL depends on the charger's ID.
    """

    def __init__(self, config: Any, backend_manager: Any = None) -> None:
        self.config = config
        self.backend_manager = backend_manager
        self.services = parse_services(list(getattr(config, "ocpp_services", []) or []))
        self.tx_store = TransactionStore(os.getenv("TX_MAP_PATH"))
        self._session: aiohttp.ClientSession | None = None
        self._relay: ChargerRelay | None = None

    @property
    def primary(self) -> ServiceConfig | None:
        """The service whose answers are passed back to the charger."""
        return next((s for s in self.services if s.primary), None)

    async def start_services(self) -> None:
        """Log the configured services; connections open once the charger connects."""
        if not self.services:
            _LOGGER.info("No OCPP services configured; the proxy answers the charger itself")
        for service in self.services:
            role = "primary" if service.primary else "secondary"
            access = "read-only" if service.read_only else "control"
            _LOGGER.info(
                "OCPP service %s (%s, %s): %s", service.service_id, role, access, service.url
            )

    async def get_session(self) -> aiohttp.ClientSession:
        """Return the shared HTTP session for outbound WebSocket connections."""
        if self._session is None or self._session.closed:
            # The timeout covers the WebSocket handshake, not the open connection.
            self._session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30))
        return self._session

    def attach(self, relay: "ChargerRelay") -> None:
        """Register the relay of the currently connected charger."""
        self._relay = relay

    def detach(self, relay: "ChargerRelay") -> None:
        """Unregister a relay when its charger disconnects."""
        if self._relay is relay:
            self._relay = None

    async def stop_all_services(self) -> None:
        """Close all upstream connections."""
        if self._relay is not None:
            await self._relay.close_links()
        if self._session is not None and not self._session.closed:
            await self._session.close()

    def get_service_status(self) -> dict[str, dict[str, Any]]:
        """Return the connection state of every configured service."""
        if self._relay is not None:
            return self._relay.link_status()
        return {
            s.service_id: {
                "connected": False,
                "primary": s.primary,
                "read_only": s.read_only,
                "url": s.url,
            }
            for s in self.services
        }
