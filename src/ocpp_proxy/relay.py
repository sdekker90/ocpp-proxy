"""Transparent OCPP-J relay between one charger and the configured upstream services.

The charger talks to the proxy as if it were its CSMS. Every message the charger sends is
forwarded to all services; the primary service's answer goes back to the charger. Requests
from the primary service (remote start/stop, configuration, ...) are forwarded to the
charger. Read-only services only receive data.

For OCPP 1.6 each service issues its own transaction IDs, so the proxy hands the charger its
own ID and translates it per service.
"""

import asyncio
import datetime
import json
import logging
import random
import time
import uuid
from collections import deque
from collections.abc import Coroutine
from dataclasses import dataclass
from typing import Any

import aiohttp
from aiohttp import web

from .ocpp_service_manager import OCPPServiceManager, ServiceConfig, TransactionStore

_LOGGER = logging.getLogger(__name__)

CALL = 2
CALLRESULT = 3
CALLERROR = 4

SUBPROTOCOLS = {"1.6": "ocpp1.6", "2.0.1": "ocpp2.0.1"}

RESPONSE_TIMEOUT = 20.0
CONNECT_WAIT = 10.0
RECONNECT_MIN_DELAY = 5.0
RECONNECT_MAX_DELAY = 60.0
QUEUE_LIMIT = 1000

# Charger messages a service must still receive after being offline for a while.
TRANSACTIONAL_ACTIONS = frozenset(
    {"StartTransaction", "StopTransaction", "MeterValues", "TransactionEvent"}
)
# Requests a read-only service may still send to the charger.
READ_ONLY_ACTIONS = frozenset(
    {"GetConfiguration", "TriggerMessage", "GetVariables", "GetBaseReport", "GetReport"}
)
START_ACTIONS = frozenset({"RemoteStartTransaction", "RequestStartTransaction"})

Frame = list[Any]


def version_from_subprotocol(subprotocol: str | None, default: str = "1.6") -> str:
    """Map the negotiated WebSocket subprotocol to an OCPP version."""
    if subprotocol == "ocpp2.0.1":
        return "2.0.1"
    if subprotocol == "ocpp1.6":
        return "1.6"
    return default


def parse_frame(data: str) -> Frame | None:
    """Parse an OCPP-J frame, returning None when it is malformed."""
    try:
        frame = json.loads(data)
    except ValueError:
        return None
    if not isinstance(frame, list) or len(frame) < 3 or not isinstance(frame[1], str):
        return None
    if frame[0] == CALL:
        if len(frame) < 4 or not isinstance(frame[2], str) or not isinstance(frame[3], dict):
            return None
        return frame
    if frame[0] in (CALLRESULT, CALLERROR):
        return frame
    return None


def _now() -> str:
    return datetime.datetime.now(datetime.UTC).isoformat()


class _TransactionIds:
    """Transaction IDs handed to the charger; they keep increasing across restarts."""

    last = 0

    @classmethod
    def next(cls) -> int:
        cls.last = max(cls.last + 1, int(time.time()) % 2_000_000_000)
        return cls.last


def local_response(version: str, action: str, charger_tx: int | None = None) -> dict[str, Any]:
    """Answer a charger request when the primary service cannot."""
    if action == "BootNotification":
        return {"status": "Accepted", "currentTime": _now(), "interval": 300}
    if action == "Heartbeat":
        return {"currentTime": _now()}
    if action == "Authorize":
        key = "idTokenInfo" if version == "2.0.1" else "idTagInfo"
        return {key: {"status": "Accepted"}}
    if action == "StartTransaction":
        return {"transactionId": charger_tx, "idTagInfo": {"status": "Accepted"}}
    if action == "DataTransfer":
        return {"status": "UnknownVendorId"}
    return {}


@dataclass(eq=False)
class _Item:
    """A charger message waiting to be delivered to one service."""

    action: str
    payload: dict[str, Any]
    charger_tx: int | None = None
    future: "asyncio.Future[Frame | None] | None" = None
    replay: bool = False

    def resolve(self, frame: Frame | None) -> None:
        if self.future is not None and not self.future.done():
            self.future.set_result(frame)


class UpstreamLink:
    """Connection from the proxy to one service, acting as the charger towards it."""

    def __init__(self, service: ServiceConfig, relay: "ChargerRelay") -> None:
        self.service = service
        self.relay = relay
        self._ws: aiohttp.ClientWebSocketResponse | None = None
        self._connected = asyncio.Event()
        self._first_attempt_done = asyncio.Event()
        self._pending: dict[str, asyncio.Future[Frame | None]] = {}
        self._queue: deque[_Item] = deque()
        self._queue_signal = asyncio.Event()
        self._tasks: list[asyncio.Task[None]] = []

    @property
    def service_id(self) -> str:
        return self.service.service_id

    @property
    def connected(self) -> bool:
        return self._ws is not None and not self._ws.closed

    @property
    def url(self) -> str:
        return self.service.url_for(self.relay.cp_id)

    def start(self) -> None:
        self._tasks = [asyncio.create_task(self._run()), asyncio.create_task(self._sender())]

    async def close(self) -> None:
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks = []
        for item in self._queue:
            item.resolve(None)

    async def wait_ready(self, timeout: float) -> None:
        """Wait until connected or until the first connection attempt has finished."""
        if self.connected or self._first_attempt_done.is_set():
            return
        try:
            await asyncio.wait_for(self._first_attempt_done.wait(), timeout)
        except TimeoutError:
            _LOGGER.warning("%s is not reachable yet", self.service_id)

    def enqueue(
        self,
        action: str,
        payload: dict[str, Any],
        charger_tx: int | None = None,
        want_response: bool = False,
    ) -> _Item | None:
        """Queue a charger message for this service; returns None when it is dropped."""
        if not self.connected and action not in TRANSACTIONAL_ACTIONS:
            return None
        future = asyncio.get_running_loop().create_future() if want_response else None
        item = _Item(action, payload, charger_tx, future)
        if len(self._queue) >= QUEUE_LIMIT:
            self._queue.popleft().resolve(None)
            _LOGGER.warning("Queue for %s is full; dropped the oldest message", self.service_id)
        self._queue.append(item)
        self._queue_signal.set()
        return item

    def push_front(self, items: list[_Item]) -> None:
        for item in reversed(items):
            self._queue.appendleft(item)
        self._queue_signal.set()

    async def await_response(self, item: _Item) -> Frame | None:
        """Wait for the service's answer to a queued item."""
        if item.future is None:
            return None
        try:
            frame = await asyncio.wait_for(asyncio.shield(item.future), RESPONSE_TIMEOUT + 1)
        except TimeoutError:
            frame = None
        return frame

    async def send_frame(self, frame: Frame) -> None:
        ws = self._ws
        if ws is None or ws.closed:
            return
        try:
            await ws.send_str(json.dumps(frame))
        except ConnectionError:
            _LOGGER.warning("Could not send to %s: connection lost", self.service_id)

    def status(self) -> dict[str, Any]:
        return {
            "connected": self.connected,
            "primary": self.service.primary,
            "read_only": self.service.read_only,
            "url": self.url,
            "queued": len(self._queue),
        }

    async def _run(self) -> None:
        delay = RECONNECT_MIN_DELAY
        while True:
            if await self._connect_once():
                delay = RECONNECT_MIN_DELAY
            await asyncio.sleep(delay)
            delay = min(delay * 2, RECONNECT_MAX_DELAY)

    async def _connect_once(self) -> bool:
        """Connect and relay until the connection drops; returns whether it connected."""
        protocol = SUBPROTOCOLS[self.relay.version]
        connected = False
        try:
            session = await self.relay.services.get_session()
            async with session.ws_connect(
                self.url,
                protocols=(protocol,),
                headers=self.service.headers_for(self.relay.cp_id),
                heartbeat=30,
            ) as ws:
                self._ws = ws
                connected = True
                if ws.protocol != protocol:
                    _LOGGER.warning(
                        "%s did not confirm subprotocol %s (got %s)",
                        self.service_id,
                        protocol,
                        ws.protocol,
                    )
                _LOGGER.info("Connected to %s at %s", self.service_id, self.url)
                self.relay.on_link_connected(self)
                self._connected.set()
                self._first_attempt_done.set()
                async for msg in ws:
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        self._on_message(msg.data)
                    elif msg.type == aiohttp.WSMsgType.ERROR:
                        break
                _LOGGER.warning(
                    "%s closed the connection (code %s)", self.service_id, ws.close_code
                )
        except aiohttp.WSServerHandshakeError as err:
            _LOGGER.warning(
                "%s rejected the connection to %s: HTTP %s", self.service_id, self.url, err.status
            )
        except (aiohttp.ClientError, OSError, TimeoutError) as err:
            _LOGGER.warning("Could not connect to %s at %s: %s", self.service_id, self.url, err)
        finally:
            self._ws = None
            self._connected.clear()
            self._first_attempt_done.set()
            for future in self._pending.values():
                if not future.done():
                    future.set_result(None)
        return connected

    def _on_message(self, data: str) -> None:
        frame = parse_frame(data)
        if frame is None:
            _LOGGER.warning("Ignoring malformed message from %s: %s", self.service_id, data)
            return
        if frame[0] == CALL:
            self.relay.spawn(self.relay.handle_service_call(self, frame[1], frame[2], frame[3]))
            return
        future = self._pending.get(frame[1])
        if future is not None and not future.done():
            future.set_result(frame)

    async def _sender(self) -> None:
        """Deliver queued charger messages one at a time, in order."""
        while True:
            await self._connected.wait()
            if not self._queue:
                self._queue_signal.clear()
                await self._queue_signal.wait()
                continue
            await self._deliver(self._queue.popleft())

    async def _deliver(self, item: _Item) -> None:
        payload = self._outgoing_payload(item)
        if payload is None:
            item.resolve(None)
            return
        try:
            response = await self._call([CALL, str(uuid.uuid4()), item.action, payload])
        except TimeoutError:
            _LOGGER.warning("%s did not answer %s in time", self.service_id, item.action)
            item.resolve(None)
            return
        if response is None:
            # Connection lost: keep transaction data for after the reconnect.
            if item.action in TRANSACTIONAL_ACTIONS and not item.replay:
                self._queue.appendleft(item)
            else:
                item.resolve(None)
            await asyncio.sleep(1)
            return
        self._after_response(item, response)
        item.resolve(response)

    async def _call(self, frame: Frame) -> Frame | None:
        """Send a request and wait for the answer; None means the connection was lost."""
        ws = self._ws
        if ws is None or ws.closed:
            return None
        future: asyncio.Future[Frame | None] = asyncio.get_running_loop().create_future()
        self._pending[frame[1]] = future
        try:
            await ws.send_str(json.dumps(frame))
            result = await asyncio.wait_for(future, RESPONSE_TIMEOUT)
        except ConnectionError:
            result = None
        finally:
            self._pending.pop(frame[1], None)
        return result

    def _outgoing_payload(self, item: _Item) -> dict[str, Any] | None:
        """Translate the charger's transaction ID to the one this service issued."""
        payload = item.payload
        tx = payload.get("transactionId")
        if (
            self.relay.version != "1.6"
            or item.action not in ("StopTransaction", "MeterValues")
            or tx is None
        ):
            return payload
        mapped = self.relay.tx_store.get(self.service_id, tx)
        if mapped is not None:
            return {**payload, "transactionId": mapped}
        if item.action == "MeterValues":
            return {key: value for key, value in payload.items() if key != "transactionId"}
        _LOGGER.warning(
            "Not sending StopTransaction %s to %s: it never received the start",
            tx,
            self.service_id,
        )
        return None

    def _after_response(self, item: _Item, response: Frame) -> None:
        if response[0] != CALLRESULT or self.relay.version != "1.6":
            return
        body = response[2] if isinstance(response[2], dict) else {}
        if item.action == "StartTransaction" and item.charger_tx is not None:
            service_tx = body.get("transactionId")
            if isinstance(service_tx, int):
                self.relay.tx_store.set(self.service_id, item.charger_tx, service_tx)
        elif item.action == "StopTransaction":
            tx = item.payload.get("transactionId")
            if isinstance(tx, int):
                self.relay.tx_store.remove(self.service_id, tx)


class ChargerRelay:
    """Relays one charger connection to all configured services."""

    def __init__(
        self,
        cp_id: str,
        ws: web.WebSocketResponse,
        version: str,
        services: OCPPServiceManager,
        manager: Any = None,
        ha_bridge: Any = None,
        event_logger: Any = None,
    ) -> None:
        self.cp_id = cp_id
        self.ws = ws
        self.version = version
        self.services = services
        self.manager = manager
        self.ha_bridge = ha_bridge
        self.event_logger = event_logger
        self.links = [UpstreamLink(service, self) for service in services.services]
        self._pending: dict[str, asyncio.Future[Frame | None]] = {}
        self._incoming: asyncio.Queue[tuple[str, str, dict[str, Any]]] = asyncio.Queue()
        self._tasks: set[asyncio.Task[Any]] = set()
        self._boot: dict[str, Any] | None = None
        self._forwarding: str | None = None
        self._statuses: dict[tuple[Any, Any], dict[str, Any]] = {}
        self._sessions: dict[int, dict[str, Any]] = {}

    @property
    def ocpp_version(self) -> str:
        return self.version

    @property
    def tx_store(self) -> TransactionStore:
        return self.services.tx_store

    @property
    def primary(self) -> UpstreamLink | None:
        return next((link for link in self.links if link.service.primary), None)

    def spawn(self, coro: Coroutine[Any, Any, Any]) -> None:
        task = asyncio.create_task(coro)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    async def run(self) -> None:
        """Relay messages until the charger disconnects."""
        self.services.attach(self)
        for link in self.links:
            link.start()
        worker = asyncio.create_task(self._process_charger_calls())
        try:
            async for msg in self.ws:
                if msg.type == aiohttp.WSMsgType.TEXT:
                    self._on_charger_message(msg.data)
                elif msg.type == aiohttp.WSMsgType.ERROR:
                    break
        finally:
            worker.cancel()
            await asyncio.gather(worker, return_exceptions=True)
            for future in self._pending.values():
                if not future.done():
                    future.set_result(None)
            await self.close_links()
            self.services.detach(self)

    async def close_links(self) -> None:
        await asyncio.gather(*(link.close() for link in self.links))

    def link_status(self) -> dict[str, dict[str, Any]]:
        return {link.service_id: link.status() for link in self.links}

    # ----- charger -> services -------------------------------------------------------------

    def _on_charger_message(self, data: str) -> None:
        frame = parse_frame(data)
        if frame is None:
            _LOGGER.warning("Ignoring malformed message from charger: %s", data)
            return
        if frame[0] == CALL:
            self._incoming.put_nowait((frame[1], frame[2], frame[3]))
            return
        future = self._pending.get(frame[1])
        if future is not None and not future.done():
            future.set_result(frame)

    async def _process_charger_calls(self) -> None:
        while True:
            uid, action, payload = await self._incoming.get()
            try:
                await self._handle_charger_call(uid, action, payload)
            except Exception:
                _LOGGER.exception("Error relaying %s from charger", action)

    async def _handle_charger_call(self, uid: str, action: str, payload: dict[str, Any]) -> None:
        charger_tx = None
        if self.version == "1.6" and action == "StartTransaction":
            charger_tx = _TransactionIds.next()
        self._remember(action, payload)

        frame = await self._forward(action, payload, charger_tx)
        if frame is not None and frame[0] == CALLERROR:
            await self._send_charger([CALLERROR, uid, *frame[2:]])
            return
        if frame is None:
            response = local_response(self.version, action, charger_tx)
        else:
            response = frame[2] if isinstance(frame[2], dict) else {}
            if charger_tx is not None:
                response = {**response, "transactionId": charger_tx}
        await self._send_charger([CALLRESULT, uid, response])
        await self._emit_event(action, payload, charger_tx)

    async def _forward(
        self, action: str, payload: dict[str, Any], charger_tx: int | None
    ) -> Frame | None:
        """Send to every service; return the primary's answer, or None to answer locally."""
        primary = self.primary
        primary_item = None
        # Links that connect while we wait get this message from the loop below, so they
        # must not also get it replayed (see on_link_connected).
        self._forwarding = action
        try:
            if primary is not None:
                await primary.wait_ready(CONNECT_WAIT)
            for link in self.links:
                item = link.enqueue(action, payload, charger_tx, want_response=link is primary)
                if link is primary:
                    primary_item = item
        finally:
            self._forwarding = None
        if primary is None or primary_item is None or not primary.connected:
            if primary is not None:
                _LOGGER.info("%s is offline; answering %s locally", primary.service_id, action)
            return None
        return await primary.await_response(primary_item)

    def _remember(self, action: str, payload: dict[str, Any]) -> None:
        """Keep the latest boot and status so they can be replayed after a reconnect."""
        if action == "BootNotification":
            self._boot = payload
            self._statuses.clear()
        elif action == "StatusNotification":
            self._statuses[(payload.get("evseId"), payload.get("connectorId"))] = payload

    def on_link_connected(self, link: UpstreamLink) -> None:
        """Bring a (re)connected service up to date before anything else is sent."""
        if self._forwarding == "BootNotification":
            return
        replay = []
        if self._boot is not None:
            replay.append(_Item("BootNotification", self._boot, replay=True))
        replay.extend(
            _Item("StatusNotification", status, replay=True) for status in self._statuses.values()
        )
        if replay:
            link.push_front(replay)

    async def _send_charger(self, frame: Frame) -> None:
        try:
            await self.ws.send_str(json.dumps(frame))
        except ConnectionError:
            _LOGGER.warning("Could not send to charger: connection lost")

    # ----- services -> charger -------------------------------------------------------------

    async def handle_service_call(
        self, link: UpstreamLink, uid: str, action: str, payload: dict[str, Any]
    ) -> None:
        """Forward a request from a service to the charger and return the answer."""
        if link.service.read_only and action not in READ_ONLY_ACTIONS:
            _LOGGER.info("Refused %s from read-only service %s", action, link.service_id)
            await link.send_frame(
                [CALLERROR, uid, "SecurityError", "Read-only access via OCPP proxy", {}]
            )
            return
        if action in START_ACTIONS and self.manager is not None:
            allowed = await self.manager.request_control(f"ocpp_service_{link.service_id}")
            if not allowed:
                _LOGGER.info("Refused %s from %s: control denied", action, link.service_id)
                await link.send_frame([CALLRESULT, uid, {"status": "Rejected"}])
                return
        frame = await self.call_charger(action, self._to_charger_payload(link, action, payload))
        if frame is None:
            await link.send_frame([CALLERROR, uid, "GenericError", "Charger did not respond", {}])
        else:
            await link.send_frame([frame[0], uid, *frame[2:]])

    def _to_charger_payload(
        self, link: UpstreamLink, action: str, payload: dict[str, Any]
    ) -> dict[str, Any]:
        if self.version != "1.6":
            return payload
        if action == "RemoteStopTransaction":
            return self._charger_tx_field(link, payload)
        profile = payload.get("csChargingProfiles")
        if action == "SetChargingProfile" and isinstance(profile, dict):
            return {**payload, "csChargingProfiles": self._charger_tx_field(link, profile)}
        return payload

    def _charger_tx_field(self, link: UpstreamLink, data: dict[str, Any]) -> dict[str, Any]:
        tx = data.get("transactionId")
        if not isinstance(tx, int):
            return data
        charger_tx = self.tx_store.reverse(link.service_id, tx)
        return data if charger_tx is None else {**data, "transactionId": charger_tx}

    async def call_charger(self, action: str, payload: dict[str, Any]) -> Frame | None:
        """Send a request to the charger and wait for its answer."""
        if self.ws.closed:
            return None
        uid = str(uuid.uuid4())
        future: asyncio.Future[Frame | None] = asyncio.get_running_loop().create_future()
        self._pending[uid] = future
        try:
            await self.ws.send_str(json.dumps([CALL, uid, action, payload]))
            result = await asyncio.wait_for(future, RESPONSE_TIMEOUT)
        except (TimeoutError, ConnectionError):
            result = None
        finally:
            self._pending.pop(uid, None)
        return result

    # ----- local control API (used by /backend clients) --------------------------------------

    async def send_remote_start_transaction(self, connector_id: int, id_tag: str) -> bool:
        if self.version == "2.0.1":
            frame = await self.call_charger(
                "RequestStartTransaction",
                {
                    "evseId": connector_id,
                    "idToken": {"idToken": id_tag, "type": "Central"},
                    "remoteStartId": random.randint(1, 2_000_000_000),
                },
            )
        else:
            frame = await self.call_charger(
                "RemoteStartTransaction", {"connectorId": connector_id, "idTag": id_tag}
            )
        return _accepted(frame)

    async def send_remote_stop_transaction(self, transaction_id: Any) -> bool:
        if self.version == "2.0.1":
            frame = await self.call_charger(
                "RequestStopTransaction", {"transactionId": str(transaction_id)}
            )
        else:
            frame = await self.call_charger(
                "RemoteStopTransaction", {"transactionId": transaction_id}
            )
        return _accepted(frame)

    # ----- events, sessions and notifications ------------------------------------------------

    async def _emit_event(
        self, action: str, payload: dict[str, Any], charger_tx: int | None
    ) -> None:
        handlers = {
            "BootNotification": self._event_boot,
            "Heartbeat": self._event_heartbeat,
            "StatusNotification": self._event_status,
            "MeterValues": self._event_meter,
            "StartTransaction": self._event_start,
            "StopTransaction": self._event_stop,
            "TransactionEvent": self._event_transaction,
        }
        handler = handlers.get(action)
        if handler is not None:
            await handler(payload, charger_tx)

    async def _broadcast(self, event: dict[str, Any]) -> None:
        if self.manager is not None:
            await self.manager.broadcast_event(event)

    async def _notify(self, title: str, message: str) -> None:
        if self.ha_bridge is not None:
            await self.ha_bridge.send_notification(title, message)

    async def _event_boot(self, payload: dict[str, Any], _tx: int | None) -> None:
        station = payload.get("chargingStation") or {}
        await self._broadcast(
            {
                "type": "boot",
                "vendor": payload.get("chargePointVendor", station.get("vendorName")),
                "model": payload.get("chargePointModel", station.get("model")),
            }
        )

    async def _event_heartbeat(self, _payload: dict[str, Any], _tx: int | None) -> None:
        await self._broadcast({"type": "heartbeat", "current_time": _now()})

    async def _event_status(self, payload: dict[str, Any], _tx: int | None) -> None:
        status = str(payload.get("status", payload.get("connectorStatus", "")))
        error_code = str(payload.get("errorCode", "NoError"))
        await self._broadcast(
            {
                "type": "status",
                "connector_id": payload.get("connectorId"),
                "error_code": error_code,
                "status": status,
            }
        )
        if status.lower() in ("faulted", "unavailable"):
            if self.manager is not None:
                self.manager.release_control()
            await self._notify("Charger Fault", f"Status={status}, Error={error_code}")

    async def _event_meter(self, payload: dict[str, Any], _tx: int | None) -> None:
        await self._broadcast(
            {
                "type": "meter",
                "connector_id": payload.get("connectorId", payload.get("evseId")),
                "values": payload.get("meterValue", []),
            }
        )

    async def _event_start(self, payload: dict[str, Any], charger_tx: int | None) -> None:
        if charger_tx is None:
            return
        self._sessions[charger_tx] = {
            "start_time": payload.get("timestamp"),
            "start_meter": payload.get("meterStart", 0),
        }
        await self._broadcast(
            {
                "type": "transaction_started",
                "transaction_id": charger_tx,
                "connector_id": payload.get("connectorId"),
                "id_tag": payload.get("idTag"),
                "meter_start": payload.get("meterStart"),
                "timestamp": payload.get("timestamp"),
            }
        )

    async def _event_stop(self, payload: dict[str, Any], _tx: int | None) -> None:
        tx = payload.get("transactionId")
        meter_stop = payload.get("meterStop", 0)
        await self._broadcast(
            {
                "type": "transaction_stopped",
                "transaction_id": tx,
                "meter_stop": meter_stop,
                "timestamp": payload.get("timestamp"),
            }
        )
        info = self._sessions.pop(tx, None) if isinstance(tx, int) else None
        if info is None:
            return
        duration = _duration(info.get("start_time"), payload.get("timestamp"))
        energy = (meter_stop - info.get("start_meter", 0)) / 1000.0
        owner = getattr(self.manager, "_lock_owner", None) or ""
        if self.event_logger is not None:
            self.event_logger.log_session(owner, duration, energy, 0.0)
        await self._notify(
            "Charging session ended",
            f"Provider={owner}, kWh={energy:.2f}, duration={duration:.0f}s",
        )

    async def _event_transaction(self, payload: dict[str, Any], _tx: int | None) -> None:
        await self._broadcast(
            {
                "type": "transaction_event",
                "event_type": payload.get("eventType"),
                "transaction_id": (payload.get("transactionInfo") or {}).get("transactionId"),
                "timestamp": payload.get("timestamp"),
            }
        )


def _accepted(frame: Frame | None) -> bool:
    return (
        frame is not None
        and frame[0] == CALLRESULT
        and isinstance(frame[2], dict)
        and frame[2].get("status") == "Accepted"
    )


def _duration(start: Any, end: Any) -> float:
    try:
        t0 = datetime.datetime.fromisoformat(str(start))
        t1 = datetime.datetime.fromisoformat(str(end))
        seconds = (t1 - t0).total_seconds()
    except (ValueError, TypeError):
        return 0.0
    return seconds
