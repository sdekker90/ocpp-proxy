"""End-to-end tests: a fake charger talks to the proxy, which relays to fake CSMS servers."""

import asyncio
import json
import uuid
from typing import Any

import aiohttp
import pytest
import yaml
from aiohttp import web

from src.ocpp_proxy import relay as relay_module
from src.ocpp_proxy.main import init_app
from src.ocpp_proxy.relay import (
    CALL,
    CALLERROR,
    CALLRESULT,
    _accepted,
    _duration,
    local_response,
    parse_frame,
    version_from_subprotocol,
)

BOOT_16 = {"chargePointVendor": "Webasto", "chargePointModel": "Unite"}


async def wait_until(predicate, timeout: float = 3.0) -> None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while not predicate():
        if loop.time() > deadline:
            raise AssertionError("condition not met in time")
        await asyncio.sleep(0.01)


class FakeCsms:
    """A minimal CSMS (like Monta or Zeres) that records what the proxy sends."""

    def __init__(self, tx_base: int, boot_interval: int) -> None:
        self.calls: list[list[Any]] = []
        self.replies: list[list[Any]] = []
        self.paths: list[str] = []
        self.headers: list[dict[str, str]] = []
        self.protocols: list[str | None] = []
        self.connections = 0
        self.close_after_boot = False
        self.error_actions: set[str] = set()
        self.ws: web.WebSocketResponse | None = None
        self._tx = tx_base
        self._boot_interval = boot_interval
        self.app = web.Application()
        self.app.router.add_get("/ocpp/{cp_id}", self._handler)
        self.server: Any = None

    async def start(self, aiohttp_server) -> "FakeCsms":
        self.server = await aiohttp_server(self.app)
        return self

    def url(self, path: str = "/ocpp") -> str:
        return str(self.server.make_url(path).with_scheme("ws"))

    @property
    def connected(self) -> bool:
        return self.ws is not None and not self.ws.closed

    async def _handler(self, request: web.Request) -> web.WebSocketResponse:
        ws = web.WebSocketResponse(protocols=("ocpp1.6", "ocpp2.0.1"))
        await ws.prepare(request)
        self.ws = ws
        self.connections += 1
        self.paths.append(request.path)
        self.headers.append(dict(request.headers))
        self.protocols.append(ws.ws_protocol)
        async for msg in ws:
            frame = json.loads(msg.data)
            if frame[0] != CALL:
                self.replies.append(frame)
                continue
            self.calls.append(frame)
            if frame[2] in self.error_actions:
                await ws.send_str(json.dumps([CALLERROR, frame[1], "InternalError", "nope", {}]))
            else:
                await ws.send_str(json.dumps([CALLRESULT, frame[1], self._answer(frame[2])]))
            if self.close_after_boot and frame[2] == "BootNotification":
                self.close_after_boot = False
                await ws.close()
        return ws

    def _answer(self, action: str) -> dict[str, Any]:
        if action == "BootNotification":
            return {
                "status": "Accepted",
                "currentTime": "2026-11-19T10:00:00Z",
                "interval": self._boot_interval,
            }
        if action == "StartTransaction":
            self._tx += 1
            return {"transactionId": self._tx, "idTagInfo": {"status": "Accepted"}}
        if action == "Authorize":
            return {"idTagInfo": {"status": "Blocked"}}
        return {}

    def actions(self) -> list[str]:
        return [frame[2] for frame in self.calls]

    def payloads(self, action: str) -> list[dict[str, Any]]:
        return [frame[3] for frame in self.calls if frame[2] == action]

    async def call(self, action: str, payload: dict[str, Any]) -> str:
        uid = str(uuid.uuid4())
        assert self.ws is not None
        await self.ws.send_str(json.dumps([CALL, uid, action, payload]))
        return uid

    async def reply_to(self, uid: str) -> list[Any]:
        await wait_until(lambda: any(frame[1] == uid for frame in self.replies))
        return next(frame for frame in self.replies if frame[1] == uid)


class FakeCharger:
    """Speaks OCPP-J to the proxy like a real charger."""

    def __init__(self, ws: aiohttp.ClientWebSocketResponse) -> None:
        self.ws = ws
        self.inbox: list[list[Any]] = []

    async def _receive(self, timeout: float = 5.0) -> list[Any]:
        msg = await self.ws.receive(timeout=timeout)
        assert msg.type == aiohttp.WSMsgType.TEXT, msg
        return json.loads(msg.data)

    async def call(self, action: str, payload: dict[str, Any]) -> list[Any]:
        uid = str(uuid.uuid4())
        await self.ws.send_str(json.dumps([CALL, uid, action, payload]))
        while True:
            frame = await self._receive()
            if frame[0] == CALL:
                self.inbox.append(frame)
            elif frame[1] == uid:
                return frame

    async def next_call(self) -> list[Any]:
        if self.inbox:
            return self.inbox.pop(0)
        while True:
            frame = await self._receive()
            if frame[0] == CALL:
                return frame

    async def reply(self, uid: str, payload: dict[str, Any]) -> None:
        await self.ws.send_str(json.dumps([CALLRESULT, uid, payload]))


@pytest.fixture(autouse=True)
def fast_timings(monkeypatch):
    monkeypatch.setattr(relay_module, "RECONNECT_MIN_DELAY", 0.05)
    monkeypatch.setattr(relay_module, "RECONNECT_MAX_DELAY", 0.1)
    monkeypatch.setattr(relay_module, "CONNECT_WAIT", 3.0)
    monkeypatch.setattr(relay_module, "RESPONSE_TIMEOUT", 3.0)


@pytest.fixture
async def monta(aiohttp_server):
    return await FakeCsms(tx_base=500, boot_interval=123).start(aiohttp_server)


@pytest.fixture
async def zeres(aiohttp_server):
    return await FakeCsms(tx_base=900, boot_interval=456).start(aiohttp_server)


@pytest.fixture
def make_proxy(aiohttp_client, tmp_path, monkeypatch):
    async def factory(services: list[dict[str, Any]], **options: Any):
        config = {
            "allow_shared_charging": True,
            "rate_limit_seconds": 1,
            "ocpp_services": services,
            **options,
        }
        options_file = tmp_path / "options.yaml"
        options_file.write_text(yaml.safe_dump(config))
        monkeypatch.setenv("ADDON_CONFIG_FILE", str(options_file))
        monkeypatch.setenv("LOG_DB_PATH", str(tmp_path / "usage.db"))
        monkeypatch.setenv("TX_MAP_PATH", str(tmp_path / "tx.json"))
        app = await init_app()
        client = await aiohttp_client(app)
        return app, client

    return factory


def monta_and_zeres(monta: FakeCsms, zeres: FakeCsms) -> list[dict[str, Any]]:
    return [
        {"id": "monta", "url": monta.url(), "primary": True},
        {"id": "zeres", "url": zeres.url(), "read_only": True},
    ]


async def connect_charger(client, cp_id: str = "CP123", protocol: str = "ocpp1.6") -> FakeCharger:
    ws = await client.ws_connect(f"/charger/{cp_id}", protocols=(protocol,))
    return FakeCharger(ws)


async def booted_charger(client, *services: FakeCsms) -> FakeCharger:
    charger = await connect_charger(client)
    await wait_until(lambda: all(s.connected for s in services))
    await charger.call("BootNotification", BOOT_16)
    return charger


@pytest.mark.e2e
async def test_boot_is_relayed_with_charger_id_and_primary_answer(make_proxy, monta, zeres):
    app, client = await make_proxy(monta_and_zeres(monta, zeres))
    charger = await connect_charger(client)
    try:
        await wait_until(lambda: monta.connected and zeres.connected)
        reply = await charger.call("BootNotification", BOOT_16)

        assert reply[0] == CALLRESULT
        assert reply[2]["interval"] == 123  # Monta's answer, not Zeres' (456)
        await wait_until(lambda: "BootNotification" in zeres.actions())
        assert monta.paths == ["/ocpp/CP123"]
        assert zeres.paths == ["/ocpp/CP123"]
        assert monta.protocols == ["ocpp1.6"]
        assert monta.payloads("BootNotification") == [BOOT_16]

        status = app["ocpp_service_manager"].get_service_status()
        assert status["monta"]["connected"] is True
        assert status["monta"]["primary"] is True
        assert status["zeres"]["read_only"] is True
        assert status["zeres"]["url"].endswith("/ocpp/CP123")
    finally:
        await charger.ws.close()

    await wait_until(lambda: "charge_point" not in app)
    await wait_until(lambda: not monta.connected and not zeres.connected)


@pytest.mark.e2e
async def test_boot_sent_once_when_charger_boots_before_services_connect(
    make_proxy, monta, zeres
):
    _, client = await make_proxy(monta_and_zeres(monta, zeres))
    charger = await connect_charger(client)
    try:
        reply = await charger.call("BootNotification", BOOT_16)  # no waiting for services
        assert reply[2]["interval"] == 123  # the proxy waited for Monta's answer
        await wait_until(lambda: "BootNotification" in zeres.actions())
        await asyncio.sleep(0.2)
        assert monta.actions().count("BootNotification") == 1
    finally:
        await charger.ws.close()


@pytest.mark.e2e
async def test_primary_decides_authorization(make_proxy, monta, zeres):
    _, client = await make_proxy(monta_and_zeres(monta, zeres))
    charger = await booted_charger(client, monta, zeres)
    try:
        reply = await charger.call("Authorize", {"idTag": "TAG1"})
        assert reply[2] == {"idTagInfo": {"status": "Blocked"}}
    finally:
        await charger.ws.close()


@pytest.mark.e2e
async def test_transaction_ids_are_translated_per_service(make_proxy, monta, zeres):
    app, client = await make_proxy(monta_and_zeres(monta, zeres))
    charger = await booted_charger(client, monta, zeres)
    try:
        start = await charger.call(
            "StartTransaction",
            {
                "connectorId": 1,
                "idTag": "TAG1",
                "meterStart": 1000,
                "timestamp": "2026-11-19T10:00:00+00:00",
            },
        )
        charger_tx = start[2]["transactionId"]
        assert start[2]["idTagInfo"] == {"status": "Accepted"}
        assert charger_tx not in (501, 901)

        await charger.call(
            "MeterValues",
            {
                "connectorId": 1,
                "transactionId": charger_tx,
                "meterValue": [{"timestamp": "2026-11-19T10:30:00+00:00", "sampledValue": []}],
            },
        )
        stop = await charger.call(
            "StopTransaction",
            {
                "transactionId": charger_tx,
                "meterStop": 6000,
                "timestamp": "2026-11-19T11:00:00+00:00",
            },
        )
        assert stop[0] == CALLRESULT

        await wait_until(lambda: "StopTransaction" in zeres.actions())
        assert monta.payloads("MeterValues")[0]["transactionId"] == 501
        assert zeres.payloads("MeterValues")[0]["transactionId"] == 901
        assert monta.payloads("StopTransaction")[0]["transactionId"] == 501
        assert zeres.payloads("StopTransaction")[0]["transactionId"] == 901

        tx_store = app["ocpp_service_manager"].tx_store
        assert tx_store.get("monta", charger_tx) is None  # cleaned up after the stop
        sessions = app["event_logger"].get_sessions()
        assert len(sessions) == 1
        assert sessions[0]["energy_kwh"] == 5.0
        assert sessions[0]["duration_s"] == 3600.0
    finally:
        await charger.ws.close()


@pytest.mark.e2e
async def test_primary_can_remote_start_and_stop(make_proxy, monta, zeres):
    _, client = await make_proxy(monta_and_zeres(monta, zeres))
    charger = await booted_charger(client, monta, zeres)
    try:
        start = await charger.call(
            "StartTransaction",
            {"connectorId": 1, "idTag": "T", "meterStart": 0, "timestamp": "2026-11-19T10:00:00Z"},
        )
        charger_tx = start[2]["transactionId"]

        uid = await monta.call("RemoteStartTransaction", {"connectorId": 1, "idTag": "MONTA"})
        incoming = await charger.next_call()
        assert incoming[2] == "RemoteStartTransaction"
        assert incoming[3] == {"connectorId": 1, "idTag": "MONTA"}
        await charger.reply(incoming[1], {"status": "Accepted"})
        reply = await monta.reply_to(uid)
        assert reply == [CALLRESULT, uid, {"status": "Accepted"}]

        uid = await monta.call("RemoteStopTransaction", {"transactionId": 501})
        incoming = await charger.next_call()
        assert incoming[3] == {"transactionId": charger_tx}
        await charger.reply(incoming[1], {"status": "Accepted"})
        assert (await monta.reply_to(uid))[2] == {"status": "Accepted"}
    finally:
        await charger.ws.close()


@pytest.mark.e2e
async def test_read_only_service_cannot_control_the_charger(make_proxy, monta, zeres):
    _, client = await make_proxy(monta_and_zeres(monta, zeres))
    charger = await booted_charger(client, monta, zeres)
    try:
        uid = await zeres.call("RemoteStartTransaction", {"connectorId": 1, "idTag": "Z"})
        reply = await zeres.reply_to(uid)
        assert reply[0] == CALLERROR
        assert reply[2] == "SecurityError"

        uid = await zeres.call("GetConfiguration", {})
        incoming = await charger.next_call()
        assert incoming[2] == "GetConfiguration"  # the RemoteStart never reached the charger
        await charger.reply(incoming[1], {"configurationKey": []})
        assert (await zeres.reply_to(uid))[0] == CALLRESULT
    finally:
        await charger.ws.close()


@pytest.mark.e2e
async def test_remote_start_is_rejected_when_control_is_denied(make_proxy, monta):
    _, client = await make_proxy(
        [{"id": "monta", "url": monta.url()}], allow_shared_charging=False
    )
    charger = await booted_charger(client, monta)
    try:
        uid = await monta.call("RemoteStartTransaction", {"connectorId": 1, "idTag": "M"})
        assert (await monta.reply_to(uid))[2] == {"status": "Rejected"}
    finally:
        await charger.ws.close()


@pytest.mark.e2e
async def test_charger_is_answered_locally_when_primary_is_unreachable(make_proxy, monta):
    app, client = await make_proxy([{"id": "monta", "url": monta.url("/wrong-path")}])
    charger = await connect_charger(client)
    try:
        boot = await charger.call("BootNotification", BOOT_16)
        assert boot[2]["status"] == "Accepted"
        assert boot[2]["interval"] == 300

        auth = await charger.call("Authorize", {"idTag": "TAG"})
        assert auth[2] == {"idTagInfo": {"status": "Accepted"}}

        start = await charger.call(
            "StartTransaction",
            {"connectorId": 1, "idTag": "T", "meterStart": 0, "timestamp": "2026-11-19T10:00:00Z"},
        )
        assert isinstance(start[2]["transactionId"], int)

        status = app["ocpp_service_manager"].get_service_status()["monta"]
        assert status["connected"] is False
        assert status["queued"] == 1  # StartTransaction kept for when Monta is reachable
        assert monta.connections == 0
    finally:
        await charger.ws.close()


@pytest.mark.e2e
async def test_primary_errors_are_passed_to_the_charger(make_proxy, monta):
    monta.error_actions.add("DataTransfer")
    _, client = await make_proxy([{"id": "monta", "url": monta.url()}])
    charger = await booted_charger(client, monta)
    try:
        reply = await charger.call("DataTransfer", {"vendorId": "x"})
        assert reply[0] == CALLERROR
        assert reply[2] == "InternalError"
    finally:
        await charger.ws.close()


@pytest.mark.e2e
async def test_unknown_transactions_are_not_sent_upstream(make_proxy, monta):
    _, client = await make_proxy([{"id": "monta", "url": monta.url()}])
    charger = await booted_charger(client, monta)
    try:
        await charger.call("MeterValues", {"connectorId": 1, "transactionId": 42, "meterValue": []})
        stop = await charger.call(
            "StopTransaction",
            {"transactionId": 42, "meterStop": 10, "timestamp": "2026-11-19T10:00:00Z"},
        )
        assert stop[0] == CALLRESULT
        assert stop[2] == {}  # answered by the proxy
        assert "StopTransaction" not in monta.actions()
        assert monta.payloads("MeterValues") == [{"connectorId": 1, "meterValue": []}]
    finally:
        await charger.ws.close()


@pytest.mark.e2e
async def test_boot_and_status_are_replayed_after_service_reconnects(make_proxy, monta, zeres):
    zeres.close_after_boot = True
    _, client = await make_proxy(monta_and_zeres(monta, zeres))
    charger = await booted_charger(client, monta, zeres)
    try:
        await charger.call(
            "StatusNotification", {"connectorId": 1, "errorCode": "NoError", "status": "Available"}
        )
        await wait_until(lambda: zeres.connections == 2 and "StatusNotification" in zeres.actions())
        actions = zeres.actions()
        assert actions.count("BootNotification") >= 2
        last_boot = len(actions) - 1 - actions[::-1].index("BootNotification")
        assert last_boot < actions.index("StatusNotification")
    finally:
        await charger.ws.close()


@pytest.mark.e2e
async def test_basic_auth_uses_charger_id_as_username(make_proxy, monta):
    _, client = await make_proxy(
        [{"id": "monta", "url": monta.url(), "auth_type": "basic", "password": "secret"}]
    )
    charger = await connect_charger(client)
    try:
        await wait_until(lambda: monta.connected)
        assert monta.headers[0]["Authorization"] == "Basic Q1AxMjM6c2VjcmV0"  # CP123:secret
    finally:
        await charger.ws.close()


@pytest.mark.e2e
async def test_backend_clients_get_events_and_can_control_without_services(make_proxy):
    app, client = await make_proxy([])
    backend = await client.ws_connect("/backend?id=b1")
    charger = await connect_charger(client)
    try:
        await charger.call("BootNotification", BOOT_16)
        event = await backend.receive_json(timeout=3)
        assert event == {"type": "boot", "vendor": "Webasto", "model": "Unite"}

        await charger.call(
            "StatusNotification",
            {"connectorId": 1, "errorCode": "GroundFailure", "status": "Faulted"},
        )
        event = await backend.receive_json(timeout=3)
        assert event["type"] == "status"
        assert event["status"] == "Faulted"

        relay = app["charge_point"]
        task = asyncio.create_task(relay.send_remote_start_transaction(1, "TAG"))
        incoming = await charger.next_call()
        assert incoming[2:] == ["RemoteStartTransaction", {"connectorId": 1, "idTag": "TAG"}]
        await charger.reply(incoming[1], {"status": "Accepted"})
        assert await task is True

        task = asyncio.create_task(relay.send_remote_stop_transaction(7))
        incoming = await charger.next_call()
        assert incoming[2:] == ["RemoteStopTransaction", {"transactionId": 7}]
        await charger.reply(incoming[1], {"status": "Rejected"})
        assert await task is False
    finally:
        await charger.ws.close()
        await backend.close()


@pytest.mark.e2e
async def test_ocpp_201_is_relayed_unchanged(make_proxy, monta):
    app, client = await make_proxy([{"id": "monta", "url": monta.url()}])
    charger = await connect_charger(client, protocol="ocpp2.0.1")
    try:
        await wait_until(lambda: monta.connected)
        assert monta.protocols == ["ocpp2.0.1"]
        boot = {"chargingStation": {"model": "Next", "vendorName": "Webasto"}, "reason": "PowerUp"}
        assert (await charger.call("BootNotification", boot))[2]["interval"] == 123

        event = {
            "eventType": "Started",
            "timestamp": "2026-11-19T10:00:00Z",
            "triggerReason": "Authorized",
            "seqNo": 0,
            "transactionInfo": {"transactionId": "abc"},
        }
        await charger.call("TransactionEvent", event)
        assert monta.payloads("TransactionEvent") == [event]

        relay = app["charge_point"]
        task = asyncio.create_task(relay.send_remote_start_transaction(1, "TAG"))
        incoming = await charger.next_call()
        assert incoming[2] == "RequestStartTransaction"
        assert incoming[3]["idToken"] == {"idToken": "TAG", "type": "Central"}
        await charger.reply(incoming[1], {"status": "Accepted"})
        assert await task is True

        task = asyncio.create_task(relay.send_remote_stop_transaction("abc"))
        incoming = await charger.next_call()
        assert incoming[2:] == ["RequestStopTransaction", {"transactionId": "abc"}]
        await charger.reply(incoming[1], {"status": "Accepted"})
        assert await task is True
    finally:
        await charger.ws.close()


@pytest.mark.e2e
async def test_malformed_charger_messages_are_ignored(make_proxy):
    _, client = await make_proxy([])
    charger = await connect_charger(client)
    try:
        await charger.ws.send_str("not json")
        await charger.ws.send_str(json.dumps([9, "x"]))
        reply = await charger.call("Heartbeat", {})
        assert "currentTime" in reply[2]
    finally:
        await charger.ws.close()


@pytest.mark.unit
class TestHelpers:
    def test_version_from_subprotocol(self):
        assert version_from_subprotocol("ocpp2.0.1") == "2.0.1"
        assert version_from_subprotocol("ocpp1.6", "2.0.1") == "1.6"
        assert version_from_subprotocol(None, "2.0.1") == "2.0.1"

    def test_parse_frame(self):
        assert parse_frame('[2, "1", "Heartbeat", {}]') == [2, "1", "Heartbeat", {}]
        assert parse_frame('[3, "1", {}]') == [3, "1", {}]
        assert parse_frame('[4, "1", "GenericError", "", {}]') is not None
        assert parse_frame("nope") is None
        assert parse_frame('{"a": 1}') is None
        assert parse_frame('[2, 1, "Heartbeat", {}]') is None
        assert parse_frame('[2, "1", "Heartbeat"]') is None
        assert parse_frame('[2, "1", "Heartbeat", []]') is None
        assert parse_frame('[7, "1", {}]') is None

    def test_local_responses(self):
        assert local_response("1.6", "BootNotification")["status"] == "Accepted"
        assert "currentTime" in local_response("1.6", "Heartbeat")
        assert local_response("2.0.1", "Authorize") == {"idTokenInfo": {"status": "Accepted"}}
        assert local_response("1.6", "StartTransaction", 5)["transactionId"] == 5
        assert local_response("1.6", "DataTransfer") == {"status": "UnknownVendorId"}
        assert local_response("1.6", "StatusNotification") == {}

    def test_accepted(self):
        assert _accepted([CALLRESULT, "1", {"status": "Accepted"}])
        assert not _accepted([CALLRESULT, "1", {"status": "Rejected"}])
        assert not _accepted([CALLERROR, "1", "GenericError", "", {}])
        assert not _accepted(None)

    def test_duration(self):
        assert _duration("2026-11-19T10:00:00Z", "2026-11-19T10:01:00Z") == 60.0
        assert _duration("garbage", "2026-11-19T10:01:00Z") == 0.0
        assert _duration("2026-11-19T10:00:00", "2026-11-19T10:01:00Z") == 0.0
        assert _duration(None, None) == 0.0
