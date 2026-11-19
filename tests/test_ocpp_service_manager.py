import base64
import json
from unittest.mock import AsyncMock, Mock

import pytest

from src.ocpp_proxy.ocpp_service_manager import (
    OCPPServiceManager,
    ServiceConfig,
    TransactionStore,
    parse_services,
)


@pytest.mark.unit
class TestParseServices:
    def test_first_enabled_service_is_primary_by_default(self):
        services = parse_services(
            [
                {"id": "off", "url": "wss://off", "enabled": False},
                {"id": "monta", "url": "wss://monta"},
                {"id": "zeres", "url": "wss://zeres"},
            ]
        )
        assert [s.service_id for s in services] == ["monta", "zeres"]
        assert services[0].primary
        assert not services[0].read_only
        assert not services[1].primary
        assert services[1].read_only

    def test_explicit_primary_and_read_only_override(self):
        services = parse_services(
            [
                {"id": "zeres", "url": "wss://zeres", "read_only": False},
                {"id": "monta", "url": "wss://monta", "primary": True},
            ]
        )
        by_id = {s.service_id: s for s in services}
        assert by_id["monta"].primary
        assert not by_id["zeres"].primary
        assert not by_id["zeres"].read_only

    def test_multiple_primaries_uses_first(self):
        services = parse_services(
            [
                {"id": "a", "url": "wss://a", "primary": True},
                {"id": "b", "url": "wss://b", "primary": True},
            ]
        )
        assert [s.primary for s in services] == [True, False]

    def test_services_without_id_or_url_are_ignored(self):
        assert parse_services([{"id": "x"}, {"url": "wss://y"}]) == []

    def test_auth_fields(self):
        (service,) = parse_services(
            [{"id": "a", "url": "wss://a", "auth_type": "basic", "password": "pw", "username": ""}]
        )
        assert service.auth_type == "basic"
        assert service.password == "pw"
        assert service.username is None


@pytest.mark.unit
class TestServiceConfig:
    def test_charger_id_is_appended(self):
        service = ServiceConfig("monta", "wss://ocpp.example.com/")
        assert service.url_for("7000730523000060") == "wss://ocpp.example.com/7000730523000060"

    def test_charger_id_placeholder(self):
        service = ServiceConfig("monta", "wss://ocpp.example.com/{charge_point_id}/ws")
        assert service.url_for("CP 1") == "wss://ocpp.example.com/CP%201/ws"

    def test_basic_auth_defaults_username_to_charger_id(self):
        headers = ServiceConfig("a", "wss://a", auth_type="basic", password="pw").headers_for("CP1")
        assert headers["Authorization"] == "Basic " + base64.b64encode(b"CP1:pw").decode()

    def test_basic_auth_with_username(self):
        service = ServiceConfig("a", "wss://a", auth_type="basic", username="u", password="pw")
        expected = "Basic " + base64.b64encode(b"u:pw").decode()
        assert service.headers_for("CP1")["Authorization"] == expected

    def test_token_auth(self):
        service = ServiceConfig("a", "wss://a", auth_type="token", token="abc")
        assert service.headers_for("CP1") == {"Authorization": "Bearer abc"}

    def test_no_auth(self):
        assert ServiceConfig("a", "wss://a").headers_for("CP1") == {}
        assert ServiceConfig("a", "wss://a", auth_type="basic").headers_for("CP1") == {}


@pytest.mark.unit
class TestTransactionStore:
    def test_in_memory_mapping(self):
        store = TransactionStore()
        store.set("monta", 1000, 501)
        assert store.get("monta", 1000) == 501
        assert store.get("zeres", 1000) is None
        assert store.reverse("monta", 501) == 1000
        assert store.reverse("monta", 999) is None
        store.remove("monta", 1000)
        assert store.get("monta", 1000) is None
        store.remove("monta", 1000)  # removing twice is fine

    def test_persists_across_instances(self, tmp_path):
        path = str(tmp_path / "tx.json")
        TransactionStore(path).set("monta", 1000, 501)
        assert TransactionStore(path).get("monta", 1000) == 501
        with open(path) as f:
            assert json.load(f) == {"monta": {"1000": 501}}

    def test_corrupt_file_is_ignored(self, tmp_path):
        path = tmp_path / "tx.json"
        path.write_text("{not json")
        store = TransactionStore(str(path))
        assert store.get("monta", 1) is None

    def test_unwritable_path_does_not_raise(self, tmp_path):
        store = TransactionStore(str(tmp_path / "missing-dir" / "tx.json"))
        store.set("monta", 1, 2)
        assert store.get("monta", 1) == 2


@pytest.mark.unit
class TestOCPPServiceManager:
    def make_config(self, services):
        config = Mock()
        config.ocpp_services = services
        return config

    async def test_status_without_charger(self):
        manager = OCPPServiceManager(
            self.make_config([{"id": "monta", "url": "wss://monta"}, {"id": "z", "url": "wss://z"}])
        )
        await manager.start_services()
        assert manager.primary is not None
        assert manager.primary.service_id == "monta"
        status = manager.get_service_status()
        assert status["monta"] == {
            "connected": False,
            "primary": True,
            "read_only": False,
            "url": "wss://monta",
        }
        assert status["z"]["read_only"] is True

    async def test_no_services(self):
        manager = OCPPServiceManager(self.make_config(None))
        await manager.start_services()
        assert manager.services == []
        assert manager.primary is None
        assert manager.get_service_status() == {}

    async def test_attached_relay_reports_status_and_is_closed(self):
        manager = OCPPServiceManager(self.make_config([]))
        relay = Mock()
        relay.link_status = Mock(return_value={"monta": {"connected": True}})
        relay.close_links = AsyncMock()

        manager.attach(relay)
        assert manager.get_service_status() == {"monta": {"connected": True}}

        session = await manager.get_session()
        assert await manager.get_session() is session

        await manager.stop_all_services()
        relay.close_links.assert_awaited_once()
        assert session.closed

        manager.detach(Mock())  # another relay: ignored
        assert manager.get_service_status() == {"monta": {"connected": True}}
        manager.detach(relay)
        assert manager.get_service_status() == {}

    def test_tx_store_uses_env_path(self, tmp_path, monkeypatch):
        path = tmp_path / "tx.json"
        monkeypatch.setenv("TX_MAP_PATH", str(path))
        manager = OCPPServiceManager(self.make_config([]))
        manager.tx_store.set("monta", 1, 2)
        assert path.exists()
