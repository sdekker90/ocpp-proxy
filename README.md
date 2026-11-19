# OCPP Proxy - Multi-Version EV Charger Sharing

A secure, lightweight OCPP proxy server designed as a Home Assistant add-on that enables intelligent EV charger sharing. Supports both **OCPP 1.6** and **OCPP 2.0.1** protocols with automatic version detection.

The proxy sits between a single EV charger and multiple backend services (energy providers, fleet management, charging networks), allowing multiple third-party services to use spare charger capacity while keeping the homeowner in full control.

## ✨ Key Features

### 🔌 **Multi-Protocol Support**
- **OCPP 1.6** and **OCPP 2.0.1** support with automatic version detection
- WebSocket subprotocol negotiation and header-based version detection
- Unified API across both protocol versions

### 🏠 **Homeowner Control**
- User maintains full control and can override any backend at any time
- Smart arbitration with configurable rules and preferences
- Home Assistant integration for monitoring and manual overrides

### ⚡ **Intelligent Sharing**
- Single charger connection supports multiple backend subscribers
- Real-time event broadcasting to all connected services
- Automatic control arbitration with safety-first design

### 🔗 **Dual Connection Types**
- **WebSocket Backends**: Traditional services connect to the proxy
- **OCPP Service Clients**: Proxy connects outbound to OCPP services
- Both types compete for control using the same arbitration rules

### 🛡️ **Safety & Security**
- Automatic safety controls prevent conflicts and handle charger faults
- Rate limiting and provider filtering (allowlist/blocklist support)
- Charger fault detection automatically revokes backend control

### 📊 **Monitoring & Analytics**
- Session tracking with SQLite persistence
- Revenue tracking for different providers
- CSV export functionality via REST API
- Real-time status monitoring via Home Assistant

## 🚀 Installation

### Home Assistant Add-on (Recommended)

#### Via Add-on Store
1. In Home Assistant, go to **Settings** → **Add-ons**
2. Click the **Add-on Store** tab
3. Click the **⋮** menu (three dots) in the top right corner
4. Select **Repositories**
5. Add repository URL: `https://github.com/smartenergycontrol-be/ocpp-proxy`
6. Click **Add**
7. Refresh the add-on store page
8. Search for "OCPP Proxy" or browse the available add-ons
9. Click on "OCPP Proxy" and then **Install**
10. Configure options via the add-on **Configuration** tab
11. Start the add-on from the **Info** tab

#### Manual Installation
1. SSH into your Home Assistant instance
2. Navigate to `/addons` directory
3. Clone this repository: `git clone https://github.com/smartenergycontrol-be/ocpp-proxy.git`
4. Restart Home Assistant
5. The add-on will appear in your local add-ons list

### Standalone (Docker Compose)
```yaml
version: '3'
services:
  ocpp_proxy:
    build: .
    environment:
      - HA_URL=http://homeassistant.local:8123
      - HA_TOKEN=YOUR_LONG_LIVED_ACCESS_TOKEN
    ports:
      - '9000:9000'
    volumes:
      - ./config:/data
```

### Development Setup
```bash
# Install dependencies
pip install -r requirements.txt

# Run tests
make test

# Start development server
make run
```

## ⚙️ Configuration

### Basic Configuration (YAML)
```yaml
# OCPP Protocol Settings
ocpp_version: "1.6"              # Default version (1.6 or 2.0.1)
auto_detect_ocpp_version: true   # Auto-detect from connection

# Sharing Control
allow_shared_charging: true
preferred_provider: "energy_provider_1"
rate_limit_seconds: 10

# Home Assistant Integration
presence_sensor: "binary_sensor.someone_home"
override_input_boolean: "input_boolean.charger_override"

# Provider Management
allowed_providers: ["provider1", "provider2"]  # Optional allowlist
blocked_providers: ["spammer"]                 # Optional blocklist

# OCPP Services (Outbound Connections)
ocpp_services:
  - id: "monta"
    url: "wss://ocpp.example-monta-url"   # as given by the portal, WITHOUT the charger ID
    primary: true                         # its answers go back to the charger
    enabled: true

  - id: "zeres"
    url: "wss://ocpp.example-zeres-url"
    read_only: true                       # receives data only; cannot control the charger
    auth_type: "none"                     # none | basic | token
    enabled: true
```

### Connecting the charger and services
Set the charger's OCPP URL to `ws://<home-assistant-ip>:9000/charger`. Like most chargers it
appends its own ID (e.g. `/charger/7000730523000060`). The proxy connects to every enabled
service at `<service url>/<charger id>`, so register the same charger ID in each service's
portal. Put `{charge_point_id}` in a service URL to place the ID elsewhere.

How messages are relayed:
- Everything the charger sends (boot, status, meter values, transactions) goes to all services.
- The **primary** service (the one marked `primary: true`, otherwise the first) answers the
  charger, so it decides on RFID authorization and transaction IDs. If it is unreachable, the
  proxy answers itself (accepting the charger) and delivers transaction messages once the
  service is back.
- Requests from the primary (remote start/stop, configuration, charging profiles) are
  forwarded to the charger. Remote starts are subject to the sharing rules (presence sensor,
  override boolean, `allow_shared_charging`).
- Non-primary services are **read-only** unless `read_only: false` is set; they may only read
  configuration and trigger messages.
- With OCPP 1.6 each service issues its own transaction IDs; the proxy translates them and
  stores the mapping in `TX_MAP_PATH` so it survives restarts.
- The OCPP version (1.6 or 2.0.1) follows the charger's WebSocket subprotocol.

## 🔌 API Endpoints

### WebSocket Endpoints
- **`/charger/<charger id>`** (or `/charger`) - EV charger connection (CSMS role)
  - OCPP version from the WebSocket subprotocol (`ocpp1.6` / `ocpp2.0.1`)
- **`/backend?id=backend_id`** - Backend service connections
  - Custom protocol for control requests and event subscriptions

### REST API
- **`GET /`** - Web interface with endpoint documentation
- **`GET /sessions`** - Charging sessions as JSON
- **`GET /sessions.csv`** - Charging sessions as CSV
- **`GET /status`** - Backend status and current control owner
- **`POST /override`** - Manual control override

## 🏗️ Architecture

### Core Components

```
src/ocpp_proxy/
├── main.py                    # Application entry point & HTTP server
├── relay.py                   # OCPP-J relay: charger <-> upstream services
├── backend_manager.py         # Multi-backend control arbitration
├── ocpp_service_manager.py    # Service config, transaction ID map, shared session
├── config.py                  # Configuration management
├── ha_bridge.py              # Home Assistant API integration
└── logger.py                 # Session tracking & persistence
```

### Protocol Differences Handled
- **OCPP 1.6**: `RemoteStartTransaction`, `StartTransaction`/`StopTransaction`
- **OCPP 2.0.1**: `RequestStartTransaction`, `TransactionEvent` (Started/Ended)
- Messages are relayed as raw OCPP-J; only 1.6 transaction IDs are rewritten

### Control Flow
1. **Connection**: Charger connects → Version detected → Appropriate handler created
2. **Registration**: Multiple backends subscribe to events and request control
3. **Arbitration**: Smart control arbitration based on rules and preferences
4. **Safety**: Automatic fault handling and user override capabilities

## 🧪 Testing

```bash
# Run all tests
make test

# Run specific test types
make test-unit         # Unit tests only
make test-integration  # Integration tests only
make test-e2e         # End-to-end tests only

# Coverage reporting
make test-coverage     # Generate HTML coverage report
```

Test coverage requirement: **85% minimum**

## 🔧 Development Commands

```bash
# Development
make run              # Start development server
make clean           # Clean temporary files

# Testing
make test            # Run full test suite
make test-quick      # Quick unit tests only

# Quality
make lint            # Code linting (when configured)
make format          # Code formatting (when configured)

# Docker
make docker-build    # Build Docker image
make docker-run      # Run in container
```

## 📊 Use Cases

### Primary Scenario
Homeowner with EV charger wants to:
- Allow energy providers to use spare capacity during off-peak rates
- Let fleet services use charger for delivery vehicles during work hours
- Provide public access through charging networks when on vacation
- Maintain full control with Home Assistant automation

### Example Integrations
- **Energy Providers**: Smart charging during cheap electricity periods
- **Fleet Management**: Scheduled charging for commercial vehicles  
- **Charging Networks**: Revenue sharing through public access
- **Home Automation**: Presence-based blocking and smart overrides

## 🛡️ Safety Features

- **Fault Handling**: Charger faults immediately revoke all backend control
- **Conflict Prevention**: Only one backend can control charger simultaneously
- **User Override**: Always possible via Home Assistant interface
- **Rate Limiting**: Prevents spam requests from backends
- **Provider Filtering**: Allowlist/blocklist support for security

## 📈 Future Roadmap

### OCPP 2.1 Support (2025 H2)
- Bidirectional charging (V2G) support
- Distributed Energy Resource (DER) control
- Enhanced payment integration
- Battery swapping support

### Planned Features
- Advanced pricing APIs
- Multi-charger support
- Enhanced authentication mechanisms
- Real-time analytics dashboard

## 📄 License

Open source - check license file for details.

## 🤝 Contributing

Contributions welcome! Please:
1. Run tests: `make test`
2. Follow existing code style
3. Add tests for new features
4. Update documentation

## 🔗 Links

- [OCPP 1.6 Specification](https://openchargealliance.org/protocols/ocpp-16/)
- [OCPP 2.0.1 Specification](https://openchargealliance.org/protocols/ocpp-201/)
- [Home Assistant Add-on Development](https://developers.home-assistant.io/docs/add-ons/)