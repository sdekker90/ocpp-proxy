#!/usr/bin/with-contenv bashio

bashio::log.info "Starting OCPP Proxy..."

# Change to application directory and set PYTHONPATH
cd /app
export PYTHONPATH=/app/src

# Home Assistant writes the add-on options as JSON (valid YAML) to /data/options.json.
# /data is persistent, so the session log and transaction ID map survive restarts.
export ADDON_CONFIG_FILE=/data/options.json
export LOG_DB_PATH=/data/usage_log.db
export TX_MAP_PATH=/data/ocpp_tx_map.json

# Start the OCPP Proxy
exec python3 -m ocpp_proxy.main
