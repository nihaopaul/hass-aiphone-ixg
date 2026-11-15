"""Constants for the Aiphone IXG integration."""
from __future__ import annotations

DOMAIN = "aiphone_ixg"
CONFIG_VERSION = 1

# Config-entry data keys
CONF_CREDENTIALS = "credentials"
CONF_CLIID = "cliid"
CONF_IOT_HOST = "iot_host"
CONF_SNDTPC = "sndtpc"
CONF_RCVTPC = "rcvtpc"
CONF_RCV_TOPICS = "rcv_topics"
CONF_CERT_PEM = "cert_pem"
CONF_SECKEY_PEM = "seckey_pem"
CONF_TERMNAME = "termname"
# unit_mac is kept as the property id for the existing entity/device code
CONF_UNIT_MAC = "unit_mac"
CONF_PROPERTY_ID = "property_id"
CONF_BUILDING_NO = "building_no"
CONF_BUILDING_NAMES = "building_names"
CONF_ROOM_NO = "room_no"
CONF_ROOM_NAMES = "room_names"
CONF_TERMINAL_NO = "terminal_no"
CONF_TERMINAL_NAMES = "terminal_names"
CONF_ROOM_CLIENT_IDS = "room_client_ids"
CONF_ROOM_TERMINALS = "room_terminals"

# AWS IoT
DEFAULT_IOT_PORT = 8883
ALPN_PROTO = "x-amzn-mqtt-ca"

# Dispatcher signals
SIGNAL_STATE_UPDATE = f"{DOMAIN}_state_update"
SIGNAL_CAMERA_REFRESH = f"{DOMAIN}_camera_refresh"

# Doorbell state machine
STATE_IDLE = "idle"
STATE_RINGING = "ringing"
STATE_ANSWERED = "answered"

RING_TIMEOUT_S = 45

# Media capture
RECORDINGS_SUBDIR = f"{DOMAIN}/recordings"
PASSIVE_HOLD_AFTER_END_S = 8
MONITOR_DEFAULT_DURATION_S = 30
SDP_OFFER_GATHER_TIMEOUT_S = 4

# HA bus events
EVENT_INCOMING_CALL = f"{DOMAIN}_incoming_call"
EVENT_DOOR_RELEASE = f"{DOMAIN}_door_release"
EVENT_MEDIA_READY = f"{DOMAIN}_media_ready"
EVENT_UNREGISTERED = f"{DOMAIN}_unregistered"
