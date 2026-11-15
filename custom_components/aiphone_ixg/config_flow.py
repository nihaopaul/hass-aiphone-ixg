"""Config flow for Aiphone IXG.

The IXG bridge has already completed the one-time QR/app-slot registration.
This flow imports the resulting `.state/credentials.json` so Home Assistant can
connect to the IXG cloud MQTT endpoint directly.
"""
from __future__ import annotations

import json
import logging
from typing import Any

import voluptuous as vol
from homeassistant import config_entries
from homeassistant.data_entry_flow import FlowResult
from homeassistant.helpers import selector

from .const import (
    CONF_BUILDING_NAMES,
    CONF_BUILDING_NO,
    CONF_CERT_PEM,
    CONF_CLIID,
    CONF_CREDENTIALS,
    CONF_IOT_HOST,
    CONF_PROPERTY_ID,
    CONF_RCV_TOPICS,
    CONF_RCVTPC,
    CONF_ROOM_CLIENT_IDS,
    CONF_ROOM_NAMES,
    CONF_ROOM_NO,
    CONF_ROOM_TERMINALS,
    CONF_SECKEY_PEM,
    CONF_SNDTPC,
    CONF_TERMINAL_NAMES,
    CONF_TERMINAL_NO,
    CONF_TERMNAME,
    CONF_UNIT_MAC,
    CONFIG_VERSION,
    DOMAIN,
)

_LOGGER = logging.getLogger(__name__)

STEP_USER_DATA_SCHEMA = vol.Schema(
    {
        vol.Required(CONF_CREDENTIALS): selector.TextSelector(
            selector.TextSelectorConfig(
                multiline=True,
                type=selector.TextSelectorType.TEXT,
            )
        ),
    }
)


class AiphoneIxgConfigFlow(config_entries.ConfigFlow, domain=DOMAIN):
    """Import IXG cloud credentials into Home Assistant."""

    VERSION = CONFIG_VERSION

    async def async_step_user(self, user_input: dict[str, Any] | None = None) -> FlowResult:
        errors: dict[str, str] = {}
        if user_input is not None:
            try:
                data = self._parse_credentials(user_input[CONF_CREDENTIALS])
            except ValueError as err:
                errors["credentials"] = str(err)
            except Exception:
                _LOGGER.exception("Unexpected IXG credentials error")
                errors["base"] = "unknown"
            else:
                await self.async_set_unique_id(f"{data[CONF_PROPERTY_ID]}::{data[CONF_CLIID]}")
                self._abort_if_unique_id_configured()
                title = (
                    f"Aiphone IXG {data.get(CONF_BUILDING_NO, '')} "
                    f"{data.get(CONF_ROOM_NO, '')}"
                ).strip()
                return self.async_create_entry(title=title, data=data)

        return self.async_show_form(
            step_id="user",
            data_schema=STEP_USER_DATA_SCHEMA,
            errors=errors,
        )

    def _parse_credentials(self, raw: str) -> dict[str, Any]:
        try:
            src = json.loads(raw)
        except json.JSONDecodeError as err:
            raise ValueError("invalid_json") from err
        if not isinstance(src, dict):
            raise ValueError("invalid_json")

        client_id = str(src.get("client_id") or "")
        prop_id = str(src.get("property_id") or "")
        cert = str(src.get("cert_pem") or "")
        seckey = str(src.get("key_pem") or "")
        iot_url = str(src.get("iot_url") or "")
        snd_topic = str(src.get("snd_topic") or "")
        rcv_topics = src.get("rcv_topics") or []
        if isinstance(rcv_topics, str):
            rcv_topics = [rcv_topics]

        missing = [
            name
            for name, value in (
                ("client_id", client_id),
                ("property_id", prop_id),
                ("cert_pem", cert),
                ("key_pem", seckey),
                ("iot_url", iot_url),
                ("snd_topic", snd_topic),
            )
            if not value
        ]
        if missing:
            raise ValueError("missing_fields")
        if "BEGIN CERTIFICATE" not in cert:
            raise ValueError("invalid_certificate")
        if "BEGIN" not in seckey or "PRIVATE KEY" not in seckey:
            raise ValueError("invalid_private_key")
        if not rcv_topics:
            raise ValueError("missing_receive_topics")

        rcvtpc = next(
            (t for t in rcv_topics if "/Client/" in str(t)),
            f"{prop_id}/Client/{client_id}",
        )
        terms = src.get("room_terminals") or []
        room_ids = src.get("room_client_ids") or [
            str(t.get("client_id")) for t in terms if isinstance(t, dict) and t.get("client_id")
        ]
        if not room_ids:
            raise ValueError("room_list_missing")

        terminal_names = src.get("terminal_names") or []
        termname = terminal_names[0] if terminal_names else "IXG"

        return {
            CONF_CLIID: client_id,
            CONF_IOT_HOST: iot_url,
            CONF_SNDTPC: snd_topic,
            CONF_RCVTPC: rcvtpc,
            CONF_RCV_TOPICS: [str(t) for t in rcv_topics],
            CONF_CERT_PEM: cert,
            CONF_SECKEY_PEM: seckey,
            CONF_TERMNAME: termname,
            # unit_mac is historically the property discriminator in the entity code
            CONF_UNIT_MAC: prop_id,
            CONF_PROPERTY_ID: prop_id,
            CONF_BUILDING_NO: str(src.get("building_no") or ""),
            CONF_BUILDING_NAMES: src.get("building_names") or [],
            CONF_ROOM_NO: str(src.get("room_no") or ""),
            CONF_ROOM_NAMES: src.get("room_names") or [],
            CONF_TERMINAL_NO: str(src.get("terminal_no") or ""),
            CONF_TERMINAL_NAMES: terminal_names,
            CONF_ROOM_CLIENT_IDS: [str(x) for x in room_ids],
            CONF_ROOM_TERMINALS: terms,
        }
