"""The Aiphone IXG integration."""
from __future__ import annotations

import logging

import voluptuous as vol
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import Platform
from homeassistant.core import HomeAssistant, ServiceCall

from .const import DOMAIN, SERVICE_FETCH_RECORDING
from .coordinator import AiphoneCoordinator

_LOGGER = logging.getLogger(__name__)

PLATFORMS: list[Platform] = [
    Platform.BINARY_SENSOR,
    Platform.BUTTON,
    Platform.CAMERA,
    Platform.IMAGE,
    Platform.LOCK,
    Platform.SENSOR,
]


async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Set up Aiphone IXG from a config entry."""
    coordinator = AiphoneCoordinator(hass, entry)
    try:
        await coordinator.async_start()
    except Exception:
        _LOGGER.exception("Failed to start Aiphone IXG coordinator")
        return False

    hass.data.setdefault(DOMAIN, {})[entry.entry_id] = coordinator

    if not hass.services.has_service(DOMAIN, SERVICE_FETCH_RECORDING):
        async def _async_fetch_recording(call: ServiceCall) -> None:
            rec_id = call.data.get("rec_id")
            for coord in hass.data.get(DOMAIN, {}).values():
                await coord.async_fetch_recording(rec_id)

        hass.services.async_register(
            DOMAIN,
            SERVICE_FETCH_RECORDING,
            _async_fetch_recording,
            schema=vol.Schema({vol.Optional("rec_id"): str}),
        )

    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)
    return True


async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Unload a config entry."""
    if await hass.config_entries.async_unload_platforms(entry, PLATFORMS):
        coordinator: AiphoneCoordinator = hass.data[DOMAIN].pop(entry.entry_id)
        await coordinator.async_stop()
        return True
    return False
