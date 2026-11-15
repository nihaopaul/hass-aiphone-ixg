"""Button: trigger a monitor (camera-on, no ring) session."""
from __future__ import annotations

from homeassistant.components.button import ButtonEntity
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from .const import DOMAIN
from .coordinator import AiphoneCoordinator


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    coord: AiphoneCoordinator = hass.data[DOMAIN][entry.entry_id]
    async_add_entities([
        AiphoneMonitorButton(coord),
        AiphoneAnswerButton(coord),
    ])


def _device_info(coord: AiphoneCoordinator):
    return coord.device_info


class AiphoneMonitorButton(ButtonEntity):
    _attr_has_entity_name = True
    _attr_name = "Monitor"
    _attr_icon = "mdi:cctv"

    def __init__(self, coordinator: AiphoneCoordinator) -> None:
        self._coord = coordinator
        self._attr_unique_id = f"{coordinator.device_id}_monitor"
        self._attr_device_info = _device_info(coordinator)

    async def async_press(self) -> None:
        await self._coord.async_start_monitor()


class AiphoneAnswerButton(ButtonEntity):
    """Accept the currently-ringing call. Unit stops ringing; audio opens."""

    _attr_has_entity_name = True
    _attr_name = "Answer"
    _attr_icon = "mdi:phone-incoming"

    def __init__(self, coordinator: AiphoneCoordinator) -> None:
        self._coord = coordinator
        self._attr_unique_id = f"{coordinator.device_id}_answer"
        self._attr_device_info = _device_info(coordinator)

    async def async_press(self) -> None:
        self._coord.async_answer_call()
