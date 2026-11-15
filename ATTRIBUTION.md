# Attribution

This integration is derived from
[`yufeikang/hass-aiphone`](https://github.com/yufeikang/hass-aiphone), an MIT
licensed Home Assistant integration for Aiphone WP-2MED intercoms.

The original project reverse-engineered the Aiphone cloud MQTT and WebRTC
protocols. This repository adapts that work for the Aiphone IXG multi-tenant
system, including QR/cloud registration, `SYS:"3"`, property-wide topic
filtering, IXG door-release events, and the IXG credential flow.

The original MIT license is included in `LICENSE` and
`custom_components/aiphone_ixg/LICENSE`.
