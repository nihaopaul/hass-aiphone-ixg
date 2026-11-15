# Aiphone IXG for Home Assistant

Native Home Assistant integration for the **Aiphone IXG** multi-tenant intercom
system (IXG-2C7, IXG-MK, IXG-DM7, IXGW-GW/TGW). It connects directly to the
Aiphone cloud MQTT endpoint with the client certificate from a registered IXG
app slot — **no phone app and no HA MQTT broker required**.

This is an independent, unofficial project. It is not affiliated with Aiphone.

## Features

- Doorbell binary sensor (`ringing` / `idle` / `answered`)
- Last caller / doorbell state sensors
- Door-release / access sensor, plus HA bus events
- Momentary `lock` entity for door release (MID `26021`)
- Entrance `camera` / last-snapshot `image` from a receive-only WebRTC capture
- Last recording sensor (MP4 path)
- Monitor / Answer buttons
- Direct AWS IoT MQTT: mTLS, ALPN `x-amzn-mqtt-ca`, QoS 1

## Requirements

- Home Assistant 2024.3 or newer
- An IXG app slot registered by the IXG bridge (`.state/credentials.json`)
- Outbound HTTPS 443 to `api-ixg3-r*.ixg.aiphone-app.net` and MQTT 8883 to the
  AWS IoT endpoint in the credentials
- The IXG gateway must be online and the app slot must not be deregistered

## Install

### HACS (custom repository)

1. HACS → Integrations → ⋮ → **Custom repositories**
2. Add `https://github.com/nihaopaul/hass-aiphone-ixg` as an **Integration**
3. Install **Aiphone IXG**, restart Home Assistant

### Manual

Copy `custom_components/aiphone_ixg` into your HA `config/custom_components/`
directory, then restart Home Assistant.

## Configure

1. In the IXG bridge project, make sure the slot is registered and the room list
   is current:
   ```bash
   npm run refresh-room
   ```
2. Copy the contents of `.state/credentials.json`.
3. In Home Assistant: **Settings → Devices & Services → Add Integration →
   Aiphone IXG**.
4. Paste the entire credentials JSON and submit.

The integration stores the client certificate and private key in the HA config
entry. Treat your HA backup as sensitive.

> If the Node bridge is still running, stop it before enabling this integration:
> both use the same IXG MQTT client ID, and AWS IoT allows only one connection
> per client ID.

## Entities

| Entity | Purpose |
|---|---|
| `binary_sensor.…_doorbell` | Doorbell ringing |
| `sensor.…_doorbell_state` | `idle` / `ringing` / `answered` |
| `sensor.…_last_caller` | Last `DSP` display text |
| `sensor.…_last_door_release` | Timestamp + door/user/cert attributes |
| `sensor.…_last_recording` | Newest MP4 path and size |
| `lock.…_door` | Momentary door release |
| `camera.…_entrance` | Live frame during a ring; latest recording otherwise |
| `image.…_last_snapshot` | First frame of the last recording |
| `button.…_monitor` | Active monitor capture |
| `button.…_answer` | Answer the ringing call |

## HA events

Use these in automations:

- `aiphone_ixg_incoming_call` — `cid`, `display`, `last_caller`, `source_no`,
  `priority`, `raw`
- `aiphone_ixg_door_release` — `phase`, `client_id`, `terminal_name`, `key_no`,
  `door_name`, `unlock_name`, `cert_type`, `timestamp`, `raw`
- `aiphone_ixg_unregistered` — the cloud deregistered the app slot

Example automation:

```yaml
alias: Doorbell announce
trigger:
  - platform: event
    event_type: aiphone_ixg_incoming_call
action:
  - service: notify.google_home
    data:
      message: "Someone is at the {{ trigger.event.data.last_caller }}"
```

(The exact notify service depends on your Google Home/Nabu Casa setup.)

## Google Home

Direct Google Home integration would require a Smart Home Action (OAuth, public
HTTPS endpoint, device traits, camera streaming constraints). The practical
route is Home Assistant as the hub: expose the doorbell/lock/camera entities to
Google Home via **Nabu Casa Cloud → Google Assistant**, or use a Google
Assistant SDK / TTS notify service for announcements.

## Limitations

- **One subscriber per ring.** The cloud admits one video subscriber per call;
  if the phone app joins first, the integration may get `RSLT=400` and no video.
- **Local station unlocks may not reach the cloud.** App-originated unlock
  requests (`26021`) and cloud unlock notifications (`29003`) are captured;
  a door release made locally on a tenant station may not be published.
- **Property-wide topic.** The integration subscribes to the property namespace
  because app unlock requests are published there, then filters strictly to the
  configured room's app-slot CLIIDs, room/client topics, HHID, and terminals.
  Other units' messages are discarded before state handling.
- **Camera** is snapshot/last-frame oriented. Live HA streaming would need an
  additional HLS/RTSP path.
- Firmware/cloud changes can break the protocol. Use on equipment you own.

## Troubleshooting

| Symptom | Fix |
|---|---|
| “room_client_ids is missing” | Run `npm run refresh-room` in the bridge and paste the updated credentials. |
| MQTT not connected | Check outbound 8883, system clock, and that the app slot is still registered. |
| App shows “Functionality limited / EULA” | Run `npm run accept-eula -- --yes` in the bridge, then reopen the app. |
| `passive answer setup failed` / `Failed to set remote video description send parameters` | Fixed in 0.1.1: the integration now registers the H.264 profile offered by IXG with aiortc. The HA log prints the cloud's `m=video` block so the offered profile is visible. |
| `monitor: 31001 ... timeout` | The cloud did not answer the monitor room offer. 0.1.1 uses IXG's `<PROPID>-<CLIID>-<SID>` call-ID format and logs every `31xxx` signaling message (including ones dropped by the unit filter). Re-test and check the log. |
| Monitor pressed while the doorbell is ringing | A ring stores the only video slot; monitor is skipped. A ring now also pre-empts (cancels) a running monitor. |
| Camera blank right after ring | DTLS + first H264 keyframe can take 1–3 s; check HA logs for `aiortc`/`passive track`. |
| No door-release events from a tenant station | Local station unlocks may never reach the cloud; only app/cloud unlock events are visible. |
| Google Home doesn’t see the camera | Expose it through Nabu Casa/Google Assistant; some Google displays support only specific stream types. |

## Attribution

This integration is derived from
[`yufeikang/hass-aiphone`](https://github.com/yufeikang/hass-aiphone) (MIT),
which reverse-engineered the Aiphone WP-2MED cloud/WebRTC protocol. The IXG
transport, registration model, topic filtering, door-release events, and
entities were adapted for this project. The original MIT license is included in
`custom_components/aiphone_ixg/LICENSE`.

## Disclaimer

Unofficial interoperability project for equipment you own. Use may violate the
manufacturer's terms of service. Firmware updates can break it. The app slot's
private key can answer calls and release doors; keep your HA configuration and
backups private.
