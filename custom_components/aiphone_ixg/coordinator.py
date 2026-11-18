"""IXG cloud MQTT connection, event normalisation, and HA state."""
from __future__ import annotations

import asyncio
import json
import logging
import ssl
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import paho.mqtt.client as mqtt
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.device_registry import DeviceInfo
from homeassistant.helpers.dispatcher import async_dispatcher_send

from .const import (
    ALPN_PROTO,
    CONF_BUILDING_NO,
    CONF_CERT_PEM,
    CONF_CLIID,
    CONF_IOT_HOST,
    CONF_PROPERTY_ID,
    CONF_RCV_TOPICS,
    CONF_RCVTPC,
    CONF_ROOM_CLIENT_IDS,
    CONF_ROOM_NO,
    CONF_ROOM_TERMINALS,
    CONF_SECKEY_PEM,
    CONF_SNDTPC,
    CONF_TERMINAL_NAMES,
    CONF_TERMINAL_NO,
    CONF_TERMNAME,
    CONF_UNIT_MAC,
    DEFAULT_IOT_PORT,
    DOMAIN,
    EVENT_DOOR_RELEASE,
    EVENT_INCOMING_CALL,
    EVENT_UNREGISTERED,
    MONITOR_DEFAULT_DURATION_S,
    RING_TIMEOUT_S,
    SIGNAL_CAMERA_REFRESH,
    SIGNAL_STATE_UPDATE,
    STATE_ANSWERED,
    STATE_IDLE,
    STATE_RINGING,
)
from .media import VideoCapture, now_sid

_LOGGER = logging.getLogger(__name__)


def _display_list(flat: dict[str, Any]) -> list[str]:
    raw = flat.get("DSP")
    if isinstance(raw, list):
        return [str(x) for x in raw if x]
    if isinstance(raw, str):
        return [x.strip() for x in raw.replace(",", "\n").splitlines() if x.strip()]
    out = []
    i = 1
    while flat.get(f"DSP{i}") is not None:
        out.append(str(flat[f"DSP{i}"]))
        i += 1
    return out


def _display_text(flat: dict[str, Any]) -> str:
    display = _display_list(flat)
    if not display:
        return ""
    parts = [p.strip() for p in display[-1].split("&&&") if p.strip()]
    return " – ".join(parts[1:] or parts)


def _parse_time(value: Any) -> str | None:
    if value is None or value == "":
        return None
    s = str(value)
    if s.isdigit():
        if len(s) >= 14:
            stamp = s[:14]
            try:
                return datetime.strptime(stamp, "%Y%m%d%H%M%S").astimezone().isoformat()
            except ValueError:
                return None
        # epoch seconds/milliseconds
        n = int(s)
        ms = n if n > 1_000_000_000_000 else n * 1000
        return datetime.fromtimestamp(ms / 1000, tz=UTC).isoformat()
    return s


class AiphoneCoordinator:
    """Owns the IXG cloud connection and exposes parsed state to entities."""

    def __init__(self, hass: HomeAssistant, entry: ConfigEntry) -> None:
        self.hass = hass
        self.entry = entry
        d = entry.data

        self.cliid: str = str(d[CONF_CLIID])
        self.property_id: str = str(d.get(CONF_PROPERTY_ID) or d.get(CONF_UNIT_MAC))
        self.iot_host: str = d[CONF_IOT_HOST]
        self.iot_port: int = DEFAULT_IOT_PORT
        self.sndtpc: str = d[CONF_SNDTPC]
        self.rcvtpc: str = d[CONF_RCVTPC]
        self.rcv_topics: list[str] = [str(t) for t in d.get(CONF_RCV_TOPICS, [self.rcvtpc])]
        self.termname: str = d.get(CONF_TERMNAME) or "IXG"
        self.building_no: str = str(d.get(CONF_BUILDING_NO) or "")
        self.room_no: str = str(d.get(CONF_ROOM_NO) or "")
        self.terminal_no: str = str(d.get(CONF_TERMINAL_NO) or "")
        self.terminal_names: list[str] = d.get(CONF_TERMINAL_NAMES) or []
        self.room_client_ids: set[str] = {str(x) for x in d.get(CONF_ROOM_CLIENT_IDS, [])}
        self.room_terminals: list[dict[str, Any]] = d.get(CONF_ROOM_TERMINALS) or []
        self._cert_pem: str = d[CONF_CERT_PEM]
        self._seckey_pem: str = d[CONF_SECKEY_PEM]

        self.doorbell_state: str = STATE_IDLE
        self.last_caller: str = ""
        self.last_event: dict[str, Any] = {}
        self.last_door_release: dict[str, Any] = {}
        self.last_source_no: str = ""
        self.last_source_cliid: str = ""
        self.last_call_cid: str | None = None
        self._active_cid: str | None = None
        self._seen_cids: dict[str, float] = {}
        self._fetching: set[str] = set()

        self._client: mqtt.Client | None = None
        self._cert_path: Path | None = None
        self._key_path: Path | None = None
        self._timer_handle: asyncio.TimerHandle | None = None

        self.video = VideoCapture(self)

    # ------------------------------------------------------------------
    # lifecycle
    # ------------------------------------------------------------------
    async def async_start(self) -> None:
        await self.hass.async_add_executor_job(self._load_media_state)
        await self.hass.async_add_executor_job(self._materialize_certs)
        await self.hass.async_add_executor_job(self._connect)

    async def async_stop(self) -> None:
        await self.video.aclose()
        if self._client is not None:
            await self.hass.async_add_executor_job(self._client.loop_stop)
            await self.hass.async_add_executor_job(self._client.disconnect)
            self._client = None
        if self._timer_handle is not None:
            self._timer_handle.cancel()
            self._timer_handle = None

    def _materialize_certs(self) -> None:
        """Write cert/key for paho's SSL context."""
        runtime = Path(self.hass.config.path(DOMAIN)) / "_runtime" / self.cliid[:8]
        runtime.mkdir(parents=True, exist_ok=True)
        self._cert_path = runtime / "cert.pem"
        self._key_path = runtime / "key.pem"
        self._cert_path.write_text(self._cert_pem)
        self._key_path.write_text(self._seckey_pem)
        try:
            self._key_path.chmod(0o600)
        except OSError:
            pass

    # ------------------------------------------------------------------
    # remembered monitor target (last entrance station that called)
    # ------------------------------------------------------------------
    @property
    def _state_path(self) -> Path:
        return Path(self.hass.config.path(DOMAIN)) / "state.json"

    def _load_media_state(self) -> None:
        """Runs in executor — restore the last caller across HA restarts."""
        try:
            data = json.loads(self._state_path.read_text())
        except (OSError, ValueError):
            return
        self.last_source_no = str(data.get("last_source_no") or "")
        self.last_source_cliid = str(data.get("last_source_cliid") or "")

    def _save_media_state(self) -> None:
        """Runs in executor — remember the entrance station for monitoring."""
        path = self._state_path
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            tmp = path.with_suffix(".tmp")
            tmp.write_text(
                json.dumps(
                    {
                        "last_source_no": self.last_source_no,
                        "last_source_cliid": self.last_source_cliid,
                    }
                )
            )
            tmp.replace(path)
        except OSError:
            _LOGGER.warning("could not persist monitor target", exc_info=True)

    # ------------------------------------------------------------------
    # MQTT
    # ------------------------------------------------------------------
    def _connect(self) -> None:
        c = mqtt.Client(client_id=self.cliid, protocol=mqtt.MQTTv311, clean_session=False)
        ctx = ssl.create_default_context()
        ctx.load_cert_chain(certfile=str(self._cert_path), keyfile=str(self._key_path))
        ctx.set_alpn_protocols([ALPN_PROTO])
        c.tls_set_context(ctx)
        c.on_connect = self._on_connect
        c.on_disconnect = self._on_disconnect
        c.on_message = self._on_message
        c.connect_async(self.iot_host, self.iot_port, keepalive=60)
        c.loop_start()
        self._client = c

    def _on_connect(self, client: mqtt.Client, userdata, flags, rc):
        if rc != 0:
            _LOGGER.error("AWS IoT CONNACK rc=%s", rc)
            return
        wildcard = f"{self.property_id}/#"
        client.subscribe(wildcard, qos=1)
        _LOGGER.info("IXG AWS IoT connected; subscribed %s", wildcard)

    def _on_disconnect(self, client, userdata, rc):
        _LOGGER.warning("IXG AWS IoT disconnected rc=%s — paho will reconnect", rc)

    def _on_message(self, client, userdata, msg):
        try:
            raw = json.loads(msg.payload)
        except Exception:
            return
        self.hass.loop.call_soon_threadsafe(self._dispatch, raw, msg.topic)

    # ------------------------------------------------------------------
    # outbound
    # ------------------------------------------------------------------
    def publish(self, msg: dict[str, Any]) -> None:
        if self._client is None:
            _LOGGER.warning("publish called before MQTT client connected")
            return
        payload = json.dumps(msg, separators=(",", ":")) + "\n"
        self._client.publish(self.sndtpc, payload, qos=1)

    async def async_start_monitor(
        self, duration_s: int = MONITOR_DEFAULT_DURATION_S
    ) -> None:
        self.video.start_monitor(duration_s)

    def async_answer_call(self) -> bool:
        cid = self._active_cid or getattr(self.video, "_mon_cid", None)
        if not cid:
            _LOGGER.warning("answer_call: no active call/monitor to answer")
            return False
        self.publish(
            {
                "HEADER": {
                    "MID": "24000",
                    "VER": "1.0",
                    "SYS": "3",
                    "CLIID": self.cliid,
                    "CKIND": 1,
                    "RECKIND": 0,
                    "AVID": 0,
                    "CID": cid,
                    "RSLT": 200,
                    "REASON": 0,
                    "LEN": 0,
                    "SID": now_sid(),
                    "SNDTPC": self.sndtpc,
                    "RCVTPC": self.rcvtpc,
                },
                "BODY": {},
            }
        )
        _LOGGER.info("IXG 24000 RSLT=200 answer sent for cid=%s", str(cid)[-30:])
        return True

    def async_unlock_door(self) -> None:
        header = {
            "MID": "26021",
            "VER": "1.0",
            "SYS": "3",
            "CLIID": self.cliid,
            "RCVTPC": self.rcvtpc,
            "RLS": "0",
            "RSLT": 0,
            "LEN": 0,
            "SID": now_sid(),
        }
        if self._active_cid:
            header["CID"] = self._active_cid
        self.publish({"HEADER": header, "BODY": {"KEYNO": 1}})
        _LOGGER.info("IXG 26021 SET_UNLOCK_REQ sent")

    # ------------------------------------------------------------------
    # filtering
    # ------------------------------------------------------------------
    @property
    def _client_topic(self) -> str | None:
        return next((t for t in self.rcv_topics if "/Client/" in t), None)

    @property
    def client_topic(self) -> str:
        """The app slot's own Client topic (media join lives here)."""
        return self._client_topic or f"{self.property_id}/Client/{self.cliid}"

    @property
    def _room_topic(self) -> str | None:
        return next((t for t in self.rcv_topics if "/Room/" in t), None)

    @property
    def _building_topic(self) -> str | None:
        return next((t for t in self.rcv_topics if "/Building/" in t), None)

    @property
    def _room_id(self) -> str | None:
        topic = self._room_topic
        return topic.split("/")[-1] if topic else None

    def _term_labels(self) -> set[str]:
        labels: set[str] = set()
        for term in self.room_terminals:
            labels.add(str(term.get("term_no", "")))
            for name in term.get("names", []) or []:
                labels.add(str(name))
        return {x for x in labels if x}

    def _is_relevant(self, raw: dict[str, Any], topic: str) -> bool:
        head = raw.get("HEADER", {}) or {}
        body = raw.get("BODY", {}) or {}
        flat = {**head, **body}
        mid = str(head.get("MID", ""))
        cliid = str(flat.get("CLIID", ""))
        rcvtpc = str(flat.get("RCVTPC", ""))
        cid = str(flat.get("CID", ""))
        hhid = str(flat.get("HHID") or flat.get("SRCHHID") or flat.get("DESTHHID") or "")
        term = str(flat.get("UNLOCKTERMNM") or flat.get("TERMNO") or "")

        client_topic = self._client_topic
        room_topic = self._room_topic
        building_topic = self._building_topic

        if topic in (client_topic, room_topic):
            return True
        if topic == building_topic and mid == "99991":
            return True
        # Everything else must match this unit.
        if cliid in self.room_client_ids:
            return True
        if rcvtpc and rcvtpc in (room_topic, client_topic):
            return True
        if cid and any(
            cid.find(f"-{rid}-") >= 0 or cid.endswith(f"-{rid}")
            for rid in self.room_client_ids
        ):
            return True
        if hhid and self._room_id and hhid == self._room_id:
            return True
        if term and term in self._term_labels():
            return True
        return False

    # ------------------------------------------------------------------
    # state machine (HA loop)
    # ------------------------------------------------------------------
    @callback
    def _dispatch(self, raw: dict[str, Any], topic: str = "") -> None:
        head = raw.get("HEADER", {}) or {}
        mid = str(head.get("MID", ""))
        # Signaling diagnostics: log the 3xxxx MediaRoom messages and 24021
        # replies even when strict unit filtering later drops them.
        if mid.startswith("31") or mid in ("24021", "24022", "24023"):
            _LOGGER.info(
                "IXG signaling MID=%s topic=%s cliid=%s cid=%s rslt=%s reason=%s",
                mid,
                topic,
                head.get("CLIID"),
                head.get("CID"),
                head.get("RSLT"),
                head.get("REASON"),
            )

        if not self._is_relevant(raw, topic):
            _LOGGER.debug("IXG: ignoring message outside this unit: %s", topic)
            return

        body = raw.get("BODY", {}) or {}
        flat = {**head, **body}
        mid = str(head.get("MID", ""))
        cid = str(head.get("CID") or "") or None
        now = datetime.now(UTC).isoformat()

        if mid in ("23001", "24000"):
            if mid == "24000" and flat.get("RSLT") not in (200, "200"):
                return
            if cid:
                seen = self._seen_cids.get(cid)
                if seen is not None and (datetime.now().timestamp() - seen) < 60:
                    return
                self._seen_cids[cid] = datetime.now().timestamp()
                self._seen_cids = {
                    k: v
                    for k, v in self._seen_cids.items()
                    if datetime.now().timestamp() - v < 120
                }

            display = _display_list(flat)
            self.last_caller = _display_text(flat) or "?"
            new_source_no = str(flat.get("SRCNO") or "")
            new_source_cliid = (
                str(head.get("CLIID") or "") if mid == "23001" else ""
            )
            target_changed = False
            if new_source_no and new_source_no != self.last_source_no:
                self.last_source_no = new_source_no
                target_changed = True
            if new_source_cliid and new_source_cliid != self.last_source_cliid:
                self.last_source_cliid = new_source_cliid
                target_changed = True
            if target_changed:
                self.hass.async_create_task(
                    self.hass.async_add_executor_job(self._save_media_state)
                )
            self.last_event = {
                "ts": now,
                "event": "incoming_call",
                "mid": mid,
                "cid": cid,
                "display": display,
                "last_caller": self.last_caller,
                "source_no": flat.get("SRCNO"),
                "priority": flat.get("PRI"),
            }
            self._set_doorbell(STATE_RINGING)
            self._arm_timer()
            self._active_cid = cid
            if cid:
                self.last_call_cid = cid
            self.hass.bus.async_fire(
                EVENT_INCOMING_CALL,
                {
                    "device_id": self.device_id,
                    "cid": cid,
                    "display": display,
                    "last_caller": self.last_caller,
                    "source_no": flat.get("SRCNO"),
                    "priority": flat.get("PRI"),
                    "mid": mid,
                    "raw": raw,
                },
            )
            if cid:
                # On 23001 CLIID is the calling (entrance) station. On 24000 it
                # is the receiving app slot, so it is not the peer client id.
                source_cliid = str(head.get("CLIID") or "") if mid == "23001" else None
                self.hass.async_create_task(
                    self.video.passive_capture(cid, self.last_caller, source_cliid)
                )
        elif mid == "24002":
            self._set_doorbell(STATE_ANSWERED)
            self._cancel_timer()
            if cid and cid == self._active_cid:
                self._active_cid = None
                self.hass.async_create_task(self.video.passive_finalize())
        elif mid in ("24003", "24004"):
            self._set_doorbell(STATE_IDLE)
            self._cancel_timer()
            if cid:
                self.hass.async_create_task(self._auto_fetch_cloud(cid))
            if cid and cid == self._active_cid:
                self._active_cid = None
                self.hass.async_create_task(self.video.passive_finalize(0))
        elif mid in ("26021", "29003"):
            term = str(flat.get("UNLOCKTERMNM") or "")
            label = next(
                (
                    name
                    for t in self.room_terminals
                    for name in t.get("names", [])
                    if str(t.get("term_no")) == term or str(t.get("client_id")) == str(flat.get("CLIID"))
                ),
                None,
            )
            self.last_door_release = {
                "ts": now,
                "event": "door_release",
                "mid": mid,
                "phase": "request" if mid == "26021" else "notification",
                "cid": cid,
                "client_id": flat.get("CLIID"),
                "terminal_name": label,
                "key_no": flat.get("KEYNO"),
                "door_name": flat.get("DOORNAME"),
                "unlock_name": flat.get("UNLOCKNAME"),
                "unlock_terminal": flat.get("UNLOCKTERMNM"),
                "cert_type": flat.get("CERTTYPE"),
                "timestamp": _parse_time(flat.get("TIMESTAMP") or flat.get("SID")),
            }
            self.last_event = self.last_door_release
            self.hass.bus.async_fire(EVENT_DOOR_RELEASE, {**self.last_door_release, "raw": raw})
        elif mid == "29030":
            _LOGGER.error("IXG app slot was deregistered by the cloud")
            self.hass.bus.async_fire(EVENT_UNREGISTERED, {"device_id": self.device_id})
        elif mid in ("31011", "31012", "31001", "24021", "24041"):
            self.video.on_signal(mid, raw)

        async_dispatcher_send(self.hass, SIGNAL_STATE_UPDATE)

    async def async_fetch_recording(self, rec_id: str | None = None) -> None:
        """Download a cloud recording by RECID (defaults to the last call)."""
        rec_id = rec_id or self.last_call_cid
        if not rec_id:
            _LOGGER.warning("fetch_recording: no recording ID available")
            return
        await self.video.fetch_cloud_recording(rec_id)

    async def _auto_fetch_cloud(self, cid: str) -> None:
        if cid in self._fetching:
            return
        self._fetching.add(cid)
        try:
            await asyncio.sleep(8)
            await self.video.fetch_cloud_recording(cid, attempts=3)
        finally:
            self._fetching.discard(cid)

    def _set_doorbell(self, new_state: str) -> None:
        if new_state == self.doorbell_state:
            return
        self.doorbell_state = new_state
        async_dispatcher_send(self.hass, SIGNAL_STATE_UPDATE)

    def _arm_timer(self) -> None:
        self._cancel_timer()
        self._timer_handle = self.hass.loop.call_later(RING_TIMEOUT_S, self._on_timeout)

    def _cancel_timer(self) -> None:
        if self._timer_handle is not None:
            self._timer_handle.cancel()
            self._timer_handle = None

    @callback
    def _on_timeout(self) -> None:
        self._timer_handle = None
        if self.doorbell_state == STATE_RINGING:
            _LOGGER.info("IXG doorbell timeout → idle")
            self._set_doorbell(STATE_IDLE)

    @callback
    def _on_recording_ready(self) -> None:
        async_dispatcher_send(self.hass, SIGNAL_CAMERA_REFRESH)

    # ------------------------------------------------------------------
    # device info
    # ------------------------------------------------------------------
    @property
    def device_id(self) -> str:
        return f"{DOMAIN}_{self.property_id}_{self.room_no}".replace(" ", "_")

    @property
    def device_info(self) -> DeviceInfo:
        name = f"Aiphone IXG {self.room_no}".strip()
        return DeviceInfo(
            identifiers={(DOMAIN, self.device_id)},
            name=name,
            manufacturer="Aiphone",
            model="IXG",
        )
