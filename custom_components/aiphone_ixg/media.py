"""WebRTC media capture for Aiphone — passive (doorbell ring) + active (monitor).

Two-PC architecture for active monitor (per CallManager.getMonitorRequests):
  PC #1 publisher placeholder  — claims our slot in the Janus VideoRoom
  PC #2 subscriber to IXG — actually receives the camera feed

Passive doorbell flow uses just one PC as subscriber (mirrors the official
phone app's behavior when receiving an incoming ring).

Both flows: we munge Janus's `setup:actpass` SDP offer to `setup:active` so
aiortc, as RFC-compliant answerer, MUST become passive (DTLS server). This
prevents the both-active deadlock we hit before.
"""
from __future__ import annotations

import asyncio
import io
import logging
import re
import time
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

# aiortc pins av<17 but HA pins av==17.0.1; install it --no-deps before importing
# (runtime-compatible). See _deps.py for the full rationale.
from ._deps import ensure_runtime_deps

ensure_runtime_deps()

from aiortc import RTCConfiguration, RTCPeerConnection, RTCSessionDescription
from aiortc.codecs import CODECS
from aiortc.contrib.media import MediaRecorder
from aiortc.mediastreams import MediaStreamTrack
from aiortc.rtcrtpparameters import RTCRtcpFeedback, RTCRtpCodecParameters
from homeassistant.helpers.dispatcher import async_dispatcher_send

from .const import (
    MONITOR_DEFAULT_DURATION_S,
    PASSIVE_HOLD_AFTER_END_S,
    RECORDINGS_SUBDIR,
    SDP_OFFER_GATHER_TIMEOUT_S,
    SIGNAL_CAMERA_REFRESH,
)

if TYPE_CHECKING:
    from .coordinator import AiphoneCoordinator

_LOGGER = logging.getLogger(__name__)


def now_sid() -> str:
    n = datetime.now()
    return n.strftime("%Y%m%d%H%M%S") + f"{n.microsecond:06d}"


def _strip_extra_fp(sdp: str) -> str:
    """Janus's DTLS parser only handles the first fingerprint reliably."""
    return "\r\n".join(
        ln for ln in sdp.split("\r\n")
        if not (ln.startswith("a=fingerprint:") and not ln.startswith("a=fingerprint:sha-256"))
    )


def _munge_offer(sdp: str) -> str:
    """Make an IXG/Janus offer palatable to aiortc.

    - actpass -> active so aiortc becomes the passive DTLS peer.
    - drop bare ``a=ssrc:<id>`` lines: IXG emits them without the attribute
      aiortc's SDP parser requires, which otherwise raises ValueError.
    """
    sdp = sdp.replace("a=setup:actpass", "a=setup:active")
    return "\r\n".join(
        ln
        for ln in sdp.split("\r\n")
        if not (ln.startswith("a=ssrc:") and " " not in ln)
    )


def _filename_safe(s: str) -> str:
    return re.sub(r"[^A-Za-z0-9]+", "_", s) or "recording"


def _consume_task_exception(task: asyncio.Task) -> None:
    if task.cancelled():
        return
    try:
        task.exception()
    except Exception:  # noqa: BLE001, S110 - the point is to consume it
        pass


def _consume_aiortc_connect_tasks() -> None:
    """Retrieve aiortc's internal ``__connect()`` task exceptions.

    aiortc schedules ``__connect()`` with ``asyncio.ensure_future()`` and keeps
    no handle. Closing a peer connection before ICE starts makes that task
    raise ``InvalidStateError``, which Home Assistant then logs as an
    unhandled task exception. Attaching a consumer marks it as retrieved.
    """
    for task in asyncio.all_tasks():
        coro = task.get_coro()
        if getattr(coro, "__qualname__", "").endswith(
            "RTCPeerConnection.__connect"
        ):
            task.add_done_callback(_consume_task_exception)


def _log_audio_section(tag: str, sdp: str) -> None:
    """Print the m=audio media block + its codec/rtpmap/direction lines."""
    in_audio = False
    lines: list[str] = []
    for ln in sdp.splitlines():
        if ln.startswith("m="):
            in_audio = ln.startswith("m=audio")
            if in_audio:
                lines.append(ln)
            continue
        if in_audio and ln.startswith(("a=rtpmap:", "a=fmtp:", "a=sendrecv",
                                       "a=sendonly", "a=recvonly", "a=inactive",
                                       "a=mid:", "c=")):
            lines.append(ln)
    _LOGGER.info("%s audio block:\n  %s", tag, "\n  ".join(lines) if lines else "(none)")


# aiortc barely advertises H.264: constrained baseline (42001f / 42e01f) with
# packetization-mode=1 only.  IXG offers H.264 with whatever profile the
# gateway/entrance station is configured for (often Main/High or
# packetization-mode=0), which makes aiortc raise
# "Failed to set remote video description send parameters" before we can even
# answer.  PyAV (aiortc's decoder) handles every H.264 profile, so register the
# offered profile with aiortc instead of refusing the stream.
_H264_LOCAL_PT = 127
_H264_DEFAULT_PROFILE = "42e01f"
_KNOWN_H264: set[tuple[str, str]] = {("42001f", "1"), ("42e01f", "1")}

# AiaccLibDef.AiaccLibReplyCode — the values IXG uses in 24021/30012 replies.
_REPLY_CODE_NAMES = {
    200: "OK",
    302: "moved temporarily",
    404: "not found",
    410: "gone",
    451: "unavailable for legal reasons",
    480: "unavailable",
    481: "does not exist",
    486: "busy",
    487: "priority processing",
    488: "not acceptable here",
    603: "rejected",
    604: "recording file access error",
}


def _sdp_media_block(sdp: str, kind: str) -> list[str]:
    """Return the lines of the first ``m=<kind>`` section of an SDP."""
    block: list[str] = []
    inside = False
    for line in sdp.splitlines():
        if line.startswith("m="):
            if inside:
                break
            inside = line.startswith(f"m={kind} ")
            block = [line] if inside else []
        elif inside:
            block.append(line)
    return block


def _sdp_rtpmaps(sdp: str, kind: str) -> dict[int, tuple[str, int]]:
    codecs: dict[int, tuple[str, int]] = {}
    for line in _sdp_media_block(sdp, kind):
        m = re.match(r"a=rtpmap:(\d+)\s+([^/\s]+)/(\d+)", line)
        if m:
            codecs[int(m.group(1))] = (m.group(2), int(m.group(3)))
    return codecs


def _sdp_fmtps(sdp: str, kind: str) -> dict[int, dict[str, str]]:
    out: dict[int, dict[str, str]] = {}
    for line in _sdp_media_block(sdp, kind):
        m = re.match(r"a=fmtp:(\d+)\s+(.*)", line)
        if not m:
            continue
        params: dict[str, str] = {}
        for part in m.group(2).split(";"):
            part = part.strip()
            if not part:
                continue
            key, _, value = part.partition("=")
            params[key.strip()] = value.strip()
        out[int(m.group(1))] = params
    return out


def _log_video_section(tag: str, sdp: str) -> None:
    """Print the m=video block + its codec/direction lines (diagnostics)."""
    wanted = (
        "m=",
        "a=rtpmap:",
        "a=fmtp:",
        "a=sendrecv",
        "a=sendonly",
        "a=recvonly",
        "a=inactive",
        "a=mid:",
        "c=",
    )
    lines = [ln for ln in _sdp_media_block(sdp, "video") if ln.startswith(wanted)]
    _LOGGER.info("%s video block:\n  %s", tag, "\n  ".join(lines) if lines else "(none)")


def _prepare_offer_video(tag: str, sdp: str) -> bool:
    """Make aiortc able to negotiate the video codecs in a cloud offer.

    Returns False when the offer carries a video section but none of its
    codecs can be decoded by aiortc.
    """
    _log_video_section(tag, sdp)
    if not _sdp_media_block(sdp, "video"):
        return True

    rtpmaps = _sdp_rtpmaps(sdp, "video")
    if not rtpmaps:
        _LOGGER.warning("%s: video section has no rtpmap", tag)
        return False

    fmtps = _sdp_fmtps(sdp, "video")
    decodable = False
    for pt, (name, clock_rate) in rtpmaps.items():
        upper = name.upper()
        if upper == "H264":
            decodable = True
            params = dict(fmtps.get(pt) or {})
            profile = params.get("profile-level-id", _H264_DEFAULT_PROFILE).lower()
            packetization = str(params.get("packetization-mode", "0"))
            key = (profile, packetization)
            if key in _KNOWN_H264:
                continue
            _KNOWN_H264.add(key)
            CODECS["video"].append(
                RTCRtpCodecParameters(
                    mimeType="video/H264",
                    clockRate=clock_rate or 90000,
                    payloadType=_H264_LOCAL_PT,
                    parameters=params,
                    rtcpFeedback=[
                        RTCRtcpFeedback(type="nack"),
                        RTCRtcpFeedback(type="nack", parameter="pli"),
                        RTCRtcpFeedback(type="goog-remb"),
                    ],
                )
            )
            _LOGGER.info(
                "%s: accepted offered H.264 profile-level-id=%s packetization-mode=%s",
                tag,
                profile,
                packetization,
            )
        elif upper == "VP8":
            decodable = True
        elif upper != "RTX":
            _LOGGER.warning("%s: cloud offers unsupported video codec %s", tag, name)

    return decodable


class _PassthroughVideoTrack(MediaStreamTrack):
    """Tees frames to MediaRecorder while exposing the latest one as a snapshot."""

    kind = "video"

    def __init__(self, source: MediaStreamTrack, holder: VideoCapture) -> None:
        super().__init__()
        self._source = source
        self._holder = holder

    async def recv(self):
        frame = await self._source.recv()
        self._holder._latest_video_frame = frame
        return frame


def _frame_to_jpeg(frame) -> bytes | None:
    try:
        img = frame.to_image()
        buf = io.BytesIO()
        img.save(buf, format="JPEG", quality=80)
        return buf.getvalue()
    except Exception:
        _LOGGER.exception("frame→jpeg conversion failed")
        return None


async def _log_inbound_stats(pc: RTCPeerConnection, tag: str) -> None:
    """Dump per-track packetsReceived / bytesReceived from aiortc stats."""
    try:
        report = await pc.getStats()
    except Exception:
        _LOGGER.warning("%s getStats failed", tag, exc_info=True)
        return
    for stat in report.values():
        if getattr(stat, "type", "") == "inbound-rtp":
            _LOGGER.info(
                "%s inbound-rtp kind=%s codec=%s pkts=%s bytes=%s",
                tag,
                getattr(stat, "kind", "?"),
                getattr(stat, "codec", getattr(stat, "mimeType", "?")),
                getattr(stat, "packetsReceived", "?"),
                getattr(stat, "bytesReceived", "?"),
            )


async def _periodic_stats_logger(
    pc: RTCPeerConnection,
    tag: str,
    period_s: float = 3.0,
    duration_s: float = 60.0,
) -> None:
    """[Phase 1 diagnostic] Log RTP stats every N sec for the call duration so we
    can see exactly *when* audio packets first start flowing — critical for
    validating the 24000 → cloud-opens-audio hypothesis."""
    try:
        end = time.time() + duration_s
        while time.time() < end:
            await asyncio.sleep(period_s)
            try:
                report = await pc.getStats()
            except Exception:
                continue
            for stat in report.values():
                if getattr(stat, "type", "") == "inbound-rtp":
                    pkts = getattr(stat, "packetsReceived", 0) or 0
                    if pkts > 0:
                        _LOGGER.info(
                            "📊 %s [+%.0fs] inbound-rtp kind=%s codec=%s pkts=%s bytes=%s",
                            tag,
                            period_s,
                            getattr(stat, "kind", "?"),
                            getattr(stat, "codec", getattr(stat, "mimeType", "?")),
                            pkts,
                            getattr(stat, "bytesReceived", "?"),
                        )
    except asyncio.CancelledError:
        return


class VideoCapture:
    """Owns active capture state. Single instance per coordinator."""

    def __init__(self, coord: AiphoneCoordinator) -> None:
        self.coord = coord
        self._lock = asyncio.Lock()
        self._latest_video_frame: Any = None  # av.VideoFrame; lazily JPEG-encoded

        # passive (doorbell ring)
        self._passive_pc: RTCPeerConnection | None = None
        self._passive_recorder: MediaRecorder | None = None
        self._passive_path: Path | None = None
        self._passive_cid: str | None = None
        self._passive_offer_fut: asyncio.Future | None = None
        self._passive_stop: asyncio.Event | None = None

        # active (monitor)
        self._mon_pc1: RTCPeerConnection | None = None
        self._mon_pc2: RTCPeerConnection | None = None
        self._mon_recorder: MediaRecorder | None = None
        self._mon_path: Path | None = None
        self._mon_cid: str | None = None
        self._mon_task: asyncio.Task | None = None
        self._mon_fut_30021: asyncio.Future | None = None
        self._mon_fut_31001: asyncio.Future | None = None
        self._mon_fut_24021: asyncio.Future | None = None
        self._mon_fut_31011: asyncio.Future | None = None
        self._mon_fut_31012: asyncio.Future | None = None

    @property
    def recordings_dir(self) -> Path:
        d = Path(self.coord.hass.config.path(RECORDINGS_SUBDIR))
        d.mkdir(parents=True, exist_ok=True)
        return d

    async def async_live_jpeg(self) -> bytes | None:
        """Return JPEG of the most recently received video frame, or None."""
        frame = self._latest_video_frame
        if frame is None:
            return None
        return await self.coord.hass.async_add_executor_job(_frame_to_jpeg, frame)

    @property
    def latest_recording(self) -> Path | None:
        try:
            files = sorted(self.recordings_dir.glob("*.mp4"), key=lambda p: p.stat().st_mtime)
            return files[-1] if files else None
        except OSError:
            return None

    # ------------------------------------------------------------------
    # signaling routing — coordinator forwards relevant MQTT messages
    # ------------------------------------------------------------------
    def on_signal(self, mid: str, raw: dict) -> None:
        head = raw.get("HEADER", {}) or {}
        body = raw.get("BODY", {}) or {}
        cid = head.get("CID")

        # passive doorbell flow
        if cid is not None and cid == self._passive_cid:
            if mid == "31011" and self._passive_offer_fut and not self._passive_offer_fut.done():
                self._passive_offer_fut.set_result(raw)

        # active monitor flow
        if cid is not None and cid == self._mon_cid:
            if mid == "30021" and self._mon_fut_30021 and not self._mon_fut_30021.done():
                self._mon_fut_30021.set_result(raw)
            elif mid == "31001" and self._mon_fut_31001 and not self._mon_fut_31001.done():
                self._mon_fut_31001.set_result(body.get("SDP", ""))
            elif mid == "24021" and self._mon_fut_24021 and not self._mon_fut_24021.done():
                self._mon_fut_24021.set_result(raw)
            elif mid == "31011" and self._mon_fut_31011 and not self._mon_fut_31011.done():
                self._mon_fut_31011.set_result(body.get("SDP", ""))
            elif mid == "31012" and self._mon_fut_31012 and not self._mon_fut_31012.done():
                self._mon_fut_31012.set_result(head.get("RSLT"))

    # ------------------------------------------------------------------
    # PASSIVE — triggered by MID 23001 (doorbell ring)
    # ------------------------------------------------------------------
    async def passive_capture(self, cid: str, dsp1: str, source_cliid: str | None = None) -> None:
        if self._passive_pc is not None:
            if self._passive_cid == cid:
                _LOGGER.info("passive capture already running for this ring; ignoring duplicate")
                return
            _LOGGER.info("passive: new ring while the previous capture is finishing; stopping it")
            stop = self._passive_stop
            if stop is not None:
                stop.set()
        # A monitor session is lower priority than an actual doorbell ring:
        # cancel it and take the single video slot instead of skipping the ring.
        if self._lock.locked():
            _LOGGER.info("passive: pre-empting active monitor capture for incoming ring")
            await self.cancel_monitor()
        await self._lock.acquire()
        try:
            await self._passive_run(cid, dsp1, source_cliid)
        except asyncio.CancelledError:
            raise
        except Exception:
            _LOGGER.exception("passive capture failed")
        finally:
            await self._passive_teardown()
            self._lock.release()

    async def _passive_run(self, cid: str, dsp1: str, source_cliid: str | None = None) -> None:
        if self._passive_pc is not None:
            _LOGGER.warning("passive already in progress, skipping")
            return
        ts = datetime.now().strftime("%Y%m%d-%H%M%S")
        self._passive_path = self.recordings_dir / f"{ts}-{_filename_safe(dsp1 or 'ring')}.mp4"
        self._passive_cid = cid
        self._passive_stop = asyncio.Event()
        passive_t0 = time.time()  # [Phase 1] timeline anchor for audio diagnostics
        _LOGGER.info("🎥 passive capture cid=%s dsp1=%s -> %s", cid[-30:], dsp1, self._passive_path)

        pc = RTCPeerConnection(RTCConfiguration(iceServers=[]))
        pc.addTransceiver("audio", direction="sendrecv")
        pc.addTransceiver("video", direction="recvonly")
        rec = MediaRecorder(str(self._passive_path))
        @pc.on("track")
        def _on_track(track):
            elapsed = time.time() - passive_t0
            # [Phase 1] richer diagnostic on track arrival — kind, id, mimeType
            # via the receiver, transceiver direction. The MIME hints whether
            # the cloud actually offered the codec (PCMU for audio, H264 video).
            mime = "?"
            try:
                for r in pc.getReceivers():
                    if r.track is track:
                        params = r.getParameters()
                        codecs = getattr(params, "codecs", []) or []
                        if codecs:
                            mime = codecs[0].mimeType
                        break
            except Exception:
                pass
            _LOGGER.info(
                "🎥 passive track [+%.2fs] kind=%s id=%s mime=%s",
                elapsed, track.kind, getattr(track, "id", "?"), mime,
            )
            if track.kind == "audio":
                # Distinguish: track event fired but RTP may not flow until
                # cloud opens audio path (after 24000). Watch for periodic
                # stats below to confirm packets actually arrive.
                _LOGGER.info("🎤 AUDIO TRACK ATTACHED — recorder will receive frames if RTP flows")
            if track.kind == "video":
                rec.addTrack(_PassthroughVideoTrack(track, self))
            else:
                rec.addTrack(track)
        self._passive_pc = pc
        self._passive_recorder = rec

        # Step 1: 30011 with empty body (subscribe to the IXG stream in our room)
        pcliid = source_cliid or self.coord.sndtpc.rsplit("/", 1)[-1]
        self._passive_offer_fut = asyncio.get_event_loop().create_future()
        msg = {"HEADER": {
            "MID": "30011", "VER": "1.0", "SYS": "3",
            "CLIID": self.coord.cliid, "PCLIID": pcliid,
            "RCVTPC": self.coord.rcvtpc, "RLS": "0",
            "RSLT": 0, "LEN": 0, "SID": now_sid(), "CID": cid,
        }, "BODY": {}}
        self.coord.publish(msg)

        # Step 2: wait 31011 with Janus offer SDP
        try:
            offer_msg = await asyncio.wait_for(self._passive_offer_fut, timeout=8)
        except TimeoutError:
            _LOGGER.error("31011 timeout (passive)")
            return
        offer_sdp = offer_msg.get("BODY", {}).get("SDP", "")
        if not offer_sdp:
            _LOGGER.error("31011 missing SDP body")
            return
        _log_audio_section("passive janus offer", offer_sdp)
        if not _prepare_offer_video("passive janus offer", offer_sdp):
            _LOGGER.error(
                "passive: no video codec in the cloud offer can be decoded by aiortc; "
                "not subscribing"
            )
            return

        # Step 3: setRemoteDescription with actpass→active munge, createAnswer
        offer_munged = _munge_offer(offer_sdp)
        try:
            await pc.setRemoteDescription(RTCSessionDescription(offer_munged, "offer"))
            ans = await pc.createAnswer()
            await pc.setLocalDescription(ans)
            _consume_aiortc_connect_tasks()
        except Exception:
            _LOGGER.exception("passive answer setup failed")
            return
        end = time.time() + SDP_OFFER_GATHER_TIMEOUT_S
        while pc.iceGatheringState != "complete" and time.time() < end:
            await asyncio.sleep(0.1)
        answer_sdp = _strip_extra_fp(pc.localDescription.sdp)

        # Step 4: 30012 with our answer
        msg2 = {"HEADER": {
            "MID": "30012", "VER": "1.0", "SYS": "3",
            "CLIID": self.coord.cliid, "PCLIID": pcliid,
            "RCVTPC": self.coord.rcvtpc, "RLS": "0",
            "RSLT": 0, "LEN": len(answer_sdp), "SID": now_sid(), "CID": cid,
        }, "BODY": {"SDP": answer_sdp}}
        self.coord.publish(msg2)

        await rec.start()
        _LOGGER.info("🎬 passive recorder started — will stop %ss after 24002",
                     PASSIVE_HOLD_AFTER_END_S)

        # [Phase 1] periodic RTP stats so we can see when audio actually starts
        # flowing relative to the 24000 send. The task is killed at teardown.
        stats_task = asyncio.create_task(
            _periodic_stats_logger(pc, "passive", period_s=3.0, duration_s=120.0)
        )

        # Hold; coordinator's 24002 handler sets _passive_stop and we tear down.
        try:
            stop = self._passive_stop
            if stop is not None:
                # Cap at two minutes in case 24002 never arrives.
                await asyncio.wait_for(stop.wait(), timeout=120)
        except TimeoutError:
            _LOGGER.info("passive: hold cap reached (no 24002); stopping")
        finally:
            stats_task.cancel()

    async def passive_finalize(self, delay_s: int = PASSIVE_HOLD_AFTER_END_S) -> None:
        """Called from coordinator on 24002 — wait briefly then stop."""
        if self._passive_pc is None:
            return
        if delay_s:
            await asyncio.sleep(delay_s)
        stop = self._passive_stop
        if stop is not None:
            stop.set()

    async def _passive_teardown(self) -> None:
        rec, pc, path = self._passive_recorder, self._passive_pc, self._passive_path
        self._passive_pc = self._passive_recorder = None
        self._passive_path = self._passive_cid = None
        self._passive_offer_fut = None
        self._passive_stop = None
        if rec is not None:
            try:
                await rec.stop()
            except Exception:
                _LOGGER.warning("passive recorder.stop failed", exc_info=True)
        if pc is not None:
            await _log_inbound_stats(pc, "passive")
            _consume_aiortc_connect_tasks()
            try:
                await pc.close()
            except Exception:
                pass
        if path is not None and path.exists() and path.stat().st_size > 1000:
            _LOGGER.info("💾 passive recorded %s (%d B)", path, path.stat().st_size)
            async_dispatcher_send(self.coord.hass, SIGNAL_CAMERA_REFRESH)

    # ------------------------------------------------------------------
    # ACTIVE — monitor button press
    # ------------------------------------------------------------------
    def start_monitor(self, duration_s: int = MONITOR_DEFAULT_DURATION_S) -> None:
        """Start a monitor session in the background (single instance)."""
        if self._mon_task is not None and not self._mon_task.done():
            _LOGGER.info("monitor: a capture is already running")
            return
        self._mon_task = self.coord.hass.async_create_task(
            self.monitor_capture(duration_s)
        )

    async def cancel_monitor(self) -> None:
        """Cancel an in-flight monitor session and wait for its teardown."""
        task = self._mon_task
        self._mon_task = None
        if task is not None and not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

    async def monitor_capture(self, duration_s: int = MONITOR_DEFAULT_DURATION_S) -> None:
        if self._lock.locked():
            _LOGGER.info("monitor: another capture (ring) is in progress; skipping")
            return
        async with self._lock:
            try:
                await self._monitor_run(duration_s)
            except asyncio.CancelledError:
                raise
            except Exception:
                _LOGGER.exception("monitor capture failed")
            finally:
                await self._monitor_teardown()

    async def _monitor_run(self, duration_s: int) -> None:
        # IXG monitor media arrives one of two ways:
        #   1. the station sends MID 30021 with an SDP offer (direct/LAN path),
        #      which is answered with 31021, or
        #   2. 24021 reports success and 30011/31011 joins the cloud room.
        # The 24021 reply can be 480/busy while the 30021 offer is still valid,
        # so never abort just because 24021 is not 200.
        ts = datetime.now().strftime("%Y%m%d-%H%M%S")
        cid = f"{self.coord.property_id}-{self.coord.cliid}-{now_sid()}"
        self._mon_cid = cid
        self._mon_path = self.recordings_dir / f"monitor-{ts}.mp4"
        _LOGGER.info("🎥 monitor cid=%s -> %s", cid[-30:], self._mon_path)

        pc2 = RTCPeerConnection(RTCConfiguration(iceServers=[]))
        pc2.addTransceiver("audio", direction="recvonly")
        pc2.addTransceiver("video", direction="recvonly")
        rec = MediaRecorder(str(self._mon_path))
        @pc2.on("track")
        def _on_track(track):
            _LOGGER.info("🎥 monitor track: %s", track.kind)
            if track.kind == "video":
                rec.addTrack(_PassthroughVideoTrack(track, self))
            else:
                rec.addTrack(track)
        self._mon_pc2 = pc2
        self._mon_recorder = rec

        loop = asyncio.get_event_loop()
        self._mon_fut_30021 = loop.create_future()
        self._mon_fut_24021 = loop.create_future()
        self._mon_fut_31011 = loop.create_future()
        self._mon_fut_31012 = loop.create_future()

        if not self.coord.last_source_no:
            _LOGGER.warning(
                "monitor: no target station remembered yet — ring the doorbell "
                "once so the entrance station can be stored, then retry"
            )
        self.coord.publish({"HEADER": {
            "MID": "23021", "VER": "1.0", "SYS": "3",
            "PROPID": self.coord.property_id,
            "CLIID": self.coord.cliid,
            "SRCNO": self.coord.terminal_no,
            "DESTNO": self.coord.last_source_no or "",
            "TMKIND": 19, "PRI": 0, "TTIME": 0, "MTIME": 0,
            "DSP": [self.coord.termname],
            "CKIND": 1, "CID": cid,
            "RECKIND": 0, "AVID": 0,
            "SNDTPC": self.coord.sndtpc, "RCVTPC": self.coord.rcvtpc,
            "REASON": 0,
            "RSLT": 0, "LEN": 0, "SID": now_sid(),
        }})

        done, _ = await asyncio.wait(
            {self._mon_fut_30021, self._mon_fut_24021},
            timeout=45,
            return_when=asyncio.FIRST_COMPLETED,
        )
        if not done:
            _LOGGER.error(
                "monitor: no 24021/30021 after 23021 for cid=%s "
                "(SRCNO=%s DESTNO=%s)",
                cid[-30:],
                self.coord.terminal_no,
                self.coord.last_source_no or "<none>",
            )
            return

        if self._mon_fut_30021 in done:
            raw = self._mon_fut_30021.result()
            if await self._monitor_direct(pc2, rec, raw, cid):
                await asyncio.sleep(duration_s)
            return

        reply = self._mon_fut_24021.result()
        reply_head = reply.get("HEADER", {}) or {}
        rslt = reply_head.get("RSLT")
        if rslt in (404, 481):
            _LOGGER.error(
                "monitor: 24021 RSLT=%s (%s) — no target station for DESTNO=%s; "
                "ring the doorbell once so the entrance station is remembered",
                rslt,
                _REPLY_CODE_NAMES.get(rslt, "unknown"),
                self.coord.last_source_no or "<none>",
            )
            return
        if rslt in (486, 488, 603):
            _LOGGER.error(
                "monitor: 24021 RSLT=%s (%s) — the station is busy or rejected "
                "the monitor request. End any monitor/call on the station or in "
                "the app, or power-cycle the IXGW gateway. The integration sends "
                "23003/30031 on teardown to release stale sessions.",
                rslt,
                _REPLY_CODE_NAMES.get(rslt, "unknown"),
            )
            return
        if rslt != 200:
            _LOGGER.warning(
                "monitor: 24021 RSLT=%s (%s) — request SRCNO=%s DESTNO=%s; "
                "still waiting for the station's 30021 media offer",
                rslt,
                _REPLY_CODE_NAMES.get(rslt, "unknown"),
                self.coord.terminal_no,
                self.coord.last_source_no or "<none>",
            )
            try:
                raw = await asyncio.wait_for(
                    asyncio.shield(self._mon_fut_30021), timeout=50
                )
            except TimeoutError:
                _LOGGER.error(
                    "monitor: no 30021 media offer for cid=%s after 24021 RSLT=%s",
                    cid[-30:],
                    rslt,
                )
                return
            if await self._monitor_direct(pc2, rec, raw, cid):
                await asyncio.sleep(duration_s)
            return

        # 24021 = 200: join the cloud room as a receiving user.
        pcliid = (
            str(reply_head.get("CLIID") or "")
            or self.coord.last_source_cliid
            or self.coord.sndtpc.rsplit("/", 1)[-1]
        )
        self.coord.publish({"HEADER": {
            "MID": "30011", "VER": "1.0", "SYS": "3",
            "CLIID": self.coord.cliid, "PCLIID": pcliid,
            "RCVTPC": self.coord.rcvtpc, "RLS": "0",
            "RSLT": 0, "LEN": 0, "SID": now_sid(), "CID": cid,
        }, "BODY": {}})
        try:
            sdp_o2 = await asyncio.wait_for(self._mon_fut_31011, timeout=10)
        except TimeoutError:
            _LOGGER.warning(
                "monitor: 31011 subscriber offer timeout for cid=%s (PCLIID=%s); "
                "waiting for a direct 30021 offer",
                cid[-30:],
                pcliid,
            )
            try:
                raw = await asyncio.wait_for(
                    asyncio.shield(self._mon_fut_30021), timeout=50
                )
            except TimeoutError:
                _LOGGER.error("monitor: no subscriber or direct media offer")
                return
            if await self._monitor_direct(pc2, rec, raw, cid):
                await asyncio.sleep(duration_s)
            return

        if not sdp_o2:
            _LOGGER.error("monitor: 31011 had no SDP body")
            return
        _log_audio_section("monitor janus offer", sdp_o2)
        if not _prepare_offer_video("monitor janus offer", sdp_o2):
            _LOGGER.error(
                "monitor: no video codec in the cloud offer can be decoded by "
                "aiortc; aborting capture"
            )
            return
        await pc2.setRemoteDescription(
            RTCSessionDescription(_munge_offer(sdp_o2), "offer")
        )
        _consume_aiortc_connect_tasks()

        ans2 = await pc2.createAnswer()
        await pc2.setLocalDescription(
            RTCSessionDescription(_strip_extra_fp(ans2.sdp), "answer")
        )
        _consume_aiortc_connect_tasks()
        end = time.time() + SDP_OFFER_GATHER_TIMEOUT_S
        while pc2.iceGatheringState != "complete" and time.time() < end:
            await asyncio.sleep(0.1)
        answer2 = pc2.localDescription.sdp
        self.coord.publish({"HEADER": {
            "MID": "30012", "VER": "1.0", "SYS": "3",
            "CLIID": self.coord.cliid, "PCLIID": pcliid,
            "RCVTPC": self.coord.rcvtpc, "RLS": "0",
            "RSLT": 0, "LEN": len(answer2), "SID": now_sid(), "CID": cid,
        }, "BODY": {"SDP": answer2}})
        try:
            await asyncio.wait_for(self._mon_fut_31012, timeout=6)
        except TimeoutError:
            _LOGGER.warning("31012 timeout (non-fatal)")

        await rec.start()
        _LOGGER.info("🎬 monitor room recording, holding %ss", duration_s)
        await asyncio.sleep(duration_s)

    async def _monitor_direct(
        self, pc2: RTCPeerConnection, rec: MediaRecorder, raw: dict, cid: str
    ) -> bool:
        """Answer the station's 30021 SDP offer with 31021."""
        head = raw.get("HEADER", {}) or {}
        sdp = (raw.get("BODY") or {}).get("SDP", "")
        if not sdp:
            _LOGGER.error("monitor: direct 30021 had no SDP body")
            return False
        _log_audio_section("monitor direct offer", sdp)
        if not _prepare_offer_video("monitor direct offer", sdp):
            _LOGGER.error(
                "monitor: direct 30021 offer has no video codec aiortc can decode"
            )
            return False
        try:
            await pc2.setRemoteDescription(
                RTCSessionDescription(_munge_offer(sdp), "offer")
            )
            _consume_aiortc_connect_tasks()
            ans = await pc2.createAnswer()
            await pc2.setLocalDescription(
                RTCSessionDescription(_strip_extra_fp(ans.sdp), "answer")
            )
            _consume_aiortc_connect_tasks()
        except Exception:
            _LOGGER.exception("monitor: direct 30021 answer failed")
            return False
        end = time.time() + SDP_OFFER_GATHER_TIMEOUT_S
        while pc2.iceGatheringState != "complete" and time.time() < end:
            await asyncio.sleep(0.1)
        answer = pc2.localDescription.sdp
        pcliid = str(head.get("CLIID") or self.coord.last_source_cliid or "")
        self.coord.publish({"HEADER": {
            "MID": "31021", "VER": "1.0", "SYS": "3",
            "CLIID": self.coord.cliid, "PCLIID": pcliid,
            "CID": cid, "RSLT": 200, "REASON": 0, "RLS": "0",
            "SNDTPC": self.coord.sndtpc,
            "RCVTPC": head.get("SNDTPC") or self.coord.rcvtpc,
            "MEDIATYPE": head.get("MEDIATYPE", 3),
            "LEN": len(answer), "SID": now_sid(),
        }, "BODY": {"SDP": answer}})
        _LOGGER.info("🎬 monitor direct 31021 sent (station offer answered)")
        try:
            await rec.start()
        except Exception:
            _LOGGER.warning("monitor: recorder start failed", exc_info=True)
            return False
        asyncio.create_task(
            _periodic_stats_logger(pc2, "monitor direct", period_s=3.0, duration_s=60.0)
        )
        return True

    async def aclose(self) -> None:
        """Coordinator shutdown — drop any in-flight capture cleanly."""
        stop = self._passive_stop
        if stop is not None:
            stop.set()
        await self.cancel_monitor()
        await self._passive_teardown()
        await self._monitor_teardown()

    async def _monitor_teardown(self) -> None:
        rec, pc1, pc2, path = self._mon_recorder, self._mon_pc1, self._mon_pc2, self._mon_path
        cid = self._mon_cid
        self._mon_pc1 = self._mon_pc2 = self._mon_recorder = None
        self._mon_path = self._mon_cid = None
        self._mon_fut_31001 = self._mon_fut_24021 = None
        self._mon_fut_30021 = None
        self._mon_fut_31011 = self._mon_fut_31012 = None
        if cid:
            # Free the station's monitor session (the app sends 23003/30031 on hangup).
            self.coord.publish({"HEADER": {
                "MID": "23003", "VER": "1.0", "SYS": "3",
                "CLIID": self.coord.cliid, "CID": cid,
                "RSLT": 200, "REASON": 0, "LEN": 0, "SID": now_sid(),
                "SNDTPC": self.coord.sndtpc, "RCVTPC": self.coord.rcvtpc,
            }, "BODY": {}})
            self.coord.publish({"HEADER": {
                "MID": "30031", "VER": "1.0", "SYS": "3",
                "CLIID": self.coord.cliid,
                "PCLIID": self.coord.last_source_cliid or "",
                "CID": cid, "RCVTPC": self.coord.rcvtpc, "RLS": "0",
                "RSLT": 200, "REASON": 0, "LEN": 0, "SID": now_sid(),
            }, "BODY": {}})
        if rec is not None:
            try:
                await rec.stop()
            except Exception:
                _LOGGER.warning("monitor recorder.stop failed", exc_info=True)
        if pc2 is not None:
            await _log_inbound_stats(pc2, "monitor")
        _consume_aiortc_connect_tasks()
        for pc in (pc2, pc1):
            if pc is not None:
                try:
                    await pc.close()
                except Exception:
                    pass
        if path is not None and path.exists() and path.stat().st_size > 1000:
            _LOGGER.info("💾 monitor recorded %s (%d B)", path, path.stat().st_size)
            async_dispatcher_send(self.coord.hass, SIGNAL_CAMERA_REFRESH)
