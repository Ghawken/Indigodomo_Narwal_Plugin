"""Narwal Robot Vacuum plugin for Indigo home automation.

Talks to Narwal vacuums over the local WebSocket API using the vendored,
upstream-verbatim ``narwal_client`` library (from sjmotew/NarwalIntegration).
The library is fully async; this plugin runs a single asyncio event loop in a
daemon thread alongside Indigo's own threading model — the same pattern used by
the WeatherFlow Tempest and Dreame plugins.

Threading model
---------------
* ``startup()`` starts an asyncio loop with ``run_forever`` in a daemon thread.
* Per Indigo device we create one ``NarwalClient`` and one ``start_listening``
  task, scheduled with ``asyncio.run_coroutine_threadsafe``.
* The client's ``on_state_update`` push callback runs *in the async thread* and
  writes Indigo states directly — Indigo's device/state API is thread-safe.
* Heavy work (Pillow map rendering) is deliberately kept OFF the event loop:
  ``runConcurrentThread`` (Indigo's own thread) does the rendering on a throttle,
  plus acts as a watchdog that restarts any listener task that has died.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import threading
import time
from datetime import datetime
from typing import Any, Callable, Coroutine

import indigo  # type: ignore

# Vendored, upstream-verbatim client library. The plugin's "Server Plugin"
# folder is on sys.path, so this imports the bundled package. Third-party deps
# (websockets, bbpb, Pillow) are auto-installed by Indigo from requirements.txt.
from narwal_client import (
    NarwalClient,
    NarwalCommandError,
    NarwalConnectionError,
    NarwalState,
    FanLevel,
    MopHumidity,
    WorkingStatus,
)
import narwal_map


class Plugin(indigo.PluginBase):
    """Narwal robot vacuum plugin."""

    def __init__(self, pluginId, pluginDisplayName, pluginVersion, pluginPrefs):
        super().__init__(pluginId, pluginDisplayName, pluginVersion, pluginPrefs)

        # --- Logging: file handler gets a detailed formatter; each handler
        #     filters at its own configured level. Logger itself is DEBUG so
        #     both handlers see everything and decide independently. ---
        pfmt = logging.Formatter(
            "%(asctime)s.%(msecs)03d\t%(levelname)s\t%(name)s.%(funcName)s:\t%(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        self.plugin_file_handler.setFormatter(pfmt)
        self.logger.setLevel(logging.DEBUG)
        self._apply_log_levels(pluginPrefs)

        # --- Async runtime ---
        self._event_loop: asyncio.AbstractEventLoop | None = None
        self._async_thread: threading.Thread | None = None

        # dev.id -> NarwalClient
        self._clients: dict[int, NarwalClient] = {}
        # dev.id -> asyncio.Task running start_listening()
        self._listen_tasks: dict[int, asyncio.Task] = {}
        # dev.id -> True while a (re)connect coroutine is in flight
        self._connecting: set[int] = set()

        # dev.id -> monotonic time of last map render (render throttle)
        self._last_map_render: dict[int, float] = {}
        # dev.id -> monotonic time of last fallback status poll
        self._last_status_poll: dict[int, float] = {}
        # dev.id -> monotonic time of last get_map recovery fetch
        self._last_map_fetch: dict[int, float] = {}
        # dev.id -> monotonic time the robot was last actively cleaning (drives
        # the "clear overlay after idle" behaviour).
        self._last_active: dict[int, float] = {}
        # dev.id -> monotonic time of last full-state debug dump
        self._last_full_log: dict[int, float] = {}

        # Map rendering support:
        # dev.id -> list of (grid_x, grid_y) robot positions this cleaning session
        self._trail: dict[int, list[tuple[float, float]]] = {}
        # dev.id -> (signature, cached base dict) so the static floor plan
        # is only re-rendered when the map itself changes.
        self._base_map_cache: dict[int, tuple[Any, Any]] = {}
        # dev.id -> set of cleaned-cell indices (display_map field 7) accumulated
        # this session for the swept-area overlay. Index = y*mapWidth + x.
        self._cleaned_cells: dict[int, set] = {}
        # One-shot: next display_map broadcast dumps its full structure to the log.
        self._dump_map_next = False
        # dev.id -> current robot session_id (per power-cycle, not per-clean).
        self._session: dict[int, str] = {}
        # dev.id -> last cleaning_time seen. A NEW clean is detected when this
        # resets/decreases (session_id stays constant across cleans on this robot).
        self._clean_time: dict[int, int] = {}

        # dev.id -> previous snapshot for edge-triggered events
        self._prev: dict[int, dict[str, Any]] = {}

    # ------------------------------------------------------------------ #
    # Logging helpers                                                     #
    # ------------------------------------------------------------------ #
    def _apply_log_levels(self, prefs) -> None:
        try:
            self.logLevel = int(prefs.get("showDebugLevel", logging.INFO))
        except (ValueError, TypeError):
            self.logLevel = logging.INFO
        try:
            file_level = int(prefs.get("showDebugFileLevel", logging.DEBUG))
        except (ValueError, TypeError):
            file_level = logging.DEBUG
        self.indigo_log_handler.setLevel(self.logLevel)
        self.plugin_file_handler.setLevel(file_level)

    # ------------------------------------------------------------------ #
    # Plugin lifecycle                                                    #
    # ------------------------------------------------------------------ #
    def startup(self):
        self.logger.info("Narwal plugin starting")
        self._event_loop = asyncio.new_event_loop()
        self._async_thread = threading.Thread(
            target=self._run_event_loop, name="NarwalAsyncLoop", daemon=True
        )
        self._async_thread.start()

    def _run_event_loop(self):
        assert self._event_loop is not None
        asyncio.set_event_loop(self._event_loop)
        self._event_loop.run_forever()

    def shutdown(self):
        self.logger.info("Narwal plugin shutting down")
        loop = self._event_loop
        if loop and loop.is_running():
            # Disconnect every client, then stop the loop. Scheduled via
            # call_soon_threadsafe so we never await from Indigo's thread.
            for dev_id, client in list(self._clients.items()):
                task = self._listen_tasks.get(dev_id)
                if task and not task.done():
                    loop.call_soon_threadsafe(task.cancel)
                asyncio.run_coroutine_threadsafe(client.disconnect(), loop)
            loop.call_soon_threadsafe(loop.stop)

    # ------------------------------------------------------------------ #
    # Indigo device lifecycle                                             #
    # ------------------------------------------------------------------ #
    def deviceStartComm(self, dev):
        # Sync state list from Devices.xml (picks up new states after updates).
        dev.stateListOrDisplayStateIdChanged()
        if dev.deviceTypeId != "narwalVacuum":
            return
        dev.setErrorStateOnServer(None)
        dev.updateStateOnServer("connected", False)
        dev.updateStateOnServer("statusMessage", "Connecting…")
        self._prev[dev.id] = {}
        self._schedule_connect(dev.id)

    def deviceStopComm(self, dev):
        if dev.deviceTypeId != "narwalVacuum":
            return
        loop = self._event_loop
        task = self._listen_tasks.pop(dev.id, None)
        client = self._clients.pop(dev.id, None)
        if loop and loop.is_running():
            if task and not task.done():
                loop.call_soon_threadsafe(task.cancel)
            if client:
                asyncio.run_coroutine_threadsafe(client.disconnect(), loop)
        self._last_map_render.pop(dev.id, None)
        self._last_status_poll.pop(dev.id, None)
        self._last_map_fetch.pop(dev.id, None)
        self._last_active.pop(dev.id, None)
        self._last_full_log.pop(dev.id, None)
        self._prev.pop(dev.id, None)
        self._trail.pop(dev.id, None)
        self._base_map_cache.pop(dev.id, None)
        self._cleaned_cells.pop(dev.id, None)
        self._session.pop(dev.id, None)
        self._clean_time.pop(dev.id, None)
        self.logger.info("%s: comm stopped", dev.name)

    def _schedule_connect(self, dev_id: int) -> None:
        loop = self._event_loop
        if not loop or not loop.is_running():
            return
        if dev_id in self._connecting:
            return
        self._connecting.add(dev_id)
        asyncio.run_coroutine_threadsafe(self._connect_device(dev_id), loop)

    async def _connect_device(self, dev_id: int) -> None:
        """Create a client, fetch initial state, then start the push listener."""
        loop = self._event_loop
        assert loop is not None
        try:
            dev = indigo.devices[dev_id]
            props = dev.pluginProps
            host = (props.get("host") or "").strip()
            if not host:
                self.logger.error("%s: no IP address configured", dev.name)
                dev.setErrorStateOnServer("no IP")
                return
            try:
                port = int(props.get("port", 9002))
            except (ValueError, TypeError):
                port = 9002
            device_id = (props.get("deviceId") or "").strip()
            topic_prefix = self._normalise_prefix(props.get("topicPrefix"))

            # Keep the previous client's map so a reconnect (e.g. after a dock
            # visit) whose get_map fails doesn't lose it and stall rendering.
            prev_client = self._clients.get(dev_id)
            prev_map = (prev_client.state.map_data
                        if prev_client and getattr(prev_client, "state", None) else None)

            client = NarwalClient(
                host=host, port=port, device_id=device_id, topic_prefix=topic_prefix
            )
            self._clients[dev_id] = client

            self.logger.info("%s: connecting to %s:%d", dev.name, host, port)
            await client.connect()

            if not client.device_id:
                self.logger.info("%s: discovering device ID…", dev.name)
                try:
                    await client.discover_device_id()
                except NarwalCommandError as ex:
                    self.logger.warning("%s: device ID discovery failed: %s", dev.name, ex)

            # Fetch initial state BEFORE the listener starts (no concurrent recv).
            # Each is best-effort: the robot may be asleep on the dock.
            for label, coro in (
                ("device info", client.get_device_info()),
                ("status", client.get_status(full_update=True)),
                ("map", client.get_map()),
            ):
                try:
                    await coro
                except Exception:
                    self.logger.debug("%s: initial %s fetch failed (robot asleep?)", dev.name, label)

            try:
                await client.subscribe_to_topics()
            except Exception:
                self.logger.debug("%s: topic subscription failed at startup", dev.name)

            # If get_map didn't succeed on this (re)connect, reuse the last-known
            # map so the renderer keeps working (recovery poll re-fetches later).
            if (not client.state.map_data or not client.state.map_data.compressed_map) and prev_map:
                client.state.map_data = prev_map
                self.logger.info("%s: kept previous map (get_map unavailable this connect)", dev.name)

            # Restore the last cleaning trail + swept area from disk so they're
            # visible immediately after a plugin reload (like re-opening the app).
            if dev_id not in self._trail:
                self._trail[dev_id] = self._load_trail(dev)
            if dev_id not in self._cleaned_cells:
                self._cleaned_cells[dev_id] = self._load_cleaned(dev)

            # Push updates from now on land in _on_state_update (async thread).
            client.on_state_update = lambda state, i=dev_id: self._on_state_update(i, state)
            # Raw-message hook is always attached but stays cheap: it only decodes
            # a display_map when the overlay is enabled or a structure dump is
            # pending (a topic-string check otherwise).
            client.on_message = lambda msg, i=dev_id: self._on_raw_message(i, msg)
            self._update_device_states(dev_id, client.state)

            self._listen_tasks[dev_id] = loop.create_task(client.start_listening())
            self.logger.info(
                "%s: connected — status=%s battery=%d%% awake=%s",
                dev.name,
                client.state.working_status.name,
                client.state.battery_level,
                client.robot_awake,
            )
        except NarwalConnectionError as ex:
            self.logger.error("%s: connection failed: %s", indigo.devices[dev_id].name, ex)
            try:
                indigo.devices[dev_id].setErrorStateOnServer("offline")
            except Exception:
                pass
        except Exception:
            self.logger.exception("%s: unexpected error during connect", dev_id)
        finally:
            self._connecting.discard(dev_id)

    @staticmethod
    def _normalise_prefix(value) -> str | None:
        value = (value or "").strip()
        if not value:
            return None
        return value if value.startswith("/") else "/" + value

    # ------------------------------------------------------------------ #
    # State mapping (called from the async thread — Indigo API is safe)   #
    # ------------------------------------------------------------------ #
    def _on_state_update(self, dev_id: int, state: NarwalState) -> None:
        try:
            self._check_new_clean(dev_id, state)
            self._update_device_states(dev_id, state)
            self._sample_trail(dev_id, state)
            self._evaluate_triggers(dev_id, state)
        except Exception:
            self.logger.exception("%s: error handling state update", dev_id)

    def _check_new_clean(self, dev_id: int, state: NarwalState) -> None:
        """Reset coverage/trail when a genuinely new clean starts.

        The robot's session_id is per power-cycle (constant across many cleans),
        so we key primarily on cleaning_time resetting/decreasing — it counts up
        within a clean and returns to ~0 for a new one. A mid-clean vacuum->mop
        dock visit does NOT reset cleaning_time, so coverage is preserved then."""
        sid = state.session_id
        ct = state.cleaning_time
        prev_sid = self._session.get(dev_id)
        prev_ct = self._clean_time.get(dev_id)
        self._session[dev_id] = sid or prev_sid
        self._clean_time[dev_id] = ct

        new_clean = False
        if prev_sid is not None and sid and sid != prev_sid:
            new_clean = True
        # cleaning_time went backwards by a clear margin -> a new clean began
        if prev_ct is not None and ct is not None and ct + 30 < prev_ct:
            new_clean = True
        if new_clean:
            self._reset_coverage(dev_id, "new clean detected")

    def _reset_coverage(self, dev_id: int, reason: str) -> None:
        dev = indigo.devices.get(dev_id)
        self.logger.info("%s: %s — resetting coverage & trail",
                         dev.name if dev else dev_id, reason)
        self._trail[dev_id] = []
        self._cleaned_cells[dev_id] = set()
        self._last_map_render.pop(dev_id, None)

    def _sample_trail(self, dev_id: int, state: NarwalState) -> None:
        """Append the robot's current grid position to this session's trail.

        Runs on every display_map broadcast (~1.5 s while cleaning), so the trail
        is far smoother than sampling only at render time. Points are stored in
        grid coordinates and de-duplicated / bounded.
        """
        md = state.map_data
        disp = state.map_display_data
        if not md or disp is None:
            return
        gc = disp.to_grid_coords(md.resolution, md.origin_x, md.origin_y)
        if gc is None:
            return
        gx, gy = gc
        if not (0 <= gx < md.width and 0 <= gy < md.height):
            return
        trail = self._trail.setdefault(dev_id, [])
        if trail:
            lx, ly = trail[-1]
            if abs(gx - lx) < 1.0 and abs(gy - ly) < 1.0:
                return  # hasn't moved a full grid cell — skip duplicate
        trail.append((gx, gy))
        if len(trail) > 3000:
            del trail[: len(trail) - 3000]

    def _on_raw_message(self, dev_id: int, msg) -> None:
        """Handle a raw display_map broadcast.

        Always decodes the cleaned-cell indices (field 7) — they drive both the
        live cleaned-area figure AND the swept overlay. The overlay checkbox only
        controls whether the cells are *drawn*, not whether they're counted.
        """
        try:
            if getattr(msg, "short_topic", "") != "map/display_map":
                return
            import blackboxprotobuf
            decoded, _ = blackboxprotobuf.decode_message(msg.payload)

            if self._dump_map_next:
                self._dump_map_next = False
                self._dump_display_map(dev_id, msg.payload, decoded)

            indices = self._decode_cleaned_indices(decoded)
            if indices:
                self._cleaned_cells.setdefault(dev_id, set()).update(indices)
        except Exception:
            self.logger.debug("%s: raw display_map handling failed", dev_id, exc_info=True)

    def _cleaned_area_m2(self, dev_id: int, state: NarwalState) -> float:
        """Cleaned area in m² from working_status field 2 (coveredArea, float32,
        already m²) — upstream PR #51. This is the robot's own figure and matches
        the Narwal app; it grows through the clean (e.g. 3.3 -> 18.0 m²). The
        vendored library instead reads field 13, a station-dry timer stuck at
        18000 (=1.8 m²) — that's the wrong one. Falls back to the distinct
        cleaned-cell count, then field 13, only if field 2 is absent."""
        area = self._f32(self._d(state.raw_working_status).get("2"))
        if area is not None and 0 <= area < 100000:
            return round(area, 2)
        cells = self._cleaned_cells.get(dev_id)
        res = state.map_data.resolution if state.map_data else 0
        if cells and res > 0:
            return round(len(cells) * (res / 1000.0) ** 2, 2)
        return round(state.cleaning_area / 10000.0, 2)

    def _decode_cleaned_indices(self, decoded: dict) -> list:
        """Decode display_map field 7 into cleaned-cell indices.

        Field 7.3 is zlib-compressed; the decompressed payload is a protobuf whose
        field 1 (tag 0x0a) is packed varints — linear indices into the map grid
        (index = y*mapWidth + x). Confirmed against a live Flow 2 (values 66056–
        78289 for a 284x363 map; deltas dominated by 283/284 = one map row).
        """
        import zlib

        f7 = decoded.get("7")
        if not isinstance(f7, dict):
            return []
        comp = f7.get("3", b"")
        if isinstance(comp, str):
            comp = comp.encode("latin-1")
        if not comp:
            return []
        try:
            d = zlib.decompress(comp)
        except Exception:
            return []
        if not d or d[0] != 0x0A:  # expect field 1, wire type 2
            return []
        length, j = self._read_varint(d, 1)
        chunk = d[j:j + length]
        indices = []
        k = 0
        while k < len(chunk):
            v, k = self._read_varint(chunk, k)
            indices.append(v)
        return indices

    def _dump_display_map(self, dev_id: int, payload: bytes, decoded: dict) -> None:
        """Log the full structure of a display_map message so the swept-area
        encoding can be identified. Also probes every bytes field for zlib data."""
        import binascii
        import zlib

        def describe(val, depth=0):
            if isinstance(val, dict):
                if depth >= 4:
                    return f"dict(keys={list(val.keys())})"
                return {k: describe(v, depth + 1) for k, v in val.items()}
            if isinstance(val, list):
                return f"list(len={len(val)}, item0={describe(val[0], depth + 1) if val else None})"
            if isinstance(val, (bytes, bytearray)):
                return f"bytes(len={len(val)}, head={binascii.hexlify(bytes(val[:12])).decode()})"
            if isinstance(val, str):
                return f"str(len={len(val)}, {val[:40]!r})"
            return f"{type(val).__name__}={val}"

        self.logger.info("=== display_map dump (dev %s) — payload %d bytes ===", dev_id, len(payload))
        # Log the map coordinate frame so index/float values can be interpreted.
        try:
            md = self._clients[dev_id].state.map_data
            if md:
                self.logger.info(
                    "  map frame: %dx%d  resolution=%s  origin=(%d,%d)  dock=(%s,%s)  rooms=%d",
                    md.width, md.height, md.resolution, md.origin_x, md.origin_y,
                    md.dock_x, md.dock_y, len(md.rooms),
                )
        except Exception:
            pass
        for key in sorted(decoded, key=lambda k: int(k) if str(k).isdigit() else 0):
            self.logger.info("  field %s: %s", key, describe(decoded[key]))

        # Probe every bytes value: compression, packed-varint content, float32 arrays.
        def walk(val, path="") -> None:
            if isinstance(val, dict):
                for k, v in val.items():
                    walk(v, f"{path}.{k}" if path else str(k))
            elif isinstance(val, list):
                for i, v in enumerate(val[:3]):
                    walk(v, f"{path}[{i}]")
            elif isinstance(val, (bytes, bytearray)) and len(val) >= 8:
                self._probe_bytes(path, bytes(val))

        walk(decoded)
        self.logger.info("=== end display_map dump ===")

    @staticmethod
    def _read_varint(buf: bytes, i: int) -> tuple[int, int]:
        shift = 0
        result = 0
        while i < len(buf):
            b = buf[i]
            i += 1
            result |= (b & 0x7F) << shift
            if not (b & 0x80):
                break
            shift += 7
        return result, i

    def _probe_bytes(self, path: str, b: bytes) -> None:
        """Log the likely meaning of a bytes field: zlib+packed-varints (sparse
        cleaned-cell indices) or a float32 coordinate array."""
        import binascii
        import struct
        import zlib

        magic = binascii.hexlify(b[:4]).decode()

        # zlib? -> decompress and try to read field-1 packed varints
        try:
            d = zlib.decompress(b)
        except Exception:
            d = None

        if d is not None:
            info = f"len={len(b)} magic={magic} -> zlib OK, decompressed={len(d)} bytes"
            try:
                if d and d[0] == 0x0A:  # field 1, wire type 2 (length-delimited)
                    ln, j = self._read_varint(d, 1)
                    chunk = d[j:j + ln]
                    vals = []
                    k = 0
                    while k < len(chunk):
                        v, k = self._read_varint(chunk, k)
                        vals.append(v)
                    if vals:
                        deltas = [vals[i + 1] - vals[i] for i in range(min(len(vals) - 1, 20))]
                        info += (
                            f"; field1 packed varints: n={len(vals)} min={min(vals)} "
                            f"max={max(vals)} first10={vals[:10]} last5={vals[-5:]} "
                            f"first_deltas={deltas}; trailing_bytes={len(d) - (j + ln)}"
                        )
            except Exception:
                info += " (packed-varint parse failed)"
            self.logger.info("  bytes at field %s: %s", path, info)
            return

        # not zlib: float32 array? (length multiple of 4, values in a sane range)
        if len(b) % 4 == 0 and len(b) <= 4096:
            try:
                floats = struct.unpack(f"<{len(b) // 4}f", b)
                sample = [round(f, 2) for f in floats[:8]]
                self.logger.info(
                    "  bytes at field %s: len=%d magic=%s -> float32[%d] first8=%s",
                    path, len(b), magic, len(floats), sample,
                )
                return
            except Exception:
                pass
        self.logger.info("  bytes at field %s: len=%d magic=%s (unrecognised)", path, len(b), magic)

    # ------------------------------------------------------------------ #
    # Trail persistence                                                   #
    # ------------------------------------------------------------------ #
    def _trail_file(self, dev) -> str:
        safe = "".join(c for c in str(dev.id) if c.isalnum())
        return os.path.join(self._map_output_dir(dev), f"narwal_trail_{safe}.json")

    def _load_trail(self, dev) -> list:
        try:
            with open(self._trail_file(dev), "r") as fh:
                data = json.load(fh)
            return [(float(x), float(y)) for x, y in data][-3000:]
        except (OSError, ValueError, TypeError):
            return []

    def _save_trail(self, dev, trail: list) -> None:
        try:
            path = self._trail_file(dev)
            os.makedirs(os.path.dirname(path), exist_ok=True)
            tmp = path + ".tmp"
            with open(tmp, "w") as fh:
                json.dump([[round(x, 2), round(y, 2)] for x, y in trail], fh)
            os.replace(tmp, path)
        except OSError:
            self.logger.debug("%s: could not save trail", dev.name)

    def _cleaned_file(self, dev) -> str:
        safe = "".join(c for c in str(dev.id) if c.isalnum())
        return os.path.join(self._map_output_dir(dev), f"narwal_cleaned_{safe}.json")

    def _load_cleaned(self, dev) -> set:
        try:
            with open(self._cleaned_file(dev), "r") as fh:
                return set(int(i) for i in json.load(fh))
        except (OSError, ValueError, TypeError):
            return set()

    def _save_cleaned(self, dev, cells) -> None:
        try:
            path = self._cleaned_file(dev)
            os.makedirs(os.path.dirname(path), exist_ok=True)
            tmp = path + ".tmp"
            with open(tmp, "w") as fh:
                json.dump(list(cells), fh)
            os.replace(tmp, path)
        except OSError:
            self.logger.debug("%s: could not save swept area", dev.name)

    @staticmethod
    def _derive_flags(state: NarwalState):
        """Robustly derive activity flags.

        The robot keeps working_status=CLEANING while docked servicing (mop wash
        etc.), and the library's is_docked returns False for any CLEANING status.
        But the dock fields DO flip when physically docked (confirmed on Flow 2
        fw v01.07.10.33: field11 1->3, field47 2->1). So we trust the dock fields
        to override a stale CLEANING status.
        """
        physically_docked = state.dock_field11 >= 2 or state.dock_field47 in (1, 3)
        is_docked = state.is_docked or physically_docked
        is_returning = state.is_returning and not physically_docked
        is_cleaning = state.is_cleaning and not physically_docked and not is_returning
        is_charging = is_docked and state.battery_level < 100
        return is_cleaning, is_docked, is_charging, is_returning, physically_docked

    # Enum tables aligned with upstream PR #50 (WorkMode/FanLevel/MopHumidity are
    # 1-indexed in the CleanParam schema, distinct from the older 0-indexed enums
    # in the vendored client). See _parse_clean_config.
    _WORK_MODE = {1: "Vacuum", 2: "Mop", 3: "Vacuum then Mop", 4: "Vacuum & Mop"}
    _FAN_LEVELS = {1: "Mute", 2: "Normal", 3: "Strong", 4: "Deep", 5: "Super"}
    _WATER_LEVELS = {1: "Dry", 2: "Normal", 3: "Wet"}
    # base_status tank/bin states (PR #52): 1=OK, 2/3/4=problem.
    _WATER_TANK = {1: "OK", 2: "Empty", 3: "Abnormal", 4: "Not installed"}
    _BIN_STATE = {1: "OK", 2: "Full", 3: "Abnormal", 4: "Not installed"}

    @staticmethod
    def _f32(v):
        """Interpret an int (or float) as an IEEE-754 float32."""
        import struct
        if isinstance(v, float):
            return v
        if isinstance(v, int):
            try:
                return struct.unpack("<f", struct.pack("<I", v & 0xFFFFFFFF))[0]
            except (struct.error, ValueError):
                return None
        return None

    @staticmethod
    def _parse_clean_config(raw_base_status: dict) -> dict:
        """Extract the running clean config from base_status field 48.1[0].5.1.

        Schema (upstream PR #50 CleanParam): {1: work_mode, 2: fan, 3: mop_strength,
        4: water}. Confirmed on Flow 2: {1:3, 2:2} → work_mode 3 = 'Vacuum then Mop'
        (matches the user's routine), fan 2 = 'Normal'. Returns {} if absent."""
        try:
            cfg = raw_base_status["48"]["1"][0]["5"]["1"]
        except (KeyError, IndexError, TypeError):
            return {}
        out = {}
        for fld, name in (("1", "mode"), ("2", "fan"), ("3", "mop_strength"), ("4", "water")):
            v = cfg.get(fld)
            if isinstance(v, int):
                out[name] = v
        return out

    def _update_device_states(self, dev_id: int, state: NarwalState) -> None:
        dev = indigo.devices.get(dev_id)
        if dev is None:
            return
        client = self._clients.get(dev_id)

        # No real data yet (robot asleep at connect: empty base/working status).
        # Don't blank out the device's good states with UNKNOWN/0 — just reflect
        # the connection and wait for the first real broadcast.
        if (not state.raw_base_status and not state.raw_working_status
                and state.working_status == WorkingStatus.UNKNOWN):
            kv = [{"key": "connected", "value": bool(client and client.connected)}]
            if client is not None:
                kv.append({"key": "robotAwake", "value": client.robot_awake})
            self._safe_update(dev, kv)
            return

        is_cleaning, is_docked, is_charging, is_returning, at_dock = self._derive_flags(state)
        rooms = state.map_data.rooms if state.map_data else []
        room_names = ", ".join(r.display_name for r in rooms)

        cfg = self._parse_clean_config(state.raw_base_status)
        fan_name = self._fan_name(state.raw_base_status)
        mop_name = self._mop_name(state.raw_base_status)
        clean_mode = self._clean_mode_name(cfg)
        area_m2 = self._cleaned_area_m2(dev_id, state)
        current_room = self._current_room_name(state)
        progress = self._cleaning_progress(state)
        station_activity = self._station_activity(state.raw_base_status)
        user_action = self._user_action(state.raw_base_status)

        disp = state.map_display_data
        key_values = [
            {"key": "statusMessage",
             "value": self._status_message(state, is_cleaning, is_docked, is_returning,
                                           is_charging, at_dock, clean_mode, area_m2,
                                           progress, station_activity)},
            {"key": "workingStatus", "value": state.working_status.name},
            {"key": "workingStatusCode", "value": int(state.working_status)},
            {"key": "batteryLevel", "value": int(state.battery_level),
             "uiValue": f"{int(state.battery_level)}%"},
            {"key": "isCleaning", "value": is_cleaning},
            {"key": "isDocked", "value": is_docked},
            {"key": "isCharging", "value": is_charging},
            {"key": "atDock", "value": at_dock},
            {"key": "isPaused", "value": bool(state.is_paused) and not at_dock},
            {"key": "isReturning", "value": is_returning},
            {"key": "cleaningArea", "value": area_m2, "uiValue": f"{area_m2} m²"},
            {"key": "cleaningTime", "value": int(state.cleaning_time // 60),
             "uiValue": f"{int(state.cleaning_time // 60)} min"},
            {"key": "cleaningMode", "value": clean_mode},
            {"key": "currentRoom", "value": current_room},
            {"key": "cleaningProgress", "value": progress if progress is not None else 0,
             "uiValue": f"{progress}%" if progress is not None else ""},
            {"key": "stationActivity", "value": station_activity},
            {"key": "userAction", "value": user_action},
            {"key": "fanSpeed", "value": fan_name},
            {"key": "mopHumidity", "value": mop_name},
            {"key": "firmwareVersion", "value": state.firmware_version or ""},
            {"key": "roomCount", "value": len(rooms)},
            {"key": "roomList", "value": room_names},
            {"key": "lastUpdate", "value": datetime.now().strftime("%Y-%m-%d %H:%M:%S")},
            {"key": "onOffState", "value": is_cleaning},
        ]
        if state.device_info:
            key_values.append({"key": "deviceId", "value": state.device_info.device_id})
            key_values.append({"key": "productKey", "value": state.device_info.product_key})
        if client is not None:
            key_values.append({"key": "connected", "value": client.connected})
            key_values.append({"key": "robotAwake", "value": client.robot_awake})
        if disp is not None:
            key_values.append({"key": "robotX", "value": round(disp.robot_x, 2)})
            key_values.append({"key": "robotY", "value": round(disp.robot_y, 2)})
            key_values.append({"key": "robotHeading", "value": round(disp.robot_heading, 1)})

        key_values.extend(self._station_states(state))
        self._safe_update(dev, key_values)

        # Battery icon on the device (relay devices show a battery indicator).
        try:
            dev.updateStateImageOnServer(
                indigo.kStateImageSel.SensorOn if is_cleaning
                else indigo.kStateImageSel.SensorOff
            )
        except Exception:
            pass

    @staticmethod
    def _status_message(state, is_cleaning, is_docked, is_returning, is_charging,
                        at_dock, clean_mode="", area_m2=0.0, progress=None,
                        station_activity="") -> str:
        if at_dock:
            # The robot reports CLEANING while docked servicing (mop wash etc.)
            # mid-task; show the specific station activity when we can identify it.
            if station_activity:
                return f"At dock — {station_activity}"
            if state.working_status in (WorkingStatus.CLEANING, WorkingStatus.CLEANING_ALT):
                return "At dock — servicing"
            if state.battery_level >= 100:
                return "Charged"
            if is_charging:
                return f"Charging ({state.battery_level}%)"
            return f"Docked ({state.battery_level}%)"
        if state.is_paused and is_cleaning:
            return "Paused"
        if is_returning:
            return "Returning to dock"
        if is_cleaning:
            parts = ["Cleaning"]
            if clean_mode and clean_mode not in ("", "—"):
                parts.append(clean_mode)
            if progress is not None:
                parts.append(f"{progress}%")
            parts.append(f"{area_m2:g} m²")
            return " · ".join(parts)
        if is_docked:
            return "Charged" if state.battery_level >= 100 else f"Docked ({state.battery_level}%)"
        return state.working_status.name.replace("_", " ").title()

    # LIVE suction/mop settings broadcast in base_status (PR #34/#31) — distinct
    # from the CleanParam config in field 48. These update in real time.
    _LIVE_FAN = {1: "Quiet", 2: "Normal", 3: "Strong", 4: "Super Powerful"}
    _LIVE_MOP = {1: "Slightly Dry", 2: "Standard", 3: "Slightly Wet"}

    @staticmethod
    def _d(v):
        """Coerce to a dict — the library can store a str/bytes in raw_* fields
        when a protobuf field decodes as a scalar; guards all .get() access."""
        return v if isinstance(v, dict) else {}

    def _fan_name(self, rbs) -> str:
        return self._LIVE_FAN.get(self._d(rbs).get("26"), "")

    def _mop_name(self, rbs) -> str:
        return self._LIVE_MOP.get(self._d(rbs).get("29"), "")

    def _clean_mode_name(self, cfg: dict) -> str:
        return self._WORK_MODE.get(cfg.get("mode"), "")

    # Station activity from base_status field 48.1 (PR #34). An entry with sub-key
    # 1==1 is the active module; its 2.1 is the activity code. Value->name is
    # TENTATIVE pending labelled captures (works like we nailed the dock/phase).
    _STATION_ACTIVITY = {
        1: "Mop washing", 2: "Mop drying", 3: "Dust emptying",
        4: "Dust-bag drying", 5: "Disinfecting",
    }
    # base_status field 3.16 = a prompt for the user to do something.
    _USER_ACTION = {2: "Fill water tank", 3: "Empty / return after clean",
                    4: "Ready — start clean"}

    def _station_activity(self, rbs) -> str:
        f48 = self._d(rbs).get("48")
        if not isinstance(f48, dict):
            return ""
        entries = f48.get("1", [])
        if isinstance(entries, dict):
            entries = [entries]
        for e in entries:
            if isinstance(e, dict) and e.get("1") == 1:
                sub = e.get("2")
                code = None
                if isinstance(sub, dict):
                    try:
                        code = int(sub.get("1"))
                    except (ValueError, TypeError):
                        code = None
                return self._STATION_ACTIVITY.get(code, "Servicing")
        return ""

    def _user_action(self, rbs) -> str:
        f3 = self._d(rbs).get("3")
        if isinstance(f3, dict):
            try:
                return self._USER_ACTION.get(int(f3.get("16")), "")
            except (ValueError, TypeError):
                return ""
        return ""

    def _cleaning_progress(self, state: NarwalState):
        p = self._f32(self._d(state.raw_working_status).get("1"))
        if p is not None and 0 <= p <= 100:
            return int(round(p))
        return None

    def _current_room_name(self, state: NarwalState) -> str:
        """Current room from working_status field 6 (= room_id, PR #24)."""
        try:
            rid = int(self._d(state.raw_working_status).get("6", 0))
        except (ValueError, TypeError):
            return ""
        if rid <= 0 or not state.map_data:
            return ""
        for room in state.map_data.rooms:
            if room.room_id == rid:
                return room.display_name
        return ""

    def _station_states(self, state: NarwalState) -> list:
        """Station/consumable diagnostics from base_status (PR #52)."""
        rbs = self._d(state.raw_base_status)
        kv = []
        needs_attention = False

        def _tank(field, table, key):
            nonlocal needs_attention
            v = rbs.get(field)
            if isinstance(v, int):
                kv.append({"key": key, "value": table.get(v, f"State {v}")})
                if v != 1:
                    needs_attention = True

        _tank("23", self._WATER_TANK, "waterTank")
        _tank("24", self._BIN_STATE, "sewageTank")
        _tank("20", self._BIN_STATE, "dustBox")
        _tank("39", self._BIN_STATE, "stationBag")

        health = self._f32(rbs.get("35"))
        if health is not None and 0 <= health <= 100:
            kv.append({"key": "dustBagHealth", "value": int(round(health)),
                       "uiValue": f"{int(round(health))}%"})
        if isinstance(rbs.get("41"), int):
            kv.append({"key": "detergentRemaining", "value": int(rbs["41"]),
                       "uiValue": f"{int(rbs['41'])}%"})

        # field 1 = error code(s); empty dict/list = no error.
        err = rbs.get("1")
        has_error = bool(err) and isinstance(err, (dict, list)) and len(err) > 0
        if self._user_action(rbs):
            needs_attention = True
        kv.append({"key": "hasError", "value": has_error})
        kv.append({"key": "needsAttention", "value": needs_attention})
        return kv

    def _safe_update(self, dev, key_values: list[dict]) -> None:
        """Write states, auto-registering any that aren't present yet."""
        missing = [kv["key"] for kv in key_values if kv["key"] not in dev.states]
        if missing:
            dev.stateListOrDisplayStateIdChanged()
            dev = indigo.devices.get(dev.id)
            if dev is None:
                return
            key_values = [kv for kv in key_values if kv["key"] in dev.states]
        if key_values:
            dev.updateStatesOnServer(key_values)

    # ------------------------------------------------------------------ #
    # Triggers                                                            #
    # ------------------------------------------------------------------ #
    def _evaluate_triggers(self, dev_id: int, state: NarwalState) -> None:
        prev = self._prev.get(dev_id, {})
        is_cleaning, is_docked, _c, _r, _ad = self._derive_flags(state)
        battery = state.battery_level

        if prev:
            if is_cleaning and not prev.get("cleaning", False):
                # Fire the trigger on the cleaning transition. Coverage/trail are
                # reset separately, by session_id change (see _check_session), so
                # a mid-clean dock visit doesn't wipe accumulated coverage.
                self._fire_triggers("cleaningStarted", dev_id)
            if prev.get("cleaning", False) and is_docked and not is_cleaning:
                self._fire_triggers("cleaningFinished", dev_id)
            if is_docked != prev.get("docked", is_docked):
                self._fire_triggers("dockedChanged", dev_id)
            self._check_battery_low(dev_id, battery, prev.get("battery", 100))

        self._prev[dev_id] = {
            "cleaning": is_cleaning,
            "docked": is_docked,
            "battery": battery,
        }

    def _fire_triggers(self, event_id: str, dev_id: int) -> None:
        for trig in indigo.triggers.iter("self"):
            if not trig.enabled or trig.pluginTypeId != event_id:
                continue
            try:
                if int(trig.pluginProps.get("deviceId", 0)) != dev_id:
                    continue
            except (ValueError, TypeError):
                continue
            indigo.trigger.execute(trig)

    def _check_battery_low(self, dev_id: int, battery: int, prev_battery: int) -> None:
        for trig in indigo.triggers.iter("self"):
            if not trig.enabled or trig.pluginTypeId != "batteryLow":
                continue
            try:
                if int(trig.pluginProps.get("deviceId", 0)) != dev_id:
                    continue
                threshold = int(trig.pluginProps.get("threshold", 20))
            except (ValueError, TypeError):
                continue
            if battery <= threshold < prev_battery:
                indigo.trigger.execute(trig)

    # ------------------------------------------------------------------ #
    # Concurrent thread: watchdog + throttled map rendering               #
    # ------------------------------------------------------------------ #
    def runConcurrentThread(self):
        try:
            while True:
                self.sleep(5)
                loop = self._event_loop
                if not loop or not loop.is_running():
                    continue
                now = time.monotonic()
                for dev in indigo.devices.iter("self"):
                    if dev.deviceTypeId != "narwalVacuum" or not dev.enabled:
                        continue
                    self._watchdog(dev, now)
                    self._maybe_poll_status(dev, now)
                    self._maybe_recover_map(dev, now)
                    self._maybe_idle_clear(dev, now)
                    self._maybe_render_map(dev, now)
                    self._maybe_full_log(dev, now)
        except self.StopThread:
            pass

    def _watchdog(self, dev, now: float) -> None:
        """Restart a listener task that has died (crash / connection lost)."""
        if dev.id in self._connecting:
            return
        task = self._listen_tasks.get(dev.id)
        client = self._clients.get(dev.id)
        if client is None or task is None or task.done():
            self.logger.warning("%s: listener not running — reconnecting", dev.name)
            self._schedule_connect(dev.id)

    def _maybe_poll_status(self, dev, now: float) -> None:
        """Fallback get_status() every 60s in case broadcasts stop.

        When the robot isn't broadcasting (asleep / docked / washing), it returns
        a STALE working_status cached from the last active session. Doing a full
        update then would freeze the status on 'CLEANING'. So we only full-update
        while the robot is awake; otherwise we refresh battery/health only.
        """
        client = self._clients.get(dev.id)
        if client is None or not client.connected:
            return
        last = self._last_status_poll.get(dev.id, 0.0)
        if now - last < 60.0:
            return
        self._last_status_poll[dev.id] = now
        self._run_coro(client.get_status(full_update=client.robot_awake))

    def _maybe_full_log(self, dev, now: float) -> None:
        """Every 10s (when Debug is on), dump the robot's full decoded state so
        transitions like cleaning -> dock washing can be troubleshot."""
        if not self.logger.isEnabledFor(logging.DEBUG):
            return
        client = self._clients.get(dev.id)
        if client is None:
            return
        if now - self._last_full_log.get(dev.id, 0.0) < 10.0:
            return
        self._last_full_log[dev.id] = now
        self._log_full_state(dev, client.state, client, logging.DEBUG)

    def _log_full_state(self, dev, state, client, level=logging.INFO) -> None:
        """Log every field we have from the robot, including the raw decoded
        base_status / working_status dicts (where undecoded fields live)."""
        log = self.logger.log
        d_cleaning, d_docked, d_charging, d_returning, d_atdock = self._derive_flags(state)
        cfg = self._parse_clean_config(state.raw_base_status)
        log(level, "===== %s: full robot state =====", dev.name)
        log(level, "  working_status = %s (%d)", state.working_status.name, int(state.working_status))
        log(level, "  DERIVED: is_cleaning=%s  is_docked=%s  is_charging=%s  is_returning=%s  at_dock=%s",
            d_cleaning, d_docked, d_charging, d_returning, d_atdock)
        log(level, "  clean: mode=%s  fan=%s(f26=%s)  mop=%s(f29=%s)  current_room=%s  area=%.2f m²  (cfg %s)",
            self._clean_mode_name(cfg), self._fan_name(state.raw_base_status), self._d(state.raw_base_status).get("26"),
            self._mop_name(state.raw_base_status), self._d(state.raw_base_status).get("29"),
            self._current_room_name(state) or "-", self._cleaned_area_m2(dev.id, state), cfg)
        log(level, "  progress=%s%%  station_activity=%s  user_action=%s  session=%s  clean_time=%ss",
            self._cleaning_progress(state), self._station_activity(state.raw_base_status) or "-",
            self._user_action(state.raw_base_status) or "-", (state.session_id or "")[:8],
            state.cleaning_time)
        log(level, "  library flags: is_cleaning=%s  is_docked=%s  is_paused=%s  is_returning=%s  is_returning_to_dock=%s",
            state.is_cleaning, state.is_docked, state.is_paused, state.is_returning, state.is_returning_to_dock)
        log(level, "  battery=%d%%  health=%d  cleaning_time=%ds",
            state.battery_level, state.battery_health, state.cleaning_time)
        log(level, "  cleaned_area=%.2f m² (%d cells)  coveredArea(f2)=%s  field13=%d(stuck timer)",
            self._cleaned_area_m2(dev.id, state), len(self._cleaned_cells.get(dev.id, ())),
            self._f32(self._d(state.raw_working_status).get("2")), state.cleaning_area)
        log(level, "  dock: field11=%s  field47=%s  sub_state=%s  activity=%s  presence=%s",
            state.dock_field11, state.dock_field47, state.dock_sub_state,
            state.dock_activity, state.dock_presence)
        log(level, "  firmware=%s  target=%s  session=%s  download_status=%s  upgrade_status=%s",
            state.firmware_version, state.firmware_target, state.session_id,
            state.download_status, state.upgrade_status_code)
        if state.map_display_data is not None:
            d = state.map_display_data
            log(level, "  robot pos=(%.1f, %.1f)  heading=%.1f°  ts=%d",
                d.robot_x, d.robot_y, d.robot_heading, d.timestamp)
        if client is not None:
            log(level, "  connected=%s  awake=%s  last_broadcast_age=%.1fs  cleaned_cells=%d  trail=%d",
                client.connected, client.robot_awake, client.last_broadcast_age,
                len(self._cleaned_cells.get(dev.id, ())), len(self._trail.get(dev.id, [])))
        log(level, "  raw_base_status    = %r", state.raw_base_status)
        log(level, "  raw_working_status = %r", state.raw_working_status)
        log(level, "===== end %s state =====", dev.name)

    def _maybe_recover_map(self, dev, now: float) -> None:
        """Re-fetch the map if it's missing while the robot is reachable.

        get_map is only called once at connect; if it failed (robot busy resuming
        after a dock visit) the map would otherwise stay missing and rendering
        would stall. This retries every 45s until a map is loaded."""
        client = self._clients.get(dev.id)
        if client is None or not client.connected or not client.robot_awake:
            return
        md = client.state.map_data
        if md and md.compressed_map:
            return
        if now - self._last_map_fetch.get(dev.id, 0.0) < 45.0:
            return
        self._last_map_fetch[dev.id] = now
        self.logger.info("%s: map missing — re-fetching get_map()", dev.name)
        self._run_coro(client.get_map())

    def _maybe_idle_clear(self, dev, now: float) -> None:
        """Clear the cleaned overlay + trail once the robot has been idle (not
        cleaning) for the configured time, so the map falls back to the bare
        floor plan. Setting 'Never' keeps the last clean's coverage."""
        try:
            mins = int(dev.pluginProps.get("idleClearMinutes", 15))
        except (ValueError, TypeError):
            mins = 15
        if mins <= 0:
            return
        client = self._clients.get(dev.id)
        if client is None:
            return
        if self._derive_flags(client.state)[0]:  # is_cleaning -> reset the idle clock
            self._last_active[dev.id] = now
            return
        last = self._last_active.get(dev.id)
        if last is None:
            self._last_active[dev.id] = now  # start the idle clock (e.g. on connect)
            return
        if now - last < mins * 60:
            return
        if self._cleaned_cells.get(dev.id) or self._trail.get(dev.id):
            self.logger.info("%s: idle %d min — clearing map overlay & trail", dev.name, mins)
            self._cleaned_cells[dev.id] = set()
            self._trail[dev.id] = []
            self._delete_coverage_files(dev)
            self._last_map_render.pop(dev.id, None)  # force one blank re-render

    def _delete_coverage_files(self, dev) -> None:
        for path in (self._trail_file(dev), self._cleaned_file(dev)):
            try:
                os.remove(path)
            except OSError:
                pass

    def _maybe_render_map(self, dev, now: float) -> None:
        if not dev.pluginProps.get("renderMap", True):
            return
        client = self._clients.get(dev.id)
        if client is None:
            return
        state = client.state
        if not state.map_data or not state.map_data.compressed_map:
            # No map yet — _maybe_recover_map re-fetches it while reachable.
            return
        # Fast (~15s) whenever the robot is broadcasting (awake) — this keeps the
        # robot/coverage moving through docking transitions where the library
        # is_cleaning flag goes momentarily stale; otherwise a ~60s heartbeat.
        fast = dev.pluginProps.get("mapWhileCleaning", True) and client.robot_awake
        interval = 15.0 if fast else 60.0
        if now - self._last_map_render.get(dev.id, 0.0) < interval:
            return
        self._last_map_render[dev.id] = now
        self._render_map(dev, state)

    def _render_map(self, dev, state: NarwalState) -> None:
        """Render the map PNG. Runs on Indigo's thread — keeps Pillow off
        the event loop. Fully defensive: never breaks core functionality.

        Uses an upscaled floor plan (crisp NEAREST scaling) plus the accumulated
        cleaning trail, via the plugin-side narwal_map helper. The static base
        image is cached and only rebuilt when the map itself changes.
        """
        try:
            md = state.map_data
            disp = state.map_display_data

            # Rebuild the base only when the map changes (dimensions + creation
            # time + room count form a cheap signature).
            signature = (md.width, md.height, md.created_at, len(md.rooms))
            cached = self._base_map_cache.get(dev.id)
            if cached is None or cached[0] != signature:
                base_img = narwal_map.build_base(md)
                if base_img is None:
                    return
                self._base_map_cache[dev.id] = (signature, base_img)
            else:
                base_img = cached[1]

            # Snapshot the trail + swept cells (appended from the async thread).
            # The trail includes travel/navigation moves (straight lines that can
            # cross walls), so it's only drawn when the user opts in.
            show_trail = dev.pluginProps.get("showTrail", False)
            sampled_trail = list(self._trail.get(dev.id, []))
            trail = sampled_trail if show_trail else []
            cleaned = None
            cleaned_alpha = narwal_map.DEFAULT_CLEANED_ALPHA
            if dev.pluginProps.get("overlayCleanedArea", False):
                cleaned = list(self._cleaned_cells.get(dev.id, ()))
                try:
                    pct = float(dev.pluginProps.get("sweptOpacity", 60))
                    cleaned_alpha = max(30, min(235, int(round(pct / 100.0 * 255))))
                except (ValueError, TypeError):
                    pass
            # Burned-in caption so freshness is obvious even in a caching viewer.
            room = self._current_room_name(state)
            area = self._cleaned_area_m2(dev.id, state)
            caption = (f"{datetime.now():%H:%M:%S}  {room or 'Cleaning'}  "
                       f"{area:g} m2  batt {state.battery_level}%")
            png = narwal_map.compose(base_img, md, disp, trail, cleaned=cleaned,
                                     cleaned_alpha=cleaned_alpha, caption=caption)
            if not png:
                return
            self.logger.debug(
                "%s: rendering map %dx%d — trail=%d pts, robot=%s, cleaned=%d cells",
                dev.name, md.width, md.height, len(trail),
                (round(disp.robot_x, 1), round(disp.robot_y, 1)) if disp else None,
                len(cleaned) if cleaned else 0,
            )
            # Persist the (full) trail + swept area so they survive plugin reloads.
            self._save_trail(dev, sampled_trail)
            self._save_cleaned(dev, list(self._cleaned_cells.get(dev.id, ())))

            out_dir = self._map_output_dir(dev)
            os.makedirs(out_dir, exist_ok=True)
            safe_id = (state.device_info.device_id if state.device_info else str(dev.id)) or str(dev.id)
            safe_id = "".join(c for c in safe_id if c.isalnum() or c in "-_")
            path = os.path.join(out_dir, f"narwal_map_{safe_id}.png")
            tmp = path + ".tmp"
            with open(tmp, "wb") as fh:
                fh.write(png)
            os.replace(tmp, path)
            if dev.states.get("mapPath") != path:
                dev.updateStateOnServer("mapPath", path)
            self.logger.debug("%s: map rendered (%d bytes) → %s", dev.name, len(png), path)
        except Exception:
            self.logger.exception("%s: map rendering failed", dev.name)

    def _map_output_dir(self, dev) -> str:
        custom = (dev.pluginProps.get("mapDir") or "").strip()
        if custom:
            return custom
        # Default under the Indigo web server assets so control pages can serve it.
        return os.path.join(
            indigo.server.getInstallFolderPath(), "Web Assets", "images", "narwal"
        )

    # ------------------------------------------------------------------ #
    # Coroutine dispatch helpers                                          #
    # ------------------------------------------------------------------ #
    def _run_coro(self, coro: Coroutine) -> None:
        """Fire-and-forget schedule a coroutine on the event loop."""
        loop = self._event_loop
        if loop and loop.is_running():
            asyncio.run_coroutine_threadsafe(coro, loop)
        else:
            coro.close()

    def _run_coro_blocking(self, coro: Coroutine, timeout: float = 25.0):
        """Schedule a coroutine and block for its result (config-time use)."""
        loop = self._event_loop
        if not loop or not loop.is_running():
            raise RuntimeError("event loop not running")
        return asyncio.run_coroutine_threadsafe(coro, loop).result(timeout)

    def _dispatch(self, dev, factory: Callable[[NarwalClient], Coroutine], desc: str) -> None:
        """Run a client command for a device, logging the outcome."""
        client = self._clients.get(dev.id)
        if client is None:
            self.logger.error("%s: not connected — cannot %s", dev.name, desc)
            return
        self.logger.info("%s: %s", dev.name, desc)

        async def _wrapped():
            try:
                await factory(client)
            except NarwalCommandError as ex:
                self.logger.warning("%s: %s failed: %s", dev.name, desc, ex)
            except Exception:
                self.logger.exception("%s: %s errored", dev.name, desc)

        self._run_coro(_wrapped())

    # ------------------------------------------------------------------ #
    # Relay on/off control                                                #
    # ------------------------------------------------------------------ #
    def actionControlDevice(self, action, dev):
        if dev.deviceTypeId != "narwalVacuum":
            return
        if action.deviceAction == indigo.kDeviceAction.TurnOn:
            self._do_relay_on(dev)
        elif action.deviceAction == indigo.kDeviceAction.TurnOff:
            self._do_relay_off(dev)
        elif action.deviceAction == indigo.kDeviceAction.Toggle:
            if dev.onState:
                self._do_relay_off(dev)
            else:
                self._do_relay_on(dev)

    def _do_relay_on(self, dev):
        """Perform the configured 'ON' action (device setting relayOnAction)."""
        props = dev.pluginProps
        on_action = props.get("relayOnAction", "start")
        self._reset_coverage(dev.id, f"relay ON ({on_action})")

        if on_action == "rooms":
            raw = props.get("relayOnRooms", [])
            if isinstance(raw, str):
                raw = [raw]
            room_ids = []
            for r in raw:
                try:
                    room_ids.append(int(r))
                except (ValueError, TypeError):
                    continue
            if not room_ids:
                self.logger.warning(
                    "%s: ON is set to clean rooms but none are selected — "
                    "starting a whole-house clean instead", dev.name)
                self._dispatch(dev, lambda c: c.start(), "start clean (relay ON, no rooms)")
                return

            def _int(key, default):
                try:
                    return int(props.get(key, default))
                except (ValueError, TypeError):
                    return default
            mode = _int("relayOnMode", 3)
            fan = _int("relayOnFan", 2)
            water = _int("relayOnWater", 2)
            self._dispatch(dev, lambda c: self._clean_rooms(c, room_ids, mode, fan, water, 1),
                           f"relay ON: clean rooms {room_ids} mode={self._WORK_MODE.get(mode, mode)}")
        elif on_action == "easy":
            self._dispatch(dev, lambda c: c.start_easy_clean(), "easy clean (relay ON)")
        else:
            self._dispatch(dev, lambda c: c.start(), "start clean (relay ON)")

    def _do_relay_off(self, dev):
        """Perform the configured 'OFF' action (device setting relayOffAction)."""
        off_action = dev.pluginProps.get("relayOffAction", "dock")
        if off_action == "pause":
            self._dispatch(dev, lambda c: c.pause(), "pause (relay OFF)")
        elif off_action == "stop":
            self._dispatch(dev, lambda c: c.stop(), "stop (relay OFF)")
        elif off_action == "nothing":
            self.logger.info("%s: relay OFF — no action configured", dev.name)
        else:
            self._dispatch(dev, lambda c: c.return_to_base(), "return to dock (relay OFF)")

    # ------------------------------------------------------------------ #
    # Action callbacks                                                    #
    # ------------------------------------------------------------------ #
    def actionStartClean(self, action, dev):
        self._reset_coverage(dev.id, "start clean requested")
        self._dispatch(dev, lambda c: c.start(), "start clean")

    def actionPause(self, action, dev):
        self._dispatch(dev, lambda c: c.pause(), "pause")

    def actionResume(self, action, dev):
        self._dispatch(dev, lambda c: c.resume(), "resume")

    def actionStop(self, action, dev):
        self._dispatch(dev, lambda c: c.stop(), "stop")

    def actionReturnToBase(self, action, dev):
        self._dispatch(dev, lambda c: c.return_to_base(), "return to dock")

    def actionLocate(self, action, dev):
        self._dispatch(dev, lambda c: c.locate(), "locate")

    def actionWashMop(self, action, dev):
        self._dispatch(dev, lambda c: c.wash_mop(), "wash mop")

    def actionDryMop(self, action, dev):
        self._dispatch(dev, lambda c: c.dry_mop(), "dry mop")

    def actionEmptyDustbin(self, action, dev):
        self._dispatch(dev, lambda c: c.empty_dustbin(), "empty dustbin")

    def actionSetFanSpeed(self, action, dev):
        try:
            level = FanLevel(int(action.props.get("fanLevel", 3)))
        except (ValueError, TypeError):
            level = FanLevel.MAX
        self._dispatch(dev, lambda c: c.set_fan_speed(level), f"set fan speed {level.name}")

    def actionSetMopHumidity(self, action, dev):
        try:
            level = MopHumidity(int(action.props.get("mopLevel", 2)))
        except (ValueError, TypeError):
            level = MopHumidity.WET
        self._dispatch(dev, lambda c: c.set_mop_humidity(level), f"set mop humidity {level.name}")

    # WorkMode -> (CleanParam.mode value, pass-count field tags). From upstream
    # PR #49/#50 (clean/start_clean CleanTask schema).
    _WORK_MODE_PARAM = {
        1: (2, ("5",)),        # Vacuum
        2: (3, ("6",)),        # Mop
        3: (5, ("5", "6")),    # Vacuum then Mop
        4: (4, ("7",)),        # Vacuum & Mop (one pass)
    }

    @staticmethod
    def _active_map_id(client) -> int:
        md = getattr(client, "state", None) and client.state.map_data
        if md and isinstance(getattr(md, "raw", None), dict):
            try:
                return int(md.raw.get("1", 0))
            except (ValueError, TypeError):
                return 0
        return 0

    def _build_room_clean_payload(self, map_id, room_ids, work_mode, fan, water,
                                  passes, mop_strength=1):
        """Build the CleanTask room-clean protobuf (upstream PR #49 schema).

        Envelope: {1: map_id, 2: [CleanItem], 3: {}, 5: work_mode}. Each CleanItem
        = {1: {1: 1(=ROOM), 2: room_id}, 2: CleanParam, 3: order}. CleanParam
        = {1: mode, 2: fan, 3: mop_strength, 4: water, <pass tags>: passes}."""
        import blackboxprotobuf

        param_mode, pass_tags = self._WORK_MODE_PARAM.get(work_mode, (5, ("5", "6")))
        items = []
        for i, rid in enumerate(room_ids):
            param = {"1": param_mode, "2": fan, "3": mop_strength, "4": water}
            for t in pass_tags:
                param[t] = passes
            items.append({"1": {"1": 1, "2": rid}, "2": param, "3": i + 1})

        param_tags = sorted({"1", "2", "3", "4", *pass_tags}, key=int)
        item_typedef = {
            "type": "message", "seen_repeated": True,
            "message_typedef": {
                "1": {"type": "message", "message_typedef": {"1": {"type": "int"}, "2": {"type": "int"}}},
                "2": {"type": "message", "message_typedef": {t: {"type": "int"} for t in param_tags}},
                "3": {"type": "int"},
            },
        }
        msg = {"1": {"1": int(map_id), "2": items if len(items) > 1 else items[0],
                     "3": {}, "5": int(work_mode)}}
        typedef = {"1": {"type": "message", "message_typedef": {
            "1": {"type": "int"}, "2": item_typedef,
            "3": {"type": "message", "message_typedef": {}}, "5": {"type": "int"},
        }}}
        return blackboxprotobuf.encode_message(msg, typedef)

    def actionCleanRooms(self, action, dev):
        raw = action.props.get("rooms", [])
        if isinstance(raw, str):
            raw = [raw]
        room_ids: list[int] = []
        for r in raw:
            try:
                room_ids.append(int(r))
            except (ValueError, TypeError):
                continue
        if not room_ids:
            self.logger.warning("%s: clean rooms — no valid rooms selected", dev.name)
            return

        def _opt(key, default):
            try:
                return int(action.props.get(key, default))
            except (ValueError, TypeError):
                return default

        mode = _opt("mode", 3)
        fan = _opt("fan", 2)
        water = _opt("water", 2)
        passes = _opt("passes", 1)
        desc = (f"clean rooms {room_ids} (mode={self._WORK_MODE.get(mode, mode)}, "
                f"fan={self._FAN_LEVELS.get(fan, fan)}, water={self._WATER_LEVELS.get(water, water)}, "
                f"passes={passes})")
        self._reset_coverage(dev.id, "room clean requested")
        self._dispatch(dev, lambda c: self._clean_rooms(c, room_ids, mode, fan, water, passes), desc)

    async def _clean_rooms(self, client, room_ids, work_mode, fan, water, passes):
        """Room-specific clean with a selectable mode (PR #49 CleanTask schema).

        Sends the new clean/start_clean CleanTask; falls back to the vendored
        start_rooms (clean/plan/start, mode not applied) if it isn't accepted or
        the payload can't be built."""
        from narwal_client.const import CommandResult

        map_id = self._active_map_id(client)
        try:
            payload = self._build_room_clean_payload(map_id, room_ids, work_mode, fan, water, passes)
        except Exception:
            self.logger.exception("Could not build room-clean payload — using library start_rooms")
            return await client.start_rooms(room_ids)

        resp = await client.send_command("clean/start_clean", payload=payload, timeout=12.0)
        try:
            result_name = CommandResult(resp.result_code).name
        except ValueError:
            result_name = f"UNKNOWN({resp.result_code})"

        if resp.success:
            self.logger.info("Room clean accepted (%s): rooms=%s mode=%s",
                             result_name, room_ids, self._WORK_MODE.get(work_mode, work_mode))
            return resp

        self.logger.warning(
            "Room clean via clean/start_clean not accepted (%s) — falling back to library "
            "start_rooms (mode ignored). CONFLICT/NOT_APPLICABLE usually means the robot is "
            "busy or mid-dock-cycle; try when idle on the dock.", result_name)
        return await client.start_rooms(room_ids)

    def actionRefreshMap(self, action, dev):
        self._last_map_render.pop(dev.id, None)
        client = self._clients.get(dev.id)
        if client is not None:
            self._run_coro(client.get_map())
        self.logger.info("%s: map refresh requested", dev.name)

    def actionRefreshStatus(self, action, dev):
        client = self._clients.get(dev.id)
        if client is not None:
            self._run_coro(client.get_status(full_update=True))
        self.logger.info("%s: status refresh requested", dev.name)

    # ------------------------------------------------------------------ #
    # Config UI callbacks                                                 #
    # ------------------------------------------------------------------ #
    def validateDeviceConfigUi(self, valuesDict, typeId, devId):
        errors = indigo.Dict()
        if typeId == "narwalVacuum":
            if not (valuesDict.get("host") or "").strip():
                errors["host"] = "Enter the vacuum's IP address or hostname."
            port = (valuesDict.get("port") or "9002").strip()
            if not port.isdigit() or not (1 <= int(port) <= 65535):
                errors["port"] = "Port must be a number between 1 and 65535."
        if errors:
            return False, valuesDict, errors
        return True, valuesDict, errors

    def discoverDeviceIdButton(self, valuesDict, typeId, devId):
        """ConfigUI button: connect briefly and read the robot's device ID."""
        host = (valuesDict.get("host") or "").strip()
        if not host:
            valuesDict["discoverResult"] = "Enter an IP address first."
            return valuesDict
        try:
            port = int(valuesDict.get("port", 9002))
        except (ValueError, TypeError):
            port = 9002
        prefix = self._normalise_prefix(valuesDict.get("topicPrefix"))

        async def _probe():
            client = NarwalClient(host=host, port=port, topic_prefix=prefix)
            try:
                await client.connect()
                dev_id = await client.discover_device_id()
                try:
                    info = await client.get_device_info()
                    product_key = info.product_key
                except Exception:
                    product_key = client.topic_prefix.lstrip("/")
                return dev_id, product_key
            finally:
                await client.disconnect()

        try:
            dev_id, product_key = self._run_coro_blocking(_probe(), timeout=30.0)
            valuesDict["deviceId"] = dev_id
            if product_key:
                valuesDict["topicPrefix"] = "/" + product_key.lstrip("/")
            valuesDict["discoverResult"] = f"Found device {dev_id} (key {product_key})"
            self.logger.info("Discovered Narwal device %s (product key %s)", dev_id, product_key)
        except Exception as ex:
            valuesDict["discoverResult"] = f"Discovery failed: {ex}"
            self.logger.warning("Narwal discovery failed: %s", ex)
        return valuesDict

    def roomListGenerator(self, filter="", valuesDict=None, typeId="", targetId=0):
        """Populate the 'clean rooms' list from the device's cached map."""
        client = self._clients.get(int(targetId)) if targetId else None
        if client is None or not client.state.map_data or not client.state.map_data.rooms:
            return [("__none__", "── No map / rooms available yet ──")]
        items = []
        for room in client.state.map_data.rooms:
            if room.room_id:
                items.append((str(room.room_id), room.display_name))
        return items or [("__none__", "── No rooms with IDs ──")]

    def deviceListGenerator(self, filter="", valuesDict=None, typeId="", targetId=0):
        items = []
        for dev in indigo.devices.iter("self"):
            if dev.deviceTypeId == "narwalVacuum":
                items.append((str(dev.id), dev.name))
        return items or [("0", "── No Narwal vacuums ──")]

    def validatePrefsConfigUi(self, valuesDict):
        return True, valuesDict

    def closedPrefsConfigUi(self, valuesDict, userCancelled):
        if not userCancelled:
            self._apply_log_levels(valuesDict)
            self.logger.info("Narwal: preferences saved")

    # ------------------------------------------------------------------ #
    # Menu items                                                          #
    # ------------------------------------------------------------------ #
    def menuReconnectAll(self):
        self.logger.info("Narwal: reconnecting all vacuums")
        for dev in indigo.devices.iter("self"):
            if dev.deviceTypeId == "narwalVacuum" and dev.enabled:
                task = self._listen_tasks.pop(dev.id, None)
                client = self._clients.pop(dev.id, None)
                loop = self._event_loop
                if loop and loop.is_running():
                    if task and not task.done():
                        loop.call_soon_threadsafe(task.cancel)
                    if client:
                        asyncio.run_coroutine_threadsafe(client.disconnect(), loop)
                self._schedule_connect(dev.id)

    def menuDumpMapStructure(self):
        """Arm a one-shot dump of the next display_map broadcast's structure."""
        self._dump_map_next = True
        self.logger.info(
            "Narwal: will dump the structure of the next map/display_map broadcast. "
            "Make sure the robot is awake (cleaning or just started) so it broadcasts."
        )

    def menuLogStatus(self):
        found = False
        for dev in indigo.devices.iter("self"):
            if dev.deviceTypeId != "narwalVacuum":
                continue
            found = True
            client = self._clients.get(dev.id)
            if client is None:
                self.logger.info("  %s: no client", dev.name)
                continue
            # Full dump at INFO so it shows regardless of the current log level.
            self._log_full_state(dev, client.state, client, logging.INFO)
        if not found:
            self.logger.info("No Narwal vacuum devices defined.")
