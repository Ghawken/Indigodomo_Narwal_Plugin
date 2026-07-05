# Narwal Robot Vacuum — Indigo Plugin

Local control and monitoring of [Narwal](https://www.narwal.com/) robot vacuums
in [Indigo](https://www.indigodomo.com/) home automation, over the robot's
**local WebSocket API** (no cloud account required).

It is built on the vendored, upstream‑verbatim
[`narwal_client`](https://github.com/sjmotew/NarwalIntegration) library (the same
core used by the Home Assistant integration), driven by an asyncio event loop
running in a background thread — the pattern proven by the WeatherFlow Tempest
and Dreame Indigo plugins.

## Features

- **Device**: each vacuum is an Indigo *relay* device — **ON = start clean**,
  **OFF = return to dock** — so it works on control pages and with on/off triggers.
- **Live state** (push, ~1.5 s while awake): working status, battery %, cleaning /
  docked / charging / paused / returning flags, cleaning area (m²) and time,
  firmware, device ID, product key, robot X/Y/heading, room list.
- **Actions**: Start / Pause / Resume / Stop / Return to Dock / Locate,
  Set Fan Speed, Set Mop Humidity, Wash Mop, Dry Mop, Empty Dustbin,
  **Clean Selected Rooms** (populated from the robot's saved map), Refresh Map,
  Refresh Status.
- **Map snapshots**: upscaled PNG of the house map (crisp nearest‑neighbour
  scaling) with **sharp room labels** (drawn after upscaling), dock, the live
  robot marker (with heading) and a **growing cleaning trail** that **persists
  across plugin reloads** (saved beside the PNG). Optional **experimental
  swept‑area overlay** (per‑device checkbox — reverse‑engineered, may need
  alignment tuning). Saved to the Indigo web assets folder (control‑page ready).
  Rendering is plugin‑side (`narwal_map.py`), so the vendored library stays a
  clean drop‑in.
- **Triggers**: Cleaning Started, Cleaning Finished / Returned to Dock,
  Docked State Changed, Battery Low.

## Requirements

- Indigo 2024.2+ (developed against **2025.2**, Python 3.13). Indigo auto‑installs
  the Python dependencies from `Server Plugin/requirements.txt`
  (`websockets`, `protobuf`, `bbpb`, `Pillow`) on first load.
- The robot on your LAN (a static DHCP reservation is recommended).

## Setup

1. Double‑click `Narwal.indigoPlugin` to install; enable it in Indigo.
2. **New Device → Narwal Robot Vacuum**. Enter the robot's IP address.
   Press **Discover Device ID** to auto‑fill the device ID and product key
   (the robot must be powered and reachable), or leave blank to auto‑discover
   on connect.
3. Save. The device connects, fetches status + map, and begins live updates.

> **Tested against a Narwal Flow 2** (product key `QxMSPG6VSO`), confirmed
> working over the local WebSocket. Other models listed in
> `narwal_client/const.py` are supported to varying, community‑reported degrees.

## Updating the vendored library

`Server Plugin/narwal_client/` is a clean drop‑in copy of upstream — **do not edit
it locally**. To pull in upstream fixes, follow
`Server Plugin/narwal_client/VENDORED.md`. All Indigo‑specific glue lives only in
`Server Plugin/plugin.py`.

## Layout

```
Narwal.indigoPlugin/Contents/
├── Info.plist
└── Server Plugin/
    ├── plugin.py            # Indigo glue: async loop, state mapping, actions
    ├── Devices.xml          # relay device + custom states + ConfigUI
    ├── Actions.xml          # full action set
    ├── Events.xml           # triggers
    ├── PluginConfig.xml     # log levels
    ├── MenuItems.xml        # reconnect / log status
    ├── requirements.txt     # auto-installed by Indigo
    └── narwal_client/       # vendored upstream library (verbatim)
```

## Credits

- Local Narwal protocol + client library: [sjmotew/NarwalIntegration](https://github.com/sjmotew/NarwalIntegration)
- Async‑in‑Indigo pattern: [Ghawken/WeatherFlowTempest](https://github.com/Ghawken/WeatherFlowTempest)
- Vacuum feature reference: [Ghawken/Dreame-Indigo](https://github.com/Ghawken/Dreame-Indigo)
