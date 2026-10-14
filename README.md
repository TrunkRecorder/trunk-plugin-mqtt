# MQTT for Trunk Recorder Pro

Publishes calls, unit activity and system status from [Trunk Recorder Pro](https://github.com/TrunkRecorder/trunk-recorder-pro)
to an MQTT broker. Messages use the format of the Trunk Recorder [MQTT Status plugin](https://github.com/TrunkRecorder/tr-plugin-mqtt),
so existing consumers (Home Assistant, Node-RED, dashboards) read them unchanged.

## Features

- MQTT 3.1.1 and 5, over TCP, TLS (`ssl://`), WebSocket (`ws://`) and secure WebSocket (`wss://`)
- Call start, call end, active calls, decode rates, systems, and call audio
- Unit activity: registrations, affiliations, transmissions, data grants, call alerts and more
- An outbound queue with age and size limits, so a slow or unreachable broker never holds up recording
- Pings and a heartbeat topic; a broker that stops answering is reconnected
- Retained `config`, `systems` and status topics, sent again after each reconnect
- The broker's state, the queue and what's been sent, on Trunk Recorder Pro's dashboard
- Optional [Home Assistant discovery](#home-assistant): the recorder's entities appear on their own

## Install

In Trunk Recorder Pro, open **Plugins**, choose **Install from GitHub…**, and enter
`taclane/trunk-plugin-mqtt`. Then turn it on and fill in its settings in
**Setup → Plugins**.

To build it yourself:

```sh
cargo build --release
```

and add `target/release/mqtt` with **Add from a file…**.

## Configure

| Setting | Key | Default | Description |
| ------- | --- | ------- | ----------- |
| Broker | `broker` | `tcp://localhost:1883` | Broker URL with protocol and port: `tcp://`, `ssl://`, `ws://` or `wss://` |
| Topic | `topic` | | Base topic for status messages. Required. |
| Unit topic | `unitTopic` | | Base topic for unit activity, followed by the system's short name. Empty turns it off. |
| Username | `username` | | Broker username |
| Password | `password` | | Broker password |
| Client ID | `clientId` | `tr-status-<crc>` | Made from the broker and topic when empty |
| Instance ID | `instanceId` | `trunk-recorder` | Added to every message as `instance_id` |
| QoS | `qos` | `0` | 0, 1 or 2 |
| MQTT version | `mqttVersion` | `3` | `3` (3.1.1) or `5` |
| Send call audio | `mqttAudio` | `false` | Publish each recorded call's audio, base64-encoded, to `topic/audio` |
| Audio format | `mqttAudioType` | `wav` | `wav`, `m4a`, `both` or `none` (metadata only) |
| Queue age limit | `queueMaxAge` | `60` | Seconds a message may wait for the broker before it's dropped |
| Audio age limit | `audioMaxAge` | `300` | The same, for audio |
| Queue size limit | `queueMaxMb` | `32` | MB of waiting messages; past this, the oldest are dropped |
| Heartbeat interval | `heartbeatInterval` | `10` | Seconds between pings and heartbeat messages (minimum 5). `0` turns them off. |
| Broker timeout | `heartbeatTimeout` | `30` | Seconds a connection attempt or a send may take. Raise it for audio over a slow uplink. |
| Home Assistant discovery | `homeAssistant` | `false` | Publish Home Assistant's [MQTT discovery](#home-assistant) messages |
| Discovery prefix | `discoveryPrefix` | `homeassistant` | The prefix Home Assistant's MQTT integration listens under |

The Trunk Recorder plugin's keys (`unit_topic`, `mqtt_audio`, `queue_max_age`, …)
are accepted too, so its settings can be pasted in as they are.

M4A audio is there when the recorder encodes it for another plugin (OpenMHz,
Broadcastify Calls). Without it, `m4a` sends no audio and `both` sends WAV only.

## MQTT Messages

Every message is JSON: `{"type": <subtopic>, <name>: <data>, "timestamp": <unix s>, "instance_id": <id>}`.

| Topic | Subtopic | Retained | Description |
| ----- | -------- | :------: | ----------- |
| topic | `config` | ✓ | Systems, capture folder, instance ID |
| topic | `systems` | ✓ | Configured systems |
| topic | `rates` | | Control channel decode rates, every 5 seconds |
| topic | `calls_active` | | Active calls, every second |
| topic | `call_start` | | A call started |
| topic | `call_end` | | A recorded call is complete |
| topic | `call_ignored` | | A call ended without being recorded, and why |
| topic | `audio` | | Audio and metadata of a recorded call |
| topic/trunk_recorder | `status` | ✓ | `connected`, or `disconnected` (also the last will) |
| topic/trunk_recorder | `heartbeat` | | QoS 1, every `heartbeatInterval` seconds |
| unitTopic/shortname | `call` | | The unit that started a call |
| unitTopic/shortname | `end` | | Each transmission of a recorded call |
| unitTopic/shortname | `on` | | Registration |
| unitTopic/shortname | `off` | | Deregistration |
| unitTopic/shortname | `ackresp` | | Acknowledge response |
| unitTopic/shortname | `join` | | Group affiliation |
| unitTopic/shortname | `data` | | Data grant |
| unitTopic/shortname | `ans_req` | | Answer request |
| unitTopic/shortname | `location` | | Location registration |
| unitTopic/shortname | `call_alert` | | Unit-to-unit page |
| unitTopic/shortname | `emergency` | | Emergency alarm |

Message fields match the Trunk Recorder plugin's [example messages](https://github.com/TrunkRecorder/tr-plugin-mqtt/blob/main/example_messages.md).

### Additions

Trunk Recorder Pro reports some things Trunk Recorder doesn't, and these are added
(see [example messages](example_messages.md)):

- `call_ignored`: a call that wasn't recorded, with its `reason` (`encrypted`, `unknown_tg`,
  `ignored`, `no_recorder`, `no_source`)
- `emergency`: a radio's emergency alarm, with its talkgroup
- In `call_end`: `snr` and `clean_voice_pct`, when measured
- In call messages: `decrypted`, and `algid` and `key_id` once the call's voice has named its
  cipher (`132` is AES-256)
- In unit `end`: `lat` and `lon`, when the radio sent its position

### Differences from the Trunk Recorder plugin

Trunk Recorder Pro doesn't report everything Trunk Recorder does. These are not published:

- `recorders`, `recorder` and `system`
- `console` (log messages) and the trunking message topic
- In `systems`: `sysid`, `wacn`, `nac`, `rfss` and `site_id` are empty
- In active-call messages (`call_start`, `calls_active`): `rec_num`, `src_num` and `rec_state` are `-1`
- `config` lists systems only, without sources

Talkgroup and unit names in unit messages come from calls already heard, so
the first messages for a talkgroup or radio may have empty names.

A call that was recorded but then not saved (no audio, or shorter than its
system's minimum) gets neither `call_end` nor `call_ignored`: the recorder
tells plugins only that it ended, as a recorded call. This is to be fixed in
Trunk Recorder Pro, by giving such calls a reason (`too_short`), not here.

## Home Assistant

Turn on **Home Assistant discovery** and Home Assistant's MQTT integration adds a
device for the recorder, with no YAML:

| Entity | Reads |
| ------ | ----- |
| Recorder (connectivity) | `topic/trunk_recorder/status` |
| Active calls | `topic/calls_active` |
| Last call (its talkgroup; the rest as attributes) | `topic/call_end` |
| `<system>` decode rate (msg/s) | `topic/rates` |
| `<system>` control channel (MHz) | `topic/rates` |
| `<system>` active calls | `topic/calls_active` |

The systems with a control channel get the last three; conventional systems have none.
The entities read what the plugin already publishes, so nothing more is sent
to the broker, and they show as unavailable while the recorder is. The
discovery messages are retained and sent again after each reconnect.

Discovery adds entities; it doesn't remove them. If a system is renamed or removed,
or discovery is turned off, delete the old device in Home Assistant (Settings →
Devices & services → MQTT) or publish an empty retained message to its
`<prefix>/<component>/<instance ID>/<object>/config` topic.

## License

GPL-3.0-or-later, like Trunk Recorder.
