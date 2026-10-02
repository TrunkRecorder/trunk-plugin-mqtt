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

Message fields match the Trunk Recorder plugin's [example messages](https://github.com/TrunkRecorder/tr-plugin-mqtt/blob/main/example_messages.md).

### Differences from the Trunk Recorder plugin

Trunk Recorder Pro doesn't report everything Trunk Recorder does. These are not published:

- `recorders`, `recorder` and `system`
- `console` (log messages) and the trunking message topic
- In `systems`: `sysid`, `wacn`, `nac`, `rfss` and `site_id` are empty
- In active-call messages (`call_start`, `calls_active`): `rec_num`, `src_num` and `rec_state` are `-1`
- `config` lists systems only, without sources

Talkgroup and unit names in unit messages come from calls already heard, so
the first messages for a talkgroup or radio may have empty names.

## License

GPL-3.0-or-later, like Trunk Recorder.
