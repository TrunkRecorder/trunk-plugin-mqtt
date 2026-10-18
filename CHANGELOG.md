# Changelog

## [0.1.7]

- Built with trunk-recorder-plugin 0.5.0 from crates.io, for Trunk Recorder Pro v0.2.0. A
  conventional system is told by its kind (systems are numbered from 0 there, conventional ones
  too). `freq_error`, `signal`, `noise`, `snr`, `clean_voice_pct`, `rec_num`, `src_num`, the cipher
  and each transmission's errors are read from the call record's named fields: on v0.2.0 they
  are no longer in its extra fields, where they would have read as 0 or -1.

## [0.1.6]

- `systems` carries each system's ID, WACN, NAC, RFSS and site once its control channel has
  stated them, and is sent again whenever they change; `system` is sent for the system that
  changed. Needs Trunk Recorder Pro v0.1.12 or newer.

## [0.1.5]

- Home Assistant discovery (off by default): the recorder's status, active calls, last call,
  and each system's decode rate, control channel and calls appear in Home Assistant on their own.
- QoS and MQTT version are chosen from a menu

## [0.1.4]

- Builds against the plugin SDK of Trunk Recorder Pro v0.1.11
- `call_ignored`: calls that weren't recorded, with the reason
- Emergency alarms on `unitTopic/<shortname>/emergency`
- `snr` and `clean_voice_pct` in `call_end`; `lat` and `lon` in unit `end`
- `decrypted` and the cipher (`algid`, `key_id`) in call messages
- The broker's state, the queue and what's been sent, on Trunk Recorder Pro's dashboard
- Each recorded call is reported done to the recorder, which no longer holds its files
  waiting on this plugin
- Calls on talkgroups marked Ignore get `mon_state` `IGNORED_TG`
- `example_messages.md` with the messages and fields only Trunk Recorder Pro has

## [0.1.0]

First release. Publishes Trunk Recorder Pro's calls, unit activity and system
status to an MQTT broker, in the format of Trunk Recorder's MQTT Status
plugin.
