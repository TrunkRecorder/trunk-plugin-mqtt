# Changelog

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
