# Example messages

Messages in the Trunk Recorder plugin's format are as in its
[example messages](https://github.com/TrunkRecorder/tr-plugin-mqtt/blob/main/example_messages.md).
These are the ones that differ: the messages and fields only Trunk Recorder Pro has.

## call_end

`topic/call_end`. Trunk Recorder's fields, then `snr` and `clean_voice_pct` (left out when
not measured), `decrypted`, and `algid` and `key_id` when the call's voice named its cipher.
`algid` is the P25 algorithm ID: `132` (0x84) is AES-256, `170` (0xAA) is ADP.

```json
{
  "type": "call_end",
  "call": {
    "id": "1_26121_1791993081",
    "call_num": 227,
    "sys_num": 1,
    "sys_name": "clmrn-G",
    "freq": 852200000.0,
    "unit": 1611018,
    "unit_alpha_tag": "161 ENG3-M",
    "talkgroup": 26121,
    "talkgroup_alpha_tag": "Wilton Fire",
    "talkgroup_description": "Fire Operations",
    "talkgroup_group": "Wilton - Fairfield County",
    "talkgroup_tag": "Fire Dispatch",
    "talkgroup_patches": "",
    "elapsed": 9,
    "length": 7.0,
    "call_state": -1,
    "call_state_type": "COMPLETED",
    "mon_state": 0,
    "mon_state_type": "UNSPECIFIED",
    "audio_type": "digital tdma",
    "phase2_tdma": true,
    "tdma_slot": 0,
    "analog": false,
    "rec_num": 1,
    "src_num": 0,
    "rec_state": 6,
    "rec_state_type": "STOPPED",
    "conventional": false,
    "encrypted": false,
    "emergency": false,
    "start_time": 1791993081,
    "stop_time": 1791993090,
    "process_call_time": 1791993094,
    "error_count": 28,
    "spike_count": 0,
    "retry_attempt": 0,
    "freq_error": 4765.0,
    "signal": -32.5,
    "noise": -56.8,
    "call_filename": "/dev/shm/trunk-pro/clmrn-G/2026/10/14/26121-1791993081_852200000.0.wav",
    "snr": 24.3,
    "clean_voice_pct": 99.1,
    "decrypted": false
  },
  "timestamp": 1791993094,
  "instance_id": "trunk-recorder"
}
```

## call_start, calls_active

As Trunk Recorder's, with `decrypted`, and `algid` and `key_id` once the cipher has been heard
(usually not yet at the start of a call).

## call_ignored

`topic/call_ignored`. A call that ended without being recorded. The fields of an active call,
ended, with `reason`:

| `reason` | |
| -------- | - |
| `encrypted` | Encrypted, and encrypted calls aren't recorded |
| `unknown_tg` | Not in the talkgroup file, and unknown talkgroups aren't recorded |
| `ignored` | Marked Ignore in the talkgroup file |
| `no_recorder` | No recorder was free |
| `no_source` | No source covers its frequency |

`mon_state` and `mon_state_type` give the same reason in Trunk Recorder's terms.

```json
{
  "type": "call_ignored",
  "call": {
    "id": "1_15101_1791992922",
    "call_num": 144,
    "sys_num": 1,
    "sys_name": "clmrn-G",
    "freq": 852200000.0,
    "unit": 511597,
    "unit_alpha_tag": "",
    "talkgroup": 15101,
    "talkgroup_alpha_tag": "Fairfield PD",
    "talkgroup_description": "",
    "talkgroup_group": "",
    "talkgroup_tag": "",
    "talkgroup_patches": "",
    "elapsed": 12,
    "length": 11.6,
    "call_state": -1,
    "call_state_type": "COMPLETED",
    "mon_state": 5,
    "mon_state_type": "ENCRYPTED",
    "audio_type": "digital tdma",
    "phase2_tdma": true,
    "tdma_slot": 1,
    "analog": false,
    "rec_num": -1,
    "src_num": -1,
    "rec_state": -1,
    "rec_state_type": "",
    "conventional": false,
    "encrypted": true,
    "emergency": false,
    "start_time": 1791992922,
    "stop_time": 1791992934,
    "algid": 132,
    "key_id": 4097,
    "decrypted": false,
    "reason": "encrypted"
  },
  "timestamp": 1791992934,
  "instance_id": "trunk-recorder"
}
```

## Unit end

`unitTopic/<shortname>/end`. With `lat` and `lon` (decimal degrees) when the radio sent its
position during the transmission.

```json
{
  "type": "end",
  "end": {
    "sys_num": 1,
    "sys_name": "clmrn-G",
    "unit": 1891068,
    "unit_alpha_tag": "NTD BUS 181-M",
    "talkgroup": 4111,
    "talkgroup_alpha_tag": "NorwalkTrns FixR",
    "talkgroup_description": "Transit - Fixed Route",
    "talkgroup_group": "Norwalk Transit",
    "talkgroup_tag": "Transportation",
    "talkgroup_patches": "",
    "call_num": 879,
    "freq": 769531250.0,
    "position": 6.64,
    "length": 3.2,
    "emergency": false,
    "encrypted": false,
    "start_time": 1791994934,
    "stop_time": 1791994937,
    "error_count": 0,
    "spike_count": 0,
    "sample_count": 25600,
    "transmission_filename": "/dev/shm/trunk-pro/clmrn-G/2026/10/14/4111-1791994927_769531250.1.wav",
    "lat": 41.117744,
    "lon": -73.408232
  },
  "timestamp": 1791994941,
  "instance_id": "trunk-recorder"
}
```

## Emergency

`unitTopic/<shortname>/emergency`. A radio's emergency alarm, with the talkgroup it was on.
(Illustrative: no alarm has been captured yet.)

```json
{
  "type": "emergency",
  "emergency": {
    "sys_num": 1,
    "sys_name": "clmrn-G",
    "unit": 1611018,
    "unit_alpha_tag": "161 ENG3-M",
    "talkgroup": 26121,
    "talkgroup_alpha_tag": "Wilton Fire",
    "talkgroup_description": "Fire Operations",
    "talkgroup_group": "Wilton - Fairfield County",
    "talkgroup_tag": "Fire Dispatch",
    "talkgroup_patches": ""
  },
  "timestamp": 1791993120,
  "instance_id": "trunk-recorder"
}
```
