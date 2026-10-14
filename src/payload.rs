//! Message bodies, in the MQTT Status plugin's format for Trunk Recorder, so
//! existing consumers read them unchanged. Fields the recorder doesn't report
//! keep their place with an empty or zero value.

use std::collections::HashMap;
use std::path::Path;

use serde_json::{json, Value};
use trunk_recorder_plugin::{CallInfo, ConcludedCall, Status, UnitEvent, CONVENTIONAL};

#[derive(Clone, Default)]
struct Talkgroup {
    alpha_tag: String,
    description: String,
    group: String,
    tag: String,
}

/// The systems, and the talkgroup and radio names seen so far.
#[derive(Default)]
pub struct Directory {
    /// (index, short name, kind); a system's `sys_num` is its position.
    systems: Vec<(u16, String, String)>,
    talkgroups: HashMap<(u16, u32), Talkgroup>,
    units: HashMap<(u16, u32), String>,
}

impl Directory {
    /// `systems`: (index, short name, kind), in the recorder's order.
    pub fn new(systems: impl IntoIterator<Item = (u16, String, String)>) -> Directory {
        Directory { systems: systems.into_iter().collect(), ..Default::default() }
    }

    pub fn sys_num(&self, index: u16) -> usize {
        self.systems.iter().position(|s| s.0 == index).unwrap_or(index as usize)
    }

    /// Learn names from a call that started.
    pub fn learn_start(&mut self, c: &CallInfo) {
        if !c.talkgroup_tag.is_empty() {
            self.talkgroups.entry((c.system, c.talkgroup)).or_default().alpha_tag = c.talkgroup_tag.clone();
        }
    }

    /// Learn names from a recorded call.
    pub fn learn_concluded(&mut self, c: &ConcludedCall) {
        let r = &c.call;
        if !r.talkgroup_tag.is_empty() || !r.talkgroup_description.is_empty() {
            self.talkgroups.insert(
                (c.system, r.talkgroup),
                Talkgroup {
                    alpha_tag: r.talkgroup_tag.clone(),
                    description: r.talkgroup_description.clone(),
                    group: r.talkgroup_group.clone(),
                    tag: r.talkgroup_group_tag.clone(),
                },
            );
        }
        for s in &r.src_list {
            let name = if s.tag.is_empty() { &s.tag_ota } else { &s.tag };
            if !name.is_empty() && s.src > 0 {
                self.units.insert((c.system, s.src as u32), name.clone());
            }
        }
    }

    fn unit_tag(&self, system: u16, unit: u32) -> &str {
        self.units.get(&(system, unit)).map_or("", |s| s.as_str())
    }

    fn tg_fields(&self, system: u16, talkgroup: u32, patches: &[u32]) -> Value {
        let t = self.talkgroups.get(&(system, talkgroup)).cloned().unwrap_or_default();
        json!({
            "talkgroup": talkgroup,
            "talkgroup_alpha_tag": t.alpha_tag,
            "talkgroup_description": t.description,
            "talkgroup_group": t.group,
            "talkgroup_tag": t.tag,
            "talkgroup_patches": patch_string(patches),
        })
    }
}

fn patch_string(p: &[u32]) -> String {
    p.iter().map(|t| t.to_string()).collect::<Vec<_>>().join(",")
}

fn round2(x: f64) -> f64 {
    (x * 100.0).round() / 100.0
}

/// Merge `b`'s fields into `a` (both objects), keeping `a`'s order.
fn extend(mut a: Value, b: Value) -> Value {
    if let (Some(a), Value::Object(b)) = (a.as_object_mut(), b) {
        a.extend(b);
    }
    a
}

/// Trunk Recorder's state names.
fn state_type(s: i64) -> &'static str {
    match s {
        0 => "MONITORING",
        1 => "RECORDING",
        2 => "INACTIVE",
        3 => "ACTIVE",
        4 => "IDLE",
        6 => "STOPPED",
        7 => "AVAILABLE",
        8 => "IGNORE",
        -1 => "COMPLETED",
        _ => "",
    }
}

/// Trunk Recorder's monitoring state for why a call isn't recorded.
fn mon_state(reason: Option<&str>) -> (i64, &'static str) {
    match reason {
        Some("unknown_tg") => (1, "UNKNOWN_TG"),
        // Trunk Recorder Pro says "ignored".
        Some("ignored" | "ignored_tg") => (2, "IGNORED_TG"),
        Some("no_source") => (3, "NO_SOURCE"),
        Some("no_recorder") => (4, "NO_RECORDER"),
        Some("encrypted") => (5, "ENCRYPTED"),
        Some("duplicate") => (6, "DUPLICATE"),
        Some("superseded") => (7, "SUPERSEDED"),
        _ => (0, "UNSPECIFIED"),
    }
}

/// A call in progress (`call_start`, `calls_active`).
pub fn active_call(d: &Directory, c: &CallInfo, now: f64) -> Value {
    let start = c.start_time.floor() as i64;
    let unit = c.units.first().copied().unwrap_or(0);
    let state = if c.recording { 1 } else { 0 };
    let (mon, mon_type) = mon_state(c.reason.as_deref());
    let audio_type = if c.analog {
        "analog"
    } else if c.tdma_slot.is_some() {
        "digital tdma"
    } else {
        "digital"
    };
    let head = json!({
        "id": format!("{}_{}_{}", d.sys_num(c.system), c.talkgroup, start),
        "call_num": c.id,
        "sys_num": d.sys_num(c.system),
        "sys_name": c.short_name,
        "freq": c.freq_hz as f64,
        "unit": unit,
        "unit_alpha_tag": d.unit_tag(c.system, unit),
    });
    let tail = json!({
        "elapsed": (now - c.start_time).max(0.0).round() as i64,
        "length": round2((now - c.start_time).max(0.0)),
        "call_state": state,
        "call_state_type": state_type(state),
        "mon_state": mon,
        "mon_state_type": mon_type,
        "audio_type": audio_type,
        "phase2_tdma": c.tdma_slot.is_some(),
        "tdma_slot": c.tdma_slot.unwrap_or(0),
        "analog": c.analog,
        "rec_num": -1,
        "src_num": -1,
        "rec_state": -1,
        "rec_state_type": "",
        "conventional": c.system == CONVENTIONAL,
        "encrypted": c.encrypted,
        "emergency": c.emergency,
        "start_time": start,
        "stop_time": 0,
    });
    let mut v = extend(extend(head, d.tg_fields(c.system, c.talkgroup, &c.patched_talkgroups)), tail);
    cipher(&mut v, c.algid.map(i64::from), c.key_id.map(i64::from), c.decrypted);
    v
}

/// The cipher an encrypted call's voice named, once heard, and whether it was decrypted.
fn cipher(v: &mut Value, algid: Option<i64>, key_id: Option<i64>, decrypted: bool) {
    if let (Some(a), Some(k)) = (algid, key_id) {
        v["algid"] = json!(a);
        v["key_id"] = json!(k);
    }
    v["decrypted"] = json!(decrypted);
}

/// A call that ended without being recorded (`call_ignored`), and why.
pub fn ignored_call(d: &Directory, c: &CallInfo, now: f64) -> Value {
    let mut v = active_call(d, c, now);
    v["call_state"] = json!(-1);
    v["call_state_type"] = json!(state_type(-1));
    v["stop_time"] = json!(now.floor() as i64);
    v["reason"] = json!(c.reason.as_deref().unwrap_or("unspecified"));
    v
}

/// The unit that started a call (`unit_topic/<short name>/call`); None without one.
pub fn unit_call(d: &Directory, c: &CallInfo) -> Option<Value> {
    let unit = *c.units.first()?;
    let head = json!({ "sys_num": d.sys_num(c.system), "sys_name": c.short_name, "unit": unit, "unit_alpha_tag": d.unit_tag(c.system, unit) });
    let tail = json!({ "call_num": c.id, "freq": c.freq_hz as f64, "encrypted": c.encrypted, "start_time": c.start_time.floor() as i64 });
    Some(extend(extend(head, d.tg_fields(c.system, c.talkgroup, &c.patched_talkgroups)), tail))
}

fn extra_f64(c: &ConcludedCall, key: &str) -> f64 {
    c.call.extra.get(key).and_then(Value::as_f64).unwrap_or(0.0)
}

/// The audio file `call_filename` names: the M4A when `m4a`, else the WAV.
fn file_name(p: &Path) -> String {
    p.display().to_string()
}

/// A recorded call (`call_end`).
pub fn call_end(d: &Directory, c: &ConcludedCall, prefer_m4a: bool, now: i64) -> Value {
    let r = &c.call;
    let first = r.src_list.first();
    let unit = first.map_or(0, |s| s.src);
    let unit_tag = first.map_or(String::new(), |s| if s.tag.is_empty() { s.tag_ota.clone() } else { s.tag.clone() });
    let sys_num = d.sys_num(c.system);
    let file = match (&c.files.m4a, prefer_m4a) {
        (Some(m4a), true) => file_name(m4a),
        _ => file_name(&c.files.wav),
    };
    let mut v = json!({
        "id": format!("{sys_num}_{}_{}", r.talkgroup, r.start_time),
        "call_num": r.call_num,
        "sys_num": sys_num,
        "sys_name": r.short_name,
        "freq": r.freq as f64,
        "unit": unit,
        "unit_alpha_tag": unit_tag,
        "talkgroup": r.talkgroup,
        "talkgroup_alpha_tag": r.talkgroup_tag,
        "talkgroup_description": r.talkgroup_description,
        "talkgroup_group": r.talkgroup_group,
        "talkgroup_tag": r.talkgroup_group_tag,
        "talkgroup_patches": patch_string(&r.patched_talkgroups),
        "elapsed": r.stop_time - r.start_time,
        "length": round2(r.call_length),
        "call_state": -1,
        "call_state_type": "COMPLETED",
        "mon_state": 0,
        "mon_state_type": "UNSPECIFIED",
        "audio_type": r.audio_type,
        "phase2_tdma": r.phase2_tdma,
        "tdma_slot": r.tdma_slot,
        "analog": r.audio_type == "analog",
        "rec_num": r.extra.get("recorder_num").and_then(Value::as_i64).unwrap_or(-1),
        "src_num": r.extra.get("source_num").and_then(Value::as_i64).unwrap_or(-1),
        "rec_state": 6,
        "rec_state_type": "STOPPED",
        "conventional": c.system == CONVENTIONAL,
        "encrypted": r.encrypted,
        "emergency": r.emergency,
        "start_time": r.start_time,
        "stop_time": r.stop_time,
        "process_call_time": now,
        "error_count": r.error_count(),
        "spike_count": r.spike_count(),
        "retry_attempt": 0,
        "freq_error": extra_f64(c, "freq_error"),
        "signal": round2(extra_f64(c, "signal")),
        "noise": round2(extra_f64(c, "noise")),
        "call_filename": file,
    });
    // Trunk Recorder Pro's own measurements, when it made them (not in Trunk Recorder's message).
    for k in ["snr", "clean_voice_pct"] {
        if let Some(x) = r.extra.get(k).and_then(Value::as_f64) {
            v[k] = json!(round2(x));
        }
    }
    // The call JSON writes -1 for no cipher heard.
    let id = |k: &str| r.extra.get(k).and_then(Value::as_i64).filter(|x| *x >= 0);
    cipher(&mut v, id("algid"), id("key_id"), r.decrypted);
    v
}

/// Each transmission of a recorded call (`unit_topic/<short name>/end`).
pub fn unit_ends(d: &Directory, c: &ConcludedCall) -> Vec<Value> {
    let r = &c.call;
    // Decoder errors by stretch of audio, to share out between transmissions.
    let errors: Vec<(f64, u64)> = r
        .extra
        .get("errorList")
        .and_then(Value::as_array)
        .map(|a| a.iter().map(|e| (e["pos"].as_f64().unwrap_or(0.0), e["error_count"].as_u64().unwrap_or(0))).collect())
        .unwrap_or_default();
    let n = r.src_list.len();
    (0..n)
        .map(|i| {
            let s = &r.src_list[i];
            let end = if i + 1 < n { r.src_list[i + 1].pos } else { r.call_length };
            let length = (end - s.pos).max(0.0);
            let error_count: u64 = errors.iter().filter(|(p, _)| *p >= s.pos && (*p < end || i + 1 == n)).map(|(_, e)| e).sum();
            let stop = s.time + length.round() as i64;
            let mut head = json!({
                "sys_num": d.sys_num(c.system),
                "sys_name": r.short_name,
                "unit": s.src,
                "unit_alpha_tag": if s.tag.is_empty() { &s.tag_ota } else { &s.tag },
                "talkgroup": r.talkgroup,
                "talkgroup_alpha_tag": r.talkgroup_tag,
                "talkgroup_description": r.talkgroup_description,
                "talkgroup_group": r.talkgroup_group,
                "talkgroup_tag": r.talkgroup_group_tag,
                "talkgroup_patches": patch_string(&r.patched_talkgroups),
                "call_num": r.call_num,
                "freq": r.freq as f64,
                "position": round2(s.pos),
                "length": round2(length),
                "emergency": s.emergency,
                "encrypted": r.encrypted,
                "start_time": s.time,
                "stop_time": stop,
                "error_count": error_count,
                "spike_count": 0,
                "sample_count": (length * 8000.0).round() as u64,
                "transmission_filename": file_name(&c.files.wav),
            });
            // Where the radio said it was (GPS), when it did.
            if let (Some(lat), Some(lon)) = (s.lat, s.lon) {
                head["lat"] = json!(lat);
                head["lon"] = json!(lon);
            }
            head
        })
        .collect()
}

/// A unit event: its subtopic and body; None for kinds this plugin doesn't send.
pub fn unit_event(d: &Directory, e: &UnitEvent) -> Option<(&'static str, Value)> {
    let sub = match e.kind.as_str() {
        "registration" => "on",
        "deregistration" => "off",
        "acknowledge" => "ackresp",
        "affiliation" => "join",
        "data_grant" => "data",
        "answer_request" => "ans_req",
        "location" => "location",
        "call_alert" => "call_alert",
        "emergency_alarm" => "emergency",
        _ => return None,
    };
    let head = json!({ "sys_num": d.sys_num(e.system), "sys_name": e.short_name, "unit": e.unit, "unit_alpha_tag": d.unit_tag(e.system, e.unit) });
    let body = match (sub, e.talkgroup) {
        ("call_alert", target) => {
            let target = target.unwrap_or(0);
            extend(head, json!({ "target_unit": target, "target_unit_alpha_tag": d.unit_tag(e.system, target) }))
        }
        (_, Some(tg)) => extend(head, d.tg_fields(e.system, tg, &[])),
        (_, None) => head,
    };
    Some((sub, body))
}

/// Control channel decode rates (`rates`); conventional channels have none.
pub fn rates(d: &Directory, s: &Status, interval: f64) -> Value {
    Value::Array(
        s.systems
            .iter()
            .filter(|y| y.index != CONVENTIONAL)
            .map(|y| {
                json!({
                    "sys_num": d.sys_num(y.index),
                    "sys_name": y.short_name,
                    "decoderate": round2(y.decode_rate),
                    "decoderate_interval": round2(interval),
                    "control_channel": y.control_channel_hz.unwrap_or(0) as f64,
                })
            })
            .collect(),
    )
}

/// The systems (`systems`, retained).
pub fn systems(d: &Directory) -> Value {
    Value::Array(
        d.systems
            .iter()
            .enumerate()
            .map(|(i, (_, name, kind))| json!({ "sys_num": i, "sys_name": name, "type": kind, "sysid": "", "wacn": "", "nac": "", "rfss": 0, "site_id": 0 }))
            .collect(),
    )
}

/// What the recorder is set up with (`config`, retained).
pub fn config(d: &Directory, capture_dir: &Path, instance_id: &str) -> Value {
    let systems: Vec<Value> = d.systems.iter().enumerate().map(|(i, (_, name, kind))| json!({ "sys_num": i, "sys_name": name, "system_type": kind })).collect();
    json!({ "systems": systems, "capture_dir": capture_dir.display().to_string(), "instance_id": instance_id })
}

#[cfg(test)]
mod tests {
    use super::*;
    use trunk_recorder_plugin::{CallRecord, SrcEntry, SystemStatus};

    fn dir() -> Directory {
        Directory {
            systems: vec![(0, "east".into(), "p25".into()), (1, "west".into(), "p25".into()), (CONVENTIONAL, "conv".into(), "conventional".into())],
            ..Default::default()
        }
    }

    #[test]
    fn active_call_matches_trunk_recorder() {
        let d = dir();
        let c = CallInfo {
            id: 42,
            system: CONVENTIONAL,
            short_name: "conv".into(),
            talkgroup: 101,
            freq_hz: 154_430_000,
            analog: true,
            recording: false,
            reason: Some("encrypted".into()),
            start_time: 1_700_000_000.4,
            units: vec![7],
            ..Default::default()
        };
        let v = active_call(&d, &c, 1_700_000_003.0);
        assert_eq!(v["id"], "2_101_1700000000");
        assert_eq!(v["sys_num"], 2);
        assert_eq!((v["mon_state"].as_i64(), v["mon_state_type"].as_str()), (Some(5), Some("ENCRYPTED")));
        assert_eq!(v["call_state_type"], "MONITORING");
        assert_eq!(v["audio_type"], "analog");
        assert_eq!(v["elapsed"], 3);
        assert_eq!(v["conventional"], true);
        let keys: Vec<&str> = v.as_object().unwrap().keys().map(|k| k.as_str()).collect();
        assert_eq!(&keys[..9], ["id", "call_num", "sys_num", "sys_name", "freq", "unit", "unit_alpha_tag", "talkgroup", "talkgroup_alpha_tag"]);
    }

    #[test]
    fn transmissions_share_out_errors() {
        let d = dir();
        let mut call = CallRecord { short_name: "west".into(), talkgroup: 9, call_length: 10.0, start_time: 100, stop_time: 110, ..Default::default() };
        call.src_list = vec![
            SrcEntry { src: 1, time: 100, pos: 0.0, tag_ota: "ENG 1".into(), ..Default::default() },
            SrcEntry { src: 2, time: 104, pos: 4.0, lat: Some(30.0529), lon: Some(-83.17565), ..Default::default() },
        ];
        call.extra.insert("errorList".into(), json!([{ "pos": 0.0, "error_count": 3 }, { "pos": 5.0, "error_count": 4 }, { "pos": 9.5, "error_count": 1 }]));
        call.extra.insert("snr".into(), json!(18.5));
        call.extra.insert("clean_voice_pct".into(), Value::Null);
        // No cipher heard.
        call.extra.insert("algid".into(), json!(-1));
        call.extra.insert("key_id".into(), json!(-1));
        let c = ConcludedCall { path: "west/x".into(), system: 1, call, ..Default::default() };
        let ends = unit_ends(&d, &c);
        assert_eq!(ends.len(), 2);
        assert_eq!((ends[0]["length"].as_f64(), ends[0]["error_count"].as_u64()), (Some(4.0), Some(3)));
        assert_eq!((ends[1]["length"].as_f64(), ends[1]["error_count"].as_u64()), (Some(6.0), Some(5)));
        assert_eq!(ends[0]["unit_alpha_tag"], "ENG 1");
        assert_eq!(ends[1]["stop_time"], 110);
        assert!(ends[0].get("lat").is_none());
        assert_eq!((ends[1]["lat"].as_f64(), ends[1]["lon"].as_f64()), (Some(30.0529), Some(-83.17565)));
        let end = call_end(&d, &c, false, 200);
        assert_eq!(end["id"], "1_9_100");
        assert_eq!(end["unit_alpha_tag"], "ENG 1");
        assert_eq!((end["snr"].as_f64(), end["decrypted"].as_bool()), (Some(18.5), Some(false)));
        assert!(end.get("clean_voice_pct").is_none(), "not measured: left out");
        assert!(end.get("algid").is_none() && end.get("key_id").is_none(), "no cipher heard: left out");
        // An encrypted call with its cipher, from the call JSON as the recorder writes it.
        let call: CallRecord = serde_json::from_value(json!({ "encrypted": 1, "decrypted": 1, "algid": 0x84, "key_id": 4097 })).unwrap();
        let end = call_end(&d, &ConcludedCall { system: 1, call, ..Default::default() }, false, 200);
        assert_eq!((end["algid"].as_i64(), end["key_id"].as_i64(), end["decrypted"].as_bool()), (Some(0x84), Some(4097), Some(true)));
    }

    #[test]
    fn calls_not_recorded_say_why() {
        let d = dir();
        let c = CallInfo {
            id: 9,
            system: 0,
            short_name: "east".into(),
            talkgroup: 5,
            encrypted: true,
            algid: Some(0xAA),
            key_id: Some(0x1B50),
            recording: false,
            reason: Some("encrypted".into()),
            start_time: 100.0,
            ..Default::default()
        };
        let v = ignored_call(&d, &c, 108.5);
        assert_eq!(
            (v["reason"].as_str(), v["mon_state_type"].as_str(), v["call_state_type"].as_str()),
            (Some("encrypted"), Some("ENCRYPTED"), Some("COMPLETED"))
        );
        assert_eq!((v["stop_time"].as_i64(), v["length"].as_f64()), (Some(108), Some(8.5)));
        assert_eq!((v["algid"].as_i64(), v["key_id"].as_i64(), v["decrypted"].as_bool()), (Some(0xAA), Some(0x1B50), Some(false)));
        let c = CallInfo { reason: Some("ignored".into()), encrypted: false, algid: None, key_id: None, ..c };
        let v = ignored_call(&d, &c, 101.0);
        assert_eq!((v["reason"].as_str(), v["mon_state_type"].as_str()), (Some("ignored"), Some("IGNORED_TG")));
        assert!(v.get("algid").is_none());
    }

    #[test]
    fn unit_events_use_learned_names() {
        let mut d = dir();
        let mut call = CallRecord { talkgroup: 5, talkgroup_tag: "Fire Disp".into(), talkgroup_group_tag: "Fire Dispatch".into(), ..Default::default() };
        call.src_list = vec![SrcEntry { src: 77, tag: "Medic 7".into(), ..Default::default() }];
        d.learn_concluded(&ConcludedCall { system: 0, call, ..Default::default() });
        let e = UnitEvent { system: 0, short_name: "east".into(), kind: "affiliation".into(), unit: 77, talkgroup: Some(5), time: 0.0 };
        let (sub, v) = unit_event(&d, &e).unwrap();
        assert_eq!(sub, "join");
        assert_eq!(v["unit_alpha_tag"], "Medic 7");
        assert_eq!((v["talkgroup_alpha_tag"].as_str(), v["talkgroup_tag"].as_str()), (Some("Fire Disp"), Some("Fire Dispatch")));
        let e = UnitEvent { kind: "call_alert".into(), unit: 3, talkgroup: Some(77), ..e };
        let (sub, v) = unit_event(&d, &e).unwrap();
        assert_eq!((sub, v["target_unit"].as_u64(), v["target_unit_alpha_tag"].as_str()), ("call_alert", Some(77), Some("Medic 7")));
        let e = UnitEvent { kind: "emergency_alarm".into(), unit: 77, talkgroup: Some(5), ..e };
        let (sub, v) = unit_event(&d, &e).unwrap();
        assert_eq!((sub, v["unit_alpha_tag"].as_str(), v["talkgroup_alpha_tag"].as_str()), ("emergency", Some("Medic 7"), Some("Fire Disp")));
        assert!(unit_event(&d, &UnitEvent { kind: "something_new".into(), ..Default::default() }).is_none());
    }

    #[test]
    fn rates_skip_conventional() {
        let d = dir();
        let s = Status {
            time: 0.0,
            systems: vec![
                SystemStatus { index: 1, short_name: "west".into(), control_channel_hz: Some(851_012_500), decode_rate: 38.456, ..Default::default() },
                SystemStatus { index: CONVENTIONAL, short_name: "conv".into(), ..Default::default() },
            ],
        };
        let v = rates(&d, &s, 5.0);
        assert_eq!(v.as_array().unwrap().len(), 1);
        assert_eq!((v[0]["sys_num"].as_u64(), v[0]["decoderate"].as_f64()), (Some(1), Some(38.46)));
    }
}
