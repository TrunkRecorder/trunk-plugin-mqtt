//! MQTT — a Trunk Recorder Pro plugin that publishes calls, unit activity and
//! system status to an MQTT broker, in the format of Trunk Recorder's MQTT
//! Status plugin.

mod homeassistant;
mod payload;
mod publisher;
mod transport;

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use trunk_recorder_plugin::{base64, topic, CallInfo, ConcludedCall, Host, Manifest, NoConfig, Outcome, Plugin, Setup, Status, UnitEvent, CONVENTIONAL};

use payload::Directory;
use publisher::{unix_now, Kind, Limits, Publisher};
use transport::Broker;

#[derive(Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", default)]
struct Config {
    /// Broker
    ///
    /// The broker's address, with its protocol and port: tcp://host:1883,
    /// ssl://host:8883, ws://host:9001/mqtt or wss://host/mqtt.
    #[schemars(extend("x-required" = true))]
    broker: String,
    /// Topic
    ///
    /// The base topic. Calls, rates and the recorder's status are published
    /// under it (topic/call_start, topic/rates, …).
    #[schemars(extend("x-required" = true))]
    topic: String,
    /// Unit topic
    ///
    /// Publishes radio activity (registrations, affiliations, transmissions)
    /// under this topic, then the system's short name. Leave it empty to not
    /// publish it.
    #[serde(alias = "unit_topic")]
    unit_topic: String,
    /// Username
    username: String,
    /// Password
    #[schemars(extend("x-secret" = true))]
    password: String,
    /// Client ID
    ///
    /// Leave it empty for one made from the broker and topic.
    #[serde(alias = "client_id")]
    client_id: String,
    /// Instance ID
    ///
    /// Added to every message as instance_id, to tell recorders apart.
    #[serde(alias = "instance_id")]
    instance_id: String,
    /// QoS
    ///
    /// How hard the broker tries to deliver each message.
    // A list rather than a number to type: the settings form draws it as a menu.
    #[schemars(extend("enum" = [0, 1, 2], "x-enum-labels" = ["0: at most once", "1: at least once", "2: exactly once"]))]
    qos: u8,
    /// MQTT version
    ///
    /// MQTT 5 needs a broker that supports it.
    #[serde(alias = "mqtt_version")]
    #[schemars(extend("enum" = [3, 5], "x-enum-labels" = ["3.1.1", "5"]))]
    mqtt_version: u8,
    /// Send call audio
    ///
    /// Publishes each recorded call's audio, base64-encoded, with its details
    /// to topic/audio.
    #[serde(alias = "mqtt_audio")]
    mqtt_audio: bool,
    /// Audio format
    ///
    /// M4A is there when the recorder encodes it for another plugin (OpenMHz,
    /// Broadcastify); otherwise only WAV is sent.
    #[serde(alias = "mqtt_audio_type")]
    mqtt_audio_type: AudioType,
    /// Queue age limit
    ///
    /// Seconds a message may wait for the broker before it's dropped.
    #[serde(alias = "queue_max_age")]
    #[schemars(range(min = 1, max = 86400))]
    queue_max_age: u64,
    /// Audio age limit
    ///
    /// The same, for call audio.
    #[serde(alias = "audio_max_age")]
    #[schemars(range(min = 1, max = 86400))]
    audio_max_age: u64,
    /// Queue size limit (MB)
    ///
    /// Past this, the oldest waiting messages are dropped.
    #[serde(alias = "queue_max_mb")]
    #[schemars(range(min = 1, max = 4096))]
    queue_max_mb: u64,
    /// Heartbeat interval
    ///
    /// Seconds between heartbeats and pings to the broker. A broker that
    /// doesn't answer a ping by the next one is reconnected. 0 turns them off.
    #[serde(alias = "heartbeat_interval")]
    #[schemars(range(min = 0, max = 3600))]
    heartbeat_interval: u64,
    /// Broker timeout
    ///
    /// Seconds a connection attempt or a send may take. Raise it to send
    /// audio over a slow connection.
    #[serde(alias = "heartbeat_timeout")]
    #[schemars(range(min = 1, max = 600))]
    heartbeat_timeout: u64,
    /// Home Assistant discovery
    ///
    /// Publishes the entities Home Assistant adds on its own for the recorder:
    /// its status, active calls, and each system's decode rate, control
    /// channel and calls, and the last call heard.
    #[serde(alias = "home_assistant")]
    home_assistant: bool,
    /// Discovery prefix
    ///
    /// The prefix Home Assistant's MQTT integration listens under. Change it
    /// only if it was changed there.
    #[serde(alias = "discovery_prefix")]
    discovery_prefix: String,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            broker: "tcp://localhost:1883".into(),
            topic: String::new(),
            unit_topic: String::new(),
            username: String::new(),
            password: String::new(),
            client_id: String::new(),
            instance_id: "trunk-recorder".into(),
            qos: 0,
            mqtt_version: 3,
            mqtt_audio: false,
            mqtt_audio_type: AudioType::Wav,
            queue_max_age: 60,
            audio_max_age: 300,
            queue_max_mb: 32,
            heartbeat_interval: 10,
            heartbeat_timeout: 30,
            home_assistant: false,
            discovery_prefix: "homeassistant".into(),
        }
    }
}

#[derive(Serialize, Deserialize, JsonSchema, Clone, Copy, PartialEq)]
#[serde(rename_all = "lowercase")]
enum AudioType {
    /// WAV
    Wav,
    /// M4A
    M4a,
    /// WAV and M4A
    Both,
    /// None (details only)
    None,
}

struct Active {
    dir: Directory,
    calls: BTreeMap<u32, CallInfo>,
}

struct Mqtt {
    host: Host,
    publisher: Publisher,
    topic: String,
    unit_topic: Option<String>,
    audio: Option<AudioType>,
    state: Arc<Mutex<Active>>,
    stop_ticker: Arc<AtomicBool>,
    last_status: Option<Instant>,
}

impl Plugin for Mqtt {
    type Config = Config;
    type SystemConfig = NoConfig;

    fn manifest() -> Manifest {
        Manifest {
            name: "MQTT".into(),
            subscribe: vec![topic::CALL_START.into(), topic::CALL_END.into(), topic::CALL_CONCLUDED.into(), topic::UNIT.into(), topic::STATUS.into()],
            ..trunk_recorder_plugin::manifest!()
        }
    }

    fn start(host: Host, setup: Setup<Config, NoConfig>) -> Result<Self, String> {
        let c = setup.config;
        let broker = Broker::parse(&c.broker)?;
        let topic = clean_topic(&c.topic);
        if topic.is_empty() {
            return Err("Set the topic to publish under.".into());
        }
        let unit_topic = Some(clean_topic(&c.unit_topic)).filter(|t| !t.is_empty());
        for t in std::iter::once(&topic).chain(&unit_topic) {
            if !transport::valid_topic(t) {
                return Err(format!("The topic \"{t}\" can't have + or # in it."));
            }
        }
        let discovery_prefix = clean_topic(&c.discovery_prefix);
        if c.home_assistant && !transport::valid_topic(&discovery_prefix) {
            return Err("The discovery prefix can't be empty or have + or # in it.".into());
        }
        if c.qos > 2 {
            return Err("QoS is 0, 1 or 2.".into());
        }
        if c.mqtt_version != 3 && c.mqtt_version != 5 {
            return Err("The MQTT version is 3 (for 3.1.1) or 5.".into());
        }
        let client_id = if c.client_id.trim().is_empty() { default_client_id(&c.broker, &topic) } else { c.client_id.trim().to_string() };
        let instance_id = if c.instance_id.trim().is_empty() { "trunk-recorder".to_string() } else { c.instance_id.trim().to_string() };
        let heartbeat = (c.heartbeat_interval > 0).then(|| Duration::from_secs(c.heartbeat_interval.max(5)));
        let settings = transport::Settings {
            broker,
            client_id: client_id.clone(),
            username: c.username.clone(),
            password: c.password.clone(),
            version: c.mqtt_version,
            // rumqttc pings every keep_alive and reconnects when the last one went unanswered.
            keep_alive: heartbeat.unwrap_or(Duration::ZERO),
            timeout: c.heartbeat_timeout.max(1),
        };
        let limits = Limits {
            max_age: Duration::from_secs(c.queue_max_age.max(1)),
            audio_max_age: Duration::from_secs(c.audio_max_age.max(1)),
            max_bytes: (c.queue_max_mb.max(1) as usize) << 20,
            heartbeat,
        };
        host.info(format!(
            "broker {}, client id {client_id}, topic {topic}, units {}, audio {}, QoS {}, MQTT {}",
            c.broker,
            unit_topic.as_deref().unwrap_or("off"),
            if c.mqtt_audio { serde_json::to_string(&c.mqtt_audio_type).unwrap_or_default().replace('"', "") } else { "off".into() },
            c.qos,
            if c.mqtt_version == 5 { "5" } else { "3.1.1" },
        ));
        let publisher = Publisher::start(settings, limits, c.qos, &topic, &instance_id, host.clone());

        let dir = Directory::new(setup.systems.iter().map(|s| (s.index, s.short_name.clone(), s.kind.clone())));
        publisher.send(&topic, Kind::Retained, "systems", "systems", payload::systems(&dir));
        publisher.send(&topic, Kind::Retained, "config", "config", payload::config(&dir, &setup.capture_dir, &instance_id));
        if c.home_assistant {
            let controlled: Vec<String> = setup.systems.iter().filter(|s| s.index != CONVENTIONAL).map(|s| s.short_name.clone()).collect();
            let discovery = homeassistant::Discovery { prefix: &discovery_prefix, topic: &topic, instance_id: &instance_id, systems: &controlled };
            for (t, v) in discovery.messages() {
                publisher.send_raw(&t, Kind::Retained, serde_json::to_vec(&v).unwrap_or_default());
            }
        }
        let state = Arc::new(Mutex::new(Active { dir, calls: BTreeMap::new() }));
        let stop_ticker = Arc::new(AtomicBool::new(false));
        start_ticker(publisher.clone(), topic.clone(), state.clone(), stop_ticker.clone());

        Ok(Mqtt { host, publisher, topic, unit_topic, audio: c.mqtt_audio.then_some(c.mqtt_audio_type), state, stop_ticker, last_status: None })
    }

    fn call_start(&mut self, call: CallInfo) {
        let mut st = self.state.lock().unwrap();
        st.dir.learn_start(&call);
        if let Some(t) = &self.unit_topic {
            if let Some(v) = payload::unit_call(&st.dir, &call) {
                self.publisher.send(&format!("{t}/{}", call.short_name), Kind::Event, "call", "call", v);
            }
        }
        self.publisher.send(&self.topic, Kind::Event, "call", "call_start", payload::active_call(&st.dir, &call, now_f64()));
        st.calls.insert(call.id, call);
    }

    fn call_end(&mut self, call: CallInfo) {
        let mut st = self.state.lock().unwrap();
        st.calls.remove(&call.id);
        // A recorded call is published once its files are written (call_concluded).
        if !call.recording {
            let v = payload::ignored_call(&st.dir, &call, now_f64());
            drop(st);
            self.publisher.send(&self.topic, Kind::Event, "call", "call_ignored", v);
        }
    }

    fn call_concluded(&mut self, call: ConcludedCall) {
        let mut st = self.state.lock().unwrap();
        st.dir.learn_concluded(&call);
        let prefer_m4a = matches!(self.audio, Some(AudioType::M4a));
        let end = payload::call_end(&st.dir, &call, prefer_m4a, unix_now());
        let ends = if self.unit_topic.is_some() { payload::unit_ends(&st.dir, &call) } else { Vec::new() };
        drop(st);
        if let Some(t) = &self.unit_topic {
            let t = format!("{t}/{}", call.call.short_name);
            for v in ends {
                self.publisher.send(&t, Kind::Event, "end", "end", v);
            }
        }
        if let Some(kind) = self.audio {
            match audio(&call, kind) {
                Ok(v) => self.publisher.send(&self.topic, Kind::Audio, "call", "audio", v),
                Err(e) => self.host.warn(format!("{}: audio not sent: {e}", call.path)),
            }
        }
        self.publisher.send(&self.topic, Kind::Event, "call", "call_end", end);
        // Tell the recorder we're done with the call's files (the audio was read above),
        // or it keeps them and counts the call as failed after an hour. Ok shows as
        // "sent" on the dashboard; skipped means it's queued for a broker that's away.
        if self.publisher.connected() {
            self.host.call_result(&call.path, Outcome::Ok, "", "");
        } else {
            self.host.call_result(&call.path, Outcome::Skipped, "queued: the broker isn't connected", "");
        }
    }

    fn unit(&mut self, event: UnitEvent) {
        let Some(t) = &self.unit_topic else { return };
        let st = self.state.lock().unwrap();
        if let Some((sub, v)) = payload::unit_event(&st.dir, &event) {
            self.publisher.send(&format!("{t}/{}", event.short_name), Kind::Event, sub, sub, v);
        }
    }

    fn status(&mut self, status: Status) {
        let interval = self.last_status.map_or(0.0, |t| t.elapsed().as_secs_f64());
        self.last_status = Some(Instant::now());
        let mut st = self.state.lock().unwrap();
        self.publisher.send(&self.topic, Kind::Snapshot, "rates", "rates", payload::rates(&st.dir, &status, interval));
        // A system's identity, as its control channel states it, as Trunk Recorder
        // sends it: the whole list again (retained), then the system that changed.
        let changed = st.dir.learn_identity(&status);
        if !changed.is_empty() {
            self.publisher.send(&self.topic, Kind::Retained, "systems", "systems", payload::systems(&st.dir));
            for index in changed {
                if let Some(v) = payload::system(&st.dir, index) {
                    self.publisher.send(&self.topic, Kind::Event, "system", "system", v);
                }
            }
        }
    }

    fn shutdown(&mut self, grace: Duration) {
        self.stop_ticker.store(true, Ordering::Relaxed);
        self.publisher.shutdown(grace);
    }
}

// start_ticker()
//   Publish the active calls once a second, as Trunk Recorder does.
fn start_ticker(publisher: Publisher, topic: String, state: Arc<Mutex<Active>>, stop: Arc<AtomicBool>) {
    let _ = std::thread::Builder::new().name("mqtt-calls".into()).spawn(move || {
        while !stop.load(Ordering::Relaxed) {
            std::thread::sleep(Duration::from_secs(1));
            let now = now_f64();
            let mut st = state.lock().unwrap();
            // A call whose end never came.
            st.calls.retain(|_, c| now - c.start_time < 12.0 * 3600.0);
            let calls: Vec<Value> = st.calls.values().map(|c| payload::active_call(&st.dir, c, now)).collect();
            drop(st);
            publisher.send(&topic, Kind::Snapshot, "calls", "calls_active", Value::Array(calls));
        }
    });
}

/// `topic/audio`: the call's audio as base64, and its call JSON.
fn audio(call: &ConcludedCall, kind: AudioType) -> Result<Value, String> {
    let mut metadata: Value = match std::fs::read(&call.files.json) {
        Ok(b) => serde_json::from_slice(&b).map_err(|e| format!("{}: {e}", call.files.json.display()))?,
        Err(_) => serde_json::to_value(&call.call).unwrap_or_default(),
    };
    let read = |p: &PathBuf| std::fs::read(p).map(|b| base64::encode(&b)).map_err(|e| format!("{}: {e}", p.display()));
    let name = |p: &PathBuf| p.file_name().map_or(String::new(), |n| n.to_string_lossy().into_owned());
    let (mut wav, mut m4a) = (String::new(), String::new());
    if matches!(kind, AudioType::M4a | AudioType::Both) {
        if let Some(p) = &call.files.m4a {
            m4a = read(p)?;
            metadata["filename"] = name(p).into();
        }
    }
    if matches!(kind, AudioType::Wav | AudioType::Both) {
        wav = read(&call.files.wav)?;
        metadata["filename"] = name(&call.files.wav).into();
    }
    Ok(json!({ "audio_wav_base64": wav, "audio_m4a_base64": m4a, "metadata": metadata }))
}

fn clean_topic(t: &str) -> String {
    t.trim().trim_end_matches('/').to_string()
}

/// `tr-status-<crc32 of broker/topic>`, as Trunk Recorder's plugin makes it.
fn default_client_id(broker: &str, topic: &str) -> String {
    format!("tr-status-{:x}", crc32(format!("{broker}/{topic}").as_bytes()))
}

fn crc32(b: &[u8]) -> u32 {
    let mut c = !0u32;
    for &x in b {
        c ^= x as u32;
        for _ in 0..8 {
            c = if c & 1 != 0 { (c >> 1) ^ 0xEDB8_8320 } else { c >> 1 };
        }
    }
    !c
}

fn now_f64() -> f64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map_or(0.0, |d| d.as_secs_f64())
}

fn main() {
    trunk_recorder_plugin::run::<Mqtt>();
}

#[cfg(test)]
mod tests {
    use super::*;
    use trunk_recorder_plugin::{testing, HostMessage, EXIT_CONFIG};

    #[test]
    fn client_id_is_trunk_recorders() {
        assert_eq!(crc32(b"123456789"), 0xCBF4_3926);
        assert_eq!(default_client_id("tcp://localhost:1883", "trunk"), format!("tr-status-{:x}", crc32(b"tcp://localhost:1883/trunk")));
    }

    #[test]
    fn trunk_recorder_settings_read_as_they_are() {
        let c: Config = serde_json::from_value(json!({
            "broker": "tcp://10.0.0.2:1883", "topic": "tr/feeds", "unit_topic": "tr/units", "mqtt_audio": true,
            "mqtt_audio_type": "both", "queue_max_mb": 8, "heartbeat_timeout": 45, "instance_id": "east"
        }))
        .unwrap();
        assert_eq!((c.unit_topic.as_str(), c.mqtt_audio, c.queue_max_mb, c.heartbeat_timeout), ("tr/units", true, 8, 45));
        assert!(c.mqtt_audio_type == AudioType::Both);
        assert_eq!(c.instance_id, "east");
    }

    #[test]
    fn qos_and_version_are_menus() {
        let schema = serde_json::to_value(schemars::schema_for!(Config)).unwrap();
        let p = &schema["properties"];
        assert_eq!((p["qos"]["enum"].clone(), p["mqttVersion"]["enum"].clone()), (json!([0, 1, 2]), json!([3, 5])));
        assert_eq!(p["mqttVersion"]["x-enum-labels"], json!(["3.1.1", "5"]));
    }

    #[test]
    fn bad_settings_stop_it() {
        for bad in [
            json!({ "topic": "" }),
            json!({ "topic": "a/#" }),
            json!({ "topic": "t", "broker": "localhost" }),
            json!({ "topic": "t", "mqttVersion": 4 }),
            json!({ "topic": "t", "homeAssistant": true, "discoveryPrefix": "/" }),
        ] {
            let dir = testing::temp_dir("mqtt");
            let out = testing::run::<Mqtt>([HostMessage::Hello(testing::hello(&dir, bad.clone()))]);
            assert_eq!(out.exit_code, EXIT_CONFIG, "{bad}");
        }
    }

    #[test]
    fn audio_payload_carries_the_call() {
        let dir = testing::temp_dir("mqtt");
        let call = testing::call(&dir, "sys1", 101);
        let v = audio(&call, AudioType::Both).unwrap();
        assert!(!v["audio_wav_base64"].as_str().unwrap().is_empty());
        assert!(!v["audio_m4a_base64"].as_str().unwrap().is_empty());
        assert_eq!(v["metadata"]["talkgroup"], 101);
        assert!(v["metadata"]["filename"].as_str().unwrap().ends_with(".wav"));
        let v = audio(&call, AudioType::None).unwrap();
        assert_eq!(v["audio_wav_base64"], "");
    }
}
