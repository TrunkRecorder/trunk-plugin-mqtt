//! The built plugin against a real broker (mosquitto; skipped without one).

use std::io::Write;
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde_json::{json, Value};
use trunk_recorder_plugin::{testing, CallInfo, HostMessage, Shutdown, Status, SystemStatus, UnitEvent};

struct Broker {
    child: Child,
    port: u16,
}

impl Drop for Broker {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn mosquitto() -> Option<Broker> {
    // (MOSQUITTO names one that isn't installed in the usual places.)
    let named = std::env::var("MOSQUITTO").ok();
    let bin = named
        .as_deref()
        .into_iter()
        .chain(["/usr/sbin/mosquitto", "/usr/local/sbin/mosquitto", "/opt/homebrew/sbin/mosquitto"])
        .find(|p| std::path::Path::new(p).exists())?;
    let port = std::net::TcpListener::bind("127.0.0.1:0").ok()?.local_addr().ok()?.port();
    // No config file (packaged mosquitto may only read its own): local-only, anonymous.
    let child = Command::new(bin).arg("-p").arg(port.to_string()).stdout(Stdio::null()).stderr(Stdio::null()).spawn().ok()?;
    let t0 = Instant::now();
    while std::net::TcpStream::connect(("127.0.0.1", port)).is_err() {
        if t0.elapsed() > Duration::from_secs(5) {
            return None;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    Some(Broker { child, port })
}

/// Every message published under `filter`, as (topic, JSON, retained).
fn subscribe(port: u16, filter: &str) -> Arc<Mutex<Vec<(String, Value, bool)>>> {
    let got = Arc::new(Mutex::new(Vec::new()));
    static N: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);
    let id = format!("test-sub-{}", N.fetch_add(1, std::sync::atomic::Ordering::Relaxed));
    let mut o = rumqttc::MqttOptions::new(id, "127.0.0.1", port);
    // Audio is bigger than the 10 KB default.
    o.set_max_packet_size(64 << 20, 64 << 20);
    let (client, mut conn) = rumqttc::Client::new(o, 100);
    client.subscribe(filter, rumqttc::QoS::AtMostOnce).unwrap();
    let g = got.clone();
    std::thread::spawn(move || {
        let _client = client;
        for ev in conn.iter() {
            match ev {
                Ok(rumqttc::Event::Incoming(rumqttc::Packet::Publish(p))) => {
                    let v = serde_json::from_slice(&p.payload).unwrap_or(Value::Null);
                    g.lock().unwrap().push((p.topic.clone(), v, p.retain));
                }
                Ok(_) => {}
                Err(_) => return,
            }
        }
    });
    got
}

fn line(m: &HostMessage) -> String {
    serde_json::to_string(m).unwrap() + "\n"
}

/// The broker's state and the bytes sent, from each metrics message the plugin wrote.
fn broker_states(log: &str) -> Vec<(String, u64)> {
    log.lines()
        .filter_map(|l| serde_json::from_str::<Value>(l).ok())
        .filter(|v| v["type"] == "metrics")
        .map(|v| (v["endpoints"][0]["state"].as_str().unwrap_or("").to_string(), v["bytesSent"].as_u64().unwrap_or(0)))
        .collect()
}

#[test]
fn publishes_to_a_broker() {
    let Some(broker) = mosquitto() else {
        eprintln!("no mosquitto: skipped");
        return;
    };
    let got = subscribe(broker.port, "#");
    std::thread::sleep(Duration::from_millis(300));

    let dir = testing::temp_dir("mqtt-e2e");
    let hello = testing::hello(
        &dir,
        json!({ "broker": format!("tcp://127.0.0.1:{}", broker.port), "topic": "tr/feeds", "unitTopic": "tr/units", "mqttAudio": true, "instanceId": "test" }),
    );
    let mut plugin = Command::new(env!("CARGO_BIN_EXE_mqtt")).stdin(Stdio::piped()).stdout(Stdio::piped()).stderr(Stdio::inherit()).spawn().unwrap();
    let mut stdin = plugin.stdin.take().unwrap();
    let out = plugin.stdout.take().unwrap();
    let log = std::thread::spawn(move || std::io::read_to_string(out).unwrap_or_default());
    let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs_f64();
    let start = CallInfo {
        id: 5,
        system: 0,
        short_name: "sys1".into(),
        talkgroup: 101,
        freq_hz: 851_012_500,
        recording: true,
        start_time: now,
        units: vec![1234],
        ..Default::default()
    };
    let unit = UnitEvent { system: 0, short_name: "sys1".into(), kind: "affiliation".into(), unit: 1234, talkgroup: Some(101), time: now };
    let status = Status {
        time: now,
        systems: vec![SystemStatus {
            index: 0,
            short_name: "sys1".into(),
            control_channel_hz: Some(851_000_000),
            decode_rate: 40.0,
            nac: Some(0xBCC),
            wacn: Some(0xBBBBB),
            sys_id: Some(0x555),
            rfss: Some(1),
            site: Some(7),
            ..Default::default()
        }],
    };
    for m in [HostMessage::Hello(hello), HostMessage::CallStart(start.clone()), HostMessage::Unit(unit), HostMessage::Status(status)] {
        stdin.write_all(line(&m).as_bytes()).unwrap();
    }
    // A second of calls_active with the call in it.
    std::thread::sleep(Duration::from_millis(1500));
    // And a call that wasn't recorded: it only ends.
    let skipped = CallInfo { id: 6, talkgroup: 202, recording: false, reason: Some("no_recorder".into()), ..start.clone() };
    for m in [
        HostMessage::CallEnd(start),
        HostMessage::CallEnd(skipped),
        HostMessage::CallConcluded(testing::call(&dir, "sys1", 101)),
        HostMessage::Shutdown(Shutdown { grace_s: 5.0 }),
    ] {
        stdin.write_all(line(&m).as_bytes()).unwrap();
    }
    drop(stdin);
    let t0 = Instant::now();
    let code = loop {
        if let Some(s) = plugin.try_wait().unwrap() {
            break s.code();
        }
        assert!(t0.elapsed() < Duration::from_secs(10), "the plugin didn't exit");
        std::thread::sleep(Duration::from_millis(20));
    };
    assert_eq!(code, Some(0));
    std::thread::sleep(Duration::from_millis(300));
    let log = log.join().unwrap();
    assert!(!log.contains("\"warn\""), "{log}");
    // The concluded call was settled (sent, while connected), so the host doesn't hold its files.
    assert!(log.lines().any(|l| l.contains("\"call.result\"") && l.contains("\"ok\"")), "{log}");
    // The dashboard heard the broker was up, and what went out.
    let metrics = broker_states(&log);
    assert!(metrics.iter().any(|(s, _)| s == "up"), "{log}");
    assert!(metrics.iter().any(|(_, sent)| *sent > 0), "{log}");

    let got = got.lock().unwrap().clone();
    let topics: Vec<&str> = got.iter().map(|(t, _, _)| t.as_str()).collect();
    for t in [
        "tr/feeds/systems",
        "tr/feeds/config",
        "tr/feeds/call_start",
        "tr/feeds/calls_active",
        "tr/feeds/rates",
        "tr/feeds/audio",
        "tr/feeds/call_end",
        "tr/feeds/call_ignored",
        "tr/feeds/system",
        "tr/units/sys1/call",
        "tr/units/sys1/join",
        "tr/units/sys1/end",
    ] {
        assert!(topics.contains(&t), "{t} missing from {topics:?}");
    }
    let statuses: Vec<&str> =
        got.iter().filter(|(t, _, _)| t == "tr/feeds/trunk_recorder/status").map(|(_, v, _)| v["status"].as_str().unwrap_or("")).collect();
    assert_eq!(statuses, ["connected", "disconnected"]);
    let find = |t: &str| got.iter().find(|(x, _, _)| x == t).map(|(_, v, r)| (v.clone(), *r)).unwrap();
    let (systems, _) = find("tr/feeds/systems");
    assert_eq!(systems["instance_id"], "test");
    assert_eq!(systems["systems"][0]["sys_name"], "sys1");
    let (ignored, _) = find("tr/feeds/call_ignored");
    assert_eq!((ignored["call"]["talkgroup"].as_u64(), ignored["call"]["reason"].as_str()), (Some(202), Some("no_recorder")));
    let (start, _) = find("tr/feeds/call_start");
    assert_eq!((start["type"].as_str(), start["call"]["talkgroup"].as_u64(), start["call"]["unit"].as_u64()), (Some("call_start"), Some(101), Some(1234)));
    let active = got.iter().filter(|(t, _, _)| t == "tr/feeds/calls_active").map(|(_, v, _)| v["calls"].as_array().map_or(0, |a| a.len())).max();
    assert_eq!(active, Some(1));
    let (audio, _) = find("tr/feeds/audio");
    assert!(!audio["call"]["audio_wav_base64"].as_str().unwrap().is_empty());
    assert_eq!(audio["call"]["metadata"]["talkgroup"], 101);
    // A subscriber that arrives later gets the retained topics from the broker.
    let late = subscribe(broker.port, "tr/feeds/#");
    std::thread::sleep(Duration::from_millis(500));
    let late = late.lock().unwrap();
    let retained: Vec<&str> = late.iter().filter(|(_, _, r)| *r).map(|(t, _, _)| t.as_str()).collect();
    for t in ["tr/feeds/systems", "tr/feeds/config", "tr/feeds/trunk_recorder/status"] {
        assert!(retained.contains(&t), "{t} not retained: {retained:?}");
    }
    let last = late.iter().find(|(t, _, _)| t == "tr/feeds/trunk_recorder/status").unwrap();
    // The system's identity, once its control channel stated it: on its own, and in the retained list.
    let (system, _) = find("tr/feeds/system");
    assert_eq!((system["system"]["sysid"].as_str(), system["system"]["site_id"].as_u64()), (Some("555"), Some(7)));
    let systems = late.iter().find(|(t, _, _)| t == "tr/feeds/systems").unwrap();
    assert_eq!((systems.1["systems"][0]["wacn"].as_str(), systems.1["systems"][0]["nac"].as_str()), (Some("BBBBB"), Some("BCC")));
    assert_eq!(last.1["status"], "disconnected");
    // The connected status comes before anything else of ours.
    let first = topics.iter().position(|t| t.starts_with("tr/")).unwrap();
    assert_eq!(topics[first], "tr/feeds/trunk_recorder/status");
}

/// Home Assistant's discovery is retained at the broker, and says "unavailable" with the recorder.
#[test]
fn home_assistant_discovery_is_retained() {
    let Some(broker) = mosquitto() else {
        eprintln!("no mosquitto: skipped");
        return;
    };
    let dir = testing::temp_dir("mqtt-ha");
    let hello =
        testing::hello(&dir, json!({ "broker": format!("tcp://127.0.0.1:{}", broker.port), "topic": "tr/feeds", "homeAssistant": true, "instanceId": "east" }));
    let mut plugin = Command::new(env!("CARGO_BIN_EXE_mqtt")).stdin(Stdio::piped()).stdout(Stdio::piped()).stderr(Stdio::null()).spawn().unwrap();
    let mut stdin = plugin.stdin.take().unwrap();
    stdin.write_all(line(&HostMessage::Hello(hello)).as_bytes()).unwrap();
    std::thread::sleep(Duration::from_millis(800));
    stdin.write_all(line(&HostMessage::Shutdown(Shutdown { grace_s: 5.0 })).as_bytes()).unwrap();
    drop(stdin);
    assert!(plugin.wait().unwrap().success());
    // A Home Assistant that starts afterwards still finds every entity.
    let late = subscribe(broker.port, "homeassistant/#");
    std::thread::sleep(Duration::from_millis(500));
    let late = late.lock().unwrap();
    assert!(late.iter().all(|(_, _, retained)| *retained));
    let topics: Vec<&str> = late.iter().map(|(t, _, _)| t.as_str()).collect();
    for t in [
        "homeassistant/binary_sensor/east/recorder/config",
        "homeassistant/sensor/east/active_calls/config",
        "homeassistant/sensor/east/last_call/config",
        "homeassistant/sensor/east/sys1_decode_rate/config",
        "homeassistant/sensor/east/sys1_control_channel/config",
        "homeassistant/sensor/east/sys1_active_calls/config",
    ] {
        assert!(topics.contains(&t), "{t} missing from {topics:?}");
    }
    let rate = late.iter().find(|(t, _, _)| t.ends_with("sys1_decode_rate/config")).unwrap();
    assert_eq!(rate.1["state_topic"], "tr/feeds/rates");
    assert_eq!(rate.1["availability"]["topic"], "tr/feeds/trunk_recorder/status");
}

/// Off unless asked for: nothing is published under Home Assistant's prefix.
#[test]
fn discovery_is_off_by_default() {
    let Some(broker) = mosquitto() else {
        eprintln!("no mosquitto: skipped");
        return;
    };
    let got = subscribe(broker.port, "#");
    std::thread::sleep(Duration::from_millis(300));
    let dir = testing::temp_dir("mqtt-ha-off");
    let hello = testing::hello(&dir, json!({ "broker": format!("tcp://127.0.0.1:{}", broker.port), "topic": "tr/feeds" }));
    let mut plugin = Command::new(env!("CARGO_BIN_EXE_mqtt")).stdin(Stdio::piped()).stdout(Stdio::piped()).stderr(Stdio::null()).spawn().unwrap();
    let mut stdin = plugin.stdin.take().unwrap();
    stdin.write_all(line(&HostMessage::Hello(hello)).as_bytes()).unwrap();
    std::thread::sleep(Duration::from_millis(800));
    stdin.write_all(line(&HostMessage::Shutdown(Shutdown { grace_s: 5.0 })).as_bytes()).unwrap();
    drop(stdin);
    assert!(plugin.wait().unwrap().success());
    std::thread::sleep(Duration::from_millis(200));
    let got = got.lock().unwrap();
    assert!(!got.is_empty() && got.iter().all(|(t, _, _)| !t.starts_with("homeassistant")));
}

#[test]
fn an_unreachable_broker_doesnt_hold_up_shutdown() {
    let port = std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port();
    let dir = testing::temp_dir("mqtt-down");
    let hello = testing::hello(&dir, json!({ "broker": format!("tcp://127.0.0.1:{port}"), "topic": "t" }));
    let mut plugin = Command::new(env!("CARGO_BIN_EXE_mqtt")).stdin(Stdio::piped()).stdout(Stdio::piped()).stderr(Stdio::null()).spawn().unwrap();
    let mut stdin = plugin.stdin.take().unwrap();
    stdin.write_all(line(&HostMessage::Hello(hello)).as_bytes()).unwrap();
    std::thread::sleep(Duration::from_millis(500));
    let t0 = Instant::now();
    stdin.write_all(line(&HostMessage::Shutdown(Shutdown { grace_s: 10.0 })).as_bytes()).unwrap();
    drop(stdin);
    let status = plugin.wait().unwrap();
    assert!(status.success());
    assert!(t0.elapsed() < Duration::from_secs(4), "took {:?}", t0.elapsed());
    let log = std::io::read_to_string(plugin.stdout.take().unwrap()).unwrap();
    assert_eq!(broker_states(&log).last().map(|(s, _)| s.as_str()), Some("down"), "{log}");
}

/// A broker that stops answering (SIGSTOP) is noticed by its missed pings and
/// reconnected once it's back. Slow: `cargo test -- --ignored`.
#[cfg(unix)]
#[test]
#[ignore]
fn a_stalled_broker_is_reconnected() {
    let Some(broker) = mosquitto() else {
        eprintln!("no mosquitto: skipped");
        return;
    };
    let dir = testing::temp_dir("mqtt-stall");
    let hello =
        testing::hello(&dir, json!({ "broker": format!("tcp://127.0.0.1:{}", broker.port), "topic": "s", "heartbeatInterval": 5, "heartbeatTimeout": 3 }));
    let mut plugin = Command::new(env!("CARGO_BIN_EXE_mqtt")).stdin(Stdio::piped()).stdout(Stdio::piped()).stderr(Stdio::null()).spawn().unwrap();
    let mut stdin = plugin.stdin.take().unwrap();
    let out = plugin.stdout.take().unwrap();
    let log = Arc::new(Mutex::new(Vec::<(Instant, String)>::new()));
    let l = log.clone();
    std::thread::spawn(move || {
        use std::io::BufRead;
        for line in std::io::BufReader::new(out).lines().map_while(Result::ok) {
            l.lock().unwrap().push((Instant::now(), line));
        }
    });
    stdin.write_all(line(&HostMessage::Hello(hello)).as_bytes()).unwrap();
    let wait_for = |what: &str, secs: u64| {
        let t0 = Instant::now();
        while t0.elapsed() < Duration::from_secs(secs) {
            if log.lock().unwrap().iter().filter(|(_, l)| l.contains(what)).count() > 0 {
                return t0.elapsed();
            }
            std::thread::sleep(Duration::from_millis(100));
        }
        panic!("no \"{what}\" within {secs} s: {:?}", log.lock().unwrap().iter().map(|(_, l)| l).collect::<Vec<_>>());
    };
    wait_for("connected to", 5);
    let pid = broker.child.id().to_string();
    Command::new("kill").args(["-STOP", &pid]).status().unwrap();
    let noticed = wait_for("lost the connection", 30);
    eprintln!("stall noticed after {noticed:?}");
    log.lock().unwrap().clear();
    Command::new("kill").args(["-CONT", &pid]).status().unwrap();
    let back = wait_for("connected to", 40);
    eprintln!("reconnected {back:?} after SIGCONT");
    let got = subscribe(broker.port, "s/#");
    std::thread::sleep(Duration::from_millis(300));
    let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs_f64();
    let start = CallInfo { id: 9, system: 0, short_name: "sys1".into(), talkgroup: 7, start_time: now, ..Default::default() };
    stdin.write_all(line(&HostMessage::CallStart(start)).as_bytes()).unwrap();
    std::thread::sleep(Duration::from_millis(1000));
    assert!(got.lock().unwrap().iter().any(|(t, _, _)| t == "s/call_start"), "nothing published after the reconnect");
    drop(stdin);
    let _ = plugin.wait();
}
