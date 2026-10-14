//! The outbound queue and the threads that publish it.
//!
//! Event handlers only serialize and enqueue. The publish thread is the only
//! one that hands messages to the client, and only while connected, so a slow
//! or unreachable broker backs up here, where the age and size limits apply.

use std::collections::{BTreeMap, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use serde_json::{json, Value};
use trunk_recorder_plugin::{Endpoint, EndpointState, Host, Metrics, State};

use crate::transport::{self, valid_topic, Client, Net, Settings};

/// How a message is queued.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Kind {
    /// Sent once, in order.
    Event,
    /// A snapshot: only the latest copy waits.
    Snapshot,
    /// Audio: kept longer (`audio_max_age`).
    Audio,
    /// A snapshot kept at the broker, and sent again after each reconnect.
    Retained,
}

pub struct Limits {
    pub max_age: Duration,
    pub audio_max_age: Duration,
    pub max_bytes: usize,
    pub heartbeat: Option<Duration>,
}

struct Outbound {
    topic: String,
    payload: Vec<u8>,
    qos: u8,
    retain: bool,
    coalesce: bool,
    expires: Instant,
}

#[derive(Default)]
struct Queue {
    items: VecDeque<Outbound>,
    bytes: usize,
    dropped_stale: u64,
    dropped_overflow: u64,
    /// Both, since the start (for the dashboard).
    dropped_total: u64,
}

impl Queue {
    fn push(&mut self, o: Outbound, max_bytes: usize, front: bool) {
        if o.coalesce && front {
            if let Some(i) = self.items.iter().position(|m| m.coalesce && m.topic == o.topic) {
                let old = self.items.remove(i).unwrap();
                self.bytes -= old.payload.len();
            }
        } else if o.coalesce {
            if let Some(m) = self.items.iter_mut().find(|m| m.coalesce && m.topic == o.topic) {
                self.bytes = self.bytes - m.payload.len() + o.payload.len();
                *m = o;
                return;
            }
        }
        self.bytes += o.payload.len();
        if front {
            self.items.push_front(o);
        } else {
            self.items.push_back(o);
        }
        while self.bytes > max_bytes && self.items.len() > 1 {
            let old = self.items.pop_front().unwrap();
            self.bytes -= old.payload.len();
            self.dropped_overflow += 1;
            self.dropped_total += 1;
        }
    }

    fn pop(&mut self) -> Option<Outbound> {
        let o = self.items.pop_front()?;
        self.bytes -= o.payload.len();
        Some(o)
    }

    fn purge(&mut self, now: Instant) {
        let n = self.items.len();
        let mut freed = 0;
        self.items.retain(|m| {
            let keep = m.expires > now;
            if !keep {
                freed += m.payload.len();
            }
            keep
        });
        self.bytes -= freed;
        self.dropped_stale += (n - self.items.len()) as u64;
        self.dropped_total += (n - self.items.len()) as u64;
    }
}

/// What the connection has done, for the dashboard ([`Metrics`]).
#[derive(Default)]
struct Stats {
    /// A connection has been tried (the broker's state is unknown until then).
    tried: bool,
    connected_once: bool,
    sent: u64,
    bytes_sent: u64,
    reconnects: u64,
    /// Unix s of the last message handed to the connection, and of the last failure.
    last_ok: Option<f64>,
    last_error: Option<f64>,
    last_error_text: String,
}

/// Metrics go out at most this often while they change, and at least this often regardless;
/// at once when the broker's state changes.
const METRICS_MIN: Duration = Duration::from_secs(5);
const METRICS_MAX: Duration = Duration::from_secs(30);

struct Shared {
    queue: Mutex<Queue>,
    cv: Condvar,
    connected: AtomicBool,
    stopping: AtomicBool,
    /// While stopping: when to give up sending what's queued.
    deadline: Mutex<Option<Instant>>,
    /// Retained messages by topic, sent again after each reconnect.
    retained: Mutex<BTreeMap<String, Vec<u8>>>,
    limits: Limits,
    qos: u8,
    status_topic: String,
    heartbeat_topic: String,
    instance_id: String,
    client_id: String,
    broker: String,
    host: Host,
    stats: Mutex<Stats>,
}

#[derive(Clone)]
pub struct Publisher {
    sh: Arc<Shared>,
    threads: Arc<Mutex<Vec<JoinHandle<()>>>>,
}

impl Publisher {
    /// Connect (in the background) and start publishing. `base`: the status topic.
    pub fn start(settings: Settings, limits: Limits, qos: u8, base: &str, instance_id: &str, host: Host) -> Publisher {
        let status_topic = format!("{base}/trunk_recorder/status");
        // The broker says "disconnected" for us if we vanish.
        let will = transport::Will { topic: status_topic.clone(), payload: status_payload("disconnected", instance_id, &settings.client_id), qos };
        let sh = Arc::new(Shared {
            queue: Mutex::new(Queue::default()),
            cv: Condvar::new(),
            connected: AtomicBool::new(false),
            stopping: AtomicBool::new(false),
            deadline: Mutex::new(None),
            retained: Mutex::new(BTreeMap::new()),
            limits,
            qos,
            status_topic,
            heartbeat_topic: format!("{base}/trunk_recorder/heartbeat"),
            instance_id: instance_id.to_string(),
            client_id: settings.client_id.clone(),
            broker: settings.broker.url_or_host(),
            host,
            stats: Mutex::new(Stats::default()),
        });
        let (client, connection) = transport::connect(&settings, &will);
        let net = {
            let sh = sh.clone();
            std::thread::Builder::new().name("mqtt-net".into()).spawn(move || net_loop(&sh, connection)).expect("thread")
        };
        let publish = {
            let sh = sh.clone();
            std::thread::Builder::new().name("mqtt-publish".into()).spawn(move || publish_loop(&sh, client)).expect("thread")
        };
        Publisher { sh, threads: Arc::new(Mutex::new(vec![publish, net])) }
    }

    /// Queue `{"type": type, name: data, "timestamp", "instance_id"}` for `topic/type`.
    pub fn send(&self, topic: &str, kind: Kind, name: &str, type_: &str, data: Value) {
        let payload = json!({ "type": type_, name: data, "timestamp": unix_now(), "instance_id": self.sh.instance_id });
        self.send_raw(&format!("{topic}/{type_}"), kind, serde_json::to_vec(&payload).unwrap_or_default());
    }

    pub fn send_raw(&self, topic: &str, kind: Kind, payload: Vec<u8>) {
        let sh = &self.sh;
        if sh.stopping.load(Ordering::Relaxed) || !valid_topic(topic) {
            return;
        }
        if kind == Kind::Retained {
            sh.retained.lock().unwrap().insert(topic.to_string(), payload.clone());
        }
        let age = if kind == Kind::Audio { sh.limits.audio_max_age } else { sh.limits.max_age };
        let o = Outbound {
            topic: topic.to_string(),
            payload,
            qos: sh.qos,
            retain: kind == Kind::Retained,
            coalesce: matches!(kind, Kind::Snapshot | Kind::Retained),
            expires: Instant::now() + age,
        };
        sh.queue.lock().unwrap().push(o, sh.limits.max_bytes, false);
        sh.cv.notify_one();
    }

    pub fn connected(&self) -> bool {
        self.sh.connected.load(Ordering::Relaxed)
    }

    /// Send what's queued (while connected) within `grace`, then say goodbye and disconnect.
    pub fn shutdown(&self, grace: Duration) {
        let sh = &self.sh;
        let deadline = Instant::now() + grace.saturating_sub(Duration::from_millis(500)).min(Duration::from_secs(10));
        // The publish thread drains until empty or the deadline.
        *sh.deadline.lock().unwrap() = Some(deadline);
        sh.stopping.store(true, Ordering::Relaxed);
        sh.cv.notify_all();
        let threads: Vec<_> = self.threads.lock().unwrap().drain(..).collect();
        for t in threads {
            while !t.is_finished() && Instant::now() < deadline + Duration::from_millis(400) {
                std::thread::sleep(Duration::from_millis(10));
            }
            if t.is_finished() {
                let _ = t.join();
            }
        }
    }
}

impl transport::Broker {
    fn url_or_host(&self) -> String {
        if self.url.is_empty() {
            format!("{}:{}", self.host, self.port)
        } else {
            self.url.clone()
        }
    }
}

pub fn status_payload(status: &str, instance_id: &str, client_id: &str) -> Vec<u8> {
    serde_json::to_vec(&json!({ "status": status, "instance_id": instance_id, "client_id": client_id })).unwrap_or_default()
}

pub fn unix_now() -> i64 {
    unix_now_f64() as i64
}

fn unix_now_f64() -> f64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map_or(0.0, |d| d.as_secs_f64())
}

// broker_state()
//   Up while connected; Degraded while connected with a backlog (the broker
//   is slow to take it); Down while not connected.
fn broker_state(connected: bool, queued: usize, st: &Stats) -> EndpointState {
    if !st.tried {
        EndpointState::Unknown
    } else if !connected {
        EndpointState::Down
    } else if queued > 100 {
        EndpointState::Degraded
    } else {
        EndpointState::Up
    }
}

// metrics()
//   The dashboard's figures: the queue, what's been sent, and the broker.
fn metrics(sh: &Shared, q: &Queue) -> (Metrics, EndpointState) {
    let st = sh.stats.lock().unwrap();
    let connected = sh.connected.load(Ordering::Relaxed);
    let state = broker_state(connected, q.items.len(), &st);
    let mut extra = BTreeMap::new();
    extra.insert("Messages sent".to_string(), json!(st.sent));
    extra.insert("Dropped".to_string(), json!(q.dropped_total));
    extra.insert("Reconnects".to_string(), json!(st.reconnects));
    extra.insert("Queued KB".to_string(), json!(q.bytes / 1024));
    let last_error = if state == EndpointState::Up { String::new() } else { st.last_error_text.clone() };
    let m = Metrics {
        queued: Some(q.items.len() as u64),
        bytes_sent: Some(st.bytes_sent),
        last_ok: st.last_ok,
        last_error: st.last_error,
        last_error_text: st.last_error_text.clone(),
        endpoints: vec![Endpoint { name: sh.broker.clone(), state, last_ok: st.last_ok, last_error, ..Default::default() }],
        extra,
        ..Default::default()
    };
    (m, state)
}

// net_loop()
//   Drive the connection: report state, and back off between failed attempts.
fn net_loop(sh: &Shared, mut conn: transport::Connection) {
    let mut backoff = Duration::from_secs(1);
    let mut last_error = String::new();
    let mut last_error_logged: Option<Instant> = None;
    while let Some(ev) = conn.next() {
        let failed = match ev {
            Net::Connected => {
                {
                    let mut st = sh.stats.lock().unwrap();
                    if st.connected_once {
                        st.reconnects += 1;
                    }
                    st.tried = true;
                    st.connected_once = true;
                }
                // Queue the status first, so it's the first thing sent.
                on_connect(sh);
                sh.connected.store(true, Ordering::Relaxed);
                sh.cv.notify_one();
                backoff = Duration::from_secs(1);
                last_error.clear();
                sh.host.info(format!("connected to {}", sh.broker));
                sh.host.status(State::Ok, format!("connected to {}", sh.broker));
                None
            }
            Net::Refused(why) => Some(format!("the broker refused the connection ({why})")),
            Net::Lost(why) => Some(why),
            Net::BrokerClosed(why) => Some(format!("the broker closed the connection: {why}")),
            Net::Closed => break,
            Net::Other => None,
        };
        let Some(why) = failed else { continue };
        let was = sh.connected.swap(false, Ordering::Relaxed);
        {
            let mut st = sh.stats.lock().unwrap();
            st.tried = true;
            st.last_error = Some(unix_now_f64());
            st.last_error_text = why.clone();
        }
        // The publish thread reports the change.
        sh.cv.notify_one();
        if sh.stopping.load(Ordering::Relaxed) {
            sh.host.warn(format!("connection to {} ended while stopping: {why}", sh.broker));
            break;
        }
        // Say it when it changes, and once a minute while it lasts.
        if was || why != last_error || last_error_logged.is_none_or(|t| t.elapsed() > Duration::from_secs(60)) {
            let queued = sh.queue.lock().unwrap().items.len();
            let text = if was { format!("lost the connection to {}: {why}", sh.broker) } else { format!("can't connect to {}: {why}", sh.broker) };
            sh.host.warn(&text);
            sh.host.status(State::Warning, format!("{text} ({queued} queued)"));
            last_error = why;
            last_error_logged = Some(Instant::now());
        }
        let until = Instant::now() + backoff;
        while Instant::now() < until && !sh.stopping.load(Ordering::Relaxed) {
            std::thread::sleep(Duration::from_millis(100));
        }
        if sh.stopping.load(Ordering::Relaxed) {
            break;
        }
        backoff = (backoff * 2).min(Duration::from_secs(30));
    }
}

// on_connect()
//   Queue the retained "connected" status first, then the retained snapshots.
fn on_connect(sh: &Shared) {
    let now = Instant::now();
    let item = |topic: &str, payload: Vec<u8>| Outbound {
        topic: topic.to_string(),
        payload,
        qos: sh.qos,
        retain: true,
        coalesce: true,
        expires: now + sh.limits.max_age,
    };
    let retained = sh.retained.lock().unwrap().clone();
    let mut q = sh.queue.lock().unwrap();
    for (topic, payload) in retained.into_iter().rev() {
        q.push(item(&topic, payload), sh.limits.max_bytes, true);
    }
    q.push(item(&sh.status_topic, status_payload("connected", &sh.instance_id, &sh.client_id)), sh.limits.max_bytes, true);
    drop(q);
    sh.cv.notify_one();
}

// publish_loop()
//   Hand queued messages to the client while connected; purge, report and heartbeat.
fn publish_loop(sh: &Shared, client: Client) {
    let mut next_purge = Instant::now();
    let mut next_report = Instant::now() + Duration::from_secs(30);
    let mut next_heartbeat = sh.limits.heartbeat.map(|h| Instant::now() + h);
    let mut stop_by: Option<(Instant, Instant)> = None;
    let mut reported: Option<(Instant, Metrics, EndpointState)> = None;
    let mut q = sh.queue.lock().unwrap();
    loop {
        let stopping = sh.stopping.load(Ordering::Relaxed);
        if stopping {
            let now = Instant::now();
            let deadline = *stop_by.get_or_insert_with(|| {
                let d = sh.deadline.lock().unwrap().unwrap_or(now);
                // Not connected: wait a moment for a connection on its way, no longer.
                (d, d.min(now + Duration::from_secs(2)))
            });
            let connected = sh.connected.load(Ordering::Relaxed);
            if q.items.is_empty() || now >= deadline.0 || (!connected && now >= deadline.1) {
                break;
            }
            if !connected {
                let (g, _) = sh.cv.wait_timeout(q, Duration::from_millis(50)).unwrap();
                q = g;
                continue;
            }
        } else {
            let (g, _) = sh.cv.wait_timeout_while(q, Duration::from_millis(500), |q| q.items.is_empty() || !sh.connected.load(Ordering::Relaxed)).unwrap();
            q = g;
        }
        let now = Instant::now();
        if now >= next_purge {
            q.purge(now);
            next_purge = now + Duration::from_secs(1);
        }
        if now >= next_report {
            next_report = now + Duration::from_secs(30);
            let (stale, overflow) = (std::mem::take(&mut q.dropped_stale), std::mem::take(&mut q.dropped_overflow));
            if stale + overflow > 0 {
                let (n, kb) = (q.items.len(), q.bytes / 1024);
                drop(q);
                sh.host.warn(format!("queue: dropped {stale} expired, {overflow} over the size limit; {n} queued ({kb} KB)"));
                q = sh.queue.lock().unwrap();
            }
        }
        let (m, state) = metrics(sh, &q);
        let due = match &reported {
            None => true,
            Some((at, last, last_state)) => state != *last_state || at.elapsed() >= METRICS_MAX || (at.elapsed() >= METRICS_MIN && m != *last),
        };
        if due {
            drop(q);
            sh.host.metrics(&m);
            reported = Some((now, m, state));
            q = sh.queue.lock().unwrap();
        }
        if !sh.connected.load(Ordering::Relaxed) {
            next_heartbeat = sh.limits.heartbeat.map(|h| now + h);
            continue;
        }
        if let (Some(at), Some(every)) = (next_heartbeat, sh.limits.heartbeat) {
            if now >= at {
                next_heartbeat = Some(now + every);
                let hb = json!({ "type": "heartbeat", "timestamp": unix_now(), "instance_id": sh.instance_id });
                let o = Outbound {
                    topic: sh.heartbeat_topic.clone(),
                    payload: serde_json::to_vec(&hb).unwrap_or_default(),
                    qos: 1,
                    retain: false,
                    coalesce: true,
                    expires: now + every,
                };
                q.push(o, sh.limits.max_bytes, true);
            }
        }
        let Some(o) = q.pop() else { continue };
        drop(q);
        if client.try_publish(&o.topic, o.qos, o.retain, o.payload.clone()) {
            let mut st = sh.stats.lock().unwrap();
            st.sent += 1;
            st.bytes_sent += o.payload.len() as u64;
            st.last_ok = Some(unix_now_f64());
            drop(st);
            q = sh.queue.lock().unwrap();
            continue;
        }
        // The client's channel is full (the connection is busy or stalled):
        // put it back, where the age and size limits apply, and wait a little.
        std::thread::sleep(Duration::from_millis(20));
        q = sh.queue.lock().unwrap();
        q.push(o, sh.limits.max_bytes, true);
    }
    // The last figures, as it stops.
    let (m, _) = metrics(sh, &q);
    drop(q);
    sh.host.metrics(&m);
    if sh.connected.load(Ordering::Relaxed) {
        let bye = status_payload("disconnected", &sh.instance_id, &sh.client_id);
        let deadline = Instant::now() + Duration::from_millis(500);
        while !client.try_publish(&sh.status_topic, sh.qos, true, bye.clone()) && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(20));
        }
        client.try_disconnect();
        sh.host.info(format!("disconnected from {}", sh.broker));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn o(topic: &str, n: usize, coalesce: bool) -> Outbound {
        Outbound { topic: topic.into(), payload: vec![0; n], qos: 0, retain: false, coalesce, expires: Instant::now() + Duration::from_secs(60) }
    }

    #[test]
    fn snapshots_coalesce() {
        let mut q = Queue::default();
        q.push(o("a/calls_active", 10, true), 1000, false);
        q.push(o("a/call_start", 5, false), 1000, false);
        q.push(o("a/calls_active", 20, true), 1000, false);
        assert_eq!(q.items.len(), 2);
        assert_eq!(q.bytes, 25);
        assert_eq!(q.items[0].payload.len(), 20, "the newest snapshot keeps its place");
    }

    #[test]
    fn the_size_limit_drops_the_oldest() {
        let mut q = Queue::default();
        for i in 0..5 {
            q.push(o(&format!("t/{i}"), 100, false), 250, false);
        }
        assert_eq!(q.items.len(), 2);
        assert_eq!(q.items[0].topic, "t/3");
        assert_eq!(q.dropped_overflow, 3);
        // One message over the limit on its own is still kept.
        let mut q = Queue::default();
        q.push(o("big", 1000, false), 250, false);
        assert_eq!(q.items.len(), 1);
    }

    #[test]
    fn expired_messages_are_purged() {
        let mut q = Queue::default();
        let mut old = o("old", 10, false);
        old.expires = Instant::now() - Duration::from_secs(1);
        q.push(old, 1000, false);
        q.push(o("new", 10, false), 1000, false);
        q.purge(Instant::now());
        assert_eq!(q.items.len(), 1);
        assert_eq!((q.bytes, q.dropped_stale), (10, 1));
    }
}
