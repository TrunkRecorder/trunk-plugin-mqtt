//! The MQTT connection: rumqttc's 3.1.1 and 5 clients behind one interface.

use std::time::Duration;

use rumqttc::{NetworkOptions, QoS, Transport};

/// Where and how to connect.
#[derive(Clone, Debug)]
pub struct Settings {
    pub broker: Broker,
    pub client_id: String,
    pub username: String,
    pub password: String,
    pub version: u8,
    pub keep_alive: Duration,
    /// Seconds a connect or a write may take.
    pub timeout: u64,
}

/// What the broker publishes (retained) if the connection is lost.
pub struct Will {
    pub topic: String,
    pub payload: Vec<u8>,
    pub qos: u8,
}

/// A broker URL, parsed.
#[derive(Clone, Debug, PartialEq)]
pub struct Broker {
    pub scheme: Scheme,
    pub host: String,
    pub port: u16,
    /// For websockets: the whole URL.
    pub url: String,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Scheme {
    Tcp,
    Tls,
    Ws,
    Wss,
}

impl Broker {
    /// `tcp://`, `mqtt://`, `ssl://`, `mqtts://`, `tls://`, `ws://` or `wss://`, host and optional port.
    pub fn parse(s: &str) -> Result<Broker, String> {
        let s = s.trim();
        let (scheme, rest) = s.split_once("://").ok_or_else(|| format!("broker \"{s}\" needs a protocol, like tcp://{s}:1883"))?;
        let (scheme, default_port) = match scheme.to_ascii_lowercase().as_str() {
            "tcp" | "mqtt" => (Scheme::Tcp, 1883),
            "ssl" | "tls" | "mqtts" => (Scheme::Tls, 8883),
            "ws" => (Scheme::Ws, 80),
            "wss" => (Scheme::Wss, 443),
            other => return Err(format!("broker protocol \"{other}\" isn't one of tcp, ssl, ws or wss")),
        };
        let authority = rest.split('/').next().unwrap_or_default();
        let (host, port) = match authority.rsplit_once(':') {
            Some((h, p)) if !h.ends_with(']') || authority.starts_with('[') => match p.parse::<u16>() {
                Ok(p) => (h, p),
                Err(_) => return Err(format!("broker port \"{p}\" isn't a number")),
            },
            _ => (authority, default_port),
        };
        let host = host.trim_start_matches('[').trim_end_matches(']');
        if host.is_empty() {
            return Err(format!("broker \"{s}\" has no host"));
        }
        // rumqttc wants a websocket URL with its port.
        let url = match scheme {
            Scheme::Ws | Scheme::Wss => {
                let path = rest.find('/').map_or("/", |i| &rest[i..]);
                let proto = if scheme == Scheme::Ws { "ws" } else { "wss" };
                format!("{proto}://{authority_host}:{port}{path}", authority_host = authority.rsplit_once(':').map_or(authority, |(h, _)| h))
            }
            _ => String::new(),
        };
        Ok(Broker { scheme, host: host.to_string(), port, url })
    }

    fn address(&self) -> &str {
        match self.scheme {
            Scheme::Ws | Scheme::Wss => &self.url,
            _ => &self.host,
        }
    }

    fn transport(&self) -> Transport {
        match self.scheme {
            Scheme::Tcp => Transport::tcp(),
            Scheme::Tls => Transport::tls_with_default_config(),
            Scheme::Ws => Transport::Ws,
            Scheme::Wss => Transport::Wss(Default::default()),
        }
    }
}

/// A topic a message can be published to: no wildcards, no NUL, not empty.
pub fn valid_topic(t: &str) -> bool {
    !t.is_empty() && !t.contains(['+', '#', '\0'])
}

fn qos5(q: u8) -> rumqttc::v5::mqttbytes::QoS {
    use rumqttc::v5::mqttbytes::QoS;
    match q {
        1 => QoS::AtLeastOnce,
        2 => QoS::ExactlyOnce,
        _ => QoS::AtMostOnce,
    }
}

pub fn qos(q: u8) -> QoS {
    match q {
        1 => QoS::AtLeastOnce,
        2 => QoS::ExactlyOnce,
        _ => QoS::AtMostOnce,
    }
}

/// What the connection reports.
#[derive(Debug)]
pub enum Net {
    Connected,
    /// The broker refused the connection.
    Refused(String),
    /// The connection failed or was lost (it reconnects on the next poll).
    Lost(String),
    /// An MQTT 5 broker closed the connection, with its reason.
    BrokerClosed(String),
    /// Our own disconnect went out.
    Closed,
    Other,
}

#[derive(Clone)]
pub enum Client {
    V3(rumqttc::Client),
    V5(rumqttc::v5::Client),
}

pub enum Connection {
    V3(Box<rumqttc::Connection>),
    V5(Box<rumqttc::v5::Connection>),
}

/// Requests the client may have waiting for the connection loop.
const REQUEST_CAPACITY: usize = 10;

pub fn connect(s: &Settings, will: &Will) -> (Client, Connection) {
    // rustls needs a crypto provider; ring is the one built in.
    let _ = rustls::crypto::ring::default_provider().install_default();
    let mut net = NetworkOptions::new();
    net.set_connection_timeout(s.timeout);
    if s.version == 5 {
        use rumqttc::v5::{mqttbytes::v5::LastWill, MqttOptions};
        let mut o = MqttOptions::new(&s.client_id, s.broker.address(), s.broker.port);
        o.set_transport(s.broker.transport());
        o.set_keep_alive(s.keep_alive);
        o.set_clean_start(true);
        o.set_last_will(LastWill::new(&will.topic, will.payload.clone(), qos5(will.qos), true, None));
        if !s.username.is_empty() {
            o.set_credentials(&s.username, &s.password);
        }
        o.set_network_options(net);
        let (c, conn) = rumqttc::v5::Client::new(o, REQUEST_CAPACITY);
        (Client::V5(c), Connection::V5(Box::new(conn)))
    } else {
        use rumqttc::{LastWill, MqttOptions};
        let mut o = MqttOptions::new(&s.client_id, s.broker.address(), s.broker.port);
        o.set_transport(s.broker.transport());
        o.set_keep_alive(s.keep_alive);
        o.set_clean_session(true);
        o.set_last_will(LastWill::new(&will.topic, will.payload.clone(), qos(will.qos), true));
        // Audio payloads run to megabytes; the default is 10 KB.
        o.set_max_packet_size(256 * 1024, 256 * 1024 * 1024);
        if !s.username.is_empty() {
            o.set_credentials(&s.username, &s.password);
        }
        let (c, mut conn) = rumqttc::Client::new(o, REQUEST_CAPACITY);
        conn.eventloop.set_network_options(net);
        (Client::V3(c), Connection::V3(Box::new(conn)))
    }
}

impl Client {
    /// Hand a message to the connection loop. False: its request channel is
    /// full (or the topic is invalid: check with [`valid_topic`] first).
    pub fn try_publish(&self, topic: &str, q: u8, retain: bool, payload: Vec<u8>) -> bool {
        match self {
            Client::V3(c) => c.try_publish(topic, qos(q), retain, payload).is_ok(),
            Client::V5(c) => c.try_publish(topic, qos5(q), retain, payload).is_ok(),
        }
    }

    pub fn try_disconnect(&self) {
        let _ = match self {
            Client::V3(c) => c.try_disconnect().map_err(|_| ()),
            Client::V5(c) => c.try_disconnect().map_err(|_| ()),
        };
    }
}

impl Connection {
    /// The next event; blocks. None once every client is dropped.
    pub fn next(&mut self) -> Option<Net> {
        match self {
            Connection::V3(c) => {
                use rumqttc::{ConnectReturnCode, Event, Outgoing, Packet};
                Some(match c.recv().ok()? {
                    Ok(Event::Incoming(Packet::ConnAck(a))) if a.code == ConnectReturnCode::Success => Net::Connected,
                    Ok(Event::Incoming(Packet::ConnAck(a))) => Net::Refused(format!("{:?}", a.code)),
                    Ok(Event::Outgoing(Outgoing::Disconnect)) => Net::Closed,
                    Ok(_) => Net::Other,
                    Err(rumqttc::ConnectionError::ConnectionRefused(code)) => Net::Refused(format!("{code:?}")),
                    Err(e) => Net::Lost(e.to_string()),
                })
            }
            Connection::V5(c) => {
                use rumqttc::v5::mqttbytes::v5::{ConnectReturnCode, Packet};
                use rumqttc::v5::{ConnectionError, Event};
                use rumqttc::Outgoing;
                Some(match c.recv().ok()? {
                    Ok(Event::Incoming(Packet::ConnAck(a))) if a.code == ConnectReturnCode::Success => Net::Connected,
                    Ok(Event::Incoming(Packet::ConnAck(a))) => Net::Refused(format!("{:?}", a.code)),
                    Ok(Event::Incoming(Packet::Disconnect(d))) => {
                        let why = d.properties.and_then(|p| p.reason_string).map_or(String::new(), |r| format!(": {r}"));
                        Net::BrokerClosed(format!("{:?}{why}", d.reason_code))
                    }
                    Ok(Event::Outgoing(Outgoing::Disconnect)) => Net::Closed,
                    Ok(_) => Net::Other,
                    Err(ConnectionError::ConnectionRefused(code)) => Net::Refused(format!("{code:?}")),
                    Err(e) => Net::Lost(e.to_string()),
                })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn broker_urls() {
        let b = Broker::parse("tcp://localhost:1883").unwrap();
        assert_eq!((b.scheme, b.host.as_str(), b.port), (Scheme::Tcp, "localhost", 1883));
        let b = Broker::parse("ssl://broker.example.com").unwrap();
        assert_eq!((b.scheme, b.port), (Scheme::Tls, 8883));
        let b = Broker::parse("mqtt://10.0.0.5").unwrap();
        assert_eq!((b.scheme, b.port), (Scheme::Tcp, 1883));
        let b = Broker::parse("ws://host:9001/mqtt").unwrap();
        assert_eq!((b.scheme, b.port, b.url.as_str()), (Scheme::Ws, 9001, "ws://host:9001/mqtt"));
        let b = Broker::parse("wss://host").unwrap();
        assert_eq!((b.port, b.url.as_str()), (443, "wss://host:443/"));
        let b = Broker::parse("tcp://[::1]:1884").unwrap();
        assert_eq!((b.host.as_str(), b.port), ("::1", 1884));
        assert!(Broker::parse("localhost:1883").is_err());
        assert!(Broker::parse("http://x").is_err());
        assert!(Broker::parse("tcp://x:port").is_err());
        assert!(Broker::parse("tcp://").is_err());
    }
}
