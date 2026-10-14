//! Home Assistant's MQTT discovery: the entities Home Assistant adds for the
//! recorder (its status, active calls, and each system's decode rate, control
//! channel and calls), read from the topics this plugin already publishes.

use serde_json::{json, Value};

/// Where discovery is published, and what the entities read.
pub struct Discovery<'a> {
    /// Home Assistant's discovery prefix (`homeassistant` unless it was changed there).
    pub prefix: &'a str,
    /// The base topic the plugin publishes under.
    pub topic: &'a str,
    pub instance_id: &'a str,
    /// The systems with a control channel, by short name.
    pub systems: &'a [String],
}

/// A name for use in a topic or an entity ID: letters, digits, `_` and `-`.
fn id(s: &str) -> String {
    s.chars().map(|c| if c.is_ascii_alphanumeric() || c == '_' || c == '-' { c } else { '_' }).collect()
}

/// `s` as a string in a Home Assistant template.
fn quoted(s: &str) -> String {
    format!("'{}'", s.replace('\\', "\\\\").replace('\'', "\\'"))
}

impl Discovery<'_> {
    /// The retained configuration messages: (topic, payload).
    pub fn messages(&self) -> Vec<(String, Value)> {
        let node = id(self.instance_id);
        let status = format!("{}/trunk_recorder/status", self.topic);
        let (rates, active, ended) = (format!("{}/rates", self.topic), format!("{}/calls_active", self.topic), format!("{}/call_end", self.topic));
        let device = json!({
            "identifiers": [format!("trunk_recorder_{node}")],
            "name": if self.instance_id == "trunk-recorder" { "Trunk Recorder".to_string() } else { format!("Trunk Recorder {}", self.instance_id) },
            "manufacturer": "Trunk Recorder",
            "model": "Trunk Recorder Pro",
        });
        // Entities are unavailable while the recorder is (the status is retained, and the last will says "disconnected").
        let availability =
            json!({ "topic": status, "value_template": "{{ value_json.status }}", "payload_available": "connected", "payload_not_available": "disconnected" });
        let mut out = Vec::new();
        let mut add = |component: &str, object: String, mut config: Value| {
            config["unique_id"] = format!("trunk_recorder_{node}_{object}").into();
            config["has_entity_name"] = true.into();
            config["device"] = device.clone();
            out.push((format!("{}/{component}/{node}/{object}/config", self.prefix), config));
        };
        add(
            "binary_sensor",
            "recorder".into(),
            json!({
                "name": "Recorder", "device_class": "connectivity", "state_topic": status, "value_template": "{{ value_json.status }}",
                "payload_on": "connected", "payload_off": "disconnected",
            }),
        );
        add(
            "sensor",
            "active_calls".into(),
            json!({
                "name": "Active calls", "icon": "mdi:phone-in-talk", "state_topic": active, "value_template": "{{ value_json.calls | length }}",
                "state_class": "measurement", "availability": availability,
            }),
        );
        add(
            "sensor",
            "last_call".into(),
            json!({
                "name": "Last call", "icon": "mdi:radio-handheld", "state_topic": ended, "availability": availability,
                "value_template": "{{ value_json.call.talkgroup_alpha_tag or value_json.call.talkgroup }}",
                "json_attributes_topic": ended,
                "json_attributes_template": "{{ {'system': value_json.call.sys_name, 'talkgroup': value_json.call.talkgroup, 'description': value_json.call.talkgroup_description, 'unit': value_json.call.unit, 'unit_alpha_tag': value_json.call.unit_alpha_tag, 'length': value_json.call.length, 'encrypted': value_json.call.encrypted, 'emergency': value_json.call.emergency, 'start_time': value_json.call.start_time} | tojson }}",
            }),
        );
        for name in self.systems {
            let (s, sys) = (id(name), quoted(name));
            let row = format!("{{% set r = value_json.rates | selectattr('sys_name', 'eq', {sys}) | list %}}");
            add(
                "sensor",
                format!("{s}_decode_rate"),
                json!({
                    "name": format!("{name} decode rate"), "icon": "mdi:chart-line", "state_topic": rates, "unit_of_measurement": "msg/s", "state_class": "measurement",
                    "value_template": format!("{row}{{{{ r[0].decoderate if r else none }}}}"), "availability": availability,
                }),
            );
            add(
                "sensor",
                format!("{s}_control_channel"),
                json!({
                    "name": format!("{name} control channel"), "state_topic": rates, "device_class": "frequency", "unit_of_measurement": "MHz",
                    "value_template": format!("{row}{{{{ (r[0].control_channel / 1000000) | round(4) if r and r[0].control_channel else none }}}}"), "availability": availability,
                }),
            );
            add(
                "sensor",
                format!("{s}_active_calls"),
                json!({
                    "name": format!("{name} active calls"), "icon": "mdi:phone-in-talk", "state_topic": active, "state_class": "measurement",
                    "value_template": format!("{{{{ value_json.calls | selectattr('sys_name', 'eq', {sys}) | list | length }}}}"), "availability": availability,
                }),
            );
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn all(systems: &[&str], instance: &str) -> Vec<(String, Value)> {
        let systems: Vec<String> = systems.iter().map(|s| s.to_string()).collect();
        Discovery { prefix: "homeassistant", topic: "tr/feeds", instance_id: instance, systems: &systems }.messages()
    }

    #[test]
    fn one_set_for_the_recorder_and_three_a_system() {
        let m = all(&["east", "west"], "trunk-recorder");
        assert_eq!(m.len(), 3 + 2 * 3);
        let topics: Vec<&str> = m.iter().map(|(t, _)| t.as_str()).collect();
        assert!(topics.contains(&"homeassistant/binary_sensor/trunk-recorder/recorder/config"));
        assert!(topics.contains(&"homeassistant/sensor/trunk-recorder/west_decode_rate/config"));
        // Every entity has its own ID, and they all share a device.
        let mut ids: Vec<&str> = m.iter().map(|(_, v)| v["unique_id"].as_str().unwrap()).collect();
        ids.sort_unstable();
        ids.dedup();
        assert_eq!(ids.len(), m.len());
        assert!(m.iter().all(|(_, v)| v["device"]["identifiers"] == json!(["trunk_recorder_trunk-recorder"])));
        assert_eq!(m[0].1["device"]["name"], "Trunk Recorder");
    }

    #[test]
    fn entities_read_the_topics_the_plugin_publishes() {
        let m = all(&["east"], "north");
        let by = |object: &str| m.iter().find(|(t, _)| t.contains(&format!("/{object}/config"))).map(|(_, v)| v.clone()).unwrap();
        assert_eq!(by("recorder")["state_topic"], "tr/feeds/trunk_recorder/status");
        assert_eq!(by("active_calls")["state_topic"], "tr/feeds/calls_active");
        assert_eq!(by("last_call")["state_topic"], "tr/feeds/call_end");
        assert_eq!(by("east_decode_rate")["state_topic"], "tr/feeds/rates");
        assert_eq!(by("east_decode_rate")["availability"]["topic"], "tr/feeds/trunk_recorder/status");
        assert_eq!(by("east_decode_rate")["device"]["name"], "Trunk Recorder north");
        // The recorder's own status can't depend on itself being available.
        assert!(by("recorder").get("availability").is_none());
    }

    #[test]
    fn names_are_safe_in_topics_and_templates() {
        let m = all(&["it's a/b+c#"], "x y/z");
        for (topic, v) in &m {
            assert!(!topic.trim_start_matches("homeassistant/").contains(['+', '#', ' ']), "{topic}");
            assert_eq!(topic.matches('/').count(), 4, "{topic}");
            assert!(v["unique_id"].as_str().unwrap().chars().all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-'));
        }
        let rate = m.iter().find(|(t, _)| t.ends_with("_decode_rate/config")).unwrap();
        assert!(rate.1["value_template"].as_str().unwrap().contains(r"'it\'s a/b+c#'"));
        // The name shown keeps what it was.
        assert_eq!(rate.1["name"], "it's a/b+c# decode rate");
    }
}
