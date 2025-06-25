use serde::{Deserialize, Deserializer, Serializer};
use std::fmt::Write;
use std::time::Duration;

/// Deserialize a duration string like "42.868726ms" or "8d19h55m4s" into Duration
#[allow(clippy::cast_sign_loss)]
#[allow(clippy::cast_possible_truncation)]
pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<Duration>, D::Error>
where
    D: Deserializer<'de>,
{
    let s: Option<String> = Option::deserialize(deserializer)?;

    if let Some(s) = s {
        if s == "0s" || s.is_empty() {
            return Ok(Some(Duration::from_secs(0)));
        }

        // Handles simple duration formats like "42.868726ms"
        if let Some(ms_idx) = s.find("ms") {
            let num_part = &s[0..ms_idx];
            if let Ok(ms) = num_part.parse::<f64>() {
                return Ok(Some(Duration::from_nanos((ms * 1_000_000.0) as u64)));
            }
        }

        // Handles formats like "8d19h55m4s"
        let mut total_secs = 0u64;
        let mut current_num = String::new();

        for c in s.chars() {
            if c.is_ascii_digit() || c == '.' {
                current_num.push(c);
            } else if !current_num.is_empty() {
                if let Ok(num) = current_num.parse::<f64>() {
                    match c {
                        'd' => total_secs += (num * 86400.0) as u64,
                        'h' => total_secs += (num * 3600.0) as u64,
                        'm' => total_secs += (num * 60.0) as u64,
                        's' => total_secs += num as u64,
                        _ => {}
                    }
                    current_num.clear();
                }
            }
        }

        Ok(Some(Duration::from_secs(total_secs)))
    } else {
        Ok(None)
    }
}

/// Serialize Duration back to string format like "42.868726ms" or "8d19h55m4s"
#[allow(clippy::ref_option)] // Needed for serde
pub fn serialize<S>(duration: &Option<Duration>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    match duration {
        Some(duration) => {
            let secs = duration.as_secs();
            if secs == 0 {
                let nanos = duration.subsec_nanos();
                if nanos == 0 {
                    serializer.serialize_str("0s")
                } else {
                    let ms = f64::from(nanos) / 1_000_000.0;
                    serializer.serialize_str(&format!("{ms}ms"))
                }
            } else if secs < 60 {
                serializer.serialize_str(&format!("{secs}s"))
            } else if secs < 3600 {
                let mins = secs / 60;
                let remaining_secs = secs % 60;
                if remaining_secs == 0 {
                    serializer.serialize_str(&format!("{mins}m"))
                } else {
                    serializer.serialize_str(&format!("{mins}m{remaining_secs}s"))
                }
            } else if secs < 86400 {
                let hours = secs / 3600;
                let mins = (secs % 3600) / 60;
                let remaining_secs = secs % 60;
                let mut result = format!("{hours}h");
                if mins > 0 {
                    write!(result, "{mins}m").unwrap();
                }
                if remaining_secs > 0 {
                    write!(result, "{remaining_secs}s").unwrap();
                }
                serializer.serialize_str(&result)
            } else {
                let days = secs / 86400;
                let hours = (secs % 86400) / 3600;
                let mins = (secs % 3600) / 60;
                let remaining_secs = secs % 60;
                let mut result = format!("{days}d");
                if hours > 0 {
                    write!(result, "{hours}h").unwrap();
                }
                if mins > 0 {
                    write!(result, "{mins}m").unwrap();
                }
                if remaining_secs > 0 {
                    write!(result, "{remaining_secs}s").unwrap();
                }
                serializer.serialize_str(&result)
            }
        }
        None => serializer.serialize_none(),
    }
}
