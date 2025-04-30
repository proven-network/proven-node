use serde::{Deserialize, Deserializer, Serializer};
use std::time::Duration;

/// Deserialize a duration string like "42.868726ms" or "8d19h55m4s" into Duration
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
            } else {
                if !current_num.is_empty() {
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
        }

        Ok(Some(Duration::from_secs(total_secs)))
    } else {
        Ok(None)
    }
}

/// Serialize Duration back to string format like "42.868726ms" or "8d19h55m4s"
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
                    let ms = nanos as f64 / 1_000_000.0;
                    serializer.serialize_str(&format!("{}ms", ms))
                }
            } else if secs < 60 {
                serializer.serialize_str(&format!("{}s", secs))
            } else if secs < 3600 {
                let mins = secs / 60;
                let remaining_secs = secs % 60;
                if remaining_secs == 0 {
                    serializer.serialize_str(&format!("{}m", mins))
                } else {
                    serializer.serialize_str(&format!("{}m{}s", mins, remaining_secs))
                }
            } else if secs < 86400 {
                let hours = secs / 3600;
                let mins = (secs % 3600) / 60;
                let remaining_secs = secs % 60;
                let mut result = format!("{}h", hours);
                if mins > 0 {
                    result.push_str(&format!("{}m", mins));
                }
                if remaining_secs > 0 {
                    result.push_str(&format!("{}s", remaining_secs));
                }
                serializer.serialize_str(&result)
            } else {
                let days = secs / 86400;
                let hours = (secs % 86400) / 3600;
                let mins = (secs % 3600) / 60;
                let remaining_secs = secs % 60;
                let mut result = format!("{}d", days);
                if hours > 0 {
                    result.push_str(&format!("{}h", hours));
                }
                if mins > 0 {
                    result.push_str(&format!("{}m", mins));
                }
                if remaining_secs > 0 {
                    result.push_str(&format!("{}s", remaining_secs));
                }
                serializer.serialize_str(&result)
            }
        }
        None => serializer.serialize_none(),
    }
}
