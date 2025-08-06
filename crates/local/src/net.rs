use crate::error::Error;

use tracing::info;

/// Fetches the external IP address using myip.com API
pub async fn fetch_external_ip() -> Result<std::net::IpAddr, Error> {
    let response = reqwest::get("https://api.ipify.org?format=json")
        .await
        .map_err(|e| Error::Io(format!("Failed to fetch external IP: {e}")))?;

    let json_response = response
        .json::<serde_json::Value>()
        .await
        .map_err(|e| Error::Io(format!("Failed to parse JSON response: {e}")))?;

    let ip_text = json_response["ip"]
        .as_str()
        .ok_or_else(|| Error::Io("IP field not found in response".to_string()))?;

    let ip_addr = ip_text
        .parse::<std::net::IpAddr>()
        .map_err(|e| Error::Io(format!("Failed to parse external IP: {e}")))?;

    info!("External IP detected: {}", ip_addr);

    Ok(ip_addr)
}
