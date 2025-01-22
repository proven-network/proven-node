use std::fs::read_to_string;
use std::path::Path;

pub fn check_hosts_file() -> bool {
    #[cfg(target_family = "unix")]
    {
        let hosts_path = Path::new("/etc/hosts");
        check_host_entry(hosts_path, "proven.local")
    }

    #[cfg(target_family = "windows")]
    {
        let hosts_path = Path::new(r"C:\Windows\System32\drivers\etc\hosts");
        check_host_entry(hosts_path, "proven.local")
    }
}

fn check_host_entry(hosts_path: &Path, hostname: &str) -> bool {
    read_to_string(hosts_path).map_or(false, |contents| {
        contents.lines().any(|line| {
            let line = line.trim();
            if line.starts_with('#') || line.is_empty() {
                return false;
            }
            line.split_whitespace()
                .any(|part| part == "127.0.0.1" || part == "::1")
                && line.contains(hostname)
        })
    })
}
