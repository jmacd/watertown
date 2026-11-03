// SPDX-FileCopyrightText: 2026 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

use serde::Serialize;
use sha2::{Digest, Sha256};
use std::{
    fs::File,
    io::{Read, Write},
    net::{SocketAddr, TcpListener, TcpStream},
    path::Path,
    process::{Child, Command, Stdio},
    thread::sleep,
    time::{Duration, Instant},
};

struct Process(Child);

impl Drop for Process {
    fn drop(&mut self) {
        if self.0.try_wait().expect("process status").is_none() {
            self.0.kill().expect("kill test process");
            self.0.wait().expect("reap test process");
        }
    }
}

fn request(address: SocketAddr, method: &str, path: &str, body: &str) -> String {
    let mut stream = TcpStream::connect_timeout(&address, Duration::from_secs(1)).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(6)))
        .unwrap();
    write!(stream, "{method} {path} HTTP/1.1\r\nHost: noyocenter.org\r\nOrigin: https://noyocenter.org\r\nContent-Type: application/json\r\nCookie: private-cookie-marker\r\nUser-Agent: private-agent-marker\r\nContent-Length: {}\r\n\r\n{body}", body.len()).unwrap();
    let mut response = String::new();
    stream.read_to_string(&mut response).unwrap();
    response
}

fn start(config: &Path, stderr: &Path, address: SocketAddr) -> Process {
    let child = Command::new(env!("CARGO_BIN_EXE_site-analytics-collector"))
        .args(["--config", config.to_str().unwrap()])
        .stdout(Stdio::null())
        .stderr(
            File::options()
                .create(true)
                .append(true)
                .open(stderr)
                .unwrap(),
        )
        .spawn()
        .unwrap();
    let mut process = Process(child);
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if let Some(status) = process.0.try_wait().unwrap() {
            panic!(
                "collector exited {status}: {}",
                std::fs::read_to_string(stderr).unwrap()
            );
        }
        if TcpStream::connect(address).is_ok() {
            assert!(request(address, "GET", "/_analytics/health", "").starts_with("HTTP/1.1 200"));
            return process;
        }
        assert!(Instant::now() < deadline, "collector did not become ready");
        sleep(Duration::from_millis(20));
    }
}

#[test]
fn acknowledged_records_survive_process_kill_and_catalog_reload_is_host_side() {
    let temp = tempfile::tempdir().unwrap();
    let catalogs = temp.path().join("catalogs");
    std::fs::create_dir(&catalogs).unwrap();
    #[derive(Serialize)]
    struct Entry {
        page_uri: &'static str,
        title: &'static str,
        aliases: Vec<&'static str>,
    }
    #[derive(Serialize)]
    struct Identity {
        schema_version: u32,
        site_id: &'static str,
        entries: Vec<Entry>,
    }
    let canonical = serde_json::to_vec(&Identity {
        schema_version: 1,
        site_id: "noyo",
        entries: vec![Entry {
            page_uri: "/",
            title: "Home",
            aliases: vec!["/", "/index", "/index.html"],
        }],
    })
    .unwrap();
    let id: String = Sha256::digest(&canonical)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    let mut catalog: serde_json::Value = serde_json::from_slice(&canonical).unwrap();
    catalog["catalog_id"] = id.clone().into();
    let catalog_path = catalogs.join(format!("{id}.json"));
    std::fs::write(&catalog_path, serde_json::to_vec(&catalog).unwrap()).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    drop(listener);
    let config = temp.path().join("collector.json");
    let logs = temp.path().join("logs");
    std::fs::write(
        &config,
        serde_json::to_vec(&serde_json::json!({
            "site_id": "noyo", "origin": "https://noyocenter.org", "listen": address.to_string(),
            "catalog_dir": catalogs, "log_dir": logs, "endpoint": "/_analytics/view",
            "max_log_bytes": 1048576, "segment_bytes": 4096,
        }))
        .unwrap(),
    )
    .unwrap();
    let stderr = temp.path().join("stderr.log");
    let mut process = start(&config, &stderr, address);
    let payload = serde_json::json!({
        "schema_version":1, "event":"pageview", "catalog_id":id, "page_uri":"/",
    })
    .to_string();
    let started = Instant::now();
    for _ in 0..12 {
        assert!(request(address, "POST", "/_analytics/view", &payload).starts_with("HTTP/1.1 204"));
    }
    eprintln!("12 durable sequential views: {:?}", started.elapsed());
    assert!(
        request(
            address,
            "POST",
            "/_analytics/view",
            "{\"private-body-marker\":true}"
        )
        .starts_with("HTTP/1.1 400")
    );
    process.0.kill().unwrap();
    process.0.wait().unwrap();
    drop(process);

    let mut process = start(&config, &stderr, address);
    let inventory: serde_json::Value =
        serde_json::from_slice(&std::fs::read(logs.join("inventory.json")).unwrap()).unwrap();
    let count: u64 = inventory["closed"]
        .as_array()
        .unwrap()
        .iter()
        .map(|s| s["record_count"].as_u64().unwrap())
        .sum();
    assert_eq!(count, 12);
    std::fs::remove_file(&catalog_path).unwrap();
    assert!(
        Command::new("kill")
            .args(["-HUP", &process.0.id().to_string()])
            .status()
            .unwrap()
            .success()
    );
    let deadline = Instant::now() + Duration::from_secs(5);
    while !std::fs::read_to_string(&stderr)
        .unwrap()
        .contains("catalog reload rejected")
    {
        assert!(
            Instant::now() < deadline,
            "missing reload failure diagnostic"
        );
        sleep(Duration::from_millis(20));
    }
    assert!(request(address, "POST", "/_analytics/view", &payload).starts_with("HTTP/1.1 204"));
    assert!(
        Command::new("kill")
            .args(["-TERM", &process.0.id().to_string()])
            .status()
            .unwrap()
            .success()
    );
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if let Some(status) = process.0.try_wait().unwrap() {
            assert!(status.success(), "{status}");
            break;
        }
        assert!(
            Instant::now() < deadline,
            "collector did not drain on SIGTERM"
        );
        sleep(Duration::from_millis(20));
    }
    let mut retained = std::fs::read_to_string(stderr).unwrap();
    for entry in std::fs::read_dir(logs).unwrap() {
        retained.push_str(&String::from_utf8_lossy(
            &std::fs::read(entry.unwrap().path()).unwrap(),
        ));
    }
    for marker in [
        "private-cookie-marker",
        "private-agent-marker",
        "private-body-marker",
    ] {
        assert!(
            !retained.contains(marker),
            "unexpected retained request data: {marker}"
        );
    }
}
