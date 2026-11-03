// SPDX-FileCopyrightText: 2026 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

use crate::{
    catalog::Registry,
    config::Config,
    journal::{Event, Journal, Segment},
};
use http_body_util::{BodyExt, Full};
use hyper::{
    Request, Response, StatusCode,
    body::{Bytes, Incoming},
    server::conn::http1,
    service::service_fn,
};
use hyper_util::rt::TokioIo;
use serde::Deserialize;
use std::{
    convert::Infallible,
    io,
    sync::{
        Arc, Mutex, RwLock,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
        mpsc,
    },
    thread::JoinHandle,
    time::{Duration, Instant},
};
use tokio::{
    net::TcpListener,
    sync::{Semaphore, oneshot},
    task::JoinSet,
};

const BODY_LIMIT: usize = 2048;
const QUEUE_CAPACITY: usize = 256;
const MAX_BATCH: usize = 128;
const MAX_CONNECTIONS: usize = 64;
const REQUESTS_PER_SECOND: u32 = 100;
const GROUP_DELAY: Duration = Duration::from_millis(10);
const CONNECTION_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Default)]
pub struct Health {
    pub ready: AtomicBool,
    accepted: AtomicU64,
    rejected: AtomicU64,
    rate_limited: AtomicU64,
    storage_failed: AtomicU64,
    disconnected: AtomicU64,
    pending: AtomicUsize,
    durable: Mutex<Option<Segment>>,
}

struct Pending {
    event: Event,
    reply: oneshot::Sender<bool>,
}

pub struct App {
    pub config: Config,
    pub registry: RwLock<Registry>,
    pub health: Arc<Health>,
    writer: mpsc::SyncSender<Pending>,
    rate: Mutex<(Instant, u32)>,
}

impl App {
    pub fn start(
        config: Config,
        registry: Registry,
        journal: Journal,
    ) -> io::Result<(Arc<Self>, JoinHandle<io::Result<()>>)> {
        let health = Arc::new(Health::default());
        health.ready.store(true, Ordering::SeqCst);
        let (writer, receiver) = mpsc::sync_channel(QUEUE_CAPACITY);
        let thread_health = health.clone();
        let handle = std::thread::Builder::new()
            .name("analytics-writer".into())
            .spawn(move || write_loop(journal, receiver, thread_health))?;
        Ok((
            Arc::new(Self {
                config,
                registry: RwLock::new(registry),
                health,
                writer,
                rate: Mutex::new((Instant::now(), 0)),
            }),
            handle,
        ))
    }

    fn allow_request(&self) -> bool {
        let mut rate = self.rate.lock().expect("rate mutex poisoned");
        if rate.0.elapsed() >= Duration::from_secs(1) {
            *rate = (Instant::now(), 0);
        }
        if rate.1 >= REQUESTS_PER_SECOND {
            return false;
        }
        rate.1 += 1;
        true
    }

    pub fn reload(&self) -> Result<(), String> {
        let new = Registry::load(&self.config)?;
        let mut registry = self.registry.write().expect("catalog lock poisoned");
        if !new.preserves(&registry) {
            return Err("catalog reload would remove historical catalogs; refusing reload".into());
        }
        *registry = new;
        Ok(())
    }

    async fn handle(&self, request: Request<Incoming>) -> Response<Full<Bytes>> {
        let response = self.dispatch(request).await;
        if response.status().is_client_error() {
            self.health.rejected.fetch_add(1, Ordering::Relaxed);
        }
        response
    }

    async fn dispatch(&self, mut request: Request<Incoming>) -> Response<Full<Bytes>> {
        if request.method() == hyper::Method::GET && request.uri() == "/_analytics/health" {
            let ready = self.health.ready.load(Ordering::SeqCst);
            let body = serde_json::json!({
                "ready": ready,
                "site_id": self.config.site_id,
                "catalog_ids": self.registry.read().expect("catalog lock poisoned").ids(),
                "accepted": self.health.accepted.load(Ordering::Relaxed),
                "rejected": self.health.rejected.load(Ordering::Relaxed),
                "rate_limited": self.health.rate_limited.load(Ordering::Relaxed),
                "storage_failed": self.health.storage_failed.load(Ordering::Relaxed),
                "disconnected": self.health.disconnected.load(Ordering::Relaxed),
                "pending": self.health.pending.load(Ordering::Relaxed),
                "durable": *self.health.durable.lock().expect("progress mutex poisoned"),
            })
            .to_string();
            let mut response = respond(if ready {
                StatusCode::OK
            } else {
                StatusCode::SERVICE_UNAVAILABLE
            });
            *response.body_mut() = Full::new(Bytes::from(body));
            response
                .headers_mut()
                .insert("content-type", "application/json".parse().expect("header"));
            return response;
        }
        if !self.allow_request() {
            self.health.rate_limited.fetch_add(1, Ordering::Relaxed);
            return respond(StatusCode::TOO_MANY_REQUESTS);
        }
        if request.uri().path_and_query().map(|p| p.as_str()) != Some(&self.config.endpoint) {
            return respond(StatusCode::NOT_FOUND);
        }
        if request.method() != hyper::Method::POST {
            return respond(StatusCode::METHOD_NOT_ALLOWED);
        }
        if single_header(&request, "host") != Some(self.config.host())
            || single_header(&request, "origin") != Some(&self.config.origin)
        {
            return respond(StatusCode::FORBIDDEN);
        }
        for (header, allowed) in [
            ("sec-fetch-site", &["same-origin"][..]),
            ("sec-fetch-mode", &["cors", "same-origin"][..]),
            ("sec-fetch-dest", &["empty"][..]),
        ] {
            if request.headers().contains_key(header)
                && single_header(&request, header).is_none_or(|value| !allowed.contains(&value))
            {
                return respond(StatusCode::FORBIDDEN);
            }
        }
        let content_type = single_header(&request, "content-type").unwrap_or("");
        if !content_type
            .split(';')
            .next()
            .unwrap_or("")
            .trim()
            .eq_ignore_ascii_case("application/json")
            || request.headers().contains_key("content-encoding")
        {
            return respond(StatusCode::UNSUPPORTED_MEDIA_TYPE);
        }
        if single_header(&request, "content-length")
            .and_then(|s| s.parse::<u64>().ok())
            .is_some_and(|length| length > BODY_LIMIT as u64)
        {
            return respond(StatusCode::PAYLOAD_TOO_LARGE);
        }
        if !self.health.ready.load(Ordering::SeqCst) {
            return respond(StatusCode::SERVICE_UNAVAILABLE);
        }
        let mut body = Vec::new();
        while let Some(frame) = request.body_mut().frame().await {
            let Ok(frame) = frame else {
                return respond(StatusCode::BAD_REQUEST);
            };
            if let Some(bytes) = frame.data_ref() {
                if body.len() + bytes.len() > BODY_LIMIT {
                    return respond(StatusCode::PAYLOAD_TOO_LARGE);
                }
                body.extend_from_slice(bytes);
            } else {
                return respond(StatusCode::BAD_REQUEST);
            }
        }
        let event = match serde_json::from_slice::<Payload>(&body) {
            Ok(payload)
                if payload.schema_version == 1
                    && payload.event == "pageview"
                    && payload.page_uri.len() <= 1024
                    && self
                        .registry
                        .read()
                        .expect("catalog lock poisoned")
                        .accepts(&payload.catalog_id, &payload.page_uri) =>
            {
                Event {
                    received_at_us: chrono::Utc::now().timestamp_micros(),
                    catalog_id: payload.catalog_id,
                    page_uri: payload.page_uri,
                }
            }
            _ => return respond(StatusCode::BAD_REQUEST),
        };
        let (reply, receipt) = oneshot::channel();
        self.health.pending.fetch_add(1, Ordering::Relaxed);
        match self.writer.try_send(Pending { event, reply }) {
            Ok(()) => {}
            Err(error) => {
                self.health.pending.fetch_sub(1, Ordering::Relaxed);
                return respond(match error {
                    mpsc::TrySendError::Full(_) => {
                        self.health.rate_limited.fetch_add(1, Ordering::Relaxed);
                        StatusCode::TOO_MANY_REQUESTS
                    }
                    mpsc::TrySendError::Disconnected(_) => StatusCode::SERVICE_UNAVAILABLE,
                });
            }
        }
        match receipt.await {
            Ok(true) => respond(StatusCode::NO_CONTENT),
            _ => respond(StatusCode::SERVICE_UNAVAILABLE),
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Payload {
    schema_version: u32,
    event: String,
    catalog_id: String,
    page_uri: String,
}

fn single_header<'a>(request: &'a Request<Incoming>, name: &str) -> Option<&'a str> {
    let mut values = request.headers().get_all(name).iter();
    let value = values.next()?.to_str().ok()?;
    if values.next().is_some() {
        return None;
    }
    Some(value)
}

fn respond(status: StatusCode) -> Response<Full<Bytes>> {
    Response::builder()
        .status(status)
        .header("cache-control", "no-store")
        .body(Full::new(Bytes::new()))
        .expect("constant response")
}

fn write_loop(
    mut journal: Journal,
    receiver: mpsc::Receiver<Pending>,
    health: Arc<Health>,
) -> io::Result<()> {
    while let Ok(first) = receiver.recv() {
        let deadline = Instant::now() + GROUP_DELAY;
        let mut batch = vec![first];
        while batch.len() < MAX_BATCH {
            match receiver.recv_timeout(deadline.saturating_duration_since(Instant::now())) {
                Ok(pending) => batch.push(pending),
                Err(_) => break,
            }
        }
        let events: Vec<_> = batch.iter().map(|p| p.event.clone()).collect();
        let result = journal.append(&events);
        match &result {
            Ok(segment) => {
                *health.durable.lock().expect("progress mutex poisoned") = Some(segment.clone());
                health
                    .accepted
                    .fetch_add(batch.len() as u64, Ordering::Relaxed);
            }
            Err(error) => {
                health.ready.store(false, Ordering::SeqCst);
                health
                    .storage_failed
                    .fetch_add(batch.len() as u64, Ordering::Relaxed);
                eprintln!("ERROR: analytics durable writer stopped: {error}");
            }
        }
        for pending in batch {
            health.pending.fetch_sub(1, Ordering::Relaxed);
            if pending.reply.send(result.is_ok()).is_err() {
                health.disconnected.fetch_add(1, Ordering::Relaxed);
            }
        }
        if let Err(error) = result {
            for pending in receiver.try_iter() {
                health.pending.fetch_sub(1, Ordering::Relaxed);
                health.storage_failed.fetch_add(1, Ordering::Relaxed);
                if pending.reply.send(false).is_err() {
                    health.disconnected.fetch_add(1, Ordering::Relaxed);
                }
            }
            return Err(error);
        }
    }
    Ok(())
}

pub async fn serve(listener: TcpListener, app: Arc<App>) -> io::Result<()> {
    let slots = Arc::new(Semaphore::new(MAX_CONNECTIONS));
    let mut connections = JoinSet::new();
    loop {
        while let Some(result) = connections.try_join_next() {
            result.map_err(|_| io::Error::other("collector connection task failed"))?;
        }
        tokio::select! {
            result = listener.accept() => {
                let (stream, _) = result?;
                let Ok(permit) = slots.clone().try_acquire_owned() else {
                    app.health.rate_limited.fetch_add(1, Ordering::Relaxed);
                    let response = b"HTTP/1.1 503 Service Unavailable\r\nConnection: close\r\nCache-Control: no-store\r\nContent-Length: 0\r\n\r\n";
                    // No extra task or buffer is allocated for overload responses.
                    if !matches!(stream.try_write(response), Ok(length) if length == response.len()) {
                        app.health.disconnected.fetch_add(1, Ordering::Relaxed);
                    }
                    continue;
                };
                let app = app.clone();
                connections.spawn(async move {
                    let _permit = permit;
                    let service_app = app.clone();
                    let service = service_fn(move |request| {
                        let app = service_app.clone();
                        async move { Ok::<_, Infallible>(app.handle(request).await) }
                    });
                    let result = tokio::time::timeout(CONNECTION_TIMEOUT,
                        http1::Builder::new().keep_alive(false).max_buf_size(8192)
                            .serve_connection(TokioIo::new(stream), service)).await;
                    if !matches!(result, Ok(Ok(()))) {
                        // Never format hyper errors: they can contain request details.
                        app.health.disconnected.fetch_add(1, Ordering::Relaxed);
                    }
                });
            }
            Some(result) = connections.join_next(), if !connections.is_empty() => {
                result.map_err(|_| io::Error::other("collector connection task failed"))?;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{catalog::install_fixture, config::fixture};
    use std::net::SocketAddr;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    struct Harness {
        temp: tempfile::TempDir,
        app: Arc<App>,
        writer: JoinHandle<io::Result<()>>,
        server: tokio::task::JoinHandle<io::Result<()>>,
        address: SocketAddr,
        id: String,
    }

    impl Harness {
        async fn new() -> Self {
            let temp = tempfile::tempdir().unwrap();
            let config = fixture(temp.path());
            let id = install_fixture(&config, "/noyo-harbor/");
            let registry = Registry::load(&config).unwrap();
            let journal = Journal::open(&config).unwrap();
            let (app, writer) = App::start(config, registry, journal).unwrap();
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let server = tokio::spawn(serve(listener, app.clone()));
            Self {
                temp,
                app,
                writer,
                server,
                address,
                id,
            }
        }

        fn body(&self) -> String {
            serde_json::json!({
                "schema_version": 1, "event": "pageview",
                "catalog_id": self.id, "page_uri": "/noyo-harbor/",
            })
            .to_string()
        }

        fn headers(&self) -> Vec<(&str, &str)> {
            vec![
                ("Host", "noyocenter.org"),
                ("Origin", "https://noyocenter.org"),
                ("Content-Type", "application/json"),
            ]
        }

        async fn request(
            &self,
            method: &str,
            path: &str,
            headers: &[(&str, &str)],
            body: &str,
        ) -> String {
            let mut stream = tokio::net::TcpStream::connect(self.address).await.unwrap();
            let mut request = format!(
                "{method} {path} HTTP/1.1\r\nContent-Length: {}\r\n",
                body.len()
            );
            for (key, value) in headers {
                request.push_str(&format!("{key}: {value}\r\n"));
            }
            request.push_str("\r\n");
            request.push_str(body);
            stream.write_all(request.as_bytes()).await.unwrap();
            let mut response = String::new();
            stream.read_to_string(&mut response).await.unwrap();
            response
        }

        async fn close(self) -> io::Result<()> {
            self.server.abort();
            assert!(self.server.await.unwrap_err().is_cancelled());
            drop(self.app);
            let result = self.writer.join().unwrap();
            drop(self.temp);
            result
        }
    }

    fn status(response: &str) -> u16 {
        response.split_whitespace().nth(1).unwrap().parse().unwrap()
    }

    #[tokio::test]
    async fn durable_response_records_only_approved_fields_and_private_health() {
        let harness = Harness::new().await;
        let mut headers = harness.headers();
        headers.extend([
            ("Cookie", "not-retained=secret"),
            ("Referer", "https://noyocenter.org/private"),
            ("User-Agent", "not-retained"),
            ("Sec-Fetch-Site", "same-origin"),
        ]);
        let response = harness
            .request("POST", "/_analytics/view", &headers, &harness.body())
            .await;
        assert_eq!(status(&response), 204);
        assert!(response.contains("cache-control: no-store"));
        assert!(!response.contains("set-cookie"));
        assert!(!response.contains("access-control-allow-origin"));
        let inventory: serde_json::Value = serde_json::from_slice(
            &std::fs::read(harness.app.config.log_dir.join("inventory.json")).unwrap(),
        )
        .unwrap();
        assert_eq!(
            inventory["active"]["record_count"], 1,
            "checkpoint durable before HTTP success"
        );
        let text = std::fs::read_to_string(harness.app.config.log_dir.join("views.jsonl")).unwrap();
        let record: serde_json::Value = serde_json::from_str(&text).unwrap();
        assert_eq!(record["site_id"], "noyo");
        assert_eq!(record["page_uri"], "/noyo-harbor/");
        assert_eq!(record.as_object().unwrap().len(), 7);
        assert!(!text.contains("secret"));
        assert!(!text.contains("private"));
        assert!(!text.contains("not-retained"));
        let health = harness
            .request("GET", "/_analytics/health", &[("Host", "localhost")], "")
            .await;
        assert_eq!(status(&health), 200);
        assert!(health.contains("\"accepted\":1"));
        harness.close().await.unwrap();
    }

    #[tokio::test]
    async fn invalid_requests_never_become_views() {
        let harness = Harness::new().await;
        let normal = harness.headers();
        for (method, path, headers, body, expected) in [
            (
                "GET",
                "/_analytics/view",
                normal.clone(),
                harness.body(),
                405,
            ),
            (
                "POST",
                "/_analytics/view?extra=secret",
                normal.clone(),
                harness.body(),
                404,
            ),
            (
                "POST",
                "/_analytics/view",
                vec![
                    ("Host", "noyocenter.org"),
                    ("Content-Type", "application/json"),
                ],
                harness.body(),
                403,
            ),
            (
                "POST",
                "/_analytics/view",
                vec![
                    ("Host", "noyocenter.org"),
                    ("Origin", "https://casparwater.us"),
                    ("Content-Type", "application/json"),
                ],
                harness.body(),
                403,
            ),
            (
                "POST",
                "/_analytics/view",
                normal
                    .iter()
                    .copied()
                    .chain([("Sec-Fetch-Site", "cross-site")])
                    .collect(),
                harness.body(),
                403,
            ),
            (
                "POST",
                "/_analytics/view",
                normal
                    .iter()
                    .copied()
                    .chain([("Content-Encoding", "gzip")])
                    .collect(),
                harness.body(),
                415,
            ),
            (
                "POST",
                "/_analytics/view",
                normal.clone(),
                "x".repeat(2049),
                413,
            ),
            (
                "POST",
                "/_analytics/view",
                normal.clone(),
                "{\"invalid\":true}".into(),
                400,
            ),
            (
                "POST",
                "/_analytics/view",
                normal.clone(),
                harness.body().replace("pageview", "click"),
                400,
            ),
            (
                "POST",
                "/_analytics/view",
                normal.clone(),
                harness.body().replace("/noyo-harbor/", "/"),
                400,
            ),
            (
                "POST",
                "/_analytics/view",
                normal.clone(),
                harness.body().replace(&harness.id, &"0".repeat(64)),
                400,
            ),
            (
                "POST",
                "/_analytics/view",
                normal.clone(),
                harness
                    .body()
                    .replacen('{', "{\"site_id\":\"caspar-water\",", 1),
                400,
            ),
        ] {
            let response = harness.request(method, path, &headers, &body).await;
            assert_eq!(status(&response), expected, "{method} {path}: {response}");
        }
        assert_eq!(
            std::fs::metadata(harness.app.config.log_dir.join("views.jsonl"))
                .unwrap()
                .len(),
            0
        );
        assert_eq!(harness.app.health.accepted.load(Ordering::Relaxed), 0);
        harness.close().await.unwrap();
    }

    #[tokio::test]
    async fn chunked_body_limit_and_connection_deadline_are_enforced() {
        let harness = Harness::new().await;
        let mut stream = tokio::net::TcpStream::connect(harness.address)
            .await
            .unwrap();
        stream.write_all(b"POST /_analytics/view HTTP/1.1\r\nHost: noyocenter.org\r\nOrigin: https://noyocenter.org\r\nContent-Type: application/json\r\nTransfer-Encoding: chunked\r\n\r\n").await.unwrap();
        stream
            .write_all(format!("801\r\n{}\r\n0\r\n\r\n", "x".repeat(2049)).as_bytes())
            .await
            .unwrap();
        let mut response = String::new();
        stream.read_to_string(&mut response).await.unwrap();
        assert_eq!(status(&response), 413);
        let mut slow = tokio::net::TcpStream::connect(harness.address)
            .await
            .unwrap();
        slow.write_all(b"POST /_analytics/view HTTP/1.1\r\nHost:")
            .await
            .unwrap();
        let mut byte = [0];
        let result = tokio::time::timeout(Duration::from_secs(7), slow.read(&mut byte))
            .await
            .unwrap();
        assert!(matches!(result, Ok(0)) || result.is_err());
        assert_eq!(harness.app.health.accepted.load(Ordering::Relaxed), 0);
        harness.close().await.unwrap();
    }

    #[tokio::test]
    async fn failed_catalog_reload_keeps_old_registry_and_new_catalogs_are_site_bound() {
        let harness = Harness::new().await;
        let new = install_fixture(&harness.app.config, "/");
        harness.app.reload().unwrap();
        assert!(harness.app.registry.read().unwrap().accepts(&new, "/"));
        std::fs::remove_file(
            harness
                .app
                .config
                .catalog_dir
                .join(format!("{}.json", harness.id)),
        )
        .unwrap();
        assert!(harness.app.reload().is_err());
        assert!(
            harness
                .app
                .registry
                .read()
                .unwrap()
                .accepts(&harness.id, "/noyo-harbor/")
        );
        let response = harness
            .request(
                "POST",
                "/_analytics/view",
                &harness.headers(),
                &harness.body(),
            )
            .await;
        assert_eq!(status(&response), 204);
        harness.close().await.unwrap();
    }

    #[tokio::test]
    async fn storage_failure_returns_unavailable_and_never_success() {
        let harness = Harness::new().await;
        // The writer must create this temporary control file to commit progress.
        std::fs::create_dir(harness.app.config.log_dir.join("inventory.tmp")).unwrap();
        let response = harness
            .request(
                "POST",
                "/_analytics/view",
                &harness.headers(),
                &harness.body(),
            )
            .await;
        assert_eq!(status(&response), 503);
        assert!(!harness.app.health.ready.load(Ordering::SeqCst));
        let retry = harness
            .request(
                "POST",
                "/_analytics/view",
                &harness.headers(),
                &harness.body(),
            )
            .await;
        assert_eq!(status(&retry), 503);
        assert_eq!(harness.app.health.accepted.load(Ordering::Relaxed), 0);
        let health = harness
            .request("GET", "/_analytics/health", &[("Host", "localhost")], "")
            .await;
        assert_eq!(status(&health), 503);
        assert!(harness.close().await.is_err());
    }

    #[tokio::test]
    async fn queue_saturation_is_bounded_and_explicit() {
        let temp = tempfile::tempdir().unwrap();
        let config = fixture(temp.path());
        let id = install_fixture(&config, "/");
        let registry = Registry::load(&config).unwrap();
        let (writer, receiver) = mpsc::sync_channel(1);
        let health = Arc::new(Health::default());
        health.ready.store(true, Ordering::SeqCst);
        writer
            .try_send(Pending {
                event: Event {
                    received_at_us: 0,
                    catalog_id: id.clone(),
                    page_uri: "/".into(),
                },
                reply: oneshot::channel().0,
            })
            .unwrap();
        let app = Arc::new(App {
            config,
            registry: RwLock::new(registry),
            health,
            writer,
            rate: Mutex::new((Instant::now(), 0)),
        });
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let task = tokio::spawn(serve(listener, app.clone()));
        let mut stream = tokio::net::TcpStream::connect(address).await.unwrap();
        let body = serde_json::json!({"schema_version":1,"event":"pageview","catalog_id":id,"page_uri":"/"}).to_string();
        stream.write_all(format!("POST /_analytics/view HTTP/1.1\r\nHost: noyocenter.org\r\nOrigin: https://noyocenter.org\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}", body.len()).as_bytes()).await.unwrap();
        let mut response = String::new();
        stream.read_to_string(&mut response).await.unwrap();
        assert_eq!(status(&response), 429);
        assert_eq!(app.health.pending.load(Ordering::Relaxed), 0);
        assert!(receiver.try_recv().is_ok());
        assert!(receiver.try_recv().is_err());
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
    }

    #[tokio::test]
    async fn fixed_window_rate_limit_rejects_without_reading_more_bodies() {
        let harness = Harness::new().await;
        *harness.app.rate.lock().unwrap() = (Instant::now(), REQUESTS_PER_SECOND);
        let response = harness
            .request(
                "POST",
                "/_analytics/view",
                &harness.headers(),
                &harness.body(),
            )
            .await;
        assert_eq!(status(&response), 429);
        assert_eq!(harness.app.health.accepted.load(Ordering::Relaxed), 0);
        harness.close().await.unwrap();
    }
}
