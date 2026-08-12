// SPDX-FileCopyrightText: 2026 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

use crate::SiteConfig;
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use clap::Parser;
use hmac::{Hmac, Mac};
use provider::registry::{ExecutionContext, ExecutionMode, FactoryCommand};
use provider::{FactoryContext, register_executable_factory};
use reqwest::{Method, Response, StatusCode};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::time::{Duration, Instant, SystemTime};
use tinyfs::ResultExt;
use url::Url;
use uuid::Uuid;

const EMAIL_API_VERSION: &str = "2025-09-01";
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(300);

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct EmailReportConfig {
    sitegen: String,
    report: String,
    endpoint_env: String,
    access_key_env: String,
    recipient_env: String,
    sender: String,
    subject: String,
    #[serde(default)]
    reply_to: Option<String>,
    #[serde(default)]
    timeout: Option<String>,
}

#[derive(Debug, Parser)]
enum EmailReportCommand {
    /// Render and send the configured report.
    Send,
}

impl FactoryCommand for EmailReportCommand {
    fn allowed(&self) -> ExecutionMode {
        ExecutionMode::PondReadWriter
    }
}

fn validate_config(config: &[u8]) -> tinyfs::Result<Value> {
    let config: EmailReportConfig =
        serde_yaml::from_slice(config).map_other_context("Invalid email-report config")?;
    config.validate().map_err(tinyfs::Error::InvalidConfig)?;
    serde_json::to_value(config).map_other_context("Config serialization error")
}

impl EmailReportConfig {
    fn validate(&self) -> Result<(), String> {
        for (field, value) in [
            ("sitegen", self.sitegen.as_str()),
            ("report", self.report.as_str()),
            ("sender", self.sender.as_str()),
            ("subject", self.subject.as_str()),
        ] {
            if value.trim().is_empty() {
                return Err(format!("{field} cannot be empty"));
            }
        }
        for (field, value) in [
            ("endpoint_env", self.endpoint_env.as_str()),
            ("access_key_env", self.access_key_env.as_str()),
            ("recipient_env", self.recipient_env.as_str()),
        ] {
            if !valid_env_name(value) {
                return Err(format!("{field} must name an environment variable"));
            }
        }
        validate_email_address("sender", &self.sender)?;
        if let Some(reply_to) = &self.reply_to {
            validate_email_address("reply_to", reply_to)?;
        }
        if let Some(timeout) = &self.timeout {
            let timeout = humantime::parse_duration(timeout)
                .map_err(|error| format!("invalid timeout: {error}"))?;
            if timeout.is_zero() {
                return Err("timeout must be greater than zero".to_string());
            }
        }
        Ok(())
    }
}

fn valid_env_name(value: &str) -> bool {
    let mut characters = value.chars();
    characters
        .next()
        .is_some_and(|character| character == '_' || character.is_ascii_alphabetic())
        && characters.all(|character| character == '_' || character.is_ascii_alphanumeric())
}

fn validate_email_address(field: &str, value: &str) -> Result<(), String> {
    let mut parts = value.split('@');
    let local = parts.next().unwrap_or_default();
    let domain = parts.next().unwrap_or_default();
    if local.is_empty()
        || domain.is_empty()
        || parts.next().is_some()
        || value.chars().any(char::is_whitespace)
    {
        return Err(format!("{field} must be an email address"));
    }
    Ok(())
}

async fn initialize(_config: Value, _context: FactoryContext) -> Result<(), tinyfs::Error> {
    Ok(())
}

async fn execute(
    config: Value,
    context: FactoryContext,
    execution: ExecutionContext,
) -> Result<(), tinyfs::Error> {
    let config: EmailReportConfig =
        serde_json::from_value(config).map_other_context("Invalid email-report config")?;
    let args = std::iter::once("factory".to_string())
        .chain(execution.args().iter().cloned())
        .collect::<Vec<_>>();
    let command = EmailReportCommand::try_parse_from(args).map_other()?;
    match command {
        EmailReportCommand::Send => send_report(&config, &context).await,
    }
    .map_err(tinyfs::Error::Other)
}

async fn send_report(config: &EmailReportConfig, context: &FactoryContext) -> Result<(), String> {
    let endpoint = required_env(&config.endpoint_env)?;
    let access_key = required_env(&config.access_key_env)?;
    let recipient = required_env(&config.recipient_env)?;
    validate_email_address("recipient", &recipient)?;

    let root = context.root().await.map_err(|error| error.to_string())?;
    let sitegen_bytes = root
        .read_file_path_to_vec(&config.sitegen)
        .await
        .map_err(|error| format!("cannot read sitegen config '{}': {error}", config.sitegen))?;
    let sitegen: SiteConfig = serde_yaml::from_slice(&sitegen_bytes)
        .map_err(|error| format!("invalid sitegen config '{}': {error}", config.sitegen))?;
    let report = crate::report::render_named_report(
        &sitegen,
        &root,
        &context.context,
        &config.report,
        chrono::Utc::now(),
    )
    .await
    .map_err(|error| error.to_string())?;

    let payload = email_payload(config, &recipient, &report);
    let timeout = config
        .timeout
        .as_deref()
        .map(humantime::parse_duration)
        .transpose()
        .map_err(|error| format!("invalid timeout: {error}"))?
        .unwrap_or(DEFAULT_TIMEOUT);
    let operation = send_email(&endpoint, &access_key, &payload, timeout).await?;
    log::info!(
        "Email report '{}' sent successfully (operation {})",
        config.report,
        operation
    );
    Ok(())
}

fn required_env(name: &str) -> Result<String, String> {
    std::env::var(name)
        .map_err(|error| format!("missing environment variable {name}: {error}"))
        .and_then(|value| {
            if value.trim().is_empty() {
                Err(format!("environment variable {name} is empty"))
            } else {
                Ok(value)
            }
        })
}

fn email_payload(
    config: &EmailReportConfig,
    recipient: &str,
    report: &crate::report::RenderedReport,
) -> Value {
    let mut html = report.html.clone();
    let attachments: Vec<Value> = report
        .charts
        .iter()
        .map(|chart| {
            html = html.replace(
                &format!("src=\"{}\"", chart.filename),
                &format!("src=\"cid:{}\"", chart.filename),
            );
            json!({
                "name": chart.filename,
                "contentType": "image/png",
                "contentInBase64": BASE64.encode(&chart.png),
                "contentId": chart.filename,
            })
        })
        .collect();
    let mut payload = json!({
        "senderAddress": config.sender,
        "content": {
            "subject": config.subject,
            "plainText": report.plain_text,
            "html": html,
        },
        "recipients": {
            "to": [{"address": recipient}],
        },
        "attachments": attachments,
        "userEngagementTrackingDisabled": true,
    });
    if let Some(reply_to) = &config.reply_to {
        payload["replyTo"] = json!([{"address": reply_to}]);
    }
    payload
}

async fn send_email(
    endpoint: &str,
    access_key: &str,
    payload: &Value,
    timeout: Duration,
) -> Result<String, String> {
    let endpoint =
        Url::parse(endpoint).map_err(|error| format!("invalid ACS email endpoint: {error}"))?;
    if endpoint.scheme() != "https" || endpoint.host_str().is_none() {
        return Err("ACS email endpoint must be an HTTPS URL with a host".to_string());
    }
    let mut send_url = endpoint.clone();
    send_url.set_path("/emails:send");
    send_url.set_query(Some(&format!("api-version={EMAIL_API_VERSION}")));
    let body = serde_json::to_vec(payload)
        .map_err(|error| format!("cannot serialize email request: {error}"))?;
    let client = reqwest::Client::builder()
        .timeout(timeout)
        .build()
        .map_err(|error| format!("cannot create email client: {error}"))?;
    let operation_id = Uuid::new_v4().to_string();
    let response = authenticated_request(
        &client,
        Method::POST,
        send_url,
        access_key,
        body,
        Some(&operation_id),
    )
    .await?;
    if response.status() != StatusCode::ACCEPTED {
        return Err(response_error("email send", &response));
    }
    let operation_location = response
        .headers()
        .get("operation-location")
        .ok_or_else(|| "email send response omitted Operation-Location".to_string())?
        .to_str()
        .map_err(|error| format!("invalid Operation-Location header: {error}"))?;
    let operation_url = Url::parse(operation_location)
        .map_err(|error| format!("invalid Operation-Location URL: {error}"))?;
    if operation_url.host_str() != endpoint.host_str() || operation_url.scheme() != "https" {
        return Err("email operation URL does not match the configured endpoint".to_string());
    }
    let mut retry_delay = retry_after(&response);
    let started = Instant::now();
    loop {
        if started.elapsed() >= timeout {
            return Err(format!(
                "email operation {operation_id} did not complete within {}",
                humantime::format_duration(timeout)
            ));
        }
        tokio::time::sleep(retry_delay).await;
        let response = authenticated_request(
            &client,
            Method::GET,
            operation_url.clone(),
            access_key,
            Vec::new(),
            None,
        )
        .await?;
        if response.status() != StatusCode::OK {
            return Err(response_error("email operation", &response));
        }
        retry_delay = retry_after(&response);
        let status: Value = response
            .json()
            .await
            .map_err(|error| format!("invalid email operation response: {error}"))?;
        match status.get("status").and_then(Value::as_str) {
            Some("Succeeded") => return Ok(operation_id),
            Some("Failed" | "Canceled") => {
                let code = status
                    .pointer("/error/code")
                    .and_then(Value::as_str)
                    .unwrap_or("unknown");
                return Err(format!("email operation {operation_id} failed ({code})"));
            }
            Some("NotStarted" | "Running") => {}
            Some(other) => {
                return Err(format!(
                    "email operation {operation_id} returned unknown status '{other}'"
                ));
            }
            None => {
                return Err(format!(
                    "email operation {operation_id} response omitted status"
                ));
            }
        }
    }
}

async fn authenticated_request(
    client: &reqwest::Client,
    method: Method,
    url: Url,
    access_key: &str,
    body: Vec<u8>,
    operation_id: Option<&str>,
) -> Result<Response, String> {
    let date = httpdate::fmt_http_date(SystemTime::now());
    let content_hash = BASE64.encode(Sha256::digest(&body));
    let authorization = authorization(&method, &url, &date, &content_hash, access_key)?;
    let mut request = client
        .request(method, url)
        .header("Authorization", authorization)
        .header("x-ms-date", date)
        .header("x-ms-content-sha256", content_hash);
    if let Some(operation_id) = operation_id {
        request = request
            .header("Operation-Id", operation_id)
            .header("Content-Type", "application/json");
    }
    request
        .body(body)
        .send()
        .await
        .map_err(|error| format!("ACS email request failed: {error}"))
}

fn authorization(
    method: &Method,
    url: &Url,
    date: &str,
    content_hash: &str,
    access_key: &str,
) -> Result<String, String> {
    let host = url
        .host_str()
        .ok_or_else(|| "ACS email URL has no host".to_string())?;
    let path_and_query = url.path_and_query();
    let string_to_sign = format!(
        "{}\n{}\n{};{};{}",
        method.as_str(),
        path_and_query,
        date,
        host,
        content_hash
    );
    let key = BASE64
        .decode(access_key)
        .map_err(|_| "ACS email access key is not valid base64".to_string())?;
    let mut mac = Hmac::<Sha256>::new_from_slice(&key)
        .map_err(|_| "ACS email access key is invalid".to_string())?;
    mac.update(string_to_sign.as_bytes());
    let signature = BASE64.encode(mac.finalize().into_bytes());
    Ok(format!(
        "HMAC-SHA256 SignedHeaders=x-ms-date;host;x-ms-content-sha256&Signature={signature}"
    ))
}

trait UrlPathAndQuery {
    fn path_and_query(&self) -> String;
}

impl UrlPathAndQuery for Url {
    fn path_and_query(&self) -> String {
        self.query().map_or_else(
            || self.path().to_string(),
            |query| format!("{}?{query}", self.path()),
        )
    }
}

fn retry_after(response: &Response) -> Duration {
    response
        .headers()
        .get("retry-after")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok())
        .map_or(Duration::from_secs(5), |seconds| {
            Duration::from_secs(seconds.clamp(1, 30))
        })
}

fn response_error(action: &str, response: &Response) -> String {
    let error_code = response
        .headers()
        .get("x-ms-error-code")
        .and_then(|value| value.to_str().ok())
        .unwrap_or("unknown");
    format!(
        "{action} returned HTTP {} ({error_code})",
        response.status()
    )
}

register_executable_factory!(
    name: "email-report",
    description: "Render a named sitegen report and deliver it through Azure Communication Services Email",
    validate: validate_config,
    initialize: initialize,
    execute: execute
);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::report::{RenderedReport, ReportChart};

    fn config() -> EmailReportConfig {
        EmailReportConfig {
            sitegen: "/system/etc/90-sitegen".to_string(),
            report: "weekly".to_string(),
            endpoint_env: "ACS_EMAIL_ENDPOINT".to_string(),
            access_key_env: "ACS_EMAIL_ACCESS_KEY".to_string(),
            recipient_env: "WEEKLY_REPORT_RECIPIENT".to_string(),
            sender: "reports@casparwater.us".to_string(),
            subject: "Weekly pond summary".to_string(),
            reply_to: None,
            timeout: None,
        }
    }

    #[test]
    fn config_requires_env_names_and_addresses() {
        assert!(config().validate().is_ok());
        let mut bad_env = config();
        bad_env.recipient_env = "person@example.test".to_string();
        assert!(bad_env.validate().is_err());
        let mut bad_sender = config();
        bad_sender.sender = "reports".to_string();
        assert!(bad_sender.validate().is_err());
    }

    #[test]
    fn charts_become_inline_attachments() {
        let report = RenderedReport {
            html: "<img src=\"chart-0.png\">".to_string(),
            plain_text: "summary".to_string(),
            charts: vec![ReportChart {
                filename: "chart-0.png".to_string(),
                png: vec![1, 2, 3],
            }],
        };
        let payload = email_payload(&config(), "private@example.test", &report);
        assert_eq!(
            payload.pointer("/attachments/0/contentId"),
            Some(&json!("chart-0.png"))
        );
        assert!(
            payload
                .pointer("/content/html")
                .and_then(Value::as_str)
                .is_some_and(|html| html.contains("src=\"cid:chart-0.png\""))
        );
    }

    #[test]
    fn signing_is_stable_and_covers_query() {
        let url = Url::parse("https://example.communication.azure.com/emails:send?api-version=1")
            .expect("URL");
        let key = BASE64.encode(b"test key");
        let first = authorization(
            &Method::POST,
            &url,
            "Wed, 12 Aug 2026 19:00:00 GMT",
            "content",
            &key,
        )
        .expect("signature");
        let second = authorization(
            &Method::POST,
            &url,
            "Wed, 12 Aug 2026 19:00:00 GMT",
            "content",
            &key,
        )
        .expect("signature");
        assert_eq!(first, second);
        assert!(first.contains("SignedHeaders=x-ms-date;host;x-ms-content-sha256"));
    }

    #[test]
    fn send_url_keeps_the_https_endpoint() {
        let mut url = Url::parse("https://example.communication.azure.com").expect("endpoint URL");
        url.set_path("/emails:send");
        url.set_query(Some(&format!("api-version={EMAIL_API_VERSION}")));
        assert_eq!(url.scheme(), "https");
        assert_eq!(url.host_str(), Some("example.communication.azure.com"));
        assert_eq!(url.path_and_query(), "/emails:send?api-version=2025-09-01");
    }
}
