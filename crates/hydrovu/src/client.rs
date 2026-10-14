// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

use crate::models::{Location, LocationReadings, Names};
use anyhow::{Context, Result, anyhow};
use log::debug;
use oauth2::{AuthUrl, ClientId, ClientSecret, Scope, TokenResponse, TokenUrl, basic::BasicClient};
use std::time::Duration;

const BASE_URL: &str = "https://www.hydrovu.com";
const TIMEOUT_SECONDS: u64 = 60;

/// Async HydroVu API client
#[derive(Clone)]
pub struct Client {
    http_client: reqwest_oauth2::Client,
    token: String,
}

impl Client {
    /// Create a new client with OAuth2 authentication
    pub async fn new(client_id: String, client_secret: String) -> Result<Self> {
        let oauth_client = BasicClient::new(ClientId::new(client_id))
            .set_client_secret(ClientSecret::new(client_secret))
            .set_auth_uri(
                AuthUrl::new(Self::auth_url())
                    .with_context(|| "Failed to create authorization URL")?,
            )
            .set_token_uri(
                TokenUrl::new(Self::token_url()).with_context(|| "Failed to create token URL")?,
            );

        let http_client = reqwest_oauth2::Client::new();

        let token_result = oauth_client
            .exchange_client_credentials()
            .add_scope(Scope::new("read:locations".to_string()))
            .add_scope(Scope::new("read:data".to_string()))
            .request_async(&http_client)
            .await;

        let token = match token_result {
            Ok(token) => token.access_token().secret().clone(),
            Err(e) => {
                // Provide more detailed error information
                return Err(anyhow!(
                    "OAuth2 authentication failed: {}\n\
                    This could be due to:\n\
                    - Incorrect client_id or client_secret\n\
                    - Network connectivity issues\n\
                    - HydroVu API service unavailable\n\
                    - Invalid OAuth scopes\n\
                    \n\
                    Please verify your credentials at: https://www.hydrovu.com/public-api/docs/",
                    e
                ));
            }
        };

        let http_client = reqwest_oauth2::Client::builder()
            .timeout(Duration::from_secs(TIMEOUT_SECONDS))
            .build()
            .with_context(|| "Failed to create HTTP client")?;

        Ok(Client { http_client, token })
    }

    /// Fetch parameter and unit names/mappings
    pub(crate) async fn fetch_names(&self) -> Result<Names> {
        let url = Self::names_url();
        self.fetch_json(&url).await
    }

    /// Fetch list of available locations
    pub async fn fetch_locations(&self) -> Result<Vec<Location>> {
        let url = Self::locations_url();
        self.fetch_json(&url).await
    }

    /// Fetch data for a location with full pagination support and optional record limit
    #[allow(clippy::print_stderr)]
    pub(crate) async fn fetch_location_data(
        &self,
        location_id: i64,
        start_time: i64,
        stop_at_points: usize,
    ) -> Result<LocationReadings> {
        let base_url = Self::location_data_url(location_id, start_time, None);
        let mut all_data = None;
        let mut next_page_token: Option<String> = None;
        let mut page_count = 0;
        let mut total_points = 0;

        debug!("Starting fetch location {location_id} since timestamp {start_time}");

        loop {
            page_count += 1;

            // Build request with optional pagination token
            let mut request = self.http_client.get(&base_url).bearer_auth(&self.token);

            let pagination_header = if let Some(ref token) = next_page_token {
                debug!("Using pagination token for page {page_count}: {token}");
                request = request.header("x-isi-start-page", token);
                format!("x-isi-start-page: {}", token)
            } else {
                "(none - initial request)".to_string()
            };

            let response = request
                .send()
                .await
                .with_context(|| format!("Failed to send request to {}", base_url))?;

            if !response.status().is_success() {
                let status = response.status();
                let error_text = response
                    .text()
                    .await
                    .unwrap_or_else(|_| "Unknown error".to_string());

                // Print detailed error message to stderr (once)
                eprintln!(
                    "+==============================================================================+\n\
                     | HydroVu API Request Failed - Server Error                                   |\n\
                     +==============================================================================+\n\
                     \n\
                     REQUEST DETAILS:\n\
                     --------------------------------------------------------------------------------\n\
                     URL:              {}\n\
                     Method:           GET\n\
                     Authorization:    Bearer <token>\n\
                     Pagination:       {}\n\
                     \n\
                     QUERY PARAMETERS:\n\
                     --------------------------------------------------------------------------------\n\
                     Location ID:      {}\n\
                     Start Time:       {} (Unix timestamp)\n\
                     Start Date:       {}\n\
                     \n\
                     PAGINATION STATE:\n\
                     --------------------------------------------------------------------------------\n\
                     Current Page:     {}\n\
                     Points Fetched:   {} / {} (limit)\n\
                     \n\
                     RESPONSE:\n\
                     --------------------------------------------------------------------------------\n\
                     Status:           HTTP {}\n\
                     Body:\n\
                     {}\n\
                     \n\
                     +==============================================================================+\n\
                     | ACTION REQUIRED                                                              |\n\
                     +==============================================================================+\n\
                     \n\
                     This is a server-side error (HTTP 500). Please report to HydroVu support:\n\
                     \n\
                     1. Copy the COMPLETE error message above\n\
                     2. Include the exact request URL and parameters\n\
                     3. Include the timestamp range: {} onwards\n\
                     4. Include the location ID: {}\n\
                     5. Contact: https://www.hydrovu.com/support\n\
                     \n\
                     The error occurred while fetching page {} of the results.\n",
                    base_url,
                    pagination_header,
                    location_id,
                    start_time,
                    crate::utc2date(start_time).unwrap_or_else(|_| "invalid date".to_string()),
                    page_count,
                    total_points,
                    stop_at_points,
                    status,
                    error_text,
                    crate::utc2date(start_time).unwrap_or_else(|_| "invalid date".to_string()),
                    location_id,
                    page_count
                );

                // Return a concise error summary for the error chain
                return Err(anyhow!(
                    "HTTP {} from HydroVu API (location {}, page {})",
                    status,
                    location_id,
                    page_count
                ));
            }

            // Check for next page token in response headers
            next_page_token = response
                .headers()
                .get("x-isi-next-page")
                .and_then(|v| v.to_str().ok())
                .map(|s| s.to_string());

            let json_text = response
                .text()
                .await
                .with_context(|| "Failed to read response body")?;

            let page_data: LocationReadings = serde_json::from_str(&json_text)
                .with_context(|| format!("Failed to parse JSON response from {}", base_url))?;

            // Count points on this page
            let page_points: usize = page_data
                .parameters
                .iter()
                .map(|param| param.readings.len())
                .sum();

            total_points += page_points;
            debug!(
                "Page {page_count} for location {location_id}: {page_points} points (total: {total_points})"
            );

            // Merge this page with accumulated data
            match &mut all_data {
                None => all_data = Some(page_data),
                Some(accumulated) => {
                    // Merge the parameters from this page
                    for param_data in page_data.parameters {
                        // Find existing parameter or add new one
                        match accumulated
                            .parameters
                            .iter_mut()
                            .find(|p| p.parameter_id == param_data.parameter_id)
                        {
                            Some(existing_param) => {
                                existing_param.readings.extend(param_data.readings);
                            }
                            None => {
                                accumulated.parameters.push(param_data);
                            }
                        }
                    }
                }
            }

            // Continue if we have a next page token and haven't hit limit, otherwise break
            if next_page_token.is_none() {
                debug!("No more pages available for location {location_id}");
                break;
            } else if total_points >= stop_at_points {
                debug!("Reached point limit ({stop_at_points}) for location {location_id}");
                break;
            } else {
                debug!("Continuing to next page for location {location_id}...");
            }
        }

        debug!(
            "Completed fetch for location {location_id}: {page_count} pages, {total_points} total points"
        );
        all_data.ok_or_else(|| anyhow!("No data received from API"))
    }

    /// Generic JSON fetch with authentication and pagination support
    async fn fetch_json<T>(&self, url: &str) -> Result<T>
    where
        T: for<'de> serde::Deserialize<'de>,
    {
        let (data, _next_page) = self.fetch_json_paginated(url, None).await?;
        Ok(data)
    }

    /// Generic JSON fetch with authentication and pagination support
    async fn fetch_json_paginated<T>(
        &self,
        url: &str,
        start_page: Option<&str>,
    ) -> Result<(T, Option<String>)>
    where
        T: for<'de> serde::Deserialize<'de>,
    {
        let mut request = self.http_client.get(url).bearer_auth(&self.token);

        // Add pagination header if provided
        if let Some(page_token) = start_page {
            request = request.header("x-isi-start-page", page_token);
            debug!("Using pagination token for {url}: {page_token}");
        }

        let response = request
            .send()
            .await
            .with_context(|| format!("Failed to send request to {}", url))?;

        // Extract next page token from response headers
        let next_page = response
            .headers()
            .get("x-isi-next-page")
            .and_then(|header_value| header_value.to_str().ok())
            .map(|s| {
                debug!("Found next page token for {url}: {s}");
                s.to_string()
            });

        if !response.status().is_success() {
            let status = response.status();
            let error_text = response
                .text()
                .await
                .unwrap_or_else(|_| "Unknown error".to_string());
            return Err(anyhow!(
                "HTTP {} error from {}: {}",
                status,
                url,
                error_text
            ));
        }

        let json_text = response
            .text()
            .await
            .with_context(|| "Failed to read response body")?;

        let data: T = serde_json::from_str(&json_text)
            .with_context(|| format!("Failed to parse JSON response from {}", url))?;

        Ok((data, next_page))
    }

    // URL construction helpers
    fn combine(base: &str, path: &str) -> String {
        format!("{}/public-api/{}", base, path)
    }

    fn names_url() -> String {
        Self::combine(BASE_URL, "v1/sispec/friendlynames")
    }

    fn locations_url() -> String {
        Self::combine(BASE_URL, "v1/locations/list")
    }

    fn location_data_url(location_id: i64, start_time: i64, end_time: Option<i64>) -> String {
        // Convert from internal microseconds to HydroVu's expected seconds
        let start_time_seconds = start_time / 1_000_000;
        let mut url = format!(
            "v1/locations/{}/data?startTime={}",
            location_id, start_time_seconds
        );

        if let Some(end_time) = end_time {
            let end_time_seconds = end_time / 1_000_000;
            url.push_str(&format!("&endTime={}", end_time_seconds));
        }

        Self::combine(BASE_URL, &url)
    }

    fn auth_url() -> String {
        Self::combine(BASE_URL, "oauth/authorize")
    }

    fn token_url() -> String {
        Self::combine(BASE_URL, "oauth/token")
    }
}
