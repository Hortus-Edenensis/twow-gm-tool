use std::convert::Infallible;
use std::io::{Read, Write};
use std::net::SocketAddr;
use std::net::TcpStream;
use std::process::Command;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use std::time::Duration;

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine as _;
use bytes::Bytes;
use hmac::{Hmac, Mac};
use http::header::{AUTHORIZATION, CONTENT_TYPE, HeaderMap};
use http::{Method, Request, Response, StatusCode, Uri};
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::Sha256;
use thiserror::Error;

#[cfg(test)]
use std::sync::Mutex;

const DEFAULT_BIND_ADDR: &str = "0.0.0.0:8080";
const DEFAULT_REALM_ID_STR: &str = "1";
const DEFAULT_SINK_MODE: &str = "pending_commands";
const DEFAULT_WORLD_TIMEOUT_SECONDS_STR: &str = "5";
const MAX_COMMAND_LEN: usize = 512;
type HmacSha256 = Hmac<Sha256>;

pub type ResponseBody = Full<Bytes>;

#[derive(Debug, Clone)]
pub struct Config {
    pub bind_addr: SocketAddr,
    pub sink_mode: SinkMode,
    pub command_allowlist: Vec<String>,
    pub jws_secret: String,
    pub jws_issuer: String,
    pub jws_audience: String,
    pub db_host: String,
    pub db_port: u16,
    pub db_user: String,
    pub db_password: String,
    pub logon_db: String,
    pub default_realm_id: u32,
    pub world_base_url: Option<String>,
    pub world_api_key: Option<String>,
    pub world_timeout_seconds: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SinkMode {
    PendingCommands,
    DirectWorldHttp,
}

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("missing required environment variable: {0}")]
    MissingEnv(&'static str),
    #[error("invalid socket address in GM_TOOL_BIND_ADDR: {0}")]
    InvalidBindAddr(String),
    #[error("invalid GM_TOOL_SINK_MODE: {0}")]
    InvalidSinkMode(String),
    #[error("invalid integer in {key}: {value}")]
    InvalidInteger { key: &'static str, value: String },
}

impl Config {
    pub fn from_env() -> Result<Self, ConfigError> {
        let bind_addr = env_or_default("GM_TOOL_BIND_ADDR", DEFAULT_BIND_ADDR);
        let bind_addr = bind_addr
            .parse::<SocketAddr>()
            .map_err(|_| ConfigError::InvalidBindAddr(bind_addr.clone()))?;
        let sink_mode = parse_sink_mode("GM_TOOL_SINK_MODE", DEFAULT_SINK_MODE)?;
        let command_allowlist = parse_command_allowlist_env("GM_TOOL_COMMAND_ALLOWLIST")?;
        let jws_secret = required_env("GM_TOOL_JWS_SECRET")?;
        let jws_issuer = required_env("GM_TOOL_JWS_ISSUER")?;
        let jws_audience = required_env("GM_TOOL_JWS_AUDIENCE")?;
        let db_host = required_env("TWOW_DB_HOST")?;
        let db_port = parse_env_u16("TWOW_DB_PORT", "3306")?;
        let db_user = required_env("TWOW_DB_USER")?;
        let db_password = required_env("TWOW_DB_PASSWORD")?;
        let logon_db = required_env("TWOW_LOGON_DB")?;
        let default_realm_id = parse_env_u32("GM_TOOL_DEFAULT_REALM_ID", DEFAULT_REALM_ID_STR)?;
        let world_timeout_seconds = parse_env_u64(
            "GM_TOOL_WORLD_TIMEOUT_SECONDS",
            DEFAULT_WORLD_TIMEOUT_SECONDS_STR)?;
        let world_base_url = match sink_mode {
            SinkMode::PendingCommands => std::env::var("GM_TOOL_WORLD_BASE_URL").ok(),
            SinkMode::DirectWorldHttp => Some(required_env("GM_TOOL_WORLD_BASE_URL")?),
        };
        let world_api_key = match sink_mode {
            SinkMode::PendingCommands => std::env::var("GM_TOOL_WORLD_API_KEY").ok(),
            SinkMode::DirectWorldHttp => Some(required_env("GM_TOOL_WORLD_API_KEY")?),
        };

        Ok(Self {
            bind_addr,
            sink_mode,
            command_allowlist,
            jws_secret,
            jws_issuer,
            jws_audience,
            db_host,
            db_port,
            db_user,
            db_password,
            logon_db,
            default_realm_id,
            world_base_url,
            world_api_key,
            world_timeout_seconds,
        })
    }
}

fn required_env(key: &'static str) -> Result<String, ConfigError> {
    std::env::var(key).map_err(|_| ConfigError::MissingEnv(key))
}

fn env_or_default(key: &'static str, default: &'static str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

fn parse_env_u16(key: &'static str, default: &'static str) -> Result<u16, ConfigError> {
    let value = env_or_default(key, default);
    value.parse::<u16>().map_err(|_| ConfigError::InvalidInteger { key, value })
}

fn parse_env_u32(key: &'static str, default: &'static str) -> Result<u32, ConfigError> {
    let value = env_or_default(key, default);
    value.parse::<u32>().map_err(|_| ConfigError::InvalidInteger { key, value })
}

fn parse_env_u64(key: &'static str, default: &'static str) -> Result<u64, ConfigError> {
    let value = env_or_default(key, default);
    value.parse::<u64>().map_err(|_| ConfigError::InvalidInteger { key, value })
}

fn parse_sink_mode(key: &'static str, default: &'static str) -> Result<SinkMode, ConfigError> {
    let value = env_or_default(key, default);
    match value.as_str() {
        "pending_commands" => Ok(SinkMode::PendingCommands),
        "direct_world_http" => Ok(SinkMode::DirectWorldHttp),
        _ => Err(ConfigError::InvalidSinkMode(value)),
    }
}

fn parse_command_allowlist_env(key: &'static str) -> Result<Vec<String>, ConfigError> {
    let Some(raw_value) = std::env::var(key).ok() else {
        return Ok(Vec::new());
    };

    raw_value
        .split(',')
        .map(normalize_allowlist_entry)
        .filter(|entry| !entry.is_empty())
        .map(|entry| {
            validate_command(&entry)
                .map(|_| entry)
                .map_err(|error| ConfigError::InvalidSinkMode(format!("{key}: {error}")))
        })
        .collect()
}

#[derive(Clone)]
pub struct AppState {
    jws_secret: Arc<Vec<u8>>,
    jws_issuer: Arc<String>,
    jws_audience: Arc<String>,
    default_realm_id: u32,
    command_allowlist: Arc<Vec<String>>,
    sink: Arc<dyn CommandSink>,
    clock: Arc<dyn Clock>,
}

impl AppState {
    pub fn new(
        jws_secret: String,
        jws_issuer: String,
        jws_audience: String,
        default_realm_id: u32,
        command_allowlist: Vec<String>,
        sink: Arc<dyn CommandSink>,
        clock: Arc<dyn Clock>,
    ) -> Self {
        Self {
            jws_secret: Arc::new(jws_secret.into_bytes()),
            jws_issuer: Arc::new(jws_issuer),
            jws_audience: Arc::new(jws_audience),
            default_realm_id,
            command_allowlist: Arc::new(command_allowlist),
            sink,
            clock,
        }
    }
}

pub trait Clock: Send + Sync {
    fn now_epoch_seconds(&self) -> u64;
}

pub struct SystemClock;

impl Clock for SystemClock {
    fn now_epoch_seconds(&self) -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QueueCommand {
    pub realm_id: u32,
    pub command: String,
    pub run_at_unix: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct QueueReceipt {
    pub id: u64,
    pub realm_id: u32,
    pub command: String,
    pub run_at_unix: u64,
}

pub trait CommandSink: Send + Sync {
    fn healthcheck(&self) -> Result<(), AppError>;
    fn enqueue(&self, request: QueueCommand) -> Result<QueueReceipt, AppError>;
}

#[derive(Clone)]
pub struct MariadbCliSink {
    db_host: String,
    db_port: u16,
    db_user: String,
    db_password: String,
    logon_db: String,
}

impl MariadbCliSink {
    pub fn from_config(config: &Config) -> Self {
        Self {
            db_host: config.db_host.clone(),
            db_port: config.db_port,
            db_user: config.db_user.clone(),
            db_password: config.db_password.clone(),
            logon_db: config.logon_db.clone(),
        }
    }

    fn run_sql(&self, sql: &str) -> Result<String, AppError> {
        let output = Command::new("mariadb")
            .arg("--host")
            .arg(&self.db_host)
            .arg("--port")
            .arg(self.db_port.to_string())
            .arg("--user")
            .arg(&self.db_user)
            .arg("--database")
            .arg(&self.logon_db)
            .arg("--batch")
            .arg("--raw")
            .arg("--skip-column-names")
            .arg("--execute")
            .arg(sql)
            .env("MYSQL_PWD", &self.db_password)
            .output()
            .map_err(|error| AppError::Dependency(format!("failed to spawn mariadb: {error}")))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
            return Err(AppError::Upstream(if stderr.is_empty() {
                format!("mariadb exited with {}", output.status)
            } else {
                stderr
            }));
        }

        Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
    }
}

impl CommandSink for MariadbCliSink {
    fn healthcheck(&self) -> Result<(), AppError> {
        self.run_sql("SELECT 1;").map(|_| ())
    }

    fn enqueue(&self, request: QueueCommand) -> Result<QueueReceipt, AppError> {
        let escaped_command = escape_sql_literal(&request.command)?;
        let sql = format!(
            "INSERT INTO pending_commands (realm_id, run_at_time, command) VALUES ({}, {}, '{}'); SELECT LAST_INSERT_ID();",
            request.realm_id, request.run_at_unix, escaped_command
        );
        let output = self.run_sql(&sql)?;
        let id = output
            .lines()
            .last()
            .unwrap_or_default()
            .trim()
            .parse::<u64>()
            .map_err(|_| AppError::Upstream(format!("unexpected LAST_INSERT_ID output: {output}")))?;

        Ok(QueueReceipt {
            id,
            realm_id: request.realm_id,
            command: request.command,
            run_at_unix: request.run_at_unix,
        })
    }
}

#[derive(Clone)]
pub struct DirectWorldHttpSink {
    world_base_url: String,
    world_api_key: String,
    host: String,
    port: u16,
    base_path: String,
    timeout: Duration,
}

impl DirectWorldHttpSink {
    pub fn from_config(config: &Config) -> Result<Self, AppError> {
        let world_base_url = config
            .world_base_url
            .clone()
            .ok_or_else(|| AppError::Dependency("GM_TOOL_WORLD_BASE_URL is required in direct_world_http mode".to_string()))?;
        let world_api_key = config
            .world_api_key
            .clone()
            .ok_or_else(|| AppError::Dependency("GM_TOOL_WORLD_API_KEY is required in direct_world_http mode".to_string()))?;

        let uri: Uri = world_base_url
            .parse()
            .map_err(|error| AppError::Dependency(format!("invalid GM_TOOL_WORLD_BASE_URL: {error}")))?;

        if uri.scheme_str() != Some("http") {
            return Err(AppError::Dependency(
                "GM_TOOL_WORLD_BASE_URL must use http scheme".to_string(),
            ));
        }

        let host = uri
            .host()
            .ok_or_else(|| AppError::Dependency("GM_TOOL_WORLD_BASE_URL must include a host".to_string()))?
            .to_string();
        let port = uri.port_u16().unwrap_or(80);
        let path = uri.path();
        let base_path = if path.is_empty() { "/".to_string() } else { path.to_string() };

        Ok(Self {
            world_base_url,
            world_api_key,
            host,
            port,
            base_path,
            timeout: Duration::from_secs(config.world_timeout_seconds.max(1)),
        })
    }

    fn endpoint_path(&self, suffix: &str) -> String {
        if self.base_path == "/" {
            suffix.to_string()
        } else {
            format!("{}{}", self.base_path.trim_end_matches('/'), suffix)
        }
    }

    fn perform_request(&self, method: &str, path: &str, body: Option<&str>) -> Result<(u16, String), AppError> {
        let mut stream = TcpStream::connect((self.host.as_str(), self.port))
            .map_err(|error| AppError::Dependency(format!("failed to connect to {}: {}",
                self.world_base_url, error)))?;
        stream
            .set_read_timeout(Some(self.timeout))
            .map_err(|error| AppError::Dependency(format!("failed to set read timeout: {error}")))?;
        stream
            .set_write_timeout(Some(self.timeout))
            .map_err(|error| AppError::Dependency(format!("failed to set write timeout: {error}")))?;

        let body = body.unwrap_or("");
        let request = if body.is_empty() {
            format!(
                "{method} {path} HTTP/1.1\r\nHost: {}\r\nX-API-Key: {}\r\nConnection: close\r\n\r\n",
                self.host, self.world_api_key
            )
        } else {
            format!(
                "{method} {path} HTTP/1.1\r\nHost: {}\r\nX-API-Key: {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                self.host, self.world_api_key, body.len(), body
            )
        };

        stream
            .write_all(request.as_bytes())
            .map_err(|error| AppError::Upstream(format!("failed to write world API request: {error}")))?;
        stream
            .flush()
            .map_err(|error| AppError::Upstream(format!("failed to flush world API request: {error}")))?;

        let mut response = String::new();
        stream
            .read_to_string(&mut response)
            .map_err(|error| AppError::Upstream(format!("failed to read world API response: {error}")))?;

        let mut sections = response.splitn(2, "\r\n\r\n");
        let header_block = sections.next().unwrap_or_default();
        let response_body = sections.next().unwrap_or_default().to_string();
        let status_line = header_block.lines().next().unwrap_or_default();
        let status_code = status_line
            .split_whitespace()
            .nth(1)
            .unwrap_or_default()
            .parse::<u16>()
            .map_err(|_| AppError::Upstream(format!("unexpected world API status line: {status_line}")))?;

        Ok((status_code, response_body))
    }
}

impl CommandSink for DirectWorldHttpSink {
    fn healthcheck(&self) -> Result<(), AppError> {
        let path = self.endpoint_path("/healthz");
        let (status_code, response_body) = self.perform_request("GET", &path, None)?;
        if status_code == 200 {
            return Ok(());
        }

        Err(AppError::Upstream(format!(
            "world API healthcheck returned {status_code}: {}",
            response_body.trim()
        )))
    }

    fn enqueue(&self, request: QueueCommand) -> Result<QueueReceipt, AppError> {
        let path = self.endpoint_path("/admin/gm/commands");
        let body = json!({ "command": request.command }).to_string();
        let (status_code, response_body) = self.perform_request("POST", &path, Some(&body))?;
        if status_code == 200 || status_code == 201 || status_code == 202 {
            return Ok(QueueReceipt {
                id: 0,
                realm_id: request.realm_id,
                command: request.command,
                run_at_unix: request.run_at_unix,
            });
        }

        Err(AppError::Upstream(format!(
            "world API command call returned {status_code}: {}",
            response_body.trim()
        )))
    }
}

#[derive(Debug, Error)]
pub enum AppError {
    #[error("unauthorized")]
    Unauthorized,
    #[error("{0}")]
    Forbidden(String),
    #[error("{0}")]
    BadRequest(String),
    #[error("{0}")]
    Upstream(String),
    #[error("{0}")]
    Dependency(String),
    #[error("{0}")]
    Internal(String),
}

impl AppError {
    fn status_code(&self) -> StatusCode {
        match self {
            Self::Unauthorized => StatusCode::UNAUTHORIZED,
            Self::Forbidden(_) => StatusCode::FORBIDDEN,
            Self::BadRequest(_) => StatusCode::BAD_REQUEST,
            Self::Upstream(_) | Self::Dependency(_) => StatusCode::BAD_GATEWAY,
            Self::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    fn response(&self) -> Response<ResponseBody> {
        json_response(
            self.status_code(),
            json!({
                "ok": false,
                "error": self.to_string(),
            }),
        )
    }
}

#[derive(Debug, Deserialize)]
struct RawCommandRequest {
    command: String,
    #[serde(default)]
    realm_id: Option<u32>,
    #[serde(default)]
    run_after_seconds: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct ReviveRequest {
    character: String,
    #[serde(default)]
    realm_id: Option<u32>,
    #[serde(default)]
    run_after_seconds: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct TeleportRequest {
    character: String,
    teleport: String,
    #[serde(default)]
    realm_id: Option<u32>,
    #[serde(default)]
    run_after_seconds: Option<u64>,
}

#[derive(Debug, Serialize)]
struct HealthPayload<'a> {
    status: &'a str,
}

#[derive(Debug, Deserialize)]
struct JwsHeader {
    alg: String,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum JwsAudience {
    One(String),
    Many(Vec<String>),
}

impl JwsAudience {
    fn contains(&self, expected: &str) -> bool {
        match self {
            Self::One(value) => value == expected,
            Self::Many(values) => values.iter().any(|value| value == expected),
        }
    }
}

#[derive(Debug, Deserialize)]
struct JwsClaims {
    iss: String,
    aud: JwsAudience,
    exp: u64,
    #[serde(default)]
    nbf: Option<u64>,
}

pub async fn handle_request(
    state: Arc<AppState>,
    request: Request<Incoming>,
) -> Result<Response<ResponseBody>, Infallible> {
    let method = request.method().clone();
    let path = request.uri().path().to_string();
    let headers = request.headers().clone();
    let body = match request.into_body().collect().await {
        Ok(collected) => collected.to_bytes(),
        Err(error) => {
            return Ok(AppError::BadRequest(format!("failed to read request body: {error}")).response())
        }
    };

    let response = match route_request(state, method, &path, &headers, body).await {
        Ok(response) => response,
        Err(AppError::BadRequest(message)) if message.starts_with("unsupported route:") => json_response(
            StatusCode::NOT_FOUND,
            json!({
                "ok": false,
                "error": message,
            }),
        ),
        Err(error) => error.response(),
    };

    Ok(response)
}

async fn route_request(
    state: Arc<AppState>,
    method: Method,
    path: &str,
    headers: &HeaderMap,
    body: Bytes,
) -> Result<Response<ResponseBody>, AppError> {
    match (method, path) {
        (Method::GET, "/healthz") => Ok(json_response(StatusCode::OK, json!(HealthPayload { status: "ok" }))),
        (Method::GET, "/readyz") => readyz(state).await,
        (Method::POST, "/api/v1/gm/commands") => {
            authorize(
                headers,
                state.jws_secret.as_slice(),
                state.jws_issuer.as_str(),
                state.jws_audience.as_str(),
                state.clock.now_epoch_seconds(),
            )?;
            let payload: RawCommandRequest = parse_json(&body)?;
            enqueue_from_raw(state, payload).await
        }
        (Method::POST, "/api/v1/gm/revive") => {
            authorize(
                headers,
                state.jws_secret.as_slice(),
                state.jws_issuer.as_str(),
                state.jws_audience.as_str(),
                state.clock.now_epoch_seconds(),
            )?;
            let payload: ReviveRequest = parse_json(&body)?;
            enqueue_structured(
                state,
                payload.realm_id,
                payload.run_after_seconds,
                build_revive_command(&payload.character)?,
            )
            .await
        }
        (Method::POST, "/api/v1/gm/teleport") => {
            authorize(
                headers,
                state.jws_secret.as_slice(),
                state.jws_issuer.as_str(),
                state.jws_audience.as_str(),
                state.clock.now_epoch_seconds(),
            )?;
            let payload: TeleportRequest = parse_json(&body)?;
            enqueue_structured(
                state,
                payload.realm_id,
                payload.run_after_seconds,
                build_teleport_command(&payload.character, &payload.teleport)?,
            )
            .await
        }
        _ => Err(AppError::BadRequest(format!("unsupported route: {path}"))),
    }
}

async fn readyz(state: Arc<AppState>) -> Result<Response<ResponseBody>, AppError> {
    let sink = state.sink.clone();
    tokio::task::spawn_blocking(move || sink.healthcheck())
        .await
        .map_err(|error| AppError::Internal(format!("health worker failed: {error}")))??;

    Ok(json_response(
        StatusCode::OK,
        json!(HealthPayload { status: "ready" }),
    ))
}

async fn enqueue_from_raw(
    state: Arc<AppState>,
    payload: RawCommandRequest,
) -> Result<Response<ResponseBody>, AppError> {
    let command = normalize_raw_command(&payload.command)?;
    authorize_raw_command(state.command_allowlist.as_slice(), &command)?;
    enqueue_structured(state, payload.realm_id, payload.run_after_seconds, command).await
}

async fn enqueue_structured(
    state: Arc<AppState>,
    realm_id: Option<u32>,
    run_after_seconds: Option<u64>,
    command: String,
) -> Result<Response<ResponseBody>, AppError> {
    let request = QueueCommand {
        realm_id: realm_id.unwrap_or(state.default_realm_id),
        command,
        run_at_unix: state.clock.now_epoch_seconds() + run_after_seconds.unwrap_or(0),
    };
    let sink = state.sink.clone();
    let request_clone = request.clone();
    let receipt = tokio::task::spawn_blocking(move || sink.enqueue(request_clone))
        .await
        .map_err(|error| AppError::Internal(format!("queue worker failed: {error}")))??;

    Ok(json_response(
        StatusCode::CREATED,
        json!({
            "ok": true,
            "queued": receipt,
        }),
    ))
}

fn authorize(
    headers: &HeaderMap,
    secret: &[u8],
    expected_issuer: &str,
    expected_audience: &str,
    now_epoch_seconds: u64,
) -> Result<(), AppError> {
    let token = headers
        .get(AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or(AppError::Unauthorized)?;

    verify_jws(
        token,
        secret,
        expected_issuer,
        expected_audience,
        now_epoch_seconds,
    )
}

fn verify_jws(
    token: &str,
    secret: &[u8],
    expected_issuer: &str,
    expected_audience: &str,
    now_epoch_seconds: u64,
) -> Result<(), AppError> {
    let mut parts = token.split('.');
    let header_b64 = parts.next().ok_or(AppError::Unauthorized)?;
    let payload_b64 = parts.next().ok_or(AppError::Unauthorized)?;
    let signature_b64 = parts.next().ok_or(AppError::Unauthorized)?;
    if parts.next().is_some() {
        return Err(AppError::Unauthorized);
    }

    let signing_input = format!("{header_b64}.{payload_b64}");
    let header_bytes = URL_SAFE_NO_PAD.decode(header_b64).map_err(|_| AppError::Unauthorized)?;
    let payload_bytes = URL_SAFE_NO_PAD.decode(payload_b64).map_err(|_| AppError::Unauthorized)?;
    let signature = URL_SAFE_NO_PAD
        .decode(signature_b64)
        .map_err(|_| AppError::Unauthorized)?;

    let header: JwsHeader = serde_json::from_slice(&header_bytes).map_err(|_| AppError::Unauthorized)?;
    if header.alg != "HS256" {
        return Err(AppError::Unauthorized);
    }

    let mut mac = HmacSha256::new_from_slice(secret)
        .map_err(|error| AppError::Internal(format!("invalid JWS secret: {error}")))?;
    mac.update(signing_input.as_bytes());
    mac.verify_slice(&signature).map_err(|_| AppError::Unauthorized)?;

    let claims: JwsClaims = serde_json::from_slice(&payload_bytes).map_err(|_| AppError::Unauthorized)?;
    if claims.iss != expected_issuer {
        return Err(AppError::Unauthorized);
    }
    if !claims.aud.contains(expected_audience) {
        return Err(AppError::Unauthorized);
    }
    if claims.exp <= now_epoch_seconds {
        return Err(AppError::Unauthorized);
    }
    if let Some(nbf) = claims.nbf {
        if now_epoch_seconds < nbf {
            return Err(AppError::Unauthorized);
        }
    }

    Ok(())
}

fn parse_json<T>(body: &[u8]) -> Result<T, AppError>
where
    T: for<'de> Deserialize<'de>,
{
    serde_json::from_slice(body).map_err(|error| AppError::BadRequest(format!("invalid json: {error}")))
}

fn normalize_raw_command(command: &str) -> Result<String, AppError> {
    let normalized = command.trim().trim_start_matches('.').trim().to_string();
    validate_command(&normalized)?;
    Ok(normalized)
}

fn build_revive_command(character: &str) -> Result<String, AppError> {
    let character = validate_single_token("character", character)?;
    Ok(format!("revive {character}"))
}

fn build_teleport_command(character: &str, teleport: &str) -> Result<String, AppError> {
    let character = validate_single_token("character", character)?;
    let teleport = validate_single_token("teleport", teleport)?;
    Ok(format!("tele name {character} {teleport}"))
}

fn authorize_raw_command(allowlist: &[String], command: &str) -> Result<(), AppError> {
    if allowlist.is_empty() {
        return Err(AppError::Forbidden(
            "raw gm commands are disabled because GM_TOOL_COMMAND_ALLOWLIST is empty".to_string(),
        ));
    }

    if allowlist
        .iter()
        .any(|entry| command_matches_allowlist_entry(command, entry))
    {
        return Ok(());
    }

    Err(AppError::Forbidden(format!(
        "raw gm command is not allowlisted: {command}"
    )))
}

fn command_matches_allowlist_entry(command: &str, entry: &str) -> bool {
    command == entry
        || command
            .strip_prefix(entry)
            .is_some_and(|suffix| suffix.starts_with(char::is_whitespace))
}

fn validate_single_token(field: &str, value: &str) -> Result<String, AppError> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(AppError::BadRequest(format!("{field} must not be empty")));
    }
    if trimmed.contains(char::is_whitespace) {
        return Err(AppError::BadRequest(format!("{field} must be a single token")));
    }
    if trimmed.contains('\'') || trimmed.contains('"') || trimmed.contains('\n') || trimmed.contains('\r') {
        return Err(AppError::BadRequest(format!("{field} contains unsupported characters")));
    }
    Ok(trimmed.to_string())
}

fn validate_command(command: &str) -> Result<(), AppError> {
    if command.is_empty() {
        return Err(AppError::BadRequest("command must not be empty".to_string()));
    }
    if command.len() > MAX_COMMAND_LEN {
        return Err(AppError::BadRequest(format!(
            "command exceeds max length {MAX_COMMAND_LEN}"
        )));
    }
    if command.contains('\0') || command.contains('\n') || command.contains('\r') {
        return Err(AppError::BadRequest(
            "command must be a single line without NUL bytes".to_string(),
        ));
    }
    Ok(())
}

fn normalize_allowlist_entry(value: &str) -> String {
    value.trim().trim_start_matches('.').trim().to_string()
}

fn escape_sql_literal(value: &str) -> Result<String, AppError> {
    if value.contains('\0') {
        return Err(AppError::BadRequest(
            "command must not contain NUL bytes".to_string(),
        ));
    }
    Ok(value.replace('\'', "''"))
}

fn json_response(status: StatusCode, payload: serde_json::Value) -> Response<ResponseBody> {
    Response::builder()
        .status(status)
        .header(CONTENT_TYPE, "application/json")
        .body(Full::new(Bytes::from(payload.to_string())))
        .expect("json response is valid")
}

#[cfg(test)]
#[derive(Clone)]
struct FixedClock(u64);

#[cfg(test)]
impl Clock for FixedClock {
    fn now_epoch_seconds(&self) -> u64 {
        self.0
    }
}

#[cfg(test)]
#[derive(Default)]
struct RecordingSink {
    queued: Mutex<Vec<QueueCommand>>,
    ready: Mutex<bool>,
}

#[cfg(test)]
impl RecordingSink {
    fn with_ready(ready: bool) -> Self {
        Self {
            queued: Mutex::new(Vec::new()),
            ready: Mutex::new(ready),
        }
    }
}

#[cfg(test)]
impl CommandSink for RecordingSink {
    fn healthcheck(&self) -> Result<(), AppError> {
        if *self.ready.lock().expect("ready lock") {
            Ok(())
        } else {
            Err(AppError::Upstream("db not ready".to_string()))
        }
    }

    fn enqueue(&self, request: QueueCommand) -> Result<QueueReceipt, AppError> {
        self.queued.lock().expect("queued lock").push(request.clone());
        Ok(QueueReceipt {
            id: 42,
            realm_id: request.realm_id,
            command: request.command,
            run_at_unix: request.run_at_unix,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const DEFAULT_REALM_ID: u32 = 1;
    const TEST_SECRET: &str = "secret";
    const TEST_ISSUER: &str = "twow-control-plane";
    const TEST_AUDIENCE: &str = "twow-gm-tool";

    fn build_state() -> Arc<AppState> {
        Arc::new(AppState::new(
            TEST_SECRET.to_string(),
            TEST_ISSUER.to_string(),
            TEST_AUDIENCE.to_string(),
            DEFAULT_REALM_ID,
            Vec::new(),
            Arc::new(RecordingSink::with_ready(true)),
            Arc::new(FixedClock(1_717_171_717)),
        ))
    }

    fn sign_test_jws(secret: &str, claims: serde_json::Value) -> String {
        let header = URL_SAFE_NO_PAD.encode(br#"{"alg":"HS256","typ":"JWT"}"#);
        let payload = URL_SAFE_NO_PAD.encode(claims.to_string().as_bytes());
        let signing_input = format!("{header}.{payload}");
        let mut mac = HmacSha256::new_from_slice(secret.as_bytes()).expect("test secret");
        mac.update(signing_input.as_bytes());
        let signature = URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes());
        format!("{signing_input}.{signature}")
    }

    async fn call(
        state: Arc<AppState>,
        method: Method,
        path: &str,
        body: serde_json::Value,
        token: Option<&str>,
    ) -> Response<ResponseBody> {
        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, "application/json".parse().expect("content-type header"));
        if let Some(value) = token {
            headers.insert(
                AUTHORIZATION,
                format!("Bearer {value}")
                    .parse()
                    .expect("authorization header"),
            );
        }

        route_request(state, method, path, &headers, Bytes::from(body.to_string()))
            .await
            .unwrap_or_else(|error| error.response())
    }

    #[tokio::test]
    async fn revive_endpoint_queues_expected_command() {
        let sink = Arc::new(RecordingSink::with_ready(true));
        let state = Arc::new(AppState::new(
            TEST_SECRET.to_string(),
            TEST_ISSUER.to_string(),
            TEST_AUDIENCE.to_string(),
            DEFAULT_REALM_ID,
            Vec::new(),
            sink.clone(),
            Arc::new(FixedClock(100)),
        ));
        let token = sign_test_jws(
            TEST_SECRET,
            json!({"sub":"test","iss":TEST_ISSUER,"aud":TEST_AUDIENCE,"exp":101}),
        );

        let response = call(
            state,
            Method::POST,
            "/api/v1/gm/revive",
            json!({"character":"Qianfuren","realm_id":7}),
            Some(&token),
        )
        .await;

        assert_eq!(response.status(), StatusCode::CREATED);
        let queued = sink.queued.lock().expect("queued lock");
        assert_eq!(
            queued.as_slice(),
            &[QueueCommand {
                realm_id: 7,
                command: "revive Qianfuren".to_string(),
                run_at_unix: 100,
            }]
        );
    }

    #[tokio::test]
    async fn teleport_endpoint_rejects_whitespace_aliases() {
        let response = call(
            build_state(),
            Method::POST,
            "/api/v1/gm/teleport",
            json!({"character":"Qianfuren","teleport":"gm island"}),
            Some(&sign_test_jws(
                TEST_SECRET,
                json!({"sub":"test","iss":TEST_ISSUER,"aud":TEST_AUDIENCE,"exp":1_717_171_718u64}),
            )),
        )
        .await;

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn raw_endpoint_strips_leading_dot() {
        let sink = Arc::new(RecordingSink::with_ready(true));
        let state = Arc::new(AppState::new(
            TEST_SECRET.to_string(),
            TEST_ISSUER.to_string(),
            TEST_AUDIENCE.to_string(),
            DEFAULT_REALM_ID,
            vec!["broadcast".to_string()],
            sink.clone(),
            Arc::new(FixedClock(200)),
        ));
        let token = sign_test_jws(
            TEST_SECRET,
            json!({"sub":"test","iss":TEST_ISSUER,"aud":TEST_AUDIENCE,"exp":205}),
        );

        let response = call(
            state,
            Method::POST,
            "/api/v1/gm/commands",
            json!({"command":".broadcast hello","run_after_seconds":5}),
            Some(&token),
        )
        .await;

        assert_eq!(response.status(), StatusCode::CREATED);
        let queued = sink.queued.lock().expect("queued lock");
        assert_eq!(queued[0].command, "broadcast hello");
        assert_eq!(queued[0].run_at_unix, 205);
    }

    #[tokio::test]
    async fn raw_endpoint_rejects_non_allowlisted_commands() {
        let sink = Arc::new(RecordingSink::with_ready(true));
        let state = Arc::new(AppState::new(
            TEST_SECRET.to_string(),
            TEST_ISSUER.to_string(),
            TEST_AUDIENCE.to_string(),
            DEFAULT_REALM_ID,
            vec!["broadcast".to_string()],
            sink.clone(),
            Arc::new(FixedClock(200)),
        ));
        let token = sign_test_jws(
            TEST_SECRET,
            json!({"sub":"test","iss":TEST_ISSUER,"aud":TEST_AUDIENCE,"exp":205}),
        );

        let response = call(
            state,
            Method::POST,
            "/api/v1/gm/commands",
            json!({"command":"saveall"}),
            Some(&token),
        )
        .await;

        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        assert!(sink.queued.lock().expect("queued lock").is_empty());
    }

    #[tokio::test]
    async fn raw_endpoint_allows_prefix_matches() {
        let sink = Arc::new(RecordingSink::with_ready(true));
        let state = Arc::new(AppState::new(
            TEST_SECRET.to_string(),
            TEST_ISSUER.to_string(),
            TEST_AUDIENCE.to_string(),
            DEFAULT_REALM_ID,
            vec!["tele name".to_string()],
            sink.clone(),
            Arc::new(FixedClock(250)),
        ));
        let token = sign_test_jws(
            TEST_SECRET,
            json!({"sub":"test","iss":TEST_ISSUER,"aud":TEST_AUDIENCE,"exp":255}),
        );

        let response = call(
            state,
            Method::POST,
            "/api/v1/gm/commands",
            json!({"command":"tele name Qianfuren Goldshire"}),
            Some(&token),
        )
        .await;

        assert_eq!(response.status(), StatusCode::CREATED);
        let queued = sink.queued.lock().expect("queued lock");
        assert_eq!(queued[0].command, "tele name Qianfuren Goldshire");
    }

    #[tokio::test]
    async fn write_endpoints_require_valid_jws() {
        let response = call(
            build_state(),
            Method::POST,
            "/api/v1/gm/revive",
            json!({"character":"Qianfuren"}),
            None,
        )
        .await;

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn expired_jws_is_rejected() {
        let response = call(
            build_state(),
            Method::POST,
            "/api/v1/gm/revive",
            json!({"character":"Qianfuren"}),
            Some(&sign_test_jws(
                TEST_SECRET,
                json!({"sub":"test","iss":TEST_ISSUER,"aud":TEST_AUDIENCE,"exp":1_717_171_716u64}),
            )),
        )
        .await;

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn invalid_jws_signature_is_rejected() {
        let response = call(
            build_state(),
            Method::POST,
            "/api/v1/gm/revive",
            json!({"character":"Qianfuren"}),
            Some(&sign_test_jws(
                "wrong-secret",
                json!({"sub":"test","iss":TEST_ISSUER,"aud":TEST_AUDIENCE,"exp":1_717_171_718u64}),
            )),
        )
        .await;

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn readyz_reflects_sink_health() {
        let state = Arc::new(AppState::new(
            TEST_SECRET.to_string(),
            TEST_ISSUER.to_string(),
            TEST_AUDIENCE.to_string(),
            DEFAULT_REALM_ID,
            Vec::new(),
            Arc::new(RecordingSink::with_ready(false)),
            Arc::new(FixedClock(0)),
        ));

        let response = route_request(state, Method::GET, "/readyz", &HeaderMap::new(), Bytes::new())
            .await
            .unwrap_err()
            .response();
        assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
    }

    #[tokio::test]
    async fn wrong_issuer_is_rejected() {
        let response = call(
            build_state(),
            Method::POST,
            "/api/v1/gm/revive",
            json!({"character":"Qianfuren"}),
            Some(&sign_test_jws(
                TEST_SECRET,
                json!({"sub":"test","iss":"wrong-issuer","aud":TEST_AUDIENCE,"exp":1_717_171_718u64}),
            )),
        )
        .await;

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn wrong_audience_is_rejected() {
        let response = call(
            build_state(),
            Method::POST,
            "/api/v1/gm/revive",
            json!({"character":"Qianfuren"}),
            Some(&sign_test_jws(
                TEST_SECRET,
                json!({"sub":"test","iss":TEST_ISSUER,"aud":"wrong-audience","exp":1_717_171_718u64}),
            )),
        )
        .await;

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn audience_array_is_supported() {
        let sink = Arc::new(RecordingSink::with_ready(true));
        let state = Arc::new(AppState::new(
            TEST_SECRET.to_string(),
            TEST_ISSUER.to_string(),
            TEST_AUDIENCE.to_string(),
            DEFAULT_REALM_ID,
            Vec::new(),
            sink.clone(),
            Arc::new(FixedClock(300)),
        ));
        let token = sign_test_jws(
            TEST_SECRET,
            json!({"sub":"test","iss":TEST_ISSUER,"aud":["other", TEST_AUDIENCE],"exp":301}),
        );

        let response = call(
            state,
            Method::POST,
            "/api/v1/gm/revive",
            json!({"character":"Qianfuren"}),
            Some(&token),
        )
        .await;

        assert_eq!(response.status(), StatusCode::CREATED);
    }

    #[test]
    fn direct_world_http_sink_posts_to_world_api() {
        use std::net::TcpListener;
        use std::sync::{Arc, Mutex};
        use std::thread;

        let listener = TcpListener::bind("127.0.0.1:0").expect("bind listener");
        let addr = listener.local_addr().expect("listener addr");
        let captured = Arc::new(Mutex::new(String::new()));
        let captured_clone = captured.clone();

        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("accept");
            stream.set_read_timeout(Some(Duration::from_millis(200))).expect("read timeout");
            let mut request = String::new();
            let mut buf = [0u8; 4096];
            loop {
                match stream.read(&mut buf) {
                    Ok(0) => break,
                    Ok(read) => request.push_str(&String::from_utf8_lossy(&buf[..read])),
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock || error.kind() == std::io::ErrorKind::TimedOut => break,
                    Err(error) => panic!("read request: {error}"),
                }
            }
            *captured_clone.lock().expect("captured lock") = request;
            let response = "HTTP/1.1 202 Accepted\r\nContent-Type: application/json\r\nContent-Length: 11\r\nConnection: close\r\n\r\n{\"ok\":true}";
            stream.write_all(response.as_bytes()).expect("write response");
            stream.flush().expect("flush response");
        });

        let config = Config {
            bind_addr: "127.0.0.1:8080".parse().expect("bind addr"),
            sink_mode: SinkMode::DirectWorldHttp,
            command_allowlist: vec!["broadcast".to_string()],
            jws_secret: TEST_SECRET.to_string(),
            jws_issuer: TEST_ISSUER.to_string(),
            jws_audience: TEST_AUDIENCE.to_string(),
            db_host: "ignored".to_string(),
            db_port: 3306,
            db_user: "ignored".to_string(),
            db_password: "ignored".to_string(),
            logon_db: "ignored".to_string(),
            default_realm_id: DEFAULT_REALM_ID,
            world_base_url: Some(format!("http://127.0.0.1:{}", addr.port())),
            world_api_key: Some("Gheor".to_string()),
            world_timeout_seconds: 5,
        };
        let sink = DirectWorldHttpSink::from_config(&config).expect("direct sink");

        let receipt = sink
            .enqueue(QueueCommand {
                realm_id: 1,
                command: "broadcast hello".to_string(),
                run_at_unix: 123,
            })
            .expect("enqueue");

        server.join().expect("server join");
        let request = captured.lock().expect("captured lock").clone();

        assert_eq!(receipt.id, 0);
        assert!(request.contains("POST /admin/gm/commands HTTP/1.1"));
        assert!(request.contains("X-API-Key: Gheor"));
        assert!(request.contains("{\"command\":\"broadcast hello\"}"));
    }

    #[test]
    fn direct_world_http_sink_healthcheck_uses_healthz() {
        use std::net::TcpListener;
        use std::sync::{Arc, Mutex};
        use std::thread;

        let listener = TcpListener::bind("127.0.0.1:0").expect("bind listener");
        let addr = listener.local_addr().expect("listener addr");
        let captured = Arc::new(Mutex::new(String::new()));
        let captured_clone = captured.clone();

        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("accept");
            stream.set_read_timeout(Some(Duration::from_millis(200))).expect("read timeout");
            let mut request = String::new();
            let mut buf = [0u8; 4096];
            loop {
                match stream.read(&mut buf) {
                    Ok(0) => break,
                    Ok(read) => request.push_str(&String::from_utf8_lossy(&buf[..read])),
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock || error.kind() == std::io::ErrorKind::TimedOut => break,
                    Err(error) => panic!("read request: {error}"),
                }
            }
            *captured_clone.lock().expect("captured lock") = request;
            let response = "HTTP/1.1 200 OK\r\nContent-Length: 3\r\nConnection: close\r\n\r\nok\n";
            stream.write_all(response.as_bytes()).expect("write response");
            stream.flush().expect("flush response");
        });

        let config = Config {
            bind_addr: "127.0.0.1:8080".parse().expect("bind addr"),
            sink_mode: SinkMode::DirectWorldHttp,
            command_allowlist: vec!["broadcast".to_string()],
            jws_secret: TEST_SECRET.to_string(),
            jws_issuer: TEST_ISSUER.to_string(),
            jws_audience: TEST_AUDIENCE.to_string(),
            db_host: "ignored".to_string(),
            db_port: 3306,
            db_user: "ignored".to_string(),
            db_password: "ignored".to_string(),
            logon_db: "ignored".to_string(),
            default_realm_id: DEFAULT_REALM_ID,
            world_base_url: Some(format!("http://127.0.0.1:{}", addr.port())),
            world_api_key: Some("Gheor".to_string()),
            world_timeout_seconds: 5,
        };
        let sink = DirectWorldHttpSink::from_config(&config).expect("direct sink");

        sink.healthcheck().expect("healthcheck");

        server.join().expect("server join");
        let request = captured.lock().expect("captured lock").clone();
        assert!(request.contains("GET /healthz HTTP/1.1"));
    }
}
