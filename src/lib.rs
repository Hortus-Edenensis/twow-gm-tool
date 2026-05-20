use std::convert::Infallible;
use std::net::SocketAddr;
use std::process::Command;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use http::header::{CONTENT_TYPE, HeaderMap};
use http::{Method, Request, Response, StatusCode};
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use serde::{Deserialize, Serialize};
use serde_json::json;
use thiserror::Error;

#[cfg(test)]
use std::sync::Mutex;

const DEFAULT_BIND_ADDR: &str = "0.0.0.0:8080";
const DEFAULT_REALM_ID_STR: &str = "1";
const MAX_COMMAND_LEN: usize = 512;

pub type ResponseBody = Full<Bytes>;

#[derive(Debug, Clone)]
pub struct Config {
    pub bind_addr: SocketAddr,
    pub api_key: String,
    pub db_host: String,
    pub db_port: u16,
    pub db_user: String,
    pub db_password: String,
    pub logon_db: String,
    pub default_realm_id: u32,
}

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("missing required environment variable: {0}")]
    MissingEnv(&'static str),
    #[error("invalid socket address in GM_TOOL_BIND_ADDR: {0}")]
    InvalidBindAddr(String),
    #[error("invalid integer in {key}: {value}")]
    InvalidInteger { key: &'static str, value: String },
}

impl Config {
    pub fn from_env() -> Result<Self, ConfigError> {
        let bind_addr = env_or_default("GM_TOOL_BIND_ADDR", DEFAULT_BIND_ADDR);
        let bind_addr = bind_addr
            .parse::<SocketAddr>()
            .map_err(|_| ConfigError::InvalidBindAddr(bind_addr.clone()))?;
        let api_key = required_env("GM_TOOL_API_KEY")?;
        let db_host = required_env("TWOW_DB_HOST")?;
        let db_port = parse_env_u16("TWOW_DB_PORT", "3306")?;
        let db_user = required_env("TWOW_DB_USER")?;
        let db_password = required_env("TWOW_DB_PASSWORD")?;
        let logon_db = required_env("TWOW_LOGON_DB")?;
        let default_realm_id = parse_env_u32("GM_TOOL_DEFAULT_REALM_ID", DEFAULT_REALM_ID_STR)?;

        Ok(Self {
            bind_addr,
            api_key,
            db_host,
            db_port,
            db_user,
            db_password,
            logon_db,
            default_realm_id,
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

#[derive(Clone)]
pub struct AppState {
    api_key: Arc<String>,
    default_realm_id: u32,
    sink: Arc<dyn CommandSink>,
    clock: Arc<dyn Clock>,
}

impl AppState {
    pub fn new(
        api_key: String,
        default_realm_id: u32,
        sink: Arc<dyn CommandSink>,
        clock: Arc<dyn Clock>,
    ) -> Self {
        Self {
            api_key: Arc::new(api_key),
            default_realm_id,
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

#[derive(Debug, Error)]
pub enum AppError {
    #[error("unauthorized")]
    Unauthorized,
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
            authorize(headers, &state.api_key)?;
            let payload: RawCommandRequest = parse_json(&body)?;
            enqueue_from_raw(state, payload).await
        }
        (Method::POST, "/api/v1/gm/revive") => {
            authorize(headers, &state.api_key)?;
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
            authorize(headers, &state.api_key)?;
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

fn authorize(headers: &HeaderMap, expected: &str) -> Result<(), AppError> {
    let api_key = headers
        .get("X-API-Key")
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default();

    if api_key == expected {
        Ok(())
    } else {
        Err(AppError::Unauthorized)
    }
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

    fn build_state() -> Arc<AppState> {
        Arc::new(AppState::new(
            "secret".to_string(),
            DEFAULT_REALM_ID,
            Arc::new(RecordingSink::with_ready(true)),
            Arc::new(FixedClock(1_717_171_717)),
        ))
    }

    async fn call(
        state: Arc<AppState>,
        method: Method,
        path: &str,
        body: serde_json::Value,
        api_key: Option<&str>,
    ) -> Response<ResponseBody> {
        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, "application/json".parse().expect("content-type header"));
        if let Some(value) = api_key {
            headers.insert("X-API-Key", value.parse().expect("api key header"));
        }

        route_request(state, method, path, &headers, Bytes::from(body.to_string()))
            .await
            .unwrap_or_else(|error| error.response())
    }

    #[tokio::test]
    async fn revive_endpoint_queues_expected_command() {
        let sink = Arc::new(RecordingSink::with_ready(true));
        let state = Arc::new(AppState::new(
            "secret".to_string(),
            DEFAULT_REALM_ID,
            sink.clone(),
            Arc::new(FixedClock(100)),
        ));

        let response = call(
            state,
            Method::POST,
            "/api/v1/gm/revive",
            json!({"character":"Qianfuren","realm_id":7}),
            Some("secret"),
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
            Some("secret"),
        )
        .await;

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn raw_endpoint_strips_leading_dot() {
        let sink = Arc::new(RecordingSink::with_ready(true));
        let state = Arc::new(AppState::new(
            "secret".to_string(),
            DEFAULT_REALM_ID,
            sink.clone(),
            Arc::new(FixedClock(200)),
        ));

        let response = call(
            state,
            Method::POST,
            "/api/v1/gm/commands",
            json!({"command":".broadcast hello","run_after_seconds":5}),
            Some("secret"),
        )
        .await;

        assert_eq!(response.status(), StatusCode::CREATED);
        let queued = sink.queued.lock().expect("queued lock");
        assert_eq!(queued[0].command, "broadcast hello");
        assert_eq!(queued[0].run_at_unix, 205);
    }

    #[tokio::test]
    async fn write_endpoints_require_api_key() {
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
    async fn readyz_reflects_sink_health() {
        let state = Arc::new(AppState::new(
            "secret".to_string(),
            DEFAULT_REALM_ID,
            Arc::new(RecordingSink::with_ready(false)),
            Arc::new(FixedClock(0)),
        ));

        let response = route_request(state, Method::GET, "/readyz", &HeaderMap::new(), Bytes::new())
            .await
            .unwrap_err()
            .response();
        assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
    }
}
