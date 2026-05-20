use std::sync::Arc;

use hyper::service::service_fn;
use hyper::server::conn::http1;
use hyper_util::rt::TokioIo;
use tokio::net::TcpListener;
use twow_gm_tool::{handle_request, AppState, Config, MariadbCliSink, SystemClock};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = Config::from_env()?;
    let listener = TcpListener::bind(config.bind_addr).await?;
    let state = Arc::new(AppState::new(
        config.api_key.clone(),
        config.default_realm_id,
        Arc::new(MariadbCliSink::from_config(&config)),
        Arc::new(SystemClock),
    ));

    println!("twow-gm-tool listening on {}", config.bind_addr);

    loop {
        let (stream, _) = listener.accept().await?;
        let state = state.clone();

        tokio::spawn(async move {
            let io = TokioIo::new(stream);
            let service = service_fn(move |req| handle_request(state.clone(), req));
            if let Err(error) = http1::Builder::new().serve_connection(io, service).await {
                eprintln!("connection error: {error}");
            }
        });
    }
}
