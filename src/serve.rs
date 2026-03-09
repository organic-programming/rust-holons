//! Standard gRPC server runner for Rust holons.

use crate::transport::{self, Listener, StdioTransport, DEFAULT_URI};
use std::convert::Infallible;
use std::error::Error;
#[cfg(unix)]
use std::path::PathBuf;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_stream::wrappers::TcpListenerStream;
#[cfg(unix)]
use tokio_stream::wrappers::UnixListenerStream;
use tonic::body::BoxBody;
use tonic::codegen::http::{Request, Response};
use tonic::server::NamedService;
use tonic::transport::server::Connected;
use tonic::transport::Server;
use tower_service::Service;

pub type Result<T> = std::result::Result<T, Box<dyn Error + Send + Sync>>;

#[derive(Debug, Clone, Copy, Default)]
pub struct RunOptions {
    pub accept_http1: bool,
}

macro_rules! serve_router {
    ($router:expr, $listen_uri:expr, $options:expr) => {{
        let router = $router;
        let options = $options;
        let listen_uri = $listen_uri;

        match transport::listen(listen_uri).await? {
            Listener::Tcp(listener) => {
                let actual_uri = bound_tcp_uri(listen_uri, &listener)?;
                announce_bound_uri(&actual_uri, options);
                router
                    .serve_with_incoming_shutdown(
                        TcpListenerStream::new(listener),
                        shutdown_signal(),
                    )
                    .await?;
            }
            #[cfg(unix)]
            Listener::Unix(listener) => {
                let cleanup = unix_socket_path(listen_uri)?;
                announce_bound_uri(listen_uri, options);
                let result = router
                    .serve_with_incoming_shutdown(
                        UnixListenerStream::new(listener),
                        shutdown_signal(),
                    )
                    .await;
                cleanup_unix_socket(cleanup.as_deref());
                result?;
            }
            Listener::Stdio => {
                announce_bound_uri("stdio://", options);
                router
                    .serve_with_incoming_shutdown(
                        tokio_stream::iter(vec![Ok::<_, std::io::Error>(transport::listen_stdio()?)]),
                        shutdown_signal(),
                    )
                    .await?;
            }
            Listener::Mem(_) => {
                return Err(boxed_err(
                    "serve::run() does not support mem://; use a custom server loop",
                ));
            }
            Listener::Ws(_) => {
                return Err(boxed_err(
                    "serve::run() does not support ws:// or wss://; use a custom server loop",
                ));
            }
        }

        Ok(())
    }};
}

/// Extract --listen or --port from command-line args.
pub fn parse_flags(args: &[String]) -> String {
    let mut i = 0;
    while i < args.len() {
        if args[i] == "--listen" && i + 1 < args.len() {
            return args[i + 1].clone();
        }
        if args[i] == "--port" && i + 1 < args.len() {
            return format!("tcp://:{}", args[i + 1]);
        }
        i += 1;
    }
    DEFAULT_URI.to_string()
}

/// Run a single gRPC service on the requested transport URI.
pub async fn run_single<Svc>(listen_uri: &str, service: Svc) -> Result<()>
where
    Svc: Service<Request<BoxBody>, Response = Response<BoxBody>, Error = Infallible>
        + NamedService
        + Clone
        + Send
        + 'static,
    Svc::Future: Send + 'static,
{
    run_single_with_options(listen_uri, service, RunOptions::default()).await
}

/// Run a single gRPC service with server options.
pub async fn run_single_with_options<Svc>(
    listen_uri: &str,
    service: Svc,
    options: RunOptions,
) -> Result<()>
where
    Svc: Service<Request<BoxBody>, Response = Response<BoxBody>, Error = Infallible>
        + NamedService
        + Clone
        + Send
        + 'static,
    Svc::Future: Send + 'static,
{
    let mut builder = Server::builder().accept_http1(options.accept_http1);
    let router = builder.add_service(service);
    serve_router!(router, listen_uri, options)
}

/// Run a gRPC service with one optional companion service, typically reflection.
pub async fn run<Extra, Svc>(
    listen_uri: &str,
    extra_service: Option<Extra>,
    service: Svc,
) -> Result<()>
where
    Extra: Service<Request<BoxBody>, Response = Response<BoxBody>, Error = Infallible>
        + NamedService
        + Clone
        + Send
        + 'static,
    Extra::Future: Send + 'static,
    Svc: Service<Request<BoxBody>, Response = Response<BoxBody>, Error = Infallible>
        + NamedService
        + Clone
        + Send
        + 'static,
    Svc::Future: Send + 'static,
{
    run_with_options(listen_uri, extra_service, service, RunOptions::default()).await
}

/// Run a gRPC service with one optional companion service and server options.
pub async fn run_with_options<Extra, Svc>(
    listen_uri: &str,
    extra_service: Option<Extra>,
    service: Svc,
    options: RunOptions,
) -> Result<()>
where
    Extra: Service<Request<BoxBody>, Response = Response<BoxBody>, Error = Infallible>
        + NamedService
        + Clone
        + Send
        + 'static,
    Extra::Future: Send + 'static,
    Svc: Service<Request<BoxBody>, Response = Response<BoxBody>, Error = Infallible>
        + NamedService
        + Clone
        + Send
        + 'static,
    Svc::Future: Send + 'static,
{
    let mut builder = Server::builder().accept_http1(options.accept_http1);
    let router = builder.add_optional_service(extra_service).add_service(service);
    serve_router!(router, listen_uri, options)
}

fn announce_bound_uri(uri: &str, options: RunOptions) {
    let mode = if options.accept_http1 {
        "http1 ON"
    } else {
        "http1 OFF"
    };
    eprintln!("gRPC server listening on {uri} ({mode})");
}

fn bound_tcp_uri(listen_uri: &str, listener: &tokio::net::TcpListener) -> Result<String> {
    let parsed = transport::parse_uri(listen_uri).map_err(boxed_err)?;
    let bound = listener.local_addr()?;
    let host = match parsed.host.as_deref() {
        Some("") | Some("0.0.0.0") | Some("::") | None => "127.0.0.1".to_string(),
        Some(host) => host.to_string(),
    };
    Ok(format!("tcp://{host}:{}", bound.port()))
}

#[cfg(unix)]
fn unix_socket_path(listen_uri: &str) -> Result<Option<PathBuf>> {
    let parsed = transport::parse_uri(listen_uri).map_err(boxed_err)?;
    Ok(parsed.path.map(PathBuf::from))
}

#[cfg(unix)]
fn cleanup_unix_socket(path: Option<&std::path::Path>) {
    if let Some(path) = path {
        let _ = std::fs::remove_file(path);
    }
}

fn boxed_err(message: impl Into<String>) -> Box<dyn Error + Send + Sync> {
    Box::new(std::io::Error::new(
        std::io::ErrorKind::Other,
        message.into(),
    ))
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("SIGTERM handler");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = terminate.recv() => {}
        }
    }

    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

impl<R, W> Connected for StdioTransport<R, W>
where
    R: AsyncRead + Unpin + Send + 'static,
    W: AsyncWrite + Unpin + Send + 'static,
{
    type ConnectInfo = ();

    fn connect_info(&self) -> Self::ConnectInfo {}
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_listen() {
        let args: Vec<String> = vec!["--listen".into(), "tcp://:8080".into()];
        assert_eq!(parse_flags(&args), "tcp://:8080");
    }

    #[test]
    fn test_parse_port() {
        let args: Vec<String> = vec!["--port".into(), "3000".into()];
        assert_eq!(parse_flags(&args), "tcp://:3000");
    }

    #[test]
    fn test_parse_default() {
        let args: Vec<String> = vec![];
        assert_eq!(parse_flags(&args), DEFAULT_URI);
    }

    #[tokio::test]
    async fn test_bound_tcp_uri_normalizes_wildcard_host() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let actual = bound_tcp_uri("tcp://:0", &listener).unwrap();
        assert!(actual.starts_with("tcp://127.0.0.1:"));
    }

    #[cfg(unix)]
    #[test]
    fn test_unix_socket_path() {
        let path = unix_socket_path("unix:///tmp/holons.sock").unwrap();
        assert_eq!(path.unwrap(), std::path::PathBuf::from("/tmp/holons.sock"));
    }
}
