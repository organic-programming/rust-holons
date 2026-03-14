//! Standard gRPC server runner for Rust holons.

use crate::describe;
use crate::transport::{self, Listener, StdioTransport, DEFAULT_URI};
use std::env;
use std::convert::Infallible;
use std::error::Error;
#[cfg(unix)]
use std::path::PathBuf;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_stream::wrappers::TcpListenerStream;
#[cfg(unix)]
use tokio_stream::wrappers::UnixListenerStream;
use tokio_stream::Stream;
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
                        StdioIncoming::new(transport::listen_stdio()?),
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
    let meta_service = auto_holon_meta_service()?;
    let router = builder.add_optional_service(meta_service).add_service(service);
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
    let meta_service = auto_holon_meta_service()?;
    let router = builder
        .add_optional_service(meta_service)
        .add_optional_service(extra_service)
        .add_service(service);
    serve_router!(router, listen_uri, options)
}

fn auto_holon_meta_service() -> Result<
    Option<
        crate::gen::holonmeta::v1::holon_meta_server::HolonMetaServer<crate::describe::MetaService>,
    >,
> {
    let root = env::current_dir()?;
    let holon_yaml_path = root.join("holon.yaml");
    if !holon_yaml_path.is_file() {
        return Ok(None);
    }

    let proto_dir = root.join("protos");
    let service = describe::service(proto_dir, holon_yaml_path)
        .map_err(|err| boxed_err(format!("failed to auto-register HolonMeta: {err}")))?;
    Ok(Some(service))
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

/// Hold a single stdio connection open for the lifetime of the server.
///
/// `serve_with_incoming_shutdown` treats the end of the incoming stream as a
/// signal that the server can drain and exit. For stdio we only ever have one
/// transport, so returning `None` immediately after yielding it makes the
/// server quit before the client sends its first RPC.
struct StdioIncoming<R = tokio::io::Stdin, W = tokio::io::Stdout> {
    transport: Option<StdioTransport<R, W>>,
}

impl<R, W> StdioIncoming<R, W> {
    fn new(transport: StdioTransport<R, W>) -> Self {
        Self {
            transport: Some(transport),
        }
    }
}

impl<R, W> Stream for StdioIncoming<R, W>
where
    R: Unpin,
    W: Unpin,
{
    type Item = std::io::Result<StdioTransport<R, W>>;

    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if let Some(transport) = this.transport.take() {
            return Poll::Ready(Some(Ok(transport)));
        }
        Poll::Pending
    }
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
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
    use crate::gen::holonmeta::v1::{holon_meta_client::HolonMetaClient, DescribeRequest};
    use crate::test_support::{acquire_process_guard, ProcessStateGuard};
    use std::future;
    use std::net::TcpListener;
    use std::fs;
    use tempfile::TempDir;
    use tokio::time::{timeout, Duration};
    use tokio_stream::StreamExt;
    use tonic::body::empty_body;

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

    #[tokio::test]
    async fn test_stdio_incoming_yields_once_then_waits() {
        let mut incoming =
            StdioIncoming::new(StdioTransport::new(tokio::io::empty(), tokio::io::sink()));

        assert!(incoming.next().await.unwrap().is_ok());
        assert!(
            timeout(Duration::from_millis(50), incoming.next())
                .await
                .is_err(),
            "stdio incoming should stay pending after the first connection"
        );
    }

    #[tokio::test]
    async fn test_run_single_with_options_auto_registers_holon_meta_from_cwd() {
        let _lock = acquire_process_guard().await;
        let _state = ProcessStateGuard::capture();
        let holon = write_echo_holon();
        env::set_current_dir(holon.path()).unwrap();

        let port = free_port();
        let listen_uri = format!("tcp://127.0.0.1:{port}");

        let server = tokio::spawn(async move {
            run_single_with_options(&listen_uri, UnimplementedService, RunOptions::default())
                .await
                .unwrap();
        });

        let endpoint = format!("http://127.0.0.1:{port}");
        let mut client = wait_for_holon_meta_client(&endpoint).await;
        let response = client
            .describe(DescribeRequest {})
            .await
            .unwrap()
            .into_inner();

        assert_eq!(response.slug, "echo-server");
        assert_eq!(response.motto, "Reply precisely.");
        assert_eq!(response.services.len(), 1);
        assert_eq!(response.services[0].name, "echo.v1.Echo");

        server.abort();
        let _ = server.await;
    }

    #[derive(Clone, Default)]
    struct UnimplementedService;

    impl NamedService for UnimplementedService {
        const NAME: &'static str = "test.v1.Noop";
    }

    impl Service<Request<BoxBody>> for UnimplementedService {
        type Response = Response<BoxBody>;
        type Error = Infallible;
        type Future = future::Ready<std::result::Result<Self::Response, Self::Error>>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<std::result::Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, _req: Request<BoxBody>) -> Self::Future {
            let mut response = Response::new(empty_body());
            let headers = response.headers_mut();
            headers.insert(
                tonic::Status::GRPC_STATUS,
                (tonic::Code::Unimplemented as i32).into(),
            );
            headers.insert(
                tonic::codegen::http::header::CONTENT_TYPE,
                tonic::codegen::http::HeaderValue::from_static("application/grpc"),
            );
            future::ready(Ok(response))
        }
    }

    async fn wait_for_holon_meta_client(endpoint: &str) -> HolonMetaClient<tonic::transport::Channel> {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        loop {
            match HolonMetaClient::connect(endpoint.to_string()).await {
                Ok(client) => return client,
                Err(err) if tokio::time::Instant::now() < deadline => {
                    let _ = err;
                    tokio::time::sleep(Duration::from_millis(25)).await;
                }
                Err(err) => panic!("timed out waiting for HolonMeta client: {err}"),
            }
        }
    }

    fn free_port() -> u16 {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.local_addr().unwrap().port()
    }

    fn write_echo_holon() -> TempDir {
        let dir = TempDir::new().unwrap();
        let proto_dir = dir.path().join("protos/echo/v1");
        fs::create_dir_all(&proto_dir).unwrap();
        fs::write(
            dir.path().join("holon.yaml"),
            "given_name: Echo\nfamily_name: Server\nmotto: Reply precisely.\n",
        )
        .unwrap();
        fs::write(
            proto_dir.join("echo.proto"),
            r#"syntax = "proto3";
package echo.v1;

// Echo echoes request payloads for documentation tests.
service Echo {
  // Ping echoes the inbound message.
  rpc Ping(PingRequest) returns (PingResponse);
}

message PingRequest {
  string message = 1;
}

message PingResponse {
  string message = 1;
}
"#,
        )
        .unwrap();
        dir
    }
}
