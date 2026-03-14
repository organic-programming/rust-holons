//! HolonMeta Describe support for Rust holons.

use crate::gen::holonmeta::v1::{
    holon_meta_server::{HolonMeta, HolonMetaServer},
    DescribeRequest, DescribeResponse, EnumValueDoc, FieldDoc, FieldLabel, MethodDoc, ServiceDoc,
};
use crate::identity;
use regex::Regex;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;
use tonic::{Request, Response, Status};

const HOLON_META_SERVICE: &str = "holonmeta.v1.HolonMeta";

type BoxError = Box<dyn std::error::Error>;

/// Build a HolonMeta response from a holon's local proto directory and holon.yaml.
pub fn build_response(
    proto_dir: impl AsRef<Path>,
    holon_yaml_path: impl AsRef<Path>,
) -> Result<DescribeResponse, BoxError> {
    let identity = identity::parse_holon(holon_yaml_path.as_ref())?;
    let index = parse_proto_directory(proto_dir.as_ref())?;

    let services = index
        .services
        .iter()
        .filter(|service| service.full_name != HOLON_META_SERVICE)
        .map(|service| service_doc(service, &index))
        .collect();

    Ok(DescribeResponse {
        slug: identity.slug(),
        motto: identity.motto,
        services,
    })
}

/// Build a manually-registrable tonic service for HolonMeta Describe.
pub fn service(
    proto_dir: impl AsRef<Path>,
    holon_yaml_path: impl AsRef<Path>,
) -> Result<HolonMetaServer<MetaService>, BoxError> {
    Ok(HolonMetaServer::new(MetaService {
        response: build_response(proto_dir, holon_yaml_path)?,
    }))
}

#[derive(Clone)]
pub struct MetaService {
    response: DescribeResponse,
}

#[tonic::async_trait]
impl HolonMeta for MetaService {
    async fn describe(
        &self,
        _request: Request<DescribeRequest>,
    ) -> Result<Response<DescribeResponse>, Status> {
        Ok(Response::new(self.response.clone()))
    }
}

fn service_doc(service: &ServiceDef, index: &ProtoIndex) -> ServiceDoc {
    ServiceDoc {
        name: service.full_name.clone(),
        description: service.comment.description.clone(),
        methods: service
            .methods
            .iter()
            .map(|method| method_doc(method, index))
            .collect(),
    }
}

fn method_doc(method: &MethodDef, index: &ProtoIndex) -> MethodDoc {
    let input_fields = index
        .messages
        .get(&method.input_type)
        .map(|message| {
            message
                .fields
                .iter()
                .map(|field| field_doc(field, index, &mut HashSet::new()))
                .collect()
        })
        .unwrap_or_default();
    let output_fields = index
        .messages
        .get(&method.output_type)
        .map(|message| {
            message
                .fields
                .iter()
                .map(|field| field_doc(field, index, &mut HashSet::new()))
                .collect()
        })
        .unwrap_or_default();

    MethodDoc {
        name: method.name.clone(),
        description: method.comment.description.clone(),
        input_type: method.input_type.clone(),
        output_type: method.output_type.clone(),
        input_fields,
        output_fields,
        client_streaming: method.client_streaming,
        server_streaming: method.server_streaming,
        example_input: method.comment.example.clone(),
    }
}

fn field_doc(field: &FieldDef, index: &ProtoIndex, seen: &mut HashSet<String>) -> FieldDoc {
    let mut doc = FieldDoc {
        name: field.name.clone(),
        r#type: field.type_name(),
        number: field.number,
        description: field.comment.description.clone(),
        label: field.label() as i32,
        map_key_type: field.map_key_type.clone().unwrap_or_default(),
        map_value_type: field.map_value_type.clone().unwrap_or_default(),
        nested_fields: Vec::new(),
        enum_values: Vec::new(),
        required: field.comment.required,
        example: field.comment.example.clone(),
    };

    if field.cardinality == FieldCardinality::Map {
        let map_value_type = field.resolved_map_value_type(index);
        if let Some(message) = index.messages.get(&map_value_type) {
            if seen.insert(message.full_name.clone()) {
                doc.nested_fields = message
                    .fields
                    .iter()
                    .map(|nested| {
                        let mut next_seen = seen.clone();
                        field_doc(nested, index, &mut next_seen)
                    })
                    .collect();
            }
        }
        if let Some(enum_def) = index.enums.get(&map_value_type) {
            doc.enum_values = enum_def.values.iter().map(enum_value_doc).collect();
        }
        return doc;
    }

    let resolved_type = field.resolved_type(index);
    if let Some(message) = index.messages.get(&resolved_type) {
        if seen.insert(message.full_name.clone()) {
            doc.nested_fields = message
                .fields
                .iter()
                .map(|nested| {
                    let mut next_seen = seen.clone();
                    field_doc(nested, index, &mut next_seen)
                })
                .collect();
        }
    }
    if let Some(enum_def) = index.enums.get(&resolved_type) {
        doc.enum_values = enum_def.values.iter().map(enum_value_doc).collect();
    }

    doc
}

fn enum_value_doc(value: &EnumValueDef) -> EnumValueDoc {
    EnumValueDoc {
        name: value.name.clone(),
        number: value.number,
        description: value.comment.description.clone(),
    }
}

fn parse_proto_directory(dir: &Path) -> Result<ProtoIndex, BoxError> {
    let mut index = ProtoIndex::default();
    if !dir.is_dir() {
        return Ok(index);
    }

    let mut files = Vec::new();
    collect_proto_files(dir, &mut files)?;
    files.sort();

    for file in files {
        parse_proto_file(&file, &mut index)?;
    }

    Ok(index)
}

fn collect_proto_files(dir: &Path, out: &mut Vec<PathBuf>) -> Result<(), io::Error> {
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            if entry.file_name().to_string_lossy().starts_with('.') {
                continue;
            }
            collect_proto_files(&path, out)?;
            continue;
        }
        if path.extension().and_then(|ext| ext.to_str()) == Some("proto") {
            out.push(path);
        }
    }
    Ok(())
}

fn parse_proto_file(path: &Path, index: &mut ProtoIndex) -> Result<(), BoxError> {
    let mut package = String::new();
    let mut stack = Vec::new();
    let mut pending_comments = Vec::new();

    for raw_line in fs::read_to_string(path)?.lines() {
        let line = raw_line.trim();
        if let Some(comment) = line.strip_prefix("//") {
            pending_comments.push(comment.trim().to_string());
            continue;
        }
        if line.is_empty() {
            continue;
        }

        if let Some(captures) = package_re().captures(line) {
            package = captures[1].to_string();
            pending_comments.clear();
            continue;
        }

        if let Some(captures) = service_re().captures(line) {
            let name = captures[1].to_string();
            let service = ServiceDef {
                full_name: qualify(&package, &name),
                comment: CommentMeta::parse(&pending_comments),
                methods: Vec::new(),
            };
            index.services.push(service);
            pending_comments.clear();
            stack.push(Block::Service(name));
            trim_closed_blocks(line, &mut stack);
            continue;
        }

        if let Some(captures) = message_re().captures(line) {
            let name = captures[1].to_string();
            let scope = message_scope(&stack);
            let message = MessageDef {
                full_name: qualify(&package, &qualify_scope(&scope, &name)),
                scope: scope.clone(),
                fields: Vec::new(),
            };
            index
                .simple_types
                .entry(message.simple_key())
                .or_insert_with(|| message.full_name.clone());
            index.messages.insert(message.full_name.clone(), message);
            pending_comments.clear();
            stack.push(Block::Message(name));
            trim_closed_blocks(line, &mut stack);
            continue;
        }

        if let Some(captures) = enum_re().captures(line) {
            let name = captures[1].to_string();
            let scope = message_scope(&stack);
            let enum_def = EnumDef {
                full_name: qualify(&package, &qualify_scope(&scope, &name)),
                scope: scope.clone(),
                values: Vec::new(),
            };
            index
                .simple_types
                .entry(enum_def.simple_key())
                .or_insert_with(|| enum_def.full_name.clone());
            index.enums.insert(enum_def.full_name.clone(), enum_def);
            pending_comments.clear();
            stack.push(Block::Enum(name));
            trim_closed_blocks(line, &mut stack);
            continue;
        }

        match stack.last() {
            Some(Block::Service(_service_name)) => {
                if let Some(captures) = rpc_re().captures(line) {
                    let input_type = resolve_type_name(&captures[3], &package, &[], index);
                    let output_type = resolve_type_name(&captures[5], &package, &[], index);
                    let comment = CommentMeta::parse(&pending_comments);
                    if let Some(service) = index.services.last_mut() {
                        service.methods.push(MethodDef {
                            name: captures[1].to_string(),
                            input_type,
                            output_type,
                            client_streaming: captures.get(2).is_some(),
                            server_streaming: captures.get(4).is_some(),
                            comment,
                        });
                    }
                    pending_comments.clear();
                    trim_closed_blocks(line, &mut stack);
                    continue;
                }
            }
            Some(Block::Message(_message_name)) => {
                let scope = message_scope(&stack);
                let key = qualify(&package, &scope.join("."));
                if let Some(captures) = map_field_re().captures(line) {
                    let comment = CommentMeta::parse(&pending_comments);
                    let field_scope = scope.clone();
                    let map_key_type =
                        resolve_type_name(&captures[2], &package, &field_scope, index);
                    let map_value_type =
                        resolve_type_name(&captures[3], &package, &field_scope, index);
                    if let Some(message) = index.messages.get_mut(&key) {
                        message.fields.push(FieldDef {
                            name: captures[4].to_string(),
                            number: captures[5].parse()?,
                            comment,
                            cardinality: FieldCardinality::Map,
                            r#type: None,
                            map_key_type: Some(map_key_type),
                            map_value_type: Some(map_value_type),
                            package: package.clone(),
                            scope: field_scope,
                        });
                    }
                    pending_comments.clear();
                    trim_closed_blocks(line, &mut stack);
                    continue;
                }

                if let Some(captures) = field_re().captures(line) {
                    let qualifier = captures.get(1).map(|m| m.as_str().trim()).unwrap_or("");
                    let comment = CommentMeta::parse(&pending_comments);
                    let field_scope = scope.clone();
                    let field_type = resolve_type_name(&captures[2], &package, &field_scope, index);
                    if let Some(message) = index.messages.get_mut(&key) {
                        message.fields.push(FieldDef {
                            name: captures[3].to_string(),
                            number: captures[4].parse()?,
                            comment,
                            cardinality: if qualifier == "repeated" {
                                FieldCardinality::Repeated
                            } else {
                                FieldCardinality::Optional
                            },
                            r#type: Some(field_type),
                            map_key_type: None,
                            map_value_type: None,
                            package: package.clone(),
                            scope: field_scope,
                        });
                    }
                    pending_comments.clear();
                    trim_closed_blocks(line, &mut stack);
                    continue;
                }
            }
            Some(Block::Enum(enum_name)) => {
                let key = qualify(&package, &qualify_scope(&message_scope(&stack), enum_name));
                if let Some(enum_def) = index.enums.get_mut(&key) {
                    if let Some(captures) = enum_value_re().captures(line) {
                        enum_def.values.push(EnumValueDef {
                            name: captures[1].to_string(),
                            number: captures[2].parse()?,
                            comment: CommentMeta::parse(&pending_comments),
                        });
                        pending_comments.clear();
                        trim_closed_blocks(line, &mut stack);
                        continue;
                    }
                }
            }
            None => {}
        }

        pending_comments.clear();
        trim_closed_blocks(line, &mut stack);
    }

    Ok(())
}

fn trim_closed_blocks(line: &str, stack: &mut Vec<Block>) {
    for _ in line.chars().filter(|ch| *ch == '}') {
        if !stack.is_empty() {
            stack.pop();
        }
    }
}

fn message_scope(stack: &[Block]) -> Vec<String> {
    stack
        .iter()
        .filter_map(|block| match block {
            Block::Message(name) => Some(name.clone()),
            _ => None,
        })
        .collect()
}

fn qualify(package: &str, name: &str) -> String {
    if name.is_empty() {
        return String::new();
    }
    let cleaned = name.trim_start_matches('.');
    if cleaned.contains('.') || package.is_empty() {
        cleaned.to_string()
    } else {
        format!("{package}.{cleaned}")
    }
}

fn qualify_scope(scope: &[String], name: &str) -> String {
    if scope.is_empty() {
        name.to_string()
    } else {
        format!("{}.{}", scope.join("."), name)
    }
}

fn resolve_type_name(
    type_name: &str,
    package: &str,
    scope: &[String],
    index: &ProtoIndex,
) -> String {
    let cleaned = type_name.trim();
    if cleaned.is_empty() {
        return String::new();
    }
    if let Some(stripped) = cleaned.strip_prefix('.') {
        return stripped.to_string();
    }
    if scalar_types().contains(cleaned) {
        return cleaned.to_string();
    }
    if cleaned.contains('.') {
        let qualified = qualify(package, cleaned);
        if index.messages.contains_key(&qualified) || index.enums.contains_key(&qualified) {
            return qualified;
        }
        return cleaned.to_string();
    }

    for i in (0..=scope.len()).rev() {
        let candidate = qualify(package, &qualify_scope(&scope[..i], cleaned));
        if index.messages.contains_key(&candidate) || index.enums.contains_key(&candidate) {
            return candidate;
        }
    }
    if let Some(found) = index.simple_types.get(&qualify_scope(scope, cleaned)) {
        return found.clone();
    }
    if let Some(found) = index.simple_types.get(cleaned) {
        return found.clone();
    }
    qualify(package, cleaned)
}

fn scalar_types() -> &'static HashSet<&'static str> {
    static TYPES: OnceLock<HashSet<&'static str>> = OnceLock::new();
    TYPES.get_or_init(|| {
        HashSet::from([
            "double", "float", "int64", "uint64", "int32", "fixed64", "fixed32", "bool", "string",
            "bytes", "uint32", "sfixed32", "sfixed64", "sint32", "sint64",
        ])
    })
}

fn package_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"^package\s+([A-Za-z0-9_.]+)\s*;").unwrap())
}

fn service_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"^service\s+([A-Za-z_][A-Za-z0-9_]*)\s*\{?").unwrap())
}

fn message_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"^message\s+([A-Za-z_][A-Za-z0-9_]*)\s*\{?").unwrap())
}

fn enum_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"^enum\s+([A-Za-z_][A-Za-z0-9_]*)\s*\{?").unwrap())
}

fn rpc_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(
            r"^rpc\s+([A-Za-z_][A-Za-z0-9_]*)\s*\(\s*(stream\s+)?([.A-Za-z0-9_]+)\s*\)\s*returns\s*\(\s*(stream\s+)?([.A-Za-z0-9_]+)\s*\)",
        )
        .unwrap()
    })
}

fn map_field_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(
            r"^(repeated\s+)?map\s*<\s*([.A-Za-z0-9_]+)\s*,\s*([.A-Za-z0-9_]+)\s*>\s+([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(\d+)\s*;",
        )
        .unwrap()
    })
}

fn field_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(r"^(optional\s+|repeated\s+)?([.A-Za-z0-9_]+)\s+([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(\d+)\s*;")
            .unwrap()
    })
}

fn enum_value_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"^([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(-?\d+)\s*;").unwrap())
}

#[derive(Default)]
struct ProtoIndex {
    services: Vec<ServiceDef>,
    messages: HashMap<String, MessageDef>,
    enums: HashMap<String, EnumDef>,
    simple_types: HashMap<String, String>,
}

struct ServiceDef {
    full_name: String,
    comment: CommentMeta,
    methods: Vec<MethodDef>,
}

struct MethodDef {
    name: String,
    input_type: String,
    output_type: String,
    client_streaming: bool,
    server_streaming: bool,
    comment: CommentMeta,
}

struct MessageDef {
    full_name: String,
    scope: Vec<String>,
    fields: Vec<FieldDef>,
}

impl MessageDef {
    fn simple_key(&self) -> String {
        qualify_scope(
            &self.scope,
            self.full_name.rsplit('.').next().unwrap_or_default(),
        )
    }
}

struct EnumDef {
    full_name: String,
    scope: Vec<String>,
    values: Vec<EnumValueDef>,
}

impl EnumDef {
    fn simple_key(&self) -> String {
        qualify_scope(
            &self.scope,
            self.full_name.rsplit('.').next().unwrap_or_default(),
        )
    }
}

struct EnumValueDef {
    name: String,
    number: i32,
    comment: CommentMeta,
}

#[derive(Clone)]
struct FieldDef {
    name: String,
    number: i32,
    comment: CommentMeta,
    cardinality: FieldCardinality,
    r#type: Option<String>,
    map_key_type: Option<String>,
    map_value_type: Option<String>,
    package: String,
    scope: Vec<String>,
}

impl FieldDef {
    fn type_name(&self) -> String {
        if self.cardinality == FieldCardinality::Map {
            format!(
                "map<{}, {}>",
                self.map_key_type.as_deref().unwrap_or_default(),
                self.map_value_type.as_deref().unwrap_or_default()
            )
        } else {
            self.r#type.clone().unwrap_or_default()
        }
    }

    fn resolved_type(&self, index: &ProtoIndex) -> String {
        resolve_type_name(
            self.r#type.as_deref().unwrap_or_default(),
            &self.package,
            &self.scope,
            index,
        )
    }

    fn resolved_map_value_type(&self, index: &ProtoIndex) -> String {
        resolve_type_name(
            self.map_value_type.as_deref().unwrap_or_default(),
            &self.package,
            &self.scope,
            index,
        )
    }

    fn label(&self) -> FieldLabel {
        match self.cardinality {
            FieldCardinality::Optional => FieldLabel::Optional,
            FieldCardinality::Repeated => FieldLabel::Repeated,
            FieldCardinality::Map => FieldLabel::Map,
        }
    }
}

#[derive(Clone, Default)]
struct CommentMeta {
    description: String,
    required: bool,
    example: String,
}

impl CommentMeta {
    fn parse(lines: &[String]) -> Self {
        let mut description = Vec::new();
        let mut examples = Vec::new();
        let mut required = false;

        for raw in lines {
            let line = raw.trim();
            if line.is_empty() {
                continue;
            }
            if line == "@required" {
                required = true;
                continue;
            }
            if let Some(example) = line.strip_prefix("@example") {
                let example = example.trim();
                if !example.is_empty() {
                    examples.push(example.to_string());
                }
                continue;
            }
            description.push(line.to_string());
        }

        Self {
            description: description.join(" "),
            required,
            example: examples.join("\n"),
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum FieldCardinality {
    Optional,
    Repeated,
    Map,
}

enum Block {
    Service(String),
    Message(String),
    Enum(String),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gen::holonmeta::v1::holon_meta_client::HolonMetaClient;
    use tempfile::TempDir;
    use tokio::sync::oneshot;
    use tokio_stream::wrappers::TcpListenerStream;
    use tonic::transport::Server;

    #[test]
    fn build_response_from_echo_proto() {
        let holon = write_echo_holon();
        let response =
            build_response(holon.path().join("protos"), holon.path().join("holon.yaml")).unwrap();

        assert_eq!(response.slug, "echo-server");
        assert_eq!(response.motto, "Reply precisely.");
        assert_eq!(response.services.len(), 1);

        let service = &response.services[0];
        assert_eq!(service.name, "echo.v1.Echo");
        assert_eq!(
            service.description,
            "Echo echoes request payloads for documentation tests."
        );
        assert_eq!(service.methods.len(), 1);

        let method = &service.methods[0];
        assert_eq!(method.name, "Ping");
        assert_eq!(method.input_type, "echo.v1.PingRequest");
        assert_eq!(method.output_type, "echo.v1.PingResponse");
        assert_eq!(
            method.example_input,
            r#"{"message":"hello","sdk":"go-holons"}"#
        );
        assert_eq!(method.input_fields.len(), 2);

        let field = &method.input_fields[0];
        assert_eq!(field.name, "message");
        assert_eq!(field.r#type, "string");
        assert_eq!(field.number, 1);
        assert_eq!(field.description, "Message to echo back.");
        assert_eq!(field.label, FieldLabel::Optional as i32);
        assert!(field.required);
        assert_eq!(field.example, r#""hello""#);
    }

    #[tokio::test]
    async fn describe_rpc_round_trip() {
        let holon = write_echo_holon();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let incoming = TcpListenerStream::new(listener);
        let service =
            service(holon.path().join("protos"), holon.path().join("holon.yaml")).unwrap();
        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

        let server = tokio::spawn(async move {
            Server::builder()
                .add_service(service)
                .serve_with_incoming_shutdown(incoming, async move {
                    let _ = shutdown_rx.await;
                })
                .await
                .unwrap();
        });

        let endpoint = format!("http://{addr}");
        let mut client = HolonMetaClient::connect(endpoint).await.unwrap();
        let response = client
            .describe(DescribeRequest {})
            .await
            .unwrap()
            .into_inner();

        assert_eq!(response.slug, "echo-server");
        assert_eq!(response.services.len(), 1);
        assert_eq!(response.services[0].name, "echo.v1.Echo");
        assert_eq!(response.services[0].methods[0].name, "Ping");

        let _ = shutdown_tx.send(());
        server.await.unwrap();
    }

    #[test]
    fn handles_missing_proto_directory() {
        let dir = TempDir::new().unwrap();
        fs::write(
            dir.path().join("holon.yaml"),
            "given_name: Silent\nfamily_name: Holon\nmotto: Quietly available.\n",
        )
        .unwrap();

        let response =
            build_response(dir.path().join("protos"), dir.path().join("holon.yaml")).unwrap();
        assert_eq!(response.slug, "silent-holon");
        assert_eq!(response.motto, "Quietly available.");
        assert!(response.services.is_empty());
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
  // @example {"message":"hello","sdk":"go-holons"}
  rpc Ping(PingRequest) returns (PingResponse);
}

message PingRequest {
  // Message to echo back.
  // @required
  // @example "hello"
  string message = 1;

  // SDK marker included in the response.
  // @example "go-holons"
  string sdk = 2;
}

message PingResponse {
  // Echoed message.
  string message = 1;

  // SDK marker from the server.
  string sdk = 2;
}
"#,
        )
        .unwrap();
        dir
    }
}
