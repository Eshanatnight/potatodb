//! `PostgreSQL` wire protocol server for `PotatoDB`.
//!
//! Uses the [`pgwire`] crate to expose `PotatoDB` over the `PostgreSQL`
//! wire protocol so that external tools (`psql`, `DBeaver`, language
//! drivers) can connect and execute SQL.
//!
//! ## Usage
//!
//! ```no_run
//! use potatodb_server::start_server;
//!
//! #[tokio::main]
//! async fn main() {
//!     start_server("./data", "127.0.0.1:5432").await.unwrap();
//! }
//! ```

use std::fmt::Debug;
use std::fs::File;
use std::io::{BufReader, Error as IoError, ErrorKind};
use std::sync::Arc;

use arrow::array::{
    Array, BooleanArray, DurationMicrosecondArray, FixedSizeBinaryArray, Float32Array,
    Float64Array, Int16Array, Int32Array, Int64Array, Int8Array, LargeStringArray, StringArray,
    UInt32Array, UInt64Array,
};
use arrow::datatypes::DataType;
use async_trait::async_trait;
use futures::stream;
use futures::Sink;
use pgwire::api::auth::md5pass::{hash_md5_password, Md5PasswordAuthStartupHandler};
use pgwire::api::auth::{AuthSource, DefaultServerParameterProvider, LoginInfo, Password};
use pgwire::api::portal::Portal;
use pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::results::{
    DataRowEncoder, DescribePortalResponse, DescribeStatementResponse, FieldFormat, FieldInfo,
    QueryResponse, Response, Tag,
};
use pgwire::api::stmt::{NoopQueryParser, StoredStatement};
use pgwire::api::{ClientInfo, ClientPortalStore, NoopHandler, PgWireServerHandlers, Type};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::PgWireBackendMessage;
use tokio::net::TcpListener;
use tokio::sync::RwLock;
use tokio::time::{sleep, Duration};
use tokio_rustls::rustls::ServerConfig;
use tokio_rustls::TlsAcceptor;

use rustls_pemfile::{certs, pkcs8_private_keys};
use rustls_pki_types::{CertificateDer, PrivateKeyDer};

use potatodb_engine::{PotatoDB, QueryResult};

struct Processor {
    db: Arc<RwLock<PotatoDB>>,
    query_parser: Arc<NoopQueryParser>,
    max_connections: Arc<tokio::sync::Semaphore>,
}

type StartupAuthHandler =
    Md5PasswordAuthStartupHandler<EnvAuthSource, DefaultServerParameterProvider>;

#[derive(Debug)]
struct EnvAuthSource;

#[async_trait]
impl AuthSource for EnvAuthSource {
    async fn get_password(&self, login_info: &LoginInfo) -> PgWireResult<Password> {
        let expected_user =
            std::env::var("POTATODB_USER").unwrap_or_else(|_| "potatodb".to_string());
        let expected_password =
            std::env::var("POTATODB_PASSWORD").unwrap_or_else(|_| "potatodb".to_string());

        let user = login_info.user().unwrap_or("");
        if user != expected_user {
            return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                "FATAL".to_string(),
                "28P01".to_string(),
                "Password authentication failed".to_string(),
            ))));
        }

        let salt = vec![1, 2, 3, 4];
        let hash = hash_md5_password(user, &expected_password, &salt);
        Ok(Password::new(Some(salt), hash.into_bytes()))
    }
}

impl Processor {
    async fn run_query(
        &self,
        query: &str,
    ) -> Result<QueryResult, Box<dyn std::error::Error + Send + Sync>> {
        let _permit = self
            .max_connections
            .acquire()
            .await
            .map_err(|e| format!("Connection pool closed: {e}"))?;
        if is_read_only_query(query) {
            let mut db = self.db.write().await;
            db.execute_readonly(query).await
        } else {
            let mut db = self.db.write().await;
            db.execute(query).await
        }
    }
}

#[async_trait]
impl SimpleQueryHandler for Processor {
    async fn do_query<C>(&self, _client: &mut C, query: &str) -> PgWireResult<Vec<Response>>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        match self.run_query(query).await {
            Ok(result) => Ok(vec![query_result_to_response(result)?]),
            Err(e) => Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_string(),
                "XX000".to_string(),
                e.to_string(),
            )))),
        }
    }
}

#[async_trait]
impl ExtendedQueryHandler for Processor {
    type Statement = String;
    type QueryParser = NoopQueryParser;

    fn query_parser(&self) -> Arc<Self::QueryParser> {
        self.query_parser.clone()
    }

    async fn do_query<C>(
        &self,
        _client: &mut C,
        portal: &Portal<Self::Statement>,
        _max_rows: usize,
    ) -> PgWireResult<Response>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let sql = bind_portal_sql(portal);
        match self.run_query(&sql).await {
            Ok(result) => query_result_to_response(result),
            Err(e) => Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_string(),
                "XX000".to_string(),
                e.to_string(),
            )))),
        }
    }

    async fn do_describe_statement<C>(
        &self,
        _client: &mut C,
        stmt: &StoredStatement<Self::Statement>,
    ) -> PgWireResult<DescribeStatementResponse>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let param_types: Vec<Type> = stmt
            .parameter_types
            .iter()
            .map(|t| t.clone().unwrap_or(Type::UNKNOWN))
            .collect();
        Ok(DescribeStatementResponse::new(param_types, vec![]))
    }

    async fn do_describe_portal<C>(
        &self,
        _client: &mut C,
        _portal: &Portal<Self::Statement>,
    ) -> PgWireResult<DescribePortalResponse>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        Ok(DescribePortalResponse::new(vec![]))
    }
}

struct PotatoHandlerFactory {
    processor: Arc<Processor>,
    startup: Arc<StartupAuthHandler>,
}

impl PgWireServerHandlers for PotatoHandlerFactory {
    fn simple_query_handler(&self) -> Arc<impl SimpleQueryHandler> {
        self.processor.clone()
    }

    fn extended_query_handler(&self) -> Arc<impl ExtendedQueryHandler> {
        self.processor.clone()
    }

    fn startup_handler(&self) -> Arc<impl pgwire::api::auth::StartupHandler> {
        self.startup.clone()
    }

    fn copy_handler(&self) -> Arc<impl pgwire::api::copy::CopyHandler> {
        Arc::new(NoopHandler)
    }
}

fn query_result_to_response(result: QueryResult) -> PgWireResult<Response> {
    match result {
        QueryResult::Message(msg) => Ok(Response::Execution(Tag::new(&msg))),
        QueryResult::Records(batches) => {
            if batches.is_empty() {
                return Ok(Response::EmptyQuery);
            }

            let schema = batches[0].schema();
            let field_infos: Vec<FieldInfo> = schema
                .fields()
                .iter()
                .map(|f| {
                    FieldInfo::new(
                        f.name().clone(),
                        None,
                        None,
                        arrow_to_pg_type(f.data_type()),
                        FieldFormat::Text,
                    )
                })
                .collect();

            let fields = Arc::new(field_infos);
            let mut data_rows: Vec<PgWireResult<_>> = Vec::new();
            for batch in &batches {
                for row_idx in 0..batch.num_rows() {
                    let mut encoder = DataRowEncoder::new(fields.clone());
                    for col_idx in 0..batch.num_columns() {
                        let col = batch.column(col_idx);
                        encode_field(&mut encoder, col, row_idx)?;
                    }
                    data_rows.push(Ok(encoder.take_row()));
                }
            }
            let row_stream = stream::iter(data_rows);
            Ok(Response::Query(QueryResponse::new(fields, row_stream)))
        }
    }
}

fn is_read_only_query(query: &str) -> bool {
    let first = query.split_whitespace().next().unwrap_or("").to_uppercase();
    matches!(
        first.as_str(),
        "SELECT" | "WITH" | "SHOW" | "DESCRIBE" | "EXPLAIN" | "VALUES"
    )
}

fn bind_portal_sql(portal: &Portal<String>) -> String {
    let mut sql = portal.statement.statement.clone();
    for i in (0..portal.parameter_len()).rev() {
        let placeholder = format!("${}", i + 1);
        let pg_type = portal
            .statement
            .parameter_types
            .get(i)
            .and_then(|t| t.as_ref())
            .unwrap_or(&Type::UNKNOWN);
        let val = format_portal_param(
            portal
                .parameters
                .get(i)
                .and_then(|p| p.as_ref())
                .map(std::convert::AsRef::as_ref),
            pg_type,
        );
        sql = sql.replace(&placeholder, &val);
    }
    sql
}

fn format_portal_param(param: Option<&[u8]>, pg_type: &Type) -> String {
    let Some(bytes) = param else {
        return "NULL".to_string();
    };
    let raw = String::from_utf8_lossy(bytes).to_string();
    if raw.eq_ignore_ascii_case("null") {
        return "NULL".to_string();
    }
    if matches!(
        *pg_type,
        Type::INT2 | Type::INT4 | Type::INT8 | Type::FLOAT4 | Type::FLOAT8 | Type::NUMERIC
    ) || *pg_type == Type::BOOL
        || (*pg_type == Type::UNKNOWN && raw.parse::<f64>().is_ok())
    {
        raw
    } else {
        format!("'{}'", raw.replace('\'', "''"))
    }
}

const fn arrow_to_pg_type(dt: &DataType) -> Type {
    match dt {
        DataType::Boolean => Type::BOOL,
        DataType::Int8 | DataType::Int16 => Type::INT2,
        DataType::Int32 | DataType::UInt8 | DataType::UInt16 => Type::INT4,
        DataType::Int64 | DataType::UInt32 | DataType::UInt64 => Type::INT8,
        DataType::Float32 => Type::FLOAT4,
        DataType::Float64 => Type::FLOAT8,
        DataType::Utf8 | DataType::LargeUtf8 => Type::VARCHAR,
        DataType::Date32 | DataType::Date64 => Type::DATE,
        DataType::Timestamp(_, _) => Type::TIMESTAMP,
        DataType::Duration(_) => Type::INTERVAL,
        DataType::FixedSizeBinary(16) => Type::UUID,
        DataType::Decimal128(_, _) => Type::NUMERIC,
        DataType::Binary | DataType::LargeBinary => Type::BYTEA,
        _ => Type::TEXT,
    }
}

fn encode_field(encoder: &mut DataRowEncoder, array: &dyn Array, row: usize) -> PgWireResult<()> {
    if array.is_null(row) {
        encoder
            .encode_field(&None::<&str>)
            .map_err(|e| PgWireError::ApiError(Box::new(std::io::Error::other(e.to_string()))))
    } else {
        let text = array_value_to_string(array, row);
        encoder
            .encode_field(&Some(&text))
            .map_err(|e| PgWireError::ApiError(Box::new(std::io::Error::other(e.to_string()))))
    }
}

fn array_value_to_string(array: &dyn Array, row: usize) -> String {
    if let Some(a) = array.as_any().downcast_ref::<Int32Array>() {
        return a.value(row).to_string();
    }
    if let Some(a) = array.as_any().downcast_ref::<Int64Array>() {
        return a.value(row).to_string();
    }
    if let Some(a) = array.as_any().downcast_ref::<Float64Array>() {
        return a.value(row).to_string();
    }
    if let Some(a) = array.as_any().downcast_ref::<StringArray>() {
        return a.value(row).to_string();
    }
    if let Some(a) = array.as_any().downcast_ref::<BooleanArray>() {
        return if a.value(row) { "t" } else { "f" }.to_string();
    }
    if let Some(a) = array.as_any().downcast_ref::<Int8Array>() {
        return a.value(row).to_string();
    }
    if let Some(a) = array.as_any().downcast_ref::<Int16Array>() {
        return a.value(row).to_string();
    }
    if let Some(a) = array.as_any().downcast_ref::<UInt32Array>() {
        return a.value(row).to_string();
    }
    if let Some(a) = array.as_any().downcast_ref::<UInt64Array>() {
        return a.value(row).to_string();
    }
    if let Some(a) = array.as_any().downcast_ref::<Float32Array>() {
        return a.value(row).to_string();
    }
    if let Some(a) = array.as_any().downcast_ref::<LargeStringArray>() {
        return a.value(row).to_string();
    }
    if let Some(a) = array.as_any().downcast_ref::<DurationMicrosecondArray>() {
        return a.value(row).to_string();
    }
    if let Some(a) = array.as_any().downcast_ref::<FixedSizeBinaryArray>() {
        let value = a.value(row);
        if value.len() == 16 {
            return format!(
                "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
                value[0],
                value[1],
                value[2],
                value[3],
                value[4],
                value[5],
                value[6],
                value[7],
                value[8],
                value[9],
                value[10],
                value[11],
                value[12],
                value[13],
                value[14],
                value[15]
            );
        }
    }
    format!("{:?}", array.slice(row, 1))
}

/// Starts the `PotatoDB` pgwire server on the given address.
///
/// `data_url` is passed to [`PotatoDB::new`]. `bind_addr` is a socket
/// address like `"127.0.0.1:5432"`.
///
/// # Errors
///
/// Returns an error if the database cannot be opened or the TCP listener
/// fails to bind.
#[allow(
    clippy::cast_precision_loss,
    clippy::significant_drop_tightening,
    clippy::manual_let_else
)]
pub async fn start_server(
    data_url: &str,
    bind_addr: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let db = PotatoDB::new(data_url.to_string(), None).await?;
    let shared_db = Arc::new(RwLock::new(db));
    let max_conn = std::env::var("POTATODB_MAX_CONNECTIONS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(100);
    let processor = Arc::new(Processor {
        db: shared_db,
        query_parser: Arc::new(NoopQueryParser::new()),
        max_connections: Arc::new(tokio::sync::Semaphore::new(max_conn)),
    });
    let startup = Arc::new(StartupAuthHandler::new(
        Arc::new(EnvAuthSource),
        Arc::new(DefaultServerParameterProvider::default()),
    ));

    let factory = Arc::new(PotatoHandlerFactory { processor, startup });

    let interval_secs = std::env::var("POTATODB_AUTO_VACUUM_INTERVAL_SECS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(0);
    let file_threshold = std::env::var("POTATODB_AUTO_VACUUM_FILE_THRESHOLD")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(25);
    let bytes_threshold = std::env::var("POTATODB_AUTO_VACUUM_BYTES_THRESHOLD")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(256 * 1024 * 1024);
    let age_threshold_secs = std::env::var("POTATODB_AUTO_VACUUM_AGE_SECS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(3600);

    if interval_secs > 0 {
        let compact_db = factory.processor.db.clone();
        tokio::spawn(async move {
            loop {
                sleep(Duration::from_secs(interval_secs)).await;
                let candidates = {
                    let db = compact_db.read().await;
                    let mut tables_to_compact: Vec<(String, f64, u64)> = Vec::new();
                    for table in db.table_names() {
                        let count = match db.parquet_file_count(&table).await {
                            Ok(c) => c,
                            Err(_) => continue,
                        };
                        if count == 0 {
                            continue;
                        }
                        let total_bytes = db.table_total_bytes(&table).await.unwrap_or(0);
                        let oldest_age_secs =
                            db.table_oldest_file_age_secs(&table).await.unwrap_or(0);

                        let file_score = count as f64 / file_threshold.max(1) as f64;
                        let bytes_score = total_bytes as f64 / bytes_threshold.max(1) as f64;
                        let score = file_score + bytes_score;
                        if score >= 1.0 || oldest_age_secs >= age_threshold_secs {
                            tables_to_compact.push((table, score, oldest_age_secs));
                        }
                    }
                    tables_to_compact.sort_by(|a, b| {
                        b.1.partial_cmp(&a.1)
                            .unwrap_or(std::cmp::Ordering::Equal)
                            .then_with(|| b.2.cmp(&a.2))
                            .then_with(|| a.0.cmp(&b.0))
                    });
                    tables_to_compact
                };

                for (table, _, _) in candidates {
                    let mut db = compact_db.write().await;
                    let _ = db.execute(&format!("VACUUM \"{table}\";")).await;
                }
            }
        });
    }

    let tls_acceptor = load_tls_acceptor_from_env()?;

    let listener = TcpListener::bind(bind_addr).await?;
    if tls_acceptor.is_some() {
        eprintln!("PotatoDB server listening on {bind_addr} with TLS");
    } else {
        eprintln!("PotatoDB server listening on {bind_addr}");
    }

    loop {
        let (socket, addr) = listener.accept().await?;
        eprintln!("New connection from {addr}");
        let f = factory.clone();
        let tls = tls_acceptor.clone();
        tokio::spawn(async move {
            if let Err(err) = pgwire::tokio::process_socket(socket, tls, f).await {
                eprintln!("Connection error from {addr}: {err}");
            }
        });
    }
}

fn load_tls_acceptor_from_env() -> Result<Option<TlsAcceptor>, IoError> {
    let cert_path = std::env::var("POTATODB_TLS_CERT").ok();
    let key_path = std::env::var("POTATODB_TLS_KEY").ok();
    let (Some(cert_path), Some(key_path)) = (cert_path, key_path) else {
        return Ok(None);
    };

    let cert_chain = certs(&mut BufReader::new(File::open(cert_path)?))
        .collect::<Result<Vec<CertificateDer<'static>>, IoError>>()?;
    let key = pkcs8_private_keys(&mut BufReader::new(File::open(key_path)?))
        .map(|key| key.map(PrivateKeyDer::from))
        .collect::<Result<Vec<PrivateKeyDer<'static>>, IoError>>()?
        .into_iter()
        .next()
        .ok_or_else(|| IoError::new(ErrorKind::InvalidInput, "No PKCS#8 private key found"))?;

    let mut config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(cert_chain, key)
        .map_err(|e| IoError::new(ErrorKind::InvalidInput, e.to_string()))?;
    config.alpn_protocols = vec![b"postgresql".to_vec()];
    Ok(Some(TlsAcceptor::from(Arc::new(config))))
}
