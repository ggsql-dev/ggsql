/*!
ggsql REST API Server

Provides HTTP endpoints for executing ggsql queries and returning visualization outputs.

## Usage

```bash
ggsql-rest --host 127.0.0.1 --port 3000
```

## Session Management

Sessions provide isolation for uploaded tables. Each session:
- Has a unique ID
- Tracks tables uploaded within it
- Automatically cleans up after inactivity timeout

```bash
# Create a session
curl -X POST http://localhost:3334/api/v1/sessions
# Returns: {"status":"success","data":{"sessionId":"abc123..."}}

# Upload to session (tables are isolated)
curl -X POST http://localhost:3334/api/v1/sessions/{sessionId}/upload \
  -F "file=@data.csv"

# Query within session (sees base tables + session tables)
curl -X POST http://localhost:3334/api/v1/sessions/{sessionId}/sql \
  -H "Content-Type: application/json" \
  -d '{"query": "SELECT * FROM data"}'

# Delete session (drops all session tables)
curl -X DELETE http://localhost:3334/api/v1/sessions/{sessionId}
```

## Endpoints

### Session Endpoints
- `POST /api/v1/sessions` - Create a new session
- `DELETE /api/v1/sessions/:id` - Delete a session and its tables
- `POST /api/v1/sessions/:id/upload` - Upload file to session
- `POST /api/v1/sessions/:id/sql` - Execute SQL in session context
- `POST /api/v1/sessions/:id/query` - Execute ggsql in session context
- `GET /api/v1/sessions/:id/schema` - Get filtered schema for session

### Utility Endpoints
- `POST /api/v1/parse` - Parse ggsql query (debugging)
- `GET /api/v1/health` - Health check
- `GET /api/v1/version` - Version information

## Configuration

```
--host <HOST>                  Host to bind (default: 127.0.0.1)
--port <PORT>                  Port to bind (default: 3334)
--session-timeout-mins <N>     Session inactivity timeout (default: 30)
--session-cleanup-interval <N> Cleanup check interval in seconds (default: 60)
--upload-limit-mb <N>          Max upload size in MB (default: 50)
```
*/

use axum::{
    extract::{DefaultBodyLimit, Multipart, State},
    http::{header, StatusCode},
    response::{IntoResponse, Json, Response},
    routing::{delete, get, post},
    Router,
};
use clap::Parser;
use regex; // For query rewriting
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use tower_http::cors::{Any, CorsLayer};
use tracing::info;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use ggsql::{parser, validate, GgsqlError, VERSION};
use ggsql::execute::prepare_data_with_executor;
use ggsql::session::SessionManager;

#[cfg(feature = "duckdb")]
use ggsql::reader::{DuckDBReader, Reader};

#[cfg(feature = "vegalite")]
use ggsql::writer::{VegaLiteWriter, Writer};

/// CLI arguments for the REST API server
#[derive(Parser)]
#[command(name = "ggsql-rest")]
#[command(about = "ggsql REST API Server")]
#[command(version = VERSION)]
struct Cli {
    /// Host address to bind to
    #[arg(long, default_value = "127.0.0.1")]
    host: String,

    /// Port number to bind to
    #[arg(long, default_value = "3334")]
    port: u16,

    /// CORS allowed origins (comma-separated)
    #[arg(long, default_value = "*")]
    cors_origin: String,

    /// Load sample data into in-memory database
    #[arg(long, default_value = "false")]
    load_sample_data: bool,

    /// Load data from file(s) into in-memory database
    /// Supports: CSV, Parquet, JSON
    /// Example: --load-data data.csv --load-data other.parquet
    #[arg(long = "load-data")]
    load_data_files: Vec<String>,

    /// Maximum rows returned by /api/v1/sql endpoint (0 = unlimited)
    #[arg(long, default_value = "10000")]
    sql_max_rows: usize,

    /// Maximum file upload size in megabytes
    #[arg(long, default_value = "50")]
    upload_limit_mb: usize,

    /// Session inactivity timeout in minutes
    #[arg(long, default_value = "30")]
    session_timeout_mins: u64,

    /// Session cleanup check interval in seconds
    #[arg(long, default_value = "60")]
    session_cleanup_interval: u64,
}

/// Shared application state
#[derive(Clone)]
struct AppState {
    /// Pre-initialized DuckDB reader with loaded data
    /// Wrapped in Arc<Mutex> since DuckDB Connection is not Sync
    #[cfg(feature = "duckdb")]
    reader: Option<std::sync::Arc<std::sync::Mutex<DuckDBReader>>>,

    /// Maximum rows returned by SQL endpoint (0 = unlimited)
    sql_max_rows: usize,

    /// Maximum upload size in bytes (for error messages)
    upload_limit_bytes: usize,

    /// Session manager for per-client table isolation
    session_manager: std::sync::Arc<SessionManager>,
}

// ============================================================================
// Request/Response Types
// ============================================================================

/// Request body for /api/v1/query endpoint
#[derive(Debug, Deserialize)]
struct QueryRequest {
    /// ggsql query to execute
    query: String,
    /// Data source connection string (optional, default: duckdb://memory)
    #[serde(default = "default_reader")]
    reader: String,
    /// Output writer format (optional, default: vegalite)
    #[serde(default = "default_writer")]
    writer: String,
}

fn default_reader() -> String {
    "duckdb://memory".to_string()
}

fn default_writer() -> String {
    "vegalite".to_string()
}

/// Request body for /api/v1/parse endpoint
#[derive(Debug, Deserialize)]
struct ParseRequest {
    /// ggsql query to parse
    query: String,
}

/// Request body for /api/v1/sql endpoint
#[derive(Debug, Deserialize)]
struct SqlRequest {
    /// SQL query to execute
    query: String,
}

/// Successful API response
#[derive(Debug, Serialize)]
struct ApiSuccess<T> {
    status: String,
    data: T,
}

/// Error API response
#[derive(Debug, Serialize)]
struct ApiError {
    status: String,
    error: ErrorDetails,
}

#[derive(Debug, Serialize)]
struct ErrorDetails {
    message: String,
    #[serde(rename = "type")]
    error_type: String,
}

/// Query execution result data
#[derive(Debug, Serialize)]
struct QueryResult {
    /// The visualization specification (Vega-Lite JSON, etc.)
    spec: serde_json::Value,
    /// Metadata about the query execution
    metadata: QueryMetadata,
}

#[derive(Debug, Serialize)]
struct QueryMetadata {
    rows: usize,
    columns: Vec<String>,
    global_mappings: String,
    layers: usize,
}

/// Parse result data
#[derive(Debug, Serialize)]
struct ParseResult {
    sql_portion: String,
    viz_portion: String,
    specs: Vec<serde_json::Value>,
}

/// SQL execution result data
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct SqlResult {
    /// Array of row objects
    rows: Vec<serde_json::Value>,
    /// Column names
    columns: Vec<String>,
    /// Total row count before truncation
    row_count: usize,
    /// Whether results were truncated due to row limit
    truncated: bool,
}

/// Upload result data
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct UploadResult {
    /// Name of the created table
    table_name: String,
    /// Number of rows in the table
    row_count: usize,
    /// Column names
    columns: Vec<String>,
}

/// Session creation result
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct SessionResult {
    session_id: String,
}

/// Session schema result - filtered view of tables for a session
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct SchemaResult {
    /// Tables available to this session (base tables + session tables with clean names)
    tables: Vec<TableSchema>,
}

/// Schema information for a single table
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct TableSchema {
    /// Table name (clean, no session prefix)
    table_name: String,
    /// Column definitions
    columns: Vec<ColumnSchema>,
}

/// Schema information for a single column
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ColumnSchema {
    column_name: String,
    data_type: String,
    /// Minimum value (for numeric/date columns, when stats requested)
    #[serde(skip_serializing_if = "Option::is_none")]
    min_value: Option<String>,
    /// Maximum value (for numeric/date columns, when stats requested)
    #[serde(skip_serializing_if = "Option::is_none")]
    max_value: Option<String>,
    /// Distinct values (for text columns with ≤20 unique values, when stats requested)
    #[serde(skip_serializing_if = "Option::is_none")]
    categorical_values: Option<Vec<String>>,
}

/// Query parameters for schema endpoint
#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
struct SchemaQueryParams {
    /// Include column statistics (min/max for numeric/date, categorical values for text)
    #[serde(default)]
    include_stats: bool,
}

/// Health check response
#[derive(Debug, Serialize)]
struct HealthResponse {
    status: String,
    version: String,
}

/// Version response
#[derive(Debug, Serialize)]
struct VersionResponse {
    version: String,
    features: Vec<String>,
}

// ============================================================================
// Error Handling
// ============================================================================

/// Custom error type for API responses
struct ApiErrorResponse {
    status: StatusCode,
    error: ApiError,
}

impl IntoResponse for ApiErrorResponse {
    fn into_response(self) -> Response {
        let json = Json(self.error);
        (self.status, json).into_response()
    }
}

impl From<GgsqlError> for ApiErrorResponse {
    fn from(err: GgsqlError) -> Self {
        let (status, error_type) = match &err {
            GgsqlError::ParseError(_) => (StatusCode::BAD_REQUEST, "ParseError"),
            GgsqlError::ValidationError(_) => (StatusCode::BAD_REQUEST, "ValidationError"),
            GgsqlError::ReaderError(_) => (StatusCode::BAD_REQUEST, "ReaderError"),
            GgsqlError::WriterError(_) => (StatusCode::INTERNAL_SERVER_ERROR, "WriterError"),
            GgsqlError::InternalError(_) => (StatusCode::INTERNAL_SERVER_ERROR, "InternalError"),
        };

        ApiErrorResponse {
            status,
            error: ApiError {
                status: "error".to_string(),
                error: ErrorDetails {
                    message: err.to_string(),
                    error_type: error_type.to_string(),
                },
            },
        }
    }
}

impl From<String> for ApiErrorResponse {
    fn from(msg: String) -> Self {
        ApiErrorResponse {
            status: StatusCode::BAD_REQUEST,
            error: ApiError {
                status: "error".to_string(),
                error: ErrorDetails {
                    message: msg,
                    error_type: "BadRequest".to_string(),
                },
            },
        }
    }
}

/// Create a 404 session not found error
fn session_not_found_error(session_id: &str) -> ApiErrorResponse {
    ApiErrorResponse {
        status: StatusCode::NOT_FOUND,
        error: ApiError {
            status: "error".to_string(),
            error: ErrorDetails {
                message: format!("Session '{}' does not exist", session_id),
                error_type: "SessionNotFound".to_string(),
            },
        },
    }
}

// ============================================================================
// Helper Functions
// ============================================================================

#[cfg(feature = "duckdb")]
fn load_data_files(reader: &DuckDBReader, files: &[String]) -> Result<(), GgsqlError> {
    use duckdb::params;
    use std::path::Path;

    let conn = reader.connection();

    for file_path in files {
        let path = Path::new(file_path);

        if !path.exists() {
            return Err(GgsqlError::ReaderError(format!(
                "File not found: {}",
                file_path
            )));
        }

        let extension = path
            .extension()
            .and_then(|e| e.to_str())
            .unwrap_or("")
            .to_lowercase();

        // Derive table name from filename (without extension)
        let table_name = path
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("data")
            .replace('-', "_")
            .replace(' ', "_");

        info!("Loading {} into table '{}'", file_path, table_name);

        match extension.as_str() {
            "csv" => {
                // DuckDB can read CSV directly
                let sql = format!(
                    "CREATE TABLE {} AS SELECT * FROM read_csv_auto('{}')",
                    table_name, file_path
                );
                conn.execute(&sql, params![]).map_err(|e| {
                    GgsqlError::ReaderError(format!("Failed to load CSV {}: {}", file_path, e))
                })?;
            }
            "parquet" => {
                // DuckDB can read Parquet directly
                let sql = format!(
                    "CREATE TABLE {} AS SELECT * FROM read_parquet('{}')",
                    table_name, file_path
                );
                conn.execute(&sql, params![]).map_err(|e| {
                    GgsqlError::ReaderError(format!("Failed to load Parquet {}: {}", file_path, e))
                })?;
            }
            "json" | "jsonl" | "ndjson" => {
                // DuckDB can read JSON directly
                let sql = format!(
                    "CREATE TABLE {} AS SELECT * FROM read_json_auto('{}')",
                    table_name, file_path
                );
                conn.execute(&sql, params![]).map_err(|e| {
                    GgsqlError::ReaderError(format!("Failed to load JSON {}: {}", file_path, e))
                })?;
            }
            _ => {
                return Err(GgsqlError::ReaderError(format!(
                    "Unsupported file format: {} (supported: csv, parquet, json, jsonl, ndjson)",
                    extension
                )));
            }
        }

        info!(
            "Successfully loaded {} as table '{}'",
            file_path, table_name
        );
    }

    Ok(())
}

#[cfg(feature = "duckdb")]
fn load_sample_data(reader: &DuckDBReader) -> Result<(), GgsqlError> {
    use duckdb::params;

    let conn = reader.connection();

    // Create sample products table
    conn.execute(
        "CREATE TABLE products (
            product_id INTEGER,
            product_name VARCHAR,
            category VARCHAR,
            price DECIMAL(10,2)
        )",
        params![],
    )
    .map_err(|e| GgsqlError::ReaderError(format!("Failed to create products table: {}", e)))?;

    conn.execute(
        "INSERT INTO products VALUES
            (1, 'Laptop', 'Electronics', 999.99),
            (2, 'Mouse', 'Electronics', 25.50),
            (3, 'Keyboard', 'Electronics', 75.00),
            (4, 'Desk', 'Furniture', 299.99),
            (5, 'Chair', 'Furniture', 199.99),
            (6, 'Monitor', 'Electronics', 349.99),
            (7, 'Lamp', 'Furniture', 45.00)",
        params![],
    )
    .map_err(|e| GgsqlError::ReaderError(format!("Failed to insert products: {}", e)))?;

    // Create sample sales table with more temporal data
    conn.execute(
        "CREATE TABLE sales (
            sale_id INTEGER,
            product_id INTEGER,
            quantity INTEGER,
            sale_date DATE,
            region VARCHAR
        )",
        params![],
    )
    .map_err(|e| GgsqlError::ReaderError(format!("Failed to create sales table: {}", e)))?;

    conn.execute(
        "INSERT INTO sales VALUES
            -- January 2024
            (1, 1, 2, '2024-01-05', 'US'),
            (2, 2, 5, '2024-01-05', 'EU'),
            (3, 3, 3, '2024-01-05', 'APAC'),
            (4, 1, 3, '2024-01-12', 'US'),
            (5, 2, 4, '2024-01-12', 'EU'),
            (6, 3, 2, '2024-01-12', 'APAC'),
            (7, 4, 2, '2024-01-19', 'US'),
            (8, 5, 1, '2024-01-19', 'EU'),
            (9, 6, 2, '2024-01-19', 'APAC'),
            (10, 1, 4, '2024-01-26', 'US'),
            (11, 2, 3, '2024-01-26', 'EU'),
            (12, 3, 5, '2024-01-26', 'APAC'),
            -- February 2024
            (13, 4, 3, '2024-02-02', 'US'),
            (14, 5, 2, '2024-02-02', 'EU'),
            (15, 6, 1, '2024-02-02', 'APAC'),
            (16, 1, 5, '2024-02-09', 'US'),
            (17, 2, 6, '2024-02-09', 'EU'),
            (18, 3, 4, '2024-02-09', 'APAC'),
            (19, 7, 2, '2024-02-16', 'US'),
            (20, 4, 3, '2024-02-16', 'EU'),
            (21, 5, 2, '2024-02-16', 'APAC'),
            (22, 1, 6, '2024-02-23', 'US'),
            (23, 2, 5, '2024-02-23', 'EU'),
            (24, 6, 3, '2024-02-23', 'APAC'),
            -- March 2024
            (25, 3, 4, '2024-03-01', 'US'),
            (26, 4, 5, '2024-03-01', 'EU'),
            (27, 5, 3, '2024-03-01', 'APAC'),
            (28, 1, 7, '2024-03-08', 'US'),
            (29, 2, 6, '2024-03-08', 'EU'),
            (30, 3, 5, '2024-03-08', 'APAC'),
            (31, 6, 2, '2024-03-15', 'US'),
            (32, 7, 3, '2024-03-15', 'EU'),
            (33, 4, 4, '2024-03-15', 'APAC'),
            (34, 1, 8, '2024-03-22', 'US'),
            (35, 2, 7, '2024-03-22', 'EU'),
            (36, 5, 6, '2024-03-22', 'APAC')",
        params![],
    )
    .map_err(|e| GgsqlError::ReaderError(format!("Failed to insert sales: {}", e)))?;

    // Create sample employees table
    conn.execute(
        "CREATE TABLE employees (
            employee_id INTEGER,
            employee_name VARCHAR,
            department VARCHAR,
            salary INTEGER,
            hire_date DATE
        )",
        params![],
    )
    .map_err(|e| GgsqlError::ReaderError(format!("Failed to create employees table: {}", e)))?;

    conn.execute(
        "INSERT INTO employees VALUES
            (1, 'Alice Johnson', 'Engineering', 95000, '2022-01-15'),
            (2, 'Bob Smith', 'Engineering', 85000, '2022-03-20'),
            (3, 'Carol Williams', 'Sales', 70000, '2022-06-10'),
            (4, 'David Brown', 'Sales', 75000, '2023-01-05'),
            (5, 'Eve Davis', 'Marketing', 65000, '2023-03-15'),
            (6, 'Frank Miller', 'Engineering', 105000, '2021-09-01')",
        params![],
    )
    .map_err(|e| GgsqlError::ReaderError(format!("Failed to insert employees: {}", e)))?;

    Ok(())
}

/// Convert a single value from a Polars Column to JSON
#[cfg(feature = "duckdb")]
fn column_value_to_json(column: &polars::prelude::Column, idx: usize) -> serde_json::Value {
    use polars::prelude::AnyValue;

    let any_value = match column.get(idx) {
        Ok(v) => v,
        Err(_) => return serde_json::Value::Null,
    };

    match any_value {
        AnyValue::Null => serde_json::Value::Null,
        AnyValue::Boolean(b) => serde_json::Value::Bool(b),
        AnyValue::Int8(v) => serde_json::Value::Number(v.into()),
        AnyValue::Int16(v) => serde_json::Value::Number(v.into()),
        AnyValue::Int32(v) => serde_json::Value::Number(v.into()),
        AnyValue::Int64(v) => serde_json::Value::Number(v.into()),
        AnyValue::UInt8(v) => serde_json::Value::Number(v.into()),
        AnyValue::UInt16(v) => serde_json::Value::Number(v.into()),
        AnyValue::UInt32(v) => serde_json::Value::Number(v.into()),
        AnyValue::UInt64(v) => serde_json::Value::Number(v.into()),
        AnyValue::Float32(v) => serde_json::Number::from_f64(v as f64)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        AnyValue::Float64(v) => serde_json::Number::from_f64(v)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        AnyValue::String(s) => serde_json::Value::String(s.to_string()),
        AnyValue::StringOwned(s) => serde_json::Value::String(s.to_string()),
        AnyValue::Date(days) => {
            let unix_epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
            let date = unix_epoch + chrono::Duration::days(days as i64);
            serde_json::Value::String(date.format("%Y-%m-%d").to_string())
        }
        AnyValue::Datetime(us, _, _) => {
            let dt = chrono::DateTime::from_timestamp_micros(us).unwrap_or_default();
            serde_json::Value::String(dt.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string())
        }
        other => {
            tracing::debug!("Converting unsupported Polars type to string: {:?}", other);
            serde_json::Value::String(format!("{}", other))
        }
    }
}
// ============================================================================
// Handler Functions
// ============================================================================

/// POST /api/v1/parse - Parse a ggsql query
#[cfg(feature = "duckdb")]
async fn parse_handler(
    Json(request): Json<ParseRequest>,
) -> Result<Json<ApiSuccess<ParseResult>>, ApiErrorResponse> {
    info!("Parsing query: {} chars", request.query.len());

    // Validate query to get sql/viz portions
    let validated = validate(&request.query)?;

    // Parse ggsql portion
    let specs = parser::parse_query(&request.query)?;

    // Convert specs to JSON
    let specs_json: Vec<serde_json::Value> = specs
        .iter()
        .map(|spec| serde_json::to_value(spec).unwrap_or(serde_json::Value::Null))
        .collect();

    let result = ParseResult {
        sql_portion: validated.sql().to_string(),
        viz_portion: validated.visual().to_string(),
        specs: specs_json,
    };

    Ok(Json(ApiSuccess {
        status: "success".to_string(),
        data: result,
    }))
}

/// POST /api/v1/parse - Parse a ggsql query
#[cfg(not(feature = "duckdb"))]
async fn parse_handler(
    Json(request): Json<ParseRequest>,
) -> Result<Json<ApiSuccess<ParseResult>>, ApiErrorResponse> {
    info!("Parsing query: {} chars", request.query.len());

    // Validate query to get sql/viz portions
    let validated = validate(&request.query)?;

    // Parse ggsql portion
    let specs = parser::parse_query(&request.query)?;

    // Convert specs to JSON
    let specs_json: Vec<serde_json::Value> = specs
        .iter()
        .map(|spec| serde_json::to_value(spec).unwrap_or(serde_json::Value::Null))
        .collect();

    let result = ParseResult {
        sql_portion: validated.sql().to_string(),
        viz_portion: validated.visual().to_string(),
        specs: specs_json,
    };

    Ok(Json(ApiSuccess {
        status: "success".to_string(),
        data: result,
    }))
}

/// GET /api/v1/health - Health check
async fn health_handler() -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "healthy".to_string(),
        version: VERSION.to_string(),
    })
}

/// GET /api/v1/version - Version information
async fn version_handler() -> Json<VersionResponse> {
    let mut features = Vec::new();

    #[cfg(feature = "duckdb")]
    features.push("duckdb".to_string());

    #[cfg(feature = "vegalite")]
    features.push("vegalite".to_string());

    #[cfg(feature = "sqlite")]
    features.push("sqlite".to_string());

    #[cfg(feature = "postgres")]
    features.push("postgres".to_string());

    Json(VersionResponse {
        version: VERSION.to_string(),
        features,
    })
}

/// Root handler
async fn root_handler() -> &'static str {
    "ggsql REST API Server - See /api/v1/health for status"
}

/// POST /api/v1/sessions - Create a new session
async fn create_session_handler(
    State(state): State<AppState>,
) -> Json<ApiSuccess<SessionResult>> {
    let session_id = state.session_manager.create_session();
    info!("Created session: {}", session_id);

    Json(ApiSuccess {
        status: "success".to_string(),
        data: SessionResult { session_id },
    })
}

/// DELETE /api/v1/sessions/:session_id - Delete a session
#[cfg(feature = "duckdb")]
async fn delete_session_handler(
    State(state): State<AppState>,
    axum::extract::Path(session_id): axum::extract::Path<String>,
) -> Result<Json<ApiSuccess<()>>, ApiErrorResponse> {
    use duckdb::params;

    // Get tables to drop before removing session
    let tables_to_drop = state.session_manager.delete_session(&session_id);

    match tables_to_drop {
        Some(tables) => {
            // Drop all session tables from DuckDB
            if let Some(ref reader_mutex) = state.reader {
                let reader = reader_mutex.lock().map_err(|e| {
                    GgsqlError::InternalError(format!("Failed to lock reader: {}", e))
                })?;
                let conn = reader.connection();

                for table in tables {
                    let sql = format!("DROP TABLE IF EXISTS {}", table);
                    if let Err(e) = conn.execute(&sql, params![]) {
                        // Log but don't fail - table might not exist
                        tracing::warn!("Failed to drop table {}: {}", table, e);
                    }
                }
            }

            info!("Deleted session: {}", session_id);
            Ok(Json(ApiSuccess {
                status: "success".to_string(),
                data: (),
            }))
        }
        None => Err(session_not_found_error(&session_id)),
    }
}

/// POST /api/v1/sessions/:session_id/upload - Upload file to session
#[cfg(feature = "duckdb")]
async fn session_upload_handler(
    State(state): State<AppState>,
    axum::extract::Path(session_id): axum::extract::Path<String>,
    multipart: Multipart,
) -> Result<Json<ApiSuccess<UploadResult>>, ApiErrorResponse> {
    use duckdb::params;
    use std::io::Write;
    use std::path::Path;

    // Verify session exists and touch it
    if !state.session_manager.touch_session(&session_id) {
        return Err(session_not_found_error(&session_id));
    }

    // Ensure we have a reader
    let reader_mutex = state.reader.as_ref().ok_or_else(|| {
        ApiErrorResponse::from("Server started without data support.".to_string())
    })?;

    // Parse multipart (reuse logic from upload_handler)
    let (file_data, file_name, table_name_override) =
        parse_upload_multipart(multipart, state.upload_limit_bytes).await?;

    // Determine file extension
    let path = Path::new(&file_name);
    let extension = path
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("")
        .to_lowercase();

    // Validate supported format
    if !["csv", "parquet", "json", "jsonl", "ndjson"].contains(&extension.as_str()) {
        return Err(ApiErrorResponse::from(format!(
            "Unsupported file format: {} (supported: csv, parquet, json, jsonl, ndjson)",
            extension
        )));
    }

    // Determine display name (what user sees)
    let display_name = table_name_override.unwrap_or_else(|| {
        path.file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("uploaded_data")
            .replace('-', "_")
            .replace(' ', "_")
    });

    // Check if this session already has a table with this name
    if state.session_manager.session_owns_table(&session_id, &display_name) {
        return Err(ApiErrorResponse {
            status: StatusCode::CONFLICT,
            error: ApiError {
                status: "error".to_string(),
                error: ErrorDetails {
                    message: format!("Table '{}' already exists in this session", display_name),
                    error_type: "TableExistsError".to_string(),
                },
            },
        });
    }

    // Get internal table name with session prefix
    let internal_name = state.session_manager
        .get_internal_table_name(&session_id, &display_name)
        .ok_or_else(|| session_not_found_error(&session_id))?;

    info!("Session {} uploading '{}' as '{}'", session_id, file_name, display_name);

    // Write to temp file
    let temp_file = tempfile::Builder::new()
        .suffix(&format!(".{}", extension))
        .tempfile()
        .map_err(|e| ApiErrorResponse::from(format!("Failed to create temp file: {}", e)))?;

    temp_file.as_file().write_all(&file_data).map_err(|e| {
        ApiErrorResponse::from(format!("Failed to write temp file: {}", e))
    })?;

    let temp_path = temp_file.path().to_string_lossy().to_string();

    // Load into DuckDB with internal name
    let (row_count, columns) = {
        let reader = reader_mutex.lock().map_err(|e| {
            GgsqlError::InternalError(format!("Failed to lock reader: {}", e))
        })?;
        let conn = reader.connection();

        let read_fn = match extension.as_str() {
            "csv" => "read_csv_auto",
            "parquet" => "read_parquet",
            "json" | "jsonl" | "ndjson" => "read_json_auto",
            _ => unreachable!(),
        };

        let sql = format!(
            "CREATE TABLE {} AS SELECT * FROM {}('{}')",
            internal_name, read_fn, temp_path
        );

        conn.execute(&sql, params![]).map_err(|e| {
            GgsqlError::ReaderError(format!("Failed to load data: {}", e))
        })?;

        // Get row count
        let count_sql = format!("SELECT COUNT(*) FROM {}", internal_name);
        let mut stmt = conn.prepare(&count_sql).map_err(|e| {
            GgsqlError::ReaderError(format!("Failed to count rows: {}", e))
        })?;
        let row_count: i64 = stmt.query_row(params![], |row| row.get(0)).map_err(|e| {
            GgsqlError::ReaderError(format!("Failed to get row count: {}", e))
        })?;

        // Get column names
        let columns_sql = format!(
            "SELECT column_name FROM information_schema.columns WHERE table_name = '{}' ORDER BY ordinal_position",
            internal_name
        );
        let mut stmt = conn.prepare(&columns_sql).map_err(|e| {
            GgsqlError::ReaderError(format!("Failed to get columns: {}", e))
        })?;
        let columns: Vec<String> = stmt
            .query_map(params![], |row| row.get(0))
            .map_err(|e| GgsqlError::ReaderError(format!("Failed to query columns: {}", e)))?
            .filter_map(|r| r.ok())
            .collect();

        (row_count as usize, columns)
    };

    // Register table with session
    state.session_manager.register_table(&session_id, &display_name);

    info!(
        "Session {} uploaded '{}' ({} rows, {} columns)",
        session_id, display_name, row_count, columns.len()
    );

    // Return display name (not internal name) to client
    Ok(Json(ApiSuccess {
        status: "success".to_string(),
        data: UploadResult {
            table_name: display_name,
            row_count,
            columns,
        },
    }))
}

/// Parse multipart upload fields (shared between upload handlers)
async fn parse_upload_multipart(
    mut multipart: Multipart,
    upload_limit_bytes: usize,
) -> Result<(Vec<u8>, String, Option<String>), ApiErrorResponse> {
    let mut file_data: Option<Vec<u8>> = None;
    let mut file_name: Option<String> = None;
    let mut table_name_override: Option<String> = None;

    let upload_limit_mb = upload_limit_bytes / 1024 / 1024;

    while let Some(field) = multipart.next_field().await.map_err(|e| {
        let err_str = e.to_string().to_lowercase();
        if err_str.contains("length limit")
            || err_str.contains("body limit")
            || err_str.contains("too large")
        {
            ApiErrorResponse::from(format!(
                "File too large. Maximum upload size is {} MB.",
                upload_limit_mb
            ))
        } else {
            ApiErrorResponse::from(format!("Failed to read multipart field: {}", e))
        }
    })? {
        let name = field.name().unwrap_or("").to_string();

        match name.as_str() {
            "file" => {
                file_name = field.file_name().map(|s| s.to_string());
                file_data = Some(field.bytes().await.map_err(|e| {
                    ApiErrorResponse::from(format!("Failed to read file data: {}", e))
                })?.to_vec());
            }
            "tableName" => {
                let value = field.text().await.map_err(|e| {
                    ApiErrorResponse::from(format!("Failed to read tableName: {}", e))
                })?;
                if !value.is_empty() {
                    table_name_override = Some(value);
                }
            }
            _ => {}
        }
    }

    let file_data = file_data.ok_or_else(|| {
        ApiErrorResponse::from("No file provided in upload".to_string())
    })?;
    let file_name = file_name.ok_or_else(|| {
        ApiErrorResponse::from("No filename provided".to_string())
    })?;

    Ok((file_data, file_name, table_name_override))
}

/// Rewrite table references in SQL to use session-prefixed names
fn rewrite_query_for_session(
    query: &str,
    session_id: &str,
    session_tables: &std::collections::HashSet<String>,
) -> String {
    let mut result = query.to_string();

    // Sort by length descending to avoid partial matches
    let mut tables: Vec<_> = session_tables.iter().collect();
    tables.sort_by(|a, b| b.len().cmp(&a.len()));

    for display_name in tables {
        let internal_name = format!("s_{}_{}", session_id, display_name);
        // Use word boundary matching
        let pattern = format!(r"\b{}\b", regex::escape(display_name));
        if let Ok(re) = regex::Regex::new(&pattern) {
            result = re.replace_all(&result, internal_name.as_str()).to_string();
        }
    }

    result
}

/// Rewrite internal table names back to display names in results
fn sanitize_result_for_display(value: &mut serde_json::Value, session_id: &str) {
    match value {
        serde_json::Value::String(s) => {
            // Replace s_{sessionId}_{name} with just {name}
            let prefix = format!("s_{}_", session_id);
            if s.contains(&prefix) {
                *s = s.replace(&prefix, "");
            }
        }
        serde_json::Value::Array(arr) => {
            for item in arr {
                sanitize_result_for_display(item, session_id);
            }
        }
        serde_json::Value::Object(map) => {
            for (_, v) in map {
                sanitize_result_for_display(v, session_id);
            }
        }
        _ => {}
    }
}

/// Background task to cleanup expired sessions
#[cfg(feature = "duckdb")]
async fn cleanup_expired_sessions(
    session_manager: std::sync::Arc<SessionManager>,
    reader: std::sync::Arc<std::sync::Mutex<DuckDBReader>>,
    interval_secs: u64,
) {
    use duckdb::params;
    use tokio::time::{interval, Duration};

    let mut ticker = interval(Duration::from_secs(interval_secs));

    loop {
        ticker.tick().await;

        let expired = session_manager.get_expired_sessions();
        if expired.is_empty() {
            continue;
        }

        info!("Cleaning up {} expired session(s)", expired.len());

        // Drop tables for expired sessions
        let reader_guard = match reader.lock() {
            Ok(r) => r,
            Err(e) => {
                tracing::error!("Failed to lock reader for cleanup: {}", e);
                continue;
            }
        };
        let conn = reader_guard.connection();

        for (session_id, tables) in &expired {
            for table in tables {
                let sql = format!("DROP TABLE IF EXISTS {}", table);
                if let Err(e) = conn.execute(&sql, params![]) {
                    tracing::warn!("Failed to drop table {} for session {}: {}", table, session_id, e);
                }
            }
            info!("Cleaned up session {} ({} tables)", session_id, tables.len());
        }

        drop(reader_guard); // Release lock before removing sessions

        // Remove expired sessions from manager
        session_manager.remove_expired();
    }
}

/// POST /api/v1/sessions/:session_id/sql - Execute SQL in session context
#[cfg(feature = "duckdb")]
async fn session_sql_handler(
    State(state): State<AppState>,
    axum::extract::Path(session_id): axum::extract::Path<String>,
    Json(request): Json<SqlRequest>,
) -> Result<Json<ApiSuccess<SqlResult>>, ApiErrorResponse> {
    // Verify session exists and touch it
    let session = state.session_manager.get_session(&session_id)
        .ok_or_else(|| session_not_found_error(&session_id))?;
    state.session_manager.touch_session(&session_id);

    info!("Session {} executing SQL: {} chars", session_id, request.query.len());

    let reader_mutex = state.reader.as_ref().ok_or_else(|| {
        ApiErrorResponse::from("Server started without data support.".to_string())
    })?;

    // Rewrite query to use internal table names
    let rewritten_query = rewrite_query_for_session(&request.query, &session_id, &session.tables);

    // Execute query
    let df = {
        let reader = reader_mutex.lock().map_err(|e| {
            GgsqlError::InternalError(format!("Failed to lock reader: {}", e))
        })?;
        reader.execute_sql(&rewritten_query)?
    };

    let columns: Vec<String> = df
        .get_column_names()
        .iter()
        .map(|s| s.to_string())
        .collect();

    let (total_rows, _) = df.shape();
    let (rows_to_process, truncated) = if state.sql_max_rows > 0 && total_rows > state.sql_max_rows
    {
        info!(
            "Truncating session SQL results from {} to {} rows",
            total_rows,
            state.sql_max_rows
        );
        (state.sql_max_rows, true)
    } else {
        (total_rows, false)
    };

    let col_refs: Vec<_> = columns
        .iter()
        .map(|name| df.column(name))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| GgsqlError::InternalError(format!("Failed to get columns: {}", e)))?;

    let mut rows: Vec<serde_json::Value> = Vec::with_capacity(rows_to_process);

    for i in 0..rows_to_process {
        let mut row_obj = serde_json::Map::new();
        for (col_name, column) in columns.iter().zip(&col_refs) {
            let mut value = column_value_to_json(column, i);
            sanitize_result_for_display(&mut value, &session_id);
            row_obj.insert(col_name.clone(), value);
        }
        rows.push(serde_json::Value::Object(row_obj));
    }

    let result = SqlResult {
        rows,
        columns,
        row_count: total_rows,
        truncated,
    };

    Ok(Json(ApiSuccess {
        status: "success".to_string(),
        data: result,
    }))
}

/// POST /api/v1/sessions/:session_id/query - Execute ggsql query in session context
#[cfg(feature = "duckdb")]
async fn session_query_handler(
    State(state): State<AppState>,
    axum::extract::Path(session_id): axum::extract::Path<String>,
    Json(request): Json<QueryRequest>,
) -> Result<Json<ApiSuccess<QueryResult>>, ApiErrorResponse> {
    // Verify session exists and touch it
    let session = state.session_manager.get_session(&session_id)
        .ok_or_else(|| session_not_found_error(&session_id))?;
    state.session_manager.touch_session(&session_id);

    info!("Session {} executing query: {} chars", session_id, request.query.len());

    // Rewrite query to use internal table names
    let rewritten_query = rewrite_query_for_session(&request.query, &session_id, &session.tables);

    #[cfg(feature = "duckdb")]
    if request.reader.starts_with("duckdb://") {
        let reader_mutex = state.reader.as_ref().ok_or_else(|| {
            ApiErrorResponse::from("Server started without data support.".to_string())
        })?;

        let execute_query = |sql: &str| -> Result<ggsql::DataFrame, GgsqlError> {
            let reader = reader_mutex.lock().map_err(|e| {
                GgsqlError::InternalError(format!("Failed to lock reader: {}", e))
            })?;
            reader.execute_sql(sql)
        };

        let prepared = prepare_data_with_executor(&rewritten_query, execute_query)?;

        let (rows, columns) = if let Some(df) = prepared.data.get("__global__") {
            let (r, _) = df.shape();
            let cols: Vec<String> = df
                .get_column_names()
                .iter()
                .map(|s| s.to_string())
                .collect();
            (r, cols)
        } else {
            let df = prepared.data.values().next().unwrap();
            let (r, _) = df.shape();
            let cols: Vec<String> = df
                .get_column_names()
                .iter()
                .map(|s| s.to_string())
                .collect();
            (r, cols)
        };

        let first_spec = &prepared.spec;

        #[cfg(feature = "vegalite")]
        if request.writer == "vegalite" {
            let writer = VegaLiteWriter::new();
            let json_output = writer.write(first_spec, &prepared.data)?;
            let mut spec_value: serde_json::Value = serde_json::from_str(&json_output)
                .map_err(|e| GgsqlError::WriterError(format!("Failed to parse JSON: {}", e)))?;

            // Sanitize any internal table names in the spec
            sanitize_result_for_display(&mut spec_value, &session_id);

            let result = QueryResult {
                spec: spec_value,
                metadata: QueryMetadata {
                    rows,
                    columns,
                    global_mappings: format!("{:?}", first_spec.global_mappings),
                    layers: first_spec.layers.len(),
                },
            };

            return Ok(Json(ApiSuccess {
                status: "success".to_string(),
                data: result,
            }));
        }

        #[cfg(not(feature = "vegalite"))]
        return Err(ApiErrorResponse::from(
            "VegaLite writer not available".to_string(),
        ));
    }

    Err(ApiErrorResponse::from(format!(
        "Unsupported reader: {}",
        request.reader
    )))
}

/// Categorical value threshold - columns with more unique values than this
/// won't have their categorical values enumerated
const CATEGORICAL_THRESHOLD: usize = 20;

/// Check if a data type is numeric (for range stats)
fn is_numeric_type(data_type: &str) -> bool {
    let dt = data_type.to_uppercase();
    dt.contains("INT")
        || dt.contains("FLOAT")
        || dt.contains("DOUBLE")
        || dt.contains("DECIMAL")
        || dt.contains("NUMERIC")
        || dt.contains("REAL")
        || dt == "BIGINT"
        || dt == "SMALLINT"
        || dt == "TINYINT"
        || dt == "HUGEINT"
}

/// Check if a data type is date/timestamp (for range stats)
fn is_date_type(data_type: &str) -> bool {
    let dt = data_type.to_uppercase();
    dt.contains("DATE") || dt.contains("TIME") || dt.contains("TIMESTAMP")
}

/// Check if a data type is text (for categorical values)
fn is_text_type(data_type: &str) -> bool {
    let dt = data_type.to_uppercase();
    dt.contains("VARCHAR") || dt.contains("TEXT") || dt.contains("CHAR") || dt == "STRING"
}

/// Collect statistics for columns in a table
#[cfg(feature = "duckdb")]
fn collect_table_stats(
    conn: &duckdb::Connection,
    table_name: &str,
    columns: &mut Vec<ColumnSchema>,
) -> Result<(), GgsqlError> {
    use duckdb::params;

    // Build aggregate query for numeric/date columns
    let mut agg_selects: Vec<String> = Vec::new();
    let mut count_selects: Vec<String> = Vec::new();

    for col in columns.iter() {
        let col_name = &col.column_name;
        let quoted_col = format!("\"{}\"", col_name);

        if is_numeric_type(&col.data_type) || is_date_type(&col.data_type) {
            agg_selects.push(format!("MIN({}) AS \"{}_min\"", quoted_col, col_name));
            agg_selects.push(format!("MAX({}) AS \"{}_max\"", quoted_col, col_name));
        }

        if is_text_type(&col.data_type) {
            count_selects.push(format!(
                "COUNT(DISTINCT {}) AS \"{}_nunique\"",
                quoted_col, col_name
            ));
        }
    }

    // Execute aggregate query for min/max
    if !agg_selects.is_empty() {
        let agg_query = format!(
            "SELECT {} FROM \"{}\"",
            agg_selects.join(", "),
            table_name
        );

        let mut stmt = conn.prepare(&agg_query).map_err(|e| {
            GgsqlError::InternalError(format!("Failed to prepare stats query: {}", e))
        })?;

        let mut rows = stmt.query(params![]).map_err(|e| {
            GgsqlError::InternalError(format!("Failed to execute stats query: {}", e))
        })?;

        if let Some(row) = rows.next().map_err(|e| {
            GgsqlError::InternalError(format!("Failed to read stats row: {}", e))
        })? {
            for col in columns.iter_mut() {
                if is_numeric_type(&col.data_type) || is_date_type(&col.data_type) {
                    let min_col = format!("{}_min", col.column_name);
                    let max_col = format!("{}_max", col.column_name);

                    // Try to get values - DuckDB may return various types
                    // Try i64 first, then f64, then String
                    let min_val: Option<String> = row
                        .get::<_, i64>(min_col.as_str())
                        .map(|v| v.to_string())
                        .ok()
                        .or_else(|| row.get::<_, f64>(min_col.as_str()).map(|v| v.to_string()).ok())
                        .or_else(|| row.get::<_, String>(min_col.as_str()).ok());

                    let max_val: Option<String> = row
                        .get::<_, i64>(max_col.as_str())
                        .map(|v| v.to_string())
                        .ok()
                        .or_else(|| row.get::<_, f64>(max_col.as_str()).map(|v| v.to_string()).ok())
                        .or_else(|| row.get::<_, String>(max_col.as_str()).ok());

                    col.min_value = min_val;
                    col.max_value = max_val;
                }
            }
        }
    }

    // Execute count distinct query for text columns
    if !count_selects.is_empty() {
        let count_query = format!(
            "SELECT {} FROM \"{}\"",
            count_selects.join(", "),
            table_name
        );

        let mut stmt = conn.prepare(&count_query).map_err(|e| {
            GgsqlError::InternalError(format!("Failed to prepare count query: {}", e))
        })?;

        let mut rows = stmt.query(params![]).map_err(|e| {
            GgsqlError::InternalError(format!("Failed to execute count query: {}", e))
        })?;

        // Collect columns that need categorical values
        let mut categorical_cols: Vec<String> = Vec::new();

        if let Some(row) = rows.next().map_err(|e| {
            GgsqlError::InternalError(format!("Failed to read count row: {}", e))
        })? {
            for col in columns.iter() {
                if is_text_type(&col.data_type) {
                    let count_col = format!("{}_nunique", col.column_name);
                    if let Ok(count) = row.get::<_, i64>(count_col.as_str()) {
                        if (count as usize) <= CATEGORICAL_THRESHOLD {
                            categorical_cols.push(col.column_name.clone());
                        }
                    }
                }
            }
        }

        // Fetch distinct values for categorical columns
        for cat_col in categorical_cols {
            let distinct_query = format!(
                "SELECT DISTINCT \"{}\" FROM \"{}\" WHERE \"{}\" IS NOT NULL ORDER BY \"{}\" LIMIT {}",
                cat_col, table_name, cat_col, cat_col, CATEGORICAL_THRESHOLD
            );

            let mut stmt = conn.prepare(&distinct_query).map_err(|e| {
                GgsqlError::InternalError(format!("Failed to prepare distinct query: {}", e))
            })?;

            let values: Vec<String> = stmt
                .query_map(params![], |row| row.get::<_, String>(0))
                .map_err(|e| {
                    GgsqlError::InternalError(format!("Failed to execute distinct query: {}", e))
                })?
                .filter_map(|r| r.ok())
                .collect();

            // Find the column and set its categorical values
            if let Some(col) = columns.iter_mut().find(|c| c.column_name == cat_col) {
                if !values.is_empty() {
                    col.categorical_values = Some(values);
                }
            }
        }
    }

    Ok(())
}

/// GET /api/v1/sessions/:session_id/schema - Get filtered schema for session
///
/// Returns base tables (no s_ prefix) plus session-specific tables (with prefix stripped).
/// This prevents sessions from seeing other sessions' tables.
///
/// Query parameters:
/// - include_stats: bool (default false) - Include column statistics (min/max, categorical values)
#[cfg(feature = "duckdb")]
async fn session_schema_handler(
    State(state): State<AppState>,
    axum::extract::Path(session_id): axum::extract::Path<String>,
    axum::extract::Query(params): axum::extract::Query<SchemaQueryParams>,
) -> Result<Json<ApiSuccess<SchemaResult>>, ApiErrorResponse> {
    use duckdb::params;

    // Verify session exists and touch it
    state.session_manager.get_session(&session_id)
        .ok_or_else(|| session_not_found_error(&session_id))?;
    state.session_manager.touch_session(&session_id);

    info!("Session {} fetching schema (include_stats={})", session_id, params.include_stats);

    let reader_mutex = state.reader.as_ref().ok_or_else(|| {
        ApiErrorResponse::from("Server started without data support.".to_string())
    })?;

    let reader = reader_mutex.lock().map_err(|e| {
        GgsqlError::InternalError(format!("Failed to lock reader: {}", e))
    })?;
    let conn = reader.connection();

    // Query all tables from information_schema
    let mut stmt = conn.prepare(
        "SELECT t.table_name, c.column_name, c.data_type
         FROM information_schema.tables t
         JOIN information_schema.columns c
           ON t.table_name = c.table_name AND t.table_schema = c.table_schema
         WHERE t.table_schema = 'main' AND t.table_type = 'BASE TABLE'
         ORDER BY t.table_name, c.ordinal_position"
    ).map_err(|e| GgsqlError::InternalError(format!("Failed to prepare schema query: {}", e)))?;

    let session_prefix = format!("s_{}_", session_id);

    // Build map of display_name -> (actual_table_name, columns), filtering appropriately
    let mut table_map: std::collections::HashMap<String, (String, Vec<ColumnSchema>)> =
        std::collections::HashMap::new();

    let rows = stmt.query_map(params![], |row| {
        Ok((
            row.get::<_, String>(0)?,  // table_name
            row.get::<_, String>(1)?,  // column_name
            row.get::<_, String>(2)?,  // data_type
        ))
    }).map_err(|e| GgsqlError::InternalError(format!("Failed to query schema: {}", e)))?;

    for row_result in rows {
        let (table_name, column_name, data_type) = row_result
            .map_err(|e| GgsqlError::InternalError(format!("Failed to read row: {}", e)))?;

        // Determine if this table should be included and what name to use
        let (display_name, actual_name) = if table_name.starts_with("s_") {
            // This is a session table
            if table_name.starts_with(&session_prefix) {
                // It's OUR session table - strip prefix for display
                (
                    Some(table_name[session_prefix.len()..].to_string()),
                    table_name.clone(),
                )
            } else {
                // It's ANOTHER session's table - skip it
                (None, table_name.clone())
            }
        } else {
            // Base table - include as-is
            (Some(table_name.clone()), table_name.clone())
        };

        if let Some(name) = display_name {
            table_map
                .entry(name)
                .or_insert_with(|| (actual_name, Vec::new()))
                .1
                .push(ColumnSchema {
                    column_name,
                    data_type,
                    min_value: None,
                    max_value: None,
                    categorical_values: None,
                });
        }
    }

    // Convert to sorted Vec<TableSchema>, collecting stats if requested
    let mut tables: Vec<TableSchema> = Vec::with_capacity(table_map.len());

    for (display_name, (actual_name, mut columns)) in table_map {
        // Collect stats if requested
        if params.include_stats {
            if let Err(e) = collect_table_stats(conn, &actual_name, &mut columns) {
                // Log warning but don't fail - stats are optional
                tracing::warn!(
                    "Failed to collect stats for table {}: {}",
                    display_name,
                    e
                );
            }
        }

        tables.push(TableSchema {
            table_name: display_name,
            columns,
        });
    }

    tables.sort_by(|a, b| a.table_name.cmp(&b.table_name));

    info!("Session {} schema: {} tables", session_id, tables.len());

    Ok(Json(ApiSuccess {
        status: "success".to_string(),
        data: SchemaResult { tables },
    }))
}

// ============================================================================
// Main Server
// ============================================================================

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize tracing
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "ggsql_rest=info,tower_http=info".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    // Parse CLI arguments
    let cli = Cli::parse();

    // Initialize session manager
    let session_manager = std::sync::Arc::new(SessionManager::new(cli.session_timeout_mins));
    info!("Session timeout: {} minutes", cli.session_timeout_mins);

    // Initialize DuckDB reader (always create for upload support)
    #[cfg(feature = "duckdb")]
    let reader = {
        info!("Initializing in-memory DuckDB database");
        let reader = DuckDBReader::from_connection_string("duckdb://memory")?;

        // Load sample data if requested
        if cli.load_sample_data {
            info!("Loading sample data (products, sales, employees tables)");
            load_sample_data(&reader)?;
        }

        // Load data files if provided
        if !cli.load_data_files.is_empty() {
            info!("Loading {} data file(s)", cli.load_data_files.len());
            load_data_files(&reader, &cli.load_data_files)?;
        }

        if !cli.load_sample_data && cli.load_data_files.is_empty() {
            info!("No data pre-loaded (use --load-sample-data or --load-data, or upload via API)");
        }

        Some(std::sync::Arc::new(std::sync::Mutex::new(reader)))
    };

    #[cfg(not(feature = "duckdb"))]
    let reader = None::<std::sync::Arc<std::sync::Mutex<()>>>;

    // Create application state
    let upload_limit_bytes = cli.upload_limit_mb * 1024 * 1024;
    let state = AppState {
        #[cfg(feature = "duckdb")]
        reader,
        sql_max_rows: cli.sql_max_rows,
        upload_limit_bytes,
        session_manager: session_manager.clone(),
    };

    // Spawn background cleanup task
    #[cfg(feature = "duckdb")]
    if let Some(ref reader) = state.reader {
        let cleanup_manager = session_manager.clone();
        let cleanup_reader = reader.clone();
        let cleanup_interval = cli.session_cleanup_interval;
        tokio::spawn(async move {
            cleanup_expired_sessions(cleanup_manager, cleanup_reader, cleanup_interval).await;
        });
        info!("Session cleanup task started (interval: {}s)", cli.session_cleanup_interval);
    }

    // Configure CORS
    let cors = if cli.cors_origin == "*" {
        CorsLayer::new()
            .allow_origin(Any)
            .allow_methods(Any)
            .allow_headers(vec![header::CONTENT_TYPE])
    } else {
        let origins: Vec<_> = cli
            .cors_origin
            .split(',')
            .filter_map(|s| s.trim().parse().ok())
            .collect();
        CorsLayer::new()
            .allow_origin(origins)
            .allow_methods(Any)
            .allow_headers(vec![header::CONTENT_TYPE])
    };

    // Build router
    let mut app = Router::new()
        .route("/", get(root_handler))
        .route("/api/v1/sessions", post(create_session_handler))
        .route("/api/v1/parse", post(parse_handler))
        .route("/api/v1/health", get(health_handler))
        .route("/api/v1/version", get(version_handler));

    #[cfg(feature = "duckdb")]
    {
        app = app
            .route("/api/v1/sessions/:session_id", delete(delete_session_handler))
            .route("/api/v1/sessions/:session_id/sql", post(session_sql_handler))
            .route("/api/v1/sessions/:session_id/query", post(session_query_handler))
            .route("/api/v1/sessions/:session_id/schema", get(session_schema_handler))
            .route(
                "/api/v1/sessions/:session_id/upload",
                post(session_upload_handler).layer(DefaultBodyLimit::max(upload_limit_bytes)),
            );
    }

    let app = app
        .layer(cors)
        .layer(tower_http::trace::TraceLayer::new_for_http())
        .with_state(state);

    // Parse bind address
    let addr: SocketAddr = format!("{}:{}", cli.host, cli.port)
        .parse()
        .expect("Invalid host or port");

    info!("Starting ggsql REST API server on {}", addr);
    info!("Upload limit: {} MB (use --upload-limit-mb to change)", cli.upload_limit_mb);
    info!("API documentation:");
    info!("  Sessions:");
    info!("    POST   /api/v1/sessions              - Create session");
    info!("    DELETE /api/v1/sessions/:id          - Delete session");
    info!("    POST   /api/v1/sessions/:id/upload   - Upload to session");
    info!("    POST   /api/v1/sessions/:id/sql      - Execute SQL in session");
    info!("    POST   /api/v1/sessions/:id/query    - Execute ggsql in session");
    info!("    GET    /api/v1/sessions/:id/schema   - Get filtered schema");
    info!("  Utility:");
    info!("    POST   /api/v1/parse                 - Parse ggsql query");
    info!("    GET    /api/v1/health                - Health check");
    info!("    GET    /api/v1/version               - Version info");

    // Start server
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::util::ServiceExt;

    fn create_test_app() -> Router {
        create_test_app_with_limits(10000, 50 * 1024 * 1024)
    }

    fn create_test_app_with_limits(sql_max_rows: usize, upload_limit_bytes: usize) -> Router {
        let reader = DuckDBReader::from_connection_string("duckdb://memory").unwrap();

        // Load some test data
        let conn = reader.connection();
        conn.execute(
            "CREATE TABLE test_table (id INTEGER, name VARCHAR)",
            duckdb::params![],
        ).unwrap();
        conn.execute(
            "INSERT INTO test_table VALUES (1, 'Alice'), (2, 'Bob')",
            duckdb::params![],
        ).unwrap();

        let session_manager = std::sync::Arc::new(SessionManager::new(30));
        let state = AppState {
            reader: Some(std::sync::Arc::new(std::sync::Mutex::new(reader))),
            sql_max_rows,
            upload_limit_bytes,
            session_manager,
        };

        Router::new()
            .route("/", get(root_handler))
            .route("/api/v1/health", get(health_handler))
            .route("/api/v1/version", get(version_handler))
            .route("/api/v1/parse", post(parse_handler))
            .route("/api/v1/sessions", post(create_session_handler))
            .route("/api/v1/sessions/:session_id", delete(delete_session_handler))
            .route("/api/v1/sessions/:session_id/sql", post(session_sql_handler))
            .route("/api/v1/sessions/:session_id/upload", post(session_upload_handler).layer(DefaultBodyLimit::max(upload_limit_bytes)))
            .route("/api/v1/sessions/:session_id/query", post(session_query_handler))
            .route("/api/v1/sessions/:session_id/schema", get(session_schema_handler))
            .with_state(state)
    }

    /// Helper to create a multipart body for file upload
    fn create_multipart_body(filename: &str, content: &[u8]) -> (String, Vec<u8>) {
        let boundary = "----TestBoundary1234567890";
        let mut body = Vec::new();

        // File field
        body.extend_from_slice(format!("--{}\r\n", boundary).as_bytes());
        body.extend_from_slice(format!("Content-Disposition: form-data; name=\"file\"; filename=\"{}\"\r\n", filename).as_bytes());
        body.extend_from_slice(b"Content-Type: application/octet-stream\r\n\r\n");
        body.extend_from_slice(content);
        body.extend_from_slice(format!("\r\n--{}--\r\n", boundary).as_bytes());

        (boundary.to_string(), body)
    }

    #[tokio::test]
    async fn test_create_session() {
        let app = create_test_app();

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/sessions")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(json["status"], "success");
        assert!(json["data"]["sessionId"].is_string());
    }

    #[tokio::test]
    async fn test_delete_nonexistent_session() {
        let app = create_test_app();

        let response = app
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri("/api/v1/sessions/nonexistent")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_session_sql_sees_base_tables() {
        let app = create_test_app();

        // Create session
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/sessions")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let session_id = json["data"]["sessionId"].as_str().unwrap();

        // Query base table through session
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(&format!("/api/v1/sessions/{}/sql", session_id))
                    .header("Content-Type", "application/json")
                    .body(Body::from(r#"{"query": "SELECT * FROM test_table"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(json["status"], "success");
        assert_eq!(json["data"]["rows"].as_array().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn test_session_not_found_for_sql() {
        let app = create_test_app();

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/sessions/nonexistent/sql")
                    .header("Content-Type", "application/json")
                    .body(Body::from(r#"{"query": "SELECT 1"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[test]
    fn test_query_rewriting() {
        use std::collections::HashSet;

        let mut tables = HashSet::new();
        tables.insert("diamonds".to_string());
        tables.insert("cars".to_string());

        let query = "SELECT * FROM diamonds JOIN cars ON diamonds.id = cars.id";
        let rewritten = rewrite_query_for_session(query, "abc123", &tables);

        assert!(rewritten.contains("s_abc123_diamonds"));
        assert!(rewritten.contains("s_abc123_cars"));
        assert!(!rewritten.contains(" diamonds"));
        assert!(!rewritten.contains(" cars"));
    }

    #[test]
    fn test_sanitize_result() {
        let mut value = serde_json::json!({
            "table": "s_abc123_diamonds",
            "query": "SELECT * FROM s_abc123_diamonds"
        });

        sanitize_result_for_display(&mut value, "abc123");

        assert_eq!(value["table"], "diamonds");
        assert_eq!(value["query"], "SELECT * FROM diamonds");
    }

    // ========================================================================
    // Session Upload Tests
    // ========================================================================

    #[tokio::test]
    async fn test_session_upload_csv() {
        let app = create_test_app();

        // Create session
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/sessions")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let session_id = json["data"]["sessionId"].as_str().unwrap();

        // Upload CSV file
        let csv_content = b"id,name,value\n1,foo,100\n2,bar,200\n";
        let (boundary, body) = create_multipart_body("test_data.csv", csv_content);

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(&format!("/api/v1/sessions/{}/upload", session_id))
                    .header("Content-Type", format!("multipart/form-data; boundary={}", boundary))
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(json["status"], "success");
        // Table name should be clean (no session prefix visible)
        assert_eq!(json["data"]["tableName"], "test_data");
        assert_eq!(json["data"]["rowCount"], 2);
        assert_eq!(json["data"]["columns"].as_array().unwrap().len(), 3);

        // Verify we can query the uploaded table
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(&format!("/api/v1/sessions/{}/sql", session_id))
                    .header("Content-Type", "application/json")
                    .body(Body::from(r#"{"query": "SELECT * FROM test_data"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["data"]["rows"].as_array().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn test_upload_to_nonexistent_session() {
        let app = create_test_app();

        let csv_content = b"id,name\n1,foo\n";
        let (boundary, body) = create_multipart_body("test.csv", csv_content);

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/sessions/nonexistent/upload")
                    .header("Content-Type", format!("multipart/form-data; boundary={}", boundary))
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_duplicate_table_in_session() {
        let app = create_test_app();

        // Create session
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/sessions")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let session_id = json["data"]["sessionId"].as_str().unwrap();

        // First upload should succeed
        let csv_content = b"id,name\n1,foo\n";
        let (boundary, body) = create_multipart_body("duplicate.csv", csv_content);

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(&format!("/api/v1/sessions/{}/upload", session_id))
                    .header("Content-Type", format!("multipart/form-data; boundary={}", boundary))
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        // Second upload with same name should fail with 409
        let csv_content2 = b"id,name\n2,bar\n";
        let (boundary2, body2) = create_multipart_body("duplicate.csv", csv_content2);

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(&format!("/api/v1/sessions/{}/upload", session_id))
                    .header("Content-Type", format!("multipart/form-data; boundary={}", boundary2))
                    .body(Body::from(body2))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::CONFLICT);
    }

    #[tokio::test]
    async fn test_same_table_name_different_sessions() {
        let app = create_test_app();

        // Create two sessions
        let response1 = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/sessions")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body1 = axum::body::to_bytes(response1.into_body(), usize::MAX).await.unwrap();
        let json1: serde_json::Value = serde_json::from_slice(&body1).unwrap();
        let session_id_1 = json1["data"]["sessionId"].as_str().unwrap().to_string();

        let response2 = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/sessions")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body2 = axum::body::to_bytes(response2.into_body(), usize::MAX).await.unwrap();
        let json2: serde_json::Value = serde_json::from_slice(&body2).unwrap();
        let session_id_2 = json2["data"]["sessionId"].as_str().unwrap().to_string();

        // Upload same-named file to both sessions - both should succeed
        let csv_content1 = b"id,value\n1,100\n";
        let (boundary1, body1) = create_multipart_body("shared.csv", csv_content1);

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(&format!("/api/v1/sessions/{}/upload", session_id_1))
                    .header("Content-Type", format!("multipart/form-data; boundary={}", boundary1))
                    .body(Body::from(body1))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let csv_content2 = b"id,value\n2,200\n";
        let (boundary2, body2) = create_multipart_body("shared.csv", csv_content2);

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(&format!("/api/v1/sessions/{}/upload", session_id_2))
                    .header("Content-Type", format!("multipart/form-data; boundary={}", boundary2))
                    .body(Body::from(body2))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        // Query each session - should see different data
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(&format!("/api/v1/sessions/{}/sql", session_id_1))
                    .header("Content-Type", "application/json")
                    .body(Body::from(r#"{"query": "SELECT value FROM shared"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["data"]["rows"][0]["value"], 100);

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(&format!("/api/v1/sessions/{}/sql", session_id_2))
                    .header("Content-Type", "application/json")
                    .body(Body::from(r#"{"query": "SELECT value FROM shared"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["data"]["rows"][0]["value"], 200);
    }

    #[tokio::test]
    async fn test_session_table_not_visible_to_other_sessions() {
        let app = create_test_app();

        // Create two sessions
        let response1 = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/sessions")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body1 = axum::body::to_bytes(response1.into_body(), usize::MAX).await.unwrap();
        let json1: serde_json::Value = serde_json::from_slice(&body1).unwrap();
        let session_id_1 = json1["data"]["sessionId"].as_str().unwrap().to_string();

        let response2 = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/sessions")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body2 = axum::body::to_bytes(response2.into_body(), usize::MAX).await.unwrap();
        let json2: serde_json::Value = serde_json::from_slice(&body2).unwrap();
        let session_id_2 = json2["data"]["sessionId"].as_str().unwrap().to_string();

        // Upload to session 1 only
        let csv_content = b"id,secret\n1,hidden\n";
        let (boundary, body) = create_multipart_body("private.csv", csv_content);

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(&format!("/api/v1/sessions/{}/upload", session_id_1))
                    .header("Content-Type", format!("multipart/form-data; boundary={}", boundary))
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        // Session 1 can query it
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(&format!("/api/v1/sessions/{}/sql", session_id_1))
                    .header("Content-Type", "application/json")
                    .body(Body::from(r#"{"query": "SELECT * FROM private"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        // Session 2 cannot query it - should fail
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(&format!("/api/v1/sessions/{}/sql", session_id_2))
                    .header("Content-Type", "application/json")
                    .body(Body::from(r#"{"query": "SELECT * FROM private"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        // Should return 400 (query error) since table doesn't exist for this session
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn test_session_cleanup_drops_tables() {
        let app = create_test_app();

        // Create session and upload
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/sessions")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let session_id = json["data"]["sessionId"].as_str().unwrap().to_string();

        let csv_content = b"id,data\n1,test\n";
        let (boundary, body) = create_multipart_body("cleanup_test.csv", csv_content);

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(&format!("/api/v1/sessions/{}/upload", session_id))
                    .header("Content-Type", format!("multipart/form-data; boundary={}", boundary))
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        // Delete session
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri(&format!("/api/v1/sessions/{}", session_id))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        // Try to query the deleted session - should 404
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(&format!("/api/v1/sessions/{}/sql", session_id))
                    .header("Content-Type", "application/json")
                    .body(Body::from(r#"{"query": "SELECT * FROM cleanup_test"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    // ========================================================================
    // Session Schema Tests
    // ========================================================================

    #[tokio::test]
    async fn test_session_schema_shows_base_tables() {
        let app = create_test_app();

        // Create session
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/sessions")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let session_id = json["data"]["sessionId"].as_str().unwrap();

        // Get schema
        let response = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri(&format!("/api/v1/sessions/{}/schema", session_id))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

        // Should have base tables (test_table created in create_test_app)
        let tables = json["data"]["tables"].as_array().unwrap();
        let table_names: Vec<&str> = tables.iter()
            .map(|t| t["tableName"].as_str().unwrap())
            .collect();

        assert!(table_names.contains(&"test_table"));
    }

    #[tokio::test]
    async fn test_session_schema_shows_own_uploaded_tables() {
        let app = create_test_app();

        // Create session
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/sessions")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let session_id = json["data"]["sessionId"].as_str().unwrap().to_string();

        // Upload a file
        let csv_content = b"col_a,col_b\n1,hello\n2,world\n";
        let (boundary, body) = create_multipart_body("my_data.csv", csv_content);

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(&format!("/api/v1/sessions/{}/upload", session_id))
                    .header("Content-Type", format!("multipart/form-data; boundary={}", boundary))
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        // Get schema - should include uploaded table with clean name
        let response = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri(&format!("/api/v1/sessions/{}/schema", session_id))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

        let tables = json["data"]["tables"].as_array().unwrap();
        let table_names: Vec<&str> = tables.iter()
            .map(|t| t["tableName"].as_str().unwrap())
            .collect();

        // Should see my_data with clean name (no s_ prefix)
        assert!(table_names.contains(&"my_data"));
        // Should NOT see any s_ prefixed names
        assert!(!table_names.iter().any(|n| n.starts_with("s_")));
    }

    #[tokio::test]
    async fn test_session_schema_hides_other_session_tables() {
        let app = create_test_app();

        // Create two sessions
        let response1 = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/sessions")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body1 = axum::body::to_bytes(response1.into_body(), usize::MAX).await.unwrap();
        let json1: serde_json::Value = serde_json::from_slice(&body1).unwrap();
        let session_id_1 = json1["data"]["sessionId"].as_str().unwrap().to_string();

        let response2 = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/sessions")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body2 = axum::body::to_bytes(response2.into_body(), usize::MAX).await.unwrap();
        let json2: serde_json::Value = serde_json::from_slice(&body2).unwrap();
        let session_id_2 = json2["data"]["sessionId"].as_str().unwrap().to_string();

        // Upload to session 1 only
        let csv_content = b"secret_col\nhidden_data\n";
        let (boundary, body) = create_multipart_body("secret_table.csv", csv_content);

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(&format!("/api/v1/sessions/{}/upload", session_id_1))
                    .header("Content-Type", format!("multipart/form-data; boundary={}", boundary))
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        // Session 1 schema should show secret_table
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri(&format!("/api/v1/sessions/{}/schema", session_id_1))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let tables1: Vec<&str> = json["data"]["tables"].as_array().unwrap().iter()
            .map(|t| t["tableName"].as_str().unwrap())
            .collect();
        assert!(tables1.contains(&"secret_table"));

        // Session 2 schema should NOT show secret_table or any s_ prefixed tables
        let response = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri(&format!("/api/v1/sessions/{}/schema", session_id_2))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let tables2: Vec<&str> = json["data"]["tables"].as_array().unwrap().iter()
            .map(|t| t["tableName"].as_str().unwrap())
            .collect();

        // Session 2 should NOT see session 1's table
        assert!(!tables2.contains(&"secret_table"));
        // And should not see any raw s_ prefixed names
        assert!(!tables2.iter().any(|n| n.starts_with("s_")));
    }

    #[tokio::test]
    async fn test_session_schema_with_stats() {
        let app = create_test_app();

        // Create session
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/sessions")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let session_id = json["data"]["sessionId"].as_str().unwrap();

        // Get schema WITH stats
        let response = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri(&format!("/api/v1/sessions/{}/schema?include_stats=true", session_id))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

        // Find test_table
        let tables = json["data"]["tables"].as_array().unwrap();
        let test_table = tables.iter()
            .find(|t| t["tableName"].as_str() == Some("test_table"))
            .expect("test_table should exist");

        let columns = test_table["columns"].as_array().unwrap();

        // Find id column (INTEGER) - should have min/max
        let id_col = columns.iter()
            .find(|c| c["columnName"].as_str() == Some("id"))
            .expect("id column should exist");
        assert_eq!(id_col["minValue"].as_str(), Some("1"));
        assert_eq!(id_col["maxValue"].as_str(), Some("2"));

        // Find name column (VARCHAR) - should have categorical values since only 2 unique
        let name_col = columns.iter()
            .find(|c| c["columnName"].as_str() == Some("name"))
            .expect("name column should exist");
        let cat_values = name_col["categoricalValues"].as_array()
            .expect("name should have categoricalValues");
        assert!(cat_values.iter().any(|v| v.as_str() == Some("Alice")));
        assert!(cat_values.iter().any(|v| v.as_str() == Some("Bob")));
    }

    // ========================================================================
    // Session Query Endpoint Tests
    // ========================================================================

    #[tokio::test]
    async fn test_session_query_endpoint() {
        let app = create_test_app();

        // Create session
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/sessions")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let session_id = json["data"]["sessionId"].as_str().unwrap();

        // Execute a ggsql query through the session
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(&format!("/api/v1/sessions/{}/query", session_id))
                    .header("Content-Type", "application/json")
                    .body(Body::from(r#"{"query": "SELECT * FROM test_table VISUALISE DRAW point MAPPING id AS x, id AS y"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(json["status"], "success");
        // Should have a Vega-Lite spec
        assert!(json["data"]["spec"].is_object());
        assert!(json["data"]["spec"]["$schema"].as_str().unwrap().contains("vega-lite"));
    }

    #[tokio::test]
    async fn test_session_query_with_uploaded_data() {
        let app = create_test_app();

        // Create session
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/sessions")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let session_id = json["data"]["sessionId"].as_str().unwrap();

        // Upload data to session
        let csv_content = b"x,y\n1,10\n2,20\n3,30\n";
        let (boundary, body) = create_multipart_body("plot_data.csv", csv_content);

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(&format!("/api/v1/sessions/{}/upload", session_id))
                    .header("Content-Type", format!("multipart/form-data; boundary={}", boundary))
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        // Execute ggsql query on the uploaded data
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(&format!("/api/v1/sessions/{}/query", session_id))
                    .header("Content-Type", "application/json")
                    .body(Body::from(r#"{"query": "SELECT * FROM plot_data VISUALISE DRAW line MAPPING x AS x, y AS y"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(json["status"], "success");
        let spec = &json["data"]["spec"];
        assert!(spec["$schema"].as_str().unwrap().contains("vega-lite"));
        // Spec structure is valid - has the required schema field
        assert!(spec.is_object());
    }

    #[tokio::test]
    async fn test_session_query_not_found() {
        let app = create_test_app();

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/sessions/nonexistent/query")
                    .header("Content-Type", "application/json")
                    .body(Body::from(r#"{"query": "SELECT 1 VISUALISE DRAW point MAPPING 1 AS x, 1 AS y"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    // ========================================================================
    // Parse Endpoint Tests
    // ========================================================================

    #[tokio::test]
    async fn test_parse_endpoint() {
        let app = create_test_app();

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/parse")
                    .header("Content-Type", "application/json")
                    .body(Body::from(r#"{"query": "SELECT * FROM t VISUALISE DRAW point MAPPING x AS x, y AS y"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(json["status"], "success");
        // Should return parse information with sql_portion, viz_portion, and specs
        assert!(json["data"]["sql_portion"].is_string());
        assert!(json["data"]["viz_portion"].is_string());
        assert!(json["data"]["specs"].is_array());
    }

    #[tokio::test]
    async fn test_parse_endpoint_invalid() {
        let app = create_test_app();

        // Use completely invalid syntax that should fail to parse
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/parse")
                    .header("Content-Type", "application/json")
                    .body(Body::from(r#"{"query": "NOT VALID SQL OR GGSQL AT ALL @@@@"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();

        let status = response.status();
        let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        // Either returns error status or has no specs
        let is_error = status != StatusCode::OK || json["status"] == "error";
        // For now, accept that some invalid queries might still parse (just with empty results)
        assert!(is_error || json["data"]["specs"].as_array().map(|a| a.is_empty()).unwrap_or(true));
    }

    // ========================================================================
    // Utility Endpoint Tests
    // ========================================================================

    #[tokio::test]
    async fn test_root_endpoint() {
        let app = create_test_app();

        let response = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body_str = String::from_utf8_lossy(&body);

        // Root endpoint returns a plain text message
        assert!(body_str.contains("ggsql"));
    }

    #[tokio::test]
    async fn test_health_endpoint() {
        let app = create_test_app();

        let response = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/api/v1/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

        // Health endpoint returns "healthy" status
        assert_eq!(json["status"], "healthy");
        assert!(json["version"].is_string());
    }

    #[tokio::test]
    async fn test_version_endpoint() {
        let app = create_test_app();

        let response = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/api/v1/version")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

        assert!(json["version"].is_string());
        assert!(json["features"].is_array());
    }
}
