//! Data source abstraction layer for ggsql
//!
//! The reader module provides a pluggable interface for executing SQL queries
//! against various data sources and returning Polars DataFrames for visualization.
//!
//! # Architecture
//!
//! All readers implement the `Reader` trait, which provides:
//! - SQL query execution → DataFrame conversion
//! - Optional DataFrame registration for queryable tables
//! - Connection management and error handling
//!
//! # Example
//!
//! ```rust,ignore
//! use ggsql::reader::{Reader, DuckDBReader};
//!
//! // Basic usage
//! let reader = DuckDBReader::from_connection_string("duckdb://memory")?;
//! let df = reader.execute("SELECT * FROM table")?;
//!
//! // With DataFrame registration
//! let mut reader = DuckDBReader::from_connection_string("duckdb://memory")?;
//! reader.register("my_table", some_dataframe)?;
//! let result = reader.execute("SELECT * FROM my_table")?;
//! ```

use crate::{DataFrame, GgsqlError, Result};

#[cfg(feature = "duckdb")]
pub mod duckdb;

pub mod connection;

pub mod data;

#[cfg(feature = "duckdb")]
pub use duckdb::DuckDBReader;

/// Trait for data source readers
///
/// Readers execute SQL queries and return Polars DataFrames.
/// They provide a uniform interface for different database backends.
///
/// # DataFrame Registration
///
/// Some readers support registering DataFrames as queryable tables using
/// the [`register`](Reader::register) method. This allows you to query
/// in-memory DataFrames with SQL, join them with other tables, etc.
///
/// ```rust,ignore
/// // Register a DataFrame (takes ownership)
/// reader.register("sales", sales_df)?;
///
/// // Now you can query it
/// let result = reader.execute("SELECT * FROM sales WHERE amount > 100")?;
/// ```
pub trait Reader {
    /// Execute a SQL query and return the result as a DataFrame
    ///
    /// # Arguments
    ///
    /// * `sql` - The SQL query to execute
    ///
    /// # Returns
    ///
    /// A Polars DataFrame containing the query results
    ///
    /// # Errors
    ///
    /// Returns `GgsqlError::ReaderError` if:
    /// - The SQL is invalid
    /// - The connection fails
    /// - The table or columns don't exist
    fn execute(&self, sql: &str) -> Result<DataFrame>;

    /// Register a DataFrame as a queryable table (takes ownership)
    ///
    /// After registration, the DataFrame can be queried by name in SQL:
    /// ```sql
    /// SELECT * FROM <name> WHERE ...
    /// ```
    ///
    /// # Arguments
    ///
    /// * `name` - The table name to register under
    /// * `df` - The DataFrame to register (ownership is transferred)
    ///
    /// # Returns
    ///
    /// `Ok(())` on success, error if registration fails or isn't supported.
    ///
    /// # Default Implementation
    ///
    /// Returns an error by default. Override for readers that support registration.
    fn register(&mut self, name: &str, _df: DataFrame) -> Result<()> {
        Err(GgsqlError::ReaderError(format!(
            "This reader does not support DataFrame registration for table '{}'",
            name
        )))
    }

    /// Check if this reader supports DataFrame registration
    ///
    /// # Returns
    ///
    /// `true` if [`register`](Reader::register) is implemented, `false` otherwise.
    fn supports_register(&self) -> bool {
        false
    }
}
