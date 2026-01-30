"""Reader classes for ggsql."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

import narwhals as nw
from narwhals.typing import IntoDataFrame

from ggsql._ggsql import DuckDBReader as _DuckDBReader

if TYPE_CHECKING:
    import polars as pl
    from ggsql._ggsql import Prepared

__all__ = ["Reader", "DuckDB"]


class Reader(ABC):
    """Abstract base class for ggsql readers.

    Custom reader implementations should subclass this and implement
    the required abstract methods. The built-in `DuckDB` reader provides
    a complete implementation.

    Examples
    --------
    >>> from ggsql.readers import Reader
    >>> import polars as pl
    >>>
    >>> class MyReader(Reader):
    ...     def __init__(self):
    ...         self._tables = {}
    ...
    ...     def execute(self, query: str, data=None) -> "Prepared":
    ...         # Register tables, execute query, cleanup
    ...         ...
    ...
    ...     def execute_sql(self, sql: str) -> pl.DataFrame:
    ...         # Your SQL execution logic here
    ...         return pl.DataFrame({"x": [1, 2, 3], "y": [10, 20, 30]})
    ...
    ...     def register(self, name: str, df) -> None:
    ...         self._tables[name] = df
    ...
    ...     def unregister(self, name: str) -> None:
    ...         self._tables.pop(name, None)
    """

    @abstractmethod
    def execute(
        self,
        query: str,
        data: dict[str, IntoDataFrame] | None = None,
    ) -> "Prepared":
        """Execute a ggsql query with optional DataFrame registration.

        DataFrames should be registered before query execution and automatically
        unregistered afterward (even on error) to avoid polluting the namespace.

        Parameters
        ----------
        query
            The ggsql query to execute. Must contain a VISUALISE clause.
        data
            DataFrames to register as queryable tables. Keys are table names.

        Returns
        -------
        Prepared
            A prepared visualization ready for rendering.

        Raises
        ------
        NoVisualiseError
            If the query has no VISUALISE clause.
        """
        ...

    @abstractmethod
    def execute_sql(self, sql: str) -> "pl.DataFrame":
        """Execute a SQL query and return the result as a DataFrame.

        This is for plain SQL queries without visualization. For ggsql queries
        with VISUALISE clauses, use execute() instead.

        Parameters
        ----------
        sql
            The SQL query to execute.

        Returns
        -------
        polars.DataFrame
            The query result as a polars DataFrame.
        """
        ...

    @abstractmethod
    def register(self, name: str, df: IntoDataFrame) -> None:
        """Register a DataFrame as a queryable table.

        Parameters
        ----------
        name
            The table name to register under.
        df
            The DataFrame to register.
        """
        ...

    @abstractmethod
    def unregister(self, name: str) -> None:
        """Unregister a table by name.

        Should fail silently if the table doesn't exist.

        Parameters
        ----------
        name
            The table name to unregister.
        """
        ...

    def __enter__(self) -> "Reader":
        """Enter context manager."""
        return self

    def __exit__(self, _exc_type, _exc_val, _exc_tb) -> None:
        """Exit context manager."""
        pass


def _to_polars(df: IntoDataFrame) -> "pl.DataFrame":
    """Convert any narwhals-compatible DataFrame to polars."""
    nw_df = nw.from_native(df, pass_through=True)

    if isinstance(nw_df, nw.LazyFrame):
        nw_df = nw_df.collect()

    if not isinstance(nw_df, nw.DataFrame):
        raise TypeError("df must be a DataFrame (polars, pandas, pyarrow, etc.)")

    return nw_df.to_polars()


class DuckDB(Reader):
    """DuckDB database reader for executing SQL queries and ggsql visualizations.

    Creates an in-memory or file-based DuckDB connection that can execute
    SQL queries and register DataFrames as queryable tables.

    Accepts any narwhals-compatible DataFrame (polars, pandas, pyarrow, etc.)
    for data registration.

    Examples
    --------
    >>> import ggsql.readers
    >>> reader = ggsql.readers.DuckDB("duckdb://memory")
    >>> reader = ggsql.readers.DuckDB("duckdb:///path/to/file.db")
    """

    def __init__(self, connection: str) -> None:
        """Create a new DuckDB reader from a connection string.

        Parameters
        ----------
        connection
            Connection string. Use "duckdb://memory" for in-memory database
            or "duckdb:///path/to/file.db" for file-based database.
        """
        self._inner = _DuckDBReader(connection)
        self._connection = connection

    def __repr__(self) -> str:
        return f"<DuckDB connection={self._connection!r}>"

    def execute(
        self,
        query: str,
        data: dict[str, IntoDataFrame] | None = None,
    ) -> "Prepared":
        """Execute a ggsql query with optional DataFrame registration.

        DataFrames are registered before query execution and automatically
        unregistered afterward (even on error) to avoid polluting the namespace.

        Parameters
        ----------
        query
            The ggsql query to execute. Must contain a VISUALISE clause.
        data
            DataFrames to register as queryable tables. Keys are table names.
            Accepts any narwhals-compatible DataFrame (polars, pandas, pyarrow, etc.).

        Returns
        -------
        Prepared
            A prepared visualization ready for rendering.

        Raises
        ------
        NoVisualiseError
            If the query has no VISUALISE clause.
        ValueError
            If parsing, validation, or SQL execution fails.
        """
        polars_data: dict[str, "pl.DataFrame"] | None = None
        if data is not None:
            polars_data = {name: _to_polars(df) for name, df in data.items()}

        return self._inner.execute(query, polars_data)

    def execute_sql(self, sql: str) -> "pl.DataFrame":
        """Execute a SQL query and return the result as a DataFrame.

        This is for plain SQL queries without visualization. For ggsql queries
        with VISUALISE clauses, use execute() instead.

        Parameters
        ----------
        sql
            The SQL query to execute.

        Returns
        -------
        polars.DataFrame
            The query result as a polars DataFrame.
        """
        return self._inner.execute_sql(sql)

    def register(self, name: str, df: IntoDataFrame) -> None:
        """Register a DataFrame as a queryable table.

        After registration, the DataFrame can be queried by name in SQL.
        Note: When using execute(), DataFrames are automatically registered
        and unregistered, so manual registration is usually unnecessary.

        Parameters
        ----------
        name
            The table name to register under.
        df
            The DataFrame to register. Accepts any narwhals-compatible
            DataFrame (polars, pandas, pyarrow, etc.).
        """
        self._inner.register(name, _to_polars(df))

    def unregister(self, name: str) -> None:
        """Unregister a table by name.

        Fails silently if the table doesn't exist.

        Parameters
        ----------
        name
            The table name to unregister.
        """
        self._inner.unregister(name)

    def __enter__(self) -> "DuckDB":
        """Enter context manager."""
        return self

    def __exit__(self, _exc_type, _exc_val, _exc_tb) -> None:
        """Exit context manager.

        Currently a no-op since DuckDB connections don't require explicit cleanup,
        but future-proofs the API for connection management.
        """
        pass
