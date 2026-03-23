// Package fraggle provides database engine abstractions for composable SQL fragments.
// It allows switching between database engines (e.g., MSSQL for production, SQLite for development)
// while keeping SQL visible and explicit.
package fraggle

import "fmt"

// Engine identifies a database engine.
type Engine string

const (
	MSSQL    Engine = "sqlserver"
	SQLite   Engine = "sqlite3"
	Postgres Engine = "postgres"
)

// ParseEngine converts a string to an Engine, returning an error for unknown values.
func ParseEngine(s string) (Engine, error) {
	switch s {
	case "sqlserver", "mssql":
		return MSSQL, nil
	case "sqlite3", "sqlite":
		return SQLite, nil
	case "postgres", "postgresql":
		return Postgres, nil
	default:
		return "", fmt.Errorf("unknown database engine: %q (expected sqlserver, mssql, sqlite3, sqlite, postgres, or postgresql)", s)
	}
}

// Dialect provides engine-specific SQL fragments.
// Implementations return raw SQL strings that callers compose into full queries.
type Dialect interface {
	// Engine returns the engine identifier (used as the driver name for sql.Open).
	Engine() Engine

	// Pagination returns the pagination clause for the engine.
	//   MSSQL:    "OFFSET @Offset ROWS FETCH NEXT @Limit ROWS ONLY"
	//   SQLite:   "LIMIT @Limit OFFSET @Offset"
	//   Postgres: "LIMIT @Limit OFFSET @Offset"
	Pagination() string

	// AutoIncrement returns the column definition fragment for an auto-incrementing primary key.
	//   MSSQL:    "INT PRIMARY KEY IDENTITY(1,1)"
	//   SQLite:   "INTEGER PRIMARY KEY AUTOINCREMENT"
	//   Postgres: "SERIAL PRIMARY KEY"
	AutoIncrement() string

	// Now returns the SQL expression for the current timestamp.
	//   MSSQL:    "GETDATE()"
	//   SQLite:   "CURRENT_TIMESTAMP"
	//   Postgres: "NOW()"
	Now() string

	// TimestampType returns the column type for timestamps.
	//   MSSQL:    "DATETIME"
	//   SQLite:   "TIMESTAMP"
	//   Postgres: "TIMESTAMPTZ"
	TimestampType() string

	// StringType returns the column type for a string with the given max length.
	//   MSSQL:    "NVARCHAR(255)"
	//   SQLite:   "TEXT"
	//   Postgres: "VARCHAR(255)"
	StringType(maxLen int) string

	// VarcharType returns the column type for a varchar with the given max length.
	//   MSSQL:    "VARCHAR(255)"
	//   SQLite:   "TEXT"
	//   Postgres: "VARCHAR(255)"
	VarcharType(maxLen int) string

	// IntType returns the column type for an integer.
	//   MSSQL:    "INT"
	//   SQLite:   "INTEGER"
	//   Postgres: "INTEGER"
	IntType() string

	// TextType returns the column type for unlimited text.
	//   MSSQL:    "NVARCHAR(MAX)"
	//   SQLite:   "TEXT"
	//   Postgres: "TEXT"
	TextType() string

	// BoolType returns the column type for booleans.
	//   MSSQL:    "BIT"
	//   SQLite:   "INTEGER"
	//   Postgres: "BOOLEAN"
	BoolType() string

	// Placeholder returns the parameter placeholder for the nth argument (1-based).
	//   MSSQL:    "@p1"
	//   SQLite:   "?"
	//   Postgres: "$1"
	Placeholder(n int) string

	// ReturningClause returns a RETURNING clause for INSERT/UPDATE statements,
	// or empty string if the engine doesn't support it.
	//   MSSQL:    ""
	//   SQLite:   "RETURNING id"  (SQLite 3.35+)
	//   Postgres: "RETURNING id"
	ReturningClause(columns string) string

	// QuoteIdentifier quotes a SQL identifier (table name, column name, index name)
	// using the engine-specific quoting style.
	//   MSSQL:    [users]
	//   SQLite:   "users"
	//   Postgres: "users"
	QuoteIdentifier(name string) string

	// BigIntType returns the column type for a 64-bit integer.
	//   MSSQL:    "BIGINT"
	//   SQLite:   "INTEGER"
	//   Postgres: "BIGINT"
	BigIntType() string

	// FloatType returns the column type for a floating-point number.
	//   MSSQL:    "FLOAT"
	//   SQLite:   "REAL"
	//   Postgres: "DOUBLE PRECISION"
	FloatType() string

	// DecimalType returns the column type for an exact numeric with precision and scale.
	//   MSSQL:    "DECIMAL(10,2)"
	//   SQLite:   "REAL"
	//   Postgres: "NUMERIC(10,2)"
	DecimalType(precision, scale int) string

	// UUIDType returns the column type for UUIDs.
	//   MSSQL:    "UNIQUEIDENTIFIER"
	//   SQLite:   "TEXT"
	//   Postgres: "UUID"
	UUIDType() string

	// JSONType returns the column type for JSON data.
	//   MSSQL:    "NVARCHAR(MAX)"
	//   SQLite:   "TEXT"
	//   Postgres: "JSONB"
	JSONType() string

	// CreateTableIfNotExists wraps a CREATE TABLE body so that it only runs
	// when the table does not already exist.
	CreateTableIfNotExists(table, body string) string

	// DropTableIfExists returns the statement to drop a table if it exists.
	DropTableIfExists(table string) string

	// CreateIndexIfNotExists returns the statement to create an index if it doesn't exist.
	CreateIndexIfNotExists(indexName, table, columns string) string

	// LastInsertIDQuery returns SQL to retrieve the last inserted ID, or empty string
	// if the driver supports Result.LastInsertId() natively.
	LastInsertIDQuery() string

	// SupportsLastInsertID reports whether the driver supports Result.LastInsertId().
	SupportsLastInsertID() bool

	// TableExistsQuery returns a query that checks whether a table exists.
	// The query accepts a single positional parameter for the table name
	// and returns one row if the table exists.
	TableExistsQuery() string

	// TableColumnsQuery returns a query that lists column names for a table.
	// The query accepts a single positional parameter for the table name
	// and returns rows with a "name" column.
	TableColumnsQuery() string

	// InsertOrIgnore returns an idempotent INSERT statement that silently
	// skips rows that would violate a unique constraint.
	//   SQLite:   "INSERT OR IGNORE INTO t (cols) VALUES (vals)"
	//   Postgres: "INSERT INTO t (cols) VALUES (vals) ON CONFLICT DO NOTHING"
	//   MSSQL:    wraps the insert in an IF NOT EXISTS check using the first column
	InsertOrIgnore(table, columns, values string) string
}

// New returns a Dialect for the given engine.
func New(engine Engine) (Dialect, error) {
	switch engine {
	case MSSQL:
		return MSSQLDialect{}, nil
	case SQLite:
		return SQLiteDialect{}, nil
	case Postgres:
		return PostgresDialect{}, nil
	default:
		return nil, fmt.Errorf("unsupported database engine: %q", engine)
	}
}
