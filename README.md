# Fraggle

<img src="https://raw.githubusercontent.com/catgoose/screenshots/main/fraggle/fraggle.jpeg" alt="Fraggle Rock" width="400">

*"Down at Fraggle Rock!"*

Fraggle is a multi-dialect SQL fragment system for Go. Like the Fraggles exploring different caves in the Rock, Fraggle lets your queries travel between SQLite, PostgreSQL, and MSSQL without getting lost.

## What's a Fraggle?

Fraggles are tiny creatures who live in an interconnected series of caves. They're playful, they're curious, and they never worry about which tunnel they're in because every tunnel connects to the same Rock.

That's Fraggle. Write your SQL fragments once. Run them in any cave—er, database.

## Install

```bash
go get github.com/catgoose/fraggle
```

## Usage

```go
import "github.com/catgoose/fraggle"

// Pick your cave
d, _ := fraggle.New(fraggle.Postgres)
// or fraggle.SQLite, or fraggle.MSSQL

// Fraggle knows the local dialect
d.AutoIncrement()  // "SERIAL PRIMARY KEY" in Postgres, "INTEGER PRIMARY KEY AUTOINCREMENT" in SQLite
d.TimestampType()  // "TIMESTAMPTZ" in Postgres, "TIMESTAMP" in SQLite, "DATETIME" in MSSQL
d.Pagination()     // "LIMIT @Limit OFFSET @Offset" vs "OFFSET @Offset ROWS FETCH NEXT @Limit ROWS ONLY"
d.Now()            // "NOW()" vs "CURRENT_TIMESTAMP" vs "GETDATE()"

// Build DDL that works everywhere
d.CreateTableIfNotExists("users", "id "+d.AutoIncrement()+", name "+d.StringType(255)+" NOT NULL")
d.DropTableIfExists("users")
d.CreateIndexIfNotExists("idx_users_email", "users", "email")
```

## Parameterized queries

```go
// Each engine uses different parameter markers — Fraggle handles it
query := "SELECT name FROM users WHERE active = " + d.Placeholder(1) +
    " AND score > " + d.Placeholder(2)
// Postgres: "... active = $1 AND score > $2"
// SQLite:   "... active = ? AND score > ?"
// MSSQL:    "... active = @p1 AND score > @p2"
```

## Returning last insert ID

```go
// Postgres and SQLite support RETURNING clauses
insert := "INSERT INTO users (name) VALUES (" + d.Placeholder(1) + ") " + d.ReturningClause("id")
// Postgres: "INSERT INTO users (name) VALUES ($1) RETURNING id"
// SQLite:   "INSERT INTO users (name) VALUES (?) RETURNING id"
// MSSQL:    "INSERT INTO users (name) VALUES (@p1) "  (use LastInsertIDQuery() instead)

// For MSSQL, query SCOPE_IDENTITY() after the insert
if q := d.LastInsertIDQuery(); q != "" {
    db.QueryRow(q).Scan(&id)
}
```

## Open a connection from a URL

```go
// Fraggle figures out which cave you mean from the URL scheme
db, dialect, _ := fraggle.OpenURL(ctx, "postgres://user:pass@localhost:5432/myapp?sslmode=disable")
db, dialect, _ := fraggle.OpenURL(ctx, "sqlite:///db/app.db")
db, dialect, _ := fraggle.OpenURL(ctx, "sqlserver://user:pass@host:1433?database=erp")
```

`OpenURL` returns a raw `*sql.DB` and the matching `Dialect`. You wire it into your app however you want. Need three connections to three different engines? Call `OpenURL` three times. Fraggle doesn't judge.

For SQLite specifically, `OpenSQLite` opens a database with sensible defaults (WAL mode, 30s busy timeout, single-connection pool):

```go
db, dialect, _ := fraggle.OpenSQLite(ctx, "path/to/app.db")
```

## The Dialect Interface

Every engine implements the same interface:

```go
type Dialect interface {
    Engine() Engine
    Pagination() string
    AutoIncrement() string
    Now() string
    TimestampType() string
    StringType(maxLen int) string
    VarcharType(maxLen int) string
    IntType() string
    TextType() string
    BoolType() string
    Placeholder(n int) string              // "$1" (PG), "?" (SQLite), "@p1" (MSSQL)
    ReturningClause(columns string) string // "RETURNING id" (PG/SQLite), "" (MSSQL)
    CreateTableIfNotExists(table, body string) string
    DropTableIfExists(table string) string
    CreateIndexIfNotExists(indexName, table, columns string) string
    LastInsertIDQuery() string
    SupportsLastInsertID() bool
    TableExistsQuery() string
    TableColumnsQuery() string
}
```

## Engines

| Engine | Constant | Driver | Cave Vibe |
|--------|----------|--------|-----------|
| PostgreSQL | `fraggle.Postgres` | `lib/pq` | The Great Hall — spacious, reliable, everyone hangs out here |
| SQLite | `fraggle.SQLite` | `go-sqlite3` | Gobo's cozy nook — small, self-contained, perfect for one Fraggle |
| MSSQL | `fraggle.MSSQL` | `go-mssqldb` | The Gorgs' garden — big, enterprise-y, slightly intimidating |

## Testing

Tests run against all three engines. SQLite runs in-memory, Postgres and MSSQL run via service containers in CI.

```bash
# Unit tests (always work, no external deps)
go test ./...

# Integration tests against real databases
FRAGGLE_POSTGRES_URL="postgres://user:pass@localhost:5432/testdb?sslmode=disable" \
FRAGGLE_MSSQL_URL="sqlserver://SA:Password@localhost:1433?database=master" \
go test ./... -v
```

## Philosophy

Fraggles don't overthink it. They explore, they play, they build. Fraggle gives you SQL fragments that work across engines. No ORM, no query builder, no magic. Just the right `CREATE TABLE` syntax for whichever database you're pointing at.

*"A Fraggle is never lost. A Fraggle is just exploring."*

## License

MIT — go play in the caves.
