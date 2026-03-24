package schema_test

import (
	"context"
	"database/sql"
	"os"
	"testing"

	"github.com/catgoose/fraggle"
	"github.com/catgoose/fraggle/schema"
	"github.com/stretchr/testify/require"

	_ "github.com/catgoose/fraggle/driver/mssql"
	_ "github.com/catgoose/fraggle/driver/postgres"
	_ "github.com/catgoose/fraggle/driver/sqlite"
)

// testTable defines a representative table using most schema features.
var testTable = schema.NewTable("fraggle_schema_test").
	Columns(
		schema.AutoIncrCol("ID"),
		schema.Col("Name", schema.TypeString(255)).NotNull(),
		schema.Col("Email", schema.TypeVarchar(255)).NotNull().Unique(),
		schema.Col("Bio", schema.TypeText()),
		schema.Col("Score", schema.TypeInt()).NotNull().Default("0"),
		schema.Col("Active", schema.TypeBool()).NotNull().DefaultFn(func(d fraggle.Dialect) string {
			if d.Engine() == fraggle.Postgres {
				return "TRUE"
			}
			return "1"
		}),
	).
	WithTimestamps().
	WithSoftDelete().
	WithVersion().
	Indexes(
		schema.Index("idx_fraggle_schema_test_name", "Name"),
	)

// schemaDriftTest creates a table from the declared schema, then validates it
// using ValidateSchema to verify column names, count, nullability, and indexes match.
func schemaDriftTest(t *testing.T, db *sql.DB, d fraggle.Dialect) {
	t.Helper()
	ctx := context.Background()

	tableName := testTable.TableNameFor(d)

	// Clean up from any previous run
	_, _ = db.ExecContext(ctx, d.DropTableIfExists(tableName))

	// Create from declared schema
	for _, stmt := range testTable.CreateIfNotExistsSQL(d) {
		_, err := db.ExecContext(ctx, stmt)
		require.NoError(t, err, "create table: %s", stmt)
	}
	defer func() {
		_, _ = db.ExecContext(ctx, d.DropTableIfExists(tableName))
	}()

	t.Run("ValidateSchema", func(t *testing.T) {
		for _, e := range schema.ValidateSchema(ctx, db, d, testTable) {
			t.Errorf("schema validation error: %s", e.Error())
		}
	})
}

func TestSchemaDriftSQLite(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	schemaDriftTest(t, db, fraggle.SQLiteDialect{})
}

func TestSchemaDriftPostgres(t *testing.T) {
	dsn := os.Getenv("FRAGGLE_POSTGRES_URL")
	if dsn == "" {
		t.Skip("FRAGGLE_POSTGRES_URL not set")
	}

	ctx := context.Background()
	db, d, err := fraggle.OpenURL(ctx, dsn)
	require.NoError(t, err)
	defer db.Close()

	schemaDriftTest(t, db, d)
}

func TestSchemaDriftMSSQL(t *testing.T) {
	dsn := os.Getenv("FRAGGLE_MSSQL_URL")
	if dsn == "" {
		t.Skip("FRAGGLE_MSSQL_URL not set")
	}

	ctx := context.Background()
	db, d, err := fraggle.OpenURL(ctx, dsn)
	require.NoError(t, err)
	defer db.Close()

	schemaDriftTest(t, db, d)
}
