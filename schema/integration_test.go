package schema_test

import (
	"context"
	"database/sql"
	"os"
	"testing"

	"github.com/catgoose/fraggle"
	"github.com/catgoose/fraggle/schema"
	"github.com/stretchr/testify/assert"
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
		schema.Col("Active", schema.TypeBool()).NotNull().Default("1"),
	).
	WithTimestamps().
	WithSoftDelete().
	WithVersion().
	Indexes(
		schema.Index("idx_fraggle_schema_test_name", "Name"),
	)

// schemaDriftTest creates a table from the declared schema, then reads it back
// via LiveSnapshot and verifies that column names, count, and nullability match.
func schemaDriftTest(t *testing.T, db *sql.DB, d fraggle.Dialect) {
	t.Helper()
	ctx := context.Background()

	// Clean up from any previous run
	_, _ = db.ExecContext(ctx, d.DropTableIfExists("fraggle_schema_test"))

	// Create from declared schema
	for _, stmt := range testTable.CreateIfNotExistsSQL(d) {
		_, err := db.ExecContext(ctx, stmt)
		require.NoError(t, err, "create table: %s", stmt)
	}
	defer func() {
		_, _ = db.ExecContext(ctx, d.DropTableIfExists("fraggle_schema_test"))
	}()

	// Snapshot: declared vs live
	declared := testTable.Snapshot(d)
	live, err := schema.LiveSnapshot(ctx, db, d, "fraggle_schema_test")
	require.NoError(t, err)

	t.Run("ColumnCount", func(t *testing.T) {
		assert.Equal(t, len(declared.Columns), len(live.Columns),
			"declared %d columns, live %d columns\ndeclared: %s\nlive: %s",
			len(declared.Columns), len(live.Columns),
			testTable.SnapshotString(d), live.String())
	})

	t.Run("ColumnNames", func(t *testing.T) {
		for i, dc := range declared.Columns {
			if i >= len(live.Columns) {
				t.Errorf("missing live column at position %d (declared: %s)", i, dc.Name)
				continue
			}
			assert.Equal(t, dc.Name, live.Columns[i].Name,
				"column name mismatch at position %d", i)
		}
	})

	t.Run("Nullability", func(t *testing.T) {
		for i, dc := range declared.Columns {
			if i >= len(live.Columns) {
				continue
			}
			lc := live.Columns[i]
			assert.Equal(t, dc.NotNull, !lc.Nullable,
				"nullability mismatch for column %s: declared NOT NULL=%v, live nullable=%v",
				dc.Name, dc.NotNull, lc.Nullable)
		}
	})

	t.Run("Indexes", func(t *testing.T) {
		// Verify declared indexes exist in the live schema
		liveIndexNames := make(map[string]bool)
		for _, idx := range live.Indexes {
			liveIndexNames[idx.Name] = true
		}
		for _, idx := range declared.Indexes {
			assert.True(t, liveIndexNames[idx.Name],
				"declared index %q not found in live database", idx.Name)
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
