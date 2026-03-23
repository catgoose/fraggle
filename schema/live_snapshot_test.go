package schema

import (
	"context"
	"database/sql"
	"testing"

	"github.com/catgoose/fraggle"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	_ "github.com/catgoose/fraggle/driver/sqlite"
)

func openTestDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })
	return db
}

func TestLiveSnapshot(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	d := fraggle.SQLiteDialect{}

	table := NewTable("Tasks").
		Columns(
			AutoIncrCol("ID"),
			Col("Title", TypeString(255)).NotNull(),
			Col("Description", TypeText()),
		).
		WithTimestamps().
		WithSoftDelete().
		Indexes(
			Index("idx_tasks_title", "Title"),
		)

	// Create the table
	for _, stmt := range table.CreateIfNotExistsSQL(d) {
		_, err := db.ExecContext(ctx, stmt)
		require.NoError(t, err)
	}

	snap, err := LiveSnapshot(ctx, db, d, "Tasks")
	require.NoError(t, err)

	assert.Equal(t, "Tasks", snap.Name)

	// Should have all columns
	colNames := make([]string, len(snap.Columns))
	for i, c := range snap.Columns {
		colNames[i] = c.Name
	}
	assert.Contains(t, colNames, "ID")
	assert.Contains(t, colNames, "Title")
	assert.Contains(t, colNames, "Description")
	assert.Contains(t, colNames, "CreatedAt")
	assert.Contains(t, colNames, "UpdatedAt")
	assert.Contains(t, colNames, "DeletedAt")

	// Check nullability
	for _, c := range snap.Columns {
		switch c.Name {
		case "Title", "CreatedAt", "UpdatedAt":
			assert.False(t, c.Nullable, "%s should be NOT NULL", c.Name)
		case "Description", "DeletedAt":
			assert.True(t, c.Nullable, "%s should be nullable", c.Name)
		}
	}

	// Should have the index
	require.Len(t, snap.Indexes, 1)
	assert.Equal(t, "idx_tasks_title", snap.Indexes[0].Name)
}

func TestLiveSnapshotTableNotExists(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	d := fraggle.SQLiteDialect{}

	_, err := LiveSnapshot(ctx, db, d, "nonexistent")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "does not exist")
}

func TestLiveSnapshotString(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	d := fraggle.SQLiteDialect{}

	table := NewTable("Users").
		Columns(
			AutoIncrCol("ID"),
			Col("Email", TypeVarchar(255)).NotNull(),
		)

	for _, stmt := range table.CreateIfNotExistsSQL(d) {
		_, err := db.ExecContext(ctx, stmt)
		require.NoError(t, err)
	}

	snap, err := LiveSnapshot(ctx, db, d, "Users")
	require.NoError(t, err)

	s := snap.String()
	assert.Contains(t, s, "TABLE Users")
	assert.Contains(t, s, "ID")
	assert.Contains(t, s, "Email")
	assert.Contains(t, s, "NOT NULL")
}

func TestLiveSchemaSnapshot(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	d := fraggle.SQLiteDialect{}

	users := NewTable("Users").
		Columns(AutoIncrCol("ID"), Col("Name", TypeString(255)))
	tasks := NewTable("Tasks").
		Columns(AutoIncrCol("ID"), Col("Title", TypeString(255)))

	for _, tbl := range []*TableDef{users, tasks} {
		for _, stmt := range tbl.CreateIfNotExistsSQL(d) {
			_, err := db.ExecContext(ctx, stmt)
			require.NoError(t, err)
		}
	}

	snaps, err := LiveSchemaSnapshot(ctx, db, d, "Users", "Tasks")
	require.NoError(t, err)
	require.Len(t, snaps, 2)
	assert.Equal(t, "Users", snaps[0].Name)
	assert.Equal(t, "Tasks", snaps[1].Name)
}

func TestLiveSnapshotCompareWithDeclared(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	d := fraggle.SQLiteDialect{}

	table := NewTable("Items").
		Columns(
			AutoIncrCol("ID"),
			Col("Name", TypeString(255)).NotNull(),
			Col("Status", TypeVarchar(50)).NotNull().Default("'active'"),
		)

	for _, stmt := range table.CreateIfNotExistsSQL(d) {
		_, err := db.ExecContext(ctx, stmt)
		require.NoError(t, err)
	}

	declared := table.Snapshot(d)
	live, err := LiveSnapshot(ctx, db, d, "Items")
	require.NoError(t, err)

	// Column count should match
	assert.Equal(t, len(declared.Columns), len(live.Columns))

	// Column names should match
	for i, dc := range declared.Columns {
		assert.Equal(t, dc.Name, live.Columns[i].Name)
	}

	// Nullability should match
	for i, dc := range declared.Columns {
		assert.Equal(t, dc.NotNull, !live.Columns[i].Nullable,
			"nullability mismatch for column %s", dc.Name)
	}
}
