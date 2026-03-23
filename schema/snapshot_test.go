package schema

import (
	"encoding/json"
	"testing"

	"github.com/catgoose/fraggle"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSnapshot(t *testing.T) {
	table := NewTable("Tasks").
		Columns(
			AutoIncrCol("ID"),
			Col("Title", TypeString(255)).NotNull(),
			Col("UserID", TypeInt()).NotNull().References("Users", "ID"),
		).
		WithTimestamps().
		WithSoftDelete().
		WithVersion().
		Indexes(
			Index("idx_tasks_title", "Title"),
		)

	t.Run("struct_fields", func(t *testing.T) {
		snap := table.Snapshot(fraggle.PostgresDialect{})

		assert.Equal(t, "Tasks", snap.Name)
		assert.True(t, snap.HasSoftDelete)
		assert.True(t, snap.HasVersion)
		require.Len(t, snap.Columns, 7) // ID, Title, UserID, CreatedAt, UpdatedAt, DeletedAt, Version

		id := snap.Columns[0]
		assert.Equal(t, "ID", id.Name)
		assert.True(t, id.PrimaryKey)
		assert.True(t, id.AutoIncr)
		assert.False(t, id.Mutable)

		userID := snap.Columns[2]
		assert.Equal(t, "UserID", userID.Name)
		assert.Equal(t, "Users", userID.RefTable)
		assert.Equal(t, "ID", userID.RefColumn)
		assert.True(t, userID.NotNull)

		createdAt := snap.Columns[3]
		assert.Equal(t, "CreatedAt", createdAt.Name)
		assert.Equal(t, "NOW()", createdAt.Default)
		assert.False(t, createdAt.Mutable)

		require.Len(t, snap.Indexes, 1)
		assert.Equal(t, "idx_tasks_title", snap.Indexes[0].Name)
	})

	t.Run("json_serializable", func(t *testing.T) {
		snap := table.Snapshot(fraggle.SQLiteDialect{})
		data, err := json.MarshalIndent(snap, "", "  ")
		require.NoError(t, err)
		assert.Contains(t, string(data), `"name": "Tasks"`)
		assert.Contains(t, string(data), `"has_soft_delete": true`)
	})

	t.Run("dialect_aware_types", func(t *testing.T) {
		pgSnap := table.Snapshot(fraggle.PostgresDialect{})
		msSnap := table.Snapshot(fraggle.MSSQLDialect{})

		// Title column type differs per dialect
		assert.Equal(t, "TEXT", pgSnap.Columns[1].Type)
		assert.Equal(t, "NVARCHAR(255)", msSnap.Columns[1].Type)

		// Timestamp default differs
		assert.Equal(t, "NOW()", pgSnap.Columns[3].Default)
		assert.Equal(t, "GETDATE()", msSnap.Columns[3].Default)
	})
}

func TestSnapshotString(t *testing.T) {
	table := NewTable("Users").
		Columns(
			AutoIncrCol("ID"),
			Col("Email", TypeVarchar(255)).NotNull().Unique(),
			Col("Name", TypeString(255)),
		).
		WithTimestamps().
		Indexes(
			Index("idx_users_email", "Email"),
		)

	s := table.SnapshotString(fraggle.PostgresDialect{})

	assert.Contains(t, s, "TABLE Users")
	assert.Contains(t, s, "ID")
	assert.Contains(t, s, "PRIMARY KEY")
	assert.Contains(t, s, "Email")
	assert.Contains(t, s, "NOT NULL")
	assert.Contains(t, s, "UNIQUE")
	assert.Contains(t, s, "INDEX idx_users_email ON (Email)")
	assert.Contains(t, s, "[immutable]")
}

func TestSnapshotStringMultiTable(t *testing.T) {
	users := NewTable("Users").
		Columns(AutoIncrCol("ID"), Col("Name", TypeString(255)))

	tasks := NewTable("Tasks").
		Columns(AutoIncrCol("ID"), Col("Title", TypeString(255)).NotNull())

	s := SchemaSnapshotString(fraggle.SQLiteDialect{}, users, tasks)
	assert.Contains(t, s, "TABLE Users")
	assert.Contains(t, s, "TABLE Tasks")
}

func TestSnapshotUniqueConstraints(t *testing.T) {
	table := NewMappingTable("UserRoles", "UserID", "RoleID")
	snap := table.Snapshot(fraggle.PostgresDialect{})

	require.Len(t, snap.UniqueConstraints, 1)
	assert.Equal(t, []string{"UserID", "RoleID"}, snap.UniqueConstraints[0])
}

func TestSchemaSnapshot(t *testing.T) {
	users := NewTable("Users").
		Columns(AutoIncrCol("ID"))
	tasks := NewTable("Tasks").
		Columns(AutoIncrCol("ID"))

	snaps := SchemaSnapshot(fraggle.PostgresDialect{}, users, tasks)
	require.Len(t, snaps, 2)
	assert.Equal(t, "Users", snaps[0].Name)
	assert.Equal(t, "Tasks", snaps[1].Name)
}
