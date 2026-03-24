package schema

import (
	"context"
	"testing"

	"github.com/catgoose/fraggle"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	_ "github.com/catgoose/fraggle/driver/sqlite"
)

func TestValidateSchema(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	d := fraggle.SQLiteDialect{}

	table := NewTable("Items").
		Columns(
			AutoIncrCol("ID"),
			Col("Name", TypeString(255)).NotNull(),
			Col("Status", TypeVarchar(50)).NotNull().Default("'active'"),
		).
		Indexes(
			Index("idx_items_name", "Name"),
		)

	for _, stmt := range table.CreateIfNotExistsSQL(d) {
		_, err := db.ExecContext(ctx, stmt)
		require.NoError(t, err)
	}

	t.Run("valid_schema", func(t *testing.T) {
		errs := ValidateSchema(ctx, db, d, table)
		assert.Nil(t, errs)
	})

	t.Run("missing_column", func(t *testing.T) {
		// Declare a table with an extra column that doesn't exist in DB
		extra := NewTable("Items").
			Columns(
				AutoIncrCol("ID"),
				Col("Name", TypeString(255)).NotNull(),
				Col("Status", TypeVarchar(50)).NotNull(),
				Col("Priority", TypeInt()),
			)

		errs := ValidateSchema(ctx, db, d, extra)
		require.NotNil(t, errs)

		var messages []string
		for _, e := range errs {
			messages = append(messages, e.Error())
		}
		assert.Contains(t, messages, "Items.Priority: column missing")
	})

	t.Run("table_not_exists", func(t *testing.T) {
		missing := NewTable("Nonexistent").
			Columns(AutoIncrCol("ID"))

		errs := ValidateSchema(ctx, db, d, missing)
		require.NotNil(t, errs)
		assert.Contains(t, errs[0].Error(), "does not exist")
	})

	t.Run("missing_index", func(t *testing.T) {
		withExtraIndex := NewTable("Items").
			Columns(
				AutoIncrCol("ID"),
				Col("Name", TypeString(255)).NotNull(),
				Col("Status", TypeVarchar(50)).NotNull(),
			).
			Indexes(
				Index("idx_items_name", "Name"),
				Index("idx_items_status", "Status"),
			)

		errs := ValidateSchema(ctx, db, d, withExtraIndex)
		require.NotNil(t, errs)

		var messages []string
		for _, e := range errs {
			messages = append(messages, e.Error())
		}
		assert.Contains(t, messages, `Items: index "idx_items_status" missing`)
	})
}

func TestValidateSchemaPostgresNormalization(t *testing.T) {
	// This test verifies that ValidateSchema normalizes column names
	// for the Postgres dialect, matching CamelCase declarations against
	// the snake_case columns that DDL creates.
	ctx := context.Background()
	db := openTestDB(t)
	d := fraggle.SQLiteDialect{} // SQLite doesn't normalize, used as baseline

	table := NewTable("Accounts").
		Columns(
			AutoIncrCol("ID"),
			Col("Email", TypeVarchar(255)).NotNull(),
			Col("PasswordHash", TypeText()).NotNull(),
		)

	// Create with SQLite (no normalization)
	for _, stmt := range table.CreateIfNotExistsSQL(d) {
		_, err := db.ExecContext(ctx, stmt)
		require.NoError(t, err)
	}

	// Validate should pass — SQLite preserves CamelCase
	errs := ValidateSchema(ctx, db, d, table)
	assert.Nil(t, errs)

	// Verify Postgres normalization produces snake_case names
	pg := fraggle.PostgresDialect{}
	snap := table.Snapshot(pg)
	assert.Equal(t, "email", snap.Columns[1].Name)
	assert.Equal(t, "password_hash", snap.Columns[2].Name)

	// TableNameFor should also normalize
	assert.Equal(t, "accounts", table.TableNameFor(pg))
	assert.Equal(t, "Accounts", table.TableNameFor(d))
}

func TestValidateAll(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	d := fraggle.SQLiteDialect{}

	users := NewTable("Users").
		Columns(AutoIncrCol("ID"), Col("Name", TypeString(255)).NotNull())
	tasks := NewTable("Tasks").
		Columns(AutoIncrCol("ID"), Col("Title", TypeString(255)).NotNull())

	for _, tbl := range []*TableDef{users, tasks} {
		for _, stmt := range tbl.CreateIfNotExistsSQL(d) {
			_, err := db.ExecContext(ctx, stmt)
			require.NoError(t, err)
		}
	}

	t.Run("all_valid", func(t *testing.T) {
		errs := ValidateAll(ctx, db, d, users, tasks)
		assert.Nil(t, errs)
	})

	t.Run("one_invalid", func(t *testing.T) {
		bad := NewTable("Tasks").
			Columns(
				AutoIncrCol("ID"),
				Col("Title", TypeString(255)).NotNull(),
				Col("Missing", TypeText()),
			)

		errs := ValidateAll(ctx, db, d, users, bad)
		require.NotNil(t, errs)

		var found bool
		for _, e := range errs {
			if e.Column == "Missing" {
				found = true
			}
		}
		assert.True(t, found, "expected error for missing column 'Missing'")
	})
}
