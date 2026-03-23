package dbrepo

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestWhereBuilder(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		w := NewWhere()
		assert.False(t, w.HasConditions())
		assert.Equal(t, "", w.String())
		assert.Empty(t, w.Args())
	})

	t.Run("single_condition", func(t *testing.T) {
		w := NewWhere().And("ID = 1")
		assert.True(t, w.HasConditions())
		assert.Equal(t, "WHERE ID = 1", w.String())
	})

	t.Run("multiple_and", func(t *testing.T) {
		w := NewWhere().And("A = 1").And("B = 2")
		assert.Equal(t, "WHERE A = 1 AND B = 2", w.String())
	})

	t.Run("or", func(t *testing.T) {
		w := NewWhere().And("A = 1").Or("B = 2")
		assert.Equal(t, "WHERE A = 1 OR B = 2", w.String())
	})

	t.Run("and_if_true", func(t *testing.T) {
		w := NewWhere().AndIf(true, "A = 1")
		assert.Equal(t, "WHERE A = 1", w.String())
	})

	t.Run("and_if_false", func(t *testing.T) {
		w := NewWhere().AndIf(false, "A = 1")
		assert.False(t, w.HasConditions())
	})

	t.Run("or_if_true", func(t *testing.T) {
		w := NewWhere().And("A = 1").OrIf(true, "B = 2")
		assert.Equal(t, "WHERE A = 1 OR B = 2", w.String())
	})

	t.Run("or_if_false", func(t *testing.T) {
		w := NewWhere().And("A = 1").OrIf(false, "B = 2")
		assert.Equal(t, "WHERE A = 1", w.String())
	})
}

func TestWhereTraitFilters(t *testing.T) {
	t.Run("not_deleted", func(t *testing.T) {
		w := NewWhere().NotDeleted()
		assert.Equal(t, "WHERE DeletedAt IS NULL", w.String())
	})

	t.Run("not_expired", func(t *testing.T) {
		w := NewWhere().NotExpired()
		assert.Contains(t, w.String(), "ExpiresAt IS NULL OR ExpiresAt > CURRENT_TIMESTAMP")
	})

	t.Run("has_status", func(t *testing.T) {
		w := NewWhere().HasStatus("active")
		assert.Contains(t, w.String(), "Status = @Status")
		assert.Len(t, w.Args(), 1)
	})

	t.Run("not_archived", func(t *testing.T) {
		w := NewWhere().NotArchived()
		assert.Equal(t, "WHERE ArchivedAt IS NULL", w.String())
	})

	t.Run("has_version", func(t *testing.T) {
		w := NewWhere().HasVersion(3)
		assert.Contains(t, w.String(), "Version = @Version")
	})

	t.Run("is_root", func(t *testing.T) {
		w := NewWhere().IsRoot()
		assert.Equal(t, "WHERE ParentID IS NULL", w.String())
	})

	t.Run("has_parent", func(t *testing.T) {
		w := NewWhere().HasParent(42)
		assert.Contains(t, w.String(), "ParentID = @ParentID")
	})

	t.Run("not_replaced", func(t *testing.T) {
		w := NewWhere().NotReplaced()
		assert.Equal(t, "WHERE ReplacedByID IS NULL", w.String())
	})

	t.Run("composed", func(t *testing.T) {
		w := NewWhere().
			NotDeleted().
			NotArchived().
			HasStatus("active")
		assert.Contains(t, w.String(), "DeletedAt IS NULL")
		assert.Contains(t, w.String(), "ArchivedAt IS NULL")
		assert.Contains(t, w.String(), "Status = @Status")
	})
}

func TestWhereSearch(t *testing.T) {
	t.Run("with_search", func(t *testing.T) {
		w := NewWhere().Search("gobo", "Name", "Email")
		assert.Contains(t, w.String(), "Name LIKE @SearchPattern")
		assert.Contains(t, w.String(), "Email LIKE @SearchPattern")
		assert.Len(t, w.Args(), 2)
	})

	t.Run("empty_search", func(t *testing.T) {
		w := NewWhere().Search("", "Name")
		assert.False(t, w.HasConditions())
	})

	t.Run("no_fields", func(t *testing.T) {
		w := NewWhere().Search("gobo")
		assert.False(t, w.HasConditions())
	})

	t.Run("rejects_invalid_field_names", func(t *testing.T) {
		w := NewWhere().Search("gobo", "Name; DROP TABLE users--", "Email")
		// Only Email should survive validation
		assert.Contains(t, w.String(), "Email LIKE @SearchPattern")
		assert.NotContains(t, w.String(), "DROP TABLE")
	})

	t.Run("rejects_all_invalid_fields", func(t *testing.T) {
		w := NewWhere().Search("gobo", "1bad", "'; DROP TABLE--")
		assert.False(t, w.HasConditions())
	})

	t.Run("allows_qualified_names", func(t *testing.T) {
		w := NewWhere().Search("gobo", "t.Name", "u.Email")
		assert.Contains(t, w.String(), "t.Name LIKE @SearchPattern")
		assert.Contains(t, w.String(), "u.Email LIKE @SearchPattern")
	})
}
